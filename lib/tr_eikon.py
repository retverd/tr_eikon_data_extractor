from datetime import datetime
from logging import getLogger
from os import system
from time import sleep
from typing import Optional

import eikon as ek
import pandas as pd
from pywinauto import Application
from pywinauto.findwindows import ElementNotFoundError
from pywinauto.keyboard import send_keys

from gas_prices import gas_rics, _GAS_NO_VOL
from lib.email import send_email, MailSubjects
from lib.env import get_env

_EIKON_PATH = get_env('EIKON_PATH')
_EIKON_DATE_FORMAT = '%Y-%m-%d'
_SAVE_TIMESTAMP_FORMATTER = '%Y-%m-%dT%H-%M-%S'
_SPLASH_SCREEN_TITLE = get_env('EIKON_SPLASH_SCREEN_TITLE')
_TOOLBAR_TITLE = 'Eikon Toolbar'
_RETRIEVE_EIKON_DATA = 'Загрузка данных из TR Eikon'
_DONE_MSG = 'Готово!'
login_timer = 25
app_timer = 50

_GAS_VOL_NAME = 'VOLUME'
_DEF_UNIT = 'MWh'
gas_fields = ['HIGH', 'LOW', 'OPEN', 'CLOSE', 'VOLUME']
gas_prices_folder = 'gas_data'
gas_interval = 'daily'

fx_rates_rics = ['USDEUR=R', 'PLNEUR=R', 'GBPEUR=R', 'CZKEUR=R', 'HRKEUR=R', 'HUFEUR=R', 'BGNEUR=R', 'RONEUR=R',
                 'RUBEUR=R', 'CHFEUR=R']
fx_rates_folder = 'fx_data'
fx_rates_ts_fields = ['HIGH', 'LOW', 'OPEN', 'CLOSE']
fx_rates_ts_interval = 'daily'
fx_rates_date_fields = ["TR.BIDPRICE", "TR.ASKPRICE", "TR.MIDPRICE"]
fx_rates_date_interval = 'D'


def launch_eikon() -> None:
    logger = getLogger()
    eikon_user = get_env('EIKON_USER')
    eikon_pass = get_env('EIKON_PASS')
    # Try to start application (only one instance can be launched so no checks required)
    logger.info("Запускаю TR Eikon!")
    Application(backend="uia").start(_EIKON_PATH)
    logger.info("Жду появления формы авторизации...")
    sleep(login_timer)
    try:
        logger.info(_DONE_MSG)
        # Try to connect to login form.
        Application(backend="uia").connect(title=_SPLASH_SCREEN_TITLE)
        # Select 'User ID' input box and enter user ID
        # TODO: find more stable way
        send_keys(eikon_user + '{TAB}' + eikon_pass + '{TAB}{TAB}~')
        logger.info("Жду исчезновения формы авторизации...")
        sleep(login_timer)
        try:
            logger.info(_DONE_MSG)
            # Try to connect to login form. It should not be found due to authorization attempt
            Application(backend="uia").connect(title=_SPLASH_SCREEN_TITLE)
            # If it is still running, someone else is already being logged in. Force to login here
            logger.info("Появился запрос подтверждения принудительного входы в систему. Продолжаю вход.")
            # Select 'Sign In' option and press 'Enter'
            send_keys('{TAB}~')
            logger.info("Жду исчезновения формы...")
            sleep(login_timer)
            logger.info(_DONE_MSG)
            # Try to connect to splash screen last time. It should not be found due to send keys
            Application(backend="uia").connect(title=_SPLASH_SCREEN_TITLE)
            # If it is found again something is really wrong
            err_msg = "Не удаётся войти в TR Eikon, требуется анализ!"
            logger.error(err_msg)
            send_email(None, _RETRIEVE_EIKON_DATA, [err_msg])
            exit(1)
        except ElementNotFoundError:
            logger.info("Вход был успешно завершён!")
    except ElementNotFoundError:
        logger.warning("Форма авторизации не появилась, возможно, вход уже был выполнен!")

    logger.info("Ожидаю загрузки Eikon Desktop...")
    sleep(app_timer)
    logger.info(_DONE_MSG)


def connect_eikon() -> None:
    logger = getLogger()
    eikon_key = get_env('EIKON_KEY')

    logger.info(f"Подключаюсь к Eikon Proxy...")
    try:
        # Try to connect
        ek.set_app_key(eikon_key)
    except ek.eikonError.EikonError:
        # If cannot connect wait a bit and try again
        logger.warning(f"Не могу обнаружить API Proxy. Повторная проверка через {app_timer} секунд")
        sleep(app_timer)
        try:
            # Try to connect again
            ek.set_app_key(eikon_key)
        except ek.eikonError.EikonError:
            # If cannot connect again notify administrator and exit
            err_msg = "Вновь не могу обнаружить API Proxy. Что-то пошло не так, требуется анализ..."
            logger.error(err_msg)
            send_email(None, _RETRIEVE_EIKON_DATA, [err_msg])
            exit(-1)
    logger.info(_DONE_MSG)


def close_eikon():
    logger = getLogger()
    logger.warning("Выключаю Eikon Desktop... Пока не имплементировано!")
    # FIXME: dirty approach, but works as for now
    system("taskkill /f /im  Eikon.exe")


def fx_rates_timeseries(ric: str, start_date: str, end_date: str) -> pd.DataFrame:
    logger = getLogger()
    logger.debug(f"ek.get_timeseries({[ric]}, {fx_rates_ts_fields}, start_date={start_date}, end_date = {end_date},"
                 f"interval={fx_rates_ts_interval})")
    # Get time series part of required information
    df = ek.get_timeseries([ric], fx_rates_ts_fields, start_date=start_date, end_date=end_date,
                           interval=fx_rates_ts_interval)

    # Convert index to string to append columns further
    df.index = df.index.strftime(_EIKON_DATE_FORMAT)

    # Get data part of required information
    for field in fx_rates_date_fields:
        data_fields = [f"{field}(SDate={start_date},EDate={end_date},Frq={fx_rates_date_interval}).date",
                       f"{field}(SDate={start_date},EDate={end_date},Frq={fx_rates_date_interval})"]
        try:
            logger.debug(f"ek.get_data({[ric]}, {data_fields})")
            date_df, err = ek.get_data([ric], data_fields)
        except Exception as err:
            logger.error(err)
            raise err
        if date_df["Date"].count() > 0:
            date_df['Date'] = date_df['Date'].str[:10]
            date_df.set_index('Date', inplace=True)
            date_df.drop(['Instrument'], axis=1, inplace=True)
            df = pd.merge(df, date_df, left_index=True, right_index=True, how="outer")
        else:
            logger.warning(f"Для {ric} нет данных {field}")

    return df


def gas_price_timeseries(ric: str, details: dict, fields: list, start_date: str, end_date: str) -> \
        Optional[pd.DataFrame]:
    logger = getLogger()
    interval = gas_interval
    logger.debug(f"ek.get_data({ric}, ['CF_CURR', 'LOTSZUNITS'])[0]")
    _curr_lot = ek.get_data(ric, ['CF_CURR', 'LOTSZUNITS'])[0]
    if _curr_lot is None:
        raise Exception(f"Метаданные для {ric} не были получены!")
    # FIXME: Add multiplier 0.01 for GBp and EUc
    _curn = _curr_lot.loc[0, 'CF_CURR']

    if 'def_unit' in details:
        # Set predefined unit if required: Done for ZEEDA, value in LOTSZUNITS seems to be invalid
        _unit = details['def_unit']
    else:
        _unit = _curr_lot.loc[0, 'LOTSZUNITS']
        # Validate unit and set correct if required
        if ('numpy' in str(type(_unit))) and pd.np.isnan(_unit):
            _unit = _DEF_UNIT
        elif pd.isna(_unit):
            _unit = _DEF_UNIT
        else:
            _unit = _unit.strip()

    local_fields = fields.copy()
    # If no VOLUME expected remove redundant key from request to avoid empty response (Eikon's behaviour)
    if details['vol'] == _GAS_NO_VOL:
        local_fields.remove(_GAS_VOL_NAME)
    # Get required data
    try:
        df = ek.get_timeseries([ric], fields=local_fields, start_date=start_date, end_date=end_date, interval=interval)
    except ek.eikonError.EikonError as err:
        if err.message == f'{ric}: No data available for the requested date range | ':
            return None
        else:
            raise err
    except Exception as ex:
        logger.error(ex.__cause__)
        raise ex

    if df is None:
        raise Exception(f"Timeseries for {ric} was not received!")
    df.dropna(inplace=True)
    # If no VOLUME expected put empty column instead
    if details['vol'] == _GAS_NO_VOL:
        df[_GAS_VOL_NAME] = pd.NA
    df['orig_cur'] = _curn
    df['orig_unit'] = _unit
    df.rename({'HIGH': 'orig_high', 'LOW': 'orig_low', 'OPEN': 'orig_open', 'CLOSE': 'orig_close',
               _GAS_VOL_NAME: _GAS_VOL_NAME.lower()}, axis=1, inplace=True)

    return df


def get_fx_rates(start_date: str, end_date: str, date_range: str, retry: int, retry_delay: int, ric_delay: int) -> None:
    logger = getLogger()
    logger.info("Выгружаю курсы валют...")
    error_list = []
    rates = {}
    for ric in fx_rates_rics:
        sleep(ric_delay)
        for _ in range(retry):
            logger.info(f"Получение данных для {ric}, попытка #{_} из {retry}.")
            try:
                quotes = fx_rates_timeseries(ric, start_date, end_date)
                if quotes is not None:
                    # Add data to the dictionary
                    rates[ric] = quotes
                else:
                    error_list.append(f"Не удалось получить данные для курса {ric}!")
                break
            except ek.eikonError.EikonError as err:
                if err.message == f'{ric}: No data available for the requested date range | ' or \
                        err.message == 'Error code 400 | Backend error. 400 Bad Request':
                    break

                if err.message == 'UDF Core request failed. Gateway Time-out':
                    # Wait and try again
                    sleep(retry_delay)
                else:
                    err_msg = f"Что-то пошло не так при загрузке курса {ric}!"
                    logger.error(err_msg)
                    error_list.append(err_msg)
                    break
            except Exception as err:
                err_msg = "Не удалось получить данные по курсу {ric}! " + err.__str__()
                logger.error(err_msg)
                error_list.append(err_msg)
                break

    if rates:
        # Merging data into one DataFrame
        quotes_to_save = pd.concat(rates)
        quotes_to_save.index.names = ['ric', 'Date']
        quotes_to_save = quotes_to_save.swaplevel(0, 1)

        # Saving merged data to disk
        file_path = f'{fx_rates_folder}/fx_rates_{date_range}_{datetime.now().strftime(_SAVE_TIMESTAMP_FORMATTER)}.csv'
        quotes_to_save.to_csv(file_path)
        logger.info(f"Все курсы валют за период {date_range} были сохранёны в '{file_path}'.")

    else:
        file_path = None
        logger.info(f"Не найдено искомых курсов валют за период {date_range}.")

    logger.info(f"Выгрузка курсов валют завершена!")

    # Mail file and errors if there are any to target e-mail
    send_email(file_path, MailSubjects.get_fx_rates(date_range), error_list)


def get_gas_prices(start_date: str, end_date: str, date_range: str, retry: int, retry_delay: int,
                   ric_delay: int) -> None:
    logger = getLogger()
    logger.info("Выгружаю цены на газ...")
    error_list = []
    prices = {}
    # Iterate over all rics and save data into CSV files
    for ric, details in gas_rics.items():
        sleep(ric_delay)
        for _ in range(retry):
            logger.info(f"Получение данных для {ric}, попытка #{_} из {retry}.")
            try:
                quotes = gas_price_timeseries(ric, details, gas_fields, start_date, end_date)
                if quotes is not None:
                    # Add data to the dictionary
                    prices[ric] = quotes
                else:
                    error_list.append(f"Не удалось получить данные для цены {ric}!")
                break
            except ek.eikonError.EikonError as err:
                if err.message == f'{ric}: No data available for the requested date range | ' or \
                        err.message == 'Error code 400 | Backend error. 400 Bad Request':
                    break
                if err.message == 'UDF Core request failed. Gateway Time-out':
                    # Wait and try again
                    sleep(retry_delay)
                else:
                    err_msg = f"Что-то пошло не так при загрузке цены {ric}!"
                    logger.error(err_msg)
                    error_list.append(err_msg)
                    break
            except Exception as err:
                err_msg = "Не удалось получить данные по цене {ric}! " + err.__str__()
                logger.error(err_msg)
                error_list.append(err_msg)
                break

    if prices:
        # Merging data into one DataFrame
        quotes_to_save = pd.concat(prices)
        quotes_to_save.index.names = ['ric', 'Date']
        quotes_to_save = quotes_to_save.swaplevel(0, 1)

        # Saving merged data to disk
        file_path = f'{gas_prices_folder}/prices_{date_range}_{datetime.now().strftime(_SAVE_TIMESTAMP_FORMATTER)}.csv'
        quotes_to_save.to_csv(file_path)
        logger.info(f"Все цены за период {date_range} были сохранёны в '{file_path}'.")

    else:
        file_path = None
        logger.info(f"Не найдено искомых цен за период {date_range}.")

    logger.info(f"Выгрузка цен завершена!")

    # Mail file and errors if there are any to target e-mail
    send_email(file_path, MailSubjects.get_gas_prices(date_range), error_list)


def retrieve_data(start_date: datetime, end_date: datetime, retry: int, retry_delay: int, ric_delay: int) -> None:
    logger = getLogger()
    connect_eikon()
    logger.info("test")

    start_date = start_date.strftime(_EIKON_DATE_FORMAT)
    end_date = end_date.strftime(_EIKON_DATE_FORMAT)

    if start_date == end_date:
        date_range = start_date
    else:
        date_range = start_date + ' - ' + end_date

    get_fx_rates(start_date, end_date, date_range, retry, retry_delay, ric_delay)

    get_gas_prices(start_date, end_date, date_range, retry, retry_delay, ric_delay)
