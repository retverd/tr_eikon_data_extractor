from datetime import datetime
from logging import getLogger
from time import sleep

import eikon as ek
import pandas as pd

from gas_prices import gas_rics
from lib.email import send_email, MailSubjects


class EikonDataGetter(object):
    """
    Abstract class responsible for data retrieval from Refinitiv Eikon Desktop.
    """

    EIKON_DATE_FORMAT = '%Y-%m-%d'

    data_name = {}
    rics = []
    folder_to_save = ''
    file_prefix = ''
    save_timestamp_formatter = '%Y-%m-%dT%H-%M-%S'
    mail_header_getter = None

    ts_interval = 'daily'
    ts_fields = ['HIGH', 'LOW', 'OPEN', 'CLOSE']

    @classmethod
    def get_data(cls, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError("Please use available subclasses!")

    @classmethod
    def retrieve_data(cls, start_date: str, end_date: str, date_range: str, retry: int, retry_delay: int) -> None:
        logger = getLogger()
        quotes = None
        for _ in range(1, retry, 1):
            logger.info(f"Выгружаю {cls.data_name['nom_acc']}, попытка #{_} из {retry}.")
            try:
                quotes = cls.get_data(start_date, end_date)
                break
            except ek.eikonError.EikonError as err:
                if err.message == 'UDF Core request failed. Gateway Time-out' or \
                        err.message == 'Error code 400 | Backend error. 400 Bad Request':
                    # Wait and try again
                    sleep(retry_delay)
                else:
                    err_msg = f"Что-то пошло не так при загрузке искомых {cls.data_name['gen']}! Детали: "
                    err_list = err.message.split('|')
                    for single_err in err_list:
                        if single_err != '':
                            err_msg = err_msg + "\n\r" + single_err.strip()
                    logger.error(err_msg)
                    send_email(None, cls.mail_header_getter(date_range), [err_msg])
                    return
            except Exception as err:
                err_msg = f"Не удалось получить данные по искомым {cls.data_name['dat']}. Детали: " + err.__str__()
                logger.error(err_msg)
                send_email(None, cls.mail_header_getter(date_range), [err_msg])
                return

        if quotes is None:
            send_email(None, cls.mail_header_getter(date_range),
                       [f"Отсутствуют данные по искомым {cls.data_name['dat']}."])
            return

        logger.info(f"Выгрузка {cls.data_name['gen']} успешно завершена!")
        error_list = []

        # Check for missing data
        for ric in cls.rics:
            if ric not in quotes.ric.values:
                msg = f"Не удалось получить данные для {ric}!"
                error_list.append(msg)

        # Saving merged data to disk
        file_path = f'{cls.folder_to_save}/{cls.file_prefix}_{date_range}_' \
            f'{datetime.now().strftime(cls.save_timestamp_formatter)}.csv'
        quotes.to_csv(file_path, index=False)
        logger.info(f"{cls.data_name['nom_acc'].capitalize()} за период {date_range} были сохранёны в '{file_path}'.")

        # Mail file and errors if there are any to target e-mail
        send_email(file_path, cls.mail_header_getter(date_range), error_list)


class FXRateGetter(EikonDataGetter):
    """
    Implementation of abstract class for FX Rates.
    """

    data_name = {'nom_acc': 'курсы валют', 'gen': 'курсов валют', 'dat': 'курсам валют'}
    folder_to_save = 'fx_data'
    file_prefix = 'fx_rates'
    mail_header_getter = MailSubjects.get_fx_rates
    rics = ['USDEUR=R', 'PLNEUR=R', 'GBPEUR=R', 'CZKEUR=R', 'HRKEUR=R', 'HUFEUR=R', 'BGNEUR=R', 'RONEUR=R', 'RUBEUR=R',
            'CHFEUR=R', 'TRY=']
    data_fields = ["TR.BIDPRICE", "TR.ASKPRICE", "TR.MIDPRICE", "TR.MIDPRICE.Date"]
    data_interval = 'D'

    @classmethod
    def get_data(cls, start_date: str, end_date: str) -> pd.DataFrame:
        # Get time series part of required information
        ts_df = ek.get_timeseries(cls.rics, cls.ts_fields, start_date=start_date, end_date=end_date,
                                  interval=cls.ts_interval, normalize=True)

        ts_df['Date'] = ts_df['Date'].dt.strftime(cls.EIKON_DATE_FORMAT)
        ts_df.rename({'Security': 'ric'}, axis=1, inplace=True)
        ts_df.set_index(['Date', 'ric', 'Field'], inplace=True)
        result_df = ts_df.unstack()
        result_df.columns = result_df.columns.get_level_values(1)

        # Get data part of required information
        data_df, err = ek.get_data(cls.rics, cls.data_fields, {'SDate': start_date, 'EDate': end_date,
                                                               'FRQ': cls.data_interval})

        if data_df["Date"].count() > 0:
            data_df.dropna(inplace=True)
            data_df['Date'] = data_df['Date'].str[:10]
            data_df.rename({'Instrument': 'ric'}, axis=1, inplace=True)
            data_df.set_index(['Date', 'ric'], inplace=True)
            result_df = pd.concat([result_df, data_df], axis=1)

        result_df.reset_index(inplace=True)
        return result_df


class GasPricesGetter(EikonDataGetter):
    """
    Implementation of abstract class for Gas Prices.
    """

    def_gas_unit = 'MWh'
    def_gas_cur = 'EUR'
    data_name = {'nom_acc': 'цены на газ', 'gen': 'цен на газ', 'dat': 'ценам на газ'}
    folder_to_save = 'gas_data'
    file_prefix = 'prices'
    mail_header_getter = MailSubjects.get_gas_prices
    rics = gas_rics

    @classmethod
    def get_data(cls, start_date: str, end_date: str) -> pd.DataFrame:
        # Get units for all RICs
        lots_df = ek.get_data(list(cls.rics.keys()), ['CF_CURR', 'LOTSZUNITS'])[0]
        lots_df.rename({'LOTSZUNITS': 'orig_unit', 'CF_CURR': 'orig_cur', 'Instrument': 'ric'}, axis=1, inplace=True)
        lots_df.orig_unit = lots_df.orig_unit.str.strip()
        lots_df.rename({'Instrument': 'ric'}, axis=1, inplace=True)
        # Replace N/A values to default values
        lots_df['orig_cur'].fillna(cls.def_gas_cur, inplace=True)
        lots_df['orig_unit'].fillna(cls.def_gas_unit, inplace=True)
        # For all RICs with explicitly defined default units update values
        for ric in gas_rics:
            if isinstance(gas_rics[ric], dict) and 'def_unit' in gas_rics[ric]:
                lots_df.loc[lots_df.ric == ric, 'orig_unit'] = gas_rics[ric]['def_unit']

        ts_df = ek.get_timeseries(list(cls.rics.keys()), cls.ts_fields, start_date=start_date, end_date=end_date,
                                  interval=cls.ts_interval, normalize=True)

        # Somehow sometimes redundant columns appear and should be deleted
        for column in cls.ts_fields:
            if column in ts_df.columns:
                del ts_df[column]

        ts_df['Date'] = ts_df['Date'].dt.strftime(cls.EIKON_DATE_FORMAT)
        ts_df.rename({'Security': 'ric'}, axis=1, inplace=True)
        ts_df.set_index(['Date', 'ric', 'Field'], inplace=True)
        result_df = ts_df.unstack()
        result_df.columns = result_df.columns.get_level_values(1)
        result_df.reset_index(inplace=True)
        # Add units and currencies
        result_df = result_df.merge(lots_df)
        # Set required names
        result_df.rename({'HIGH': 'orig_high', 'LOW': 'orig_low', 'OPEN': 'orig_open', 'CLOSE': 'orig_close'},
                         axis=1, inplace=True)
        return result_df
