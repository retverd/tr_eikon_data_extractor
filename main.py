from calendar import monthrange
from datetime import date, timedelta, datetime
from logging import ERROR, WARNING, INFO, DEBUG, getLogger
from time import sleep

import click

from lib.eikon_data_getter import FXRateGetter, GasPricesGetter
from lib.eikon_desktop_handler import EikonDesktop
from lib.email import send_email, MailSubjects
from lib.logs import log_init


def get_and_send_data(date_start: datetime, date_end: datetime, fx: bool, gas: bool, type_delay: int, retry: int,
                      retry_delay: int) -> None:
    logger = getLogger()
    # Prepare dates
    start_date = date_start.strftime(FXRateGetter.EIKON_DATE_FORMAT)
    end_date = date_end.strftime(FXRateGetter.EIKON_DATE_FORMAT)

    if start_date == end_date:
        date_range = start_date
    else:
        date_range = start_date + ' - ' + end_date

    try:
        if fx:
            # Get required data for fx rates
            FXRateGetter.retrieve_data(start_date, end_date, date_range, retry, retry_delay)
        if fx and gas:
            # Wait if both are required
            sleep(type_delay)
        if gas:
            # Get required data for gas prices
            GasPricesGetter.retrieve_data(start_date, end_date, date_range, retry, retry_delay)
    except Exception as err:
        msg = f'Неожиданная ошибка при выгрузке и отправке данных.\n' \
              f'Дата начала: {date_start:%d.%m.%Y} Дата окончания: {date_end:%d.%m.%Y}.\n' \
              f'Ошибка: {err}'
        logger.error(msg)
        send_email(None, MailSubjects.get_unk_err_load_data(), [msg])
        exit(-1)


@click.command(help="Выгрузка данных из Refinitiv Eikon и направление на целевой адрес эл. почты")
@click.option('--level', '-l', type=click.Choice(["debug", "info", "warning", "error"]), default="info",
              help='Уровень записи логов')
@click.option('--log-path', '-p', 'log_path', type=click.Path(exists=True), default='./logs',
              help="Путь публикации файлов журналирования")
@click.option('--get/--no-get', default=False, help='Только получение данных, без запуска и выключения терминала')
@click.option('--fx/--no-fx', default=True, help='Получение курсов валют, по умолчанию включено')
@click.option('--gas/--no-gas', default=True, help='Получение цен на газ, по умолчанию включено')
@click.option('--debug/--no-debug', default=False, help='Работа в режиме отладки (в т.ч. вывод в консоль)')
@click.option('--backoff', '-b', help="Отступ от текущей даты для определения даты загрузки. "
                                      "Значение 1 означает 'вчера'.\n"
                                      "Даты начала и окончания загрузки будут проигнорированы.",
              type=click.INT, required=False, default=None)
@click.option('--date-start', '-ds', help="Дата начала загрузки.",
              type=click.DateTime(formats={'%d.%m.%Y'}), required=False, default=None)
@click.option('--date-end', '-de', help="Дата окончания загрузки, если не указана, "
                                        "то обрабатываются записи с датой начала.",
              type=click.DateTime(formats={'%d.%m.%Y'}), required=False, default=None)
@click.option('--retry', '-r', help="Количество повторов при запросе данных, по умолчанию 3",
              type=click.INT, required=False, default=3)
@click.option('--retry-delay', '-rd', help="Ожидание между повторными запросами, в секундах. По умолчанию 15 с.",
              type=click.INT, required=False, default=15)
@click.option('--type-delay', '-td',
              help="Ожидание между запросами разных типов инструментов, в секундах. По умолчанию 2 с.",
              type=click.INT, required=False, default=2)
def eikon_loader(level: str, log_path: str, get: bool, fx: bool, gas: bool, debug: bool, backoff: int,
                 date_start: datetime, date_end: datetime, retry: int, retry_delay: int, type_delay: int) -> None:
    # Setting log level
    log_level = INFO

    if level.lower() == "error":
        log_level = ERROR
    elif level.lower() == "warning":
        log_level = WARNING
    elif level.lower() == "info":
        log_level = INFO
    elif level.lower() == "debug":
        log_level = DEBUG
    else:
        click.BadArgumentUsage(f"Неверный уровень логгинга: {level}")

    # Define local logger to separate output to files on commands level
    log_init('GasDB', 'eikon_loader', debug, log_level, log_path)
    logger = getLogger()

    # Define dates range
    if backoff is not None:
        # ignore all other params
        date_start = date.today() - timedelta(days=backoff)
        date_end = date_start
    elif date_start is None:
        logger.error(f"Дата начала загрузки должна быть указана!")
        exit(-1)
    elif date_start is not None and date_end is None:
        date_end = date_start
    elif date_start is not None and date_end is not None:
        if date_end < date_start:
            logger.error(f"Дата начала обработки не может быть больше даты окончания.")
            exit(-1)
    else:
        logger.error(f"Неожиданный случай! backoff = {backoff}, date-start = {date_start}, date-end = {date_end}.")
        exit(-1)

    if debug:
        logger.info(f"Путь публикации файлов журналирования - {log_path}")
        logger.info(f"Запуск и выключение терминала         - {not get}")
        logger.info(f"Получение курсов валют                - {fx}")
        logger.info(f"Получение цен на газ                  - {gas}")
        logger.info(f"Дата начала загрузки                  - {date_start}")
        logger.info(f"Дата окончания загрузки               - {date_end}")
        logger.info(f"Количество повторов                   - {retry}")
        logger.info(f"Ожидание между повторными запросами   - {retry_delay} секунд")
        logger.info(f"Ожидание между разными запросами      - {type_delay} секунд")

    if not get:
        # Start Refinitiv Eikon and log in
        try:
            EikonDesktop.launch()
        except Exception as err:
            msg = f'Неожиданная ошибка при запуске терминала.\nОшибка: {err}'
            logger.error(msg)
            send_email(None, MailSubjects.get_unk_err_start_eikon(), [msg])
            exit(-1)

    try:
        # Connect to Refinitiv Eikon API Proxy
        EikonDesktop.connect()
    except Exception as err:
        msg = f'Неожиданная ошибка при подключении к API Proxy.\nОшибка: {err}'
        logger.error(msg)
        send_email(None, MailSubjects.get_unk_err_connect_eikon(), [msg])
        exit(-1)

    if (date_end - date_start).days > 31:
        date_end_new = date_start.replace(day=monthrange(date_start.year, date_start.month)[1])
        while date_start <= date_end:
            get_and_send_data(date_start, date_end_new, fx, gas, type_delay, retry, retry_delay)
            date_start = date_end_new + timedelta(days=1)
            date_end_new = min(date_start.replace(day=monthrange(date_start.year, date_start.month)[1]), date_end)
    else:
        get_and_send_data(date_start, date_end, fx, gas, type_delay, retry, retry_delay)

    if not get:
        # Log off and shutdown Refinitiv Eikon
        EikonDesktop.close()


if __name__ == '__main__':
    eikon_loader()
