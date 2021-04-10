from datetime import date, timedelta, datetime
from logging import ERROR, WARNING, INFO, DEBUG, getLogger

import click

from lib.email import send_email, MailSubjects
from lib.logs import log_init
from lib.eikon import launch_eikon, retrieve_data, close_eikon


@click.command(help="Выгрузка данных из Refinitiv Eikon и направление на целевой адрес эл. почты")
@click.option('--level', '-l', type=click.Choice(["debug", "info", "warning", "error"]), default="info",
              help='Уровень записи логов')
@click.option('--log-path', '-p', 'log_path', type=click.Path(exists=True), default='./logs',
              help="Путь публикации файлов журналирования")
@click.option('--get/--no-get', default=False, help='Только получение данных, без запуска и выключения терминала')
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
@click.option('--ric-delay', '-ricd',
              help="Ожидание между запросами разных инструментов, в секундах. По умолчанию 2 с.",
              type=click.INT, required=False, default=2)
def eikon_loader(level: str, log_path: str, get: bool, debug: bool, backoff: int, date_start: datetime,
                 date_end: datetime, retry: int, retry_delay: int, ric_delay: int) -> None:
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
        logger.info(f"Дата начала загрузки                  - {date_start}")
        logger.info(f"Дата окончания загрузки               - {date_end}")
        logger.info(f"Количество повторов                   - {retry}")
        logger.info(f"Ожидание между повторными запросами   - {retry_delay} секунд")
        logger.info(f"Ожидание между разными запросами      - {ric_delay} секунд")

    if not get:
        # Start Refinitiv Eikon and log in
        try:
            launch_eikon()
        except Exception as err:
            msg = f'Неожиданная ошибка при запуске терминала.\nОшибка: {err}'
            logger.error(msg)
            send_email(None, MailSubjects.get_unk_err_start_eikon(), [msg])
            exit(-1)

    # Get required data
    try:
        retrieve_data(date_start, date_end, retry, retry_delay, ric_delay)
    except Exception as err:
        msg = f'Неожиданная ошибка при выгрузке и отправке данных.\n' \
            f'Дата начала: {date_start:%d.%m.%Y} Дата окончания: {date_end:%d.%m.%Y}.\n' \
            f'Ошибка: {err}'
        logger.error(msg)
        send_email(None, MailSubjects.get_unk_err_load_data(), [msg])
        exit(-1)

    if not get:
        # Log off and shutdown Refinitiv Eikon
        close_eikon()


if __name__ == '__main__':
    eikon_loader()
