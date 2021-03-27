import logging
from datetime import date, timedelta, datetime

import click

from lib.logs import cli_log_init
from lib.tr_eikon import launch_eikon, retrieve_data, close_eikon


@click.command(help="Выгрузка данных из Thomson Reuters Eikon и направление на целевой адрес эл. почты")
@click.option('--level', '-l', type=click.Choice(["debug", "info", "warning", "error"]), default="info",
              help='Уровень записи логов')
@click.option('--log-path', '-p', 'log_path', type=click.Path(exists=True), default='./logs',
              help="Путь публикации файлов журналирования")
@click.option('--debug/--no-debug', default=False, help='Работа в режиме отладки (в т.ч. вывод в консоль)')
@click.option('--backoff', help="Отступ от текущей даты для определения даты загрузки. "
                                "Значение 1 означает 'вчера'.\n"
                                "Даты начала и окончания загрузки будут проигнорированы.",
              type=click.INT, required=False, default=None)
@click.option('--date-start', help="Дата начала загрузки.",
              type=click.DateTime(formats={'%d.%m.%Y'}), required=False, default=None)
@click.option('--date-end', help="Дата окончания загрузки, если не указана, то обрабатываются записи с датой начала.",
              type=click.DateTime(formats={'%d.%m.%Y'}), required=False, default=None)
def eikon_loader(level: str, log_path: str, debug: bool, backoff: int, date_start: datetime, date_end: datetime):
    # Setting log level
    log_level = None

    if level.lower() == "error":
        log_level = logging.ERROR
    elif level.lower() == "warning":
        log_level = logging.WARNING
    elif level.lower() == "info":
        log_level = logging.INFO
    elif level.lower() == "debug":
        log_level = logging.DEBUG
    else:
        click.BadArgumentUsage(f"Неверный уровень логгинга: {level}")

    # Define local logger to separate output to files on commands level
    cli_log_init('GasDB', 'cli_common', debug, log_level, log_path)

    # Define dates range
    if backoff is not None:
        # ignore all other params
        date_start = date.today() - timedelta(days=backoff)
        date_end = date_start
    elif date_start is None:
        logging.error(f"Дата начала загрузки должна быть указана!")
        exit(-1)
    elif date_start is not None and date_end is None:
        date_end = date_start
    elif date_start is not None and date_end is not None:
        if date_end < date_start:
            logging.error(f"Дата начала обработки не может быть больше даты окончания.")
            exit(-1)
    else:
        logging.error(f"Неожиданный случай! backoff = {backoff}, date-start = {date_start}, date-end = {date_end}.")
        exit(-1)

    # Start TR Eikon and log in
    launch_eikon()

    # Get required data
    try:
        retrieve_data(date_start, date_end)
    except Exception as err:
        logging.error(f'Неизвестная ошибка при выгрузке и отправке данных.\n'
                      f'Дата начала: {date_start:%d.%m.%Y} Дата окончания: {date_end:%d.%m.%Y}.\n'
                      f'Ошибка: {err}')

    # Log off and shutdown TR Eikon
    close_eikon()


if __name__ == '__main__':
    eikon_loader()
