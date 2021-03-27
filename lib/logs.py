import logging
from string import Template

LOGS_FORMAT = '[%(asctime)s - %(levelname)s] %(module)s: %(message)s'


def cli_log_init(_module_name: str, _filename: str, _debug_flag: bool, _log_level: int, log_path: str,
                 _logger_name: str = None):
    start_msg = Template(f'{_module_name} started in $mode mode!')

    if _logger_name is None:
        logger = logging.getLogger()
    else:
        logger = logging.getLogger(_logger_name)

    logger.setLevel(_log_level)

    # Write to file by default
    f_handler = logging.FileHandler(f'{log_path}/{_filename}.log', encoding='utf-8')
    f_handler.setLevel(_log_level)
    f_format = logging.Formatter(LOGS_FORMAT)
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

    # Output to screen in debug mode
    if _debug_flag:
        c_handler = logging.StreamHandler()
        c_handler.setLevel(_log_level)
        c_format = logging.Formatter(LOGS_FORMAT)
        c_handler.setFormatter(c_format)
        logger.addHandler(c_handler)
        logger.info(start_msg.substitute(mode='debug'))
    else:
        logger.info(start_msg.substitute(mode='regular'))
