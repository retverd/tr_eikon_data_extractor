from logging import getLogger
from os import system
from time import sleep

import eikon as ek
from pywinauto import Application
from pywinauto.findwindows import ElementNotFoundError
from pywinauto.keyboard import send_keys

from lib.email import send_email
from lib.env import get_env


class EikonDesktop(object):
    """
    Responsible for operations with Refinitiv Eikon Desktop: start, stop, connect to API.
    Doesn't contain operations for data retrieval.
    """

    _EIKON_PATH = get_env('EIKON_PATH')

    _SPLASH_SCREEN_TITLE = get_env('EIKON_SPLASH_SCREEN_TITLE')
    _TOOLBAR_TITLE = 'Eikon Toolbar'
    _RETRIEVE_EIKON_DATA = 'Загрузка данных из Refinitiv Eikon'
    _DONE_MSG = 'Готово!'
    login_timer = 25
    app_timer = 50

    @classmethod
    def launch(cls) -> None:
        logger = getLogger()
        eikon_user = get_env('EIKON_USER')
        eikon_pass = get_env('EIKON_PASS')
        # Try to start application (only one instance can be launched so no checks required)
        logger.info("Запускаю Refinitiv Eikon!")
        Application(backend="uia").start(cls._EIKON_PATH)
        logger.info("Жду появления формы авторизации...")
        sleep(cls.login_timer)
        try:
            logger.info(cls._DONE_MSG)
            # Try to connect to login form.
            Application(backend="uia").connect(title=cls._SPLASH_SCREEN_TITLE)
            # Select 'User ID' input box and enter user ID
            # TODO: find more stable way
            send_keys(eikon_user + '{TAB}' + eikon_pass + '{TAB}{TAB}~')
            logger.info("Жду исчезновения формы авторизации...")
            sleep(cls.login_timer)
            try:
                logger.info(cls._DONE_MSG)
                # Try to connect to login form. It should not be found due to authorization attempt
                Application(backend="uia").connect(title=cls._SPLASH_SCREEN_TITLE)
                # If it is still running, someone else is already being logged in. Force to login here
                logger.info("Появился запрос подтверждения принудительного входы в систему. Продолжаю вход.")
                # Select 'Sign In' option and press 'Enter'
                send_keys('{TAB}~')
                logger.info("Жду исчезновения формы...")
                sleep(cls.login_timer)
                logger.info(cls._DONE_MSG)
                # Try to connect to splash screen last time. It should not be found due to send keys
                Application(backend="uia").connect(title=cls._SPLASH_SCREEN_TITLE)
                # If it is found again something is really wrong
                err_msg = "Не удаётся войти в Refinitiv Eikon, требуется анализ!"
                logger.error(err_msg)
                send_email(None, cls._RETRIEVE_EIKON_DATA, [err_msg])
                exit(-1)
            except ElementNotFoundError:
                logger.info("Вход был успешно завершён!")
        except ElementNotFoundError:
            logger.warning("Форма авторизации не появилась, возможно, вход уже был выполнен!")

        logger.info("Ожидаю загрузки Refinitiv Eikon Desktop...")
        sleep(cls.app_timer)
        logger.info(cls._DONE_MSG)

    @classmethod
    def connect(cls) -> None:
        logger = getLogger()
        eikon_key = get_env('EIKON_KEY')

        logger.info(f"Подключаюсь к Refinitiv Eikon Proxy...")
        try:
            # Try to connect
            ek.set_app_key(eikon_key)
        except ek.eikonError.EikonError:
            # If cannot connect wait a bit and try again
            logger.warning(f"Не могу обнаружить API Proxy. Повторная проверка через {cls.app_timer} секунд")
            sleep(cls.app_timer)
            try:
                # Try to connect again
                ek.set_app_key(eikon_key)
            except ek.eikonError.EikonError:
                # If cannot connect again notify administrator and exit
                err_msg = "Вновь не могу обнаружить API Proxy. Что-то пошло не так, требуется анализ..."
                logger.error(err_msg)
                send_email(None, cls._RETRIEVE_EIKON_DATA, [err_msg])
                exit(-1)
        logger.info(cls._DONE_MSG)

    @classmethod
    def close(cls):
        logger = getLogger()
        logger.warning("Выключаю Refinitiv Eikon Desktop... Пока не имплементировано!")
        # FIXME: dirty approach, but works as for now
        system("taskkill /f /im  Eikon.exe")
