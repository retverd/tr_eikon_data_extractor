from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging import getLogger
from os import path, remove
from smtplib import SMTP_SSL
from ssl import create_default_context
from string import Template
from typing import Optional

from lib.env import get_env


class MailSubjects:
    fx_rates = Template("Курсы валют за $date")
    gas_prices = Template("Цены на газ за $date")
    unk_err_start_eikon = "Неожиданная ошибка при запуске терминала"
    unk_err_connect_eikon = "Неожиданная ошибка при подключении к API Proxy"
    unk_err_load_data = "Неожиданная ошибка при выгрузке и отправке данных"

    @staticmethod
    def get_fx_rates(date: str) -> str:
        return MailSubjects.fx_rates.substitute(date=date)

    @staticmethod
    def get_gas_prices(date: str) -> str:
        return MailSubjects.gas_prices.substitute(date=date)

    @staticmethod
    def get_unk_err_start_eikon() -> str:
        return MailSubjects.unk_err_start_eikon

    @staticmethod
    def get_unk_err_connect_eikon() -> str:
        return MailSubjects.unk_err_connect_eikon

    @staticmethod
    def get_unk_err_load_data() -> str:
        return MailSubjects.unk_err_load_data


MESSAGE_TEMPLATE = Template("Приветствую!\n\n$payload\n\nИскренне Ваш, ИИ.")
eol = '\n'


def send_email(attachment: Optional[str], subject: str, error_list: Optional[list] = None) -> None:
    context = create_default_context()
    logger = getLogger()
    with SMTP_SSL(get_env('GASDB_SMTP_SERVER'), int(get_env('GASDB_SMTP_PORT')), context=context) as server:
        server.login(get_env('GASDB_SMTP_LOGIN'), get_env('GASDB_SMTP_PASS'))
        sender = get_env('GASDB_SMTP_SENDER')
        if attachment is not None:
            message = MESSAGE_TEMPLATE.substitute(payload="Данные во вложении.")

            # Compose message
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = sender
            msg['To'] = get_env('GASDB_DATA_RECIPIENT')
            msg.attach(MIMEText(message, "plain"))

            # Add attachment
            basename = path.basename(attachment)
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(attachment, "rb").read())
            encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % basename)
            msg.attach(part)

            # Send e-mail
            server.sendmail(sender, msg['To'].split(","), msg.as_string())
            logger.info(f'Письмо с данными успешно отправлено {msg["To"]}')

            # Clean up
            remove(attachment)
            logger.info("Отправленный файл был успешно удалён с диска!")

        if error_list is not None and error_list.__len__() > 0:
            error_message = MESSAGE_TEMPLATE.substitute(payload=f"При загрузке и отправке данных произошли следующие "
                                                                f"ошибки:\n{eol.join(error_list)}")

            # Compose message
            err_msg = MIMEMultipart()
            err_msg['Subject'] = f'Ошибки при подготовке письма "{subject}"'
            err_msg['From'] = sender
            err_msg['To'] = get_env('GASDB_ERROR_RECIPIENT')
            err_msg.attach(MIMEText(error_message, "plain"))

            # Send e-mail
            server.sendmail(sender, err_msg['To'].split(","), err_msg.as_string())
            logger.info(f'Письмо с сообщениями об ошибках успешно отправлено {err_msg["To"]}')
