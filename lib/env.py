import logging
import os
import time


def get_env(name: str, message: str = None):
    if name in os.environ:
        return os.environ[name]
    if message is not None:
        while True:
            value = input(message)
            try:
                return str(value)
            except ValueError as e:
                logging.error(e)
                time.sleep(1)
    else:
        raise OSError(f'Environment variable {name} is not there')


if __name__ == "__main__":
    print(get_env('port', 'Enter your port: '))
