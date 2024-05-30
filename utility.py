import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import os

def get_logger():
    return logging.getLogger()


def set_stdout_logger():
    # configure logger and assign location to save log message
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) # logger message level
    # logger message formate
    formatter = logging.Formatter("%(levelname)s [%(asctime)s.%(msecs)03d at %(filename)s:%(levelno)s]: %(message)s", datefmt='%Y.%m.%d %H:%M:%S')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)

    return logger


def set_logger(app_name):
    # configure logger and assign location to save log message
    logger = logging.getLogger()
    logger.handlers.clear()  # 使用 streamlit 時需要加上這一行
    logger.setLevel(logging.INFO)  # logger message level
    # logger message formate
    formatter = logging.Formatter(
        "[%(asctime)s %(filename)s:%(levelno)s %(levelname)s]: %(message)s",
        datefmt='%Y%m%d %H:%M:%S')
    os.makedirs("log", exist_ok=True)

    rotation_logging_handler = TimedRotatingFileHandler(
        f'log/{app_name}', when='midnight',
        backupCount=7, encoding='utf-8')
    rotation_logging_handler.setLevel(logging.INFO)
    rotation_logging_handler.setFormatter(formatter)
    rotation_logging_handler.suffix = '%Y-%m-%d'

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(formatter)

    logger.addHandler(rotation_logging_handler)
    logger.addHandler(stdout_handler)

    return logger


