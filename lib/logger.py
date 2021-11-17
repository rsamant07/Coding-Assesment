import logging
from logging.handlers import RotatingFileHandler
import datetime
from lib.utils import *

Conf = loadConfig('LOGGER')

logLevelDict = {'logging.DEBUG': logging.DEBUG,
                'logging.INFO': logging.INFO,
                'logging.WARNING': logging.WARNING,
                'logging.ERROR': logging.ERROR}

FORMATTER = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
LOG_FILE = datetime.datetime.now().strftime('logs/ExecutionLog_%H_%M_%S_%d_%m_%Y.log')

def get_logger(logger_name: str):
    """
                This function is initialzing logger as per set Level and RotatingFileHandler

                Returns:
                        Logger : Logging  Object
    """
    logger = logging.getLogger(logger_name)
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=int(Conf['MaxBytes']),
                                       backupCount=int(Conf['backupCount']))
    file_handler.setFormatter(FORMATTER)
    logger.setLevel(logLevelDict[Conf['loggingLevel']])
    logger.addHandler(file_handler)
    return logger
