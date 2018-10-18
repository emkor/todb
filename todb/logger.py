import logging
import time
from os import path
from typing import Optional

GLOBAL_LOGGER = None
DEFAULT_LOG_FILE = "todb.log"


def setup_logger(level: int = logging.INFO, log_file: Optional[str] = None) -> None:
    if log_file is not None:
        log_directory = path.dirname(path.abspath(log_file))
        if path.exists(log_directory):
            logging.basicConfig(format='%(levelname)s | %(asctime)s UTC | %(message)s', level=level, filename=log_file)
        else:
            print("Can not create log file; directory {} does not exists; will use todb.log!".format(log_directory))
            logging.basicConfig(format='%(levelname)s | %(asctime)s UTC | %(message)s', level=level,
                                filename=DEFAULT_LOG_FILE)
    else:
        logging.basicConfig(format='%(levelname)s | %(asctime)s UTC | %(message)s', level=level)
    logging.Formatter.converter = time.gmtime
    sqlalchemy_logger = logging.getLogger('sqlalchemy')
    if level == logging.DEBUG:
        sqlalchemy_logger.setLevel(logging.WARNING)
    else:
        sqlalchemy_logger.setLevel(logging.ERROR)


def get_logger(calling_module: str = "todb") -> logging.Logger:
    global GLOBAL_LOGGER
    if GLOBAL_LOGGER is None:
        GLOBAL_LOGGER = logging.getLogger(calling_module)
    return GLOBAL_LOGGER
