import logging
import time

GLOBAL_LOGGER = None


def setup_logger(level: int = logging.INFO) -> None:
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
