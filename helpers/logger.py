import logging
import os
import sys


def get_logger(name='app'):
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))
    logger.addHandler(ch)

    return logger
