"""Miscellaneous utility functions"""
import base64
import logging
import uuid

from etl.config import settings


def short_uuid() -> str:
    """Creates a short unique ID string"""
    return base64.b64encode(uuid.uuid4().bytes).decode('utf-8').rstrip('=')


def process_file_stub(data: str, **kwargs):
    """A do-nothing-method to use as an example for the Python process config"""
    print(f'Received the data file {data} and the named arguments {kwargs}. Doing nothing.')


def get_logger(name: str) -> logging.Logger:
    log_level = settings.log_level
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)
    return logger
