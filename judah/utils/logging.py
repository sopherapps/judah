"""Module with utility functions for logging"""
import logging
from logging.handlers import RotatingFileHandler


def setup_rotating_file_logger(logger: logging.Logger, file_path: str, max_bytes: int = 2000000,
                               log_format: str = '%(asctime)-15s \n%(name)s - %(levelname)s\n\n %(message)s\n\n'):
    """
    Makes the logger to use a file that rotates when a given size is reached.
    It has a single back up and logs errors and above
    """
    logger_handler = RotatingFileHandler(file_path, maxBytes=max_bytes, backupCount=1)
    logger_formatter = logging.Formatter(log_format)
    logger_handler.setFormatter(logger_formatter)
    logger_handler.setLevel(logging.ERROR)

    logger.addHandler(logger_handler)
    return logger
