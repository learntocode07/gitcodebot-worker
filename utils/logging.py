import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging(name: str = None, log_file: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Sets up a logger with console and optional file logging.

    Args:
        name (str): Logger name. Use None for root logger.
        log_file (str): Path to a log file. If None, logs won't be written to file.
        level (int): Logging level. e.g., logging.DEBUG, logging.INFO, etc.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

    logger.setLevel(level)

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger