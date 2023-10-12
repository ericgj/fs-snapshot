import logging
import logging.handlers
from typing import Optional

LOG_NAME = "fs-snapshot"


def init_logger(
    *,
    name: str = LOG_NAME,
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    format: str = "[%(levelname).1s|%(asctime)s|%(threadName)s|%(module)s] %(message)s",
    propagate: bool = True,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = propagate
    h: logging.Handler
    if logger.hasHandlers():
        for h in logger.handlers:
            h.flush()
            h.close()
            logger.removeHandler(h)

    if log_file is None:
        h = logging.StreamHandler()
    else:
        h = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=(2**22),
            backupCount=9,
            encoding="UTF-8",
        )
    f = logging.Formatter(format)
    h.setFormatter(f)
    logger.addHandler(h)
    return logger


def init_db_logger(
    *,
    name: str,
    log_file: str,
    level: int = logging.INFO,
) -> logging.Logger:
    return init_logger(
        name=name,
        level=level,
        log_file=log_file,
        format="[%(levelname).1s|%(asctime)s]\n%(message)s",
        propagate=False,
    )


def get_logger(name: str, parent: str = LOG_NAME) -> logging.Logger:
    return logging.getLogger(f"{parent}.{name}")


def get_db_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
