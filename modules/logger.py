# -*- coding: utf-8 -*-
"""
Централизованное логирование.
Env:
  LOG_LEVEL=DEBUG|INFO|WARNING|ERROR (по умолчанию INFO)
  TELEGRAM_DEBUG=1 — подробные логи telegram/telegram.ext
"""

import logging
import os

_LOGGER_NAME = "helptp"
_LOG_FILE = "helptp.log"
_configured = False


def _level_from_env(default=logging.INFO):
    m = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }
    return m.get(os.getenv("LOG_LEVEL", "").upper(), default)


def setup_logging(name: str = _LOGGER_NAME, log_file: str = _LOG_FILE) -> logging.Logger:
    global _configured
    lvl = _level_from_env()

    logger = logging.getLogger(name)
    logger.setLevel(lvl)
    logger.propagate = False

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

        sh = logging.StreamHandler()
        sh.setLevel(lvl)
        sh.setFormatter(fmt)
        logger.addHandler(sh)

        try:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setLevel(lvl)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception:
            logger.warning("Не удалось открыть лог-файл %s — логирую только в stdout", log_file)

    # детальные логи PTB
    if os.getenv("TELEGRAM_DEBUG") == "1":
        for n in ("telegram", "telegram.ext"):
            l = logging.getLogger(n)
            l.setLevel(logging.DEBUG)
            if not l.handlers:
                l.addHandler(logging.StreamHandler())

    _configured = True
    return logger


def get_logger() -> logging.Logger:
    global _configured
    if not _configured:
        return setup_logging()
    return logging.getLogger(_LOGGER_NAME)


def clear_logs(log_file: str = _LOG_FILE) -> None:
    try:
        open(log_file, "w", encoding="utf-8").close()
    except Exception:
        pass
