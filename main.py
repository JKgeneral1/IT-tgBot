# -*- coding: utf-8 -*-
from modules.logger import setup_logging
from modules.utils import check_single_instance, remove_lock_file
from modules.telegram_bot import run

log = setup_logging()

if __name__ == "__main__":
    check_single_instance()
    try:
        run()
        log.info("Application started")
    except Exception as e:
        log.error("Ошибка запуска бота: %s", e, exc_info=True)
    finally:
        remove_lock_file()
