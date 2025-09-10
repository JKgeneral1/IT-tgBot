# -*- coding: utf-8 -*-
import configparser
import os
import sys
from typing import Optional
from .logger import setup_logging

logger = setup_logging()

config = configparser.ConfigParser()
config.read("config.ini")

def _get_section_case_insensitive(name: str) -> Optional[configparser.SectionProxy]:
    for s in config.sections():
        if s.lower() == name.lower():
            return config[s]
    return None

def cfg(section: str, key: str, default: Optional[str] = None, cast=str):
    env_key = f"{section}_{key}".upper().replace("-", "_")
    if env_key in os.environ:
        val = os.environ[env_key]
        try:
            return cast(val) if (val is not None and cast is not str) else val
        except Exception:
            return default
    sect = _get_section_case_insensitive(section)
    if not sect:
        return default
    val = sect.get(key, fallback=default)
    if val is None:
        return default
    try:
        return cast(val) if cast is not str else val
    except Exception:
        return default

# Telegram
TELEGRAM_TOKEN: str = cfg("Telegram", "token", "")
if not TELEGRAM_TOKEN:
    logger.error("Отсутствует Telegram token ([Telegram]/token) или переменная TELEGRAM_TOKEN")
    sys.exit(1)

# Webhook
WH_PUBLIC_BASE: str = (cfg("Webhook", "public_base", "") or "").rstrip("/")
WH_PATH: str = (cfg("Webhook", "path", "tg/secret") or "tg/secret").strip("/")
WH_LISTEN_HOST: str = cfg("Webhook", "listen_host", "0.0.0.0")
WH_LISTEN_PORT: int = int(cfg("Webhook", "listen_port", 8080))

# IntraDesk
INTRADESK_API_KEY: str = cfg("IntraDesk", "api_key", "")
INTRADESK_AUTH_TOKEN: str = cfg("IntraDesk", "auth_token", "")
INTRADESK_URL: str = cfg("IntraDesk", "url", "https://apigw.intradesk.ru")
INTRADESK_TASKLIST_URL: str = cfg("IntraDesk", "tasklist_url", f"{INTRADESK_URL}/changes/v3/tasks")
INTRADESK_LEGAL_ENTITIES_URL: str = cfg("IntraDesk", "legal_entities_url", f"{INTRADESK_URL}/settings/api/v3/clients/LegalEntities")
INTRADESK_LEGAL_USERS_URL: str = cfg("IntraDesk", "legal_users_url", f"{INTRADESK_URL}/settings/api/v3/clients/LegalEntities/Users")
INTRADESK_STATUSES_URL: str = cfg("IntraDesk", "statuses_url", f"{INTRADESK_URL}/settings/api/v3/taskStatuses")

# App
DB_FILE: str = cfg("App", "db_file", "tickets.db")
LOCK_FILE: str = "/tmp/helptp_bot.lock"
TG_LIMIT: int = int(cfg("App", "tg_limit", "3500"))

# Reopen
REOPEN_STATUSES = set(int(x) for x in cfg("App","reopen_statuses","106940,106948").split(",") if x.strip())
OPEN_STATUS_ID = int(cfg("App", "open_status_id", "106939"))

# Final
FINAL_STATUSES = set(int(x) for x in cfg("App","final_statuses","99220,99219,99216").split(",") if x.strip())

# Fallback статусы (кеш затирает)
STATUSES = {
    99209: "Открыта", 99211: "Переоткрыта", 99210: "Отложена", 99218: "Требует уточнения",
    99221: "В работе", 99216: "Выполнена", 99213: "Проверена", 99220: "Закрыта",
    99219: "Отменена", 99214: "Отказ",
    106939: "Открыта", 106940: "Отложена", 106948: "Требует уточнения",
}
