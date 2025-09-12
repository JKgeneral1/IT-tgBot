# -*- coding: utf-8 -*-
"""
Telegram → IntraDesk bot (webhook mode for Docker/NPM), keeping original logic.
- Reads settings from config.ini already present in your compose.
- Works in groups and private chats; same DB schema and flows.
- Uses PTB v20.8 JobQueue and Webhook as per requirements.txt / docker-compose.yml.
"""

from __future__ import annotations

import asyncio
import configparser
import datetime as dt
import html
import logging
import os
import re
import sqlite3
import sys
from typing import Any, Dict, Optional, Tuple

import pytz
import requests
from tenacity import retry, stop_after_attempt, wait_fixed
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.error import Forbidden, RetryAfter, BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ==========================
# Logging setup
# ==========================
LOG_FILE = "helptp.log"

_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}

log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = _LEVEL_MAP.get(log_level_name, logging.INFO)

logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger("helptp")

if os.getenv("TELEGRAM_DEBUG"):
    logging.getLogger("telegram").setLevel(logging.DEBUG)
    logging.getLogger("telegram.ext").setLevel(logging.DEBUG)

# ==========================
# Config
# ==========================
config = configparser.ConfigParser()
config.read("config.ini")

# Telegram
TELEGRAM_TOKEN: str = config["Telegram"]["token"].strip()

# IntraDesk
INTRADESK_API_KEY: str = config["IntraDesk"]["api_key"].strip()
INTRADESK_AUTH_TOKEN: str = config["IntraDesk"]["auth_token"].strip()
INTRADESK_URL: str = config["IntraDesk"]["url"].rstrip("/")

# Task endpoints: use changes/v3 for write, tasklist OData for read
# Prefer value from config; auto-fallback to OData URL if provided one is not OData
_CFG_TASKLIST_URL = config["IntraDesk"].get("tasklist_url", f"{INTRADESK_URL}/tasklist/odata/v3/tasks").strip()
TASKS_WRITE_URL = f"{INTRADESK_URL}/changes/v3/tasks"
TASKS_ODATA_URL = _CFG_TASKLIST_URL if "/odata/" in _CFG_TASKLIST_URL else f"{INTRADESK_URL}/tasklist/odata/v3/tasks"

# Legal entities endpoints
INTRADESK_LEGAL_ENTITIES_URL: str = f"{INTRADESK_URL}/settings/api/v3/clients/LegalEntities"
INTRADESK_LEGAL_USERS_URL: str = f"{INTRADESK_URL}/settings/api/v3/clients/LegalEntities/Users"

# App
DB_FILE: str = config["App"].get("db_file", "/data/tickets.db").strip()
OPEN_STATUS_ID: int = int(config["App"].get("open_status_id", "106939"))
REOPEN_STATUSES: set[int] = {int(x) for x in re.split(r"[,\s]+", config["App"].get("reopen_statuses", "106941,106940,106948").strip()) if x}
FINAL_STATUSES: set[int] = {int(x) for x in re.split(r"[,\s]+", config["App"].get("final_statuses", "106950,106949,106946").strip()) if x}
NOTIFY_STATUSES: set[int] = {int(x) for x in re.split(r"[,\s]+", config["App"].get("notify_statuses", "106948").strip()) if x}

# === Автоперевод статуса при комментарии пользователя ===
def _parse_status_map(raw: str) -> Dict[int, int]:
    """'106940->106939,106948->106939' -> {106940:106939, 106948:106939}"""
    mapping: Dict[int, int] = {}
    raw = (raw or "").strip()
    if not raw:
        return mapping
    for token in re.split(r"[,\s]+", raw):
        if not token:
            continue
        if "->" in token:
            a, b = token.split("->", 1)
        elif ":" in token:
            a, b = token.split(":", 1)
        else:
            continue
        try:
            mapping[int(a.strip())] = int(b.strip())
        except ValueError:
            logger.warning("Пропускаю некорректную пару в reopen_map_on_comment: %r", token)
    return mapping

# По умолчанию: 106940|106948 -> 106939. Можно переопределить в [App] config.ini.
REOPEN_MAP_ON_COMMENT: Dict[int, int] = _parse_status_map(
    config["App"].get("reopen_map_on_comment", "106940->106939,106948->106939")
)

# Вкл/выкл периодический опрос IntraDesk (cron)
ENABLE_STATUS_POLLING: bool = config["App"].getboolean("enable_status_polling", fallback=False)


# Webhook / Web
PUBLIC_BASE = config["Webhook"].get("public_base", "").rstrip("/")
WEBHOOK_PATH = config["Webhook"].get("path", "tg/supersecret123").lstrip("/")
LISTEN_HOST = config["Webhook"].get("listen_host", config["Web"].get("bot_host", "0.0.0.0")).strip()
LISTEN_PORT = int(config["Webhook"].get("listen_port", config["Web"].get("bot_port", "8080")))

# Single-instance guard (mostly redundant in Docker, but kept for parity)
LOCK_FILE: str = "/tmp/helptp_bot.lock"

# ===== In-memory text mappings kept as-is (optional) =====
# These names are used only for pretty output in list; IDs are source of truth.
STATUSES_NAMES: Dict[int, str] = {
    106939: "Открыта",
    106941: "Переоткрыта",
    106940: "Отложена",
    106948: "Требует уточнения",
    106951: "В работе",
    106946: "Выполнена",
    106943: "Проверена",
    106950: "Закрыта",
    106949: "Отменена",
    106944: "Отказ",
}

# Ticket evaluation map
EVALUATION_MAPPING: Dict[str, Dict[str, Any]] = {
    "5": {"id": 32989, "text": "Отлично"},
    "4": {"id": 32991, "text": "Хорошо"},
    "3": {"id": 32992, "text": "Удовлетворительно"},
    "2": {"id": 32990, "text": "Плохо"},
    "1": {"id": 32990, "text": "Плохо"},
}

# ==========================
# Utilities
# ==========================

def check_single_instance() -> None:
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            print(f"Бот уже запущен с PID {pid}. Завершите его перед новым запуском.")
            sys.exit(1)
        except Exception:
            pass
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))


def remove_lock_file() -> None:
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass


def clear_logs_job(_: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        with open(LOG_FILE, "w"):
            pass
        logger.info("Лог-файл очищен")
    except Exception as e:
        logger.error(f"Ошибка очистки логов: {e}")


def escape_html(text: Any) -> str:
    return html.escape(str(text))


# ==========================
# DB
# ==========================

def init_db(conn: sqlite3.Connection) -> None:
    with conn:
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS tickets (
                ticket_id TEXT PRIMARY KEY,
                task_number TEXT,
                chat_id INTEGER,
                user_id INTEGER,
                message_id INTEGER,
                last_user_message_id INTEGER,
                last_updated TEXT,
                status INTEGER,
                last_comment TEXT,
                notified_status INTEGER,
                last_engineer_comment TEXT,
                last_notified_reminder TEXT,
                status_changed_at TEXT
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER,
                chat_id INTEGER,
                intradesk_user_id TEXT,
                legal_entity_id TEXT,
                external_id TEXT,
                PRIMARY KEY (user_id, chat_id)
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS groups (
                chat_id INTEGER PRIMARY KEY,
                legal_entity_id TEXT,
                external_id TEXT,
                welcomed INTEGER DEFAULT 0
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS user_comments (
                ticket_id TEXT,
                comment_text TEXT,
                PRIMARY KEY (ticket_id, comment_text)
            )"""
        )
        conn.commit()
    logger.info("База данных инициализирована")


def is_group_welcomed(conn: sqlite3.Connection, chat_id: int) -> int:
    with conn:
        c = conn.cursor()
        c.execute("SELECT welcomed FROM groups WHERE chat_id = ?", (chat_id,))
        row = c.fetchone()
    return (row[0] if row else 0) if row is not None else 0


def get_legal_entity_id(conn: sqlite3.Connection, chat_id: int) -> Optional[str]:
    with conn:
        c = conn.cursor()
        c.execute("SELECT legal_entity_id FROM groups WHERE chat_id = ?", (chat_id,))
        row = c.fetchone()
    return row[0] if row else None


def get_group_external_id(conn: sqlite3.Connection, chat_id: int) -> Optional[str]:
    with conn:
        c = conn.cursor()
        c.execute("SELECT external_id FROM groups WHERE chat_id = ?", (chat_id,))
        row = c.fetchone()
    return row[0] if row else None


def mark_group_welcomed(
    conn: sqlite3.Connection,
    chat_id: int,
    legal_entity_id: Optional[str] = None,
    external_id: Optional[str] = None,
) -> None:
    with conn:
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO groups (chat_id, legal_entity_id, external_id, welcomed) VALUES (?, ?, ?, 1)",
            (chat_id, legal_entity_id, external_id),
        )
        conn.commit()
    logger.info(f"Группа {chat_id} отмечена как приветствованная")


def save_ticket(
    conn: sqlite3.Connection,
    ticket_id: str,
    task_number: str,
    chat_id: int,
    user_id: int,
    message_id: int,
    last_user_message_id: int,
    last_updated: str,
    status: int,
    last_comment: str = "",
    notified_status: Optional[int] = None,
    last_engineer_comment: Optional[str] = None,
    last_notified_reminder: Optional[str] = None,
    status_changed_at: Optional[str] = None,
) -> None:
    with conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO tickets (
                ticket_id, task_number, chat_id, user_id, message_id,
                last_user_message_id, last_updated, status, last_comment,
                notified_status, last_engineer_comment, last_notified_reminder, status_changed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticket_id,
                task_number,
                chat_id,
                user_id,
                message_id,
                last_user_message_id,
                last_updated,
                status,
                last_comment,
                notified_status,
                last_engineer_comment,
                last_notified_reminder,
                status_changed_at,
            ),
        )
        conn.commit()
    logger.info(f"Сохранена заявка: ticket_id={ticket_id}, task_number={task_number}")


def save_user_comment(conn: sqlite3.Connection, ticket_id: str, comment_text: str) -> None:
    with conn:
        c = conn.cursor()
        c.execute(
            "INSERT OR IGNORE INTO user_comments (ticket_id, comment_text) VALUES (?, ?)",
            (ticket_id, comment_text),
        )
        conn.commit()


def clear_user_comments(conn: sqlite3.Connection, ticket_id: str) -> None:
    with conn:
        c = conn.cursor()
        c.execute("DELETE FROM user_comments WHERE ticket_id = ?", (ticket_id,))
        conn.commit()


def is_user_comment(conn: sqlite3.Connection, ticket_id: str, comment_text: str) -> bool:
    with conn:
        c = conn.cursor()
        c.execute(
            "SELECT 1 FROM user_comments WHERE ticket_id = ? AND comment_text = ?",
            (ticket_id, comment_text),
        )
        row = c.fetchone()
    return bool(row)


def get_ticket_info(conn: sqlite3.Connection, ticket_id: str) -> Tuple:
    with conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT chat_id, user_id, message_id, last_user_message_id, last_updated,
                   status, last_comment, notified_status, last_engineer_comment,
                   last_notified_reminder, task_number
            FROM tickets WHERE ticket_id = ?
            """,
            (ticket_id,),
        )
        row = c.fetchone()
    return tuple(row) if row else (None, None, None, None, None, None, None, None, None, None, None)


def has_open_ticket(conn: sqlite3.Connection, user_id: int, chat_id: int) -> Optional[str]:
    with conn:
        c = conn.cursor()
        c.execute(
            "SELECT ticket_id, status FROM tickets WHERE user_id = ? AND chat_id = ?",
            (user_id, chat_id),
        )
        rows = c.fetchall()
    for t in rows or []:
        ticket_id, status = t[0], t[1]
        if status not in FINAL_STATUSES:
            logger.info(f"Найдена открытая заявка: ticket_id={ticket_id}")
            return ticket_id
    return None

# ==========================
# IntraDesk helpers
# ==========================

ID_HEADERS_JSON = {
    "Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}",
    "Accept": "application/json",
}
ID_HEADERS_JSON_W = {
    "Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}",
    "Content-Type": "application/json",
}


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def check_group_in_intradesk(external_id: str) -> Optional[str]:
    url = f"{INTRADESK_LEGAL_ENTITIES_URL}?ApiKey={INTRADESK_API_KEY}&$filter=externalId eq '{external_id}'"
    try:
        r = requests.get(url, headers=ID_HEADERS_JSON, timeout=20)
        r.raise_for_status()
        data = r.json()
        if data.get("value"):
            return str(data["value"][0]["id"])  # API sometimes returns int
        return None
    except requests.RequestException as e:
        logger.error(
            "Ошибка проверки группы в IntraDesk for external_id=%s: %s; resp=%s; URL=%s",
            external_id,
            e,
            getattr(e, "response", None).text if getattr(e, "response", None) else "<no response>",
            url,
        )
        raise


def check_legal_entity_by_inn(inn: str) -> Optional[str]:
    url = f"{INTRADESK_URL}/settings/odata/v2/Clients"
    params = {"ApiKey": INTRADESK_API_KEY, "$filter": f"(taxpayerNumber eq '{inn}' and isArchived eq false)"}
    try:
        r = requests.get(url, headers=ID_HEADERS_JSON, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        clients = data.get("value", [])
        if clients:
            return str(clients[0]["id"])
        logger.error("Компания с ИНН %s не найдена среди активных. resp=%s", inn, r.text)
        return None
    except requests.RequestException as e:
        logger.error("Ошибка запроса к IntraDesk для ИНН %s: %s; resp=%s", inn, e, getattr(e, "response", None).text if getattr(e, "response", None) else "<no response>")
        return None


async def register_legal_entity(chat_id: int, chat_title: str, chat_description: Optional[str], inn: Optional[str] = None) -> Optional[str]:
    external_id = f"telegram_personal_{chat_id}" if chat_id > 0 else f"telegram_group_{chat_id}"
    try:
        existing_id = check_group_in_intradesk(external_id)
        if existing_id:
            return existing_id
    except Exception as e:  # already logged
        logger.warning("Не удалось проверить группу в IntraDesk: %s", e)

    data: Dict[str, Any] = {
        "name": chat_title,
        "contactPersonFirstName": "Клиент",
        "externalId": external_id,
    }
    if inn:
        data["taxpayerNumber"] = inn

    url = f"{INTRADESK_LEGAL_ENTITIES_URL}?ApiKey={INTRADESK_API_KEY}"
    try:
        r = requests.post(url, json=data, headers=ID_HEADERS_JSON_W, timeout=30)
        r.raise_for_status()
        j = r.json()
        return str(j if isinstance(j, (int, str)) else j.get("id"))
    except requests.RequestException as e:
        logger.error("Ошибка регистрации юр. лица для чата %s: %s; resp=%s; URL=%s", chat_id, e, getattr(e, "response", None).text if getattr(e, "response", None) else "<no response>", url)
        return None


def check_user_in_intradesk(external_id: str) -> Optional[str]:
    logger.info("Проверка пользователя с external_id=%s отключена (GET отсутствует)", external_id)
    return None


def register_legal_entity_user(
    conn: sqlite3.Connection,
    user_id: int,
    chat_id: int,
    first_name: Optional[str],
    username: Optional[str],
    legal_entity_id: str,
) -> Optional[str]:
    with conn:
        c = conn.cursor()
        c.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
        row = c.fetchone()
        if row:
            intradesk_id = row[0]
            logger.info("Пользователь %s уже зарегистрирован в SQLite: %s", user_id, intradesk_id)
            return intradesk_id

    external_id = f"telegram_user_{user_id}_group_{chat_id}" if chat_id < 0 else f"telegram_user_{user_id}_personal_{chat_id}"
    existing_id = check_user_in_intradesk(external_id)
    if existing_id:
        with conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO users (user_id, chat_id, intradesk_user_id, legal_entity_id, external_id) VALUES (?, ?, ?, ?, ?)",
                (user_id, chat_id, str(existing_id), legal_entity_id, external_id),
            )
            conn.commit()
        return str(existing_id)

    data: Dict[str, Any] = {
        "firstName": first_name or f"ID_{user_id}",
        "userGroups": [{"id": legal_entity_id, "isDefault": True}],
        "externalId": external_id,
    }
    if username:
        data["telegramUsername"] = username

    url = f"{INTRADESK_LEGAL_USERS_URL}?ApiKey={INTRADESK_API_KEY}"
    try:
        r = requests.post(url, json=data, headers=ID_HEADERS_JSON_W, timeout=30)
        r.raise_for_status()
        j = r.json()
        intradesk_user_id = str(j if isinstance(j, (int, str)) else j.get("id"))
        with conn:
            c = conn.cursor()
            c.execute(
                "INSERT INTO users (user_id, chat_id, intradesk_user_id, legal_entity_id, external_id) VALUES (?, ?, ?, ?, ?)",
                (user_id, chat_id, intradesk_user_id, legal_entity_id, external_id),
            )
            conn.commit()
        logger.info("Пользователь зарегистрирован: %s", intradesk_user_id)
        return intradesk_user_id
    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        if resp is not None and resp.status_code == 409:
            logger.warning("Пользователь с externalId %s уже существует. resp=%s", external_id, resp.text)
            existing_id = check_user_in_intradesk(external_id)
            if existing_id:
                with conn:
                    c = conn.cursor()
                    c.execute(
                        "INSERT OR REPLACE INTO users (user_id, chat_id, intradesk_user_id, legal_entity_id, external_id) VALUES (?, ?, ?, ?, ?)",
                        (user_id, chat_id, str(existing_id), legal_entity_id, external_id),
                    )
                    conn.commit()
                return str(existing_id)
        logger.error("Ошибка регистрации пользователя: %s; resp=%s", e, resp.text if resp is not None else "<no response>")
        return None
    except requests.RequestException as e:
        logger.error("Ошибка регистрации пользователя: %s; resp=%s", e, getattr(e, "response", None).text if getattr(e, "response", None) else "<no response>")
        return None


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def create_ticket(
    conn: sqlite3.Connection,
    title: str,
    description: str,
    user_id: int,
    chat_id: int,
    chat_title: Optional[str] = None,
    file_path: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[int], str]:
    legal_entity_id = get_legal_entity_id(conn, chat_id)
    if not legal_entity_id:
        return None, None, None, "Ошибка: чат не зарегистрирован как юр. лицо"

    with conn:
        c = conn.cursor()
        c.execute("SELECT intradesk_user_id, external_id FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
        row = c.fetchone()
    if not row:
        return None, None, None, "Ошибка: пользователь не зарегистрирован"
    intradesk_user_id, external_id = row[0], row[1]

    ticket_title = f"Заявка из Telegram {chat_title}" if chat_id < 0 and chat_title else f"Заявка из Telegram {user_id}"

    data: Dict[str, Any] = {
        "blocks": {
            "name": f"{{\"value\":\"{ticket_title}\"}}",
            "description": f"{{\"value\":\"{description} (от пользователя {external_id})\"}}",
            "priority": "{\"value\":3}",
            "initiator": f"{{\"value\":{{\"groupid\":{legal_entity_id},\"userid\":{intradesk_user_id}}}}}",
        },
        "Channel": "telegram",
        "clientId": legal_entity_id,
    }

    if file_path:
        fid, fname = upload_file_to_intradesk(file_path, INTRADESK_API_KEY, "Description")
        if fid:
            size = os.path.getsize(file_path)
            ext = os.path.splitext(fname)[1][1:]
            data["blocks"]["attachments"] = (
                f"{{\"value\":{{\"addFiles\":[{{\"name\":\"{fname}\",\"id\":\"{fid}\",\"contentType\":\"{ext}\",\"size\":{size},\"target\":20}}],\"deleteFileIds\":[]}}}}"
            )

    url = f"{TASKS_WRITE_URL}?ApiKey={INTRADESK_API_KEY}"
    try:
        r = requests.post(url, json=data, headers=ID_HEADERS_JSON_W, timeout=30)
        r.raise_for_status()
        j = r.json()
        ticket_id = j.get("Id")
        task_number = str(j.get("Number")) if j.get("Number") is not None else None
        if not ticket_id or not task_number:
            logger.error("Не удалось извлечь ticket_id/Number: resp=%s", r.text)
            return None, None, None, "Ошибка: не удалось создать заявку"
        last_updated = j.get("UpdatedAt", dt.datetime.now(pytz.UTC).isoformat())
        status = int(j.get("Fields", {}).get("status", OPEN_STATUS_ID))
        save_user_comment(conn, ticket_id, description)
        save_ticket(conn, ticket_id, task_number, chat_id, user_id, 0, 0, last_updated, status)
        return ticket_id, last_updated, status, f"Заявка #{task_number} успешно создана!"
    except requests.RequestException as e:
        logger.error("Ошибка создания заявки: %s; resp=%s", e, getattr(e, "response", None).text if getattr(e, "response", None) else "<no response>")
        return None, None, None, f"Ошибка: {e}"


def upload_file_to_intradesk(file_path: str, api_key: str, target: str = "Description", ticket_id: str = "0") -> Tuple[Optional[str], Optional[str]]:
    url = f"{INTRADESK_URL}/files/api/tasks/{ticket_id}/files/target/{target}?ApiKey={api_key}"
    headers = {"Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}"}
    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f)}
            r = requests.post(url, files=files, headers=headers, timeout=60)
            r.raise_for_status()
            j = r.json()[0]
            return j.get("id"), j.get("name")
    except Exception as e:
        logger.error("Ошибка загрузки файла: %s", e)
        return None, None


def add_comment_to_ticket(conn, ticket_id, user_id, chat_id,
                          comment: Optional[str] = None,
                          file_path: Optional[str] = None,
                          last_user_message_id: Optional[int] = None) -> bool:
    # текущий статус и intradesk_user_id
    with conn:
        c = conn.cursor()
        c.execute("SELECT status FROM tickets WHERE ticket_id = ?", (ticket_id,))
        row = c.fetchone()
        current_status = int(row[0]) if row and row[0] is not None else None
        if current_status is not None and current_status in FINAL_STATUSES:
            logger.info("Комментарий к закрытой заявке %s (status=%s) отклонён", ticket_id, current_status)
            return False

        c.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
        row2 = c.fetchone()
    if not row2:
        logger.warning("Нет intradesk_user_id для user=%s chat=%s", user_id, chat_id)
        return False

    data: Dict[str, Any] = {"id": ticket_id, "blocks": {}}
    if comment:
        data["blocks"]["comment"] = f'{{"value":"{comment}"}}'
        save_user_comment(conn, ticket_id, comment)

    if file_path:
        fid, fname = upload_file_to_intradesk(file_path, INTRADESK_API_KEY, "Comment", ticket_id)
        if fid:
            size = os.path.getsize(file_path)
            ext = os.path.splitext(fname)[1][1:]
            data["blocks"]["attachments"] = (
                f'{{"value":{{"addFiles":[{{"name":"{fname}","id":"{fid}","contentType":"{ext}","size":{size},"target":30}}],'
                f'"deleteFileIds":[]}}}}'
            )

    # === АВТО-СМЕНА СТАТУСА ===
    desired_status: Optional[int] = None
    if current_status is not None and current_status in REOPEN_MAP_ON_COMMENT:
        desired_status = REOPEN_MAP_ON_COMMENT[current_status]
    if desired_status is None and current_status is not None and current_status in REOPEN_STATUSES:
        desired_status = OPEN_STATUS_ID  # у тебя в конфиге это 106939

    if desired_status is not None:
        data["blocks"]["status"] = f'{{"value":{desired_status}}}'
        logger.info("Смена статуса ticket=%s: %s -> %s из-за комментария пользователя", ticket_id, current_status, desired_status)

    url = f"{TASKS_WRITE_URL}?ApiKey={INTRADESK_API_KEY}"
    try:
        r = requests.put(url, json=data, headers=ID_HEADERS_JSON_W, timeout=30)
        r.raise_for_status()

        chat_id_db, user_id_db, msg_id, last_uid_msg_id_db, last_updated, status_db, last_comment_db, \
            notified_status, last_engineer_comment, last_notified_reminder, task_number = get_ticket_info(conn, ticket_id)

        effective_status = int(desired_status) if desired_status is not None else (int(status_db) if status_db is not None else OPEN_STATUS_ID)

        save_ticket(conn, ticket_id, task_number, chat_id, user_id,
                    msg_id, last_user_message_id or (last_uid_msg_id_db or 0),
                    last_updated, effective_status,
                    comment or (last_comment_db or ""), notified_status,
                    last_engineer_comment, last_notified_reminder)
        return True
    except requests.RequestException as e:
        logger.error("Ошибка добавления комментария/смены статуса: %s; resp=%s",
                     e, getattr(e, "response", None).text if getattr(e, "response", None) else "<no response>")
        return False



def update_ticket_evaluation(ticket_id: str, rating: str) -> bool:
    url = f"{TASKS_WRITE_URL}?ApiKey={INTRADESK_API_KEY}"
    evaluation = EVALUATION_MAPPING.get(rating, {"id": 32990, "text": "Плохо"})
    data = {
        "id": ticket_id,
        "blocks": {
            "evaluation": f"{{\"value\":{{\"value\":{rating},\"text\":\"{evaluation['text']}\"}}}}"
        },
    }
    try:
        r = requests.put(url, json=data, headers=ID_HEADERS_JSON_W, timeout=30)
        r.raise_for_status()
        logger.info("Оценка для ticket_id=%s обновлена: %s", ticket_id, rating)
        return True
    except requests.RequestException as e:
        logger.error("Ошибка обновления оценки: %s; resp=%s", e, getattr(e, "response", None).text if getattr(e, "response", None) else "<no response>")
        return False

# ==========================
# Telegram handlers
# ==========================

async def send_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_to_message_id: Optional[int] = None,
    parse_mode: str = "HTML",
    reply_markup: Optional[Any] = None,
) -> Any:
    try:
        if chat_id > 0:
            return await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
            return await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_to_message_id=reply_to_message_id,
                reply_markup=reply_markup,
            )
    except Forbidden:
        logger.warning("Бот исключен из чата %s, сообщение не отправлено", chat_id)
    except RetryAfter as e:
        logger.warning("Too Many Requests: sleep %s", e.retry_after)
        await asyncio.sleep(e.retry_after)
        return await send_message(context, chat_id, text, reply_to_message_id, parse_mode, reply_markup)
    except Exception as e:
        logger.error("Ошибка отправки сообщения в чат %s: %s", chat_id, e)
        return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE, conn: sqlite3.Connection) -> None:
    user = update.message.from_user
    chat_id = update.message.chat_id
    message_id = update.message.message_id

    if chat_id < 0:  # group/supergroup
        if not is_group_welcomed(conn, chat_id):
            full_chat = await context.bot.get_chat(chat_id)
            legal_entity_id = await register_legal_entity(chat_id, full_chat.title or str(chat_id), full_chat.description)
            if legal_entity_id:
                mark_group_welcomed(conn, chat_id, legal_entity_id, f"telegram_group_{chat_id}")
            else:
                await send_message(context, chat_id, "Ошибка регистрации группы.", message_id)
                return
        else:
            legal_entity_id = get_legal_entity_id(conn, chat_id)

        with conn:
            c = conn.cursor()
            c.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?", (user.id, chat_id))
            row = c.fetchone()
        if not row and legal_entity_id:
            intradesk_user_id = register_legal_entity_user(conn, user.id, chat_id, user.first_name, user.username, legal_entity_id)
            if not intradesk_user_id:
                await send_message(context, chat_id, "Ошибка при регистрации вас как сотрудника компании.", message_id)
                return
        keyboard = [[KeyboardButton("Создать заявку"), KeyboardButton("Открытые заявки")]]
        text = "Бот успешно добавлен в группу! Выберите действие:" if not row else "Вы уже зарегистрированы! Выберите действие:"
        await send_message(
            context,
            chat_id,
            text,
            message_id,
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False),
        )
    else:  # private chat
        with conn:
            c = conn.cursor()
            c.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?", (user.id, chat_id))
            row = c.fetchone()
        if row:
            keyboard = [[KeyboardButton("Создать заявку"), KeyboardButton("Открытые заявки")]]
            await send_message(
                context,
                chat_id,
                "Вы уже зарегистрированы! Выберите действие:",
                message_id,
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False),
            )
        else:
            context.user_data["awaiting_inn"] = True
            await send_message(context, chat_id, "Пожалуйста, введите ИНН вашей организации (10 или 12 цифр):", message_id)


async def greet_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE, conn: sqlite3.Connection) -> None:
    chat_id = update.effective_chat.id
    message_id = update.message.message_id
    legal_entity_id = get_legal_entity_id(conn, chat_id)
    if not legal_entity_id:
        logger.warning("Группа %s не зарегистрирована как юр. лицо", chat_id)
        return

    keyboard = [[KeyboardButton("Создать заявку"), KeyboardButton("Открытые заявки")]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

    for m in update.message.new_chat_members:
        if m.id != context.bot.id and not m.is_bot:
            intradesk_user_id = register_legal_entity_user(conn, m.id, chat_id, m.first_name, m.username, legal_entity_id)
            if intradesk_user_id:
                await send_message(context, chat_id, "Добро пожаловать в группу!\nЯ бот техподдержки. Выберите действие:", message_id, reply_markup=reply_markup)
            else:
                await send_message(context, chat_id, "Ошибка при регистрации. Попробуйте позже или обратитесь к администратору.", message_id)


async def create_ticket_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, conn: sqlite3.Connection) -> None:
    user = update.message.from_user
    chat_id = update.message.chat_id
    message_id = update.message.message_id

    with conn:
        c = conn.cursor()
        c.execute("SELECT intradesk_user_id, legal_entity_id FROM users WHERE user_id = ? AND chat_id = ?", (user.id, chat_id))
        row = c.fetchone()
    if not row and chat_id < 0:
        legal_entity_id = get_legal_entity_id(conn, chat_id)
        if legal_entity_id:
            intradesk_user_id = register_legal_entity_user(conn, user.id, chat_id, user.first_name, user.username, legal_entity_id)
            if not intradesk_user_id:
                await send_message(context, chat_id, "Ошибка при регистрации вас как сотрудника компании.", message_id)
                return
        else:
            await send_message(context, chat_id, "Ошибка: чат не зарегистрирован как юр. лицо.", message_id)
            return
    elif not row:
        context.user_data["awaiting_inn"] = True
        await send_message(context, chat_id, "Пожалуйста, введите ИНН вашей организации (10 или 12 цифр):", message_id)
        return

    open_ticket_id = has_open_ticket(conn, user.id, chat_id)
    if open_ticket_id:
        with conn:
            c = conn.cursor()
            c.execute("SELECT task_number FROM tickets WHERE ticket_id = ?", (open_ticket_id,))
            row2 = c.fetchone()
            task_number = row2[0] if row2 else "Unknown"
        keyboard = [[
            InlineKeyboardButton("Продолжить", callback_data=f"continue_{open_ticket_id}"),
            InlineKeyboardButton("Создать новую", callback_data=f"new_{user.id}_{chat_id}"),
        ]]
        await send_message(
            context,
            chat_id,
            f"У вас уже есть открытая заявка #{task_number}. Хотите продолжить в ней или создать новую?",
            message_id,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    else:
        ticket_id, last_updated, status, result = create_ticket(
            conn,
            "Ожидание описания",
            "Ожидание описания",
            user.id,
            chat_id,
            update.message.chat.title if chat_id < 0 else None,
        )
        if ticket_id:
            with conn:
                c = conn.cursor()
                c.execute("SELECT task_number FROM tickets WHERE ticket_id = ?", (ticket_id,))
                r = c.fetchone()
                if not r:
                    logger.error("Заявка %s не найдена в базе после создания", ticket_id)
                    await send_message(context, chat_id, "Ошибка при создании заявки.", message_id)
                    return
                task_number = r[0]
            save_ticket(conn, ticket_id, task_number, chat_id, user.id, message_id, message_id, last_updated, status)
            context.user_data["active_ticket"] = ticket_id
            sent = await send_message(context, chat_id, f"Заявка #{task_number} создана. Опишите проблему и ожидайте ответа специалиста.", message_id)
            if sent:
                with conn:
                    c = conn.cursor()
                    c.execute("UPDATE tickets SET message_id = ? WHERE ticket_id = ?", (sent.message_id, ticket_id))
                    conn.commit()
        else:
            logger.error("Не удалось создать заявку для user=%s chat=%s: %s", user.id, chat_id, result)
            await send_message(context, chat_id, escape_html(result), message_id)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE, conn: sqlite3.Connection) -> None:
    user = update.message.from_user
    chat_id = update.message.chat_id
    message_id = update.message.message_id
    message_text = update.message.text

    if message_text == "Создать заявку":
        await create_ticket_handler(update, context, conn)
        return
    if message_text == "Открытые заявки":
        await list_tickets(update, context, conn)
        return

    if chat_id > 0 and context.user_data.get("awaiting_inn"):
        inn = (message_text or "").strip()
        if not re.match(r"^\d{10}$|^\d{12}$", inn):
            await send_message(context, chat_id, "Пожалуйста, введите корректный ИНН (10 или 12 цифр).", message_id)
            return
        legal_entity_id = check_legal_entity_by_inn(inn)
        if not legal_entity_id:
            await send_message(
                context,
                chat_id,
                f"Организация с ИНН {inn} не найдена в IntraDesk. Пожалуйста, проверьте ИНН или обратитесь к администратору @itas23.",
                message_id,
            )
            return
        intradesk_user_id = register_legal_entity_user(conn, user.id, chat_id, user.first_name, user.username, legal_entity_id)
        if not intradesk_user_id:
            await send_message(context, chat_id, "Ошибка при регистрации. Попробуйте позже.", message_id)
            return
        context.user_data.pop("awaiting_inn", None)
        mark_group_welcomed(conn, chat_id, legal_entity_id, f"telegram_personal_{chat_id}")
        keyboard = [[KeyboardButton("Создать заявку"), KeyboardButton("Открытые заявки")]]
        await send_message(
            context,
            chat_id,
            "Вы успешно зарегистрированы! Выберите действие:",
            message_id,
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False),
        )
        return

    with conn:
        c = conn.cursor()
        c.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?", (user.id, chat_id))
        row = c.fetchone()
    if not row:
        await send_message(context, chat_id, "Пожалуйста, используйте /start для регистрации перед созданием заявки!", message_id)
        return

    if chat_id < 0 and not context.user_data.get("active_ticket") and not has_open_ticket(conn, user.id, chat_id):
        return

    file_path = None
    if update.message.photo:
        ph = await update.message.photo[-1].get_file()
        file_path = f"temp_{user.id}_{message_id}.jpg"
        await ph.download_to_drive(file_path)
        message_text = update.message.caption or "Фото от пользователя"
    elif update.message.document:
        doc = await update.message.document.get_file()
        safe_name = update.message.document.file_name or "file.bin"
        file_path = f"temp_{user.id}_{message_id}_{safe_name}"
        await doc.download_to_drive(file_path)
        message_text = update.message.caption or "Файл от пользователя"
    elif update.message.voice:
        vf = await update.message.voice.get_file()
        file_path = f"temp_{user.id}_{message_id}.ogg"
        await vf.download_to_drive(file_path)
        message_text = update.message.caption or "Голосовое сообщение от пользователя"
    elif not message_text:
        message_text = "Сообщение без текста"

    ticket_id = context.user_data.get("active_ticket") or has_open_ticket(conn, user.id, chat_id)
    if ticket_id:
        if add_comment_to_ticket(conn, ticket_id, user.id, chat_id, message_text, file_path, message_id):
            with conn:
                c = conn.cursor()
                c.execute("SELECT task_number, message_id FROM tickets WHERE ticket_id = ?", (ticket_id,))
                r = c.fetchone()
                ticket_message_id = r[1] if r else None
            if ticket_message_id:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=ticket_message_id)
                    with conn:
                        c = conn.cursor()
                        c.execute("UPDATE tickets SET message_id = 0 WHERE ticket_id = ?", (ticket_id,))
                        conn.commit()
                except Exception as e:
                    logger.warning("Не удалось удалить сообщение %s в чате %s: %s", ticket_message_id, chat_id, e)
        else:
            if chat_id > 0:
                await send_message(context, chat_id, "Пожалуйста, нажмите на кнопку «Создать заявку»", message_id)
    else:
        if chat_id > 0:
            await send_message(context, chat_id, "Пожалуйста, нажмите на кнопку «Создать заявку»", message_id)

    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
        except Exception:
            pass


async def list_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE, conn: sqlite3.Connection) -> None:
    user = update.message.from_user
    chat_id = update.message.chat_id
    message_id = update.message.message_id

    # 1) Формируем список финальных статусов (или дефолт)
    finals = tuple(FINAL_STATUSES) if FINAL_STATUSES else (106950, 106949, 106946)

    # 2) Динамически генерим плейсхолдеры под любой размер finals
    placeholders = ",".join("?" for _ in finals)

    sql = (
        f"SELECT ticket_id, task_number, status "
        f"FROM tickets "
        f"WHERE user_id = ? AND chat_id = ? AND status NOT IN ({placeholders})"
    )

    with conn:
        c = conn.cursor()
        # 3) Параметры: (user.id, chat_id) + finals — без звёздочки+тернарника в кортеже
        params = (user.id, chat_id) + finals
        c.execute(sql, params)
        rows = c.fetchall()

    tickets = rows or []
    if not tickets:
        await send_message(context, chat_id, "У вас нет открытых заявок.", message_id)
        return

    text = "Ваши открытые заявки:\n"
    kb: list[list[InlineKeyboardButton]] = []
    for ticket_id, task_number, status in tickets:
        status_text = STATUSES_NAMES.get(int(status), "Неизвестный")
        tn = task_number or "Unknown"
        text += f"Заявка #{tn} - {status_text}\n"
        kb.append([InlineKeyboardButton(f"Заявка #{tn}", callback_data=f"continue_{ticket_id}")])

    sent = await send_message(context, chat_id, text, message_id, reply_markup=InlineKeyboardMarkup(kb))
    if sent:
        with conn:
            c = conn.cursor()
            for ticket_id, *_ in tickets:
                c.execute("UPDATE tickets SET message_id = ? WHERE ticket_id = ?", (sent.message_id, ticket_id))
            conn.commit()



async def handle_ticket_choice(update: Update, context: ContextTypes.DEFAULT_TYPE, conn: sqlite3.Connection) -> None:
    query = update.callback_query
    await query.answer()
    user = query.from_user
    chat_id = query.message.chat_id

    action, *params = query.data.split("_")
    if action == "continue":
        ticket_id = params[0]
        with conn:
            c = conn.cursor()
            c.execute("SELECT task_number, message_id FROM tickets WHERE ticket_id = ?", (ticket_id,))
            row = c.fetchone()
            task_number = row[0] if row else "Unknown"
            ticket_message_id = row[1] if row else None
        await query.edit_message_text(f"Выбрана заявка #{task_number}. Добавьте комментарий.", parse_mode="HTML")
        context.user_data["active_ticket"] = ticket_id
        if ticket_message_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=ticket_message_id)
                with conn:
                    c = conn.cursor()
                    c.execute("UPDATE tickets SET message_id = 0 WHERE ticket_id = ?", (ticket_id,))
                    conn.commit()
            except Exception as e:
                logger.warning("Не удалось удалить сообщение %s: %s", ticket_message_id, e)
    elif action == "new":
        user_id, chat_id2 = map(int, params)
        if user_id != user.id:
            await query.edit_message_text("Вы не можете создавать заявки от имени другого пользователя!", parse_mode="HTML")
            return
        ticket_id, last_updated, status, result = create_ticket(
            conn,
            "Ожидание описания",
            "Ожидание описания",
            user.id,
            chat_id2,
            query.message.chat.title if chat_id2 < 0 else None,
        )
        if ticket_id:
            with conn:
                c = conn.cursor()
                c.execute("SELECT task_number, message_id FROM tickets WHERE ticket_id = ?", (ticket_id,))
                row = c.fetchone()
                if not row:
                    logger.error("Заявка %s не найдена в БД после создания", ticket_id)
                    text = "Ошибка при создании заявки."
                else:
                    task_number = row[0]
                    ticket_message_id = row[1]
                    save_ticket(conn, ticket_id, task_number, chat_id2, user.id, query.message.message_id, query.message.message_id, last_updated, status)
                    text = f"Заявка #{task_number} создана. Опишите проблему и ожидайте ответа специалиста."
                    context.user_data["active_ticket"] = ticket_id
                    if ticket_message_id:
                        try:
                            await context.bot.delete_message(chat_id=chat_id2, message_id=ticket_message_id)
                            with conn:
                                c = conn.cursor()
                                c.execute("UPDATE tickets SET message_id = 0 WHERE ticket_id = ?", (ticket_id,))
                                conn.commit()
                        except Exception as e:
                            logger.warning("Не удалось удалить сообщение %s: %s", ticket_message_id, e)
            await query.edit_message_text(text, parse_mode="HTML")
        else:
            await query.edit_message_text(escape_html(result), parse_mode="HTML")


async def handle_rating(update: Update, context: ContextTypes.DEFAULT_TYPE, conn: sqlite3.Connection) -> None:
    query = update.callback_query
    await query.answer()
    _, ticket_id, expected_user_id, rating = query.data.split("_")
    user = query.from_user
    chat_id = query.message.chat_id if query.message else update.effective_chat.id

    chat_id_db, user_id_db, message_id, last_user_message_id_db, last_updated, status, \
        last_comment_db, notified_status, last_engineer_comment, last_notified_reminder, task_number = get_ticket_info(conn, ticket_id)

    # только владелец заявки может оценивать
    if str(user.id) != expected_user_id or user.id != user_id_db:
        try:
            await query.edit_message_text("Вы не можете оценить эту заявку, так как она не ваша!", parse_mode="HTML")
        except BadRequest:
            # если сообщение уже удалено — просто молча игнорируем
            pass
        return

    if update_ticket_evaluation(ticket_id, rating):
        text = "Спасибо за оценку, ваше мнение важно для нас!"

        # 1) СНАЧАЛА пытаемся отредактировать сообщение с кнопками
        try:
            await query.edit_message_text(text, parse_mode="HTML")
        except BadRequest:
            # Если сообщение уже удалено/недоступно — отправим отдельное сообщение благодарности
            try:
                await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
            except Exception as e:
                logger.warning("Не удалось отправить благодарность: %s", e)

        # 2) ПОТОМ удаляем сохранённое сообщение (если оно ещё есть)
        if message_id:
            try:
                # если это то же сообщение, которое мы только что редактировали — пропустим удаление
                same_msg = query.message and (message_id == query.message.message_id)
                if not same_msg:
                    await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                with conn:
                    c = conn.cursor()
                    c.execute("UPDATE tickets SET message_id = 0 WHERE ticket_id = ?", (ticket_id,))
                    conn.commit()
            except Exception as e:
                logger.warning("Не удалось удалить сообщение %s: %s", message_id, e)
    else:
        try:
            await query.edit_message_text(f"Ошибка при сохранении оценки для заявки #{task_number}!", parse_mode="HTML")
        except BadRequest:
            pass


async def check_ticket_status(context: ContextTypes.DEFAULT_TYPE, conn: sqlite3.Connection) -> None:
    try:
        with conn:
            c = conn.cursor()
            c.execute(
                """
                SELECT ticket_id, task_number, chat_id, user_id, message_id, last_user_message_id, last_comment,
                       last_updated, status, notified_status, last_engineer_comment, last_notified_reminder, status_changed_at
                FROM tickets
                """
            )
            tickets = c.fetchall()

        for ticket in tickets or []:
            try:
                ticket_id = ticket[0]
                task_number = ticket[1]
                chat_id = ticket[2]
                user_id = ticket[3]
                message_id = ticket[4]
                last_user_message_id = ticket[5]
                last_comment = ticket[6]
                last_updated = ticket[7]
                status_db = int(ticket[8]) if ticket[8] is not None else OPEN_STATUS_ID
                notified_status = ticket[9]
                last_engineer_comment_db = ticket[10]
                last_notified_reminder = ticket[11]
                status_changed_at_db = ticket[12]

                url = f"{TASKS_ODATA_URL}?ApiKey={INTRADESK_API_KEY}&$filter=Id eq {ticket_id}"
                r = requests.get(url, headers=ID_HEADERS_JSON, timeout=30)
                r.raise_for_status()
                data = r.json()
                if not data.get("value"):
                    logger.warning("Заявка #%s не найдена в IntraDesk", task_number)
                    continue
                td = data["value"][0]
                status = int(td.get("status", status_db))
                updated_at = td.get("updatedat", "1970-01-01T00:00:00Z")

                lifetime = (td.get("lifetime", {}) or {}).get("data", [])
                latest_engineer_comment = None
                latest_client_comment_time = None
                for entry in sorted(lifetime, key=lambda x: x.get("eventat", ""), reverse=True):
                    events = (entry.get("events", {}) or {}).get("data", [])
                    for ev in events:
                        comment_text = ev.get("stringvalue", "")
                        changed_by = ev.get("changedby", "")
                        event_time = entry.get("eventat")
                        if ev.get("blockname") == "comment" and comment_text:
                            if "customer_" not in changed_by and not is_user_comment(conn, ticket_id, comment_text):
                                latest_engineer_comment = comment_text
                                break
                            elif "customer_" in changed_by and is_user_comment(conn, ticket_id, comment_text):
                                latest_client_comment_time = event_time
                    if latest_engineer_comment:
                        break

                status_changed_at = status_changed_at_db or updated_at
                if status != status_db:
                    status_changed_at = updated_at

                if last_updated != updated_at and last_user_message_id:
                    try:
                        _ = await context.bot.get_chat_member(chat_id, user_id)  # existence check

                        if latest_engineer_comment and latest_engineer_comment != last_engineer_comment_db and not is_user_comment(conn, ticket_id, latest_engineer_comment):
                            await send_message(context, chat_id, escape_html(latest_engineer_comment), last_user_message_id)

                        if status != status_db and status in NOTIFY_STATUSES and (notified_status is None or status != int(notified_status)):
                            await send_message(
                                context,
                                chat_id,
                                f"Заявка #{task_number} требует вашего ответа, добавьте комментарий или, если заявка уже не актуальна, мы её закроем!",
                                last_user_message_id,
                            )
                            save_ticket(
                                conn,
                                ticket_id,
                                task_number,
                                chat_id,
                                user_id,
                                message_id,
                                last_user_message_id,
                                updated_at,
                                status,
                                last_comment,
                                status,
                                latest_engineer_comment,
                                last_notified_reminder,
                                status_changed_at,
                            )
                        elif status != status_db and status in FINAL_STATUSES:
                            kb = [[InlineKeyboardButton(str(i), callback_data=f"rate_{ticket_id}_{user_id}_{i}") for i in range(1, 6)]]
                            await send_message(
                                context,
                                chat_id,
                                f"Заявка #{task_number} {'выполнена' if status != OPEN_STATUS_ID else 'закрыта'}! Пожалуйста, оцените качество:",
                                last_user_message_id,
                                reply_markup=InlineKeyboardMarkup(kb),
                            )
                            save_ticket(
                                conn,
                                ticket_id,
                                task_number,
                                chat_id,
                                user_id,
                                message_id,
                                last_user_message_id,
                                updated_at,
                                status,
                                last_comment,
                                status,
                                latest_engineer_comment,
                                last_notified_reminder,
                                status_changed_at,
                            )
                            if message_id:
                                try:
                                    await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                                    with conn:
                                        c = conn.cursor()
                                        c.execute("UPDATE tickets SET message_id = 0 WHERE ticket_id = ?", (ticket_id,))
                                        conn.commit()
                                except Exception as e:
                                    logger.warning("Не удалось удалить сообщение %s в чате %s: %s", message_id, chat_id, e)
                        else:
                            if status != status_db or latest_engineer_comment != last_engineer_comment_db:
                                save_ticket(
                                    conn,
                                    ticket_id,
                                    task_number,
                                    chat_id,
                                    user_id,
                                    message_id,
                                    last_user_message_id,
                                    updated_at,
                                    status,
                                    last_comment,
                                    notified_status,
                                    latest_engineer_comment,
                                    last_notified_reminder,
                                    status_changed_at,
                                )
                                if status in FINAL_STATUSES:
                                    clear_user_comments(conn, ticket_id)

                        if status in NOTIFY_STATUSES:
                            now = dt.datetime.now(pytz.UTC)
                            status_change_time = dt.datetime.fromisoformat(status_changed_at.replace("Z", "+00:00"))
                            time_diff = now - status_change_time
                            has_recent_client_comment = (
                                latest_client_comment_time
                                and dt.datetime.fromisoformat(latest_client_comment_time.replace("Z", "+00:00")) > status_change_time
                            )
                            last_notified_dt = (
                                dt.datetime.fromisoformat(last_notified_reminder.replace("Z", "+00:00"))
                                if last_notified_reminder
                                else None
                            )

                            if (
                                not has_recent_client_comment
                                and time_diff.total_seconds() >= 2 * 3600
                                and (last_notified_dt is None or last_notified_dt < now - dt.timedelta(hours=24))
                            ):
                                await send_message(
                                    context,
                                    chat_id,
                                    f"Напоминание: заявка #{task_number} требует вашего ответа, добавьте комментарий или, если заявка уже не актуальна, мы её закроем!",
                                    last_user_message_id,
                                )
                                save_ticket(
                                    conn,
                                    ticket_id,
                                    task_number,
                                    chat_id,
                                    user_id,
                                    message_id,
                                    last_user_message_id,
                                    updated_at,
                                    status,
                                    last_comment,
                                    notified_status,
                                    latest_engineer_comment,
                                    now.isoformat(),
                                    status_changed_at,
                                )
                    except Forbidden as e:
                        logger.warning("Бот исключен из чата %s или нет доступа к пользователю %s: %s", chat_id, user_id, e)
                    except Exception as e:
                        logger.error("Ошибка при обработке заявки #%s в чате %s: %s", task_number, chat_id, e)
            except Exception as e:
                logger.error("Ошибка обработки ticket_id=%s: %s", ticket[0], e)
                continue
    except Exception as e:
        logger.error("Глобальная ошибка в check_ticket_status: %s", e, exc_info=True)


async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE, conn: sqlite3.Connection) -> None:
    chat = update.my_chat_member.chat
    if chat.type in ["group", "supergroup"] and update.my_chat_member.new_chat_member.status == "member" and not is_group_welcomed(conn, chat.id):
        full_chat = await context.bot.get_chat(chat.id)
        legal_entity_id = await register_legal_entity(chat.id, full_chat.title or str(chat.id), full_chat.description)
        if legal_entity_id:
            mark_group_welcomed(conn, chat.id, legal_entity_id, f"telegram_group_{chat.id}")
            keyboard = [[KeyboardButton("Создать заявку"), KeyboardButton("Открытые заявки")]]
            await send_message(
                context,
                chat.id,
                "Бот успешно добавлен в группу! Выберите действие:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False),
            )
        else:
            await send_message(context, chat.id, "Ошибка регистрации группы.")


# ==========================
# Bootstrap
# ==========================

def main() -> None:
    check_single_instance()
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        init_db(conn)

        app = Application.builder().token(TELEGRAM_TOKEN).build()

        jq = app.job_queue

        # Опрос IntraDesk отключаем по умолчанию (включается флагом в config.ini)
        if ENABLE_STATUS_POLLING:
           jq.run_repeating(lambda ctx: asyncio.create_task(check_ticket_status(ctx, conn)), interval=5, first=5)
           logger.info("IntraDesk polling ENABLED (every 5s).")
        else:
           logger.info("IntraDesk polling DISABLED (enable_status_polling=0).")

        jq.run_daily(clear_logs_job, time=dt.time(hour=0, minute=0, tzinfo=pytz.UTC))


        # Handlers
        app.add_handler(CommandHandler("start", lambda upd, ctx: start(upd, ctx, conn)))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lambda upd, ctx: handle_message(upd, ctx, conn)))
        app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL | filters.VOICE, lambda upd, ctx: handle_message(upd, ctx, conn)))
        app.add_handler(CallbackQueryHandler(lambda upd, ctx: handle_ticket_choice(upd, ctx, conn), pattern=r"^(continue|new)_"))
        app.add_handler(CallbackQueryHandler(lambda upd, ctx: handle_rating(upd, ctx, conn), pattern=r"^rate_"))
        app.add_handler(ChatMemberHandler(lambda upd, ctx: handle_my_chat_member(upd, ctx, conn)))
        app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, lambda upd, ctx: greet_new_member(upd, ctx, conn)))

        # Webhook (compose exposes 8080, NPM will proxy HTTPS to it)
        webhook_url = f"{PUBLIC_BASE}/{WEBHOOK_PATH}" if PUBLIC_BASE else None
        if not webhook_url:
            logger.warning("PUBLIC_BASE пуст — запускаем polling как fallback")
            app.run_polling(poll_interval=2, drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
            return

        logger.info("Starting webhook on %s:%s path=/%s → %s", LISTEN_HOST, LISTEN_PORT, WEBHOOK_PATH, webhook_url)
        try:
           app.run_webhook(
               listen=LISTEN_HOST,
               port=LISTEN_PORT,
              url_path=WEBHOOK_PATH,
              webhook_url=webhook_url,
              drop_pending_updates=True,
              allowed_updates=Update.ALL_TYPES,
           )
        except Exception as e:
           logger.error("Webhook запуск не удался (%s). Переключаюсь на polling.", e)
           app.run_polling(poll_interval=2, drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


    except Exception as e:
        logger.error("Ошибка запуска бота: %s", e)
    finally:
        try:
            if conn:
                conn.close()
        finally:
            remove_lock_file()


if __name__ == "__main__":
    main()
