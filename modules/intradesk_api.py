# -*- coding: utf-8 -*-
import json
import os
import requests
import sqlite3
from typing import Any, Dict, Optional, Tuple

from tenacity import retry, stop_after_attempt, wait_fixed

from .config import (
    INTRADESK_API_KEY, INTRADESK_AUTH_TOKEN, INTRADESK_URL, INTRADESK_TASKLIST_URL,
    INTRADESK_LEGAL_ENTITIES_URL, INTRADESK_LEGAL_USERS_URL,
    OPEN_STATUS_ID, REOPEN_STATUSES
)
from .db import (
    save_ticket, save_user_comment, get_ticket_info,
    get_group_default_user_id, set_group_default_user_id,
    get_legal_entity_id
)
from .logger import setup_logging
from .status_cache import get_status_name_by_id, parse_status_from_fields
from .utils import normalize_text

logger = setup_logging()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def check_group_in_intradesk(external_id: str) -> Optional[str]:
    url = f"{INTRADESK_LEGAL_ENTITIES_URL}?ApiKey={INTRADESK_API_KEY}&$filter=externalId eq '{external_id}'"
    headers = {"Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    return str(data["value"][0]["id"]) if data.get("value") else None

def check_legal_entity_by_inn(inn: str) -> Optional[str]:
    url = f"{INTRADESK_URL}/settings/odata/v2/Clients"
    headers = {"Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}", "Accept": "application/json"}
    params = {"ApiKey": INTRADESK_API_KEY, "$filter": f"(taxpayerNumber eq '{inn}' and isArchived eq false)"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        data = r.json().get("value", [])
        return str(data[0]["id"]) if data else None
    except requests.RequestException as e:
        logger.error("Ошибка запроса Clients по ИНН %s: %s", inn, e)
        return None

def ensure_group_default_user(conn: sqlite3.Connection, chat_id: int, legal_entity_id: str, chat_title: Optional[str]) -> Optional[str]:
    existing = get_group_default_user_id(conn, chat_id)
    if existing:
        return existing
    external_id = f"telegram_group_user_{chat_id}"
    url = f"{INTRADESK_LEGAL_USERS_URL}?ApiKey={INTRADESK_API_KEY}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}"}
    data = {
        "firstName": (chat_title or f"TG Group {chat_id}")[:50],
        "userGroups": [{"id": int(legal_entity_id), "isDefault": True}],
        "externalId": external_id,
    }
    try:
        r = requests.post(url, json=data, headers=headers, timeout=15)
        r.raise_for_status()
        jd = r.json()
        intradesk_user_id = str(jd if isinstance(jd, (int, str)) else jd.get("id"))
        if not intradesk_user_id:
            logger.error("ensure_group_default_user: пустой intradesk_user_id")
            return None
        set_group_default_user_id(conn, chat_id, intradesk_user_id)
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO users (user_id, chat_id, intradesk_user_id, legal_entity_id, external_id) VALUES (?, ?, ?, ?, ?)",
                (0, chat_id, intradesk_user_id, legal_entity_id, external_id),
            )
            conn.commit()
        logger.info("ensure_group_default_user: created %s for chat %s", intradesk_user_id, chat_id)
        return intradesk_user_id
    except requests.RequestException as e:
        logger.error("ensure_group_default_user: ошибка создания пользователя IntraDesk: %s", e)
        return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def create_ticket(conn: sqlite3.Connection, title: str, description: str, user_id: int, chat_id: int,
                  chat_title: Optional[str] = None, force_intradesk_user_id: Optional[str] = None
                 ) -> Tuple[Optional[str], Optional[str], Optional[int], str]:
    legal_entity_id = get_legal_entity_id(conn, chat_id)
    if not legal_entity_id:
        return None, None, None, "Ошибка: чат не зарегистрирован как юр. лицо"
    intradesk_user_id: Optional[str] = force_intradesk_user_id
    if not intradesk_user_id:
        row = conn.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?",
                           (user_id, chat_id)).fetchone()
        intradesk_user_id = row["intradesk_user_id"] if row else None
        if chat_id < 0 and not intradesk_user_id:
            intradesk_user_id = ensure_group_default_user(conn, chat_id, legal_entity_id, chat_title)
    if not intradesk_user_id:
        return None, None, None, "Ошибка: не найден пользователь IntraDesk (личный или групповой)"

    ticket_title = (f"Заявка из Telegram {chat_title}" if chat_id < 0 and chat_title else f"Заявка из Telegram {user_id}")
    url = f"{INTRADESK_TASKLIST_URL}?ApiKey={INTRADESK_API_KEY}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}"}
    data = {
        "blocks": {
            "name": f'{{"value":"{normalize_text(ticket_title)}"}}',
            "description": f'{{"value":"{normalize_text(description)}"}}',
            "priority": '{"value":3}',
            "initiator": f'{{"value":{{"groupid":{legal_entity_id},"userid":{intradesk_user_id}}}}}',
        },
        "Channel": "telegram",
        "clientId": int(legal_entity_id),
    }
    try:
        r = requests.post(url, json=data, headers=headers, timeout=15)
        logger.info("Create ticket payload: %s", json.dumps(data, ensure_ascii=False))
        r.raise_for_status()
        jd = r.json()
        ticket_id = str(jd.get("Id"))
        task_number = str(jd.get("Number"))
        if not ticket_id or not task_number:
            return None, None, None, "Ошибка: не удалось создать заявку"
        last_updated = jd.get("UpdatedAt")
        status = 99209
        if isinstance(jd.get("Fields", {}), dict):
            status = int(jd["Fields"].get("status", 99209))
        save_user_comment(conn, ticket_id, normalize_text(description))
        save_ticket(conn, ticket_id, task_number, chat_id, user_id, 0, 0, last_updated or "", status)
        return ticket_id, last_updated, status, f"Заявка #{task_number} успешно создана!"
    except requests.RequestException as e:
        logger.error("Ошибка создания заявки: %s", e)
        return None, None, None, f"Ошибка: {e}"

def upload_file_to_intradesk(file_path: str, api_key: str, target: str = "Comment", ticket_id: str = "0") -> Tuple[Optional[str], Optional[str]]:
    url = f"{INTRADESK_URL}/files/api/tasks/{ticket_id}/files/target/{target}?ApiKey={api_key}"
    headers = {"Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}"}
    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f)}
        try:
            r = requests.post(url, files=files, headers=headers, timeout=60)
            r.raise_for_status()
            jd = r.json()[0]
            return jd.get("id"), jd.get("name")
        except requests.RequestException as e:
            logger.error("Ошибка загрузки файла: %s", e)
            return None, None

def add_comment_to_ticket(conn: sqlite3.Connection, ticket_id: str, user_id: int, chat_id: int,
                          comment: Optional[str] = None, file_path: Optional[str] = None,
                          last_user_message_id: Optional[int] = None) -> bool:
    row = conn.execute("SELECT status, task_number FROM tickets WHERE ticket_id = ?", (ticket_id,)).fetchone()
    if row and row["status"] in REOPEN_STATUSES and comment:
        pass
    if row and row["status"] in ():
        pass

    # resolve intradesk user
    row_u = conn.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?",
                         (user_id, chat_id)).fetchone()
    if not row_u and chat_id < 0:
        legal_id = get_legal_entity_id(conn, chat_id)
        group_uid = ensure_group_default_user(conn, chat_id, legal_id, None) if legal_id else None
        if group_uid:
            row_u = {"intradesk_user_id": group_uid}
        else:
            logger.info("Нет личной/групповой учётки IntraDesk (chat=%s user=%s)", chat_id, user_id)
            return False
    elif not row_u:
        logger.info("Пользователь не зарегистрирован в IntraDesk (chat=%s user=%s)", chat_id, user_id)
        return False

    current_status = row["status"] if row else None
    task_number = row["task_number"] if row else ticket_id

    url = f"{INTRADESK_TASKLIST_URL}?ApiKey={INTRADESK_API_KEY}"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}",
        "Accept": "application/json",
    }

    blocks_obj: Dict[str, Any] = {}
    norm_comment = normalize_text(comment) if comment else None
    if norm_comment:
        blocks_obj["comment"] = {"value": norm_comment}
        save_user_comment(conn, ticket_id, norm_comment)

    if file_path:
        file_id, file_name = upload_file_to_intradesk(file_path, INTRADESK_API_KEY, "Comment", ticket_id)
        if file_id:
            blocks_obj["attachments"] = {"value": {
                "addFiles": [{
                    "name": file_name,
                    "id": str(file_id),
                    "contentType": os.path.splitext(file_name)[1][1:],
                    "size": os.path.getsize(file_path),
                    "target": 30,
                }],
                "deleteFileIds": []
            }}

    should_reopen = False
    if current_status is None:
        should_reopen = True
    elif current_status != OPEN_STATUS_ID and current_status in REOPEN_STATUSES:
        should_reopen = True
    if norm_comment and should_reopen and OPEN_STATUS_ID:
        blocks_obj["status"] = {"value": int(OPEN_STATUS_ID)}
        logger.info("will set status -> %s (%s)", OPEN_STATUS_ID, get_status_name_by_id(conn, OPEN_STATUS_ID))

    data_blocks: Dict[str, Any] = {k: json.dumps(v, ensure_ascii=False) for k, v in blocks_obj.items()}
    data = {"id": ticket_id, "blocks": data_blocks}

    try:
        preview = json.dumps(data, ensure_ascii=False)
        logger.info("IntraDesk PUT payload ticket=%s: %s", ticket_id, preview[:4000])
        r = requests.put(url, json=data, headers=headers, timeout=30)
        if not r.ok:
            logger.error("IntraDesk PUT failed ticket=%s http=%s body=%s", ticket_id, r.status_code, r.text[:4000])
            r.raise_for_status()

        logger.info("IntraDesk PUT ok ticket=%s raw_response=%s", ticket_id, (r.text or "")[:4000])
        jd = r.json() if r.content else {}
        new_status = None
        try:
            fields = jd.get("Fields") if isinstance(jd, dict) else None
            new_status = parse_status_from_fields(fields)
        except Exception:
            logger.exception("Не удалось распарсить статус из ответа PUT")

        from .db import get_ticket_info  # avoid cycle top
        chat_id_db, user_id_db, message_id_db, last_user_message_id_db, last_updated_db, \
        status_db, last_comment_db, notified_status, last_engineer_comment, \
        last_notified_reminder, task_number_db = get_ticket_info(conn, ticket_id)

        old_status = status_db or current_status
        saved_status = new_status if new_status is not None else (OPEN_STATUS_ID if (norm_comment and should_reopen) else (old_status or 99209))
        saved_updated_at = (jd.get("UpdatedAt") if isinstance(jd, dict) and jd.get("UpdatedAt") else None) or ""

        status_changed_at = None
        if old_status is None or int(saved_status) != int(old_status):
            status_changed_at = __import__("datetime").datetime.utcnow().isoformat()
            logger.info("Status change ticket=%s: %s -> %s",
                        ticket_id, old_status, saved_status)

        save_ticket(
            conn, ticket_id, task_number_db or task_number or "", chat_id, user_id,
            message_id_db or 0, last_user_message_id or last_user_message_id_db or 0,
            saved_updated_at, int(saved_status), norm_comment or last_comment_db or "",
            notified_status, last_engineer_comment, last_notified_reminder, status_changed_at,
        )
        return True
    except requests.HTTPError as e:
        logger.exception("Ошибка добавления комментария (HTTPError) ticket=%s: %s", ticket_id, e)
        return False
    except Exception as e:
        logger.exception("Ошибка добавления комментария (other) ticket=%s: %s", ticket_id, e)
        return False

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def register_legal_entity(chat_id: int, chat_title: str, chat_description: Optional[str], inn: Optional[str] = None) -> Optional[str]:
    external_id = f"telegram_personal_{chat_id}" if chat_id > 0 else f"telegram_group_{chat_id}"
    try:
        existing_id = check_group_in_intradesk(external_id)
        if existing_id:
            return existing_id
    except Exception as e:
        logger.warning("Не удалось проверить группу в IntraDesk: %s", e)
    url = f"{INTRADESK_LEGAL_ENTITIES_URL}?ApiKey={INTRADESK_API_KEY}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}"}
    data = {"name": chat_title or f"TG {chat_id}", "contactPersonFirstName": "Клиент", "externalId": external_id}
    if inn:
        data["taxpayerNumber"] = inn
    try:
        r = requests.post(url, json=data, headers=headers, timeout=15)
        r.raise_for_status()
        jd = r.json()
        return str(jd if isinstance(jd, (int, str)) else jd.get("id"))
    except requests.RequestException as e:
        logger.error("Ошибка регистрации юр. лица %s: %s", chat_id, e)
        return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def register_legal_entity_user(conn: sqlite3.Connection, user_id: int, chat_id: int,
                               first_name: Optional[str], username: Optional[str], legal_entity_id: str) -> Optional[str]:
    row = conn.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?",
                       (user_id, chat_id)).fetchone()
    if row:
        return row["intradesk_user_id"]
    external_id = (f"telegram_user_{user_id}_group_{chat_id}" if chat_id < 0 else f"telegram_user_{user_id}_personal_{chat_id}")
    url = f"{INTRADESK_LEGAL_USERS_URL}?ApiKey={INTRADESK_API_KEY}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {INTRADESK_AUTH_TOKEN}"}
    data = {
        "firstName": first_name or f"ID_{user_id}",
        "userGroups": [{"id": int(legal_entity_id), "isDefault": True}],
        "externalId": external_id,
    }
    if username:
        data["telegramUsername"] = username
    try:
        r = requests.post(url, json=data, headers=headers, timeout=15)
        r.raise_for_status()
        jd = r.json()
        intradesk_user_id = str(jd if isinstance(jd, (int, str)) else jd.get("id"))
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO users (user_id, chat_id, intradesk_user_id, legal_entity_id, external_id) VALUES (?, ?, ?, ?, ?)",
                (user_id, chat_id, intradesk_user_id, legal_entity_id, external_id),
            )
            conn.commit()
        return intradesk_user_id
    except requests.RequestException as e:
        logger.error("Ошибка регистрации пользователя: %s", e)
        return None
