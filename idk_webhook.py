# -*- coding: utf-8 -*-
"""
idk_webhook.py — приёмник вебхуков от IntraDesk (FastAPI).
Парсит комментарии и статусы; шлёт сообщения в Telegram; хранит минимум в SQLite.

Изменение: отключены уведомления о смене статуса.
"""

import asyncio
import configparser
import datetime
import html
import json
import logging
import re
import sqlite3
from collections import deque
from hashlib import sha1
from typing import Any, Dict, List, Optional

import pytz
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from telegram import Bot
from telegram.error import RetryAfter, Forbidden

# ---- конфиг ----
config = configparser.ConfigParser()
config.read("config.ini")

TELEGRAM_TOKEN: str = config.get("Telegram", "token", fallback="")
IDK_SECRET_HEADER: str = config.get("IDK", "secret_header", fallback="x-api-key")
IDK_SECRET_VALUE: str = config.get("IDK", "secret_value", fallback="")
IDK_HOST: str = config.get("Web", "idk_host", fallback="0.0.0.0")
IDK_PORT: int = int(config.get("Web", "idk_port", fallback="8081"))
DB_FILE = config.get("App", "db_file", fallback="/data/tickets.db")
TG_LIMIT = int(config.get("App", "tg_limit", fallback="3500"))

# Можно оставить для внутреннего логирования/совместимости, но в Telegram не используем
STATUSES = {
    106939: "Открыта", 106941: "Переоткрыта", 106940: "Отложена", 106948: "Требует уточнения",
    106951: "В работе", 106946: "Выполнена", 106947: "ВыполнTest", 106943: "Проверена",
    106942: "ВыполнProd", 106945: "Сделка (заключен договор)", 106950: "Закрыта", 106949: "Отменена", 106944: "Отказ",
}
FINAL_STATUSES = {106950, 106949, 106946}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("idk_webhook")
UTC = pytz.UTC

# ---- DB ----
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    with conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS tickets(
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
        conn.execute(
            """CREATE TABLE IF NOT EXISTS user_comments(
                ticket_id TEXT,
                comment_text TEXT,
                PRIMARY KEY(ticket_id, comment_text)
            )"""
        )
        conn.commit()
    return conn

DB = get_db()

def get_ticket_row(ticket_id: str) -> Optional[sqlite3.Row]:
    return DB.execute(
        "SELECT ticket_id, task_number, chat_id, user_id, last_user_message_id, status FROM tickets WHERE ticket_id = ?",
        (ticket_id,),
    ).fetchone()

def clear_user_comments(ticket_id: str) -> None:
    with DB:
        DB.execute("DELETE FROM user_comments WHERE ticket_id = ?", (ticket_id,))

def user_comment_exists(ticket_id: str, text: str) -> bool:
    norm = _normalize_for_db(text)
    row = DB.execute("SELECT 1 FROM user_comments WHERE ticket_id = ? AND comment_text = ?", (ticket_id, norm)).fetchone()
    return row is not None

def save_user_comment_db(ticket_id: str, text: str) -> None:
    norm = _normalize_for_db(text)
    with DB:
        DB.execute("INSERT OR IGNORE INTO user_comments (ticket_id, comment_text) VALUES (?, ?)", (ticket_id, norm))

def update_ticket_status(ticket_id: str, new_status: Optional[int]) -> bool:
    row = DB.execute("SELECT status FROM tickets WHERE ticket_id = ?", (ticket_id,)).fetchone()
    if not row:
        return False
    old = row["status"]
    changed = (new_status is not None and old != new_status)
    with DB:
        DB.execute("UPDATE tickets SET status = COALESCE(?, status), last_updated = ? WHERE ticket_id = ?",
                   (new_status, datetime.datetime.now(UTC).isoformat(), ticket_id))
    if changed:
        log.info("Status changed for ticket %s: %s -> %s (%s)", ticket_id, old, new_status, STATUSES.get(new_status))
    return changed

# ---- utils ----
def _normalize_for_db(s: Optional[str]) -> str:
    if not s:
        return ""
    s = re.sub(r"[\u200B\u200C\u200D\uFE0E\uFE0F]", "", str(s))
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s

def clean_intradesk_html(s: str) -> str:
    if not s:
        return ""
    t = s
    if len(t) >= 2 and t[0] == '"' and t[-1] == '"':
        t = t[1:-1]
    t = re.sub(r"(?i)<br\s*/?>", "\n", t)
    t = re.sub(r"</?intradesk[-\w:]+[^>]*>", "", t)
    t = re.sub(r"<[^>]+>", "", t)
    try:
        t = html.unescape(t)
    except Exception:
        pass
    t = re.sub(r"\r\n?", "\n", t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t

def chunk_text(text: str, limit: int = TG_LIMIT) -> List[str]:
    if len(text) <= limit:
        return [text]
    parts: List[str] = []
    buf = ""
    for seg in re.split(r"(\n\n+|(?<=\.)\s)", text):
        if not seg:
            continue
        if len(buf) + len(seg) <= limit:
            buf += seg
        else:
            if buf:
                parts.append(buf)
            if len(seg) <= limit:
                buf = seg
            else:
                for i in range(0, len(seg), limit):
                    parts.append(seg[i:i+limit])
                buf = ""
    if buf:
        parts.append(buf)
    return parts

async def tg_send(bot: Bot, chat_id: int, text: str) -> None:
    """Отправляет обычные сообщения без reply_to."""
    pieces = chunk_text(text)
    total = len(pieces)
    for i, piece in enumerate(pieces, start=1):
        piece_to_send = piece if total == 1 else (piece if i == 1 else f"(продолжение {i}/{total})\n{piece}")
        try:
            await bot.send_message(chat_id, piece_to_send)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            await bot.send_message(chat_id, piece_to_send)
        except Forbidden:
            log.warning("TG: Forbidden %s", chat_id)
            break
        except Exception:
            log.exception("TG: send error")
            break

# ---- parse helpers ----
def try_parse_json_maybe_escaped(s: Optional[str]) -> Optional[Any]:
    if s is None:
        return None
    if not isinstance(s, str):
        return s
    try:
        return json.loads(s)
    except Exception:
        pass
    try:
        return json.loads(html.unescape(s))
    except Exception:
        pass
    try:
        return json.loads(s.encode("utf-8").decode("unicode_escape"))
    except Exception:
        pass
    return None

def extract_from_fields_events(payload: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    fields = payload.get("Fields") or payload.get("fields") or {}
    evs = fields.get("Events") or fields.get("events") or None
    if not evs:
        return out
    parsed = try_parse_json_maybe_escaped(evs)
    if parsed and isinstance(parsed, list):
        for ev in parsed:
            if isinstance(ev, dict):
                if str(ev.get("Block") or ev.get("block") or "").lower() == "comment":
                    nv = ev.get("NewValue") or ev.get("newValue") or ev.get("New")
                    if isinstance(nv, str) and nv.strip():
                        out.append(clean_intradesk_html(nv))
    return out

def extract_from_lifetime(payload: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    fields = payload.get("Fields") or payload.get("fields") or {}
    lf = fields.get("lifetime") or fields.get("Lifetime") or None
    parsed = try_parse_json_maybe_escaped(lf)
    if parsed and isinstance(parsed, dict):
        data = parsed.get("Data") or []
        for entry in data:
            events = (entry.get("events") or {}).get("Data") or []
            for ev in events:
                if str(ev.get("blockname") or ev.get("Block") or "").lower() == "comment":
                    text = ev.get("stringvalue") or ev.get("stringValue") or ev.get("NewValue")
                    if isinstance(text, str) and text.strip():
                        out.append(clean_intradesk_html(text))
    return out

def collect_comment_candidates(payload: Dict[str, Any]) -> List[str]:
    uniq = {}
    for t in (extract_from_fields_events(payload) + extract_from_lifetime(payload)):
        k = _normalize_for_db(t)
        if k and k not in uniq:
            uniq[k] = t
    # запасные поля
    for k in ("comment", "message", "engineer_text", "text"):
        if isinstance(payload.get(k), str) and payload[k].strip():
            t = clean_intradesk_html(payload[k])
            k2 = _normalize_for_db(t)
            if k2 and k2 not in uniq:
                uniq[k2] = t
    return list(uniq.values())

def pick_status(payload: Dict[str, Any]) -> Optional[int]:
    fields = payload.get("Fields") or payload.get("fields") or {}
    status_block = fields.get("status") or fields.get("Status") or None
    if isinstance(status_block, str):
        parsed = try_parse_json_maybe_escaped(status_block)
        if isinstance(parsed, dict):
            for k in ("Id", "id", "Value", "value"):
                if k in parsed:
                    try:
                        return int(parsed[k])
                    except Exception:
                        pass
    try:
        return int(status_block) if status_block is not None else None
    except Exception:
        return None

# ---- app ----
app = FastAPI(title="IDK Webhook")
BOT = Bot(token=TELEGRAM_TOKEN)
_seen_ids: deque[str] = deque(maxlen=5000)

def seen_event(eid: Optional[str]) -> bool:
    if not eid:
        return False
    if eid in _seen_ids:
        return True
    _seen_ids.append(eid)
    return False

@app.get("/healthz")
async def healthz():
    return PlainTextResponse("ok")

@app.post("/idk/webhook")
async def idk_webhook(request: Request):
    secret = request.headers.get(IDK_SECRET_HEADER)
    if not IDK_SECRET_VALUE or secret != IDK_SECRET_VALUE:
        raise HTTPException(status_code=403, detail="forbidden")

    raw = await request.body()
    digest = sha1(raw).hexdigest()
    if seen_event(digest):
        return JSONResponse({"ok": True, "duplicate": True})

    try:
        payload = json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        payload = await request.json()

    log.info("Webhook: %s", json.dumps(payload, ensure_ascii=False)[:1200])

    ticket_id = None
    for k in ("ticket_id", "taskId", "Id", "id", "TicketId"):
        if k in payload and payload[k] is not None:
            ticket_id = str(payload[k]); break
    if not ticket_id:
        raise HTTPException(status_code=400, detail="ticket_id missing")

    row = get_ticket_row(ticket_id)
    if not row:
        return JSONResponse({"ok": False, "error": "ticket not found in bot db"}, status_code=404)

    chat_id = int(row["chat_id"])
    task_number = row["task_number"]

    candidates = collect_comment_candidates(payload)
    chosen_comment = None
    if candidates:
        chosen_comment = max(candidates, key=lambda x: len(x or ""))
        if user_comment_exists(ticket_id, chosen_comment):
            chosen_comment = None  # эхо пользователя

    status = pick_status(payload)
    status_changed = update_ticket_status(ticket_id, status)

    # --- Отправка в TG ---
    try:
        # Больше не уведомляем о смене статуса.
        # Если пришёл комментарий инженера — отправляем только его текст.
        if chosen_comment:
            await tg_send(BOT, chat_id, f"{chosen_comment}")
    except Exception:
        log.exception("Ошибка отправки в Telegram")

    # Чистим кэш комментариев пользователя при финальных статусах
    if status is not None and status in FINAL_STATUSES:
        clear_user_comments(ticket_id)

    return JSONResponse({"ok": True})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("idk_webhook:app", host=IDK_HOST, port=IDK_PORT, reload=False)
