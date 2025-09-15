# -*- coding: utf-8 -*-
"""
idk_webhook.py — приёмник вебхуков от IntraDesk (FastAPI).
Парсит комментарии и статусы; шлёт сообщения в Telegram; хранит минимум в SQLite.

Изменение: нет фоновых уведомлений о смене статуса — всё через вебхуки.
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
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import RetryAfter, Forbidden
from difflib import SequenceMatcher

# ---- конфиг ----
config = configparser.ConfigParser()
config.read("config.ini")

TELEGRAM_TOKEN: str = config.get("Telegram", "token", fallback="")
IDK_SECRET_HEADER: str = config.get("IDK", "secret_header", fallback="x-api-key")
IDK_SECRET_VALUE: str = config.get("IDK", "secret_value", fallback="")
IDK_HOST: str = config.get("Web", "idk_host", fallback="0.0.0.0")
IDK_PORT: int = int(config.get("Web", "idk_port", fallback="8081"))
DB_FILE: str = config.get("App", "db_file", fallback="/data/tickets.db")
TG_LIMIT: int = int(config.get("App", "tg_limit", fallback="3500"))

# Карта статусов (для логов)
STATUSES: Dict[int, str] = {
    106939: "Открыта",
    106941: "Переоткрыта",
    106940: "Отложена",
    106948: "Требует уточнения",
    106951: "В работе",
    106946: "Выполнена",
    106947: "ВыполнTest",
    106943: "Проверена",
    106942: "ВыполнProd",
    106945: "Сделка (заключен договор)",
    106950: "Закрыта",
    106949: "Отменена",
    106944: "Отказ",
}
FINAL_STATUSES: set[int] = {106950, 106949, 106946}
# В каких финальных статусах показываем запрос оценки
RATING_FINAL_STATUSES: set[int] = {106950, 106946}
# Статусы, при которых просим пользователя ответить (как раньше с 99218)
NOTIFY_STATUSES: set[int] = {
    int(x) for x in re.split(r"[,\s]+", config.get("App", "notify_statuses", fallback="106948").strip()) if x
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("idk_webhook")
UTC = pytz.UTC

# ---- DB ----
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets(
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
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_comments(
                ticket_id TEXT,
                comment_text TEXT,
                PRIMARY KEY(ticket_id, comment_text)
            )
            """
        )
        conn.commit()
    return conn


DB = get_db()


def get_ticket_row(ticket_id: str) -> Optional[sqlite3.Row]:
    return DB.execute(
        """
        SELECT ticket_id, task_number, chat_id, user_id, last_user_message_id, status
        FROM tickets
        WHERE ticket_id = ?
        """,
        (ticket_id,),
    ).fetchone()


def clear_user_comments(ticket_id: str) -> None:
    with DB:
        DB.execute("DELETE FROM user_comments WHERE ticket_id = ?", (ticket_id,))


# ---- анти-эхо ----
def _normalize_for_db(s: Optional[str]) -> str:
    """Мягкая нормализация: убираем VS/ZWJ, схлопываем пробелы, приводим к нижнему регистру."""
    if not s:
        return ""
    s = str(s)
    s = re.sub(r"[\u200B\u200C\u200D\uFE0E\uFE0F]", "", s)  # скрытые селекторы/ZWJ
    s = re.sub(r"\r\n?", "\n", s)
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s.lower()


def _normalize_strict(s: Optional[str]) -> str:
    """Строгая нормализация: оставляем только буквы и цифры (для substring/ratio-сравнений)."""
    soft = _normalize_for_db(s)
    return re.sub(r"[\W_]+", "", soft, flags=re.UNICODE)


def user_comment_exists(ticket_id: str, text: str) -> bool:
    """
    Анти-эхо: считаем дублирующимся, если:
    - мягко-нормализованные строки равны, ИЛИ
    - одна из строго-нормализованных строк является подстрокой другой (длина >= 24), ИЛИ
    - схожесть по SequenceMatcher >= 0.88 при длине >= 24 символов.
    """
    if not text:
        return False

    e_soft = _normalize_for_db(text)
    e_strict = _normalize_strict(text)

    rows = DB.execute(
        "SELECT comment_text FROM user_comments WHERE ticket_id = ?",
        (ticket_id,),
    ).fetchall()

    for r in rows or []:
        u_soft = _normalize_for_db(r["comment_text"])
        if not u_soft:
            continue
        if u_soft == e_soft:
            return True

        u_strict = _normalize_strict(u_soft)

        # Подстрока (для длинных сообщений/много строк)
        if len(u_strict) >= 24 and (u_strict in e_strict or e_strict in u_strict):
            return True

        # Похожесть (когда инженер добавил/удалил немного)
        if len(u_soft) >= 24 and len(e_soft) >= 24:
            if SequenceMatcher(None, u_soft, e_soft).ratio() >= 0.88:
                return True

    return False


def save_user_comment_db(ticket_id: str, text: str) -> None:
    norm = _normalize_for_db(text)
    with DB:
        DB.execute(
            "INSERT OR IGNORE INTO user_comments (ticket_id, comment_text) VALUES (?, ?)",
            (ticket_id, norm),
        )


def update_ticket_status(ticket_id: str, new_status: Optional[int]) -> bool:
    row = DB.execute(
        "SELECT status FROM tickets WHERE ticket_id = ?",
        (ticket_id,),
    ).fetchone()
    if not row:
        return False
    old = row["status"]
    changed = new_status is not None and old != new_status
    with DB:
        DB.execute(
            "UPDATE tickets SET status = COALESCE(?, status), last_updated = ? WHERE ticket_id = ?",
            (new_status, datetime.datetime.now(UTC).isoformat(), ticket_id),
        )
    if changed:
        log.info(
            "Status changed for ticket %s: %s -> %s (%s)",
            ticket_id, old, new_status, STATUSES.get(new_status),
        )
    return changed


def _reply_kwargs(chat_id: int, reply_to_message_id: Optional[int]) -> Dict[str, int]:
    """
    Поведение как в helptp.py: reply только в группах (chat_id < 0). В личке — без reply_to.
    """
    if chat_id < 0 and reply_to_message_id:
        return {"reply_to_message_id": int(reply_to_message_id)}
    return {}


# ---- utils ----
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
                    parts.append(seg[i : i + limit])
                buf = ""
    if buf:
        parts.append(buf)
    return parts


async def tg_send(
    bot: Bot,
    chat_id: int,
    text: str,
    reply_to_message_id: Optional[int] = None,
) -> None:
    """Отправляет текст. В группах первый кусок отправляется reply на сообщение пользователя."""
    pieces = chunk_text(text)
    total = len(pieces)
    for i, piece in enumerate(pieces, start=1):
        piece_to_send = piece if total == 1 else (piece if i == 1 else f"(продолжение {i}/{total})\n{piece}")
        kwargs = {}
        if i == 1:
            kwargs.update(_reply_kwargs(chat_id, reply_to_message_id))
        try:
            await bot.send_message(chat_id, piece_to_send, **kwargs)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            await bot.send_message(chat_id, piece_to_send, **kwargs)
        except Forbidden:
            log.warning("TG: Forbidden %s", chat_id)
            break
        except Exception:
            log.exception("TG: send error")
            break


async def send_rating_prompt(
    bot: Bot,
    chat_id: int,
    ticket_id: str,
    task_number: Optional[str],
    tg_user_id: int,
    reply_to_message_id: Optional[int] = None,
) -> Optional[int]:
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(str(i), callback_data=f"rate_{ticket_id}_{tg_user_id}_{i}") for i in range(1, 6)]
    ])
    text = f"Заявка #{task_number or '—'} выполнена/закрыта. Пожалуйста, оцените качество:"
    try:
        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb,
            **_reply_kwargs(chat_id, reply_to_message_id),
        )
        return msg.message_id
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        try:
            msg = await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=kb,
                **_reply_kwargs(chat_id, reply_to_message_id),
            )
            return msg.message_id
        except Exception:
            log.exception("TG: send rating retry error")
            return None
    except Forbidden:
        log.warning("TG: Forbidden %s (rating)", chat_id)
        return None
    except Exception:
        log.exception("TG: send rating error")
        return None


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
    uniq: Dict[str, str] = {}
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
    # авторизация по секрету
    secret = request.headers.get(IDK_SECRET_HEADER)
    if not IDK_SECRET_VALUE or secret != IDK_SECRET_VALUE:
        raise HTTPException(status_code=403, detail="forbidden")

    # защита от дубликатов по sha1 сырых данных
    raw = await request.body()
    digest = sha1(raw).hexdigest()
    if seen_event(digest):
        return JSONResponse({"ok": True, "duplicate": True})

    # парсинг JSON
    try:
        payload = json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        payload = await request.json()

    log.info("Webhook: %s", json.dumps(payload, ensure_ascii=False)[:1200])

    # определяем ticket_id
    ticket_id: Optional[str] = None
    for k in ("ticket_id", "taskId", "Id", "id", "TicketId"):
        if k in payload and payload[k] is not None:
            ticket_id = str(payload[k])
            break
    if not ticket_id:
        raise HTTPException(status_code=400, detail="ticket_id missing")

    # достаём карточку из SQLite
    row = get_ticket_row(ticket_id)
    if not row:
        return JSONResponse({"ok": False, "error": "ticket not found in bot db"}, status_code=404)

    chat_id = int(row["chat_id"])
    task_number = row["task_number"]
    last_uid = int(row["last_user_message_id"] or 0)
    reply_to_id = last_uid if last_uid > 0 else None

    # собираем возможные комментарии инженера
    candidates = collect_comment_candidates(payload)
    chosen_comment: Optional[str] = None
    if candidates:
        chosen_comment = max(candidates, key=lambda x: len(x or ""))
        if user_comment_exists(ticket_id, chosen_comment):
            chosen_comment = None  # эхо пользователя

    # статус
    status = pick_status(payload)
    status_changed = update_ticket_status(ticket_id, status)

    # 1) Если есть комментарий инженера — отправляем ЕГО ПЕРВЫМ (reply в группах)
    if chosen_comment:
        await tg_send(BOT, chat_id, chosen_comment, reply_to_message_id=reply_to_id)

    # 2) Если статус стал "требует уточнения" — отправляем просьбу ответить (однократно)
    if status_changed and status in NOTIFY_STATUSES:
        notified_val = DB.execute(
            "SELECT notified_status FROM tickets WHERE ticket_id = ?",
            (ticket_id,),
        ).fetchone()
        already_notified = bool(
            notified_val and notified_val[0] is not None and int(notified_val[0]) == int(status)
        )

        if not already_notified:
            notify_text = (
                f"Заявка #{task_number or '—'} требует вашего ответа — добавьте комментарий "
                f"или, если заявка уже не актуальна, мы её закроем!"
            )
            await tg_send(BOT, chat_id, notify_text, reply_to_message_id=reply_to_id)
            now_iso = datetime.datetime.now(UTC).isoformat()
            with DB:
                DB.execute(
                    "UPDATE tickets SET notified_status = ?, status_changed_at = ? WHERE ticket_id = ?",
                    (int(status), now_iso, ticket_id),
                )

    # 3) Если статус финальный — отправляем опрос оценки (после комментария/уведомления)
    if status_changed and status in RATING_FINAL_STATUSES:
        try:
            owner_user_id = int(row["user_id"]) if row["user_id"] is not None else None
        except Exception:
            owner_user_id = None

        if owner_user_id:
            message_id = await send_rating_prompt(
                BOT, chat_id, ticket_id, task_number, owner_user_id, reply_to_message_id=reply_to_id
            )
            if message_id:
                with DB:
                    DB.execute(
                        "UPDATE tickets SET message_id = ? WHERE ticket_id = ?",
                        (message_id, ticket_id),
                    )

    # Чистим кэш пользовательских комментов при финальных статусах
    if status is not None and status in FINAL_STATUSES:
        clear_user_comments(ticket_id)

    return JSONResponse({"ok": True})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("idk_webhook:app", host=IDK_HOST, port=IDK_PORT, reload=False)
