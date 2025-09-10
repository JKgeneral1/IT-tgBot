# -*- coding: utf-8 -*-
import datetime
import sqlite3
from typing import Any, Optional, Tuple

from .logger import setup_logging
from .config import FINAL_STATUSES

logger = setup_logging()

def connect(db_file: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_file, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    with conn:
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS tickets
               (ticket_id TEXT PRIMARY KEY,
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
                status_changed_at TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS users
               (user_id INTEGER,
                chat_id INTEGER,
                intradesk_user_id TEXT,
                legal_entity_id TEXT,
                external_id TEXT,
                PRIMARY KEY (user_id, chat_id))""")
        c.execute("""CREATE TABLE IF NOT EXISTS groups
               (chat_id INTEGER PRIMARY KEY,
                legal_entity_id TEXT,
                external_id TEXT,
                welcomed INTEGER DEFAULT 0,
                group_default_user_id TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS user_comments
               (ticket_id TEXT,
                comment_text TEXT,
                PRIMARY KEY (ticket_id, comment_text))""")
        c.execute("""CREATE TABLE IF NOT EXISTS statuses
               (status_id INTEGER PRIMARY KEY,
                status_name TEXT,
                raw JSON,
                updated_at TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS threads
               (chat_id INTEGER,
                topic_id INTEGER,
                ticket_id TEXT,
                created_by INTEGER,
                created_at TEXT,
                PRIMARY KEY (chat_id, topic_id))""")
        conn.commit()
    logger.info("База данных инициализирована")

def save_ticket(conn: sqlite3.Connection, ticket_id: str, task_number: str, chat_id: int, user_id: int,
                message_id: int, last_user_message_id: int, last_updated: str, status: int,
                last_comment: str = "", notified_status: Optional[int] = None,
                last_engineer_comment: Optional[str] = None, last_notified_reminder: Optional[str] = None,
                status_changed_at: Optional[str] = None) -> None:
    with conn:
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO tickets
            (ticket_id, task_number, chat_id, user_id, message_id, last_user_message_id,
             last_updated, status, last_comment, notified_status, last_engineer_comment,
             last_notified_reminder, status_changed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticket_id, task_number, chat_id, user_id, message_id, last_user_message_id,
             last_updated, status, last_comment, notified_status, last_engineer_comment,
             last_notified_reminder, status_changed_at))
        conn.commit()
    logger.info("Сохранена заявка: %s (%s) status=%s", ticket_id, task_number, status)

def save_user_comment(conn: sqlite3.Connection, ticket_id: str, comment_text: str) -> None:
    with conn:
        conn.execute("INSERT OR IGNORE INTO user_comments (ticket_id, comment_text) VALUES (?, ?)",
                     (ticket_id, comment_text))
        conn.commit()

def clear_user_comments(conn: sqlite3.Connection, ticket_id: str) -> None:
    with conn:
        conn.execute("DELETE FROM user_comments WHERE ticket_id = ?", (ticket_id,))
        conn.commit()

def is_user_comment(conn: sqlite3.Connection, ticket_id: str, comment_text: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM user_comments WHERE ticket_id = ? AND comment_text = ?",
        (ticket_id, comment_text)
    ).fetchone()
    return row is not None

def get_ticket_info(conn: sqlite3.Connection, ticket_id: str) -> Tuple:
    row = conn.execute(
        """SELECT chat_id, user_id, message_id, last_user_message_id, last_updated,
                  status, last_comment, notified_status, last_engineer_comment,
                  last_notified_reminder, task_number
           FROM tickets WHERE ticket_id = ?""", (ticket_id,)
    ).fetchone()
    return (tuple(row) if row else (None,)*11)

def has_open_ticket(conn: sqlite3.Connection, user_id: int, chat_id: int) -> Optional[str]:
    rows = conn.execute("SELECT ticket_id, status FROM tickets WHERE user_id = ? AND chat_id = ?",
                        (user_id, chat_id)).fetchall()
    for r in rows:
        if r["status"] not in FINAL_STATUSES:
            return r["ticket_id"]
    return None

# threads
def bind_thread_ticket(conn: sqlite3.Connection, chat_id: int, topic_id: int, ticket_id: str, created_by: int) -> None:
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO threads (chat_id, topic_id, ticket_id, created_by, created_at) VALUES (?, ?, ?, ?, ?)",
            (chat_id, topic_id, ticket_id, created_by, datetime.datetime.utcnow().isoformat())
        )
        conn.commit()

def get_thread_ticket(conn: sqlite3.Connection, chat_id: int, topic_id: int) -> Optional[str]:
    row = conn.execute("SELECT ticket_id FROM threads WHERE chat_id = ? AND topic_id = ?",
                       (chat_id, topic_id)).fetchone()
    return row["ticket_id"] if row else None

def unbind_thread_ticket(conn: sqlite3.Connection, chat_id: int, topic_id: int) -> None:
    with conn:
        conn.execute("DELETE FROM threads WHERE chat_id = ? AND topic_id = ?", (chat_id, topic_id))
        conn.commit()

# groups & users
def is_group_welcomed(conn: sqlite3.Connection, chat_id: int) -> int:
    row = conn.execute("SELECT welcomed FROM groups WHERE chat_id = ?", (chat_id,)).fetchone()
    return row["welcomed"] if row else 0

def get_legal_entity_id(conn: sqlite3.Connection, chat_id: int) -> Optional[str]:
    row = conn.execute("SELECT legal_entity_id FROM groups WHERE chat_id = ?", (chat_id,)).fetchone()
    return row["legal_entity_id"] if row else None

def get_group_external_id(conn: sqlite3.Connection, chat_id: int) -> Optional[str]:
    row = conn.execute("SELECT external_id FROM groups WHERE chat_id = ?", (chat_id,)).fetchone()
    return row["external_id"] if row else None

def mark_group_welcomed(conn: sqlite3.Connection, chat_id: int, legal_entity_id: Optional[str], external_id: Optional[str]) -> None:
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO groups (chat_id, legal_entity_id, external_id, welcomed) VALUES (?, ?, ?, 1)",
            (chat_id, legal_entity_id, external_id)
        )
        conn.commit()

def get_group_default_user_id(conn: sqlite3.Connection, chat_id: int) -> Optional[str]:
    row = conn.execute("SELECT group_default_user_id FROM groups WHERE chat_id = ?", (chat_id,)).fetchone()
    return row["group_default_user_id"] if row else None

def set_group_default_user_id(conn: sqlite3.Connection, chat_id: int, intradesk_user_id: str) -> None:
    """
    Безопасно обновляет group_default_user_id:
    - если строка есть — UPDATE,
    - если нет — INSERT с минимальным набором полей.
    """
    with conn:
        cur = conn.cursor()
        cur.execute("UPDATE groups SET group_default_user_id = ? WHERE chat_id = ?", (intradesk_user_id, chat_id))
        if cur.rowcount == 0:
            # Строки ещё нет — создаём
            conn.execute(
                "INSERT INTO groups (chat_id, group_default_user_id, welcomed) VALUES (?, ?, 0)",
                (chat_id, intradesk_user_id)
            )
        conn.commit()
