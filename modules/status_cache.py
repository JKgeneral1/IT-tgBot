# -*- coding: utf-8 -*-
"""
Статусы IntraDesk из хардкода (без сетевых запросов).
"""

import datetime
import json
import re
from typing import Any, Dict, Optional

import pytz

from .logger import get_logger

UTC = pytz.UTC
log = get_logger()

# хардкод по вашему инстансу
HARDCODED_STATUSES: Dict[int, str] = {
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

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS statuses(
    status_id   INTEGER PRIMARY KEY,
    status_name TEXT,
    raw         JSON,
    updated_at  TEXT
)
"""


def fetch_and_cache_statuses(conn, force: bool = False) -> None:
    try:
        with conn:
            conn.execute(TABLE_SQL)
            cnt = int(conn.execute("SELECT COUNT(1) FROM statuses").fetchone()[0] or 0)
            if cnt > 0 and not force:
                log.info("statuses: уже есть %d записей, пропускаю (offline)", cnt)
                return
            now = datetime.datetime.now(UTC).isoformat()
            for sid, name in HARDCODED_STATUSES.items():
                conn.execute(
                    "INSERT OR REPLACE INTO statuses(status_id, status_name, raw, updated_at) VALUES (?,?,?,?)",
                    (int(sid), name, json.dumps({"Id": sid, "Name": name}, ensure_ascii=False), now),
                )
            conn.commit()
        log.info("statuses: записан хардкод (%d шт.)", len(HARDCODED_STATUSES))
    except Exception:
        log.exception("statuses: ошибка при записи хардкода")


def get_status_name_by_id(conn, status_id: Optional[int]) -> Optional[str]:
    if status_id is None:
        return None
    try:
        row = conn.execute("SELECT status_name FROM statuses WHERE status_id = ?", (int(status_id),)).fetchone()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return HARDCODED_STATUSES.get(int(status_id))


def get_status_id_by_name(conn, name: str) -> Optional[int]:
    if not name:
        return None
    try:
        row = conn.execute("SELECT status_id FROM statuses WHERE lower(status_name)=?", (name.lower(),)).fetchone()
        if row:
            return int(row[0])
    except Exception:
        pass
    for k, v in HARDCODED_STATUSES.items():
        if v.lower() == name.lower():
            return int(k)
    return None


def parse_status_from_fields(fields_obj: Any) -> Optional[int]:
    """
    Универсальный парсер id статуса из блока Fields ответа IntraDesk.
    """
    if not fields_obj:
        return None

    import json as _json

    if isinstance(fields_obj, str):
        for try_unescape in (False, True):
            s = html_unescape_safe(fields_obj) if try_unescape else fields_obj
            try:
                fields_obj = _json.loads(s)
                break
            except Exception:
                pass
        if isinstance(fields_obj, str):
            m = re.search(r'"status"\s*:\s*(\d+)', fields_obj) or re.search(r'\bstatus\b[^0-9]*(\d+)', fields_obj)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return None
            m2 = re.search(r"\d+", fields_obj)
            if m2:
                try:
                    return int(m2.group(0))
                except Exception:
                    return None
            return None

    if not isinstance(fields_obj, dict):
        return None

    status_block = (
        fields_obj.get("status")
        or fields_obj.get("Status")
        or fields_obj.get("fields", {}).get("status")
        or None
    )
    if status_block is None:
        return None

    if isinstance(status_block, (int, float)) and not isinstance(status_block, bool):
        try:
            return int(status_block)
        except Exception:
            return None

    if isinstance(status_block, dict):
        for k in ("Id", "id", "Value", "value"):
            if k in status_block:
                try:
                    return int(status_block[k])
                except Exception:
                    pass
        for k in ("json", "raw", "data"):
            v = status_block.get(k)
            if isinstance(v, str):
                try:
                    jd = _json.loads(v)
                    for kk in ("Id", "id", "Value", "value"):
                        if kk in jd:
                            return int(jd[kk])
                except Exception:
                    pass

    if isinstance(status_block, str):
        for try_unescape in (False, True):
            s = html_unescape_safe(status_block) if try_unescape else status_block
            try:
                jd = _json.loads(s)
                if isinstance(jd, dict):
                    for k in ("Id", "id", "Value", "value"):
                        if k in jd:
                            return int(jd[k])
            except Exception:
                pass
        m = re.search(r"\d+", status_block)
        if m:
            try:
                return int(m.group(0))
            except Exception:
                pass

    return None


def html_unescape_safe(s: str) -> str:
    try:
        import html
        return html.unescape(s)
    except Exception:
        return s
