# -*- coding: utf-8 -*-
import html
import os
import re
import sys
from typing import Optional

LOCK_FILE = "/tmp/helptp_bot.lock"

def check_single_instance() -> None:
    if os.path.exists(LOCK_FILE):
        with open(LOCK_FILE, "r") as f:
            pid = f.read().strip()
        try:
            os.kill(int(pid), 0)
            print(f"Бот уже запущен с PID {pid}. Завершите его перед новым запуском.")
            sys.exit(1)
        except OSError:
            pass
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

def remove_lock_file() -> None:
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def escape_html(text: str) -> str:
    return html.escape(str(text))

def normalize_text(s: Optional[str]) -> str:
    """Чистим невидимые символы, схлопываем пробелы, обрезаем чрезмерные повторы."""
    if not s:
        return ""
    s = re.sub(r"[\u200B\u200C\u200D\uFE0E\uFE0F]", "", str(s))
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    # ВАЖНО: передаём строку третьим аргументом!
    s = re.sub(r"(.)\1{3,}", lambda m: m.group(1) * 3, s, flags=re.DOTALL)
    return s
