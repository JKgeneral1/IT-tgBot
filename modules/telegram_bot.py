# -*- coding: utf-8 -*-
"""
Telegram bot bootstrap: creates DB, wires handlers, runs PTB webhook.

Важные моменты:
- job_queue callbacks оформлены как async-функции (без лямбд), чтобы PTB не пытался await-ить None.
- При старте выполняем init_db() и (по возможности) миграцию схемы.
"""

import datetime
from functools import partial

import pytz
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from .config import (
    TELEGRAM_TOKEN,
    DB_FILE,
    WH_LISTEN_HOST,
    WH_LISTEN_PORT,
    WH_PUBLIC_BASE,
    WH_PATH,
)
from .db import connect, init_db
from .logger import setup_logging, clear_logs
from .status_cache import fetch_and_cache_statuses
from .handlers import (
    start,
    create_ticket_handler,
    list_tickets,
    handle_message,
    handle_ticket_choice,
    handle_my_chat_member,
    greet_new_member,
    cmd_bind,
    cmd_ticket,
    cmd_unbind,
)

logger = setup_logging()
UTC = pytz.UTC


def _try_migrate_schema(conn):
    """Нежно добавляем отсутствующие колонки в старых БД (совместимость)."""
    try:
        conn.execute("ALTER TABLE groups ADD COLUMN group_default_user_id TEXT")
        conn.commit()
        logger.info("DB migrate: added groups.group_default_user_id")
    except Exception:
        # колонка уже есть — тихо уходим
        pass


def run() -> None:
    # --- DB bootstrap
    conn = connect(DB_FILE)
    init_db(conn)
    _try_migrate_schema(conn)

    # --- Стартовые операции (синхронно)
    try:
        fetch_and_cache_statuses(conn, force=True)
    except Exception:
        logger.exception("Не удалось получить статусы при старте")

    # --- PTB application
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Handlers
    app.add_handler(CommandHandler("start", partial(start, conn=conn)))
    app.add_handler(CommandHandler("new", partial(create_ticket_handler, conn=conn)))
    app.add_handler(CommandHandler("bind", partial(cmd_bind, conn=conn)))
    app.add_handler(CommandHandler("ticket", partial(cmd_ticket, conn=conn)))
    app.add_handler(CommandHandler("unbind", partial(cmd_unbind, conn=conn)))
    app.add_handler(CommandHandler("list", partial(list_tickets, conn=conn)))

    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, partial(handle_message, conn=conn))
    )
    app.add_handler(
        MessageHandler(filters.PHOTO | filters.Document.ALL | filters.VOICE, partial(handle_message, conn=conn))
    )

    app.add_handler(CallbackQueryHandler(partial(handle_ticket_choice, conn=conn), pattern=r"^(continue|new)_"))
    app.add_handler(ChatMemberHandler(partial(handle_my_chat_member, conn=conn)))
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, partial(greet_new_member, conn=conn)))

    # --- Jobs (важно: async-колбэки, без лямбд)
    async def _status_job(_ctx):
        try:
            fetch_and_cache_statuses(conn)
        except Exception:
            logger.exception("Job statuses update failed")

    async def _clear_job(_ctx):
        try:
            clear_logs()
        except Exception:
            # Не критично — просто молчим
            pass

    try:
        app.job_queue.run_repeating(_status_job, interval=3600, first=300)
    except Exception:
        logger.exception("Не удалось запланировать обновление статусов")

    try:
        app.job_queue.run_daily(_clear_job, time=datetime.time(hour=0, minute=0, tzinfo=UTC))
    except Exception:
        # ок, просто не будем чистить логи по расписанию
        pass

    # --- Webhook endpoint
    webhook_path = "/" + (WH_PATH or "tg/secret").strip("/")
    public_url = f"{(WH_PUBLIC_BASE or '').rstrip('/')}{webhook_path}" if WH_PUBLIC_BASE else ""
    logger.info(
        "Старт TG webhook на http://%s:%s%s (public %s)",
        WH_LISTEN_HOST,
        WH_LISTEN_PORT,
        webhook_path,
        public_url or "<empty>",
    )

    app.run_webhook(
        listen=WH_LISTEN_HOST,
        port=WH_LISTEN_PORT,
        webhook_url=public_url,          # может быть пустым — тогда бот сам зарегистрирует локальный хук
        url_path=webhook_path.lstrip("/"),
    )
