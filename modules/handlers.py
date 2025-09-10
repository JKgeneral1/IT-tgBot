# -*- coding: utf-8 -*-
import os
import re
from functools import partial
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from .config import TG_LIMIT
from .db import (
    bind_thread_ticket, clear_user_comments, get_group_default_user_id, get_legal_entity_id,
    get_thread_ticket, get_ticket_info, has_open_ticket, is_group_welcomed, mark_group_welcomed, save_ticket
)
from .intradesk_api import (
    add_comment_to_ticket, check_legal_entity_by_inn, create_ticket,
    ensure_group_default_user, register_legal_entity, register_legal_entity_user
)
from .logger import setup_logging
from .status_cache import get_status_name_by_id
from .telegram_utils import send_message

logger = setup_logging()

def _topic_id_from(update: Update) -> int:
    """
    Возвращает идентификатор темы (topic/thread) для супергрупп.
    Для ЛС и групп без форумов возвращает 0.
    Мы в проекте договорились хранить 'нет темы' как 0 (а не NULL).
    """
    return getattr(update.effective_message, "message_thread_id", 0) or 0

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    user = update.effective_user
    chat = update.effective_chat
    chat_id = chat.id
    message_id = update.effective_message.message_id

    if chat_id < 0:
        # Группа
        if not is_group_welcomed(conn, chat_id):
            full = await context.bot.get_chat(chat_id)
            legal_id = register_legal_entity(chat_id, full.title, getattr(full, "description", None))
            if legal_id:
                mark_group_welcomed(conn, chat_id, legal_id, f"telegram_group_{chat_id}")
                ensure_group_default_user(conn, chat_id, legal_id, full.title)
            else:
                await send_message(context, chat_id, "Ошибка регистрации группы.", message_id)
                return
        else:
            legal_id = get_legal_entity_id(conn, chat_id)
            if legal_id:
                ensure_group_default_user(conn, chat_id, legal_id, chat.title)

        kb = [["Создать заявку", "Открытые заявки"]]
        await send_message(context, chat_id, "Готово! Для каждой темы можно создать свою заявку.", message_id,
                           reply_markup={"keyboard": kb, "resize_keyboard": True})
    else:
        # Личные чаты — регистрация пользователя
        row = conn.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?", (user.id, chat_id)).fetchone()
        if row:
            kb = [["Создать заявку", "Открытые заявки"]]
            await send_message(context, chat_id, "Вы уже зарегистрированы! Выберите действие:", message_id,
                               reply_markup={"keyboard": kb, "resize_keyboard": True})
        else:
            context.user_data["awaiting_inn"] = True
            await send_message(context, chat_id, "Пожалуйста, введите ИНН вашей организации (10 или 12 цифр).", message_id)

async def create_ticket_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    user = update.effective_user
    chat = update.effective_chat
    chat_id = chat.id
    message_id = update.effective_message.message_id
    topic_id = _topic_id_from(update)

    row = conn.execute("SELECT intradesk_user_id, legal_entity_id FROM users WHERE user_id = ? AND chat_id = ?",
                       (user.id, chat_id)).fetchone()

    force_uid = None
    legal_id = get_legal_entity_id(conn, chat_id) if chat_id < 0 else None

    if chat_id < 0:
        # Группа: работаем через сервисного пользователя
        if not legal_id:
            await send_message(context, chat_id, "Ошибка: чат не зарегистрирован как юр. лицо.", message_id)
            return
        if not row:
            group_uid = ensure_group_default_user(conn, chat_id, legal_id, chat.title)
            if not group_uid:
                await send_message(context, chat_id, "Ошибка при подготовке групповой учётной записи.", message_id)
                return
            force_uid = group_uid
    else:
        # Личные: требуем регистрацию пользователя
        if not row:
            context.user_data["awaiting_inn"] = True
            await send_message(context, chat_id, "Пожалуйста, введите ИНН вашей организации (10 или 12 цифр):", message_id)
            return

    ticket_id, last_updated, status, result = create_ticket(
        conn, "Ожидание описания", "Ожидание описания", user.id, chat_id,
        chat.title if chat_id < 0 else None, force_intradesk_user_id=force_uid
    )
    if ticket_id:
        r = conn.execute("SELECT task_number FROM tickets WHERE ticket_id = ?", (ticket_id,)).fetchone()
        task_number = r["task_number"] if r else "Unknown"
        save_ticket(conn, ticket_id, task_number, chat_id, user.id, message_id, message_id, last_updated or "", status)

        if chat_id < 0:
            # Ключевой момент: для групп/тем создаём ЖЁСТКУЮ привязку темы к заявке
            bind_thread_ticket(conn, chat_id, topic_id, ticket_id, user.id)

        sent = await send_message(context, chat_id, f"Заявка #{task_number} создана. Опишите проблему.", message_id,
                                  reply_markup={"keyboard": [["Открытые заявки"]], "resize_keyboard": True})
        if sent:
            conn.execute("UPDATE tickets SET message_id = ? WHERE ticket_id = ?", (sent.message_id, ticket_id))
            conn.commit()

        # ВАЖНО: активную заявку в user_data ставим ТОЛЬКО в личных чатах.
        if chat_id > 0:
            context.user_data["active_ticket"] = ticket_id
    else:
        await send_message(context, chat_id, result, message_id)

async def list_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    """
    ЛС: показываем личные открытые заявки пользователя в этом чате.
    Группы: если есть текущая тема с привязкой — показываем именно её (чтобы не путать контекст).
    """
    chat = update.effective_chat
    chat_id = chat.id
    message_id = update.effective_message.message_id

    # Ветка для групп/тем — показываем привязанную к теме заявку, если есть
    if chat_id < 0:
        topic_id = _topic_id_from(update)
        t = get_thread_ticket(conn, chat_id, topic_id)
        if not t:
            await send_message(context, chat_id, "В этой теме заявка не привязана. Используйте «Создать заявку» или /bind.", message_id)
            return
        row = conn.execute("SELECT task_number, status FROM tickets WHERE ticket_id = ?", (t,)).fetchone()
        if not row:
            await send_message(context, chat_id, "Заявка привязана, но отсутствует в локальной БД (нужно обновить).", message_id)
            return
        st = get_status_name_by_id(conn, row["status"]) or row["status"]
        await send_message(context, chat_id, f"Эта тема связана с заявкой #{row['task_number']} (статус: {st}).", message_id)
        return

    # Личные чаты — как было
    user = update.effective_user
    rows = conn.execute(
        "SELECT ticket_id, task_number, status FROM tickets WHERE user_id = ? AND chat_id = ? AND status NOT IN (?, ?, ?)",
        (user.id, chat_id, 99220, 99219, 99216),
    ).fetchall()

    if not rows:
        await send_message(context, chat_id, "У вас нет открытых заявок.", message_id)
        return

    text = "Ваши открытые заявки:\n"
    keyboard = []
    for r in rows:
        status_text = get_status_name_by_id(conn, r["status"]) or "Неизвестный"
        num = r["task_number"] or "Unknown"
        text += f"• #{num} — {status_text}\n"
        keyboard.append([InlineKeyboardButton(f"Заявка #{num}", callback_data=f"continue_{r['ticket_id']}")])

    # ВАЖНО: не переписываем message_id для всех заявок одним значением.
    await send_message(context, chat_id, text, message_id, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    user = update.effective_user
    chat = update.effective_chat
    chat_id = chat.id
    message_id = update.effective_message.message_id
    topic_id = _topic_id_from(update)
    text = update.effective_message.text

    # Кнопки/команды
    if text == "Создать заявку" or (text and text.strip().lower() == "/new"):
        await create_ticket_handler(update, context, conn); return
    if text == "Открытые заявки" or (text and text.strip().lower() in ("/list", "/tickets")):
        await list_tickets(update, context, conn); return

    # Регистрация по ИНН — только для ЛС
    if chat_id > 0 and context.user_data.get("awaiting_inn"):
        inn = (text or "").strip()
        if not re.match(r"^\d{10}$|^\d{12}$", inn):
            await send_message(context, chat_id, "Введите корректный ИНН (10 или 12 цифр).", message_id); return
        legal_id = check_legal_entity_by_inn(inn)
        if not legal_id:
            await send_message(context, chat_id, f"Организация с ИНН {inn} не найдена в IntraDesk.", message_id); return
        uid = register_legal_entity_user(conn, user.id, chat_id, user.first_name, user.username, legal_id)
        if not uid:
            await send_message(context, chat_id, "Ошибка при регистрации. Попробуйте позже.", message_id); return
        context.user_data.pop("awaiting_inn", None)
        from .db import mark_group_welcomed
        mark_group_welcomed(conn, chat_id, legal_id, f"telegram_personal_{chat_id}")
        kb = [["Создать заявку", "Открытые заявки"]]
        await send_message(context, chat_id, "Регистрация завершена. Выберите действие:",
                           reply_markup={"keyboard": kb, "resize_keyboard": True})
        return

    # В ЛС проверяем, что пользователь зарегистрирован
    if chat_id > 0:
        row = conn.execute("SELECT intradesk_user_id FROM users WHERE user_id = ? AND chat_id = ?", (user.id, chat_id)).fetchone()
        if not row:
            await send_message(context, chat_id, "Пожалуйста, используйте /start для регистрации!", message_id); return

    # Парсинг вложений
    file_path = None
    if update.effective_message.photo:
        photo = await update.effective_message.photo[-1].get_file()
        file_path = f"temp_{user.id}_{message_id}.jpg"
        await photo.download_to_drive(file_path)
        text = update.effective_message.caption or "Фото от пользователя"
    elif update.effective_message.document:
        doc = await update.effective_message.document.get_file()
        safe_name = update.effective_message.document.file_name or f"file_{message_id}"
        file_path = f"temp_{user.id}_{message_id}_{safe_name}"
        await doc.download_to_drive(file_path)
        text = update.effective_message.caption or f"Файл от пользователя: {safe_name}"
    elif update.effective_message.voice:
        v = await update.effective_message.voice.get_file()
        file_path = f"temp_{user.id}_{message_id}.ogg"
        await v.download_to_drive(file_path)
        text = update.effective_message.caption or "Голосовое сообщение от пользователя"
    elif not text:
        text = "Сообщение без текста"

    # Определяем заявку по контексту
    if chat_id < 0:
        # ГРУППЫ/ТЕМЫ: только привязка threads, НЕ используем user_data.active_ticket
        mapped_ticket = get_thread_ticket(conn, chat_id, topic_id)
        if not mapped_ticket:
            if text and not text.startswith("/"):
                kb = [["Создать заявку", "Открытые заявки"]]
                await send_message(
                    context, chat_id,
                    "Для этой темы нет заявки. Нажмите «Создать заявку».",
                    message_id, reply_markup={"keyboard": kb, "resize_keyboard": True}
                )
            if file_path and os.path.exists(file_path):
                try: os.remove(file_path)
                except Exception: pass
            return
        ticket_id = mapped_ticket
        author = user.full_name or user.username or str(user.id)
        text = f"[{author}] {text}" if text else f"[{author}]"
    else:
        # ЛИЧНЫЕ: сначала явная активная (если есть), затем любая открытая в этом чате
        ticket_id = context.user_data.get("active_ticket") or has_open_ticket(conn, user.id, chat_id)

    # Добавляем комментарий (и файл при наличии)
    if ticket_id:
        if add_comment_to_ticket(conn, ticket_id, user.id, chat_id, text, file_path, message_id):
            # Подчистим служебное сообщение для ЭТОЙ заявки (если оно было)
            row = conn.execute("SELECT message_id FROM tickets WHERE ticket_id = ?", (ticket_id,)).fetchone()
            ticket_message_id = row["message_id"] if row else None
            if ticket_message_id:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=ticket_message_id)
                    conn.execute("UPDATE tickets SET message_id = 0 WHERE ticket_id = ?", (ticket_id,))
                    conn.commit()
                except Exception as e:
                    logger.warning("Не удалось удалить служебное сообщение %s: %s", ticket_message_id, e)
    else:
        if chat_id > 0:
            await send_message(context, chat_id, "Пожалуйста, нажмите «Создать заявку».", message_id)

    if file_path and os.path.exists(file_path):
        try: os.remove(file_path)
        except Exception: pass

async def handle_ticket_choice(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    """
    Колбэк из «Открытые заявки».
    ВАЖНО:
    - в группах/темах — привязываем текущую ТЕМУ к выбранной заявке (bind_thread_ticket),
      НЕ трогаем context.user_data;
    - в ЛС — ставим active_ticket в user_data.
    """
    query = update.callback_query
    await query.answer()
    user = query.from_user
    chat = query.message.chat
    chat_id = chat.id
    topic_id = getattr(query.message, "message_thread_id", 0) or 0

    action, *params = query.data.split("_")
    if action == "continue":
        ticket_id = params[0]
        row = conn.execute("SELECT task_number, message_id FROM tickets WHERE ticket_id = ?", (ticket_id,)).fetchone()
        task_number = row["task_number"] if row else "Unknown"
        ticket_message_id = row["message_id"] if row else None

        if chat_id < 0:
            # Привязываем текущую тему к выбранной заявке
            bind_thread_ticket(conn, chat_id, topic_id, ticket_id, user.id)
        else:
            # Личные — помним активную
            context.user_data["active_ticket"] = ticket_id

        await query.edit_message_text(f"Выбрана заявка #{task_number}. Добавьте комментарий.")

        if ticket_message_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=ticket_message_id)
                conn.execute("UPDATE tickets SET message_id = 0 WHERE ticket_id = ?", (ticket_id,))
                conn.commit()
            except Exception as e:
                logger.warning("Не удалось удалить сообщение %s: %s", ticket_message_id, e)

    elif action == "new":
        user_id, chat_id2 = map(int, params)
        if user_id != user.id:
            await query.edit_message_text("Нельзя создавать заявку от имени другого пользователя.")
            return

        force_uid = None
        if chat_id2 < 0:
            legal_id = get_legal_entity_id(conn, chat_id2)
            force_uid = ensure_group_default_user(conn, chat_id2, legal_id, query.message.chat.title) if legal_id else None

        ticket_id, last_updated, status, result = create_ticket(
            conn, "Ожидание описания", "Ожидание описания", user.id, chat_id2,
            query.message.chat.title if chat_id2 < 0 else None, force_intradesk_user_id=force_uid
        )
        if ticket_id:
            row = conn.execute("SELECT task_number FROM tickets WHERE ticket_id = ?", (ticket_id,)).fetchone()
            task_number = row["task_number"] if row else "Unknown"
            save_ticket(conn, ticket_id, task_number, chat_id2, user.id, query.message.message_id, query.message.message_id, last_updated or "", status)

            # Привязка темы для групп
            if chat_id2 < 0:
                topic_id2 = getattr(query.message, "message_thread_id", 0) or 0
                bind_thread_ticket(conn, chat_id2, topic_id2, ticket_id, user.id)
            else:
                context.user_data["active_ticket"] = ticket_id

            await query.edit_message_text(f"Заявка #{task_number} создана. Опишите проблему.")
        else:
            await query.edit_message_text(result)

async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    chat = update.my_chat_member.chat
    if chat.type in ("group", "supergroup") and update.my_chat_member.new_chat_member.status == "member" and not is_group_welcomed(conn, chat.id):
        full = await context.bot.get_chat(chat.id)
        legal_id = register_legal_entity(chat.id, full.title, getattr(full, "description", None))
        if legal_id:
            mark_group_welcomed(conn, chat.id, legal_id, f"telegram_group_{chat.id}")
            ensure_group_default_user(conn, chat.id, legal_id, full.title)
            kb = [["Создать заявку", "Открытые заявки"]]
            await send_message(context, chat.id, "Бот успешно добавлен в группу! Для каждой темы можно создать свою заявку.",
                               reply_markup={"keyboard": kb, "resize_keyboard": True})
        else:
            await send_message(context, chat.id, "Ошибка регистрации группы.")

async def greet_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id
    legal_id = get_legal_entity_id(conn, chat_id)
    if not legal_id:
        return
    ensure_group_default_user(conn, chat_id, legal_id, None)
    kb = [["Создать заявку", "Открытые заявки"]]
    for m in update.effective_message.new_chat_members:
        if m.id != context.bot.id and not m.is_bot:
            await send_message(context, chat_id, "Добро пожаловать! Я бот техподдержки. В этой теме можно создать заявку.",
                               message_id, reply_markup={"keyboard": kb, "resize_keyboard": True})

async def cmd_bind(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    chat_id = update.effective_chat.id
    if chat_id > 0:
        await send_message(context, chat_id, "Команда доступна только в группах."); return
    topic_id = _topic_id_from(update)
    args = (update.effective_message.text or "").split()
    if len(args) < 2:
        await send_message(context, chat_id, "Используйте: /bind <номер_заявки> (например, /bind 287)"); return
    number = args[1]
    row = conn.execute("SELECT ticket_id FROM tickets WHERE task_number = ? AND chat_id = ?", (number, chat_id)).fetchone()
    if not row:
        await send_message(context, chat_id, f"Заявка #{number} не найдена в локальной БД. Создайте/откройте её через бота.",
                           update.effective_message.message_id)
        return
    bind_thread_ticket(conn, chat_id, topic_id, row["ticket_id"], update.effective_user.id)
    await send_message(context, chat_id, f"Тема привязана к заявке #{number}. Все сообщения в этой теме — комментарии.")

async def cmd_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    chat_id = update.effective_chat.id
    topic_id = _topic_id_from(update)
    if chat_id < 0:
        t = get_thread_ticket(conn, chat_id, topic_id)
        if not t:
            await send_message(context, chat_id, "В этой теме заявка не привязана. Используйте «Создать заявку» или /bind."); return
        row = conn.execute("SELECT task_number, status FROM tickets WHERE ticket_id = ?", (t,)).fetchone()
        if not row:
            await send_message(context, chat_id, "Заявка привязана, но отсутствует в локальной БД (нужно обновить)."); return
        st = get_status_name_by_id(conn, row["status"]) or row["status"]
        await send_message(context, chat_id, f"Эта тема связана с заявкой #{row['task_number']} (статус: {st}).")
    else:
        await send_message(context, chat_id, "В личных чатах используйте «Открытые заявки».")

async def cmd_unbind(update: Update, context: ContextTypes.DEFAULT_TYPE, conn):
    chat_id = update.effective_chat.id
    if chat_id > 0:
        await send_message(context, chat_id, "Команда доступна только в группах."); return
    topic_id = _topic_id_from(update)
    from .db import unbind_thread_ticket
    unbind_thread_ticket(conn, chat_id, topic_id)
    await send_message(context, chat_id, "Привязка этой темы к заявке снята.")
