# -*- coding: utf-8 -*-
import asyncio
import logging
from typing import Any, Optional

from telegram.error import Forbidden, RetryAfter, BadRequest, NetworkError
from .logger import setup_logging

log = setup_logging()

async def send_message(
    context,
    chat_id: int,
    text: str,
    reply_to_message_id: Optional[int] = None,
    parse_mode: str = "HTML",
    reply_markup: Optional[Any] = None,
) -> Any:
    def _normalize_markup(markup: Any):
        if markup is None:
            return None
        if hasattr(markup, "to_dict"):
            try:
                return markup.to_dict()
            except Exception:
                pass
        try:
            from telegram import KeyboardButton, ReplyKeyboardMarkup
            if isinstance(markup, dict) and "keyboard" in markup:
                kb = []
                for row in markup.get("keyboard", []):
                    norm_row = []
                    for btn in row:
                        if isinstance(btn, KeyboardButton):
                            norm_row.append(btn.text)
                        elif isinstance(btn, dict) and "text" in btn:
                            norm_row.append(str(btn["text"]))
                        else:
                            norm_row.append(str(btn))
                    kb.append(norm_row)
                return ReplyKeyboardMarkup(kb, resize_keyboard=True).to_dict()
        except Exception:
            pass
        return markup

    safe_markup = _normalize_markup(reply_markup)
    try:
        if reply_to_message_id and chat_id > 0:
            return await context.bot.send_message(
                chat_id=chat_id, text=text, parse_mode=parse_mode,
                reply_to_message_id=reply_to_message_id, reply_markup=safe_markup
            )
        else:
            return await context.bot.send_message(
                chat_id=chat_id, text=text, parse_mode=parse_mode, reply_markup=safe_markup
            )
    except Forbidden:
        log.warning("Бот исключен из чата %s", chat_id)
    except RetryAfter as e:
        log.warning("Too Many Requests: пауза %s сек", e.retry_after)
        await asyncio.sleep(e.retry_after)
        return await send_message(context, chat_id, text, reply_to_message_id, parse_mode, reply_markup)
    except (BadRequest, NetworkError) as e:
        log.error("Ошибка отправки в чат %s: %s", chat_id, e)
    except Exception as e:
        log.error("Ошибка отправки сообщения: %s", e)
    return None
