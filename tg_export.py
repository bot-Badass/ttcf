#!/usr/bin/env python3
#"""Export a Telegram conversation to a plain-text file."""

import asyncio
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import User, Chat, Channel

API_ID = 36369759          # my.telegram.org → App api_id
API_HASH = "a8ca0287d63fa633d44d666fb2f6e80b"     # my.telegram.org → App api_hash
SESSION = "tg_session"     # ім'я файлу .session (зберігається локально)

# Що парсити: username, phone, або числовий ID чату
TARGET = "arrmaglobalservis"

# Скільки повідомлень (None = всі)
LIMIT = None

OUTPUT = Path("arrmaglobalservis_export.txt")


async def main() -> None:
    async with TelegramClient(SESSION, API_ID, API_HASH) as client:
        entity = await client.get_entity(TARGET)
        name = _entity_name(entity)
        print(f"Експортуємо: {name}")

        lines: list[str] = []
        count = 0

        async for msg in client.iter_messages(entity, limit=LIMIT):
            if msg.text:
                sender = await _sender_name(client, msg)
                ts = msg.date.strftime("%Y-%m-%d %H:%M")
                lines.append(f"[{ts}] {sender}: {msg.text}")
                count += 1
                if count % 500 == 0:
                    print(f"  {count} повідомлень...")

        # iter_messages повертає від нових до старих — перевернути
        lines.reverse()

        OUTPUT.write_text("\n".join(lines), encoding="utf-8")
        print(f"Готово: {count} повідомлень → {OUTPUT}")


def _entity_name(entity: object) -> str:
    if isinstance(entity, User):
        return f"{entity.first_name or ''} {entity.last_name or ''}".strip()
    if isinstance(entity, (Chat, Channel)):
        return entity.title
    return str(entity)


async def _sender_name(client: TelegramClient, msg: object) -> str:
    try:
        sender = await msg.get_sender()
        if isinstance(sender, User):
            return f"{sender.first_name or ''} {sender.last_name or ''}".strip() or "Unknown"
    except Exception:
        pass
    return "Unknown"


if __name__ == "__main__":
    asyncio.run(main())