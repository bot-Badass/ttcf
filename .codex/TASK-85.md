# TASK-85 — Queue режим: список незаписаних скриптів і підказка "далі" після рендеру

## Задача

Оператор готує кілька скриптів заздалегідь, а озвучує їх пізніше в зручний час. Зараз він мусить пам'ятати slug і вводити `/myth <slug>` вручну. Потрібна команда `/queue` яка показує список готових до озвучки скриптів як кнопки в Telegram, автоматично встановлює канал і після рендеру пропонує наступний скрипт з черги.

---

## Що реалізувати

### `src/myth_queue.py` — новий модуль (доменна логіка черги)

```python
from __future__ import annotations

import re
from pathlib import Path

from src import config


def list_unvoiced() -> list[tuple[str, str]]:
    """Return (slug, channel_key) for every myth dir that has script.txt but no voiceover.wav."""
    result: list[tuple[str, str]] = []
    for myth_dir in sorted(config.MYTH_DATA_DIR.iterdir()):
        if not myth_dir.is_dir():
            continue
        if not (myth_dir / "script.txt").is_file():
            continue
        if (myth_dir / "voiceover.wav").is_file():
            continue
        slug = myth_dir.name
        result.append((slug, slug_to_channel(slug)))
    return result


def slug_to_channel(slug: str) -> str:
    """Detect channel key from slug prefix. finance_* → finance, else → law."""
    return "finance" if slug.startswith("finance_") else "law"


def slug_to_title(slug: str) -> str:
    """Extract first text line from script.txt as human-readable title (max 55 chars).
    Falls back to slug if script not found."""
    script_path = config.MYTH_DATA_DIR / slug / "script.txt"
    if not script_path.is_file():
        return slug
    for line in script_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("##") and not stripped.startswith("PARTS:") and not stripped.startswith("**"):
            # Strip **markers** for display
            clean = re.sub(r"\*\*(.+?)\*\*", r"\1", stripped)
            return clean[:55] + ("..." if len(clean) > 55 else "")
    return slug
```

---

### `src/telegram_bot.py` — нова команда `/queue`

Додати в блок обробки команд (після блоку `/myth`):

```python
if command == "/queue":
    from src.myth_queue import list_unvoiced, slug_to_title, slug_to_channel
    unvoiced = list_unvoiced()
    if not unvoiced:
        send_message(chat_id, "✅ Черга порожня — всі скрипти озвучено.", None)
        return
    channel_emoji = {"law": "⚖️", "finance": "💰"}
    rows = []
    for slug, channel_key in unvoiced:
        title = slug_to_title(slug)
        emoji = channel_emoji.get(channel_key, "📄")
        rows.append([{
            "text": f"{emoji} {title}",
            "callback_data": f"queue_select:{slug}",
        }])
    markup = {"inline_keyboard": rows}
    send_message(chat_id, f"🎙 Готові до озвучки ({len(unvoiced)}):", markup)
    return
```

---

### `src/telegram_bot.py` — callback `queue_select:{slug}`

Додати в блок обробки callback_data (перед або після `myth_render:`):

```python
if callback_data.startswith("queue_select:"):
    from src.myth_queue import slug_to_channel
    slug = callback_data.split(":", 1)[1]
    channel_key = slug_to_channel(slug)
    _active_channel[chat_id] = channel_key
    _save_active_channels(_active_channel)
    set_pending(chat_id, slug)
    script_path = config.MYTH_DATA_DIR / slug / "script.txt"
    channel_label = config.CHANNEL_PROFILES.get(channel_key, {}).get("label", channel_key)
    if script_path.exists():
        script_text = _parse_myth_script_text(script_path)
        send_message(
            chat_id,
            f"📺 Канал: *{channel_label}*\n\n📝 *Скрипт:*\n\n{script_text}\n\n🎙 Надішли голосове повідомлення.",
            None,
        )
    else:
        send_message(
            chat_id,
            f"📺 Канал: *{channel_label}*\n✅ Готовий записати *{slug}*\n\n🎙 Надішли голосове повідомлення.",
            None,
        )
    answer_callback(callback_id, None)
    return
```

---

### `src/telegram_bot.py` — підказка "далі" після успішного рендеру

У функції `_do_render`, одразу після блоку:
```python
send_message(
    chat_id,
    f"✅ Відео і metadata.csv скопійовані в:\n`{export_dir}`",
    None,
)
```

Додати:

```python
# Suggest next unvoiced script from queue
from src.myth_queue import list_unvoiced, slug_to_title, slug_to_channel
remaining = [s for s, _ in list_unvoiced() if s != slug]
if remaining:
    next_slug = remaining[0]
    next_title = slug_to_title(next_slug)
    markup = {"inline_keyboard": [[
        {"text": "🎙 Записати", "callback_data": f"queue_select:{next_slug}"},
        {"text": "⏭ Пропустити", "callback_data": "queue_show"},
    ]]}
    send_message(
        chat_id,
        f"Далі в черзі:\n*{next_title}*",
        markup,
    )
```

---

### `src/telegram_bot.py` — callback `queue_show`

Додати в блок callback_data:

```python
if callback_data == "queue_show":
    from src.myth_queue import list_unvoiced, slug_to_title, slug_to_channel
    unvoiced = list_unvoiced()
    if not unvoiced:
        send_message(chat_id, "✅ Черга порожня — всі скрипти озвучено.", None)
        answer_callback(callback_id, None)
        return
    channel_emoji = {"law": "⚖️", "finance": "💰"}
    rows = []
    for s, channel_key in unvoiced:
        title = slug_to_title(s)
        emoji = channel_emoji.get(channel_key, "📄")
        rows.append([{
            "text": f"{emoji} {title}",
            "callback_data": f"queue_select:{s}",
        }])
    markup = {"inline_keyboard": rows}
    send_message(chat_id, f"🎙 Залишилось в черзі ({len(unvoiced)}):", markup)
    answer_callback(callback_id, None)
    return
```

---

## Обмеження

- НЕ чіпати логіку `_handle_myth_voice`, `myth_render`, `myth_rerecord`
- НЕ змінювати `myth_session.py` — `set_pending` / `get_pending` використовуються як є
- `slug_to_channel` і `list_unvoiced` живуть тільки в `src/myth_queue.py`, не в `telegram_bot.py`
- Якщо черга порожня після рендеру — просто не показувати кнопку "далі" (без зайвих повідомлень)

---

## Тести

1. `make test` — всі тести зелені
2. Перевірити вручну: `/queue` → список показує тільки скрипти без `voiceover.wav`
3. Перевірити вручну: тапнути скрипт з черги → канал переключився автоматично → скрипт відображено → голосове записується
4. Перевірити вручну: після рендеру → з'являється підказка з наступним скриптом → кнопки "Записати" і "Пропустити" працюють
5. Перевірити вручну: `/queue` коли всі скрипти озвучені → "Черга порожня"
