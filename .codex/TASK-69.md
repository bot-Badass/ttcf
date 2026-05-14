# TASK-69 — Команда /myth в Telegram-боті для запису голосу

## Задача

Оператор хоче записати голос для myth-bust відео через Telegram-бот, але myth-pipeline ще не інтегрований в бот повністю. Потрібна мінімальна команда `/myth` яка:
1. Приймає голосове повідомлення
2. Зберігає `voiceover.wav` у відому папку
3. Відповідає шляхом до файлу щоб оператор міг передати його в `myth_render.py`

Ніяких рендерів, жодної логіки серій — тільки зберегти аудіо.

---

## Що реалізувати

### 1. Стан очікування голосу — `src/myth_session.py` (новий файл)

Простий JSON-стейт: зберігає `chat_id` і `slug` поточної активної myth-сесії.

```python
from __future__ import annotations
from pathlib import Path
from src import config

MYTH_SESSION_PATH: Path = config.DATA_DIR / "manifests" / "myth_pending.json"

def set_pending(chat_id: str, slug: str) -> None:
    """Mark that chat_id is waiting to send myth voice."""

def get_pending(chat_id: str) -> str | None:
    """Return slug if chat_id has pending myth session, else None."""

def clear_pending(chat_id: str) -> None:
    """Remove pending session for chat_id."""
```

Зберігати як `{"<chat_id>": "<slug>"}` — простий dict у JSON.

---

### 2. Команда `/myth` — в `src/telegram_bot.py`

В `_handle_command_message`, після існуючих команд додати:

```python
if command == "/myth":
    # Розпарсити slug з тексту команди: "/myth mfo-court" → slug = "mfo-court"
    # Якщо slug не переданий — відповісти підказкою
    parts = str(message_payload.get("text", "")).strip().split(maxsplit=1)
    slug = parts[1].strip() if len(parts) > 1 else ""
    if not slug:
        send_message(chat_id, "Використання: /myth <назва>\nПриклад: /myth mfo-court", None)
        return
    set_pending(chat_id, slug)
    send_message(chat_id, f"✅ Готовий записати голос для *{slug}*\n\nНадішли голосове повідомлення.", None)
    return
```

---

### 3. Перехоплення голосу в myth-режимі — в `_handle_reply_message`

На початку `_handle_reply_message`, **до** існуючого блоку `if "voice" in message_payload`, додати перевірку:

```python
if "voice" in message_payload:
    slug = get_pending(chat_id)
    if slug:
        _handle_myth_voice(
            message_payload=message_payload,
            slug=slug,
            send_message=send_message,
            download_file=download_file,
        )
        return
    # ... існуючий _handle_voice_reply(...)
```

---

### 4. Функція `_handle_myth_voice`

```python
def _handle_myth_voice(
    *,
    message_payload: Mapping[str, Any],
    slug: str,
    send_message: SendMessageBoundary,
    download_file: DownloadFileBoundary,
) -> None:
    chat_id = _require_chat_id(message_payload)
    voice_payload = message_payload.get("voice") or message_payload.get("audio")
    file_id = voice_payload.get("file_id") if isinstance(voice_payload, Mapping) else None
    if not file_id:
        send_message(chat_id, "❌ Не вдалось отримати файл.", None)
        return

    raw_bytes = download_file(str(file_id))

    out_dir = config.DATA_DIR / "myth" / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    wav_path = out_dir / "voiceover.wav"

    # Конвертація через ffmpeg (ogg → wav), так само як це робить advice_pipeline
    import subprocess, tempfile
    with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
        tmp.write(raw_bytes)
        tmp_path = tmp.name

    subprocess.run(
        [config.FFMPEG_BIN, "-y", "-i", tmp_path, "-ar", "22050", "-ac", "1", str(wav_path)],
        check=True, capture_output=True,
    )
    Path(tmp_path).unlink(missing_ok=True)

    clear_pending(chat_id)

    send_message(
        chat_id,
        f"✅ Голос збережено:\n`{wav_path}`\n\nЗапуск рендеру:\n"
        f"`python myth_render.py --script ... --audio {wav_path} --channel law --output /tmp/{slug}.mp4`",
        None,
    )
```

---

### 5. Конфіг

В `src/config.py` додати одну константу:

```python
MYTH_DATA_DIR: Final[Path] = DATA_DIR / "myth"
```

---

## Обмеження

- НЕ чіпати advice-pipeline, render.py
- НЕ інтегрувати автоматичний запуск рендеру — тільки зберегти аудіо
- Голос може надійти **без reply** (просто пряме повідомлення в чат) — перевірка йде по `get_pending(chat_id)`, не по reply
- Якщо оператор надсилає голос без активного `/myth` — йде по існуючому маршруту (advice)

---

## Тести

1. `make test` — існуючі тести зелені
2. Написати `tests/test_myth_session.py`:
   - `test_set_and_get_pending` — set → get повертає slug
   - `test_clear_pending` — після clear → get повертає None
   - `test_multiple_chats` — різні chat_id не заважають один одному
3. Ручний тест:
   - Написати `/myth test-slug` в бот → відповідь "Готовий записати"
   - Надіслати голосове → відповідь з шляхом до файлу
   - Перевірити що `data/myth/test-slug/voiceover.wav` існує і відтворюється
