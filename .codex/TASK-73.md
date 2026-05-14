# TASK-73 — Покращений UX myth-bust: скрипт перед записом + вибір після запису

## Задача

Зараз `/myth <slug>` просто чекає голосове без показу тексту. Після збереження — надсилає команду рендеру з `--script ...` (не заповнений шлях). Треба: показувати текст скрипту перед записом, після запису давати вибір між "Рендерити" і "Перезаписати", а при рендері — запускати `myth_render.py` як subprocess і повідомляти про результат.

---

## Що реалізувати

### 1. `src/telegram_bot.py` — нова допоміжна функція `_parse_myth_script_text`

Парсить script.txt, прибирає `##bg:` рядки і заголовок `PARTS:`, повертає пронумеровані блоки тексту:

```python
def _parse_myth_script_text(script_path: Path) -> str:
    lines = script_path.read_text(encoding="utf-8").splitlines()
    blocks: list[str] = []
    current: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("PARTS:") or stripped.startswith("##bg:"):
            if current:
                blocks.append(" ".join(current))
                current = []
        elif stripped:
            current.append(stripped)
    if current:
        blocks.append(" ".join(current))
    return "\n\n".join(f"{i + 1}. {block}" for i, block in enumerate(blocks))
```

### 2. `src/telegram_bot.py` — обробник команди `/myth`

Після `set_pending(chat_id, slug)` — читати script.txt і показувати текст. Якщо скрипту нема — показати тільки "Надішли голосове":

```python
script_path = config.MYTH_DATA_DIR / slug / "script.txt"
if script_path.exists():
    script_text = _parse_myth_script_text(script_path)
    send_message(
        chat_id,
        f"📝 *Скрипт для озвучки:*\n\n{script_text}\n\n🎙 Надішли голосове повідомлення.",
        None,
    )
else:
    send_message(
        chat_id,
        f"✅ Готовий записати голос для *{slug}*\n\nНадішли голосове повідомлення.",
        None,
    )
```

### 3. `src/telegram_bot.py` — функція `_handle_myth_voice`, рядок ~1346

Замінити весь блок `send_message(...)` в кінці функції на inline keyboard з двома кнопками:

```python
    script_path = config.MYTH_DATA_DIR / slug / "script.txt"
    keyboard = {
        "inline_keyboard": [[
            {"text": "🎬 Рендерити", "callback_data": f"myth_render:{slug}"},
            {"text": "🔄 Перезаписати", "callback_data": f"myth_rerecord:{slug}"},
        ]]
    }
    send_message(
        chat_id,
        f"✅ Голос збережено: `{wav_path}`\n\nЩо далі?",
        keyboard,
    )
```

### 4. `src/telegram_bot.py` — обробник callback_query

Додати обробку двох нових callback_data у існуючий обробник callback-запитів:

**`myth_render:<slug>`** — запустити рендер:

```python
elif data.startswith("myth_render:"):
    slug = data.split(":", 1)[1]
    script_path = config.MYTH_DATA_DIR / slug / "script.txt"
    wav_path = config.MYTH_DATA_DIR / slug / "voiceover.wav"
    output_path = Path(f"/tmp/{slug}.mp4")
    send_message(chat_id, "⏳ Рендеримо відео...", None)
    try:
        result = subprocess.run(
            [
                "python", "myth_render.py",
                "--script", str(script_path),
                "--audio", str(wav_path),
                "--channel", "law",
                "--output", str(output_path),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode == 0:
            send_message(chat_id, f"✅ Готово:\n`{output_path}`", None)
        else:
            send_message(chat_id, f"❌ Помилка рендеру:\n```{result.stderr[-500:]}```", None)
    except subprocess.TimeoutExpired:
        send_message(chat_id, "❌ Рендер завис (timeout 5 хв)", None)
```

**`myth_rerecord:<slug>`** — перезапис голосу:

```python
elif data.startswith("myth_rerecord:"):
    slug = data.split(":", 1)[1]
    set_pending(chat_id, slug)
    send_message(chat_id, "🎙 Надішли нове голосове повідомлення.", None)
```

---

## Обмеження

- `subprocess.run` — синхронний виклик, блокує тред на час рендеру (~1-3 хв). Якщо бот однопотоковий — це прийнятно для рідкісної операції. Якщо є asyncio — замінити на `asyncio.create_subprocess_exec`.
- Не чіпати логіку ffmpeg-обробки голосу, set_pending / get_pending, advice-пайплайн.
- `--category` не додавати в команду рендеру — залишити дефолт "МОБІЛІЗАЦІЯ" з config.

---

## Тести

1. `make test` — всі тести зелені
2. Перевірити вручну `/myth bron-oblik`:
   - Бот показує пронумерований текст скрипту
   - Після голосового — з'являються дві кнопки
   - "Перезаписати" → бот знову чекає голосове
   - "Рендерити" → бот каже "⏳ Рендеримо..." → потім шлях до mp4
