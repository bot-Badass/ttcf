# TASK-86 — Пакетна озвучка: послідовна черга рендерів

## Задача

Зараз після збереження голосу оператор бачить кнопку "Рендерити", тисне її, чекає 5-15 хвилин і тільки після завершення рендеру бачить наступне відео в черзі. Треба прибрати це очікування: голос записується для кількох відео підряд, кожен рендер ставиться в послідовну чергу і виконується один за одним у фоні. Паралельні рендери не допускаються — кожен жере CPU/ffmpeg і при 4-5 паралельних комп зависне.

---

## Що реалізувати

### `src/telegram_bot.py` — крок 1: черга рендерів

Додати на модульному рівні (поруч з `_voice_sessions` та іншими dict-ами):

```python
import queue as _queue

# Послідовна черга рендерів: кожен елемент — (chat_id, slug, send_message)
_render_queue: _queue.Queue[tuple[str, str, Any]] = _queue.Queue()
# slugs що вже стоять у черзі або рендеряться (для дедуплікації)
_queued_renders: set[str] = set()
```

Запустити єдиний фоновий worker-потік при старті модуля (після оголошення змінних):

```python
def _render_worker() -> None:
    while True:
        chat_id, slug, send_message_fn = _render_queue.get()
        try:
            _execute_render(chat_id, slug, send_message_fn)
        finally:
            _queued_renders.discard(slug)
            _render_queue.task_done()

threading.Thread(target=_render_worker, daemon=True).start()
```

---

### `src/telegram_bot.py` — крок 2: функція `_enqueue_render`

```python
def _enqueue_render(
    chat_id: str,
    slug: str,
    send_message: SendMessageBoundary,
) -> None:
    if slug in _queued_renders:
        send_message(chat_id, f"⏳ `{slug}` вже в черзі рендеру.", None)
        return
    _queued_renders.add(slug)
    position = _render_queue.qsize() + 1
    if position > 1:
        send_message(chat_id, f"📥 `{slug}` поставлено в чергу рендеру (позиція {position}).", None)
    _render_queue.put((chat_id, slug, send_message))
```

---

### `src/telegram_bot.py` — крок 3: функція `_execute_render`

Перенести логіку з inline-замикання `_do_render` (зараз всередині `myth_render:` callback) в окрему функцію. Ця функція викликається тільки з `_render_worker` — не напряму.

```python
def _execute_render(
    chat_id: str,
    slug: str,
    send_message: SendMessageBoundary,
) -> None:
    script_path = config.MYTH_DATA_DIR / slug / "script.txt"
    wav_path = config.MYTH_DATA_DIR / slug / "voiceover.wav"
    base_output = Path(f"/tmp/{slug}.mp4")
    platform_labels = {"tiktok": "TikTok", "youtube": "YouTube", "instagram": "Instagram"}
    stderr_lines: list[str] = []

    send_message(chat_id, f"⏳ Рендеримо `{slug}`...", None)

    try:
        proc = subprocess.Popen(
            [
                "python", "myth_render.py",
                "--script", str(script_path),
                "--audio", str(wav_path),
                "--channel", _active_channel.get(chat_id, config.DEFAULT_CHANNEL),
                "--output", str(base_output),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def _read_stderr() -> None:
            for line in proc.stderr:
                stderr_lines.append(line)

        threading.Thread(target=_read_stderr, daemon=True).start()

        for line in proc.stdout:
            line = line.strip()
            if line.startswith("PROGRESS: "):
                label = platform_labels.get(line.split("PROGRESS: ", 1)[1].strip(), line)
                send_message(chat_id, f"⏳ Рендеримо {label}...", None)
            elif line.startswith("DONE: "):
                label = platform_labels.get(line.split("DONE: ", 1)[1].strip(), line)
                send_message(chat_id, f"✅ {label} готово", None)

        proc.wait()

        if proc.returncode == 0:
            channel_key = _active_channel.get(chat_id, config.DEFAULT_CHANNEL)
            export_dir = _get_or_create_myth_export_dir(channel_key, slug)

            import shutil as _shutil
            base = base_output.with_suffix("")
            for platform in ["tiktok", "youtube", "instagram"]:
                src = base.parent / f"{base.name}_{platform}.mp4"
                if src.is_file():
                    _shutil.copy2(src, export_dir / f"{export_dir.name}_{platform}.mp4")

            meta_src = config.MYTH_DATA_DIR / slug / "metadata.csv"
            if meta_src.is_file():
                _shutil.copy2(meta_src, export_dir / "metadata.csv")

            send_message(
                chat_id,
                f"✅ Відео і metadata.csv скопійовані в:\n`{export_dir}`",
                None,
            )
            from src.myth_queue import list_unvoiced, slug_to_title
            remaining = [s for s, _ in list_unvoiced() if s != slug]
            if remaining:
                next_slug = remaining[0]
                next_title = slug_to_title(next_slug)
                next_markup = {"inline_keyboard": [[
                    {"text": "🎙 Записати", "callback_data": f"queue_select:{next_slug}"},
                    {"text": "⏭ Пропустити", "callback_data": "queue_show"},
                ]]}
                send_message(chat_id, f"Далі в черзі:\n*{next_title}*", next_markup)
        else:
            stderr_tail = "".join(stderr_lines)[-500:]
            send_message(chat_id, f"❌ Помилка рендеру `{slug}`:\n```{stderr_tail}```", None)

    except Exception as exc:
        send_message(chat_id, f"❌ Помилка рендеру `{slug}`: {exc}", None)
```

---

### `src/telegram_bot.py` — крок 4: змінити `_handle_myth_voice`

Замінити поточний keyboard після збереження голосу:

**Зараз (прибрати):**
```python
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

**Замінити на:**
```python
_enqueue_render(chat_id, slug, send_message)

keyboard = {
    "inline_keyboard": [[
        {"text": "🎙 Наступне", "callback_data": "queue_show"},
        {"text": "🔄 Перезаписати", "callback_data": f"myth_rerecord:{slug}"},
    ]]
}
send_message(
    chat_id,
    f"✅ Голос збережено. Рендер в черзі.\n\nЩо далі?",
    keyboard,
)
```

---

### `src/telegram_bot.py` — крок 5: спростити `myth_render:` callback

Замінити весь inline `_do_render` + `threading.Thread` на виклик `_enqueue_render`:

```python
if callback_data.startswith("myth_render:"):
    slug = callback_data.split(":", 1)[1]
    _enqueue_render(chat_id, slug, send_message)
    answer_callback(callback_id, None)
    return
```

Цей callback залишається для ручного запуску рендеру після `myth_rerecord:`.

---

## Обмеження

- Не чіпати advice pipeline (`_handle_voice_reply`, `confirm_voice:`, `retake_voice:`)
- Не чіпати `myth_rerecord:` callback
- Worker-потік єдиний — рендери завжди послідовні, ніколи паралельні
- `_execute_render` не викликати напряму — тільки через `_enqueue_render` → worker
- Не додавати config.py змінних — все налаштування вже є

---

## Тести

1. `make test` — всі тести зелені
2. Перевірити вручну — пакетний сценарій:
   - Записати голос A → "Рендер в черзі" + [Наступне]
   - Натиснути [Наступне] → записати голос B → "Рендер в черзі (позиція 2)"
   - Записати голос C → "Рендер в черзі (позиція 3)"
   - Рендер A починається одразу, B чекає завершення A, C чекає B
   - Бот надсилає "✅ відео скопійовано" для кожного після завершення
3. Перевірити дедуплікацію:
   - Записати голос A → одразу знову спробувати поставити A → "вже в черзі"
4. Перевірити [Перезаписати]:
   - Записати голос → [Перезаписати] → записати знову → рендер ставиться в чергу знову (попередній або вже виконався або виконається з новим wav якщо ще не почався)
