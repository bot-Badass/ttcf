# TASK-87 — Персистентна черга рендерів: відновлення після краша бота

## Задача

Черга рендерів з TASK-86 живе тільки в пам'яті. Якщо бот впав або перезапустився — всі незавершені рендери в черзі зникають і оператор не отримає відео. Треба зберігати чергу на диск і відновлювати її при кожному старті бота.

---

## Що реалізувати

### `src/config.py` — новий шлях

Додати поруч з іншими `MANIFEST_PATH`/`PUBLISH_QUEUE_PATH`:

```python
RENDER_QUEUE_PATH: Final[Path] = DATA_DIR / "manifests" / "render_queue.json"
```

---

### `src/telegram_bot.py` — крок 1: додати впорядкований список для персистентності

Поруч з `_render_queue` і `_queued_renders` додати:

```python
# Впорядкований список {chat_id, slug} для збереження на диск.
# Завжди синхронний з _queued_renders.
_queued_render_items: list[dict[str, str]] = []
```

---

### `src/telegram_bot.py` — крок 2: функція `_persist_render_queue`

```python
def _persist_render_queue() -> None:
    path = config.RENDER_QUEUE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_queued_render_items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
```

---

### `src/telegram_bot.py` — крок 3: оновити `_enqueue_render`

Після `_queued_renders.add(slug)` додати запис в `_queued_render_items` і виклик persist:

```python
def _enqueue_render(
    chat_id: str,
    slug: str,
    send_message: Any,
) -> None:
    if slug in _queued_renders:
        send_message(chat_id, f"⏳ `{slug}` вже в черзі рендеру.", None)
        return
    _queued_renders.add(slug)
    _queued_render_items.append({"chat_id": chat_id, "slug": slug})
    _persist_render_queue()
    position = _render_queue.qsize() + 1
    if position > 1:
        send_message(chat_id, f"📥 `{slug}` поставлено в чергу рендеру (позиція {position}).", None)
    _render_queue.put((chat_id, slug, send_message))
```

---

### `src/telegram_bot.py` — крок 4: оновити `_render_worker`

У блоці `finally` після `_queued_renders.discard(slug)` видаляти slug з `_queued_render_items` і персистити:

```python
def _render_worker() -> None:
    while True:
        chat_id, slug, send_message_fn = _render_queue.get()
        try:
            _execute_render(chat_id, slug, send_message_fn)
        finally:
            _queued_renders.discard(slug)
            _queued_render_items[:] = [i for i in _queued_render_items if i["slug"] != slug]
            _persist_render_queue()
            _render_queue.task_done()
```

---

### `src/telegram_bot.py` — крок 5: функція `_restore_render_queue`

```python
def _restore_render_queue() -> None:
    path = config.RENDER_QUEUE_PATH
    if not path.is_file():
        return
    try:
        items = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        LOGGER.warning("Failed to read render queue from %s", path)
        return
    for item in items:
        chat_id = item.get("chat_id", "")
        slug = item.get("slug", "")
        if chat_id and slug and slug not in _queued_renders:
            LOGGER.info("Restoring render from queue: slug=%s chat_id=%s", slug, chat_id)
            _enqueue_render(chat_id, slug, _default_send_message)
```

---

### `src/telegram_bot.py` — крок 6: виклик при старті

На початку `run_polling_loop`, після рядку `message_sender = send_message or _default_send_message`:

```python
_restore_render_queue()
```

Виклик саме тут (а не на модульному рівні) — бо `_default_send_message` має бути вже визначена, і bot token доступний.

---

## Обмеження

- Не чіпати логіку `_execute_render`
- Не чіпати advice pipeline
- `_restore_render_queue` викликати тільки з `run_polling_loop` — не на модульному рівні
- `_persist_render_queue` не кидає виняток назовні — тільки пише на диск; якщо запис не вдався Python підніме OSError, це нормально (краш краще ніж тихий баг)

---

## Тести

1. `make test` — всі тести що були зелені залишаються зеленими
2. Перевірити вручну — сценарій краша:
   - Записати голос для A і B → обидва в черзі → `data/manifests/render_queue.json` містить обидва записи
   - Зупинити бота поки A рендериться
   - Запустити бота знову → бот відновлює чергу → рендер A запускається знову (wav вже є), після нього B
   - Після кожного завершення бот надсилає "✅ відео скопійовано"
3. Перевірити що після успішного рендеру A файл `render_queue.json` містить тільки B, після B — порожній список `[]`
