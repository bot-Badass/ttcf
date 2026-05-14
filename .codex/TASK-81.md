# TASK-81 — myth рендер: канал з active_channel + дедублікація відео

## Задача

Два баги в myth-bust pipeline.

**Баг 1:** у callback `myth_render:` в боті `--channel` захардкоджено як `"law"` — myth відео для moneyua рендеряться з брендингом dontpaniclaw (колір хук-кадру, CTA).

**Баг 2:** `build_background_video` в myth_pipeline.py завантажує кліпи незалежно для кожної секції і не знає які відео вже взяв у цьому ж рендері. Для схожих Pexels-запитів (наприклад, кілька секцій зі "smartphone") видаються ті самі відео і в результаті 2-3 кліпи повторюються.

---

## Що реалізувати

### `src/telegram_bot.py` — callback `myth_render:`

Замінити хардкод `"law"` на активний канал поточного чату:

```python
# було
"--channel", "law",

# стало
"--channel", _active_channel.get(chat_id, config.DEFAULT_CHANNEL),
```

Рядок знаходиться всередині `_do_render` closure, приблизно рядок 989. `chat_id` доступний через default argument closure.

---

### `src/pexels_client.py` — `download_background_video`

1. Додати параметр `extra_exclude_ids: set[int] | None = None`
2. Об'єднати з `used_ids` з blacklist перед викликом `_pick_hd_portrait_link`
3. Змінити return type з `None` на `int | None` — повертати `video_id` (або `None` якщо не завантажено)

```python
def download_background_video(
    output_path: Path,
    query: str,
    min_duration: int = 5,
    http_get: Callable[[str, Mapping[str, str]], bytes] | None = None,
    extra_exclude_ids: set[int] | None = None,  # нове
) -> int | None:  # було None
    ...
    blacklist = _load_blacklist()
    used_ids: set[int] = set(blacklist.get(query, []))
    if extra_exclude_ids:
        used_ids |= extra_exclude_ids  # нове
    video_link, video_id = _pick_hd_portrait_link(videos, min_duration, used_ids)
    ...
    # в кінці повернути video_id замість нічого:
    return video_id  # нове (може бути None якщо не знайдено)
```

---

### `src/myth_pipeline.py` — `build_background_video`

Відстежувати `video_id` що вже завантажені в поточному рендері і передавати в кожен наступний виклик:

```python
def build_background_video(
    sections_with_times: list[dict],
    output_path: Path,
) -> Path:
    ...
    session_used: set[int] = set()  # нове

    for i, section in enumerate(sections_with_times):
        clip_path = tmp / f"clip_{i:02d}.mp4"
        query = section["query"]
        try:
            vid_id = download_background_video(  # було без vid_id =
                clip_path,
                query=query,
                extra_exclude_ids=session_used,  # нове
            )
            if vid_id is not None:  # нове
                session_used.add(vid_id)  # нове
        except Exception as exc:
            LOGGER.warning(...)
            vid_id = download_background_video(  # нове
                clip_path,
                query=config.ADVICE_PEXELS_QUERY,
                extra_exclude_ids=session_used,  # нове
            )
            if vid_id is not None:  # нове
                session_used.add(vid_id)  # нове
        clip_paths.append(clip_path)
    ...
```

---

## Обмеження

- НЕ чіпати advice pipeline (`advice_pipeline.py`) — там `download_background_video` теж використовується, але без `extra_exclude_ids` (параметр опціональний, зворотна сумісність збережена)
- НЕ чіпати логіку blacklist — глобальна дедублікація між сесіями залишається як є
- Тести myth pipeline: `tests/test_myth_pipeline.py` — всі мають бути зелені

---

## Тести

1. `make test` — всі тести зелені
2. Перевірити вручну: `/myth test-slug` з moneyua-активним каналом → відео має золотий хук-кадр (finance брендинг), не білий (law)
3. Переглянути відрендероване відео — кожна з 5 секцій має унікальний фоновий кліп
