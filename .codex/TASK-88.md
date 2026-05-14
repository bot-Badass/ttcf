# TASK-88 — Channel separation: exports, myth queue flow, category fix

## Задача

Чотири пов'язані баги: (1) "БРОНЮВАННЯ" на кожному myth-рендері незалежно від каналу — hardcoded у `.env`; (2) `/myth <slug>` не встановлює активний канал зі slug — render йде в дефолтний "law"; (3) export-директорія плутає канали і дає подвійні префікси (`finance_finance_xxx`, `law_finance_xxx`); (4) `/myth` без аргументу показує help замість черги. Всі проблеми вирішуються разом.

---

## Що реалізувати

### 1. `src/config.py` — додати `exports_subdir` і `myth_category_default` до CHANNEL_PROFILES

У кожен профіль додати два нових ключі:

```python
CHANNEL_PROFILES: Final[dict[str, dict]] = {
    "law": {
        # ... існуючі ключі ...
        "exports_subdir": "dontpaniclaw_content_dir",
        "myth_category_default": "ПРАВА",
    },
    "finance": {
        # ... існуючі ключі ...
        "exports_subdir": "moneyua_content_dir",
        "myth_category_default": "ФІНАНСИ",
    },
}
```

### 2. `.env` — очистити HOOK_FRAME_CATEGORY

Замінити:
```
HOOK_FRAME_CATEGORY=БРОНЮВАННЯ
HOOK_FRAME_CATEGORY_BG=0xFBBF24
HOOK_FRAME_CATEGORY_FG=0x78350F
```
На:
```
HOOK_FRAME_CATEGORY=
HOOK_FRAME_CATEGORY_BG=
HOOK_FRAME_CATEGORY_FG=
```

Ці значення більше не є глобальними дефолтами — кожен канал і пайплайн встановлює категорію окремо.

### 3. `src/telegram_bot.py` — три зміни

#### 3a. `/myth <slug>` — auto-detect channel зі slug

Знайти блок `if command == "/myth":` (~рядок 637). Після `slug = parts[1].strip()` і перевірки що slug не порожній — додати встановлення каналу зі slug:

```python
if command == "/myth":
    parts = str(message_payload.get("text", "")).strip().split(maxsplit=1)
    slug = parts[1].strip() if len(parts) > 1 else ""
    if not slug:
        # НОВА ПОВЕДІНКА: показати channel picker для myth flow
        text, markup = _build_myth_channel_picker_message(chat_id)
        send_message(chat_id, text, markup)
        return
    # НОВЕ: auto-detect channel зі slug
    from src.myth_queue import slug_to_channel
    detected_channel = slug_to_channel(slug)
    _active_channel[chat_id] = detected_channel
    _save_active_channels(_active_channel)
    set_pending(chat_id, slug)
    # ... решта існуючого коду без змін ...
```

#### 3b. Новий callback `myth_channel:` і нова функція `_build_myth_channel_picker_message`

Додати нову функцію для myth-specific channel picker (окремо від `/plan` picker):

```python
def _build_myth_channel_picker_message(chat_id: str) -> tuple[str, Mapping[str, Any]]:
    """Channel picker for myth voiceover flow."""
    rows: list[list[Mapping[str, Any]]] = []
    for key, profile in config.CHANNEL_PROFILES.items():
        rows.append([{
            "text": profile["label"],
            "callback_data": f"myth_channel:{key}",
        }])
    return "🎙 Для якого каналу озвучуємо?", {"inline_keyboard": rows}
```

Додати обробку callback `myth_channel:` в `_handle_callback_query` — знайти блок де обробляються callbacks (близько рядка 930):

```python
if callback_data.startswith("myth_channel:"):
    channel_key = callback_data.removeprefix("myth_channel:")
    if channel_key in config.CHANNEL_PROFILES:
        _active_channel[chat_id] = channel_key
        _save_active_channels(_active_channel)
    # Показати чергу відфільтровану по каналу
    from src.myth_queue import list_unvoiced, slug_to_channel, slug_to_title
    unvoiced = [(s, ch) for s, ch in list_unvoiced() if ch == channel_key]
    if not unvoiced:
        send_message(chat_id, "✅ Черга порожня для цього каналу.", None)
        answer_callback(callback_id, None)
        return
    channel_emoji = {"law": "⚖️", "finance": "💰"}
    emoji = channel_emoji.get(channel_key, "📄")
    rows = []
    for s, _ in unvoiced:
        title = slug_to_title(s)
        rows.append([{
            "text": f"{emoji} {title}",
            "callback_data": f"queue_select:{s}",
        }])
    markup = {"inline_keyboard": rows}
    send_message(chat_id, f"🎙 Черга ({len(unvoiced)}):", markup)
    answer_callback(callback_id, None)
    return
```

#### 3c. `_get_or_create_myth_export_dir` — нова структура папок

Замінити повністю функцію:

```python
def _get_or_create_myth_export_dir(channel_key: str, slug: str) -> Path:
    """Return export dir for a myth slug under channel-specific subdirectory.

    Structure: data/exports/{channel_subdir}/{slug}/
    Example:   data/exports/dontpaniclaw_content_dir/shtraf-tck-ne-platy/
               data/exports/moneyua_content_dir/finance_borhy-2-servisy/
    """
    from src.publer_export import EXPORTS_ROOT
    profile = config.CHANNEL_PROFILES.get(channel_key, {})
    subdir = profile.get("exports_subdir", f"{channel_key}_content_dir")
    export_dir = EXPORTS_ROOT / subdir / slug
    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir
```

#### 3d. `_enqueue_render` — передавати `--category` до myth_render.py

Знайти виклик `subprocess.Popen(["python", "myth_render.py", ...])` (~рядок 207). Додати `--category` аргумент:

```python
# Визначити категорію: читати з category.txt якщо є, інакше channel default
channel_key = _active_channel.get(chat_id, config.DEFAULT_CHANNEL)
profile = config.CHANNEL_PROFILES.get(channel_key, {})
category_file = config.MYTH_DATA_DIR / slug / "category.txt"
if category_file.is_file():
    myth_category = category_file.read_text(encoding="utf-8").strip().upper()
else:
    myth_category = profile.get("myth_category_default", "")

proc = subprocess.Popen(
    [
        "python", "myth_render.py",
        "--script", str(script_path),
        "--audio", str(wav_path),
        "--channel", channel_key,
        "--output", str(base_output),
        "--category", myth_category,
    ],
    ...
)
```

---

## Обмеження

- НЕ чіпати логіку `queue_select:` callback — вона вже коректно встановлює канал зі slug
- НЕ чіпати `channel:` callback — це для advice `/plan` flow, залишити без змін
- НЕ видаляти існуючі export-папки — тільки нові exports йдуть у нову структуру
- НЕ змінювати `myth_render.py` — він вже приймає `--category`, цього достатньо
- Всі env змінні (крім HOOK_FRAME_CATEGORY) в `.env` — не чіпати

---

## Тести

1. `make test` — всі тести зелені
2. Перевірити вручну: `/myth` без аргументу → з'являється picker "Для якого каналу озвучуємо?" з двома кнопками
3. Вибрати канал → бот показує чергу тільки для цього каналу
4. `/myth finance_borhy-2-servisy` → бот auto-встановлює finance канал (перевірити `_active_channel`)
5. Після рендеру: файл потрапляє в `data/exports/moneyua_content_dir/finance_borhy-2-servisy/`
6. Thumbnail має зелений акцент (finance) або червоний (law) — відповідно до каналу
7. Категорія на thumbnail: "ФІНАНСИ" для moneyua, "ПРАВА" для dontpaniclaw (не "БРОНЮВАННЯ")
