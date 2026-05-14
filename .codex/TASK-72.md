# TASK-72 — Hook frame: перше речення, правильна категорія, менший шрифт

## Задача

Після TASK-71 hook frame з'явився, але має три проблеми:
1. `extract_hook_text` повертає весь перший рядок (два речення) → текст не вміщується у фрейм
2. Категорія береться з `HOOK_FRAME_CATEGORY` env var (залишок від попередньої сесії = "БРОНЮВАННЯ") — myth_render.py не передає свою
3. Шрифт 116 занадто великий → зменшити дефолт до 90

---

## Що реалізувати

### `src/myth_pipeline.py` — виправити `extract_hook_text`

```python
def extract_hook_text(script_text: str) -> str:
    """Return first sentence of first text block after first ##bg: marker."""
    past_first_bg = False
    for line in script_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("##bg:"):
            past_first_bg = True
            continue
        if past_first_bg and stripped:
            # Return only up to first sentence boundary
            for sep in (".", "?", "!"):
                idx = stripped.find(sep)
                if idx != -1:
                    return stripped[: idx + 1]
            return stripped
    return ""
```

---

### `src/render.py` — додати `category` через ланцюг викликів

**`_render_hook_frame_v2`** — додати параметр і використати замість env var:

```python
def _render_hook_frame_v2(
    ...,
    category_override: str = "",   # новий параметр — додати останнім
) -> None:
    ...
    # БУЛО:
    category = config.HOOK_FRAME_CATEGORY.strip().upper()
    # СТАЛО:
    category = (category_override.strip().upper()
                or config.HOOK_FRAME_CATEGORY.strip().upper())
```

**`_render_hook_frame`** — додати параметр і прокинути в V2:

```python
def _render_hook_frame(
    ...,
    category_override: str = "",   # додати після hook_brand_override
) -> None:
    if config.HOOK_FRAME_LAYOUT.strip().lower() == "v2":
        _render_hook_frame_v2(
            ...,
            category_override=category_override,
        )
    # classic — category_override не підтримується, ігнорувати
```

**`render_story_video`** — додати параметр і прокинути в `_render_hook_frame`:

```python
def render_story_video(
    ...,
    category_override: str = "",   # додати після hook_brand_override
) -> None:
    ...
    # В виклику _render_hook_frame:
    _render_hook_frame(
        ...,
        category_override=category_override,
    )
```

---

### `src/myth_pipeline.py` — `render_myth_video` приймає і передає category

```python
def render_myth_video(
    script_text: str,
    audio_path: Path,
    output_path: Path,
    channel: str,
    part_number: int = 1,
    total_parts: int = 1,
    category: str = "",            # новий параметр
) -> None:
    ...
    render_story_video(
        ...,
        category_override=category,
    )
```

---

### `myth_render.py` — додати `--category` CLI аргумент

```python
parser.add_argument(
    "--category",
    default="МОБІЛІЗАЦІЯ",
    help="Hook frame category label (default: МОБІЛІЗАЦІЯ)",
)

# В виклику render_myth_video:
render_myth_video(
    ...,
    category=args.category,
)
```

---

### `src/config.py` — зменшити дефолт шрифту

```python
# БУЛО:
HOOK_FRAME_V2_FONT_SIZE: Final[int] = int(os.getenv("HOOK_FRAME_V2_FONT_SIZE", "116"))
# СТАЛО:
HOOK_FRAME_V2_FONT_SIZE: Final[int] = int(os.getenv("HOOK_FRAME_V2_FONT_SIZE", "90"))
```

---

## Обмеження

- НЕ чіпати advice-pipeline — `category_override=""` скрізь за замовчуванням, поведінка не змінюється (env var продовжує працювати)
- НЕ змінювати логіку classic hook frame — `category_override` там ігнорується
- `--category` в myth_render.py має дефолт "МОБІЛІЗАЦІЯ" — найчастіша категорія для myth-bust dontpaniclaw

---

## Тести

1. `make test` — всі існуючі тести зелені
2. Ручний тест:
   ```bash
   python myth_render.py --script data/myth/vidstrochka-tsnap/script.txt \
     --audio data/myth/vidstrochka-tsnap/voiceover.wav \
     --channel law --category ВІДСТРОЧКА --output /tmp/myth_test_v3.mp4
   ```
   - Hook frame: відображає тільки перше речення ("Люди досі йдуть в ТЦК за відстрочкою і витрачають день даремно.")
   - Категорія: жовтий бейдж "ВІДСТРОЧКА" (не "БРОНЮВАННЯ")
   - Шрифт: менший і текст вміщується у 3-4 рядки
