# TASK-84 — Підсвітка ключових слів у субтитрах (ASS color tags)

## Задача

Зараз субтитри рендеряться у форматі SRT, який не підтримує кольори. Потрібно перейти на ASS-формат і додати підсвітку ключових слів, які оператор позначає у скрипті маркерами `**слово**`. Колір підсвітки береться з профілю каналу. Це дасть ефект "Alex Hormozi"-стилю — ключові цифри і терміни виділяються кольором у ритм мовлення.

---

## Що реалізувати

### `src/subtitles.py` — парсинг маркерів і запис ASS

**1. Додати функцію `_extract_highlighted_words`**

Парсить скрипт і повертає set нормалізованих слів, що позначені `**...**`.

```python
import re as _re

def _extract_highlighted_words(script: str) -> frozenset[str]:
    """Return normalized forms of words marked with **word** in script."""
    marked = _re.findall(r"\*\*(.+?)\*\*", script)
    result: set[str] = set()
    for phrase in marked:
        for token in phrase.split():
            normalized = _normalize_for_comparison(token)
            if normalized:
                result.add(normalized)
    return frozenset(result)
```

**2. Змінити сигнатуру `generate_subtitle_file`**

Додати параметр `highlight_color: str | None = None`. Колір — hex string каналу, напр. `"#FF3B30"` (law) або `"#FFD700"` (finance).

```python
def generate_subtitle_file(
    audio_path: Path,
    script: str,
    output_dir: Path,
    *,
    highlight_color: str | None = None,
    align_words: WordAlignmentBoundary | None = None,
    subtitle_writer: SubtitleWriterBoundary | None = None,
    alignment_mode: str | None = None,
    max_words_per_phrase: int = config.SUBTITLE_MAX_WORDS_PER_PHRASE,
    max_chars_per_phrase: int = config.SUBTITLE_MAX_CHARS_PER_PHRASE,
    max_silence_gap_seconds: float = config.SUBTITLE_MAX_SILENCE_GAP_SECONDS,
) -> Path:
```

**3. Передати highlighted_words і color у writer**

У тілі `generate_subtitle_file`, перед викликом writer:

```python
highlighted_words = _extract_highlighted_words(script) if highlight_color else frozenset()

output_dir.mkdir(parents=True, exist_ok=True)
subtitle_path = output_dir / "subtitles.ass"
writer = subtitle_writer or _write_ass_with_pysubs2
writer(phrases, subtitle_path, highlighted_words=highlighted_words, highlight_color=highlight_color or "")
return subtitle_path
```

**4. Замінити `_write_srt_with_pysubs2` на `_write_ass_with_pysubs2`**

```python
def _write_ass_with_pysubs2(
    phrases: Sequence[SubtitlePhrase],
    subtitle_path: Path,
    *,
    highlighted_words: frozenset[str] = frozenset(),
    highlight_color: str = "",
) -> None:
    try:
        import pysubs2
    except ModuleNotFoundError as exc:
        raise SubtitleGenerationError("pysubs2 is not installed.") from exc

    subs = pysubs2.SSAFile()

    # Style: white bold centered
    style = subs.styles["Default"]
    style.fontname = "Arial"
    style.fontsize = 52
    style.bold = True
    style.primarycolor = pysubs2.Color(255, 255, 255, 0)
    style.outlinecolor = pysubs2.Color(0, 0, 0, 0)
    style.outline = 3
    style.shadow = 1
    style.alignment = 5  # center middle

    ass_highlight = _hex_to_ass_color(highlight_color) if highlight_color else ""

    for phrase in phrases:
        text = _apply_highlight_tags(phrase.text, highlighted_words, ass_highlight)
        subs.events.append(
            pysubs2.SSAEvent(
                start=int(round(phrase.start_seconds * 1000)),
                end=int(round(phrase.end_seconds * 1000)),
                text=text,
            )
        )

    try:
        subs.save(str(subtitle_path), format_="ass")
    except Exception as exc:
        raise SubtitleGenerationError(f"Failed to save subtitle file: {exc}") from exc


def _hex_to_ass_color(hex_color: str) -> str:
    """Convert #RRGGBB to ASS color tag string &H00BBGGRR&."""
    h = hex_color.lstrip("#")
    if len(h) != 6:
        return ""
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"&H00{b:02X}{g:02X}{r:02X}&"


def _apply_highlight_tags(
    text: str,
    highlighted_words: frozenset[str],
    ass_color: str,
) -> str:
    if not highlighted_words or not ass_color:
        return text
    tokens = text.split()
    result = []
    for token in tokens:
        if _normalize_for_comparison(token) in highlighted_words:
            result.append(f"{{\\c{ass_color}}}{token}{{\\c&HFFFFFF&}}")
        else:
            result.append(token)
    return " ".join(result)
```

---

### `src/myth_pipeline.py` — передати highlight_color

У `prepare_myth_assets`, передати `highlight_color` у `generate_subtitle_file`:

```python
from src import config as _config

# після отримання channel_profile:
highlight_color = channel_profile.get("highlight_color", "")

subtitle_path = generate_subtitle_file(
    audio_path,
    script,
    output_dir,
    highlight_color=highlight_color or None,
)
```

`channel_profile` вже доступний через `config.CHANNEL_PROFILES[channel_key]`.

---

### `src/config.py` — додати `highlight_color` у профілі каналів

У `CHANNEL_PROFILES` для кожного каналу додати поле:

```python
CHANNEL_PROFILES = {
    "law": {
        ...
        "highlight_color": "#FF3B30",   # червоний акцент каналу
    },
    "finance": {
        ...
        "highlight_color": "#FFD700",   # золотий акцент каналу
    },
}
```

---

## Обмеження

- НЕ чіпати логіку `_align_words_with_faster_whisper` і `_group_aligned_words`
- НЕ змінювати `AlignedWord` і `SubtitlePhrase` dataclasses
- НЕ ламати існуючий `SubtitleWriterBoundary` контракт — новий writer отримує додаткові kwargs через `**kwargs` або прямий виклик
- Якщо `highlight_color` не передано (None) — поведінка ідентична старому SRT, просто у форматі ASS без тегів
- ffmpeg підтримує ASS нативно — зміна розширення з `.srt` на `.ass` в myth_pipeline достатня

---

## Тести

1. `make test` — всі тести зелені
2. Перевірити вручну: скрипт з `**160 тисяч**` → після рендеру "160 тисяч" виділено кольором каналу
3. Перевірити вручну: скрипт без `**...**` → субтитри білі, без тегів, відео рендериться нормально
