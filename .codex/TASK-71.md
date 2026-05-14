# TASK-71 — Myth-bust: hook frame, "Поділись" CTA з 10 сек, збільшений шрифт

## Задача

Myth-bust відео рендерується без thumbnail (hook frame) і без CTA-анімації — бо `render_myth_video` не передає `hook_text` і `CHANNEL_PROFILES["law"]` не має `cta_overlay_path`. Потрібно:
1. Витягти hook text з першого блоку скрипту і передати в рендер
2. Завжди використовувати "Поділись" (`cta_share_gs.mp4`) для myth-bust, починаючи з 10-ї секунди
3. Трохи збільшити шрифт hook frame (96 → 116)

---

## Що реалізувати

### `src/config.py` — нова константа шрифту

```python
HOOK_FRAME_V2_FONT_SIZE: Final[int] = int(os.getenv("HOOK_FRAME_V2_FONT_SIZE", "116"))
```

Додати після `HOOK_FRAME_FONT_SIZE`.

---

### `src/render.py` — три зміни

#### 1. `_render_hook_frame_v2` — замінити хардкод на конфіг

```python
# БУЛО:
FONTSIZE    = 96
FONTSIZE_SM = 34
FONTSIZE_BR = 28

# СТАЛО:
FONTSIZE    = config.HOOK_FRAME_V2_FONT_SIZE   # default 116
FONTSIZE_SM = 34
FONTSIZE_BR = 28
```

#### 2. `_build_ffmpeg_render_command` — додати `cta_start_offset`

Сигнатура (додати параметр після існуючих):
```python
def _build_ffmpeg_render_command(
    ...,
    cta_start_offset: float | None = None,   # якщо задано — overrides duration - CTA_DURATION
) -> list[str]:
```

В тілі функції замінити один рядок:
```python
# БУЛО:
cta_t0 = max(0.0, duration_seconds - config.CTA_DURATION)

# СТАЛО:
if cta_start_offset is not None:
    cta_t0 = cta_start_offset
else:
    cta_t0 = max(0.0, duration_seconds - config.CTA_DURATION)
```

#### 3. `_render_body` і `render_story_video` — прокинути параметри

В `_render_body` додати `cta_start_offset: float | None = None` і передати в `_build_ffmpeg_render_command`.

В `render_story_video` додати `cta_start_offset: float | None = None` і передати в `_render_body`.

---

### `src/myth_pipeline.py` — дві зміни

#### 1. Нова функція `extract_hook_text`

```python
def extract_hook_text(script_text: str) -> str:
    """Return first line of first text block after first ##bg: marker."""
    past_first_bg = False
    for line in script_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("##bg:"):
            past_first_bg = True
            continue
        if past_first_bg and stripped:
            return stripped
    return ""
```

#### 2. `render_myth_video` — передати hook_text, share CTA з 10 сек

```python
# Перед викликом render_story_video додати:
hook_text = extract_hook_text(script_text)

# Змінити виклик render_story_video:
render_story_video(
    audio_path=audio_path,
    background_video_path=bg_tmp,
    output_path=output_path,
    channel=channel,
    subtitle_path=subtitle_path,
    hook_text=hook_text or None,
    part_number=1,          # force use_share_cta = True (share для myth-bust завжди)
    total_parts=2,          # part 1 of 2 → не остання → "Поділись" замість "Підпишись"
    cta_start_offset=10.0,  # CTA з 10-ї секунди
)
```

**Чому `total_parts=2`:** render.py визначає `use_share_cta` коли `part_number < total_parts`. Значення 1 з 2 вмикає share-анімацію без видимого бейджа (PART_BADGE_ENABLED=false за замовчуванням).

---

## Обмеження

- НЕ чіпати advice-pipeline — `cta_start_offset=None` скрізь за замовчуванням, поведінка не змінюється
- НЕ додавати `cta_overlay_path` до профілю `"law"` — myth-bust завжди через share CTA, не через профіль
- `HOOK_FRAME_V2_FONT_SIZE` впливає на обидва канали (law і finance) — це очікувана поведінка

---

## Тести

1. `make test` — всі існуючі тести зелені
2. Ручний тест:
   ```bash
   python myth_render.py --script data/myth/vidstrochka-tsnap/script.txt \
     --audio data/myth/vidstrochka-tsnap/voiceover.wav \
     --channel law --output /tmp/myth_test_v2.mp4
   ```
   - Перші ~1 сек відео: hook frame з текстом хука видимий (більший шрифт)
   - На ~10-й секунді з'являється анімована кнопка "Поділись"
   - Анімація програється і зникає (не залишається до кінця)
