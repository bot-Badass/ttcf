# TASK-74 — Платформенні CTA + три окремі відео на рендер

## Задача

Зараз `myth_render.py` рендерить одне відео для всіх платформ без CTA-анімацій. Потрібно рендерити 3 окремих відео (TikTok / Instagram / YouTube) з платформенними CTA-анімаціями на greenscreen: "Поділись" @ ~10s і "Підпишись" в кінці. CTA-файли вже лежать у `data/assets/` у форматі 576×1024 з зеленим фоном.

---

## CTA-файли і параметри

| Файл | Платформа | Тип | Тривалість | Аудіо |
|------|-----------|-----|-----------|-------|
| `share_ty.mp4` | TikTok + Instagram | Share @ 10s | 5 сек | немає |
| `subscribe_tiktok.mp4` | TikTok | Subscribe @ кінець | 4.8 сек | немає |
| `subscribe_shorts.mp4` | YouTube | Subscribe @ кінець | 3.8 сек | немає |
| `subscribe_instagram.mp4` | Instagram | Subscribe @ кінець | 10 сек | є (треба вимкнути) |

**YouTube:** без Share CTA (тільки Subscribe в кінці).

**Позиціювання:** CTA-елементи знаходяться в центрі зеленого кадру (~55-60% висоти), що конфліктує з субтитрами (Alignment=5, MarginV=0 → Y≈960px). Рішення — scale CTA до 1080×1920 (colorkey прибирає зелений), потім overlay з негативним Y-offset щоб кнопка опинилась у верхній зоні (~120px від верху).

| CTA | Y-offset (стартовий) |
|-----|---------------------|
| share_ty | **-1032** |
| subscribe_tiktok / subscribe_shorts | **-935** |
| subscribe_instagram | **-648** |

Всі стартові — підлаштовуються через env vars після першого тест-рендеру.

---

## Що реалізувати

### 1. `src/config.py` — нові CTA змінні

Додати після блоку `CTA_SHARE_OVERLAY_Y` (рядок ~184). Старі `CTA_SHARE_OVERLAY_PATH`, `CTA_SHARE_OVERLAY_WIDTH`, `CTA_SHARE_OVERLAY_Y` — **не чіпати**, advice pipeline їх використовує.

```python
# --- Myth pipeline CTA assets (greenscreen overlays) ---
CTA_SHARE_PATH: Final[str] = os.getenv(
    "CTA_SHARE_PATH", str(DATA_DIR / "assets" / "share_ty.mp4")
)
CTA_SHARE_Y_OFFSET: Final[int] = int(os.getenv("CTA_SHARE_Y_OFFSET", "-1032"))
CTA_SHARE_START: Final[float] = float(os.getenv("CTA_SHARE_START", "10.0"))

CTA_SUBSCRIBE_TIKTOK_PATH: Final[str] = os.getenv(
    "CTA_SUBSCRIBE_TIKTOK_PATH", str(DATA_DIR / "assets" / "subscribe_tiktok.mp4")
)
CTA_SUBSCRIBE_YOUTUBE_PATH: Final[str] = os.getenv(
    "CTA_SUBSCRIBE_YOUTUBE_PATH", str(DATA_DIR / "assets" / "subscribe_shorts.mp4")
)
CTA_SUBSCRIBE_INSTAGRAM_PATH: Final[str] = os.getenv(
    "CTA_SUBSCRIBE_INSTAGRAM_PATH", str(DATA_DIR / "assets" / "subscribe_instagram.mp4")
)
CTA_SUBSCRIBE_Y_OFFSET: Final[int] = int(os.getenv("CTA_SUBSCRIBE_Y_OFFSET", "-935"))
CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET: Final[int] = int(
    os.getenv("CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET", "-648")
)
CTA_SUBSCRIBE_MAX_DURATION: Final[float] = float(
    os.getenv("CTA_SUBSCRIBE_MAX_DURATION", "4.0")
)
```

---

### 2. `src/render.py` — нова функція `_build_myth_ffmpeg_command`

Окрема функція тільки для myth-bust рендеру. **Не чіпати** `_build_ffmpeg_render_command` і `render_story_video`.

**Підхід до хук-фрейму:** рекомендується pre-render підхід — передати вже відрендерений `hook_path: Path` як input замість hook_text/overrides. Тоді `render_myth_story_video` (крок 3) самостійно викликає `_render_hook_frame` + `_render_body` перед фінальним combine-прогоном з CTA. Це дозволяє повністю перевикористати існуючу логіку хук-фрейму.

Сигнатура (якщо pre-render підхід):

```python
def _build_myth_ffmpeg_command(
    *,
    body_path: Path,
    hook_path: Path,
    output_path: Path,
    duration_seconds: float,
    ffmpeg_path: str,
    share_cta_path: str | None,
    share_cta_y_offset: int,
    share_cta_start: float,
    subscribe_cta_path: str,
    subscribe_cta_y_offset: int,
    subscribe_cta_max_duration: float,
) -> list[str]:
```

**Inputs команди:**
- `[0]` body.mp4 (pre-rendered: background + audio + subtitles)
- `[1]` hook.mp4 (pre-rendered hook frame, video only)
- `[2]` share_cta з `-itsoffset {share_cta_start}` (якщо є)
- `[2 або 3]` subscribe_cta з `-itsoffset {subscribe_start}` де `subscribe_start = max(0, duration_seconds - subscribe_cta_max_duration)`

**filter_complex (з share CTA, TikTok / Instagram):**
```
[0:v][1:v]overlay=0:0:enable='between(t,0,{hook_dur})'[body_hooked];
[2:v]scale=1080:-1,colorkey=0x00ff00:0.35:0.1[share_ol];
[3:v]trim=duration={max_dur},scale=1080:-1,colorkey=0x00ff00:0.35:0.1[sub_ol];
[body_hooked][share_ol]overlay=0:{share_y}:enable='between(t,{t0},{t0+5})'[body1];
[body1][sub_ol]overlay=0:{sub_y}:enable='gte(t,{sub_t})'[outv]
```

**filter_complex (без share CTA, YouTube):**
```
[0:v][1:v]overlay=0:0:enable='between(t,0,{hook_dur})'[body_hooked];
[2:v]trim=duration={max_dur},scale=1080:-1,colorkey=0x00ff00:0.35:0.1[sub_ol];
[body_hooked][sub_ol]overlay=0:{sub_y}:enable='gte(t,{sub_t})'[outv]
```

Output:
```python
command.extend([
    "-filter_complex", filter_complex,
    "-map", "[outv]",
    "-map", "0:a",        # аудіо з body.mp4, CTA-аудіо ніколи не потрапляє у вихід
    "-c:v", config.OUTPUT_VIDEO_CODEC,
    "-preset", config.OUTPUT_PRESET,
    "-crf", str(config.OUTPUT_CRF),
    "-pix_fmt", config.OUTPUT_PIXEL_FORMAT,
    "-c:a", config.OUTPUT_AUDIO_CODEC,
    "-movflags", config.OUTPUT_MOVFLAGS,
    str(output_path),
])
```

Не додавати `-t` або `-shortest` — тривалість визначається body.mp4.

---

### 3. `src/render.py` — нова функція `render_myth_story_video`

Обгортка-оркестратор для myth pipeline (аналог `render_story_video`):

```python
def render_myth_story_video(
    background_video_path: Path,
    audio_path: Path,
    output_path: Path,
    subtitle_path: Path | None = None,
    hook_text: str | None = None,
    platform: str = "tiktok",
    hook_bg_override: str | None = None,
    hook_accent_override: str | None = None,
    hook_brand_override: str | None = None,
    category_override: str = "",
    ffmpeg_path: str = config.FFMPEG_PATH,
) -> Path:
```

Логіка:
1. `_validate_render_inputs(...)`
2. `duration_seconds = _probe_media_duration(audio_path)`
3. Вибрати CTA params з config залежно від `platform` (tiktok / instagram / youtube)
4. Якщо `HOOK_FRAME_ENABLED and hook_text`:
   - `_render_hook_frame(...)` → `hook_{platform}.mp4`
   - `_render_body(..., part_number=None, total_parts=None)` → `body_{platform}.mp4`
   - `_build_myth_ffmpeg_command(...)` → output_path
   - cleanup: видалити `hook_{platform}.mp4` і `body_{platform}.mp4` в блоці `finally`
5. Fallback якщо hook вимкнено: перевикористати `render_story_video` без CTA
6. Повернути `output_path`

Вибір CTA params:

```python
if platform == "youtube":
    share_cta_path = None
    subscribe_cta_path = config.CTA_SUBSCRIBE_YOUTUBE_PATH
    subscribe_cta_y_offset = config.CTA_SUBSCRIBE_Y_OFFSET
elif platform == "instagram":
    share_cta_path = config.CTA_SHARE_PATH
    subscribe_cta_path = config.CTA_SUBSCRIBE_INSTAGRAM_PATH
    subscribe_cta_y_offset = config.CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET
else:  # tiktok
    share_cta_path = config.CTA_SHARE_PATH
    subscribe_cta_path = config.CTA_SUBSCRIBE_TIKTOK_PATH
    subscribe_cta_y_offset = config.CTA_SUBSCRIBE_Y_OFFSET
```

---

### 4. `src/myth_pipeline.py` — `render_myth_video`

Додати параметр `platform: str = "tiktok"`. Замінити виклик `render_story_video` на `render_myth_story_video`. Прибрати хардкод `part_number=1, total_parts=2, cta_start_offset=10.0`.

Поточна сигнатура (рядок 138):
```python
def render_myth_video(
    script_text: str,
    audio_path: Path,
    output_path: Path,
    channel: str,
    part_number: int = 1,
    total_parts: int = 1,
    category: str = "",
) -> Path:
```

Нова сигнатура:
```python
def render_myth_video(
    script_text: str,
    audio_path: Path,
    output_path: Path,
    channel: str,
    category: str = "",
    platform: str = "tiktok",
) -> Path:
```

Замінити виклик `render_story_video` (рядки 182-198) на:
```python
from src.render import render_myth_story_video

result = render_myth_story_video(
    background_video_path=bg_video,
    audio_path=audio_path,
    output_path=output_path,
    subtitle_path=subtitle_path,
    hook_text=hook_text or None,
    platform=platform,
    hook_bg_override=profile.get("hook_bg"),
    hook_accent_override=profile.get("hook_accent"),
    hook_brand_override=profile.get("hook_brand") or "",
    category_override=category,
)
```

---

### 5. `myth_render.py`

`--output` тепер base path. Видалити параметри `--part` і `--total-parts` (вони більше не потрібні). Рендерити 3 файли:

```python
base = Path(args.output).with_suffix("")
for platform in ["tiktok", "instagram", "youtube"]:
    out = base.parent / f"{base.name}_{platform}.mp4"
    result = render_myth_video(
        script_text=script_text,
        audio_path=audio_path,
        output_path=out,
        channel=args.channel,
        category=args.category,
        platform=platform,
    )
    size_mb = result.stat().st_size / 1_000_000
    print(f"✅ {platform}: {result}  ({size_mb:.1f} MB)")
```

---

## Обмеження

- **НЕ чіпати** `render_story_video` і `_build_ffmpeg_render_command` — advice pipeline використовує їх без змін
- **НЕ чіпати** `CTA_SHARE_OVERLAY_PATH`, `CTA_SHARE_OVERLAY_WIDTH`, `CTA_SHARE_OVERLAY_Y` — залишаються для advice pipeline
- Старий Card-режим (lavfi subscribe card) в `_build_ffmpeg_render_command` залишається в коді — буде прибраний окремо
- `CTA_ENABLED` env var ігнорується для myth pipeline — CTA завжди включено
- `subscribe_instagram.mp4` має власне аудіо — воно не повинно потрапляти у вихід через `-map 0:a`

---

## Тести

1. `make test` — всі тести зелені
2. Записати голос для `bron-oblik` через `/myth bron-oblik` в боті (або використати наявний `voiceover.wav` якщо є)
3. Запустити:
   ```bash
   cd /Users/a111/Projects/ttcf
   python myth_render.py \
     --script data/myth/bron-oblik/script.txt \
     --audio data/myth/bron-oblik/voiceover.wav \
     --channel law \
     --output /tmp/bron-oblik.mp4
   ```
4. З'явились 3 файли: `/tmp/bron-oblik_tiktok.mp4`, `/tmp/bron-oblik_instagram.mp4`, `/tmp/bron-oblik_youtube.mp4`
5. Перевірити зором кожен:
   - TikTok: "Поділись" @ ~10s у верхній зоні, "Підпишись" в кінці вгорі
   - Instagram: "Поділись" @ ~10s, "Підпишись" в кінці вгорі (без звуку, ~4 сек)
   - YouTube: тільки "Підпишись" в кінці вгорі
6. Якщо кнопки не на правильній висоті → налаштувати через env vars та повторити рендер:
   - `CTA_SHARE_Y_OFFSET` (default -1032)
   - `CTA_SUBSCRIBE_Y_OFFSET` (default -935)
   - `CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET` (default -648)
