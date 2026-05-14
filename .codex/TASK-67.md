# TASK-67 — CTA "Поділись" для не-фінальних частин серії

## Задача

Зараз CTA завжди показує "Підпишись" (card або overlay залежно від каналу). Потрібно змінити логіку: для частин 1..N-1 показувати анімовану кнопку "Поділись" (green-screen overlay), а для фінальної частини N/N — залишити поточний "Підпишись" без змін. Актив вже збережено: `data/assets/cta_share_gs.mp4` (4 секунди анімації).

---

## Що реалізувати

### `src/config.py` — три нові параметри після блоку CTA

Додати після рядка `CTA_FADE_IN`:

```python
CTA_SHARE_OVERLAY_PATH: str = os.getenv(
    "CTA_SHARE_OVERLAY_PATH",
    str(BASE_DIR / "data" / "assets" / "cta_share_gs.mp4"),
)
CTA_SHARE_OVERLAY_WIDTH: int = int(os.getenv("CTA_SHARE_OVERLAY_WIDTH", "500"))
CTA_SHARE_OVERLAY_Y: int = int(os.getenv("CTA_SHARE_OVERLAY_Y", "1050"))
```

---

### `src/render.py` — логіка вибору CTA в `_build_ffmpeg_render_command`

Знайти блок (рядок ~785):
```python
if config.CTA_ENABLED and part_number is not None and total_parts is not None:
    t0 = cta_t0 if cta_t0 is not None else duration_seconds - config.CTA_DURATION
    body_chain = ",".join(vf_parts)

    if cta_overlay_path:
        # Overlay mode ...
```

Замінити на:

```python
if config.CTA_ENABLED and part_number is not None and total_parts is not None:
    t0 = cta_t0 if cta_t0 is not None else duration_seconds - config.CTA_DURATION
    body_chain = ",".join(vf_parts)

    is_last_part = part_number >= total_parts
    use_share_cta = not is_last_part and config.CTA_SHARE_OVERLAY_PATH

    if use_share_cta:
        # Share CTA overlay: green-screen "Поділись" button for parts 1..N-1
        # Button is centered in 1920x1080 source: crop the button region, chromakey, scale.
        share_path = config.CTA_SHARE_OVERLAY_PATH
        share_w = config.CTA_SHARE_OVERLAY_WIDTH
        share_y = config.CTA_SHARE_OVERLAY_Y
        filter_complex = (
            f"[0:v]{body_chain}[body];"
            f"[2:v]crop=720:150:370:510,"
            f"colorkey=0x00ff00:0.35:0.1,"
            f"scale={share_w}:-1[share_ol];"
            f"[body][share_ol]overlay=x=(W-w)/2:y={share_y}"
            f":enable='gte(t,{t0:.3f})'[outv]"
        )
        cmd = _base_cmd(input_path, share_path, t0) + _audio_cmd(audio_path) + [
            "-filter_complex", filter_complex,
            "-map", "[outv]", "-map", "1:a",
        ]
    elif cta_overlay_path:
        # Per-channel subscribe overlay (finance channel)
        ...
```

**Важливо:** функція `_base_cmd` або аналогічна має прийняти другий відео-вхід (share_path) — подивись як це зроблено для `cta_overlay_path` і повтори ту саму патерн. Не дублюй код — виклич існуючий хелпер або додай параметр.

---

## Обмеження

- НЕ чіпати логіку фінальної частини (N/N) — там залишається існуючий card або overlay режим без змін
- НЕ чіпати moneyua-специфічні параметри `cta_overlay_path`, `cta_overlay_width`, `cta_overlay_y` — вони далі використовуються для фінальної частини
- Файл `data/assets/cta_share_gs.mp4` вже є, не треба його створювати

---

## Тести

1. `make test` — всі тести зелені
2. Зрендерити серію з 3 частин:
   - Ч.1 і ч.2: в останні 3 секунди з'являється червона кнопка "Поділись"
   - Ч.3 (фінальна): показується існуючий CTA ("Підпишись" card або overlay залежно від каналу)
