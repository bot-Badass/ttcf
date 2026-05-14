# TASK-65 — CTA bottom bar overlay в кінці відео

## Задача

Додати у фінальне відео плашку підписки (bottom bar), яка плавно fade-in з'являється в останні 3 секунди — overlay поверх відео поки ще йде озвучка.

---

## Що реалізувати

### `src/config.py`

Додати три змінні після блоку хук-фрейму:

```python
CTA_ENABLED: bool = os.getenv("CTA_ENABLED", "false").lower() == "true"
CTA_DURATION: float = float(os.getenv("CTA_DURATION", "3.0"))
CTA_FADE_IN: float = float(os.getenv("CTA_FADE_IN", "0.5"))
```

### `src/render.py` — `_build_ffmpeg_render_command`

Після блоку part badge (після рядка з `vf_parts.append(f"drawtext=text='{badge_text}'...")`), додати CTA overlay:

```python
if config.CTA_ENABLED and part_number is not None and total_parts is not None:
    t0 = duration_seconds - config.CTA_DURATION
    fade = config.CTA_FADE_IN
    font_file = config.HOOK_FRAME_FONT_FILE
    font_opt = f":fontfile='{font_file}'" if font_file else ""
    en = f"gte(t\\,{t0:.3f})"

    def _alpha(cap: float) -> str:
        # fade-in від 0 до cap за CTA_FADE_IN секунд починаючи з t0
        return f"min({cap}\\,max(0\\,(t-{t0:.3f})/{fade:.3f})*{cap})"

    next_part = part_number + 1
    if next_part <= total_parts:
        sub_text = f"Ch.{next_part}/{total_parts} — coming tomorrow"
    else:
        sub_text = "Series complete"

    vf_parts += [
        f"drawbox=x=0:y=1580:w=1080:h=340:color=black@{_alpha(0.92)}:t=fill:enable='{en}'",
        f"drawbox=x=0:y=1580:w=1080:h=6:color=0xFF2D2D@{_alpha(1.0)}:t=fill:enable='{en}'",
        f"drawtext=text='Підпишись'{font_opt}:fontsize=68:fontcolor=white@{_alpha(1.0)}:x=70:y=1622:enable='{en}'",
        f"drawtext=text='{sub_text}'{font_opt}:fontsize=48:fontcolor=0xAAAAAA@{_alpha(1.0)}:x=70:y=1715:enable='{en}'",
        f"drawtext=text='@dontpaniclaw'{font_opt}:fontsize=44:fontcolor=0xFF5555@{_alpha(1.0)}:x=70:y=1797:enable='{en}'",
    ]
```

**Примітка:** `_alpha()` — локальна функція всередині `if`-блоку, не виносити.  
`sub_text` не містить кириличного апострофа — перевір що ffmpeg не падає на `'` в тексті.

---

## Обмеження

- Не чіпати `_render_hook_frame`, `_render_body`, `_concat_hook_and_body`
- Не додавати concat або окремий сегмент — тільки overlay у `-vf`
- CTA не рендерити якщо `part_number is None` (Reddit/single-part)
- `sub_text` для останньої частини серії: `"Series complete"` (без @-хендлу)
- Всі нові змінні лише через `config.py`, без `os.getenv` в `render.py`

---

## Тести

Після реалізації:
1. `make test` — всі тести зелені
2. Перевірити вручну: `CTA_ENABLED=true` в `.env`, зрендерити будь-яке відео з `part_number=2, total_parts=4` — плашка з'являється в останні 3 сек з fade-in
3. Перевірити з `part_number=4, total_parts=4` — "Series complete" замість наступної частини
4. Перевірити з `CTA_ENABLED=false` (default) — нічого не змінилося
