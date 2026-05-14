# TASK-78 — Hook frame: замінити color tint на нейтральний scrim

## Задача

Зараз hook frame з відео-фоном виглядає майже чорним через два агресивні фільтри:
1. `eq=brightness=-0.42:contrast=0.72` — сильне затемнення відео
2. `drawbox color={bg}@0.55` — суцільний темно-синій кольоровий тінт поверх

Треба замінити на легкий напівпрозорий чорний scrim (40-50% opacity) щоб фонове відео залишалось видимим і thumbnail виглядав живим, а не чорною картинкою з текстом.

---

## Що реалізувати

### `src/config.py` — одна нова змінна

Додати після існуючих `HOOK_FRAME_*` змінних:

```python
HOOK_FRAME_SCRIM_OPACITY: Final[float] = float(
    os.getenv("HOOK_FRAME_SCRIM_OPACITY", "0.45")
)
```

---

### `src/render.py` — правка у `_render_hook_frame_v2`, блок `use_video_bg` (~рядок 593)

Замінити поточний `vf` для відео-фону:

```python
# БУЛО:
vf = (
    f"scale=1080:1920:force_original_aspect_ratio=decrease,"
    f"pad=1080:1920:(ow-iw)/2:(oh-ih)/2,"
    f"eq=brightness=-0.42:contrast=0.72:saturation=0.5,"
    f"drawbox=x=0:y=0:w=1080:h=1920"
    f":color={bg}@0.55:t=fill,"
    + overlay_chain
)

# СТАЛО:
vf = (
    f"scale=1080:1920:force_original_aspect_ratio=decrease,"
    f"pad=1080:1920:(ow-iw)/2:(oh-ih)/2,"
    f"eq=brightness=-0.12:contrast=0.95:saturation=0.85,"
    f"drawbox=x=0:y=0:w=1080:h=1920"
    f":color=black@{config.HOOK_FRAME_SCRIM_OPACITY}:t=fill,"
    + overlay_chain
)
```

**Що змінюється і чому:**
- `brightness=-0.42` → `-0.12`: набагато м'якше затемнення, відео залишається видимим
- `contrast=0.72` → `0.95`: прибираємо штучне знебарвлення
- `saturation=0.5` → `0.85`: відновлюємо кольори відео
- `color={bg}@0.55` → `color=black@{scrim_opacity}`: нейтральний чорний scrim замість кольорового тінту серії. Колір теми (синій/жовтий) передається через accent line і category pill — не через фон.

---

## Обмеження

- НЕ чіпати fallback гілку (solid colour background, ~рядок 619) — вона для випадків коли немає відео
- НЕ чіпати finance layout (`is_finance` гілку overlays) — тільки vf рядок
- НЕ чіпати classic hook frame (`_render_hook_frame_classic`)
- Значення eq можна тонко підкоригувати після тестового рендеру — головне не повертатись до -0.42

---

## Тести

1. `make test` — всі тести зелені
2. Тестовий рендер myth відео: hook frame має видиме фонове відео, текст читається, фон не чорний
3. Перевірити що `HOOK_FRAME_SCRIM_OPACITY` можна перевизначити через `.env` (наприклад `0.35` для більш прозорого або `0.6` для темнішого)
4. Рендер без відео-фону (fallback) — поведінка не змінилась
