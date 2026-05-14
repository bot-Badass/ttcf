# TASK-77 — Виправлення позицій CTA у myth render

## Задача

Після тестового рендеру виявлено три проблеми:
1. Share CTA ("Поділись") відсутній у YouTube-версії.
2. Subscribe CTA на YouTube притиснутий до верху — треба опустити. Вирішення: одне значення Y для всіх платформ, трохи нижче поточного.
3. Share CTA відображається зверху — треба перенести в нижню половину екрану (орієнтовно 75-80% висоти від верху).

---

## Що реалізувати

### `src/config.py`

**Правка 1:** замінити значення за замовчуванням `CTA_SUBSCRIBE_Y_OFFSET` з `-785` на `-680` (менш від'ємне = нижче на екрані):

```python
CTA_SUBSCRIBE_Y_OFFSET: Final[int] = int(os.getenv("CTA_SUBSCRIBE_Y_OFFSET", "-680"))
```

**Правка 2:** видалити або занулити `CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET` — замість нього всі платформи використовують один `CTA_SUBSCRIBE_Y_OFFSET`:

```python
CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET: Final[int] = int(
    os.getenv("CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET", str(CTA_SUBSCRIBE_Y_OFFSET))
)
```

*(тобто за замовчуванням = той самий загальний offset, але змінна залишається для зворотної сумісності)*

**Правка 3:** додати два нових рядки для позиціювання Share CTA через ffmpeg-вирази:

```python
CTA_SHARE_Y_EXPR: Final[str] = os.getenv("CTA_SHARE_Y_EXPR", "main_h*4/5-overlay_h/2")
CTA_SHARE_X_EXPR: Final[str] = os.getenv("CTA_SHARE_X_EXPR", "(main_w-overlay_w)/2")
```

`main_h*4/5` = 80% висоти відео = трохи нижче середини нижньої половини екрану (для 1920px = 1536px).
`-overlay_h/2` центрує кліп вертикально відносно цієї точки.
`(main_w-overlay_w)/2` центрує горизонтально.

---

### `src/render.py`

**Правка 4:** увімкнути share CTA для YouTube у `render_myth_story_video` (~рядок 169):

```python
if platform == "youtube":
    share_cta_path: str | None = config.CTA_SHARE_PATH
    subscribe_cta_path = config.CTA_SUBSCRIBE_YOUTUBE_PATH
    subscribe_cta_y_offset = config.CTA_SUBSCRIBE_Y_OFFSET
```

*(було `share_cta_path = None` — тепер так само як TikTok і Instagram)*

**Правка 5:** передати нові expr-параметри у виклик `_build_myth_ffmpeg_command` (~рядок 215):

```python
cmd = _build_myth_ffmpeg_command(
    ...
    share_cta_path=share_cta_path,
    share_cta_y_expr=config.CTA_SHARE_Y_EXPR,
    share_cta_x_expr=config.CTA_SHARE_X_EXPR,
    share_cta_start=config.CTA_SHARE_START,
    ...
)
```

Старий `share_cta_y_offset` параметр прибрати з виклику (і з сигнатури якщо більше не потрібен).

**Правка 6:** оновити сигнатуру і filter_complex у `_build_myth_ffmpeg_command` (~рядок 244):

Сигнатура — замінити `share_cta_y_offset: int` на два нових параметри:
```python
share_cta_y_expr: str,
share_cta_x_expr: str,
```

Рядок overlay для share CTA (~рядок 280) — замінити:
```python
# було:
f"[body_hooked][share_ol]overlay=0:{share_cta_y_offset}:enable='between(t,{share_cta_start},{share_end})'[body1]",

# стало:
f"[body_hooked][share_ol]overlay={share_cta_x_expr}:{share_cta_y_expr}:enable='between(t,{share_cta_start},{share_end})'[body1]",
```

---

## Обмеження

- НЕ чіпати advice pipeline (~рядки 900+) — там окремий CTA механізм з іншими змінними
- `CTA_SHARE_Y_OFFSET` (старий int-параметр) можна залишити в config.py як unused — але з сигнатури `_build_myth_ffmpeg_command` прибрати

---

## Тести

1. `make test` — всі тести зелені
2. Рендер TikTok-версії: share CTA з'являється в нижній частині екрану по центру на ~10с; subscribe CTA зверху в кінці, трохи нижче ніж раніше
3. Рендер YouTube-версії: share CTA присутній (раніше був відсутній), позиція та сама що TikTok; subscribe CTA нижче ніж раніше
4. Рендер Instagram-версії: subscribe CTA на тій самій висоті що TikTok і YouTube (одне значення)
5. Перевірити що `CTA_SHARE_Y_EXPR`, `CTA_SHARE_X_EXPR`, `CTA_SUBSCRIBE_Y_OFFSET` можна перевизначити через `.env`
