# TASK-66 — Зробити бейдж частини (3/4) керованим через конфіг і вимкнути за замовчуванням

## Задача

У правому куті відео зараз завжди горить бейдж "3/4" (номер частини / загальна кількість). Він вбитий у відео через ffmpeg drawtext при рендері. Після аналізу аналітики: цей бейдж на TikTok сигналізує новому глядачеві що це продовження — і він скролить не дивлячись. Потрібно зробити бейдж опціональним через env var і вимкнути за замовчуванням.

---

## Що реалізувати

### `src/config.py` — новий параметр після блоку HOOK_FRAME

Додати після рядка `HOOK_FRAME_CATEGORY_FG` (рядок ~187):

```python
PART_BADGE_ENABLED: Final[bool] = _parse_bool_env(
    os.getenv("PART_BADGE_ENABLED", "false")
)
```

---

### `src/render.py` — обгорнути drawtext бейджа умовою

Зараз (рядки 770-782):

```python
if part_number is not None and total_parts is not None:
    badge_text = f"{part_number}/{total_parts}"
    font_file = config.HOOK_FRAME_FONT_FILE
    font_opt = f":fontfile='{font_file}'" if font_file else ""
    vf_parts.append(
        f"drawtext=text='{badge_text}'{font_opt}"
        f":fontsize=38:fontcolor=white"
        f":x=w-text_w-40:y=50"
        f":borderw=2:bordercolor=black@0.8"
        f":box=1:boxcolor=black@0.55:boxborderw=14"
    )
```

Замінити на:

```python
if part_number is not None and total_parts is not None and config.PART_BADGE_ENABLED:
    badge_text = f"{part_number}/{total_parts}"
    font_file = config.HOOK_FRAME_FONT_FILE
    font_opt = f":fontfile='{font_file}'" if font_file else ""
    vf_parts.append(
        f"drawtext=text='{badge_text}'{font_opt}"
        f":fontsize=38:fontcolor=white"
        f":x=w-text_w-40:y=50"
        f":borderw=2:bordercolor=black@0.8"
        f":box=1:boxcolor=black@0.55:boxborderw=14"
    )
```

---

## Обмеження

- НЕ чіпати логіку CTA-картки з текстом "Ч.2/4 — виходить завтра" (рядки 816-829) — це інший елемент, залишаємо
- НЕ чіпати інші параметри рендеру
- НЕ змінювати дефолтне значення на `true` — має бути `false`

---

## Тести

1. `make test` — всі тести зелені
2. Зрендерити тестову сесію без `PART_BADGE_ENABLED=true` — бейдж у правому куті відсутній
3. Зрендерити з `PART_BADGE_ENABLED=true` — бейдж з'являється як раніше
