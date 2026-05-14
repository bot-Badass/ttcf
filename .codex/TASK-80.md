# TASK-80 — CTA офсети Instagram і YouTube

## Задача

Частина TASK-79 не була реалізована. Instagram subscribe CTA обрізається зверху (offset -680 = ассет вище за кадр). YouTube subscribe CTA треба опустити нижче TikTok. Потрібні два незалежні офсети.

---

## Що реалізувати

### `src/config.py`

**Правка 1:** змінити default `CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET` з аліасу на фіксоване -560:

```python
# було:
CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET: Final[int] = int(
    os.getenv("CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET", str(CTA_SUBSCRIBE_Y_OFFSET))
)

# стало:
CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET: Final[int] = int(
    os.getenv("CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET", "-560")
)
```

**Правка 2:** додати новий var для YouTube після існуючих CTA змінних:

```python
CTA_SUBSCRIBE_YOUTUBE_Y_OFFSET: Final[int] = int(
    os.getenv("CTA_SUBSCRIBE_YOUTUBE_Y_OFFSET", "-580")
)
```

---

### `src/render.py`

**Правка 3:** у `render_myth_story_video`, гілка `platform == "youtube"` (~рядок 169):

```python
# було:
subscribe_cta_y_offset = config.CTA_SUBSCRIBE_Y_OFFSET

# стало:
subscribe_cta_y_offset = config.CTA_SUBSCRIBE_YOUTUBE_Y_OFFSET
```

---

## Обмеження

- TikTok залишається на `CTA_SUBSCRIBE_Y_OFFSET = -680` — не чіпати
- -560 і -580 стартові значення — оператор тюнить через `.env` без зміни коду

---

## Тести

1. `make test` — всі тести зелені
2. Тестовий рендер: Instagram CTA повністю у кадрі (не обрізається зверху)
3. Тестовий рендер: YouTube CTA нижче ніж TikTok
4. TikTok поведінка не змінилась
