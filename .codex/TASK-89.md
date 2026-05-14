# TASK-89 — Жорстке розмежування каналів при рендері: slug → канал, завжди

## Задача

Юридичний контент (без префікса) регулярно потрапляє в `moneyua_content_dir`, а фінансовий — навпаки. Причина: при рендері `channel_key` береться з `_active_channel[chat_id]` (стан сесії), а не з самого slug. Якщо оператор останнім записував фінансове відео, всі наступні рендери йдуть у фінансову папку незалежно від slug. Виправити так, щоб канал завжди визначався зі slug — це єдине джерело правди.

---

## Що реалізувати

### `src/telegram_bot.py` — функція `_execute_render`, рядок ~203

Замінити визначення `channel_key` з сесійного стану на виклик `slug_to_channel`:

**Було:**
```python
channel_key = _active_channel.get(chat_id, config.DEFAULT_CHANNEL)
```

**Стало:**
```python
from src.myth_queue import slug_to_channel
channel_key = slug_to_channel(slug)
```

Це єдина зміна. Далі по функції `channel_key` вже використовується правильно — для `--channel` прапора рендеру і для `_get_or_create_myth_export_dir`.

---

### `src/myth_queue.py` — функція `slug_to_channel` (перевірити, не змінювати)

Функція вже існує і правильно реалізована:

```python
def slug_to_channel(slug: str) -> str:
    """Detect channel key from slug prefix. finance_* → finance, else → law."""
    return "finance" if slug.startswith("finance_") else "law"
```

Нічого не змінювати — вона вже є джерелом правди.

---

## Обмеження

- НЕ чіпати логіку `_active_channel` в інших місцях (вибір черги, відображення UI, переключення каналу оператором) — там сесійний стан потрібен для UX
- НЕ змінювати `slug_to_channel` — вона вже правильна
- НЕ чіпати `_get_or_create_myth_export_dir` — вона правильна
- Зміна лише в `_execute_render`, один рядок

---

## Тести

1. `make test` — всі тести зелені
2. Перевірити вручну сценарій-тригер помилки:
   - Записати голос для `finance_*` slug → відрендерити → переконатись що в `moneyua_content_dir`
   - Одразу після цього записати голос для slug без префікса (напр. `politsia-102-zatrymannya`) → відрендерити → переконатись що в `dontpaniclaw_content_dir`, НЕ в `moneyua_content_dir`
3. Зворотній сценарій: спочатку law, потім finance — результат той самий, кожен у свою папку
