# TASK-83 — myth render: persist active_channel + виправити дефолт категорії

## Задача

Два баги що разом дають неправильний брендинг при myth-рендері:

1. `_active_channel` — in-memory dict, порожній після перезапуску бота. Fallback на `DEFAULT_CHANNEL = "law"` → myth відео для moneyua рендеряться з red accent і "МОБІЛІЗАЦІЯ" лейблом.
2. `myth_render.py --category` має дефолт `"МОБІЛІЗАЦІЯ"` незалежно від `--channel`. Навіть якщо канал правильний — категорія залишається неправильною.

---

## Що реалізувати

### Fix 1 — `src/telegram_bot.py`: persist `_active_channel` на диск

Замість голого dict — читати/писати з `data/active_channels.json`.

Додати дві helper-функції поруч з `_active_channel`:

```python
import json as _json

_ACTIVE_CHANNELS_PATH = config.DATA_DIR / "active_channels.json"

def _load_active_channels() -> dict[str, str]:
    if _ACTIVE_CHANNELS_PATH.is_file():
        try:
            return _json.loads(_ACTIVE_CHANNELS_PATH.read_text())
        except Exception:
            return {}
    return {}

def _save_active_channels(d: dict[str, str]) -> None:
    _ACTIVE_CHANNELS_PATH.write_text(_json.dumps(d))
```

Ініціалізувати dict при старті:

```python
# було:
_active_channel: dict[str, str] = {}

# стало:
_active_channel: dict[str, str] = _load_active_channels()
```

При кожному записі в `_active_channel` — одразу зберігати на диск. Знайти рядок де `_active_channel[chat_id] = channel_key` (callback `channel:`, рядок ~768) і додати після:

```python
_active_channel[chat_id] = channel_key
_save_active_channels(_active_channel)
```

---

### Fix 2 — `myth_render.py`: змінити дефолт `--category`

```python
# було:
parser.add_argument("--category", default="МОБІЛІЗАЦІЯ",
                    help="Hook frame category label (default: МОБІЛІЗАЦІЯ)")

# стало:
parser.add_argument("--category", default="",
                    help="Hook frame category label (empty = no pill)")
```

Це означає: myth-bust відео без явно переданого `--category` не показуватиме категорійний лейбл. Категорія для myth-bust не потрібна — це standalone відео без серії.

---

## Обмеження

- НЕ чіпати advice pipeline — там категорія береться з `series_id` і `CHANNEL_PROFILES[key]["series_categories"]`, це інший шлях
- НЕ змінювати структуру `CHANNEL_PROFILES` в `config.py`
- `data/active_channels.json` не потрібно коміттити в git (додати в `.gitignore` якщо ще немає)

---

## Тести

1. `make test` — всі тести зелені
2. Перезапустити бота, НЕ клікати вибір каналу → `/myth test-slug` з moneyua → перевірити що рендер використовує `finance` (якщо останній збережений канал був moneyua)
3. Перевірити що hook frame не показує категорійний лейбл (порожня категорія)
