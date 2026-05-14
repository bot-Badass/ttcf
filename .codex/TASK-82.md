# TASK-82 — myth pipeline: автоматичний export після рендеру

## Задача

Зараз після успішного myth рендеру бот шукає *існуючу* папку в `data/exports/` за шаблоном `*_{slug}`. Якщо папки немає — відео залишаються в `/tmp` і зникають при перезавантаженні. `metadata.csv` взагалі не генерується.

Потрібно: бот після успішного рендеру *сам створює* export папку, копіює відео і переносить `metadata.csv` який Duke написав заздалегідь.

**Важливо:** metadata.csv пишеться Duke вручну в `data/myth/{slug}/metadata.csv` з platform-specific captions (TikTok/YouTube/Instagram за правилами). Бот тільки копіює готовий файл — не генерує вміст.

---

## Що реалізувати

### `src/telegram_bot.py` — `_find_myth_export_dir` → замінити на `_get_or_create_myth_export_dir`

Видалити стару функцію `_find_myth_export_dir`. Замінити новою:

```python
def _get_or_create_myth_export_dir(channel_key: str, slug: str) -> Path:
    """Return export dir for a myth slug, creating it if needed.

    Naming: data/exports/{channel}_{slug}/
    Example: data/exports/moneyua_monobank-ivr/
    """
    from src.publer_export import EXPORTS_ROOT
    dir_name = f"{channel_key}_{slug}"
    export_dir = EXPORTS_ROOT / dir_name
    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir
```

### `src/telegram_bot.py` — callback `myth_render:` після успішного рендеру

Замінити блок після `if proc.returncode == 0:`:

```python
if proc.returncode == 0:
    channel_key = _active_channel.get(chat_id, config.DEFAULT_CHANNEL)
    export_dir = _get_or_create_myth_export_dir(channel_key, slug)

    import shutil as _shutil
    base = base_output.with_suffix("")

    # копіювати відео
    for platform in ["tiktok", "youtube", "instagram"]:
        src = base.parent / f"{base.name}_{platform}.mp4"
        if src.is_file():
            dst = export_dir / f"{export_dir.name}_{platform}.mp4"
            _shutil.copy2(src, dst)

    # копіювати metadata.csv якщо Duke написав його заздалегідь
    meta_src = config.MYTH_DATA_DIR / slug / "metadata.csv"
    if meta_src.is_file():
        _shutil.copy2(meta_src, export_dir / "metadata.csv")

    send_message(
        chat_id,
        f"✅ Відео і metadata.csv скопійовані в:\n`{export_dir}`",
        None,
    )
```

---

## Обмеження

- НЕ генерувати вміст metadata.csv в боті — файл пишеться Duke вручну в `data/myth/{slug}/metadata.csv` до рендеру
- Якщо `metadata.csv` ще немає — бот копіює тільки відео і не падає з помилкою (умова `if meta_src.is_file()`)
- Старий fallback "export папку не знайдено" з повідомленням `/tmp/` шляхів — видалити

---

## Тести

1. `make test` — всі тести зелені (нових тестів не потрібно — логіка копіювання файлів)
2. Перевірити вручну: рендер myth з монобанк-ivr → в `data/exports/moneyua_monobank-ivr/` мають з'явитись 3 mp4 і metadata.csv
