# TASK-79 — Прогрес рендеру + копіювання відео в export + CTA офсети

## Задача

Три незалежні покращення:

1. **Прогрес рендеру в Telegram** — бот падає по таймауту (30 хв) бо `subprocess.run` з `capture_output=True` мовчить до кінця. Потрібен стрімінг прогресу по платформах.
2. **Копіювання відео в export** — після рендеру відео залишаються в `/tmp`, треба автоматично копіювати поряд з CSV в `data/exports/`.
3. **CTA офсети Instagram і YouTube** — Instagram subscribe CTA обрізається зверху (ассет вищий за TikTok, але offset однаковий -680). YouTube CTA треба опустити нижче TikTok.

---

## Що реалізувати

### `myth_render.py` — додати print-прогрес по платформах

Знайти місце де запускається рендер для кожної платформи (tiktok / youtube / instagram) і додати print-рядки до і після:

```python
print("PROGRESS: tiktok", flush=True)
# ... рендер tiktok ...
print("DONE: tiktok", flush=True)

print("PROGRESS: youtube", flush=True)
# ... рендер youtube ...
print("DONE: youtube", flush=True)

print("PROGRESS: instagram", flush=True)
# ... рендер instagram ...
print("DONE: instagram", flush=True)
```

`flush=True` обов'язковий — щоб рядки одразу потрапляли в pipe, а не буферизувались.

---

### `src/telegram_bot.py` — замінити `subprocess.run` на `Popen` зі стрімінгом (~рядок 974)

Замінити поточний блок `_do_render`:

```python
# БУЛО:
def _do_render(...):
    try:
        proc = subprocess.run(
            [...],
            capture_output=True,
            text=True,
            timeout=1800,
        )
        if proc.returncode == 0:
            send_message(chat_id, f"✅ Готово:\n{paths}", None)
        else:
            send_message(chat_id, f"❌ Помилка рендеру:\n```{proc.stderr[-500:]}```", None)
    except subprocess.TimeoutExpired:
        send_message(chat_id, "❌ Рендер завис (timeout 30 хв)", None)

# СТАЛО:
def _do_render(...):
    platform_labels = {
        "tiktok": "TikTok",
        "youtube": "YouTube",
        "instagram": "Instagram",
    }
    stderr_lines: list[str] = []
    try:
        proc = subprocess.Popen(
            [
                "python", "myth_render.py",
                "--script", str(script_path),
                "--audio", str(wav_path),
                "--channel", "law",
                "--output", str(base_output),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Читаємо stderr в окремому потоці щоб не блокувати
        import threading as _threading
        def _read_stderr():
            for line in proc.stderr:
                stderr_lines.append(line)
        _threading.Thread(target=_read_stderr, daemon=True).start()

        # Читаємо stdout і надсилаємо прогрес
        for line in proc.stdout:
            line = line.strip()
            if line.startswith("PROGRESS: "):
                platform = line.split("PROGRESS: ", 1)[1].strip()
                label = platform_labels.get(platform, platform)
                send_message(chat_id, f"⏳ Рендеримо {label}...", None)
            elif line.startswith("DONE: "):
                platform = line.split("DONE: ", 1)[1].strip()
                label = platform_labels.get(platform, platform)
                send_message(chat_id, f"✅ {label} готово", None)

        proc.wait()

        if proc.returncode == 0:
            base = base_output.with_suffix("")
            # Копіюємо відео в export папку поряд з CSV
            export_dir = _find_myth_export_dir(slug)
            if export_dir is not None:
                for platform in ["tiktok", "youtube", "instagram"]:
                    src = Path(f"{base}_{platform}.mp4")
                    if src.is_file():
                        dst = export_dir / f"{export_dir.name}_{platform}.mp4"
                        import shutil as _shutil
                        _shutil.copy2(src, dst)
                send_message(
                    chat_id,
                    f"✅ Всі відео готові і скопійовані в:\n`{export_dir}`",
                    None,
                )
            else:
                paths = "\n".join(
                    f"`{base}_{p}.mp4`" for p in ["tiktok", "youtube", "instagram"]
                )
                send_message(chat_id, f"✅ Відео готові (export папку не знайдено):\n{paths}", None)
        else:
            stderr_tail = "".join(stderr_lines)[-500:]
            send_message(chat_id, f"❌ Помилка рендеру:\n```{stderr_tail}```", None)

    except Exception as exc:
        send_message(chat_id, f"❌ Помилка: {exc}", None)
```

**Без timeout** — `Popen` без `timeout`, процес йде до завершення. Природний ліміт — якщо процес завис, оператор бачить що прогрес зупинився і може зупинити вручну.

---

### `src/telegram_bot.py` — додати хелпер `_find_myth_export_dir`

Додати функцію поряд з `_do_render` (або вище по файлу):

```python
def _find_myth_export_dir(slug: str) -> Path | None:
    """Find export directory for a myth slug (contains _{slug} at end or _{slug}_)."""
    from src.publer_export import EXPORTS_ROOT
    if not EXPORTS_ROOT.is_dir():
        return None
    for d in EXPORTS_ROOT.iterdir():
        if d.is_dir() and (d.name.endswith(f"_{slug}") or f"_{slug}_" in d.name):
            return d
    return None
```

---

### `src/config.py` — два нових CTA офсети

**Правка 4:** змінити default `CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET` (зараз аліасує -680, треба -560):

```python
CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET: Final[int] = int(
    os.getenv("CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET", "-560")
)
```

**Правка 5:** додати новий var для YouTube:

```python
CTA_SUBSCRIBE_YOUTUBE_Y_OFFSET: Final[int] = int(
    os.getenv("CTA_SUBSCRIBE_YOUTUBE_Y_OFFSET", "-580")
)
```

---

### `src/render.py` — використати YouTube-офсет (~рядок 169)

```python
if platform == "youtube":
    share_cta_path = config.CTA_SHARE_PATH
    subscribe_cta_path = config.CTA_SUBSCRIBE_YOUTUBE_PATH
    subscribe_cta_y_offset = config.CTA_SUBSCRIBE_YOUTUBE_Y_OFFSET  # було: CTA_SUBSCRIBE_Y_OFFSET
```

---

## Обмеження

- НЕ чіпати інші render-гілки в telegram_bot.py (advice pipeline тощо)
- НЕ чіпати myth_render.py окрім додавання print-рядків
- print рядки мають бути тільки `PROGRESS: <platform>` і `DONE: <platform>` — без зайвого тексту, бот парсить їх точно
- Якщо export папку не знайдено — не падати, просто повідомити оператора (відео залишаються в /tmp)
- TikTok залишається на `CTA_SUBSCRIBE_Y_OFFSET = -680` — не чіпати
- Значення -560 і -580 стартові для тестування — оператор може підкоригувати через `.env`

---

## Тести

1. `make test` — всі тести зелені
2. Запустити рендер через бота — в Telegram мають з'явитись:
   - "⏳ Рендеримо TikTok..."
   - "✅ TikTok готово"
   - "⏳ Рендеримо YouTube..."
   - "✅ YouTube готово"
   - "⏳ Рендеримо Instagram..."
   - "✅ Instagram готово"
   - "✅ Всі відео готові: ..."
3. Переконатись що бот не падає по таймауту навіть якщо рендер займає 45+ хвилин
4. Після рендеру перевірити що в `data/exports/dontpaniclaw_myth2_bron-oblik/` з'явились три файли: `..._tiktok.mp4`, `..._youtube.mp4`, `..._instagram.mp4`
5. Instagram CTA повністю у кадрі (не обрізається зверху)
6. YouTube CTA нижче ніж TikTok
7. TikTok CTA поведінка не змінилась
