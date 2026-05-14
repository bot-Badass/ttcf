# TASK-90 — Оновити README.md і опис репо на GitHub

## Задача

README.md описує стару advice-серійну архітектуру (PARTS: N → ChatGPT → рендер). Реальна система з квітня 2026 — myth-bust пайплайн із двома каналами, окремим `myth_render.py`, Publer CSV-експортом і веб-дашбордом. Нова людина, яка відкриє репо, не зрозуміє нічого про реальну роботу системи. Потрібно повністю переписати README і виставити опис репо.

---

## Що реалізувати

### 1. GitHub repo description

Через `gh` CLI оновити опис:

```bash
gh repo edit bot-Badass/ttcf --description "Telegram-бот для автоматизованого виробництва короткометражного контенту — TikTok / YouTube Shorts / Instagram. Два канали: юридичний і фінансовий."
```

---

### 2. `README.md` — повна заміна

Замінити весь вміст файлу на наступний:

```markdown
# ttcf — TikTok Content Factory

Telegram-бот + Python = автоматизована фабрика короткометражного контенту для двох медіаканалів на трьох платформах одночасно.

**Канали:**
- `law` → **@dontpaniclaw** — юридична освіта (українська)
- `finance` → **@moneyua_** — особисті фінанси (українська)

**Платформи:** TikTok · YouTube Shorts · Instagram Reels

---

## Як це працює (myth-bust пайплайн)

```
script.txt + metadata.csv   ← оператор готує заздалегідь
        ↓
  Telegram: /myth            ← бот показує список slug без озвучки
        ↓
  voice message              ← оператор записує голос як reply
        ↓
  myth_render.py             ← 3 відео (tiktok / youtube / instagram)
        ↓
  data/exports/{channel}/    ← готово для Publer CSV-публікації
```

**Routing за slug-префіксом:**
- `finance_*` → `moneyua_content_dir`
- все інше → `dontpaniclaw_content_dir`

---

## Структура проєкту

| Модуль | Що робить |
|--------|-----------|
| `src/config.py` | Вся конфігурація, env-driven. Канальні профілі (`CHANNEL_PROFILES`): кольори хук-кадру, шлях до контент-плану, категорії, export-директорії |
| `src/telegram_bot.py` | Telegram I/O: myth-голосовий flow, черга рендеру, export після рендеру |
| `src/myth_pipeline.py` | Ядро myth-bust рендеру: парсинг скрипту, вирівнювання аудіо, збірка фонового відео |
| `src/myth_queue.py` | Черга slug без озвучки; `slug_to_channel(slug)` — routing за префіксом |
| `src/myth_session.py` | Стан активної myth-сесії (який slug зараз записується) |
| `src/render.py` | ffmpeg: hook-фрейм + тіло відео + concat; платформні макети (TikTok V2 / Finance) |
| `src/subtitles.py` | faster-whisper (primary), пропорційний fallback, SRT→ASS, підсвітка слів |
| `src/pexels_client.py` | Завантаження B-roll з Pexels API + ffmpeg transcode до 9:16 |
| `src/publer_export.py` | Копіювання відео і metadata.csv у `data/exports/{channel}/{slug}/` |
| `src/advice_pipeline.py` | Вторинний пайплайн: мікросерії для advice-формату |
| `src/content_plan.py` | `ContentTopic` dataclass, R/W `content_plan.json` |
| `src/dashboard/` | FastAPI + HTMX веб-дашборд: управління чергою, контент-планом, налаштуваннями |
| `src/publisher.py` | SQLite черга публікацій (legacy, для advice-пайплайну) |

**Entry points:**

| Файл | Запуск |
|------|--------|
| `run.py` | Головний бот (`make run`) |
| `myth_render.py` | CLI-рендер одного slug (викликається ботом) |
| `dashboard.py` | Веб-дашборд |
| `rerender_session.py` | Перерендер advice-сесії |

---

## Структура даних

```
data/
  myth/<slug>/
    script.txt        ← скрипт з ##bg: секціями і хуком
    metadata.csv      ← TikTok / YouTube / Instagram метадані
    voiceover.wav     ← голос оператора (після запису в Telegram)

  exports/
    dontpaniclaw_content_dir/<slug>/
      <slug>_tiktok.mp4
      <slug>_youtube.mp4
      <slug>_instagram.mp4
      metadata.csv

    moneyua_content_dir/<slug>/
      <slug>_tiktok.mp4
      ...
```

---

## Вимоги

- Python 3.11+
- `ffmpeg` і `ffprobe` (в `PATH`)
- Pexels API key
- Telegram Bot Token + Chat ID

```bash
make doctor   # перевірити наявність інструментів
```

---

## Швидкий старт

```bash
git clone https://github.com/bot-Badass/ttcf && cd ttcf

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Вставити TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, PEXELS_API_KEY

make run
```

---

## Команди бота

| Команда | Дія |
|---------|-----|
| `/myth` | Показати чергу slug без озвучки, вибрати для запису |
| `/queue` | Черга рендеру |

**Myth-голосовий flow:**
1. Бот надсилає скрипт slug → оператор записує voice message як reply
2. Голос зберігається як `data/myth/<slug>/voiceover.wav`
3. Бот ставить рендер у чергу → 3 відео → export у channel-директорію

---

## Канальні профілі (`src/config.py`)

| Параметр | law (dontpaniclaw) | finance (moneyua) |
|----------|--------------------|-------------------|
| Хук акцент | `#FF3B30` (червоний) | `#FFD700` (золотий) |
| Бренд-лейбл | — | `MONEY UA` |
| Export-директорія | `dontpaniclaw_content_dir` | `moneyua_content_dir` |
| Категорія за замовчуванням | `ПРАВА` | `ФІНАНСИ` |

---

## Ключові змінні `.env`

| Змінна | Опис |
|--------|------|
| `TELEGRAM_BOT_TOKEN` | Токен бота (BotFather) |
| `TELEGRAM_CHAT_ID` | Chat ID оператора |
| `PEXELS_API_KEY` | Ключ Pexels |
| `VOICE_MODE` | `true` = голос оператора, `false` = Piper TTS |
| `HOOK_FRAME_ENABLED` | Увімкнути hook-фрейм (default: `true`) |
| `HOOK_FRAME_DURATION` | Тривалість hook-фрейму (default: `2.0`) |
| `SUBTITLE_FONT_SIZE` | Розмір шрифту субтитрів (default: `26`) |
| `FFMPEG_TIMEOUT_SECONDS` | Таймаут ffmpeg (default: `600`) |

---

## Тести

```bash
make test
```

---

## Відомі обмеження

- Один оператор: бот розрахований на один `TELEGRAM_CHAT_ID`
- Рендер синхронний у черзі воркера (один slug за раз)
- `faster-whisper` — основний бекенд субтитрів; WhisperX не підтримується на Python 3.11 macOS без CUDA
- Telegram надсилає `.oga` (Opus) → конвертується в 16 kHz mono WAV перед обробкою
```

---

## Обмеження

- НЕ чіпати `.env.example`, `Makefile`, `requirements.txt`, `src/` — тільки `README.md`
- НЕ додавати нічого зайвого від себе — README повинен точно відображати поточний стан коду
- Якщо `gh` CLI недоступний або не авторизований — пропустити крок з описом і повідомити

---

## Тести

1. `make test` — всі тести зелені (pre-existing failures не рахуються)
2. Відкрити `README.md` і перевірити що всі шляхи і назви модулів реально існують у репо
3. Переконатись що нема згадок ChatGPT, PARTS: N, Respeecher, advice-пайплайну як основного
