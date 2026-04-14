# ttcf — TikTok Content Factory

## Що це таке

Telegram-бот для автоматизованого виробництва україномовних короткометражних відео
для TikTok. Оператор вибирає тему, записує голосові повідомлення через Telegram,
бот генерує субтитри, рендерить відео з фоном Pexels та надсилає результат назад.

---

## Як це працює

1. `/plan` → бот показує список тем із `content_plan.json`
2. Оператор вибирає тему → отримує заповнений AI-промпт
3. Оператор вставляє відповідь ChatGPT/Claude у форматі `PARTS: N` як reply
4. Бот надсилає скрипт кожної частини окремим повідомленням
5. Оператор записує голосове повідомлення як reply на скрипт кожної частини
6. Оператор підтверджує (✅) або перезаписує (🔄) кожну озвучку
7. Бот завантажує фонове відео з Pexels, генерує субтитри (`faster-whisper`),
   спалює субтитри + аудіо в відео та надсилає результат і метадані в Telegram

---

## Структура проєкту

| Модуль | Що робить |
|---|---|
| `src/config.py` | Вся конфігурація, env-driven, єдине джерело налаштувань |
| `src/telegram_bot.py` | Telegram I/O, state machine (STATE 1–6), голосовий підтвердний flow |
| `src/advice_pipeline.py` | Доменна логіка: `AdviceVoiceSession`, рендер частин, конвертація OGG→WAV |
| `src/content_plan.py` | `ContentTopic` dataclass, R/W `content_plan.json` |
| `src/render.py` | ffmpeg рендер: hook-кадр + тіло + concat |
| `src/subtitles.py` | `faster-whisper` (primary), пропорційний fallback, SRT→ASS |
| `src/pexels_client.py` | Завантаження відео з Pexels API + ffmpeg transcode |
| `src/publisher.py` | SQLite черга публікацій |
| `src/ukrainian_tts_adapter.py` | Piper TTS (fallback, якщо `VOICE_MODE=false`) |
| `src/reddit_intake.py` | Reddit pipeline (вторинний, окремий) |
| `src/translator.py` | DeepL переклад |
| `src/utils.py` | `compute_sha256` (допоміжна утиліта) |

---

## Вимоги

- Python 3.11+
- `ffmpeg` та `ffprobe` (доступні в `PATH`)
- Pexels API key (безкоштовний акаунт: pexels.com/api)
- Telegram Bot Token + Chat ID (BotFather)
- Respeecher API key (для голосового pipeline)
- DeepL API key (для Reddit intake / перекладу)

Перевірити наявність інструментів:

```bash
make doctor
```

---

## Швидкий старт

```bash
# 1. Клонувати репозиторій та перейти в директорію
git clone <repo> && cd ttcf

# 2. Встановити залежності
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Налаштувати середовище
cp .env.example .env
# Відредагувати .env: вставити TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
# RESPEECHER_API_KEY, PEXELS_API_KEY

# 4. Активувати змінні
set -a && source .env && set +a

# 5. Ініціалізувати контент-план (один раз)
python advice_content_cli.py init

# 6. Запустити бота
python run.py --bot
```

---

## Команди бота

| Команда | Дія |
|---|---|
| `/plan` | Показати список тем, вибрати наступну для запису |
| `/status` | Поточний стан черги та сесій |
| `/queue` | Переглянути чергу публікацій |

**Голосовий flow:**
- Бот надсилає скрипт частини → оператор записує voice message як reply
- Бот показує кнопки ✅ (підтвердити) та 🔄 (перезаписати)
- Після підтвердження всіх частин бот рендерить відео автоматично

---

## Конфігурація

Ключові змінні `.env`:

| Змінна | Опис | За замовчуванням |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | Токен бота (BotFather) | — |
| `TELEGRAM_CHAT_ID` | Chat ID оператора | — |
| `TELEGRAM_POLLING_ENABLED` | Увімкнути polling | `false` |
| `VOICE_MODE` | `true` = голос від оператора, `false` = Piper TTS | `true` |
| `RESPEECHER_API_KEY` | Ключ Respeecher | — |
| `PEXELS_API_KEY` | Ключ Pexels | — |
| `ADVICE_LOCAL_BACKGROUND_VIDEO` | Локальне тло (замість Pexels) | `""` |
| `HOOK_FRAME_ENABLED` | Увімкнути заставку | `true` |
| `HOOK_FRAME_DURATION` | Тривалість заставки (с) | `2.0` |
| `SUBTITLE_FONT_SIZE` | Розмір шрифту субтитрів | `26` |
| `SUBTITLE_ALIGNMENT` | Вирівнювання субтитрів (ASS) | `5` |
| `FFMPEG_TIMEOUT_SECONDS` | Таймаут ffmpeg | `600` |
| `KMP_DUPLICATE_LIB_OK` | Вимкнути попередження MKL (macOS) | `TRUE` |

---

## Голосовий pipeline

Стани сесії (`AdviceVoiceSession`):

| STATE | Опис |
|---|---|
| 1 | Тема обрана, промпт надіслано оператору |
| 2 | Оператор вставив скрипт; бот надіслав частини |
| 3 | Очікування голосових повідомлень від оператора |
| 4 | Отримано голос для частини, очікування підтвердження |
| 5 | Всі частини підтверджено, рендеринг у процесі |
| 6 | Відео готово, надіслано в Telegram |

**Підтверджувальний flow:**
- Голос не зберігається одразу — він потрапляє в `_pending_voice_confirmations`
- Тільки після ✅ голос фіксується як підтверджений
- 🔄 скидає очікування і просить записати знову

---

## Субтитри

- Основний бекенд: `faster-whisper` (CTranslate2, без PyTorch)
- Fallback: пропорційний розподіл за тривалістю аудіо
- SRT конвертується в ASS для ffmpeg `subtitles=` фільтра
- Стиль повністю конфігурується через `.env` (шрифт, розмір, колір, вирівнювання)
- `SUBTITLE_ALIGNMENT=5` — центр екрану (TikTok-стиль)
- `SUBTITLE_ALIGNMENT=2` + `SUBTITLE_MARGIN_V=60` — низ екрану (класика)

```bash
# Перевірити налаштування субтитрів
python -c "from src import config; print(config.SUBTITLE_FONT_SIZE, config.SUBTITLE_ALIGNMENT)"
```

---

## Запуск тестів

```bash
make test
# або
python -m unittest discover -s tests -v
```

---

## Відомі обмеження

- **PyTorch/WhisperX**: не підтримується на Python 3.11 macOS без CUDA.
  Основний бекенд субтитрів — `faster-whisper`, WhisperX не використовується.
- **Blocking render**: рендеринг виконується синхронно в основному потоці бота.
- **Один оператор**: бот розрахований на одного оператора (один `TELEGRAM_CHAT_ID`).
- **Pexels 403**: клієнт завжди надсилає `User-Agent: Mozilla/5.0`.
- **OGG voice**: Telegram надсилає `.oga` (Opus). Конвертація через
  `convert_ogg_to_wav()` → 16 kHz mono PCM WAV перед обробкою.
