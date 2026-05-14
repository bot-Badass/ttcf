# TASK-68 — Myth-bust режим рендеру з динамічним відеорядом

## Задача

Новий формат відео: одна частина (не серія), скрипт з маркерами `##bg: запит` між блоками тексту. Кожен блок рендериться з окремим Pexels-кліпом. Існуючий advice-pipeline не чіпати — це окремий режим з окремою точкою входу.

---

## Формат скрипту

Оператор передає скрипт з маркерами:

```
##bg: stressed man reading court letter
3 речі які всі думають про МФО — і всі три неправда.

##bg: worried man documents desk
Міф 1: МФО завжди виграє в суді.

##bg: judge courtroom signing
Це неправда. Є три умови за яких суд відхиляє позов.

##bg: person relief laptop bank
Реальність: якщо заявиш про строк давності — МФО програє.
```

Маркери `##bg:` не озвучуються — вони тільки для розбивки відеоряду.

---

## Що реалізувати

### `src/myth_pipeline.py` — новий модуль

#### 1. Парсинг скрипту

```python
def parse_myth_script(script_text: str) -> list[dict]:
    """
    Повертає список секцій:
    [{"query": "stressed man reading...", "text": "3 речі які..."}, ...]
    """
    sections = []
    current_query = None
    current_lines = []
    for line in script_text.splitlines():
        if line.startswith("##bg:"):
            if current_lines and current_query is not None:
                sections.append({
                    "query": current_query,
                    "text": "\n".join(current_lines).strip()
                })
            current_query = line[5:].strip()
            current_lines = []
        elif line.strip():
            current_lines.append(line.strip())
    if current_lines and current_query is not None:
        sections.append({
            "query": current_query,
            "text": "\n".join(current_lines).strip()
        })
    return sections
```

#### 2. Витяг чистого тексту для TTS

```python
def extract_voice_text(script_text: str) -> str:
    """Прибирає ##bg: рядки — залишає тільки текст для озвучки."""
    lines = [l for l in script_text.splitlines() if not l.startswith("##bg:")]
    return "\n".join(lines).strip()
```

#### 3. Маппінг секцій на таймлайн

Word timestamps беруться з `_align_words_with_faster_whisper(audio_path)` із `src/subtitles.py` (private, але використовується в межах того самого пакету). Повертає `tuple[AlignedWord, ...]` з полями `text`, `start_seconds`, `end_seconds`.

```python
from src.subtitles import AlignedWord, _align_words_with_faster_whisper

def map_sections_to_timestamps(
    sections: list[dict],
    aligned_words: Sequence[AlignedWord],
    total_duration: float,
) -> list[dict]:
    """
    Для кожної секції знаходить перше слово в транскрипті
    і повертає sections з полями start_time і end_time.
    """
```

Алгоритм:
- Взяти перше слово кожної секції (з `section["text"]`)
- Знайти його в `aligned_words` за порядком (не по всьому тексту — щоб уникнути колізій)
- `start_time` секції = `word.start_seconds` першого слова
- `end_time` секції = `start_time` наступної секції; остання секція → `total_duration`

#### 4. Збірка фону з Pexels-кліпів

```python
def build_background_video(
    sections_with_times: list[dict],
    output_path: Path
) -> Path:
    """
    Для кожної секції: завантажує Pexels-кліп по query,
    тримає рівно (end_time - start_time) секунд,
    concat всіх кліпів → єдине фонове відео.
    """
```

Використовувати існуючий Pexels-клієнт з advice_pipeline.py — не дублювати логіку.

Concat через ffmpeg filter_complex concat (відео + відео без аудіо).

#### 5. Фінальний рендер

Передати зібране фонове відео в існуючу `render_story_video()` з `src/render.py` — вона вже вміє накладати субтитри, хук-фрейм і CTA. Нічого нового в рендері не додавати.

#### 6. Точка входу

Субтитри **генеруються всередині** — не передаються ззовні. Використовувати існуючу `generate_subtitle_file` з `src/subtitles.py` так само як це робить `advice_pipeline.py`.

```python
def render_myth_video(
    script_text: str,
    audio_path: Path,
    output_path: Path,
    channel: str,  # "law" або "finance"
    part_number: int = 1,
    total_parts: int = 1,
) -> Path:
    """Повний пайплайн myth-bust рендеру."""
    # 1. Витягти чистий текст для субтитрів і TTS
    voice_text = extract_voice_text(script_text)
    # 2. Розпарсити секції
    sections = parse_myth_script(script_text)
    # 3. Запустити whisper один раз → word timestamps
    aligned_words = _align_words_with_faster_whisper(audio_path)
    total_duration = get_audio_duration(audio_path)  # через ffprobe або mutagen
    # 4. Згенерувати subtitle файл (використовує той самий whisper під капотом)
    subtitle_path = generate_subtitle_file(audio_path, voice_text, output_path.parent)
    # 5. Маппінг секцій на таймлайн
    sections_timed = map_sections_to_timestamps(sections, aligned_words, total_duration)
    # 6. Зібрати фонове відео з Pexels кліпів
    bg_video = build_background_video(sections_timed, output_path.parent / "bg_tmp.mp4")
    # 7. Фінальний рендер через існуючу render_story_video()
    return render_story_video(bg_video, audio_path, subtitle_path, output_path, ...)
```

**Увага:** `_align_words_with_faster_whisper` і `generate_subtitle_file` обидва запускають whisper окремо. Два виклики — прийнятно для MVP (одне відео рендериться раз). Якщо стане проблемою — оптимізувати потім.

---

### `myth_render.py` — CLI скрипт (в корені проекту, поруч з `rerender_session.py`)

```python
# Запуск: python myth_render.py --script path/to/script.txt --audio path/to/audio.wav --channel law --output output.mp4
```

Мінімальний CLI для ручного тестування без Telegram-бота.

---

## Обмеження

- НЕ чіпати `src/advice_pipeline.py`, `src/render.py`, `src/telegram_bot.py`
- НЕ інтегрувати в Telegram-бот зараз — тільки CLI
- Pexels-клієнт брати з існуючого коду, не писати новий
- Якщо Pexels не знаходить кліп по запиту — fallback на наступний за популярністю або дефолтний кліп каналу

---

## Тести

1. `make test` — існуючі тести зелені
2. Написати `tests/test_myth_pipeline.py`:
   - `test_parse_myth_script` — парсинг з 3+ секціями, перевірити query і text
   - `test_extract_voice_text` — перевірити що `##bg:` рядки видалені
   - `test_map_sections_to_timestamps` — з мок `AlignedWord` об'єктами (без реального whisper)
3. Ручний тест: `python myth_render.py --script tests/fixtures/myth_sample.txt --audio tests/fixtures/sample.wav --channel law --output /tmp/myth_test.mp4`
   - Якщо `tests/fixtures/sample.wav` не існує — додати будь-який короткий WAV файл для тесту
   - Відео має N різних фонових кліпів відповідно до кількості `##bg:` маркерів
