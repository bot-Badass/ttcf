# TASK-70 — Обробка голосу: шумодав + EQ + компресія + нормалізація

## Задача

Голос записаний через Telegram звучить "сирим" — фоновий шум кімнати, нерівна гучність, немає відчуття "ефіру". Потрібно додати ffmpeg-ланцюг обробки аудіо при конвертації голосового повідомлення в `_handle_myth_voice`: шумодав → highpass → EQ → компресія → гучність до стандарту -16 LUFS.

---

## Що реалізувати

### `src/telegram_bot.py` — константа і зміна команди ffmpeg в `_handle_myth_voice`

Додати константу на рівні модуля (після імпортів, перед першою функцією):

```python
_VOICE_FILTER_CHAIN = (
    "afftdn=nf=-25,"               # шумодав: прибирає фоновий шум кімнати
    "highpass=f=80,"               # прибирає гул і вібрацію нижче 80 Гц
    "equalizer=f=3000:width_type=o:width=2:g=3,"  # +3 dB на 3 кГц — голос чіткіший
    "compand=0.3|0.3:1|1:-90/-60|-60/-40|-40/-30|-20/-20:6:0:-90:0.2,"  # компресія
    "loudnorm=I=-16:TP=-1.5:LRA=11"  # нормалізація до -16 LUFS (мобільний стандарт)
)
```

В `_handle_myth_voice` замінити поточний виклик `subprocess.run` для ffmpeg:

```python
# БУЛО:
subprocess.run(
    [config.FFMPEG_BIN, "-y", "-i", tmp_path, "-ar", "22050", "-ac", "1", str(wav_path)],
    check=True, capture_output=True,
)

# СТАЛО:
subprocess.run(
    [
        config.FFMPEG_BIN, "-y", "-i", tmp_path,
        "-af", _VOICE_FILTER_CHAIN,
        "-ar", "22050", "-ac", "1",
        str(wav_path),
    ],
    check=True, capture_output=True,
)
```

---

## Обмеження

- НЕ чіпати advice-pipeline (там своя конвертація голосу — окремий scope)
- НЕ чіпати `myth_render.py`, `myth_pipeline.py` — зміна тільки в конвертації при отриманні голосу
- НЕ виносити `_VOICE_FILTER_CHAIN` в `config.py` — це аудіо-константа, не env var

---

## Тести

1. `make test` — всі існуючі тести зелені
2. Ручний тест:
   - Записати голос через `/myth test-eq` → надіслати голосове
   - Відтворити `data/myth/test-eq/voiceover.wav` — порівняти з сирим записом
   - Голос має звучати чистіше: менше шуму кімнати, рівна гучність, трохи більше чіткості
