# TASK-75 — Рендер: Pexels одного разу + фоновий потік у боті

## Задача

Після TASK-74 `myth_render.py` рендерить 3 платформи, але `render_myth_video` викликається тричі — і кожен раз завантажує 5 Pexels-кліпів, генерує субтитри, транскрибує аудіо. Це ~15 Pexels-запитів і ~10+ хвилин замість ~5. Бот timeout = 5 хв → завжди падає. Потрібно: підготовчу роботу виконати один раз, потім рендерити 3 відео на спільних ресурсах; бот запускати рендер у фоновому потоці.

---

## Що реалізувати

### 1. `src/myth_pipeline.py` — нова функція `prepare_myth_assets`

Виносить всю "важку" підготовку (транскрипція, субтитри, Pexels) в окрему функцію:

```python
def prepare_myth_assets(
    script_text: str,
    audio_path: Path,
    work_dir: Path,
) -> dict:
    """Download Pexels clips, generate subtitles, extract hook. Call once before rendering all platforms."""
    voice_text = extract_voice_text(script_text)
    sections = parse_myth_script(script_text)

    if not sections:
        raise ValueError("Script has no ##bg: sections — nothing to render.")

    aligned_words = _align_words_with_faster_whisper(audio_path)
    total_duration = _probe_duration(audio_path)

    subtitle_path = generate_subtitle_file(
        audio_path=audio_path,
        script=voice_text,
        output_dir=work_dir,
    )

    sections_timed = map_sections_to_timestamps(sections, aligned_words, total_duration)

    bg_tmp = work_dir / "myth_bg_tmp.mp4"
    bg_video = build_background_video(sections_timed, bg_tmp)
    hook_text = extract_hook_text(script_text)

    return {
        "bg_video": bg_video,
        "subtitle_path": subtitle_path,
        "hook_text": hook_text,
    }
```

### 2. `src/myth_pipeline.py` — `render_myth_video`

Додати параметр `prebuilt_assets: dict | None = None`. Якщо передано — пропустити підготовку і одразу рендерити. Якщо `None` — викликати `prepare_myth_assets` і прибрати `bg_video` в `finally`.

Нова логіка:

```python
def render_myth_video(
    script_text: str,
    audio_path: Path,
    output_path: Path,
    channel: str,
    category: str = "",
    platform: str = "tiktok",
    prebuilt_assets: dict | None = None,
) -> Path:
    profile = config.CHANNEL_PROFILES.get(
        channel, config.CHANNEL_PROFILES[config.DEFAULT_CHANNEL]
    )

    owns_assets = prebuilt_assets is None
    if owns_assets:
        assets = prepare_myth_assets(script_text, audio_path, output_path.parent)
    else:
        assets = prebuilt_assets

    bg_video: Path = assets["bg_video"]
    subtitle_path: Path = assets["subtitle_path"]
    hook_text: str = assets["hook_text"]

    try:
        return render_myth_story_video(
            background_video_path=bg_video,
            audio_path=audio_path,
            output_path=output_path,
            subtitle_path=subtitle_path,
            hook_text=hook_text or None,
            platform=platform,
            hook_bg_override=profile.get("hook_bg"),
            hook_accent_override=profile.get("hook_accent"),
            hook_brand_override=profile.get("hook_brand") or "",
            category_override=category,
        )
    finally:
        if owns_assets:
            bg_video.unlink(missing_ok=True)
```

### 3. `myth_render.py` — підготовка один раз, рендер три рази

```python
from src.myth_pipeline import parse_myth_script, render_myth_video, prepare_myth_assets

# ... argparse, path checks, print sections ...

work_dir = output_path.parent
print("Preparing assets (Pexels + subtitles)...")
assets = prepare_myth_assets(script_text, audio_path, work_dir)

base = Path(args.output).with_suffix("")
try:
    for platform in ["tiktok", "instagram", "youtube"]:
        out = base.parent / f"{base.name}_{platform}.mp4"
        print(f"Rendering {platform}...")
        result = render_myth_video(
            script_text=script_text,
            audio_path=audio_path,
            output_path=out,
            channel=args.channel,
            category=args.category,
            platform=platform,
            prebuilt_assets=assets,
        )
        size_mb = result.stat().st_size / 1_000_000
        print(f"✅ {platform}: {result}  ({size_mb:.1f} MB)")
finally:
    assets["bg_video"].unlink(missing_ok=True)
```

### 4. `src/telegram_bot.py` — фоновий потік для `myth_render`

Замінити синхронний `subprocess.run` на запуск у фоновому `threading.Thread`. Бот одразу відповідає "⏳ Рендеримо..." і повертається до обробки повідомлень. Коли рендер завершується — надсилає результат.

Знайти блок `elif data.startswith("myth_render:")` і замінити його на:

```python
elif data.startswith("myth_render:"):
    slug = data.split(":", 1)[1]
    script_path = config.MYTH_DATA_DIR / slug / "script.txt"
    wav_path = config.MYTH_DATA_DIR / slug / "voiceover.wav"
    base_output = Path(f"/tmp/{slug}.mp4")
    send_message(chat_id, "⏳ Рендеримо відео... (~5-15 хв)", None)

    def _do_render(chat_id=chat_id, slug=slug,
                   script_path=script_path, wav_path=wav_path,
                   base_output=base_output):
        import threading as _threading
        try:
            result = subprocess.run(
                [
                    "python", "myth_render.py",
                    "--script", str(script_path),
                    "--audio", str(wav_path),
                    "--channel", "law",
                    "--output", str(base_output),
                ],
                capture_output=True,
                text=True,
                timeout=1800,
            )
            if result.returncode == 0:
                base = base_output.with_suffix("")
                paths = "\n".join(
                    f"`{base}_{p}.mp4`" for p in ["tiktok", "instagram", "youtube"]
                )
                send_message(chat_id, f"✅ Готово:\n{paths}", None)
            else:
                send_message(
                    chat_id,
                    f"❌ Помилка рендеру:\n```{result.stderr[-500:]}```",
                    None,
                )
        except subprocess.TimeoutExpired:
            send_message(chat_id, "❌ Рендер завис (timeout 30 хв)", None)

    import threading
    threading.Thread(target=_do_render, daemon=True).start()
```

---

## Обмеження

- `prepare_myth_assets` повертає `bg_video` як тимчасовий файл — `myth_render.py` відповідає за його видалення в `finally`
- `render_myth_video` з `prebuilt_assets != None` **не видаляє** `bg_video` — це обов'язок caller
- Не чіпати логіку `render_story_video` і advice pipeline
- `--channel` хардкодиться як "law" у боті (бот поки тільки для dontpaniclaw)

---

## Тести

1. `make test` — всі тести зелені
2. CLI-тест (один Pexels-завантаження):
   ```bash
   cd /Users/a111/Projects/ttcf
   time python myth_render.py \
     --script data/myth/bron-oblik/script.txt \
     --audio data/myth/bron-oblik/voiceover.wav \
     --channel law \
     --output /tmp/bron-oblik.mp4
   ```
   Очікується: у stdout "Preparing assets..." з'являється один раз, потім "Rendering tiktok/instagram/youtube"
3. З'явились 3 файли: `bron-oblik_tiktok.mp4`, `bron-oblik_instagram.mp4`, `bron-oblik_youtube.mp4`
4. Бот-тест: `/myth bron-oblik` → надіслати голосове → "🎬 Рендерити" → бот відповідає "⏳..." одразу і не зависає → через ~10 хв надсилає 3 шляхи
