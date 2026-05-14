from __future__ import annotations

import subprocess
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path

from src import config
from src.advice_pipeline import _load_voice_sessions, AdviceVoiceSession
from src.dashboard.services.plan_service import CHANNEL_PLAN_MAP, CHANNEL_LABELS
from src.content_plan import get_topic_by_id


@dataclass
class EnrichedSession:
    session: AdviceVoiceSession
    channel_key: str
    channel_label: str
    series_title: str
    topic_title: str
    parts_done: int
    parts_total: int
    is_complete: bool
    bg_size_mb: float | None
    videos_done: int = 0  # how many part video.mp4 files exist on disk


def _detect_channel(topic_id: str) -> tuple[str, str]:
    for channel_key, plan_path in CHANNEL_PLAN_MAP.items():
        if plan_path.is_file():
            t = get_topic_by_id(topic_id, plan_path)
            if t is not None:
                return channel_key, CHANNEL_LABELS[channel_key]
    return "law", "DontPanicLaw"


def get_all_sessions(status_filter: str = "all") -> list[EnrichedSession]:
    sessions = _load_voice_sessions(config.VOICE_SESSION_STORE_PATH)
    result: list[EnrichedSession] = []

    for s in reversed(sessions):
        parts_done = sum(1 for f in s.voice_files if f is not None and Path(f).is_file())
        parts_total = len(s.voice_files)
        is_complete = parts_done == parts_total and parts_total > 0
        videos_done = sum(
            1 for i in range(parts_total)
            if (config.ADVICE_OUTPUT_DIR / f"{s.review_id}_part{i + 1}" / "video.mp4").is_file()
        )

        if status_filter == "recording" and is_complete:
            continue
        if status_filter == "ready" and (not is_complete or videos_done > 0):
            continue
        if status_filter == "rendered" and videos_done == 0:
            continue
        # legacy compat
        if status_filter == "active" and is_complete:
            continue
        if status_filter == "complete" and not is_complete:
            continue

        channel_key, channel_label = _detect_channel(s.topic_id)

        bg_size_mb: float | None = None
        if s.background_video_path.is_file():
            bg_size_mb = round(s.background_video_path.stat().st_size / 1024 / 1024, 1)

        topic_title = s.micro_series.series_title
        series_title = ""
        for plan_path in CHANNEL_PLAN_MAP.values():
            if plan_path.is_file():
                t = get_topic_by_id(s.topic_id, plan_path)
                if t:
                    topic_title = t.title
                    series_title = t.series_title
                    break

        result.append(EnrichedSession(
            session=s,
            channel_key=channel_key,
            channel_label=channel_label,
            series_title=series_title,
            topic_title=topic_title,
            parts_done=parts_done,
            parts_total=parts_total,
            is_complete=is_complete,
            bg_size_mb=bg_size_mb,
            videos_done=videos_done,
        ))

    return result


def get_active_topic_ids() -> set[str]:
    sessions = _load_voice_sessions(config.VOICE_SESSION_STORE_PATH)
    result: set[str] = set()
    for s in sessions:
        parts_done = sum(1 for f in s.voice_files if f is not None and Path(f).is_file())
        if parts_done < len(s.voice_files):
            result.add(s.topic_id)
    return result


def get_session_stats() -> dict[str, int]:
    sessions = _load_voice_sessions(config.VOICE_SESSION_STORE_PATH)
    active = complete = 0
    for s in sessions:
        parts_done = sum(1 for f in s.voice_files if f is not None and Path(f).is_file())
        if parts_done == len(s.voice_files) and len(s.voice_files) > 0:
            complete += 1
        else:
            active += 1
    return {"active": active, "complete": complete, "total": active + complete}


# ---------------------------------------------------------------------------
# Render process management with full log accumulation
# ---------------------------------------------------------------------------

@dataclass
class _RenderJob:
    proc: subprocess.Popen
    log_lines: list[str] = field(default_factory=list)
    finished: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def drain(self) -> None:
        """Read all available stdout into log_lines (called from reader thread)."""
        if self.proc.stdout is None:
            return
        for line in self.proc.stdout:
            with self._lock:
                self.log_lines.append(line)
        self.proc.wait()
        with self._lock:
            self.finished = True

    def snapshot(self) -> tuple[str, bool]:
        with self._lock:
            return "".join(self.log_lines), self.finished


_jobs: dict[str, _RenderJob] = {}


def start_render(review_id: str) -> bool:
    if review_id in _jobs and not _jobs[review_id].finished:
        return False  # already running

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    proc = subprocess.Popen(
        [sys.executable, "rerender_session.py", review_id],
        cwd=str(project_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    job = _RenderJob(proc=proc)
    _jobs[review_id] = job

    # Reader thread accumulates output without blocking the event loop
    t = threading.Thread(target=job.drain, daemon=True)
    t.start()
    return True


def get_render_snapshot(review_id: str) -> tuple[str, bool]:
    """Return (full_log_so_far, is_finished)."""
    job = _jobs.get(review_id)
    if job is None:
        return "", True
    return job.snapshot()


# ---------------------------------------------------------------------------
# Dashboard → Telegram voice prompt + audio upload
# ---------------------------------------------------------------------------

def send_part_prompt(session_id: str, part_number: int) -> tuple[bool, str]:
    """Send the part script to Telegram and register the returned message_id in the session."""
    from src.advice_pipeline import get_voice_session, save_part_message_id
    from src.dashboard.services.telegram_sender import send_text_message

    session = get_voice_session(session_id)
    if session is None:
        return False, "Сесію не знайдено"
    if part_number < 1 or part_number > len(session.micro_series.parts):
        return False, "Невірний номер частини"

    part = session.micro_series.parts[part_number - 1]
    total = len(session.micro_series.parts)
    text = (
        f"🎤 Частина {part_number}/{total} — озвучуй цей текст голосовим:\n\n"
        f"{part.script_text}"
    )
    msg_id = send_text_message(text)
    if msg_id is None:
        return False, "Не вдалося надіслати повідомлення в Telegram"

    save_part_message_id(session_id, part_number, msg_id)
    return True, f"Текст надіслано в Telegram — відповідай голосовим на це повідомлення"


def mark_session_published(session_id: str) -> tuple[bool, str]:
    """Enqueue all rendered parts of a session and immediately mark them published."""
    from src.advice_pipeline import get_voice_session
    from src.dashboard.services.render_service import enqueue_render
    from src.dashboard.services.queue_service import mark_published

    session = get_voice_session(session_id)
    if session is None:
        return False, "Сесію не знайдено"

    count = 0
    for i in range(len(session.micro_series.parts)):
        pnum = i + 1
        part_dir = config.ADVICE_OUTPUT_DIR / f"{session.review_id}_part{pnum}"
        if not (part_dir / "video.mp4").is_file():
            continue
        asset_id = part_dir.name
        enqueue_render(asset_id)   # adds to queue (idempotent if already there)
        mark_published(asset_id)   # marks as published
        count += 1

    if count == 0:
        return False, "Немає зрендерованих відео для цієї сесії"
    return True, f"✅ {count} відео позначено як опубліковані"


def generate_hook_preview(session_id: str, part_number: int) -> bytes | None:
    """Render a single PNG frame of the hook overlay for a session part."""
    import subprocess
    import tempfile
    from src.advice_pipeline import get_voice_session, extract_hook_sentence
    from src.render import _render_hook_frame, _run_command

    session = get_voice_session(session_id)
    if session is None:
        return None

    idx = part_number - 1
    if idx < 0 or idx >= len(session.micro_series.parts):
        return None

    part = session.micro_series.parts[idx]
    hook_text = extract_hook_sentence(part.script_text) or session.micro_series.series_title
    series_id = session.topic_id[0].upper() if session.topic_id else None
    bg_path = session.background_video_path if session.background_video_path.is_file() else None

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        hook_vid = tmp / "hook.mp4"
        try:
            _render_hook_frame(
                hook_text=hook_text,
                output_path=hook_vid,
                ffmpeg_path=config.FFMPEG_PATH,
                run_command=_run_command,
                series_id=series_id,
                part_number=part.part_number,
                background_video_path=bg_path,
                hook_bg_override=session.hook_bg_override,
                hook_accent_override=session.hook_accent_override,
                hook_brand_override=session.hook_brand_override,
            )
        except Exception:
            return None

        if not hook_vid.is_file():
            return None

        png_path = tmp / "preview.png"
        cmd = [
            config.FFMPEG_PATH, "-y",
            "-i", str(hook_vid),
            "-vf", "select=eq(n\\,0),scale=360:640",
            "-frames:v", "1",
            str(png_path),
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        if result.returncode != 0 or not png_path.is_file():
            return None

        return png_path.read_bytes()


def refresh_session_background(session_id: str) -> tuple[bool, str, str | None]:
    """Download a fresh Pexels background for an existing session. Returns (ok, msg, bg_path)."""
    from src.advice_pipeline import get_voice_session
    from src.pexels_client import download_background_video as pexels_download, PexelsError
    from src.content_plan import get_topic_by_id
    from src.dashboard.services.plan_service import CHANNEL_PLAN_MAP

    session = get_voice_session(session_id)
    if session is None:
        return False, "Сесію не знайдено", None

    topic = None
    for plan_path in CHANNEL_PLAN_MAP.values():
        if plan_path.is_file():
            t = get_topic_by_id(session.topic_id, plan_path)
            if t:
                topic = t
                break

    query = (topic.pexels_query if topic and topic.pexels_query else None) or config.ADVICE_PEXELS_QUERY
    bg_path = session.background_video_path

    try:
        pexels_download(bg_path, query)
    except PexelsError as exc:
        return False, f"Pexels помилка: {exc}", None

    return True, "Новий фон завантажено", str(bg_path)


def save_uploaded_audio(
    session_id: str,
    part_number: int,
    audio_bytes: bytes,
    ext: str,
) -> tuple[bool, str]:
    """Save uploaded audio bytes as the voice recording for a session part."""
    from src.advice_pipeline import get_voice_session, save_part_voice, convert_ogg_to_wav, AdvicePipelineError

    session = get_voice_session(session_id)
    if session is None:
        return False, "Сесію не знайдено"
    if part_number < 1 or part_number > len(session.micro_series.parts):
        return False, "Невірний номер частини"

    review_id = session.review_id
    part_dir = config.ADVICE_OUTPUT_DIR / f"{review_id}_part{part_number}"
    part_dir.mkdir(parents=True, exist_ok=True)

    suffix = ext if ext.startswith(".") else f".{ext}"
    raw_path = part_dir / f"voice_raw{suffix}"
    raw_path.write_bytes(audio_bytes)

    wav_path = part_dir / "voiceover.wav"
    try:
        convert_ogg_to_wav(raw_path, wav_path)
    except AdvicePipelineError as exc:
        return False, f"Помилка конвертації аудіо: {exc}"

    save_part_voice(session_id, part_number, wav_path)
    return True, "Аудіо збережено"
