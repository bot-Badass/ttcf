from __future__ import annotations

import logging
import re
import subprocess
import tempfile
import unicodedata
from pathlib import Path
from typing import Sequence

from src import config
from src.pexels_client import download_background_video
from src.render import render_myth_story_video
from src.subtitles import AlignedWord, _align_words_with_faster_whisper, generate_subtitle_file

LOGGER = logging.getLogger(__name__)


def parse_myth_script(script_text: str) -> list[dict]:
    """Return list of sections: [{"query": ..., "text": ...}, ...]

    Splits on ##bg: markers. Text between consecutive markers belongs to the
    preceding marker. Lines that are blank or marker-only are excluded from text.
    """
    sections: list[dict] = []
    current_query: str | None = None
    current_lines: list[str] = []

    for line in script_text.splitlines():
        if line.startswith("##bg:"):
            if current_lines and current_query is not None:
                sections.append({
                    "query": current_query,
                    "text": "\n".join(current_lines).strip(),
                })
            current_query = line[5:].strip()
            current_lines = []
        elif line.strip():
            current_lines.append(line.strip())

    if current_lines and current_query is not None:
        sections.append({
            "query": current_query,
            "text": "\n".join(current_lines).strip(),
        })

    return sections


def extract_voice_text(script_text: str) -> str:
    """Remove ##bg: lines — only spoken text remains."""
    lines = [l for l in script_text.splitlines() if not l.startswith("##bg:")]
    return "\n".join(lines).strip()


def extract_hook_text(script_text: str) -> str:
    """Return first sentence of first text block after first ##bg: marker."""
    past_first_bg = False
    for line in script_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("##bg:"):
            past_first_bg = True
            continue
        if past_first_bg and stripped:
            for sep in (".", "?", "!"):
                idx = stripped.find(sep)
                if idx != -1:
                    return stripped[: idx + 1]
            return stripped
    return ""


def map_sections_to_timestamps(
    sections: list[dict],
    aligned_words: Sequence[AlignedWord],
    total_duration: float,
) -> list[dict]:
    """Attach start_time / end_time to each section from word-level timestamps.

    For each section the first spoken word is located sequentially in
    aligned_words (forward-only scan to avoid re-matching the same word).
    end_time of section N == start_time of section N+1.
    Last section ends at total_duration.
    """
    result: list[dict] = []
    search_index = 0

    for section in sections:
        first_word = _first_word_of(section["text"])
        start_time, new_index = _find_word_start(first_word, aligned_words, search_index)
        if start_time is None:
            start_time = result[-1]["start_time"] if result else 0.0
        else:
            search_index = new_index
        result.append({**section, "start_time": start_time, "end_time": None})

    for i in range(len(result) - 1):
        result[i]["end_time"] = result[i + 1]["start_time"]
    if result:
        result[-1]["end_time"] = total_duration

    return result


def build_background_video(
    sections_with_times: list[dict],
    output_path: Path,
) -> Path:
    """Download one Pexels clip per section, trim to duration, concat."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        clip_paths: list[Path] = []
        session_used: set[int] = set()

        for i, section in enumerate(sections_with_times):
            clip_path = tmp / f"clip_{i:02d}.mp4"
            query = section["query"]
            LOGGER.info(
                "Pexels clip %d/%d query=%r", i + 1, len(sections_with_times), query
            )
            try:
                vid_id = download_background_video(
                    clip_path, query=query, extra_exclude_ids=session_used
                )
                if vid_id is not None:
                    session_used.add(vid_id)
            except Exception as exc:
                LOGGER.warning(
                    "Pexels query %r failed (%s) — using default query", query, exc
                )
                vid_id = download_background_video(
                    clip_path, query=config.ADVICE_PEXELS_QUERY,
                    extra_exclude_ids=session_used,
                )
                if vid_id is not None:
                    session_used.add(vid_id)
            clip_paths.append(clip_path)

        _concat_clips(clip_paths, sections_with_times, output_path)

    return output_path


def prepare_myth_assets(
    script_text: str,
    audio_path: Path,
    work_dir: Path,
    *,
    highlight_color: str | None = None,
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
        highlight_color=highlight_color or None,
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


def render_myth_video(
    script_text: str,
    audio_path: Path,
    output_path: Path,
    channel: str,
    category: str = "",
    platform: str = "tiktok",
    prebuilt_assets: dict | None = None,
) -> Path:
    """Full myth-bust render pipeline."""
    profile = config.CHANNEL_PROFILES.get(
        channel, config.CHANNEL_PROFILES[config.DEFAULT_CHANNEL]
    )

    owns_assets = prebuilt_assets is None
    if owns_assets:
        assets = prepare_myth_assets(
            script_text,
            audio_path,
            output_path.parent,
            highlight_color=profile.get("highlight_color") or None,
        )
    else:
        assets = prebuilt_assets

    bg_video: Path = assets["bg_video"]
    subtitle_path: Path = assets["subtitle_path"]
    hook_text: str = assets["hook_text"]

    LOGGER.info(
        "render_myth_video: channel=%s, platform=%s, output=%s",
        channel, platform, output_path,
    )

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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _first_word_of(text: str) -> str:
    words = text.split()
    return words[0] if words else ""


def _normalize_word(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[^\w]", "", text, flags=re.UNICODE)
    return text.casefold()


def _find_word_start(
    word: str,
    aligned_words: Sequence[AlignedWord],
    from_index: int,
) -> tuple[float, int] | tuple[None, int]:
    """Return (start_seconds, next_search_index) or (None, from_index) if not found."""
    if not word:
        return None, from_index
    target = _normalize_word(word)
    for idx in range(from_index, len(aligned_words)):
        if _normalize_word(aligned_words[idx].text) == target:
            return aligned_words[idx].start_seconds, idx + 1
    return None, from_index


def _concat_clips(
    clip_paths: list[Path],
    sections: list[dict],
    output_path: Path,
) -> None:
    """Concat trimmed, timestamp-reset clips into one background video."""
    n = len(clip_paths)
    if n == 0:
        raise ValueError("No clips to concat.")

    cmd = [config.FFMPEG_BIN, "-y"]
    for clip_path, section in zip(clip_paths, sections):
        dur = max(0.5, section["end_time"] - section["start_time"])
        cmd += ["-stream_loop", "-1", "-t", f"{dur:.3f}", "-i", str(clip_path)]

    filter_parts = [
        f"[{i}:v]setpts=PTS-STARTPTS,fps=30[v{i}]" for i in range(n)
    ]
    concat_inputs = "".join(f"[v{i}]" for i in range(n))
    filter_parts.append(f"{concat_inputs}concat=n={n}:v=1:a=0[outv]")

    cmd += [
        "-filter_complex", ";".join(filter_parts),
        "-map", "[outv]",
        "-c:v", config.OUTPUT_VIDEO_CODEC,
        "-preset", config.OUTPUT_PRESET,
        "-crf", str(config.OUTPUT_CRF),
        "-pix_fmt", config.OUTPUT_PIXEL_FORMAT,
        "-an",
        str(output_path),
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=config.FFMPEG_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg concat failed: {result.stderr.strip() or 'non-zero exit'}"
        )
    LOGGER.info("Background assembled: %s (%d clips)", output_path, n)


def _probe_duration(audio_path: Path) -> float:
    result = subprocess.run(
        [
            config.FFPROBE_BIN, "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(audio_path),
        ],
        capture_output=True, text=True, check=False,
        timeout=config.FFPROBE_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {result.stderr.strip()}")
    return float(result.stdout.strip())
