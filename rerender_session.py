#!/usr/bin/env python3
"""
Re-render a completed voice session using existing WAV + SRT files.
Useful when you need to regenerate videos with updated render settings
(e.g. new hook frame design) without re-recording the voiceover.

Usage:
    python rerender_session.py <review_id>

Example:
    python rerender_session.py 8121993f85ce4e43a841793a3c019ee3
"""
import os
import sys
import json
import subprocess
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent))

# Load .env
env_path = Path(__file__).parent / ".env"
if env_path.is_file():
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key.strip(), value)

from src import config
from src.content_plan import get_topic_by_id, list_topics
from src.advice_pipeline import (
    AdviceVoiceSession,
    AdviceMicroSeries,
    AdvicePartScript,
    _load_voice_sessions,
    render_voice_session,
    extract_hook_sentence,
)
from src.render import render_story_video
from src.subtitles import generate_subtitle_file
from src.publisher import PublishCandidate, PublishQueueStore


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python rerender_session.py <review_id>")
        print("Example: python rerender_session.py 8121993f85ce4e43a841793a3c019ee3")
        sys.exit(1)

    review_id = sys.argv[1].strip()

    print(f"\n🔄 Re-render session: review_id={review_id}\n")

    # --- Find the completed session for this review_id ---
    sessions = _load_voice_sessions(config.VOICE_SESSION_STORE_PATH)
    completed = [
        s for s in sessions
        if s.review_id == review_id and all(f is not None for f in s.voice_files)
    ]

    if not completed:
        # Show what sessions exist for this review_id
        all_for_review = [s for s in sessions if s.review_id == review_id]
        if all_for_review:
            print(f"❌ No completed session found for review_id={review_id}")
            print(f"   Found {len(all_for_review)} session(s) but none have all voice files:")
            for s in all_for_review:
                filled = sum(1 for f in s.voice_files if f is not None)
                print(f"   session_id={s.session_id}  voice_files={filled}/{len(s.voice_files)}")
        else:
            print(f"❌ No sessions found for review_id={review_id}")
        sys.exit(1)

    # Use the most recent completed session
    session = completed[-1]
    print(f"✅ Found completed session: {session.session_id}")
    print(f"   topic_id : {session.topic_id}")
    print(f"   parts    : {len(session.voice_files)}")
    print(f"   bg video : {session.background_video_path}\n")

    # Verify all voice files exist on disk
    missing = [
        (i + 1, f)
        for i, f in enumerate(session.voice_files)
        if f is None or not Path(f).is_file()
    ]
    if missing:
        print("❌ Missing voice files:")
        for part_n, path in missing:
            print(f"   Part {part_n}: {path}")
        sys.exit(1)

    # Verify background video exists
    if not session.background_video_path.is_file():
        print(f"❌ Background video not found: {session.background_video_path}")
        sys.exit(1)

    # --- Find topic for series_id ---
    topic = None
    for plan_path in [config.CONTENT_PLAN_PATH,
                      config.DATA_DIR / "content_plan_moneyua.json"]:
        if plan_path.is_file():
            t = get_topic_by_id(session.topic_id, plan_path)
            if t is not None:
                topic = t
                break

    if topic is None:
        print(f"⚠️  Topic {session.topic_id} not found in any plan — series theme will use default")
        series_id = None
    else:
        series_id = topic.series_id
        print(f"   series_id: {series_id} ({topic.series_title})")

    print(f"\n🎬 Starting re-render of {len(session.micro_series.parts)} parts...\n")

    output_dir = config.ADVICE_OUTPUT_DIR
    results = []

    for part, wav_path in zip(session.micro_series.parts, session.voice_files):
        wav = Path(wav_path)
        part_id = f"{review_id}_part{part.part_number}"
        bundle_dir = output_dir / part_id
        bundle_dir.mkdir(parents=True, exist_ok=True)

        print(f"  Part {part.part_number}/{part.total_parts}:")
        print(f"    wav      : {wav}")

        # Regenerate subtitles (in case subtitle settings changed too)
        srt_path = bundle_dir / "subtitles.srt"
        print(f"    subtitle : regenerating...")
        try:
            subtitle_path = generate_subtitle_file(wav, part.script_text, bundle_dir)
        except Exception as exc:
            print(f"    ❌ Subtitle generation failed: {exc}")
            continue

        # Render video
        output_path = bundle_dir / "video.mp4"
        hook_text = extract_hook_sentence(part.script_text) or session.micro_series.series_title

        print(f"    hook     : {hook_text[:50]}...")
        print(f"    output   : {output_path}")

        try:
            render_story_video(
                background_video_path=session.background_video_path,
                audio_path=wav,
                output_path=output_path,
                subtitle_path=subtitle_path,
                hook_text=hook_text,
                part_number=part.part_number,
                total_parts=part.total_parts,
                series_id=series_id,
            )
            print(f"    ✅ Done ({output_path.stat().st_size // 1024 // 1024} MB)\n")
            results.append(output_path)
        except Exception as exc:
            print(f"    ❌ Render failed: {exc}\n")
            continue

    print(f"\n{'=' * 50}")
    print(f"✅ Re-render complete: {len(results)}/{len(session.micro_series.parts)} parts")
    for r in results:
        print(f"   {r}")
    print()


if __name__ == "__main__":
    main()
