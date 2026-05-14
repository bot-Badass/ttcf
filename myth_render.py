"""CLI for myth-bust video rendering.

Usage:
  python myth_render.py --script path/to/script.txt --audio path/to/audio.wav \
      --channel law --output /tmp/output.mp4

Produces three files: output_tiktok.mp4, output_instagram.mp4, output_youtube.mp4
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Myth-bust video renderer")
    parser.add_argument("--script", required=True, help="Script file with ##bg: markers")
    parser.add_argument("--audio", required=True, help="Voiceover WAV file")
    parser.add_argument("--channel", default="law", choices=["law", "finance"])
    parser.add_argument("--output", required=True, help="Base output path (platform suffix appended)")
    parser.add_argument("--category", default="",
                        help="Hook frame category label (empty = no pill)")
    args = parser.parse_args()

    script_path = Path(args.script)
    audio_path = Path(args.audio)

    if not script_path.is_file():
        sys.exit(f"ERROR: script not found: {script_path}")
    if not audio_path.is_file():
        sys.exit(f"ERROR: audio not found: {audio_path}")

    script_text = script_path.read_text(encoding="utf-8")

    from src import config as _config
    from src.myth_pipeline import parse_myth_script, prepare_myth_assets, render_myth_video

    channel_profile = _config.CHANNEL_PROFILES.get(
        args.channel, _config.CHANNEL_PROFILES[_config.DEFAULT_CHANNEL]
    )

    sections = parse_myth_script(script_text)
    print(f"Script: {len(sections)} sections")
    for i, s in enumerate(sections):
        print(f"  [{i+1}] bg={s['query']!r}")
    print()

    work_dir = Path(args.output).parent
    print("Preparing assets (Pexels + subtitles)...")
    assets = prepare_myth_assets(
        script_text,
        audio_path,
        work_dir,
        highlight_color=channel_profile.get("highlight_color") or None,
    )

    base = Path(args.output).with_suffix("")
    try:
        for platform in ["tiktok", "youtube", "instagram"]:
            out = base.parent / f"{base.name}_{platform}.mp4"
            print(f"PROGRESS: {platform}", flush=True)
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
            print(f"DONE: {platform}", flush=True)
            print(f"  {result}  ({size_mb:.1f} MB)", flush=True)
    finally:
        assets["bg_video"].unlink(missing_ok=True)


if __name__ == "__main__":
    main()
