"""Quick test render for CTA variants.

Renders 3 clips from existing project assets:
  out_part1_share.mp4  — part 1/4 → share CTA ("Поділись")
  out_part2_share.mp4  — part 2/4 → share CTA ("Поділись")
  out_part4_subscribe.mp4 — part 4/4 → subscribe card ("Підпишись")

Usage:
  CTA_ENABLED=true .venv/bin/python cta_test_render.py
"""

import os
import sys
from pathlib import Path

# Force CTA on before importing config/render
os.environ.setdefault("CTA_ENABLED", "true")
os.environ.setdefault("HOOK_FRAME_ENABLED", "false")  # skip hook to keep it simple

from src import config  # noqa: E402
from src.render import render_story_video  # noqa: E402

SERIES = "045e44ed39584085b81ade033078bfda"
BASE = Path("data/advice")
BACKGROUND = BASE / SERIES / "background.mp4"

# Reuse part1 voiceover for all test clips (content doesn't matter for the CTA check)
AUDIO = BASE / f"{SERIES}_part1" / "voiceover.wav"
SUBS = BASE / f"{SERIES}_part1" / "subtitles.srt"
OUT_DIR = Path("data/advice")

cases = [
    {"label": "part1_share",     "part": 1, "total": 4, "out": OUT_DIR / "cta_test_part1_share.mp4"},
    {"label": "part2_share",     "part": 2, "total": 4, "out": OUT_DIR / "cta_test_part2_share.mp4"},
    {"label": "part4_subscribe", "part": 4, "total": 4, "out": OUT_DIR / "cta_test_part4_subscribe.mp4"},
]

print(f"CTA_ENABLED       = {config.CTA_ENABLED}")
print(f"CTA_SHARE_OVERLAY = {config.CTA_SHARE_OVERLAY_PATH}")
print(f"share overlay exists: {Path(config.CTA_SHARE_OVERLAY_PATH).is_file()}")
print()

for case in cases:
    out: Path = case["out"]
    out.unlink(missing_ok=True)
    print(f"[{case['label']}] rendering part {case['part']}/{case['total']} → {out}")
    try:
        render_story_video(
            background_video_path=BACKGROUND,
            audio_path=AUDIO,
            output_path=out,
            subtitle_path=SUBS,
            part_number=case["part"],
            total_parts=case["total"],
        )
        size_mb = out.stat().st_size / 1_000_000
        print(f"  ✓ {out.name}  ({size_mb:.1f} MB)")
    except Exception as exc:
        print(f"  ✗ FAILED: {exc}", file=sys.stderr)

print()
print("Done. Open with:")
for case in cases:
    print(f"  open {case['out']}")
