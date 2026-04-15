#!/usr/bin/env python3
"""
Preview hook frame without rendering a full video.

Usage:
    # Single preview
    python preview_hook.py "2 МЛН ЗНІМУТЬ З РОЗШУКУ?" --series A --part 1 --category МОБІЛІЗАЦІЯ

    # Preview all 4 parts of a series at once
    python preview_hook.py "ТЦК заборонять силу?" --series A --category МОБІЛІЗАЦІЯ --all-parts

    # Compare all series themes for part 1
    python preview_hook.py "Штраф 25 000 грн" --all-series --category ВПО

    # Classic layout for comparison
    python preview_hook.py "2 МЛН ЗНІМУТЬ З РОЗШУКУ?" --layout classic

The script renders hook_preview*.mp4 and opens them with the system player.
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

# Allow running from project root without installing the package.
sys.path.insert(0, str(Path(__file__).parent))

# Load .env before importing config so env vars are populated.
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

from src import config  # noqa: E402
from src.render import (  # noqa: E402
    _render_hook_frame,
    _SERIES_THEMES,
    _PART_LAYOUTS,
)


def _run(cmd: list[str], timeout: int) -> subprocess.CompletedProcess:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        print("\n❌ ffmpeg error (returncode=%d):" % result.returncode, file=sys.stderr)
        print(result.stderr[-3000:], file=sys.stderr)
        sys.exit(1)
    # Show ffmpeg warnings even on success (helps debug silent rendering issues)
    if os.environ.get("PREVIEW_DEBUG"):
        if result.stderr:
            print("[ffmpeg stderr]:", result.stderr[-1000:], file=sys.stderr)
    return result


def render_one(
    text: str,
    out_path: Path,
    series_id: str | None,
    part_number: int | None,
    category: str,
    layout: str | None,
    bg_video: Path | None = None,
    hook_bg_override: str | None = None,
    hook_accent_override: str | None = None,
    hook_brand_override: str | None = None,
) -> None:
    # Apply overrides
    if category:
        config.HOOK_FRAME_CATEGORY = category  # type: ignore[attr-defined]
    if layout:
        config.HOOK_FRAME_LAYOUT = layout  # type: ignore[attr-defined]

    sid_str = series_id or "?"
    pn_str = str(part_number) if part_number else "?"
    layout_used = _PART_LAYOUTS.get(part_number or 1, "left") if (layout or config.HOOK_FRAME_LAYOUT).lower() == "v2" else "classic"
    bg_str = str(bg_video) if bg_video else "(solid colour fallback)"

    print(f"  🎬 series={sid_str}  part={pn_str}  layout={layout_used}  bg={bg_str}  → {out_path.name}")

    _render_hook_frame(
        hook_text=text,
        output_path=out_path,
        ffmpeg_path=config.FFMPEG_PATH,
        run_command=_run,
        series_id=series_id,
        part_number=part_number,
        background_video_path=bg_video,
        hook_bg_override=hook_bg_override,
        hook_accent_override=hook_accent_override,
        hook_brand_override=hook_brand_override,
    )


def open_file(path: Path) -> None:
    if sys.platform == "darwin":
        subprocess.run(["open", str(path)])
    elif sys.platform.startswith("linux"):
        subprocess.run(["xdg-open", str(path)])
    elif sys.platform == "win32":
        os.startfile(str(path))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview hook frame — single, all parts, or all series",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("text", help="Hook text (3-6 words work best)")
    parser.add_argument("--series", default=None, help="Series ID e.g. A, B, C (sets colour theme)")
    parser.add_argument("--part", type=int, default=None, help="Part number 1-4 (sets bar layout)")
    parser.add_argument("--category", default="", help="Category pill e.g. МОБІЛІЗАЦІЯ, ВПО, ФІНАНСИ")
    parser.add_argument("--layout", choices=["v2", "classic"], default=None, help="Override layout")
    parser.add_argument(
        "--all-parts", action="store_true",
        help="Render parts 1-4 for the given --series and open all",
    )
    parser.add_argument(
        "--all-series", action="store_true",
        help="Render part 1 for every series A-I and open all",
    )
    parser.add_argument("--out", default="hook_preview.mp4", help="Output file (single render)")
    parser.add_argument("--no-open", action="store_true", help="Do not open files after render")
    parser.add_argument("--bg-video", default=None, help="Path to background video (overrides .env BACKGROUND_VIDEO_PATH)")
    parser.add_argument("--channel", choices=list(config.CHANNEL_PROFILES.keys()), default=None, help="Channel profile: law or finance (applies brand colors automatically)")
    args = parser.parse_args()

    # Apply category to config so it is visible inside render functions
    if args.category:
        config.HOOK_FRAME_CATEGORY = args.category  # type: ignore[attr-defined]
    if args.layout:
        config.HOOK_FRAME_LAYOUT = args.layout  # type: ignore[attr-defined]

    active_layout = config.HOOK_FRAME_LAYOUT.strip().lower()

    # Resolve channel profile overrides
    hook_bg_override: str | None = None
    hook_accent_override: str | None = None
    hook_brand_override: str | None = None
    if args.channel:
        profile = config.CHANNEL_PROFILES[args.channel]
        hook_bg_override = profile.get("hook_bg")
        hook_accent_override = profile.get("hook_accent")
        hook_brand_override = profile.get("hook_brand")
        print(f"   channel  : {profile['label']} (bg={hook_bg_override} accent={hook_accent_override})")

    # Resolve background video path
    bg_video: Path | None = None
    bg_video_str = args.bg_video or os.environ.get("BACKGROUND_VIDEO_PATH", "")
    if bg_video_str:
        p = Path(bg_video_str)
        if p.is_file():
            bg_video = p
        else:
            print(f"⚠️  Background video not found: {p} — falling back to solid colour")

    print(f"\n🎨 Hook frame preview  |  layout={active_layout}")
    print(f"   text     : {args.text}")
    print(f"   category : {args.category or '(none)'}")
    print(f"   brand    : {config.HOOK_FRAME_BRAND_LABEL or '(none)'}")
    print(f"   bg video : {bg_video or '(solid colour)'}\n")

    rendered: list[Path] = []

    if args.all_series:
        series_keys = list(_SERIES_THEMES.keys())
        print(f"Rendering {len(series_keys)} series themes (part=1 each)...\n")
        for sid in series_keys:
            out = Path(f"hook_preview_series_{sid}.mp4")
            render_one(args.text, out, sid, 1, args.category, args.layout, bg_video, hook_bg_override, hook_accent_override, hook_brand_override)
            rendered.append(out)

    elif args.all_parts:
        series = args.series or "A"
        parts = sorted(_PART_LAYOUTS.keys())
        print(f"Rendering {len(parts)} part layouts for series={series}...\n")
        for pn in parts:
            out = Path(f"hook_preview_part{pn}.mp4")
            render_one(args.text, out, series, pn, args.category, args.layout, bg_video, hook_bg_override, hook_accent_override, hook_brand_override)
            rendered.append(out)

    else:
        out = Path(args.out)
        render_one(args.text, out, args.series, args.part, args.category, args.layout, bg_video, hook_bg_override, hook_accent_override, hook_brand_override)
        rendered.append(out)

    print(f"\n✅ Done — {len(rendered)} file(s) rendered")
    for p in rendered:
        print(f"   {p.resolve()}")

    if not args.no_open:
        print("\n🖥  Opening preview(s)...")
        for p in rendered:
            open_file(p)
            time.sleep(0.3)  # small delay so player doesn't open all at once


if __name__ == "__main__":
    main()
