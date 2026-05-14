from __future__ import annotations

import csv
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.advice_pipeline import AdviceVoiceSession

from src import config

_PLATFORMS = ["tiktok", "youtube", "instagram"]

# (morning_time, evening_time) per platform index
_PLATFORM_TIMES = [
    ("09:00", "20:00"),   # TikTok
    ("09:15", "20:15"),   # YouTube
    ("09:30", "20:30"),   # Instagram
]

DATE_FMT = "%Y-%m-%d"
DATE_COL = "Date - Intl. format or prompt"
EXPORTS_ROOT = config.DATA_DIR / "exports"

CSV_HEADERS = [
    "Date - Intl. format or prompt",
    "Text",
    "Link(s) - Separated by comma for FB carousels",
    "Media URL(s) - Separated by comma",
    "Title - For the video, pin, PDF ..",
    "Label(s) - Separated by comma",
    "Alt text(s) - Separated by ||",
    "Comment(s) - Separated by ||",
    "Pin board, FB album, or Google category",
    "Post subtype - I.e. story, reel, PDF ..",
    "CTA - For Facebook links or Google",
    "Reminder - For stories, reels, shorts, and TikToks",
]


def find_export_dir(topic_id: str) -> Path | None:
    if not EXPORTS_ROOT.is_dir():
        return None
    for d in EXPORTS_ROOT.iterdir():
        if d.is_dir() and f"_{topic_id}_" in d.name:
            return d
    return None


def draft_csv_path(export_dir: Path, platform: str) -> Path:
    return export_dir / f"publer_{platform}.csv"


def fill_csv_dates(export_dir: Path, start_date_str: str) -> None:
    """Fill Date column in all three per-platform draft CSVs.

    Schedule: part 1 -> start_date evening, part 2 -> +1day morning,
    part 3 -> +1day evening, part 4 -> +2days morning, etc.
    """
    start = datetime.strptime(start_date_str, DATE_FMT)

    for platform_idx, platform in enumerate(_PLATFORMS):
        csv_path = draft_csv_path(export_dir, platform)
        if not csv_path.is_file():
            continue

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            rows = list(reader)

        for part_idx, row in enumerate(rows):
            part_num = part_idx + 1
            is_evening = (part_num % 2 == 1)
            day_offset = (part_num - 1) // 2 if is_evening else part_num // 2
            date = start + timedelta(days=day_offset)
            time_str = _PLATFORM_TIMES[platform_idx][1 if is_evening else 0]
            row[DATE_COL] = f"{date.strftime(DATE_FMT)} {time_str}"

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)


def copy_videos_to_export(session: "AdviceVoiceSession") -> Path | None:
    """Copy rendered video.mp4 files to export directory with proper names."""
    export_dir = find_export_dir(session.topic_id)
    if export_dir is None:
        return None

    dir_name = export_dir.name
    parts_total = len(session.micro_series.parts)
    copied = 0
    for part_num in range(1, parts_total + 1):
        src = config.ADVICE_OUTPUT_DIR / f"{session.review_id}_part{part_num}" / "video.mp4"
        if src.is_file():
            dst = export_dir / f"{dir_name}_ch{part_num}.mp4"
            shutil.copy2(src, dst)
            copied += 1

    return export_dir if copied > 0 else None
