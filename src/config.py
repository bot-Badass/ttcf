from __future__ import annotations

from pathlib import Path
from typing import Final


BASE_DIR: Final[Path] = Path(__file__).resolve().parent.parent
DATA_DIR: Final[Path] = BASE_DIR / "data"

INPUT_DIR: Final[Path] = DATA_DIR / "inbox"
APPROVED_DIR: Final[Path] = DATA_DIR / "approved"
PROCESSED_DIR: Final[Path] = DATA_DIR / "processed"
REJECTED_DIR: Final[Path] = DATA_DIR / "rejected"
LOG_DIR: Final[Path] = DATA_DIR / "logs"
MANIFEST_PATH: Final[Path] = DATA_DIR / "manifests" / "manifest.sqlite3"
APPROVED_SOURCES_PATH: Final[Path] = DATA_DIR / "approved_sources.json"
STARTUP_LOG_PATH: Final[Path] = LOG_DIR / "startup.log"

MAX_DURATION_SECONDS: Final[int] = 600
CLIP_LENGTH_SECONDS: Final[int] = 30
OUTPUT_WIDTH: Final[int] = 1080
OUTPUT_HEIGHT: Final[int] = 1920

MAX_CONCURRENCY: Final[int] = 2
FILE_HASH_CHUNK_SIZE: Final[int] = 65536
FFPROBE_BIN: Final[str] = "ffprobe"
FFPROBE_TIMEOUT_SECONDS: Final[int] = 30
FFMPEG_BIN: Final[str] = "ffmpeg"
FFMPEG_TIMEOUT_SECONDS: Final[int] = 120
REJECTED_COLLISION_SEPARATOR: Final[str] = "__rejected__"
OUTPUT_VIDEO_CODEC: Final[str] = "libx264"
OUTPUT_AUDIO_CODEC: Final[str] = "aac"
OUTPUT_PIXEL_FORMAT: Final[str] = "yuv420p"
OUTPUT_PRESET: Final[str] = "medium"
OUTPUT_CRF: Final[int] = 23
OUTPUT_MOVFLAGS: Final[str] = "+faststart"
CLIP_OUTPUT_NAME: Final[str] = "clip_001.mp4"
THUMBNAIL_OUTPUT_NAME: Final[str] = "thumb.jpg"
THUMBNAIL_CAPTURE_SECOND: Final[float] = 1.0

ALLOWED_RIGHTS_STATUSES: Final[tuple[str, ...]] = (
    "owned",
    "licensed",
    "ugc-approved",
)
SUPPORTED_EXTENSIONS: Final[tuple[str, ...]] = (
    ".mp4",
    ".mov",
    ".m4v",
    ".mkv",
)
LOG_FORMAT: Final[str] = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
STARTUP_MESSAGE: Final[str] = "Startup initialization complete."
STARTUP_FAILURE_MESSAGE: Final[str] = "Startup initialization failed."

REQUIRED_DIRECTORIES: Final[tuple[Path, ...]] = (
    INPUT_DIR,
    APPROVED_DIR,
    PROCESSED_DIR,
    REJECTED_DIR,
    MANIFEST_PATH.parent,
    LOG_DIR,
)
