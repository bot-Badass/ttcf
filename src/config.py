from __future__ import annotations

import os
from pathlib import Path
from typing import Final


def _normalize_whisperx_device(device: str) -> str:
    return device.strip().lower()


def _parse_bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


BASE_DIR: Final[Path] = Path(__file__).resolve().parent.parent
DATA_DIR: Final[Path] = BASE_DIR / "data"

INPUT_DIR: Final[Path] = DATA_DIR / "inbox"
APPROVED_DIR: Final[Path] = DATA_DIR / "approved"
PROCESSED_DIR: Final[Path] = DATA_DIR / "processed"
REJECTED_DIR: Final[Path] = DATA_DIR / "rejected"
LOG_DIR: Final[Path] = DATA_DIR / "logs"
MANIFEST_PATH: Final[Path] = DATA_DIR / "manifests" / "manifest.sqlite3"
PUBLISH_QUEUE_PATH: Final[Path] = DATA_DIR / "manifests" / "publish_queue.sqlite3"
APPROVED_SOURCES_PATH: Final[Path] = DATA_DIR / "approved_sources.json"
STARTUP_LOG_PATH: Final[Path] = LOG_DIR / "startup.log"
REDDIT_BACKGROUND_VIDEO_TEMPLATE_PATH: Final[Path] = DATA_DIR / "background_template.mp4"
REDDIT_REVIEW_STORE_PATH: Final[Path] = DATA_DIR / "manifests" / "reddit_story_reviews.json"

# Reddit
MAX_DURATION_SECONDS: Final[int] = 600
CLIP_LENGTH_SECONDS: Final[int] = 30
OUTPUT_WIDTH: Final[int] = 1080
OUTPUT_HEIGHT: Final[int] = 1920
REDDIT_MIN_SCORE: Final[int] = 7000
REDDIT_MIN_COMBINED_TEXT_LENGTH: Final[int] = 120
REDDIT_APPROVAL_EXCERPT_LENGTH: Final[int] = 280
REDDIT_ALLOWED_SUBREDDITS: Final[tuple[str, ...]] = (
    "AmItheAsshole",
    "BestofRedditorUpdates",
    "offmychest",
    "relationships",
    "TIFU",
)
REDDIT_FETCH_LIMIT_PER_LISTING: Final[int] = 10
REDDIT_USER_AGENT: Final[str] = "ttcf/1.0 (reddit intake)"

# Telegram
TELEGRAM_BOT_TOKEN: Final[str | None] = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: Final[str | None] = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_POLLING_ENABLED: Final[bool] = _parse_bool_env(
    os.getenv("TELEGRAM_POLLING_ENABLED", "false")
)
TELEGRAM_POLLING_INTERVAL_SECONDS: Final[int] = int(
    os.getenv("TELEGRAM_POLLING_INTERVAL_SECONDS", "5")
)

# TTS
RESPEECHER_API_KEY: Final[str | None] = os.getenv("RESPEECHER_API_KEY")
RESPEECHER_DEFAULT_VOICE_ID: Final[str | None] = os.getenv("RESPEECHER_DEFAULT_VOICE_ID")

# Translation
DEEPL_API_KEY: Final[str | None] = os.getenv("DEEPL_API_KEY")
DEEPL_SERVER_URL: Final[str] = os.getenv(
    "DEEPL_SERVER_URL",
    "https://api-free.deepl.com",
)
DEEPL_MAX_CHARS_PER_REQUEST: Final[int] = int(
    os.getenv("DEEPL_MAX_CHARS_PER_REQUEST", "50000")
)
DEEPL_MAX_RETRIES: Final[int] = int(os.getenv("DEEPL_MAX_RETRIES", "3"))
DEEPL_RETRY_BASE_DELAY_SECONDS: Final[float] = float(
    os.getenv("DEEPL_RETRY_BASE_DELAY_SECONDS", "0.5")
)

# Subtitles
SUBTITLE_ALIGNMENT_MODE: Final[str] = os.getenv(
    "SUBTITLE_ALIGNMENT_MODE",
    "fallback",
)
WHISPERX_MODEL_SIZE: Final[str] = os.getenv("WHISPERX_MODEL_SIZE", "small")
WHISPERX_DEVICE: Final[str] = os.getenv("WHISPERX_DEVICE", "cpu")
WHISPERX_COMPUTE_TYPE: Final[str] = os.getenv(
    "WHISPERX_COMPUTE_TYPE",
    "int8" if _normalize_whisperx_device(WHISPERX_DEVICE) == "cpu" else "float16",
)
SUBTITLE_MAX_WORDS_PER_PHRASE: Final[int] = int(
    os.getenv("SUBTITLE_MAX_WORDS_PER_PHRASE", "6")
)
SUBTITLE_MAX_CHARS_PER_PHRASE: Final[int] = int(
    os.getenv("SUBTITLE_MAX_CHARS_PER_PHRASE", "42")
)
SUBTITLE_MAX_SILENCE_GAP_SECONDS: Final[float] = float(
    os.getenv("SUBTITLE_MAX_SILENCE_GAP_SECONDS", "0.5")
)

# Subtitle style
SUBTITLE_FONT_NAME: Final[str] = os.getenv("SUBTITLE_FONT_NAME", "Arial")
SUBTITLE_FONT_SIZE: Final[int] = int(os.getenv("SUBTITLE_FONT_SIZE", "22"))
SUBTITLE_PRIMARY_COLOR: Final[str] = os.getenv(
    "SUBTITLE_PRIMARY_COLOR", "&H00FFFFFF"
)
SUBTITLE_OUTLINE_COLOR: Final[str] = os.getenv(
    "SUBTITLE_OUTLINE_COLOR", "&H00000000"
)
SUBTITLE_BACK_COLOR: Final[str] = os.getenv(
    "SUBTITLE_BACK_COLOR", "&H80000000"
)
SUBTITLE_BOLD: Final[int] = int(os.getenv("SUBTITLE_BOLD", "1"))
SUBTITLE_OUTLINE_WIDTH: Final[int] = int(os.getenv("SUBTITLE_OUTLINE_WIDTH", "2"))
SUBTITLE_SHADOW_DEPTH: Final[int] = int(os.getenv("SUBTITLE_SHADOW_DEPTH", "0"))
SUBTITLE_ALIGNMENT: Final[int] = int(os.getenv("SUBTITLE_ALIGNMENT", "5"))
SUBTITLE_MARGIN_V: Final[int] = int(os.getenv("SUBTITLE_MARGIN_V", "0"))
SUBTITLE_BORDER_STYLE: Final[int] = int(os.getenv("SUBTITLE_BORDER_STYLE", "3"))

# Render
MAX_CONCURRENCY: Final[int] = 2
FILE_HASH_CHUNK_SIZE: Final[int] = 65536
BACKGROUND_VIDEO_PATH: Final[str] = os.getenv("BACKGROUND_VIDEO_PATH", "")
FFMPEG_PATH: Final[str] = os.getenv("FFMPEG_PATH", "ffmpeg")
FFMPEG_BIN: Final[str] = FFMPEG_PATH
FFPROBE_BIN: Final[str] = os.getenv("FFPROBE_BIN", "ffprobe")
FFPROBE_TIMEOUT_SECONDS: Final[int] = int(os.getenv("FFPROBE_TIMEOUT_SECONDS", "30"))
FFMPEG_TIMEOUT_SECONDS: Final[int] = int(os.getenv("FFMPEG_TIMEOUT_SECONDS", "600"))
REJECTED_COLLISION_SEPARATOR: Final[str] = "__rejected__"
OUTPUT_VIDEO_CODEC: Final[str] = "libx264"
OUTPUT_AUDIO_CODEC: Final[str] = "aac"
OUTPUT_PIXEL_FORMAT: Final[str] = "yuv420p"
OUTPUT_PRESET: Final[str] = "fast"
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

# Advice pipeline — local video fallback (skips Pexels when set)
ADVICE_LOCAL_BACKGROUND_VIDEO: Final[str] = os.getenv("ADVICE_LOCAL_BACKGROUND_VIDEO", "")

# Hook frame — static title card prepended to each video
HOOK_FRAME_ENABLED: Final[bool] = _parse_bool_env(os.getenv("HOOK_FRAME_ENABLED", "true"))
HOOK_FRAME_DURATION: Final[float] = float(os.getenv("HOOK_FRAME_DURATION", "2.0"))
HOOK_FRAME_FONT_SIZE: Final[int] = int(os.getenv("HOOK_FRAME_FONT_SIZE", "52"))
HOOK_FRAME_BG_COLOR: Final[str] = os.getenv("HOOK_FRAME_BG_COLOR", "0x0a0a0a")
HOOK_FRAME_TEXT_COLOR: Final[str] = os.getenv("HOOK_FRAME_TEXT_COLOR", "white")
HOOK_FRAME_ACCENT_COLOR: Final[str] = os.getenv("HOOK_FRAME_ACCENT_COLOR", "0xFF3B30")  # red accent bar
HOOK_FRAME_FONT_FILE: Final[str] = os.getenv("HOOK_FRAME_FONT_FILE", "")  # path to .ttf, empty = ffmpeg default
HOOK_FRAME_BRAND_LABEL: Final[str] = os.getenv("HOOK_FRAME_BRAND_LABEL", "")  # e.g. "MONEY UA" shown above hook text; empty = hidden

# Hook frame V2 — dark left-bar layout (looks like a designer made it)
# HOOK_FRAME_LAYOUT=v2 activates the new design; any other value = classic
HOOK_FRAME_LAYOUT: Final[str] = os.getenv("HOOK_FRAME_LAYOUT", "classic")
# Category pill text shown above the main hook text (e.g. "МОБІЛІЗАЦІЯ", "ФІНАНСИ")
HOOK_FRAME_CATEGORY: Final[str] = os.getenv("HOOK_FRAME_CATEGORY", "")
# Pill background colour (defaults to accent colour when empty)
HOOK_FRAME_CATEGORY_BG: Final[str] = os.getenv("HOOK_FRAME_CATEGORY_BG", "")
# Pill text colour (dark brown works on yellow; override if needed)
HOOK_FRAME_CATEGORY_FG: Final[str] = os.getenv("HOOK_FRAME_CATEGORY_FG", "0x78350F")

# Advice pipeline — content plan
CONTENT_PLAN_PATH: Final[Path] = DATA_DIR / "content_plan.json"
SCRIPT_PROMPT_PATH: Final[Path] = DATA_DIR / "prompts" / "script_prompt.md"
ADVICE_SCRIPT_MIN_WORDS: Final[int] = int(os.getenv("ADVICE_SCRIPT_MIN_WORDS", "100"))

# Multi-channel profiles — one bot, two content channels.
# Each profile overrides plan_path, prompt_path and hook-frame visuals.
CHANNEL_PROFILES: Final[dict[str, dict]] = {
    "law": {
        "label": "⚖️ DontPanicLaw",
        "plan_path": DATA_DIR / "content_plan.json",
        "prompt_path": DATA_DIR / "prompts" / "script_prompt.md",
        "hook_bg": "0x0a0a0a",
        "hook_accent": "0xFF3B30",
        "hook_brand": "",
    },
    "finance": {
        "label": "💰 MoneyUA",
        "plan_path": DATA_DIR / "content_plan_moneyua.json",
        "prompt_path": DATA_DIR / "prompts" / "script_prompt_finance.md",
        "hook_bg": "0x0d1b3e",
        "hook_accent": "0xFFD700",
        "hook_brand": "MONEY UA",
    },
}
DEFAULT_CHANNEL: Final[str] = "law"

# Advice pipeline — TTS
ADVICE_TTS_VOICE: Final[str] = os.getenv("ADVICE_TTS_VOICE", "dmytro")
ADVICE_PIPER_MODEL: Final[str] = os.getenv("ADVICE_PIPER_MODEL", "")

# Advice pipeline — Pexels
PEXELS_API_KEY: Final[str | None] = os.getenv("PEXELS_API_KEY")
ADVICE_PEXELS_QUERY: Final[str] = os.getenv("ADVICE_PEXELS_QUERY", "ukraine city night")
ADVICE_PEXELS_VIDEO_MIN_DURATION: Final[int] = int(
    os.getenv("ADVICE_PEXELS_VIDEO_MIN_DURATION", "15")
)

# Advice pipeline — micro-series
ADVICE_MICRO_SERIES_MIN_PARTS: Final[int] = int(os.getenv("ADVICE_MICRO_SERIES_MIN_PARTS", "3"))
ADVICE_MICRO_SERIES_MAX_PARTS: Final[int] = int(os.getenv("ADVICE_MICRO_SERIES_MAX_PARTS", "5"))

# Advice pipeline — store
ADVICE_REVIEW_STORE_PATH: Final[Path] = DATA_DIR / "manifests" / "advice_reviews.json"

# Advice pipeline — voice mode
VOICE_MODE: Final[bool] = _parse_bool_env(os.getenv("VOICE_MODE", "true"))
VOICE_SESSION_STORE_PATH: Final[Path] = DATA_DIR / "manifests" / "voice_sessions.json"

# Advice pipeline — output
ADVICE_OUTPUT_DIR: Final[Path] = DATA_DIR / "advice"

_TELEGRAM_RUNTIME_GROUP: Final[str] = "telegram"
_TTS_RUNTIME_GROUP: Final[str] = "tts"
_TRANSLATION_RUNTIME_GROUP: Final[str] = "translation"
_ADVICE_RUNTIME_GROUP: Final[str] = "advice"
_RUNTIME_GROUP_REQUIREMENTS: Final[dict[str, tuple[str, ...]]] = {
    _TELEGRAM_RUNTIME_GROUP: (
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
    ),
    _TTS_RUNTIME_GROUP: (
        "RESPEECHER_API_KEY",
    ),
    _TRANSLATION_RUNTIME_GROUP: (
        "DEEPL_API_KEY",
    ),
    _ADVICE_RUNTIME_GROUP: (
        "PEXELS_API_KEY",
    ),
}


def validate_runtime_config(*groups: str) -> None:
    requested_groups = groups or tuple(_RUNTIME_GROUP_REQUIREMENTS)
    missing_variables: list[str] = []

    for group in requested_groups:
        if group not in _RUNTIME_GROUP_REQUIREMENTS:
            raise RuntimeError(f"Unknown runtime config group: {group}")

        for variable_name in _RUNTIME_GROUP_REQUIREMENTS[group]:
            value = globals()[variable_name]
            if _is_missing_runtime_value(value):
                missing_variables.append(variable_name)

    if missing_variables:
        unique_missing = tuple(dict.fromkeys(missing_variables))
        raise RuntimeError(
            "Missing required runtime environment variables: "
            + ", ".join(unique_missing)
        )


def _is_missing_runtime_value(value: str | None) -> bool:
    return value is None or not value.strip()
