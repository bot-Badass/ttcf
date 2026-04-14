from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Sequence

from src import config
from src.reddit_intake import run_full_pipeline
from src.telegram_bot import run_polling_loop


LOGGER = logging.getLogger(__name__)


def ensure_directories_exist() -> None:
    for directory in config.REQUIRED_DIRECTORIES:
        directory.mkdir(parents=True, exist_ok=True)


def configure_logging() -> None:
    config.STARTUP_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(config.LOG_FORMAT)

    file_handler = logging.FileHandler(config.STARTUP_LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    configure_logging()

    try:
        ensure_directories_exist()
    except Exception:
        LOGGER.exception("Failed to ensure required directories exist.")
        return 1

    if args.bot:
        return _run_bot_mode(args)

    if args.reddit:
        return _run_reddit_mode(args)

    parser.print_help()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the TikTok content factory.")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--reddit", action="store_true", help="Run the Reddit pipeline only.")
    mode_group.add_argument("--bot", action="store_true", help="Run the Telegram polling bot.")
    parser.add_argument(
        "--background-video-path",
        type=Path,
        default=None,
        help="Override BACKGROUND_VIDEO_PATH for Reddit pipeline or bot runs.",
    )
    return parser


def _run_reddit_mode(args: argparse.Namespace) -> int:
    try:
        background_video_path = _resolve_background_video_path(args.background_video_path)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    try:
        config.validate_runtime_config("telegram", "translation", "tts")
        result = run_full_pipeline(background_video_path=background_video_path)
    except Exception:
        LOGGER.exception("Reddit pipeline run failed.")
        return 1

    LOGGER.info("Reddit pipeline run complete: %s", result)
    print(
        " | ".join(
            (
                f"fetched={result.fetched}",
                f"accepted={result.accepted}",
                f"persisted={result.persisted}",
                f"sent={result.sent_to_telegram}",
                f"processed_updates={result.processed_updates}",
                f"translated={result.translated}",
                f"rendered={result.rendered}",
                f"enqueued={result.enqueued}",
            )
        )
    )
    for stage_error in result.stage_errors:
        LOGGER.error("Reddit pipeline stage error: %s", stage_error)
    return 1 if result.stage_errors else 0


def _run_bot_mode(args: argparse.Namespace) -> int:
    if not config.TELEGRAM_POLLING_ENABLED:
        print("TELEGRAM_POLLING_ENABLED must be true to start the Telegram bot.", file=sys.stderr)
        return 1

    try:
        config.validate_runtime_config("telegram", "translation", "tts")
        LOGGER.info("Starting Telegram polling loop.")
        run_polling_loop(
            background_video_path=_resolve_optional_background_video_path(args.background_video_path)
        )
    except KeyboardInterrupt:
        LOGGER.info("Telegram polling loop stopped by operator.")
        return 0
    except Exception:
        LOGGER.exception("Telegram polling loop failed.")
        return 1
    return 0


def _resolve_background_video_path(override_path: Path | None) -> Path:
    if override_path is not None:
        return override_path
    configured_path = config.BACKGROUND_VIDEO_PATH.strip()
    if configured_path:
        return Path(configured_path)
    raise RuntimeError(
        "BACKGROUND_VIDEO_PATH is required for Reddit runs. Pass --background-video-path or set BACKGROUND_VIDEO_PATH."
    )


def _resolve_optional_background_video_path(override_path: Path | None) -> Path | None:
    if override_path is not None:
        return override_path
    configured_path = config.BACKGROUND_VIDEO_PATH.strip()
    if configured_path:
        return Path(configured_path)
    return None


if __name__ == "__main__":
    raise SystemExit(main())
