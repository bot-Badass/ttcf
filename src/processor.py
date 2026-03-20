from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Sequence

from src import config


LOGGER = logging.getLogger(__name__)
PROCESSED_STATUS: Final[str] = "processed"
FAILED_STATUS: Final[str] = "failed"


class ProcessingError(Exception):
    """Raised when processing commands fail for an asset."""


@dataclass(frozen=True, slots=True)
class ProcessorAsset:
    asset_id: str
    local_path: Path
    title: str
    duration: float


@dataclass(frozen=True, slots=True)
class ProcessorResult:
    asset_id: str
    source_path: Path
    status: str
    output_files: tuple[Path, ...]
    thumbnail_path: Path | None
    error_message: str | None


@dataclass(frozen=True, slots=True)
class ProcessorBatchResult:
    results: tuple[ProcessorResult, ...]


def process_assets(
    assets: Sequence[ProcessorAsset],
    processed_dir: Path = config.PROCESSED_DIR,
) -> ProcessorBatchResult:
    processed_dir.mkdir(parents=True, exist_ok=True)

    results = tuple(process_asset(asset=asset, processed_dir=processed_dir) for asset in assets)
    _log_processing_summary(results)
    return ProcessorBatchResult(results=results)


def process_asset(
    asset: ProcessorAsset,
    processed_dir: Path = config.PROCESSED_DIR,
) -> ProcessorResult:
    asset_output_dir = processed_dir / asset.asset_id
    asset_output_dir.mkdir(parents=True, exist_ok=True)

    clip_path = asset_output_dir / config.CLIP_OUTPUT_NAME
    thumbnail_path = asset_output_dir / config.THUMBNAIL_OUTPUT_NAME

    try:
        _probe_source_media(asset.local_path)
        _create_processed_clip(asset=asset, clip_path=clip_path)
        _ensure_output_exists(output_path=clip_path, output_label="clip")
        _create_thumbnail(asset=asset, thumbnail_path=thumbnail_path)
        _ensure_output_exists(output_path=thumbnail_path, output_label="thumbnail")
    except ProcessingError as exc:
        LOGGER.error("Processing failed for asset_id=%s: %s", asset.asset_id, exc)
        return ProcessorResult(
            asset_id=asset.asset_id,
            source_path=asset.local_path,
            status=FAILED_STATUS,
            output_files=(),
            thumbnail_path=None,
            error_message=str(exc),
        )

    LOGGER.info("Processing succeeded for asset_id=%s", asset.asset_id)
    return ProcessorResult(
        asset_id=asset.asset_id,
        source_path=asset.local_path,
        status=PROCESSED_STATUS,
        output_files=(clip_path,),
        thumbnail_path=thumbnail_path,
        error_message=None,
    )


def _probe_source_media(local_path: Path) -> None:
    command = [
        config.FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(local_path),
    ]
    _run_command(command=command, timeout_seconds=config.FFPROBE_TIMEOUT_SECONDS)


def _create_processed_clip(asset: ProcessorAsset, clip_path: Path) -> None:
    command = [
        config.FFMPEG_BIN,
        "-y",
        "-i",
        str(asset.local_path),
        "-map_metadata",
        "-1",
        "-vf",
        _build_vertical_video_filter(),
        "-c:v",
        config.OUTPUT_VIDEO_CODEC,
        "-preset",
        config.OUTPUT_PRESET,
        "-crf",
        str(config.OUTPUT_CRF),
        "-pix_fmt",
        config.OUTPUT_PIXEL_FORMAT,
        "-c:a",
        config.OUTPUT_AUDIO_CODEC,
        "-movflags",
        config.OUTPUT_MOVFLAGS,
    ]

    if asset.duration > config.CLIP_LENGTH_SECONDS:
        command.extend(["-t", str(config.CLIP_LENGTH_SECONDS)])

    command.append(str(clip_path))
    _run_command(command=command, timeout_seconds=config.FFMPEG_TIMEOUT_SECONDS)


def _create_thumbnail(asset: ProcessorAsset, thumbnail_path: Path) -> None:
    thumbnail_timestamp = _thumbnail_timestamp(asset.duration)
    command = [
        config.FFMPEG_BIN,
        "-y",
        "-ss",
        str(thumbnail_timestamp),
        "-i",
        str(asset.local_path),
        "-map_metadata",
        "-1",
        "-vf",
        _build_vertical_video_filter(),
        "-frames:v",
        "1",
        str(thumbnail_path),
    ]
    _run_command(command=command, timeout_seconds=config.FFMPEG_TIMEOUT_SECONDS)


def _ensure_output_exists(output_path: Path, output_label: str) -> None:
    if not output_path.exists():
        raise ProcessingError(
            f"Expected {output_label} output was not created: {output_path}"
        )


def _run_command(command: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    try:
        completed_process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise ProcessingError(f"Command not found: {command[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ProcessingError(f"Command timed out: {command[0]}") from exc

    if completed_process.returncode != 0:
        stderr_output = completed_process.stderr.strip() or "Command returned non-zero exit code."
        raise ProcessingError(stderr_output)

    return completed_process


def _build_vertical_video_filter() -> str:
    return (
        "scale="
        f"{config.OUTPUT_WIDTH}:{config.OUTPUT_HEIGHT}:force_original_aspect_ratio=decrease,"
        f"pad={config.OUTPUT_WIDTH}:{config.OUTPUT_HEIGHT}:(ow-iw)/2:(oh-ih)/2,"
        "setsar=1"
    )


def _thumbnail_timestamp(duration: float) -> float:
    if duration <= 0:
        return 0.0

    return min(config.THUMBNAIL_CAPTURE_SECOND, duration / 2)


def _log_processing_summary(results: Sequence[ProcessorResult]) -> None:
    processed_count = sum(result.status == PROCESSED_STATUS for result in results)
    failed_count = sum(result.status == FAILED_STATUS for result in results)
    LOGGER.info(
        "Processing summary: processed=%s failed=%s total=%s",
        processed_count,
        failed_count,
        len(results),
    )
