from __future__ import annotations

import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Sequence

from src import config


LOGGER = logging.getLogger(__name__)
VALID_STATUS: Final[str] = "valid"
REJECTED_STATUS: Final[str] = "rejected"


class ProbeError(Exception):
    """Raised when ffprobe cannot read media duration."""


@dataclass(frozen=True, slots=True)
class ValidationAsset:
    source_type: str
    source_path_or_url: str
    rights_status: str | None
    title: str | None
    local_path: Path


@dataclass(frozen=True, slots=True)
class ValidationResult:
    local_path: Path
    status: str
    reason: str
    duration: float | None
    rejected_path: Path | None


@dataclass(frozen=True, slots=True)
class ValidationBatchResult:
    results: tuple[ValidationResult, ...]


def validate_assets(
    assets: Sequence[ValidationAsset],
    rejected_dir: Path = config.REJECTED_DIR,
) -> ValidationBatchResult:
    rejected_dir.mkdir(parents=True, exist_ok=True)

    results = tuple(validate_asset(asset=asset, rejected_dir=rejected_dir) for asset in assets)
    _log_validation_summary(results)
    return ValidationBatchResult(results=results)


def validate_asset(
    asset: ValidationAsset,
    rejected_dir: Path = config.REJECTED_DIR,
) -> ValidationResult:
    rejected_dir.mkdir(parents=True, exist_ok=True)

    if not asset.local_path.exists():
        return _build_rejected_result(
            local_path=asset.local_path,
            rejected_dir=rejected_dir,
            reason=f"Local path does not exist: {asset.local_path}",
            duration=None,
        )

    if not asset.local_path.is_file():
        return _build_rejected_result(
            local_path=asset.local_path,
            rejected_dir=rejected_dir,
            reason=f"Local path is not a file: {asset.local_path}",
            duration=None,
        )

    if not _is_supported_extension(asset.local_path):
        return _build_rejected_result(
            local_path=asset.local_path,
            rejected_dir=rejected_dir,
            reason=f"Unsupported extension: {asset.local_path.suffix.lower()}",
            duration=None,
        )

    if asset.title is None or not asset.title.strip():
        return _build_rejected_result(
            local_path=asset.local_path,
            rejected_dir=rejected_dir,
            reason="Title is required.",
            duration=None,
        )

    if asset.rights_status is None or not asset.rights_status.strip():
        return _build_rejected_result(
            local_path=asset.local_path,
            rejected_dir=rejected_dir,
            reason="rights_status is required.",
            duration=None,
        )

    if asset.rights_status not in config.ALLOWED_RIGHTS_STATUSES:
        return _build_rejected_result(
            local_path=asset.local_path,
            rejected_dir=rejected_dir,
            reason=f"Invalid rights_status: {asset.rights_status}",
            duration=None,
        )

    try:
        duration = _probe_duration(asset.local_path)
    except ProbeError as exc:
        return _build_rejected_result(
            local_path=asset.local_path,
            rejected_dir=rejected_dir,
            reason=f"Failed to read media duration: {exc}",
            duration=None,
        )

    LOGGER.info("Validation passed for %s", asset.local_path)
    return ValidationResult(
        local_path=asset.local_path,
        status=VALID_STATUS,
        reason="Validation passed.",
        duration=duration,
        rejected_path=None,
    )


def _probe_duration(local_path: Path) -> float:
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

    try:
        completed_process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=config.FFPROBE_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise ProbeError(f"{config.FFPROBE_BIN} is not installed.") from exc
    except subprocess.TimeoutExpired as exc:
        raise ProbeError(f"{config.FFPROBE_BIN} timed out.") from exc

    if completed_process.returncode != 0:
        stderr_output = completed_process.stderr.strip() or "ffprobe returned a non-zero exit code."
        raise ProbeError(stderr_output)

    duration_output = completed_process.stdout.strip()
    if not duration_output:
        raise ProbeError("ffprobe returned empty duration output.")

    try:
        return float(duration_output)
    except ValueError as exc:
        raise ProbeError(f"Invalid ffprobe duration output: {duration_output}") from exc


def _build_rejected_result(
    local_path: Path,
    rejected_dir: Path,
    reason: str,
    duration: float | None,
) -> ValidationResult:
    rejected_path: Path | None = None
    final_reason = reason

    if local_path.exists() and local_path.is_file():
        try:
            rejected_path = _move_to_rejected(local_path=local_path, rejected_dir=rejected_dir)
        except OSError as exc:
            LOGGER.exception("Failed to move rejected file %s", local_path)
            final_reason = f"{reason} Failed to move rejected file: {exc}"

    LOGGER.warning("Validation rejected %s: %s", local_path, final_reason)
    return ValidationResult(
        local_path=local_path,
        status=REJECTED_STATUS,
        reason=final_reason,
        duration=duration,
        rejected_path=rejected_path,
    )


def _move_to_rejected(local_path: Path, rejected_dir: Path) -> Path:
    destination_path = rejected_dir / local_path.name
    collision_index = 1

    while destination_path.exists():
        destination_path = rejected_dir / (
            f"{local_path.stem}{config.REJECTED_COLLISION_SEPARATOR}{collision_index}{local_path.suffix}"
        )
        collision_index += 1

    moved_path = shutil.move(str(local_path), str(destination_path))
    return Path(moved_path)


def _is_supported_extension(local_path: Path) -> bool:
    return local_path.suffix.lower() in config.SUPPORTED_EXTENSIONS


def _log_validation_summary(results: Sequence[ValidationResult]) -> None:
    valid_count = sum(result.status == VALID_STATUS for result in results)
    rejected_count = sum(result.status == REJECTED_STATUS for result in results)
    LOGGER.info(
        "Validation summary: valid=%s rejected=%s total=%s",
        valid_count,
        rejected_count,
        len(results),
    )
