from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from src import config
from src.utils import compute_sha256


LOGGER = logging.getLogger(__name__)
LOCAL_FILE_SOURCE_TYPE: Final[str] = "local_file"
REMOTE_URL_SOURCE_TYPE: Final[str] = "remote_url"
INGESTED_STATUS: Final[str] = "ingested"
SKIPPED_STATUS: Final[str] = "skipped"
FAILED_STATUS: Final[str] = "failed"
REQUIRED_APPROVED_SOURCE_FIELDS: Final[tuple[str, ...]] = (
    "source_type",
    "source_path_or_url",
    "rights_status",
    "title",
)


@dataclass(frozen=True, slots=True)
class ApprovedSource:
    source_type: str
    source_path_or_url: str
    rights_status: str
    title: str


@dataclass(frozen=True, slots=True)
class IngestItemResult:
    source_type: str
    source_path_or_url: str
    rights_status: str | None
    title: str | None
    destination_path: Path | None
    status: str
    detail: str


@dataclass(frozen=True, slots=True)
class IngestBatchResult:
    items: tuple[IngestItemResult, ...]
    batch_errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ApprovedSourceLoadResult:
    approved_sources: tuple[ApprovedSource, ...]
    parse_failures: tuple[IngestItemResult, ...]
    batch_errors: tuple[str, ...]


def ingest_inputs(
    input_dir: Path = config.INPUT_DIR,
    approved_dir: Path = config.APPROVED_DIR,
    approved_sources_path: Path | None = config.APPROVED_SOURCES_PATH,
) -> IngestBatchResult:
    approved_dir.mkdir(parents=True, exist_ok=True)

    item_results: list[IngestItemResult] = []
    batch_errors: list[str] = []

    item_results.extend(_ingest_local_inbox(input_dir=input_dir, approved_dir=approved_dir))

    approved_source_load_result = _load_approved_sources(approved_sources_path)
    batch_errors.extend(approved_source_load_result.batch_errors)
    item_results.extend(approved_source_load_result.parse_failures)

    for approved_source in approved_source_load_result.approved_sources:
        item_results.append(
            _ingest_approved_source(
                approved_source=approved_source,
                approved_dir=approved_dir,
            )
        )

    _log_ingest_summary(item_results=item_results, batch_errors=batch_errors)
    return IngestBatchResult(
        items=tuple(item_results),
        batch_errors=tuple(batch_errors),
    )


def _ingest_local_inbox(input_dir: Path, approved_dir: Path) -> list[IngestItemResult]:
    if not input_dir.exists():
        LOGGER.info("Inbox directory does not exist: %s", input_dir)
        return []

    local_results: list[IngestItemResult] = []
    for source_path in sorted(
        path for path in input_dir.iterdir() if path.is_file() and not _is_hidden_file(path)
    ):
        local_results.append(
            _ingest_local_file(
                source_path=source_path,
                approved_dir=approved_dir,
                source_type=LOCAL_FILE_SOURCE_TYPE,
                rights_status="owned",
                title=source_path.stem,
            )
        )

    return local_results


def _load_approved_sources(
    approved_sources_path: Path | None,
) -> ApprovedSourceLoadResult:
    if approved_sources_path is None:
        return ApprovedSourceLoadResult(approved_sources=(), parse_failures=(), batch_errors=())

    if not approved_sources_path.exists():
        LOGGER.info("Approved source manifest not found: %s", approved_sources_path)
        return ApprovedSourceLoadResult(approved_sources=(), parse_failures=(), batch_errors=())

    try:
        source_payload = json.loads(approved_sources_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        error_message = f"Malformed approved source JSON: {approved_sources_path}"
        LOGGER.error("%s: %s", error_message, exc)
        return ApprovedSourceLoadResult(
            approved_sources=(),
            parse_failures=(),
            batch_errors=(f"{error_message}: {exc}",),
        )
    except OSError as exc:
        error_message = f"Failed to read approved source JSON: {approved_sources_path}"
        LOGGER.exception(error_message)
        return ApprovedSourceLoadResult(
            approved_sources=(),
            parse_failures=(),
            batch_errors=(f"{error_message}: {exc}",),
        )

    if not isinstance(source_payload, list):
        error_message = f"Approved source JSON must contain a list: {approved_sources_path}"
        LOGGER.error(error_message)
        return ApprovedSourceLoadResult(
            approved_sources=(),
            parse_failures=(),
            batch_errors=(error_message,),
        )

    approved_sources: list[ApprovedSource] = []
    parse_failures: list[IngestItemResult] = []

    for item in source_payload:
        parsed_source, parse_failure = _parse_approved_source_item(item)
        if parsed_source is not None:
            approved_sources.append(parsed_source)
            continue

        if parse_failure is not None:
            parse_failures.append(parse_failure)

    return ApprovedSourceLoadResult(
        approved_sources=tuple(approved_sources),
        parse_failures=tuple(parse_failures),
        batch_errors=(),
    )


def _parse_approved_source_item(item: Any) -> tuple[ApprovedSource | None, IngestItemResult | None]:
    if not isinstance(item, dict):
        detail = "Approved source item must be an object."
        LOGGER.error(detail)
        return None, _failed_source_result(
            source_type="unknown",
            source_path_or_url="unknown",
            rights_status=None,
            title=None,
            detail=detail,
        )

    missing_fields = [field for field in REQUIRED_APPROVED_SOURCE_FIELDS if field not in item]
    if missing_fields:
        detail = f"Approved source item missing required fields: {', '.join(missing_fields)}"
        LOGGER.error(detail)
        return None, _failed_source_result(
            source_type=str(item.get("source_type", "unknown")),
            source_path_or_url=str(item.get("source_path_or_url", "unknown")),
            rights_status=_optional_str(item.get("rights_status")),
            title=_optional_str(item.get("title")),
            detail=detail,
        )

    if not all(isinstance(item[field], str) for field in REQUIRED_APPROVED_SOURCE_FIELDS):
        detail = "Approved source item fields must all be strings."
        LOGGER.error(detail)
        return None, _failed_source_result(
            source_type=str(item["source_type"]),
            source_path_or_url=str(item["source_path_or_url"]),
            rights_status=_optional_str(item["rights_status"]),
            title=_optional_str(item["title"]),
            detail=detail,
        )

    return ApprovedSource(
        source_type=item["source_type"],
        source_path_or_url=item["source_path_or_url"],
        rights_status=item["rights_status"],
        title=item["title"],
    ), None


def _ingest_approved_source(approved_source: ApprovedSource, approved_dir: Path) -> IngestItemResult:
    if approved_source.source_type == LOCAL_FILE_SOURCE_TYPE:
        return _ingest_local_file(
            source_path=Path(approved_source.source_path_or_url),
            approved_dir=approved_dir,
            source_type=approved_source.source_type,
            rights_status=approved_source.rights_status,
            title=approved_source.title,
        )

    if approved_source.source_type == REMOTE_URL_SOURCE_TYPE:
        return _download_remote_source(approved_source=approved_source, approved_dir=approved_dir)

    detail = f"Unsupported source_type: {approved_source.source_type}"
    LOGGER.error(detail)
    return _failed_source_result(
        source_type=approved_source.source_type,
        source_path_or_url=approved_source.source_path_or_url,
        rights_status=approved_source.rights_status,
        title=approved_source.title,
        detail=detail,
    )


def _ingest_local_file(
    source_path: Path,
    approved_dir: Path,
    source_type: str,
    rights_status: str | None,
    title: str | None,
) -> IngestItemResult:
    if not _is_supported_extension(source_path):
        detail = f"Unsupported extension: {source_path.suffix.lower()}"
        LOGGER.info("Skipping unsupported file %s", source_path)
        return IngestItemResult(
            source_type=source_type,
            source_path_or_url=str(source_path),
            rights_status=rights_status,
            title=title,
            destination_path=None,
            status=SKIPPED_STATUS,
            detail=detail,
        )

    if not source_path.exists():
        detail = f"Source file does not exist: {source_path}"
        LOGGER.error(detail)
        return _failed_source_result(
            source_type=source_type,
            source_path_or_url=str(source_path),
            rights_status=rights_status,
            title=title,
            detail=detail,
        )

    destination_path = approved_dir / source_path.name

    if destination_path.exists():
        if _files_match(source_path, destination_path):
            detail = f"Duplicate copy skipped; identical content already exists at {destination_path}"
            LOGGER.info(detail)
            return IngestItemResult(
                source_type=source_type,
                source_path_or_url=str(source_path),
                rights_status=rights_status,
                title=title,
                destination_path=destination_path,
                status=SKIPPED_STATUS,
                detail=detail,
            )

        detail = f"Destination exists with different content: {destination_path}"
        LOGGER.error(detail)
        return _failed_source_result(
            source_type=source_type,
            source_path_or_url=str(source_path),
            rights_status=rights_status,
            title=title,
            detail=detail,
        )

    try:
        shutil.copy2(source_path, destination_path)
    except OSError as exc:
        detail = f"Failed to copy source into approved directory: {exc}"
        LOGGER.exception(detail)
        return _failed_source_result(
            source_type=source_type,
            source_path_or_url=str(source_path),
            rights_status=rights_status,
            title=title,
            detail=detail,
        )

    detail = f"Copied source into approved directory: {destination_path}"
    LOGGER.info(detail)
    return IngestItemResult(
        source_type=source_type,
        source_path_or_url=str(source_path),
        rights_status=rights_status,
        title=title,
        destination_path=destination_path,
        status=INGESTED_STATUS,
        detail=detail,
    )


def _download_remote_source(approved_source: ApprovedSource, approved_dir: Path) -> IngestItemResult:
    del approved_dir
    detail = "Remote downloads are not implemented."
    LOGGER.error(
        "Remote source ingestion is not implemented for source_path_or_url=%s",
        approved_source.source_path_or_url,
    )
    return _failed_source_result(
        source_type=approved_source.source_type,
        source_path_or_url=approved_source.source_path_or_url,
        rights_status=approved_source.rights_status,
        title=approved_source.title,
        detail=detail,
    )


def _files_match(first_path: Path, second_path: Path) -> bool:
    return compute_sha256(first_path) == compute_sha256(second_path)


def _is_supported_extension(source_path: Path) -> bool:
    return source_path.suffix.lower() in config.SUPPORTED_EXTENSIONS


def _is_hidden_file(source_path: Path) -> bool:
    return source_path.name.startswith(".")


def _failed_source_result(
    source_type: str,
    source_path_or_url: str,
    rights_status: str | None,
    title: str | None,
    detail: str,
) -> IngestItemResult:
    return IngestItemResult(
        source_type=source_type,
        source_path_or_url=source_path_or_url,
        rights_status=rights_status,
        title=title,
        destination_path=None,
        status=FAILED_STATUS,
        detail=detail,
    )


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _log_ingest_summary(
    item_results: list[IngestItemResult],
    batch_errors: list[str],
) -> None:
    ingested_count = sum(result.status == INGESTED_STATUS for result in item_results)
    skipped_count = sum(result.status == SKIPPED_STATUS for result in item_results)
    failed_count = sum(result.status == FAILED_STATUS for result in item_results)
    LOGGER.info(
        "Ingest summary: ingested=%s skipped=%s failed=%s batch_errors=%s",
        ingested_count,
        skipped_count,
        failed_count,
        len(batch_errors),
    )
