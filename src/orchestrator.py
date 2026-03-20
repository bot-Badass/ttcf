from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src import config
from src.ingest import (
    FAILED_STATUS as INGEST_FAILED_STATUS,
    INGESTED_STATUS,
    SKIPPED_STATUS,
    IngestBatchResult,
    ingest_inputs,
)
from src.processor import (
    FAILED_STATUS as PROCESSING_FAILED_STATUS,
    PROCESSED_STATUS,
    ProcessorAsset,
    ProcessorBatchResult,
    ProcessorResult,
    process_assets,
)
from src.manifest import AssetRecord, ManifestStore
from src.utils import compute_sha256
from src.validator import (
    REJECTED_STATUS,
    VALID_STATUS,
    ValidationAsset,
    ValidationBatchResult,
    validate_assets,
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class OrchestrationSummary:
    ingested: int
    skipped_ingest: int
    failed_ingest: int
    valid: int
    rejected: int
    processed: int
    failed_processing: int


@dataclass(frozen=True, slots=True)
class OrchestrationResult:
    ingest_batch_result: IngestBatchResult
    validation_batch_result: ValidationBatchResult
    processor_batch_result: ProcessorBatchResult
    summary: OrchestrationSummary


def run_pipeline(
    input_dir: Path = config.INPUT_DIR,
    approved_dir: Path = config.APPROVED_DIR,
    approved_sources_path: Path | None = config.APPROVED_SOURCES_PATH,
    rejected_dir: Path = config.REJECTED_DIR,
    processed_dir: Path = config.PROCESSED_DIR,
    manifest_store: ManifestStore | None = None,
) -> OrchestrationResult:
    ingest_batch_result = ingest_inputs(
        input_dir=input_dir,
        approved_dir=approved_dir,
        approved_sources_path=approved_sources_path,
    )

    validation_assets = _build_validation_assets(ingest_batch_result)
    validation_batch_result = validate_assets(
        assets=validation_assets,
        rejected_dir=rejected_dir,
    )

    processor_assets = _build_processor_assets(
        validation_assets=validation_assets,
        validation_batch_result=validation_batch_result,
    )
    if processor_assets:
        processor_batch_result = process_assets(
            assets=processor_assets,
            processed_dir=processed_dir,
        )
    else:
        processor_batch_result = ProcessorBatchResult(results=())

    if manifest_store is not None:
        _persist_processing_outcomes(
            manifest_store=manifest_store,
            validation_assets=validation_assets,
            processor_assets=processor_assets,
            processor_batch_result=processor_batch_result,
        )

    summary = _build_summary(
        ingest_batch_result=ingest_batch_result,
        validation_batch_result=validation_batch_result,
        processor_batch_result=processor_batch_result,
    )
    LOGGER.info("Pipeline orchestration summary: %s", summary)
    return OrchestrationResult(
        ingest_batch_result=ingest_batch_result,
        validation_batch_result=validation_batch_result,
        processor_batch_result=processor_batch_result,
        summary=summary,
    )


def _build_validation_assets(
    ingest_batch_result: IngestBatchResult,
) -> tuple[ValidationAsset, ...]:
    validation_assets: list[ValidationAsset] = []

    for ingest_item in ingest_batch_result.items:
        if ingest_item.status != INGESTED_STATUS or ingest_item.destination_path is None:
            continue

        validation_assets.append(
            ValidationAsset(
                source_type=ingest_item.source_type,
                source_path_or_url=ingest_item.source_path_or_url,
                rights_status=ingest_item.rights_status,
                title=ingest_item.title,
                local_path=ingest_item.destination_path,
            )
        )

    return tuple(validation_assets)


def _build_processor_assets(
    validation_assets: tuple[ValidationAsset, ...],
    validation_batch_result: ValidationBatchResult,
) -> tuple[ProcessorAsset, ...]:
    validation_assets_by_path = {
        validation_asset.local_path: validation_asset for validation_asset in validation_assets
    }
    processor_assets: list[ProcessorAsset] = []

    for validation_result in validation_batch_result.results:
        if validation_result.status != VALID_STATUS:
            continue

        validation_asset = validation_assets_by_path.get(validation_result.local_path)
        if validation_asset is None:
            LOGGER.error(
                "Validation result could not be mapped to a validation asset: %s",
                validation_result.local_path,
            )
            continue

        if validation_result.duration is None:
            LOGGER.error(
                "Validated asset is missing duration and cannot be processed: %s",
                validation_result.local_path,
            )
            continue

        processor_assets.append(
            ProcessorAsset(
                asset_id=_build_asset_id(validation_result.local_path),
                local_path=validation_result.local_path,
                title=validation_asset.title or validation_result.local_path.stem,
                duration=validation_result.duration,
            )
        )

    return tuple(processor_assets)


def _build_asset_id(local_path: Path) -> str:
    # Asset identity is content-based so the same bytes map to one logical record
    # across reruns and match the manifest's sha256 duplicate tracking.
    return compute_sha256(local_path)


def _build_summary(
    ingest_batch_result: IngestBatchResult,
    validation_batch_result: ValidationBatchResult,
    processor_batch_result: ProcessorBatchResult,
) -> OrchestrationSummary:
    return OrchestrationSummary(
        ingested=sum(item.status == INGESTED_STATUS for item in ingest_batch_result.items),
        skipped_ingest=sum(item.status == SKIPPED_STATUS for item in ingest_batch_result.items),
        failed_ingest=sum(item.status == INGEST_FAILED_STATUS for item in ingest_batch_result.items),
        valid=sum(result.status == VALID_STATUS for result in validation_batch_result.results),
        rejected=sum(result.status == REJECTED_STATUS for result in validation_batch_result.results),
        processed=sum(result.status == PROCESSED_STATUS for result in processor_batch_result.results),
        failed_processing=sum(
            result.status == PROCESSING_FAILED_STATUS
            for result in processor_batch_result.results
        ),
    )


def _persist_processing_outcomes(
    manifest_store: ManifestStore,
    validation_assets: tuple[ValidationAsset, ...],
    processor_assets: tuple[ProcessorAsset, ...],
    processor_batch_result: ProcessorBatchResult,
) -> None:
    validation_assets_by_path = {
        validation_asset.local_path: validation_asset for validation_asset in validation_assets
    }
    processor_assets_by_path = {
        processor_asset.local_path: processor_asset for processor_asset in processor_assets
    }

    for processor_result in processor_batch_result.results:
        validation_asset = validation_assets_by_path.get(processor_result.source_path)
        processor_asset = processor_assets_by_path.get(processor_result.source_path)

        if validation_asset is None or processor_asset is None:
            LOGGER.error(
                "Processor result could not be mapped for manifest persistence: asset_id=%s source_path=%s",
                processor_result.asset_id,
                processor_result.source_path,
            )
            continue

        manifest_record = _build_manifest_record(
            validation_asset=validation_asset,
            processor_asset=processor_asset,
            processor_result=processor_result,
        )
        _upsert_manifest_record(manifest_store=manifest_store, record=manifest_record)


def _build_manifest_record(
    validation_asset: ValidationAsset,
    processor_asset: ProcessorAsset,
    processor_result: ProcessorResult,
) -> AssetRecord:
    rights_status = validation_asset.rights_status
    if rights_status is None:
        raise ValueError(
            f"Cannot persist manifest record without rights_status: {validation_asset.local_path}"
        )

    return AssetRecord(
        asset_id=processor_result.asset_id,
        input_path=processor_result.source_path,
        source_type=validation_asset.source_type,
        source_path_or_url=validation_asset.source_path_or_url,
        rights_status=rights_status,
        ingest_time=_utc_timestamp_now(),
        sha256=processor_asset.asset_id,
        duration=processor_asset.duration,
        status=processor_result.status,
        output_files=tuple(str(path) for path in processor_result.output_files),
        error_message=processor_result.error_message,
    )


def _upsert_manifest_record(manifest_store: ManifestStore, record: AssetRecord) -> None:
    existing_record = manifest_store.get_asset(record.asset_id)
    if existing_record is None:
        manifest_store.insert_asset(record)
        return

    _assert_existing_record_consistency(existing_record=existing_record, record=record)
    manifest_store.update_status(record.asset_id, record.status)
    manifest_store.update_output_files(record.asset_id, record.output_files)
    manifest_store.update_error_message(record.asset_id, record.error_message)


def _utc_timestamp_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _assert_existing_record_consistency(existing_record: AssetRecord, record: AssetRecord) -> None:
    if existing_record.sha256 != record.sha256 or existing_record.asset_id != record.asset_id:
        raise ValueError(f"Manifest identity mismatch for asset_id={record.asset_id}")
