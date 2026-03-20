from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.ingest import FAILED_STATUS as INGEST_FAILED_STATUS
from src.ingest import INGESTED_STATUS, SKIPPED_STATUS, IngestBatchResult, IngestItemResult
from src.manifest import ASSETS_TABLE_NAME, ManifestStore
from src.orchestrator import run_pipeline
from src.processor import FAILED_STATUS as PROCESSING_FAILED_STATUS
from src.processor import PROCESSED_STATUS, ProcessorBatchResult, ProcessorResult
from src.utils import compute_sha256
from src.validator import (
    REJECTED_STATUS,
    VALID_STATUS,
    ValidationBatchResult,
    ValidationResult,
)


class OrchestratorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.base_dir = Path(self.temp_dir.name)

    def test_full_happy_path_with_mocked_downstream_modules(self) -> None:
        ingest_batch_result = IngestBatchResult(
            items=(
                IngestItemResult(
                    source_type="local_file",
                    source_path_or_url="/tmp/source.mp4",
                    rights_status="owned",
                    title="Example",
                    destination_path=Path("/tmp/approved/source.mp4"),
                    status=INGESTED_STATUS,
                    detail="copied",
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=Path("/tmp/approved/source.mp4"),
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=12.5,
                    rejected_path=None,
                ),
            )
        )
        processor_batch_result = ProcessorBatchResult(
            results=(
                ProcessorResult(
                    asset_id="asset-001",
                    source_path=Path("/tmp/approved/source.mp4"),
                    status=PROCESSED_STATUS,
                    output_files=(Path("/tmp/processed/asset-001/clip_001.mp4"),),
                    thumbnail_path=Path("/tmp/processed/asset-001/thumb.jpg"),
                    error_message=None,
                ),
            )
        )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result) as mock_ingest,
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result) as mock_validate,
            patch("src.orchestrator.process_assets", return_value=processor_batch_result) as mock_process,
            patch("src.orchestrator.compute_sha256", return_value="asset-001"),
        ):
            result = run_pipeline()

        self.assertIs(result.ingest_batch_result, ingest_batch_result)
        self.assertIs(result.validation_batch_result, validation_batch_result)
        self.assertIs(result.processor_batch_result, processor_batch_result)
        mock_ingest.assert_called_once()
        mock_validate.assert_called_once()
        mock_process.assert_called_once()

    def test_ingest_failures_do_not_crash_run(self) -> None:
        ingest_batch_result = IngestBatchResult(
            items=(
                IngestItemResult(
                    source_type="local_file",
                    source_path_or_url="/tmp/missing.mp4",
                    rights_status="owned",
                    title="Missing",
                    destination_path=None,
                    status=INGEST_FAILED_STATUS,
                    detail="copy failed",
                ),
                IngestItemResult(
                    source_type="local_file",
                    source_path_or_url="/tmp/notes.txt",
                    rights_status="owned",
                    title="Notes",
                    destination_path=None,
                    status=SKIPPED_STATUS,
                    detail="unsupported extension",
                ),
            ),
            batch_errors=(),
        )
        empty_validation_batch_result = ValidationBatchResult(results=())

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch(
                "src.orchestrator.validate_assets",
                return_value=empty_validation_batch_result,
            ) as mock_validate,
            patch("src.orchestrator.process_assets") as mock_process,
        ):
            result = run_pipeline()

        validation_assets = mock_validate.call_args.kwargs["assets"]
        self.assertEqual(validation_assets, ())
        mock_process.assert_not_called()
        self.assertEqual(result.processor_batch_result.results, ())
        self.assertEqual(result.summary.failed_ingest, 1)
        self.assertEqual(result.summary.skipped_ingest, 1)

    def test_rejected_validation_items_do_not_reach_processor(self) -> None:
        ingest_batch_result = IngestBatchResult(
            items=(
                self._ingested_item(
                    source_path="/tmp/source.mp4",
                    destination_path="/tmp/approved/source.mp4",
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=Path("/tmp/approved/source.mp4"),
                    status=REJECTED_STATUS,
                    reason="Invalid rights_status",
                    duration=None,
                    rejected_path=Path("/tmp/rejected/source.mp4"),
                ),
            )
        )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets") as mock_process,
        ):
            result = run_pipeline()

        mock_process.assert_not_called()
        self.assertEqual(result.processor_batch_result.results, ())
        self.assertEqual(result.summary.rejected, 1)

    def test_processor_receives_only_validated_assets(self) -> None:
        ingest_batch_result = IngestBatchResult(
            items=(
                self._ingested_item(
                    source_path="/tmp/first.mp4",
                    destination_path="/tmp/approved/first.mp4",
                    title="First",
                ),
                self._ingested_item(
                    source_path="/tmp/second.mp4",
                    destination_path="/tmp/approved/second.mp4",
                    title="Second",
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=Path("/tmp/approved/first.mp4"),
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=10.0,
                    rejected_path=None,
                ),
                ValidationResult(
                    local_path=Path("/tmp/approved/second.mp4"),
                    status=REJECTED_STATUS,
                    reason="Probe failed",
                    duration=None,
                    rejected_path=Path("/tmp/rejected/second.mp4"),
                ),
            )
        )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch(
                "src.orchestrator.process_assets",
                return_value=ProcessorBatchResult(results=()),
            ) as mock_process,
            patch("src.orchestrator.compute_sha256", return_value="asset-first"),
        ):
            run_pipeline()

        processor_assets = mock_process.call_args.kwargs["assets"]
        self.assertEqual(len(processor_assets), 1)
        self.assertEqual(processor_assets[0].local_path, Path("/tmp/approved/first.mp4"))
        self.assertEqual(processor_assets[0].title, "First")
        self.assertEqual(processor_assets[0].duration, 10.0)

    def test_empty_processable_set_does_not_crash(self) -> None:
        ingest_batch_result = IngestBatchResult(items=(), batch_errors=())
        validation_batch_result = ValidationBatchResult(results=())

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets") as mock_process,
        ):
            result = run_pipeline()

        mock_process.assert_not_called()
        self.assertEqual(result.processor_batch_result.results, ())
        self.assertEqual(result.summary.processed, 0)
        self.assertEqual(result.summary.failed_processing, 0)

    def test_summary_counts_are_correct(self) -> None:
        ingest_batch_result = IngestBatchResult(
            items=(
                self._ingested_item(
                    source_path="/tmp/one.mp4",
                    destination_path="/tmp/approved/one.mp4",
                ),
                self._ingested_item(
                    source_path="/tmp/two.mp4",
                    destination_path="/tmp/approved/two.mp4",
                ),
                self._ingested_item(
                    source_path="/tmp/three.mp4",
                    destination_path="/tmp/approved/three.mp4",
                ),
                IngestItemResult(
                    source_type="local_file",
                    source_path_or_url="/tmp/skip.txt",
                    rights_status="owned",
                    title="Skip",
                    destination_path=None,
                    status=SKIPPED_STATUS,
                    detail="unsupported extension",
                ),
                IngestItemResult(
                    source_type="local_file",
                    source_path_or_url="/tmp/fail.mp4",
                    rights_status="owned",
                    title="Fail",
                    destination_path=None,
                    status=INGEST_FAILED_STATUS,
                    detail="copy failed",
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=Path("/tmp/approved/one.mp4"),
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=9.0,
                    rejected_path=None,
                ),
                ValidationResult(
                    local_path=Path("/tmp/approved/two.mp4"),
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=11.0,
                    rejected_path=None,
                ),
                ValidationResult(
                    local_path=Path("/tmp/approved/three.mp4"),
                    status=REJECTED_STATUS,
                    reason="Invalid title",
                    duration=None,
                    rejected_path=Path("/tmp/rejected/three.mp4"),
                ),
            )
        )
        processor_batch_result = ProcessorBatchResult(
            results=(
                ProcessorResult(
                    asset_id="asset-one",
                    source_path=Path("/tmp/approved/one.mp4"),
                    status=PROCESSED_STATUS,
                    output_files=(Path("/tmp/processed/asset-one/clip_001.mp4"),),
                    thumbnail_path=Path("/tmp/processed/asset-one/thumb.jpg"),
                    error_message=None,
                ),
                ProcessorResult(
                    asset_id="asset-two",
                    source_path=Path("/tmp/approved/two.mp4"),
                    status=PROCESSING_FAILED_STATUS,
                    output_files=(),
                    thumbnail_path=None,
                    error_message="ffmpeg failed",
                ),
            )
        )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets", return_value=processor_batch_result),
            patch("src.orchestrator.compute_sha256", side_effect=["asset-one", "asset-two"]),
        ):
            result = run_pipeline()

        self.assertEqual(result.summary.ingested, 3)
        self.assertEqual(result.summary.skipped_ingest, 1)
        self.assertEqual(result.summary.failed_ingest, 1)
        self.assertEqual(result.summary.valid, 2)
        self.assertEqual(result.summary.rejected, 1)
        self.assertEqual(result.summary.processed, 1)
        self.assertEqual(result.summary.failed_processing, 1)

    def test_successful_processed_asset_is_written_to_manifest(self) -> None:
        approved_path = self.base_dir / "approved" / "source.mp4"
        approved_path.parent.mkdir(parents=True, exist_ok=True)
        approved_path.write_bytes(b"video-bytes")
        manifest_store = self._initialized_manifest_store()

        ingest_batch_result = IngestBatchResult(
            items=(
                self._ingested_item(
                    source_path="/tmp/source.mp4",
                    destination_path=str(approved_path),
                    title="Example",
                    rights_status="owned",
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=approved_path,
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=12.5,
                    rejected_path=None,
                ),
            )
        )

        def process_side_effect(*, assets, processed_dir):
            del processed_dir
            return ProcessorBatchResult(
                results=(
                    ProcessorResult(
                        asset_id=assets[0].asset_id,
                        source_path=assets[0].local_path,
                        status=PROCESSED_STATUS,
                        output_files=(self.base_dir / "processed" / assets[0].asset_id / "clip_001.mp4",),
                        thumbnail_path=self.base_dir / "processed" / assets[0].asset_id / "thumb.jpg",
                        error_message=None,
                    ),
                )
            )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets", side_effect=process_side_effect),
        ):
            result = run_pipeline(manifest_store=manifest_store)

        stored_asset = manifest_store.get_asset(result.processor_batch_result.results[0].asset_id)
        self.assertIsNotNone(stored_asset)
        self.assertEqual(stored_asset.asset_id, result.processor_batch_result.results[0].asset_id)
        self.assertEqual(stored_asset.status, PROCESSED_STATUS)
        self.assertEqual(
            stored_asset.output_files,
            (str(self.base_dir / "processed" / stored_asset.asset_id / "clip_001.mp4"),),
        )
        self.assertIsNone(stored_asset.error_message)

    def test_failed_processed_asset_is_written_to_manifest(self) -> None:
        approved_path = self.base_dir / "approved" / "source.mp4"
        approved_path.parent.mkdir(parents=True, exist_ok=True)
        approved_path.write_bytes(b"video-bytes")
        manifest_store = self._initialized_manifest_store()

        ingest_batch_result = IngestBatchResult(
            items=(
                self._ingested_item(
                    source_path="/tmp/source.mp4",
                    destination_path=str(approved_path),
                    title="Example",
                    rights_status="owned",
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=approved_path,
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=12.5,
                    rejected_path=None,
                ),
            )
        )

        def process_side_effect(*, assets, processed_dir):
            del processed_dir
            return ProcessorBatchResult(
                results=(
                    ProcessorResult(
                        asset_id=assets[0].asset_id,
                        source_path=assets[0].local_path,
                        status=PROCESSING_FAILED_STATUS,
                        output_files=(),
                        thumbnail_path=None,
                        error_message="ffmpeg failed",
                    ),
                )
            )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets", side_effect=process_side_effect),
        ):
            result = run_pipeline(manifest_store=manifest_store)

        stored_asset = manifest_store.get_asset(result.processor_batch_result.results[0].asset_id)
        self.assertIsNotNone(stored_asset)
        self.assertEqual(stored_asset.asset_id, result.processor_batch_result.results[0].asset_id)
        self.assertEqual(stored_asset.status, PROCESSING_FAILED_STATUS)
        self.assertEqual(stored_asset.output_files, ())
        self.assertEqual(stored_asset.error_message, "ffmpeg failed")

    def test_manifest_receives_correct_asset_fields(self) -> None:
        approved_path = self.base_dir / "approved" / "source.mp4"
        approved_path.parent.mkdir(parents=True, exist_ok=True)
        approved_path.write_bytes(b"video-bytes")
        manifest_store = self._initialized_manifest_store()

        ingest_batch_result = IngestBatchResult(
            items=(
                self._ingested_item(
                    source_path="/tmp/source-original.mp4",
                    destination_path=str(approved_path),
                    title="Example",
                    rights_status="licensed",
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=approved_path,
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=8.0,
                    rejected_path=None,
                ),
            )
        )

        def process_side_effect(*, assets, processed_dir):
            del processed_dir
            return ProcessorBatchResult(
                results=(
                    ProcessorResult(
                        asset_id=assets[0].asset_id,
                        source_path=assets[0].local_path,
                        status=PROCESSED_STATUS,
                        output_files=(self.base_dir / "processed" / assets[0].asset_id / "clip_001.mp4",),
                        thumbnail_path=self.base_dir / "processed" / assets[0].asset_id / "thumb.jpg",
                        error_message=None,
                    ),
                )
            )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets", side_effect=process_side_effect),
        ):
            result = run_pipeline(manifest_store=manifest_store)

        stored_asset = manifest_store.get_asset(result.processor_batch_result.results[0].asset_id)
        self.assertIsNotNone(stored_asset)
        self.assertEqual(stored_asset.input_path, approved_path)
        self.assertEqual(stored_asset.source_path_or_url, "/tmp/source-original.mp4")
        self.assertEqual(stored_asset.rights_status, "licensed")

    def test_empty_processor_result_does_not_crash_manifest_integration(self) -> None:
        manifest_store = self._initialized_manifest_store()
        ingest_batch_result = IngestBatchResult(items=(), batch_errors=())
        validation_batch_result = ValidationBatchResult(results=())

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets") as mock_process,
        ):
            result = run_pipeline(manifest_store=manifest_store)

        mock_process.assert_not_called()
        self.assertEqual(result.processor_batch_result.results, ())
        with sqlite3.connect(manifest_store.manifest_path) as connection:
            row = connection.execute(
                f"SELECT COUNT(*) FROM {ASSETS_TABLE_NAME}"
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], 0)

    def test_asset_identity_is_stable_across_rerun_of_same_asset(self) -> None:
        approved_path = self.base_dir / "approved" / "stable.mp4"
        approved_path.parent.mkdir(parents=True, exist_ok=True)
        approved_path.write_bytes(b"stable-video-bytes")
        manifest_store = self._initialized_manifest_store()
        ingest_batch_result = IngestBatchResult(
            items=(
                self._ingested_item(
                    source_path="/tmp/stable-source.mp4",
                    destination_path=str(approved_path),
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=approved_path,
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=6.0,
                    rejected_path=None,
                ),
            )
        )
        seen_asset_ids: list[str] = []

        def process_side_effect(*, assets, processed_dir):
            del processed_dir
            seen_asset_ids.append(assets[0].asset_id)
            return ProcessorBatchResult(
                results=(
                    ProcessorResult(
                        asset_id=assets[0].asset_id,
                        source_path=assets[0].local_path,
                        status=PROCESSED_STATUS,
                        output_files=(self.base_dir / "processed" / assets[0].asset_id / "clip_001.mp4",),
                        thumbnail_path=self.base_dir / "processed" / assets[0].asset_id / "thumb.jpg",
                        error_message=None,
                    ),
                )
            )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets", side_effect=process_side_effect),
        ):
            run_pipeline(manifest_store=manifest_store)
            run_pipeline(manifest_store=manifest_store)

        expected_asset_id = compute_sha256(approved_path)
        self.assertEqual(seen_asset_ids, [expected_asset_id, expected_asset_id])
        self.assertIsNotNone(manifest_store.get_asset(expected_asset_id))

    def test_rerun_of_same_input_keeps_manifest_state_consistent(self) -> None:
        approved_path = self.base_dir / "approved" / "rerun.mp4"
        approved_path.parent.mkdir(parents=True, exist_ok=True)
        approved_path.write_bytes(b"rerun-video-bytes")
        manifest_store = self._initialized_manifest_store()
        ingest_batch_result = IngestBatchResult(
            items=(
                self._ingested_item(
                    source_path="/tmp/rerun-source.mp4",
                    destination_path=str(approved_path),
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=approved_path,
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=7.0,
                    rejected_path=None,
                ),
            )
        )
        call_count = 0

        def process_side_effect(*, assets, processed_dir):
            nonlocal call_count
            call_count += 1
            del processed_dir
            if call_count == 1:
                return ProcessorBatchResult(
                    results=(
                        ProcessorResult(
                            asset_id=assets[0].asset_id,
                            source_path=assets[0].local_path,
                            status=PROCESSING_FAILED_STATUS,
                            output_files=(),
                            thumbnail_path=None,
                            error_message="ffmpeg failed",
                        ),
                    )
                )

            return ProcessorBatchResult(
                results=(
                    ProcessorResult(
                        asset_id=assets[0].asset_id,
                        source_path=assets[0].local_path,
                        status=PROCESSED_STATUS,
                        output_files=(self.base_dir / "processed" / assets[0].asset_id / "clip_001.mp4",),
                        thumbnail_path=self.base_dir / "processed" / assets[0].asset_id / "thumb.jpg",
                        error_message=None,
                    ),
                )
            )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets", side_effect=process_side_effect),
        ):
            run_pipeline(manifest_store=manifest_store)
            result = run_pipeline(manifest_store=manifest_store)

        asset_id = result.processor_batch_result.results[0].asset_id
        stored_asset = manifest_store.get_asset(asset_id)
        self.assertIsNotNone(stored_asset)
        self.assertEqual(stored_asset.status, PROCESSED_STATUS)
        self.assertEqual(
            stored_asset.output_files,
            (str(self.base_dir / "processed" / asset_id / "clip_001.mp4"),),
        )
        self.assertIsNone(stored_asset.error_message)
        self.assertEqual(self._manifest_row_count(manifest_store), 1)

    def test_manifest_updates_do_not_create_duplicate_records_for_same_asset(self) -> None:
        approved_path = self.base_dir / "approved" / "duplicate-check.mp4"
        approved_path.parent.mkdir(parents=True, exist_ok=True)
        approved_path.write_bytes(b"duplicate-check-video")
        manifest_store = self._initialized_manifest_store()
        ingest_batch_result = IngestBatchResult(
            items=(
                self._ingested_item(
                    source_path="/tmp/duplicate-check-source.mp4",
                    destination_path=str(approved_path),
                ),
            ),
            batch_errors=(),
        )
        validation_batch_result = ValidationBatchResult(
            results=(
                ValidationResult(
                    local_path=approved_path,
                    status=VALID_STATUS,
                    reason="Validation passed.",
                    duration=4.0,
                    rejected_path=None,
                ),
            )
        )

        def process_side_effect(*, assets, processed_dir):
            del processed_dir
            return ProcessorBatchResult(
                results=(
                    ProcessorResult(
                        asset_id=assets[0].asset_id,
                        source_path=assets[0].local_path,
                        status=PROCESSED_STATUS,
                        output_files=(self.base_dir / "processed" / assets[0].asset_id / "clip_001.mp4",),
                        thumbnail_path=self.base_dir / "processed" / assets[0].asset_id / "thumb.jpg",
                        error_message=None,
                    ),
                )
            )

        with (
            patch("src.orchestrator.ingest_inputs", return_value=ingest_batch_result),
            patch("src.orchestrator.validate_assets", return_value=validation_batch_result),
            patch("src.orchestrator.process_assets", side_effect=process_side_effect),
        ):
            run_pipeline(manifest_store=manifest_store)
            run_pipeline(manifest_store=manifest_store)

        self.assertEqual(self._manifest_row_count(manifest_store), 1)

    def _ingested_item(
        self,
        source_path: str,
        destination_path: str,
        title: str = "Example",
        rights_status: str | None = "owned",
    ) -> IngestItemResult:
        return IngestItemResult(
            source_type="local_file",
            source_path_or_url=source_path,
            rights_status=rights_status,
            title=title,
            destination_path=Path(destination_path),
            status=INGESTED_STATUS,
            detail="copied",
        )

    def _initialized_manifest_store(self) -> ManifestStore:
        manifest_store = ManifestStore(self.base_dir / "manifest.sqlite3")
        manifest_store.initialize()
        return manifest_store

    def _manifest_row_count(self, manifest_store: ManifestStore) -> int:
        with sqlite3.connect(manifest_store.manifest_path) as connection:
            row = connection.execute(f"SELECT COUNT(*) FROM {ASSETS_TABLE_NAME}").fetchone()

        self.assertIsNotNone(row)
        return int(row[0])


if __name__ == "__main__":
    unittest.main()
