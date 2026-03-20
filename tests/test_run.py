from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import run
from src.ingest import IngestBatchResult
from src.orchestrator import OrchestrationResult, OrchestrationSummary
from src.processor import ProcessorBatchResult
from src.validator import ValidationBatchResult


class RunEntrypointTests(unittest.TestCase):
    def test_run_initializes_manifest_store_exactly_once(self) -> None:
        manifest_store = Mock()

        with (
            patch("run.ensure_directories_exist"),
            patch("run.configure_logging"),
            patch("run.ManifestStore", return_value=manifest_store) as mock_manifest_store,
            patch("run.run_pipeline", return_value=self._result(failed_processing=0)),
            patch("builtins.print"),
        ):
            exit_code = run.main()

        self.assertEqual(exit_code, 0)
        mock_manifest_store.assert_called_once_with(run.config.MANIFEST_PATH)
        manifest_store.initialize.assert_called_once_with()

    def test_run_calls_orchestrator(self) -> None:
        manifest_store = Mock()

        with (
            patch("run.ensure_directories_exist"),
            patch("run.configure_logging"),
            patch("run.ManifestStore", return_value=manifest_store),
            patch("run.run_pipeline", return_value=self._result(failed_processing=0)) as mock_run_pipeline,
            patch("builtins.print"),
        ):
            run.main()

        mock_run_pipeline.assert_called_once_with(
            input_dir=run.config.INPUT_DIR,
            approved_dir=run.config.APPROVED_DIR,
            approved_sources_path=run.config.APPROVED_SOURCES_PATH,
            rejected_dir=run.config.REJECTED_DIR,
            processed_dir=run.config.PROCESSED_DIR,
            manifest_store=manifest_store,
        )

    def test_exit_code_zero_on_no_failed_processing(self) -> None:
        with (
            patch("run.ensure_directories_exist"),
            patch("run.configure_logging"),
            patch("run.ManifestStore", return_value=Mock()),
            patch("run.run_pipeline", return_value=self._result(failed_processing=0)),
            patch("builtins.print"),
        ):
            exit_code = run.main()

        self.assertEqual(exit_code, 0)

    def test_exit_code_one_when_failed_processing_exists(self) -> None:
        with (
            patch("run.ensure_directories_exist"),
            patch("run.configure_logging"),
            patch("run.ManifestStore", return_value=Mock()),
            patch("run.run_pipeline", return_value=self._result(failed_processing=1)),
            patch("builtins.print"),
        ):
            exit_code = run.main()

        self.assertEqual(exit_code, 1)

    def _result(self, failed_processing: int) -> OrchestrationResult:
        return OrchestrationResult(
            ingest_batch_result=IngestBatchResult(items=(), batch_errors=()),
            validation_batch_result=ValidationBatchResult(results=()),
            processor_batch_result=ProcessorBatchResult(results=()),
            summary=OrchestrationSummary(
                ingested=0,
                skipped_ingest=0,
                failed_ingest=0,
                valid=0,
                rejected=0,
                processed=0,
                failed_processing=failed_processing,
            ),
        )


if __name__ == "__main__":
    unittest.main()
