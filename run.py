from __future__ import annotations

import logging

from src import config
from src.manifest import ManifestStore
from src.orchestrator import OrchestrationSummary, run_pipeline


LOGGER = logging.getLogger(__name__)


def ensure_directories_exist() -> None:
    for directory in config.REQUIRED_DIRECTORIES:
        directory.mkdir(parents=True, exist_ok=True)


def configure_logging() -> None:
    logging.basicConfig(
        filename=config.STARTUP_LOG_PATH,
        level=logging.INFO,
        format=config.LOG_FORMAT,
        encoding="utf-8",
        force=True,
    )


def main() -> int:
    try:
        ensure_directories_exist()
        configure_logging()

        manifest_store = ManifestStore(config.MANIFEST_PATH)
        manifest_store.initialize()

        result = run_pipeline(
            input_dir=config.INPUT_DIR,
            approved_dir=config.APPROVED_DIR,
            approved_sources_path=config.APPROVED_SOURCES_PATH,
            rejected_dir=config.REJECTED_DIR,
            processed_dir=config.PROCESSED_DIR,
            manifest_store=manifest_store,
        )
        summary_message = format_summary(result.summary)
        LOGGER.info("Pipeline run complete: %s", summary_message)
        print(summary_message)
        return 1 if result.summary.failed_processing > 0 else 0
    except Exception:
        LOGGER.exception("Pipeline run failed.")
        return 1


def format_summary(summary: OrchestrationSummary) -> str:
    return (
        "Pipeline summary: "
        f"ingested={summary.ingested} "
        f"skipped_ingest={summary.skipped_ingest} "
        f"failed_ingest={summary.failed_ingest} "
        f"valid={summary.valid} "
        f"rejected={summary.rejected} "
        f"processed={summary.processed} "
        f"failed_processing={summary.failed_processing}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
