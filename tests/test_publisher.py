from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.processor import FAILED_STATUS as PROCESSOR_FAILED_STATUS
from src.processor import PROCESSED_STATUS, ProcessorResult
from src.publisher import (
    FAILED_STATUS,
    PUBLISHED_STATUS,
    PUBLISH_QUEUE_TABLE_NAME,
    QUEUED_STATUS,
    PublishQueueError,
    PublishQueueStore,
    build_dry_run_payload,
    build_publish_candidate,
)


class PublishQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.base_dir = Path(self.temp_dir.name)
        self.queue_path = self.base_dir / "publish_queue.sqlite3"
        self.output_dir = self.base_dir / "processed" / "asset-001"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.store = PublishQueueStore(self.queue_path)
        self.store.initialize()

    def test_queue_store_initialization(self) -> None:
        with sqlite3.connect(self.queue_path) as connection:
            row = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                (PUBLISH_QUEUE_TABLE_NAME,),
            ).fetchone()

        self.assertIsNotNone(row)

    def test_enqueue_one_processed_asset(self) -> None:
        processor_result = self._processed_result()
        candidate = build_publish_candidate(
            processor_result=processor_result,
            title="Example Title",
            caption=None,
            hashtags=("#demo",),
        )

        queue_result = self.store.enqueue_candidate(candidate)

        self.assertTrue(queue_result.created)
        self.assertEqual(queue_result.item.asset_id, "asset-001")
        self.assertEqual(queue_result.item.status, QUEUED_STATUS)
        self.assertEqual(queue_result.item.caption, "Example Title")
        self.assertEqual(queue_result.item.hashtags, ("#demo",))

    def test_duplicate_enqueue_does_not_create_second_logical_queue_item(self) -> None:
        candidate = build_publish_candidate(
            processor_result=self._processed_result(),
            title="Example Title",
            caption="Caption",
            hashtags=("#demo",),
        )

        first_result = self.store.enqueue_candidate(candidate)
        second_result = self.store.enqueue_candidate(candidate)

        self.assertTrue(first_result.created)
        self.assertFalse(second_result.created)
        self.assertEqual(second_result.item.asset_id, first_result.item.asset_id)
        self.assertEqual(second_result.item.clip_path, first_result.item.clip_path)
        self.assertEqual(self._queue_row_count(), 1)

    def test_list_queued_items(self) -> None:
        first_candidate = build_publish_candidate(
            processor_result=self._processed_result(asset_id="asset-001"),
            title="First",
            caption=None,
            hashtags=(),
        )
        second_candidate = build_publish_candidate(
            processor_result=self._processed_result(
                asset_id="asset-002",
                clip_name="clip_002.mp4",
                thumb_name="thumb_002.jpg",
            ),
            title="Second",
            caption="Second Caption",
            hashtags=("#second",),
        )

        self.store.enqueue_candidate(first_candidate)
        self.store.enqueue_candidate(second_candidate)

        queued_items = self.store.list_queued_items()

        self.assertEqual(len(queued_items), 2)
        self.assertEqual(tuple(item.asset_id for item in queued_items), ("asset-001", "asset-002"))

    def test_mark_item_as_published(self) -> None:
        candidate = build_publish_candidate(
            processor_result=self._processed_result(),
            title="Example Title",
            caption="Ready",
            hashtags=(),
        )
        queued_result = self.store.enqueue_candidate(candidate)

        published_item = self.store.mark_item_as_published(
            asset_id=queued_result.item.asset_id,
            clip_path=queued_result.item.clip_path,
        )

        self.assertEqual(published_item.status, PUBLISHED_STATUS)
        self.assertIsNone(published_item.error_message)

    def test_mark_item_as_failed(self) -> None:
        candidate = build_publish_candidate(
            processor_result=self._processed_result(),
            title="Example Title",
            caption="Ready",
            hashtags=(),
        )
        queued_result = self.store.enqueue_candidate(candidate)

        failed_item = self.store.mark_item_as_failed(
            asset_id=queued_result.item.asset_id,
            clip_path=queued_result.item.clip_path,
            error_message="Dry-run publish check failed.",
        )

        self.assertEqual(failed_item.status, FAILED_STATUS)
        self.assertEqual(failed_item.error_message, "Dry-run publish check failed.")

    def test_dry_run_payload_generation(self) -> None:
        candidate = build_publish_candidate(
            processor_result=self._processed_result(),
            title="Example Title",
            caption=None,
            hashtags=("#demo", "#shorts"),
        )
        queue_item = self.store.enqueue_candidate(candidate).item

        payload = build_dry_run_payload(queue_item)

        self.assertEqual(payload.local_video_path, queue_item.clip_path)
        self.assertEqual(payload.local_thumbnail_path, queue_item.thumbnail_path)
        self.assertEqual(payload.title, "Example Title")
        self.assertEqual(payload.caption, "Example Title")
        self.assertEqual(payload.hashtags, ("#demo", "#shorts"))

    def test_rerun_safe_queue_behavior(self) -> None:
        candidate = build_publish_candidate(
            processor_result=self._processed_result(),
            title="Example Title",
            caption="Ready",
            hashtags=("#demo",),
        )

        initial_result = self.store.enqueue_candidate(candidate)
        published_item = self.store.mark_item_as_published(
            asset_id=initial_result.item.asset_id,
            clip_path=initial_result.item.clip_path,
        )
        rerun_result = self.store.enqueue_candidate(candidate)

        self.assertEqual(published_item.status, PUBLISHED_STATUS)
        self.assertFalse(rerun_result.created)
        self.assertEqual(rerun_result.item.status, PUBLISHED_STATUS)
        self.assertEqual(self._queue_row_count(), 1)

    def test_enqueue_candidate_rejects_missing_clip_file(self) -> None:
        processor_result = self._processed_result()
        missing_clip_path = processor_result.output_files[0]
        missing_clip_path.unlink()
        candidate = build_publish_candidate(
            processor_result=processor_result,
            title="Example Title",
            caption="Ready",
            hashtags=(),
        )

        with self.assertRaisesRegex(PublishQueueError, "clip_path does not exist"):
            self.store.enqueue_candidate(candidate)

    def test_enqueue_candidate_rejects_missing_thumbnail_file(self) -> None:
        processor_result = self._processed_result()
        thumbnail_path = processor_result.thumbnail_path
        self.assertIsNotNone(thumbnail_path)
        thumbnail_path.unlink()
        candidate = build_publish_candidate(
            processor_result=processor_result,
            title="Example Title",
            caption="Ready",
            hashtags=(),
        )

        with self.assertRaisesRegex(PublishQueueError, "thumbnail_path does not exist"):
            self.store.enqueue_candidate(candidate)

    def _processed_result(
        self,
        asset_id: str = "asset-001",
        clip_name: str = "clip_001.mp4",
        thumb_name: str = "thumb.jpg",
    ) -> ProcessorResult:
        asset_output_dir = self.base_dir / "processed" / asset_id
        asset_output_dir.mkdir(parents=True, exist_ok=True)
        clip_path = asset_output_dir / clip_name
        thumbnail_path = asset_output_dir / thumb_name
        clip_path.write_bytes(b"clip-bytes")
        thumbnail_path.write_bytes(b"thumb-bytes")

        return ProcessorResult(
            asset_id=asset_id,
            source_path=self.base_dir / f"{asset_id}-source.mp4",
            status=PROCESSED_STATUS,
            output_files=(clip_path,),
            thumbnail_path=thumbnail_path,
            error_message=None,
        )

    def _queue_row_count(self) -> int:
        with sqlite3.connect(self.queue_path) as connection:
            row = connection.execute(f"SELECT COUNT(*) FROM {PUBLISH_QUEUE_TABLE_NAME}").fetchone()

        self.assertIsNotNone(row)
        return int(row[0])


class PublishCandidateBuilderTests(unittest.TestCase):
    def test_build_publish_candidate_defaults_caption_to_title(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            clip_path = base_dir / "clip.mp4"
            thumbnail_path = base_dir / "thumb.jpg"
            clip_path.write_bytes(b"clip-bytes")
            thumbnail_path.write_bytes(b"thumb-bytes")
            result = ProcessorResult(
                asset_id="asset-001",
                source_path=base_dir / "source.mp4",
                status=PROCESSED_STATUS,
                output_files=(clip_path,),
                thumbnail_path=thumbnail_path,
                error_message=None,
            )

            candidate = build_publish_candidate(processor_result=result, title="Example Title")

        self.assertEqual(candidate.title, "Example Title")
        self.assertEqual(candidate.caption, "Example Title")

    def test_processed_asset_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            result = ProcessorResult(
                asset_id="asset-001",
                source_path=base_dir / "source.mp4",
                status=PROCESSOR_FAILED_STATUS,
                output_files=(),
                thumbnail_path=None,
                error_message="ffmpeg failed",
            )

            with self.assertRaisesRegex(PublishQueueError, "Cannot enqueue processor result"):
                build_publish_candidate(processor_result=result, title="Example")


if __name__ == "__main__":
    unittest.main()
