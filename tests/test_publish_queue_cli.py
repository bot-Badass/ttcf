from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import publish_queue_cli
from src.processor import PROCESSED_STATUS, ProcessorResult
from src.publisher import PublishQueueStore, build_publish_candidate


class PublishQueueCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.base_dir = Path(self.temp_dir.name)
        self.queue_path = self.base_dir / "publish_queue.sqlite3"
        self.store = PublishQueueStore(self.queue_path)
        self.store.initialize()

    def test_list_command_shows_queued_items(self) -> None:
        queue_item = self._enqueue_item(asset_id="asset-001", title="Queue Title")

        exit_code, stdout, stderr = self._run_cli(
            "--queue-path",
            str(self.queue_path),
            "list",
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("asset_id=asset-001", stdout)
        self.assertIn(f"clip_path={queue_item.clip_path}", stdout)
        self.assertIn("title=Queue Title", stdout)
        self.assertIn("status=queued", stdout)

    def test_publish_command_updates_item_status(self) -> None:
        queue_item = self._enqueue_item(asset_id="asset-001", title="Queue Title")

        exit_code, stdout, stderr = self._run_cli(
            "--queue-path",
            str(self.queue_path),
            "publish",
            "--asset-id",
            queue_item.asset_id,
            "--clip-path",
            str(queue_item.clip_path),
        )

        updated_item = self.store.get_item(queue_item.asset_id, queue_item.clip_path)
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("Marked published", stdout)
        self.assertIsNotNone(updated_item)
        self.assertEqual(updated_item.status, "published")

    def test_fail_command_updates_item_status_and_error_message(self) -> None:
        queue_item = self._enqueue_item(asset_id="asset-001", title="Queue Title")

        exit_code, stdout, stderr = self._run_cli(
            "--queue-path",
            str(self.queue_path),
            "fail",
            "--asset-id",
            queue_item.asset_id,
            "--clip-path",
            str(queue_item.clip_path),
            "--error-message",
            "Manual publish failed.",
        )

        updated_item = self.store.get_item(queue_item.asset_id, queue_item.clip_path)
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("Marked failed", stdout)
        self.assertIsNotNone(updated_item)
        self.assertEqual(updated_item.status, "failed")
        self.assertEqual(updated_item.error_message, "Manual publish failed.")

    def test_payload_command_prints_dry_run_payload(self) -> None:
        queue_item = self._enqueue_item(asset_id="asset-001", title="Queue Title")

        exit_code, stdout, stderr = self._run_cli(
            "--queue-path",
            str(self.queue_path),
            "payload",
            "--asset-id",
            queue_item.asset_id,
            "--clip-path",
            str(queue_item.clip_path),
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn(f"local_video_path={queue_item.clip_path}", stdout)
        self.assertIn(f"local_thumbnail_path={queue_item.thumbnail_path}", stdout)
        self.assertIn("title=Queue Title", stdout)
        self.assertIn("caption=Queue Title", stdout)
        self.assertIn("hashtags=#demo", stdout)

    def test_missing_item_returns_non_zero_exit_code(self) -> None:
        missing_clip_path = self.base_dir / "missing.mp4"

        exit_code, stdout, stderr = self._run_cli(
            "--queue-path",
            str(self.queue_path),
            "payload",
            "--asset-id",
            "missing-asset",
            "--clip-path",
            str(missing_clip_path),
        )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout, "")
        self.assertIn("Queue item not found", stderr)

    def test_queue_commands_do_not_require_external_runtime_groups(self) -> None:
        with patch("publish_queue_cli.config.validate_runtime_config") as validate_runtime_config_mock:
            exit_code, stdout, stderr = self._run_cli(
                "--queue-path",
                str(self.queue_path),
                "list",
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(stdout.strip(), "No queued items.")
        validate_runtime_config_mock.assert_not_called()

    def _enqueue_item(self, asset_id: str, title: str):
        processor_result = self._processed_result(asset_id=asset_id)
        candidate = build_publish_candidate(
            processor_result=processor_result,
            title=title,
            caption=None,
            hashtags=("#demo",),
        )
        return self.store.enqueue_candidate(candidate).item

    def _processed_result(self, asset_id: str) -> ProcessorResult:
        asset_output_dir = self.base_dir / "processed" / asset_id
        asset_output_dir.mkdir(parents=True, exist_ok=True)
        clip_path = asset_output_dir / "clip_001.mp4"
        thumbnail_path = asset_output_dir / "thumb.jpg"
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

    def _run_cli(self, *args: str) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = publish_queue_cli.main(args)
        return exit_code, stdout.getvalue(), stderr.getvalue()


if __name__ == "__main__":
    unittest.main()
