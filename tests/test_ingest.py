from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.ingest import FAILED_STATUS, INGESTED_STATUS, SKIPPED_STATUS, ingest_inputs


class IngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.base_dir = Path(self.temp_dir.name)
        self.input_dir = self.base_dir / "inbox"
        self.approved_dir = self.base_dir / "approved"
        self.approved_sources_path = self.base_dir / "approved_sources.json"
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.approved_dir.mkdir(parents=True, exist_ok=True)

    def test_ingest_local_supported_file(self) -> None:
        source_path = self.input_dir / "clip.mp4"
        source_path.write_bytes(b"video-bytes")

        result = ingest_inputs(
            input_dir=self.input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=None,
        )

        copied_path = self.approved_dir / "clip.mp4"
        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0].status, INGESTED_STATUS)
        self.assertEqual(result.items[0].destination_path, copied_path)
        self.assertEqual(result.items[0].rights_status, "owned")
        self.assertEqual(copied_path.read_bytes(), b"video-bytes")
        self.assertEqual(result.batch_errors, ())

    def test_full_local_ingest_result_is_validator_compatible_for_rights_status_presence(self) -> None:
        source_path = self.input_dir / "clip.mp4"
        source_path.write_bytes(b"video-bytes")

        result = ingest_inputs(
            input_dir=self.input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=None,
        )

        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0].status, INGESTED_STATUS)
        self.assertIsNotNone(result.items[0].rights_status)
        self.assertEqual(result.items[0].rights_status, "owned")

    def test_skip_unsupported_extension(self) -> None:
        source_path = self.input_dir / "notes.txt"
        source_path.write_text("not a video", encoding="utf-8")

        result = ingest_inputs(
            input_dir=self.input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=None,
        )

        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0].status, SKIPPED_STATUS)
        self.assertIn("Unsupported extension", result.items[0].detail)
        self.assertFalse((self.approved_dir / "notes.txt").exists())

    def test_skip_duplicate_by_identical_content(self) -> None:
        source_path = self.input_dir / "clip.mp4"
        source_path.write_bytes(b"same-bytes")
        existing_destination = self.approved_dir / "clip.mp4"
        existing_destination.write_bytes(b"same-bytes")

        result = ingest_inputs(
            input_dir=self.input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=None,
        )

        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0].status, SKIPPED_STATUS)
        self.assertIn("Duplicate copy skipped", result.items[0].detail)

    def test_handle_missing_inbox_gracefully(self) -> None:
        missing_input_dir = self.base_dir / "missing-inbox"

        result = ingest_inputs(
            input_dir=missing_input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=None,
        )

        self.assertEqual(result.items, ())
        self.assertEqual(result.batch_errors, ())

    def test_handle_malformed_json_manifest_gracefully(self) -> None:
        self.approved_sources_path.write_text("{bad json", encoding="utf-8")

        result = ingest_inputs(
            input_dir=self.input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=self.approved_sources_path,
        )

        self.assertEqual(result.items, ())
        self.assertEqual(len(result.batch_errors), 1)
        self.assertIn("Malformed approved source JSON", result.batch_errors[0])

    def test_hidden_file_in_inbox_is_ignored_completely(self) -> None:
        hidden_file = self.input_dir / ".DS_Store"
        hidden_file.write_bytes(b"hidden")

        result = ingest_inputs(
            input_dir=self.input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=None,
        )

        self.assertEqual(result.items, ())
        self.assertEqual(result.batch_errors, ())
        self.assertFalse((self.approved_dir / ".DS_Store").exists())

    def test_mixed_inbox_with_hidden_and_valid_file_returns_only_valid_result(self) -> None:
        hidden_file = self.input_dir / ".gitkeep"
        hidden_file.write_text("", encoding="utf-8")
        source_path = self.input_dir / "clip.mp4"
        source_path.write_bytes(b"video-bytes")

        result = ingest_inputs(
            input_dir=self.input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=None,
        )

        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0].status, INGESTED_STATUS)
        self.assertEqual(result.items[0].source_path_or_url, str(source_path))
        self.assertTrue((self.approved_dir / "clip.mp4").exists())
        self.assertFalse((self.approved_dir / ".gitkeep").exists())

    def test_handle_one_bad_item_without_killing_batch(self) -> None:
        source_file = self.base_dir / "external.mp4"
        source_file.write_bytes(b"approved-video")
        self.approved_sources_path.write_text(
            json.dumps(
                [
                    {
                        "source_type": "local_file",
                        "source_path_or_url": str(source_file),
                        "rights_status": "owned",
                        "title": "Good Asset",
                    },
                    {
                        "source_type": "local_file",
                        "rights_status": "owned",
                        "title": "Bad Asset",
                    },
                ]
            ),
            encoding="utf-8",
        )

        result = ingest_inputs(
            input_dir=self.input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=self.approved_sources_path,
        )

        self.assertEqual(len(result.items), 2)
        self.assertEqual(
            tuple(sorted(item.status for item in result.items)),
            (FAILED_STATUS, INGESTED_STATUS),
        )
        self.assertTrue((self.approved_dir / "external.mp4").exists())

    def test_approved_source_local_file_preserves_explicit_rights_status(self) -> None:
        source_file = self.base_dir / "external.mp4"
        source_file.write_bytes(b"approved-video")
        self.approved_sources_path.write_text(
            json.dumps(
                [
                    {
                        "source_type": "local_file",
                        "source_path_or_url": str(source_file),
                        "rights_status": "licensed",
                        "title": "Licensed Asset",
                    }
                ]
            ),
            encoding="utf-8",
        )

        result = ingest_inputs(
            input_dir=self.input_dir,
            approved_dir=self.approved_dir,
            approved_sources_path=self.approved_sources_path,
        )

        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0].status, INGESTED_STATUS)
        self.assertEqual(result.items[0].rights_status, "licensed")


if __name__ == "__main__":
    unittest.main()
