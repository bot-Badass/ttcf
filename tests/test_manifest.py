from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.manifest import (
    ASSETS_TABLE_NAME,
    AssetNotFoundError,
    AssetRecord,
    DuplicateAssetIdError,
    DuplicateSha256Error,
    ManifestStore,
)


class ManifestStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.manifest_path = Path(self.temp_dir.name) / "manifest.sqlite3"
        self.store = ManifestStore(self.manifest_path)
        self.store.initialize()
        self.asset = AssetRecord(
            asset_id="asset-001",
            input_path=Path("/tmp/input/video.mp4"),
            source_type="file",
            source_path_or_url="/tmp/input/video.mp4",
            rights_status="owned",
            ingest_time="2026-03-20T16:00:00Z",
            sha256="abc123sha256",
            duration=12.5,
            status="ingested",
            output_files=(),
            error_message=None,
        )

    def test_initialize_creates_manifest_store(self) -> None:
        self.store.initialize()

        self.assertTrue(self.manifest_path.exists())
        with sqlite3.connect(self.manifest_path) as connection:
            row = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                (ASSETS_TABLE_NAME,),
            ).fetchone()

        self.assertIsNotNone(row)

    def test_insert_asset_persists_record(self) -> None:
        self.store.insert_asset(self.asset)

        stored_asset = self.store.get_asset(self.asset.asset_id)

        self.assertIsNotNone(stored_asset)
        self.assertEqual(stored_asset, self.asset)

    def test_duplicate_detection_by_sha256(self) -> None:
        duplicate_asset = AssetRecord(
            asset_id="asset-002",
            input_path=Path("/tmp/input/duplicate.mp4"),
            source_type="file",
            source_path_or_url="/tmp/input/duplicate.mp4",
            rights_status="owned",
            ingest_time="2026-03-20T16:05:00Z",
            sha256=self.asset.sha256,
            duration=9.0,
            status="ingested",
            output_files=(),
            error_message=None,
        )

        self.store.insert_asset(self.asset)

        self.assertTrue(self.store.is_duplicate_sha256(self.asset.sha256))
        with self.assertRaises(DuplicateSha256Error):
            self.store.insert_asset(duplicate_asset)

    def test_duplicate_asset_id_handling(self) -> None:
        duplicate_asset = AssetRecord(
            asset_id=self.asset.asset_id,
            input_path=Path("/tmp/input/other-video.mp4"),
            source_type="file",
            source_path_or_url="/tmp/input/other-video.mp4",
            rights_status="owned",
            ingest_time="2026-03-20T16:05:00Z",
            sha256="different-sha256",
            duration=9.0,
            status="ingested",
            output_files=(),
            error_message=None,
        )

        self.store.insert_asset(self.asset)

        with self.assertRaises(DuplicateAssetIdError):
            self.store.insert_asset(duplicate_asset)

    def test_update_status(self) -> None:
        self.store.insert_asset(self.asset)

        self.store.update_status(self.asset.asset_id, "processed")
        self.store.update_status(self.asset.asset_id, "processed")

        stored_asset = self.store.get_asset(self.asset.asset_id)
        self.assertIsNotNone(stored_asset)
        self.assertEqual(stored_asset.status, "processed")

    def test_load_existing_manifest_store_safely(self) -> None:
        self.store.insert_asset(self.asset)

        reloaded_store = ManifestStore(self.manifest_path)
        stored_asset = reloaded_store.get_asset(self.asset.asset_id)

        self.assertIsNotNone(stored_asset)
        self.assertEqual(stored_asset.sha256, self.asset.sha256)

    def test_update_output_files(self) -> None:
        self.store.insert_asset(self.asset)

        self.store.update_output_files(
            self.asset.asset_id,
            [Path("/tmp/output/clip-001.mp4"), Path("/tmp/output/clip-002.mp4")],
        )

        stored_asset = self.store.get_asset(self.asset.asset_id)
        self.assertIsNotNone(stored_asset)
        self.assertEqual(
            stored_asset.output_files,
            ("/tmp/output/clip-001.mp4", "/tmp/output/clip-002.mp4"),
        )

    def test_update_error_message(self) -> None:
        self.store.insert_asset(self.asset)

        self.store.update_error_message(self.asset.asset_id, "ffmpeg failed")

        stored_asset = self.store.get_asset(self.asset.asset_id)
        self.assertIsNotNone(stored_asset)
        self.assertEqual(stored_asset.error_message, "ffmpeg failed")

    def test_update_status_on_missing_asset_id_raises_error(self) -> None:
        with self.assertRaises(AssetNotFoundError):
            self.store.update_status("missing-asset", "processed")

    def test_update_output_files_on_missing_asset_id_raises_error(self) -> None:
        with self.assertRaises(AssetNotFoundError):
            self.store.update_output_files("missing-asset", [Path("/tmp/output/clip.mp4")])

    def test_update_error_message_on_missing_asset_id_raises_error(self) -> None:
        with self.assertRaises(AssetNotFoundError):
            self.store.update_error_message("missing-asset", "failed")

    def test_clear_error_message_back_to_none(self) -> None:
        self.store.insert_asset(self.asset)
        self.store.update_error_message(self.asset.asset_id, "ffmpeg failed")

        self.store.update_error_message(self.asset.asset_id, None)

        stored_asset = self.store.get_asset(self.asset.asset_id)
        self.assertIsNotNone(stored_asset)
        self.assertIsNone(stored_asset.error_message)

    def test_reload_after_updates(self) -> None:
        self.store.insert_asset(self.asset)
        self.store.update_status(self.asset.asset_id, "processed")
        self.store.update_output_files(self.asset.asset_id, [Path("/tmp/output/clip-001.mp4")])
        self.store.update_error_message(self.asset.asset_id, "minor warning")

        reloaded_store = ManifestStore(self.manifest_path)
        stored_asset = reloaded_store.get_asset(self.asset.asset_id)

        self.assertIsNotNone(stored_asset)
        self.assertEqual(stored_asset.status, "processed")
        self.assertEqual(stored_asset.output_files, ("/tmp/output/clip-001.mp4",))
        self.assertEqual(stored_asset.error_message, "minor warning")


if __name__ == "__main__":
    unittest.main()
