from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import config
from src.validator import (
    REJECTED_STATUS,
    VALID_STATUS,
    ValidationAsset,
    validate_asset,
    validate_assets,
)


class ValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.base_dir = Path(self.temp_dir.name)
        self.approved_dir = self.base_dir / "approved"
        self.rejected_dir = self.base_dir / "rejected"
        self.approved_dir.mkdir(parents=True, exist_ok=True)
        self.rejected_dir.mkdir(parents=True, exist_ok=True)

    def test_valid_asset_passes_validation(self) -> None:
        asset_path = self.approved_dir / "clip.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path)

        with patch("src.validator.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=["ffprobe"],
                returncode=0,
                stdout="12.5\n",
                stderr="",
            )
            result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        self.assertEqual(result.status, VALID_STATUS)
        self.assertEqual(result.reason, "Validation passed.")
        self.assertEqual(result.duration, 12.5)
        self.assertIsNone(result.rejected_path)
        self.assertTrue(asset_path.exists())

    def test_missing_file_is_rejected(self) -> None:
        missing_path = self.approved_dir / "missing.mp4"
        asset = self._build_asset(local_path=missing_path)

        result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        self.assertEqual(result.status, REJECTED_STATUS)
        self.assertIn("Local path does not exist", result.reason)
        self.assertIsNone(result.rejected_path)

    def test_unsupported_extension_is_rejected(self) -> None:
        asset_path = self.approved_dir / "clip.txt"
        asset_path.write_text("not video", encoding="utf-8")
        asset = self._build_asset(local_path=asset_path)

        result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        self.assertEqual(result.status, REJECTED_STATUS)
        self.assertIn("Unsupported extension", result.reason)
        self.assertEqual(result.rejected_path, self.rejected_dir / "clip.txt")
        self.assertFalse(asset_path.exists())

    def test_missing_title_is_rejected(self) -> None:
        asset_path = self.approved_dir / "clip.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path, title="   ")

        result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        self.assertEqual(result.status, REJECTED_STATUS)
        self.assertEqual(result.reason, "Title is required.")
        self.assertEqual(result.rejected_path, self.rejected_dir / "clip.mp4")

    def test_invalid_rights_status_is_rejected(self) -> None:
        asset_path = self.approved_dir / "clip.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path, rights_status="unknown")

        result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        self.assertEqual(result.status, REJECTED_STATUS)
        self.assertIn("Invalid rights_status", result.reason)
        self.assertEqual(result.rejected_path, self.rejected_dir / "clip.mp4")

    def test_unreadable_duration_probe_failure_is_rejected(self) -> None:
        asset_path = self.approved_dir / "clip.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path)

        with patch("src.validator.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=["ffprobe"],
                returncode=1,
                stdout="",
                stderr="probe failed",
            )
            result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        self.assertEqual(result.status, REJECTED_STATUS)
        self.assertIn("Failed to read media duration", result.reason)
        self.assertEqual(result.rejected_path, self.rejected_dir / "clip.mp4")

    def test_ffprobe_missing_binary_is_rejected(self) -> None:
        asset_path = self.approved_dir / "clip.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path)

        with patch("src.validator.subprocess.run", side_effect=FileNotFoundError):
            result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        self.assertEqual(result.status, REJECTED_STATUS)
        self.assertIn(f"{config.FFPROBE_BIN} is not installed", result.reason)
        self.assertEqual(result.rejected_path, self.rejected_dir / "clip.mp4")
        self.assertTrue((self.rejected_dir / "clip.mp4").exists())

    def test_rejected_file_is_moved_to_rejected_directory(self) -> None:
        asset_path = self.approved_dir / "clip.mkv"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path, title="")

        result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        self.assertEqual(result.status, REJECTED_STATUS)
        self.assertEqual(result.rejected_path, self.rejected_dir / "clip.mkv")
        self.assertTrue((self.rejected_dir / "clip.mkv").exists())
        self.assertFalse(asset_path.exists())

    def test_rejected_name_collision_gets_unique_filename(self) -> None:
        existing_rejected_path = self.rejected_dir / "clip.mp4"
        existing_rejected_path.write_bytes(b"existing-rejected-file")
        asset_path = self.approved_dir / "clip.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path, title="")

        result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        expected_rejected_path = self.rejected_dir / (
            f"clip{config.REJECTED_COLLISION_SEPARATOR}1.mp4"
        )
        self.assertEqual(result.status, REJECTED_STATUS)
        self.assertEqual(result.rejected_path, expected_rejected_path)
        self.assertTrue(existing_rejected_path.exists())
        self.assertTrue(expected_rejected_path.exists())
        self.assertFalse(asset_path.exists())

    def test_local_path_is_directory_is_rejected(self) -> None:
        directory_path = self.approved_dir / "clip.mp4"
        directory_path.mkdir(parents=True, exist_ok=True)
        asset = self._build_asset(local_path=directory_path)

        result = validate_asset(asset=asset, rejected_dir=self.rejected_dir)

        self.assertEqual(result.status, REJECTED_STATUS)
        self.assertIn("Local path is not a file", result.reason)
        self.assertIsNone(result.rejected_path)
        self.assertTrue(directory_path.exists())

    def test_batch_continues_when_one_item_fails(self) -> None:
        valid_path = self.approved_dir / "valid.mp4"
        invalid_path = self.approved_dir / "invalid.txt"
        valid_path.write_bytes(b"video-bytes")
        invalid_path.write_text("not video", encoding="utf-8")
        valid_asset = self._build_asset(local_path=valid_path, title="Valid")
        invalid_asset = self._build_asset(local_path=invalid_path, title="Invalid")

        with patch("src.validator.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=["ffprobe"],
                returncode=0,
                stdout="7.0\n",
                stderr="",
            )
            batch_result = validate_assets(
                assets=[invalid_asset, valid_asset],
                rejected_dir=self.rejected_dir,
            )

        self.assertEqual(len(batch_result.results), 2)
        self.assertEqual(batch_result.results[0].status, REJECTED_STATUS)
        self.assertEqual(batch_result.results[1].status, VALID_STATUS)
        self.assertTrue((self.rejected_dir / "invalid.txt").exists())
        self.assertTrue(valid_path.exists())

    def _build_asset(
        self,
        local_path: Path,
        rights_status: str | None = "owned",
        title: str | None = "Example Title",
    ) -> ValidationAsset:
        return ValidationAsset(
            source_type="local_file",
            source_path_or_url=str(local_path),
            rights_status=rights_status,
            title=title,
            local_path=local_path,
        )


if __name__ == "__main__":
    unittest.main()
