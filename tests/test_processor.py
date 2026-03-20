from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import config
from src.processor import (
    FAILED_STATUS,
    PROCESSED_STATUS,
    ProcessorAsset,
    process_asset,
    process_assets,
)


class ProcessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.base_dir = Path(self.temp_dir.name)
        self.processed_dir = self.base_dir / "processed"
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def test_successful_processing_result_with_mocked_ffmpeg_calls(self) -> None:
        asset_path = self.base_dir / "source.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path, duration=45.0)
        clip_path = self.processed_dir / asset.asset_id / config.CLIP_OUTPUT_NAME
        thumbnail_path = self.processed_dir / asset.asset_id / config.THUMBNAIL_OUTPUT_NAME

        with patch("src.processor.subprocess.run") as mock_run:
            mock_run.side_effect = self._successful_processing_side_effect(
                clip_outputs={clip_path},
                thumbnail_outputs={thumbnail_path},
                probe_stdout="45.0\n",
            )
            result = process_asset(asset=asset, processed_dir=self.processed_dir)

        self.assertEqual(result.status, PROCESSED_STATUS)
        self.assertEqual(result.output_files, (clip_path,))
        self.assertEqual(result.thumbnail_path, thumbnail_path)
        self.assertIsNone(result.error_message)
        self.assertTrue(clip_path.exists())
        self.assertTrue(thumbnail_path.exists())

    def test_processing_failure_returns_explicit_error(self) -> None:
        asset_path = self.base_dir / "source.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path, duration=45.0)

        with patch("src.processor.subprocess.run") as mock_run:
            mock_run.side_effect = [
                self._completed(stdout="45.0\n"),
                self._completed(returncode=1, stderr="ffmpeg failed"),
            ]
            result = process_asset(asset=asset, processed_dir=self.processed_dir)

        self.assertEqual(result.status, FAILED_STATUS)
        self.assertEqual(result.output_files, ())
        self.assertIsNone(result.thumbnail_path)
        self.assertEqual(result.error_message, "ffmpeg failed")

    def test_output_directory_is_created(self) -> None:
        asset_path = self.base_dir / "source.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path)
        clip_path = self.processed_dir / asset.asset_id / config.CLIP_OUTPUT_NAME
        thumbnail_path = self.processed_dir / asset.asset_id / config.THUMBNAIL_OUTPUT_NAME

        with patch("src.processor.subprocess.run") as mock_run:
            mock_run.side_effect = self._successful_processing_side_effect(
                clip_outputs={clip_path},
                thumbnail_outputs={thumbnail_path},
            )
            process_asset(asset=asset, processed_dir=self.processed_dir)

        self.assertTrue((self.processed_dir / asset.asset_id).exists())

    def test_short_source_does_not_fail(self) -> None:
        asset_path = self.base_dir / "short.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path, duration=5.0)
        clip_path = self.processed_dir / asset.asset_id / config.CLIP_OUTPUT_NAME
        thumbnail_path = self.processed_dir / asset.asset_id / config.THUMBNAIL_OUTPUT_NAME

        with patch("src.processor.subprocess.run") as mock_run:
            mock_run.side_effect = self._successful_processing_side_effect(
                clip_outputs={clip_path},
                thumbnail_outputs={thumbnail_path},
                probe_stdout="5.0\n",
            )
            result = process_asset(asset=asset, processed_dir=self.processed_dir)

        self.assertEqual(result.status, PROCESSED_STATUS)
        clip_command = mock_run.call_args_list[1].args[0]
        self.assertNotIn("-t", clip_command)

    def test_batch_continues_when_one_item_fails(self) -> None:
        first_path = self.base_dir / "first.mp4"
        second_path = self.base_dir / "second.mp4"
        first_path.write_bytes(b"video-one")
        second_path.write_bytes(b"video-two")
        first_asset = self._build_asset(asset_id="asset-001", local_path=first_path, duration=15.0)
        second_asset = self._build_asset(asset_id="asset-002", local_path=second_path, duration=15.0)
        second_clip_path = self.processed_dir / second_asset.asset_id / config.CLIP_OUTPUT_NAME
        second_thumbnail_path = self.processed_dir / second_asset.asset_id / config.THUMBNAIL_OUTPUT_NAME

        with patch("src.processor.subprocess.run") as mock_run:
            mock_run.side_effect = self._successful_processing_side_effect(
                clip_outputs={second_clip_path},
                thumbnail_outputs={second_thumbnail_path},
                failures={
                    self.processed_dir / first_asset.asset_id / config.CLIP_OUTPUT_NAME:
                    "clip generation failed"
                },
                probe_stdout="15.0\n",
            )
            batch_result = process_assets(
                assets=[first_asset, second_asset],
                processed_dir=self.processed_dir,
            )

        self.assertEqual(len(batch_result.results), 2)
        self.assertEqual(batch_result.results[0].status, FAILED_STATUS)
        self.assertEqual(batch_result.results[0].error_message, "clip generation failed")
        self.assertEqual(batch_result.results[1].status, PROCESSED_STATUS)

    def test_thumbnail_path_is_returned_correctly(self) -> None:
        asset_path = self.base_dir / "source.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path)
        clip_path = self.processed_dir / asset.asset_id / config.CLIP_OUTPUT_NAME
        thumbnail_path = self.processed_dir / asset.asset_id / config.THUMBNAIL_OUTPUT_NAME

        with patch("src.processor.subprocess.run") as mock_run:
            mock_run.side_effect = self._successful_processing_side_effect(
                clip_outputs={clip_path},
                thumbnail_outputs={thumbnail_path},
            )
            result = process_asset(asset=asset, processed_dir=self.processed_dir)

        self.assertEqual(
            result.thumbnail_path,
            self.processed_dir / asset.asset_id / config.THUMBNAIL_OUTPUT_NAME,
        )
        self.assertTrue(thumbnail_path.exists())

    def test_missing_clip_artifact_after_successful_mocked_ffmpeg_call_returns_failed(self) -> None:
        asset_path = self.base_dir / "source.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path, duration=45.0)

        with patch("src.processor.subprocess.run") as mock_run:
            mock_run.side_effect = [
                self._completed(stdout="45.0\n"),
                self._completed(),
            ]
            result = process_asset(asset=asset, processed_dir=self.processed_dir)

        expected_clip_path = self.processed_dir / asset.asset_id / config.CLIP_OUTPUT_NAME
        self.assertEqual(result.status, FAILED_STATUS)
        self.assertEqual(result.output_files, ())
        self.assertIsNone(result.thumbnail_path)
        self.assertEqual(
            result.error_message,
            f"Expected clip output was not created: {expected_clip_path}",
        )

    def test_missing_thumbnail_artifact_after_successful_mocked_ffmpeg_call_returns_failed(self) -> None:
        asset_path = self.base_dir / "source.mp4"
        asset_path.write_bytes(b"video-bytes")
        asset = self._build_asset(local_path=asset_path, duration=45.0)
        clip_path = self.processed_dir / asset.asset_id / config.CLIP_OUTPUT_NAME

        with patch("src.processor.subprocess.run") as mock_run:
            mock_run.side_effect = self._successful_processing_side_effect(
                clip_outputs={clip_path},
                thumbnail_outputs=set(),
                probe_stdout="45.0\n",
            )
            result = process_asset(asset=asset, processed_dir=self.processed_dir)

        expected_thumbnail_path = self.processed_dir / asset.asset_id / config.THUMBNAIL_OUTPUT_NAME
        self.assertEqual(result.status, FAILED_STATUS)
        self.assertEqual(result.output_files, ())
        self.assertIsNone(result.thumbnail_path)
        self.assertEqual(
            result.error_message,
            f"Expected thumbnail output was not created: {expected_thumbnail_path}",
        )

    def _build_asset(
        self,
        local_path: Path,
        duration: float = 12.0,
        asset_id: str = "asset-001",
    ) -> ProcessorAsset:
        return ProcessorAsset(
            asset_id=asset_id,
            local_path=local_path,
            title="Example Title",
            duration=duration,
        )

    def _completed(
        self,
        returncode: int = 0,
        stdout: str = "",
        stderr: str = "",
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["command"],
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
        )

    def _successful_processing_side_effect(
        self,
        clip_outputs: set[Path],
        thumbnail_outputs: set[Path],
        failures: dict[Path, str] | None = None,
        probe_stdout: str = "12.0\n",
    ):
        failure_map = failures or {}

        def side_effect(*args, **kwargs):
            del kwargs
            command = args[0]
            if command[0] == config.FFPROBE_BIN:
                return self._completed(stdout=probe_stdout)

            output_path = Path(command[-1])
            if output_path in failure_map:
                return self._completed(returncode=1, stderr=failure_map[output_path])

            if output_path in clip_outputs or output_path in thumbnail_outputs:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(b"")

            return self._completed()

        return side_effect


if __name__ == "__main__":
    unittest.main()
