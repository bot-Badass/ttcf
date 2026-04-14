from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run
from src.reddit_intake import PipelineRunResult


class RunEntrypointTests(unittest.TestCase):
    def test_logging_is_configured_before_ensure_directories_exist(self) -> None:
        call_order: list[str] = []

        with (
            patch("run.configure_logging", side_effect=lambda: call_order.append("configure_logging")),
            patch("run.ensure_directories_exist", side_effect=lambda: call_order.append("ensure_directories_exist")),
        ):
            run.main([])

        self.assertEqual(call_order[:2], ["configure_logging", "ensure_directories_exist"])

    def test_reddit_mode_requires_background_video_path(self) -> None:
        with (
            patch("run.configure_logging"),
            patch("run.ensure_directories_exist"),
            patch("run.config.BACKGROUND_VIDEO_PATH", ""),
            patch("sys.stderr.write") as stderr_write,
        ):
            exit_code = run.main(["--reddit"])

        self.assertEqual(exit_code, 1)
        stderr_write.assert_called()

    def test_reddit_mode_logs_stage_errors_and_returns_one(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            background_video_path = Path(temp_dir_name) / "background.mp4"
            background_video_path.write_bytes(b"video")

            with (
                patch("run.configure_logging"),
                patch("run.ensure_directories_exist"),
                patch("run.config.validate_runtime_config"),
                patch(
                    "run.run_full_pipeline",
                    return_value=PipelineRunResult(
                        fetched=0,
                        accepted=0,
                        persisted=0,
                        sent_to_telegram=0,
                        processed_updates=0,
                        translated=0,
                        rendered=0,
                        enqueued=0,
                        stage_errors=("fetch/persist: boom",),
                    ),
                ),
                patch("run.LOGGER") as logger,
                patch("builtins.print"),
            ):
                exit_code = run.main(
                    ["--reddit", "--background-video-path", str(background_video_path)]
                )

        self.assertEqual(exit_code, 1)
        logger.error.assert_any_call("Reddit pipeline stage error: %s", "fetch/persist: boom")

    def test_bot_mode_requires_polling_enabled(self) -> None:
        with (
            patch("run.configure_logging"),
            patch("run.ensure_directories_exist"),
            patch("run.config.TELEGRAM_POLLING_ENABLED", False),
            patch("sys.stderr.write") as stderr_write,
        ):
            exit_code = run.main(["--bot"])

        self.assertEqual(exit_code, 1)
        stderr_write.assert_called()

    def test_bot_mode_starts_without_background_video_path(self) -> None:
        with (
            patch("run.configure_logging"),
            patch("run.ensure_directories_exist"),
            patch("run.config.TELEGRAM_POLLING_ENABLED", True),
            patch("run.config.BACKGROUND_VIDEO_PATH", ""),
            patch("run.config.validate_runtime_config"),
            patch("run.run_polling_loop") as polling_loop,
        ):
            exit_code = run.main(["--bot"])

        self.assertEqual(exit_code, 0)
        polling_loop.assert_called_once_with(background_video_path=None)


if __name__ == "__main__":
    unittest.main()
