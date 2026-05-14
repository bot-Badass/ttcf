from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import config
from src.render import (
    RenderError,
    _build_ffmpeg_render_command,
    _build_subtitle_force_style,
    _escape_ffmpeg_filter_path,
    render_story_video,
)


class RenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir_handle = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.temp_dir_handle.name)
        self.background_video_path = self.base_dir / "background.mp4"
        self.audio_path = self.base_dir / "voiceover.wav"
        self.subtitle_path = self.base_dir / "subtitles.srt"
        self.output_path = self.base_dir / "video.mp4"
        self.background_video_path.write_bytes(b"video")
        self.audio_path.write_bytes(b"audio")
        self.subtitle_path.write_text("1\n00:00:00,000 --> 00:00:01,000\nSubtitle\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.temp_dir_handle.cleanup()

    def test_render_with_subtitles_calls(self) -> None:
        captured_command: list[str] = []

        def fake_run_command(command: list[str], timeout_seconds: int):
            del timeout_seconds
            captured_command[:] = command
            return type(
                "CompletedProcess",
                (),
                {"returncode": 0, "stdout": "", "stderr": ""},
            )()

        result = render_story_video(
            background_video_path=self.background_video_path,
            audio_path=self.audio_path,
            output_path=self.output_path,
            subtitle_path=self.subtitle_path,
            run_command=fake_run_command,
            probe_media_duration=lambda path: 5.0,
        )

        self.assertEqual(result, self.output_path)
        self.assertEqual(captured_command[0], config.FFMPEG_PATH)
        self.assertIn("-stream_loop", captured_command)
        self.assertIn("-map", captured_command)
        self.assertIn("0:v:0", captured_command)
        self.assertIn("1:a:0", captured_command)
        self.assertIn("-c:v", captured_command)
        self.assertIn(config.OUTPUT_VIDEO_CODEC, captured_command)
        self.assertIn("-c:a", captured_command)
        self.assertIn(config.OUTPUT_AUDIO_CODEC, captured_command)
        self.assertIn("-shortest", captured_command)
        self.assertIn("-vf", captured_command)
        self.assertTrue(
            any(
                "ass=" in argument and ".ass" in argument
                for argument in captured_command
            )
        )

    def test_subtitle_force_style_included_in_ffmpeg_command(self) -> None:
        command = _build_ffmpeg_render_command(
            background_video_path=self.background_video_path,
            audio_path=self.audio_path,
            output_path=self.output_path,
            subtitle_path=self.subtitle_path,
            duration_seconds=5.0,
            ffmpeg_path=config.FFMPEG_PATH,
        )

        self.assertIn("-vf", command)
        vf_argument = command[command.index("-vf") + 1]
        self.assertIn("ass=", vf_argument)
        self.assertIn(".ass", vf_argument)

    def test_subtitle_force_style_not_included_when_no_subtitle_path(self) -> None:
        command = _build_ffmpeg_render_command(
            background_video_path=self.background_video_path,
            audio_path=self.audio_path,
            output_path=self.output_path,
            subtitle_path=None,
            duration_seconds=5.0,
            ffmpeg_path=config.FFMPEG_PATH,
        )

        self.assertIn("-vf", command)
        vf_argument = command[command.index("-vf") + 1]
        self.assertNotIn("ass=", vf_argument)

    def test_build_subtitle_force_style_uses_config_values(self) -> None:
        with (
            patch("src.render.config.SUBTITLE_FONT_SIZE", 28),
            patch("src.render.config.SUBTITLE_ALIGNMENT", 2),
        ):
            force_style = _build_subtitle_force_style()

        self.assertIn("FontSize=28", force_style)
        self.assertIn("Alignment=2", force_style)

    def test_render_with_subtitles_failure_raises_clear_error(self) -> None:
        def failing_run_command(command: list[str], timeout_seconds: int):
            del command, timeout_seconds
            raise RenderError("subtitle filter failure")

        with self.assertRaises(RenderError) as context:
            render_story_video(
                background_video_path=self.background_video_path,
                audio_path=self.audio_path,
                output_path=self.output_path,
                subtitle_path=self.subtitle_path,
                run_command=failing_run_command,
                probe_media_duration=lambda path: 5.0,
            )

        self.assertEqual(
            str(context.exception),
            "FFmpeg subtitle burn failed: subtitle filter failure",
        )

    def test_missing_background_video_fails_clearly(self) -> None:
        self.background_video_path.unlink()

        with self.assertRaises(RenderError) as context:
            render_story_video(
                background_video_path=self.background_video_path,
                audio_path=self.audio_path,
                output_path=self.output_path,
                subtitle_path=self.subtitle_path,
                probe_media_duration=lambda path: 5.0,
            )

        self.assertEqual(
            str(context.exception),
            f"Background video file does not exist: {self.background_video_path}",
        )

    def test_missing_audio_fails_clearly(self) -> None:
        self.audio_path.unlink()

        with self.assertRaises(RenderError) as context:
            render_story_video(
                background_video_path=self.background_video_path,
                audio_path=self.audio_path,
                output_path=self.output_path,
                subtitle_path=self.subtitle_path,
                probe_media_duration=lambda path: 5.0,
            )

        self.assertEqual(
            str(context.exception),
            f"Audio file does not exist: {self.audio_path}",
        )

    def test_missing_subtitle_file_fails_clearly(self) -> None:
        self.subtitle_path.unlink()

        with self.assertRaises(RenderError) as context:
            render_story_video(
                background_video_path=self.background_video_path,
                audio_path=self.audio_path,
                output_path=self.output_path,
                subtitle_path=self.subtitle_path,
                probe_media_duration=lambda path: 5.0,
            )

        self.assertEqual(
            str(context.exception),
            f"Subtitle file does not exist: {self.subtitle_path}",
        )

    def test_subtitle_filter_path_escaping(self) -> None:
        escaped_path = _escape_ffmpeg_filter_path(
            Path("/tmp/sub:title's,file.srt"),
        )

        self.assertEqual(
            escaped_path,
            "/tmp/sub\\:title\\'s\\,file.srt",
        )

    def test_cta_share_overlay_for_non_last_part(self) -> None:
        with patch("src.render.config.CTA_ENABLED", True):
            command = _build_ffmpeg_render_command(
                background_video_path=self.background_video_path,
                audio_path=self.audio_path,
                output_path=self.output_path,
                subtitle_path=None,
                duration_seconds=10.0,
                ffmpeg_path=config.FFMPEG_PATH,
                part_number=2,
                total_parts=4,
            )

        self.assertIn("-filter_complex", command)
        fc = command[command.index("-filter_complex") + 1]
        # Non-last part: share CTA overlay ("Поділись"), not subscribe card.
        self.assertIn("colorkey=0x00ff00", fc)
        self.assertIn("share_ol", fc)
        # t0 = 10.0 - CTA_DURATION (default 3.0) = 7.0
        self.assertIn("gte(t,7.000)", fc)
        self.assertIn("[outv]", fc)
        self.assertNotIn("xfade", fc)

    def test_cta_subscribe_card_when_share_path_empty(self) -> None:
        with (
            patch("src.render.config.CTA_ENABLED", True),
            patch("src.render.config.CTA_SHARE_OVERLAY_PATH", ""),
        ):
            command = _build_ffmpeg_render_command(
                background_video_path=self.background_video_path,
                audio_path=self.audio_path,
                output_path=self.output_path,
                subtitle_path=None,
                duration_seconds=10.0,
                ffmpeg_path=config.FFMPEG_PATH,
                part_number=2,
                total_parts=4,
            )

        self.assertIn("-filter_complex", command)
        fc = command[command.index("-filter_complex") + 1]
        self.assertIn("xfade=transition=fade", fc)
        # xfade offset = 10.0 - CTA_DURATION (default 3.0) = 7.0
        self.assertIn("offset=7.000", fc)
        self.assertIn("Ч.3/4", fc)
        self.assertIn("[outv]", fc)

    def test_cta_overlay_series_complete_for_last_part(self) -> None:
        with patch("src.render.config.CTA_ENABLED", True):
            command = _build_ffmpeg_render_command(
                background_video_path=self.background_video_path,
                audio_path=self.audio_path,
                output_path=self.output_path,
                subtitle_path=None,
                duration_seconds=10.0,
                ffmpeg_path=config.FFMPEG_PATH,
                part_number=4,
                total_parts=4,
            )

        fc = command[command.index("-filter_complex") + 1]
        self.assertIn("Серію завершено", fc)
        self.assertNotIn("Ч.5", fc)

    def test_cta_overlay_not_added_when_disabled(self) -> None:
        with patch("src.render.config.CTA_ENABLED", False):
            command = _build_ffmpeg_render_command(
                background_video_path=self.background_video_path,
                audio_path=self.audio_path,
                output_path=self.output_path,
                subtitle_path=None,
                duration_seconds=10.0,
                ffmpeg_path=config.FFMPEG_PATH,
                part_number=2,
                total_parts=4,
            )

        self.assertIn("-vf", command)
        vf = command[command.index("-vf") + 1]
        self.assertNotIn("xfade", vf)
        self.assertNotIn("brand_card", vf)

    def test_cta_overlay_not_added_when_part_number_none(self) -> None:
        with patch("src.render.config.CTA_ENABLED", True):
            command = _build_ffmpeg_render_command(
                background_video_path=self.background_video_path,
                audio_path=self.audio_path,
                output_path=self.output_path,
                subtitle_path=None,
                duration_seconds=10.0,
                ffmpeg_path=config.FFMPEG_PATH,
                part_number=None,
                total_parts=None,
            )

        vf = command[command.index("-vf") + 1]
        self.assertNotIn("drawbox=x=0:y=1580", vf)


if __name__ == "__main__":
    unittest.main()
