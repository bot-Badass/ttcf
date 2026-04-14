from __future__ import annotations

import importlib
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src import config as config_module
from src.subtitles import (
    AlignedWord,
    SubtitleGenerationError,
    _align_words_with_whisperx,
    _extract_aligned_words,
    generate_subtitle_file,
)


class SubtitleTests(unittest.TestCase):
    def test_whisperx_alignment_used(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")
            align_words = Mock(
                return_value=(
                    AlignedWord("Hello", 0.0, 0.6),
                    AlignedWord("world", 0.6, 1.3),
                )
            )

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="Hello world",
                output_dir=temp_dir,
                align_words=align_words,
                subtitle_writer=_write_test_srt,
            )

            align_words.assert_called_once_with(audio_path, "Hello world")
            self.assertEqual(subtitle_path, temp_dir / "subtitles.srt")

    def test_phrase_grouping_respects_limits(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="one two three four five",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("one", 0.0, 0.4),
                    AlignedWord("two", 0.4, 0.8),
                    AlignedWord("three", 0.8, 1.2),
                    AlignedWord("four", 1.2, 1.6),
                    AlignedWord("five", 1.6, 2.0),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=3,
                max_chars_per_phrase=40,
            )

            self.assertEqual(
                subtitle_path.read_text(encoding="utf-8"),
                "1\n00:00:00,000 --> 00:00:01,200\none two three\n\n"
                "2\n00:00:01,200 --> 00:00:02,000\nfour five\n",
            )

    def test_subtitles_have_real_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="alpha beta gamma",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("alpha", 0.0, 0.45),
                    AlignedWord("beta", 0.9, 1.4),
                    AlignedWord("gamma", 2.2, 2.9),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=1,
                max_chars_per_phrase=20,
            )

            subtitle_text = subtitle_path.read_text(encoding="utf-8")
            self.assertIn("00:00:00,900 --> 00:00:01,400", subtitle_text)
            self.assertIn("00:00:02,200 --> 00:00:02,900", subtitle_text)

    def test_punctuation_aware_grouping_prefers_natural_breaks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="Hello world, this is a test. Another line follows!",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("Hello", 0.0, 0.2),
                    AlignedWord("world", 0.2, 0.5),
                    AlignedWord("this", 0.5, 0.7),
                    AlignedWord("is", 0.7, 0.85),
                    AlignedWord("a", 0.85, 0.95),
                    AlignedWord("test", 0.95, 1.3),
                    AlignedWord("Another", 1.35, 1.6),
                    AlignedWord("line", 1.6, 1.9),
                    AlignedWord("follows", 1.9, 2.4),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=10,
                max_chars_per_phrase=80,
            )

            self.assertEqual(
                subtitle_path.read_text(encoding="utf-8"),
                "1\n00:00:00,000 --> 00:00:01,300\nHello world, this is a test.\n\n"
                "2\n00:00:01,350 --> 00:00:02,400\nAnother line follows!\n",
            )

    def test_malformed_whisperx_payload_fails_clearly(self) -> None:
        with self.assertRaises(SubtitleGenerationError) as context:
            _extract_aligned_words(
                {
                    "word_segments": (
                        {"word": "Hello", "start": 0.0, "end": 0.4},
                        {"word": "", "start": 0.4, "end": 0.8},
                    )
                }
            )

        self.assertEqual(str(context.exception), "WhisperX returned a word without text.")

    def test_script_is_not_ignored_in_grouping_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="Hello, world!",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("Hello", 0.0, 0.4),
                    AlignedWord("world", 0.4, 0.9),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=5,
                max_chars_per_phrase=40,
            )

            self.assertIn("Hello, world!", subtitle_path.read_text(encoding="utf-8"))

    def test_alignment_does_not_collapse_on_missing_word(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="Hello brave world!",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("Hello", 0.0, 0.3),
                    AlignedWord("planet", 0.3, 0.6),
                    AlignedWord("world", 0.6, 1.0),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=6,
                max_chars_per_phrase=40,
            )

            self.assertIn("Hello planet world!", subtitle_path.read_text(encoding="utf-8"))

    def test_alignment_output_with_punctuation_produces_readable_phrases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="First clause, second clause, third clause.",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("First", 0.0, 0.3),
                    AlignedWord("clause", 0.3, 0.6),
                    AlignedWord("second", 0.6, 0.9),
                    AlignedWord("clause", 0.9, 1.2),
                    AlignedWord("third", 1.2, 1.5),
                    AlignedWord("clause", 1.5, 1.9),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=4,
                max_chars_per_phrase=32,
            )

            self.assertEqual(
                subtitle_path.read_text(encoding="utf-8"),
                "1\n00:00:00,000 --> 00:00:01,200\nFirst clause, second clause,\n\n"
                "2\n00:00:01,200 --> 00:00:01,900\nthird clause.\n",
            )

    def test_pause_gap_forces_phrase_split(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="One two three four",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("One", 0.0, 0.2),
                    AlignedWord("two", 0.2, 0.4),
                    AlignedWord("three", 1.1, 1.4),
                    AlignedWord("four", 1.4, 1.7),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=6,
                max_chars_per_phrase=40,
                max_silence_gap_seconds=0.5,
            )

            self.assertEqual(
                subtitle_path.read_text(encoding="utf-8"),
                "1\n00:00:00,000 --> 00:00:00,400\nOne two\n\n"
                "2\n00:00:01,100 --> 00:00:01,700\nthree four\n",
            )

    def test_strong_punctuation_priority_over_limits(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="Hello world. Again now later",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("Hello", 0.0, 0.2),
                    AlignedWord("world", 0.2, 0.5),
                    AlignedWord("Again", 0.5, 0.8),
                    AlignedWord("now", 0.8, 1.0),
                    AlignedWord("later", 1.0, 1.3),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=5,
                max_chars_per_phrase=80,
            )

            self.assertEqual(
                subtitle_path.read_text(encoding="utf-8"),
                "1\n00:00:00,000 --> 00:00:00,500\nHello world.\n\n"
                "2\n00:00:00,500 --> 00:00:01,300\nAgain now later\n",
            )

    def test_grouping_combines_pause_and_punctuation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="First chunk, still here. After pause now.",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("First", 0.0, 0.2),
                    AlignedWord("chunk", 0.2, 0.45),
                    AlignedWord("still", 0.45, 0.7),
                    AlignedWord("here", 0.7, 0.95),
                    AlignedWord("After", 1.7, 1.95),
                    AlignedWord("pause", 1.95, 2.2),
                    AlignedWord("now", 2.2, 2.45),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=8,
                max_chars_per_phrase=80,
                max_silence_gap_seconds=0.5,
            )

            self.assertEqual(
                subtitle_path.read_text(encoding="utf-8"),
                "1\n00:00:00,000 --> 00:00:00,950\nFirst chunk, still here.\n\n"
                "2\n00:00:01,700 --> 00:00:02,450\nAfter pause now.\n",
            )

    def test_subtitle_generation_failure_propagates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            with self.assertRaises(SubtitleGenerationError) as context:
                generate_subtitle_file(
                    audio_path=audio_path,
                    script="Hello world",
                    output_dir=temp_dir,
                    align_words=_raise_alignment_error,
                    subtitle_writer=_write_test_srt,
                    alignment_mode="whisperx",
                )

            self.assertEqual(str(context.exception), "WhisperX failed.")

    def test_deterministic_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            first_dir = temp_dir / "first"
            second_dir = temp_dir / "second"
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            def align_words(path: Path, script: str):
                del path, script
                return (
                    AlignedWord("same", 0.0, 0.5),
                    AlignedWord("input", 0.5, 1.1),
                    AlignedWord("output", 1.1, 1.9),
                )

            first_path = generate_subtitle_file(
                audio_path=audio_path,
                script="same input output",
                output_dir=first_dir,
                align_words=align_words,
                subtitle_writer=_write_test_srt,
            )
            second_path = generate_subtitle_file(
                audio_path=audio_path,
                script="same input output",
                output_dir=second_dir,
                align_words=align_words,
                subtitle_writer=_write_test_srt,
            )

            self.assertEqual(
                first_path.read_text(encoding="utf-8"),
                second_path.read_text(encoding="utf-8"),
            )

    def test_grouping_still_respects_word_and_char_limits(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            subtitle_path = generate_subtitle_file(
                audio_path=audio_path,
                script="One two three, four five six.",
                output_dir=temp_dir,
                align_words=lambda path, script: (
                    AlignedWord("One", 0.0, 0.2),
                    AlignedWord("two", 0.2, 0.4),
                    AlignedWord("three", 0.4, 0.6),
                    AlignedWord("four", 0.6, 0.8),
                    AlignedWord("five", 0.8, 1.0),
                    AlignedWord("six", 1.0, 1.2),
                ),
                subtitle_writer=_write_test_srt,
                max_words_per_phrase=3,
                max_chars_per_phrase=18,
            )

            self.assertEqual(
                subtitle_path.read_text(encoding="utf-8"),
                "1\n00:00:00,000 --> 00:00:00,600\nOne two three,\n\n"
                "2\n00:00:00,600 --> 00:00:01,200\nfour five six.\n",
            )

    def test_empty_script_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            with self.assertRaises(SubtitleGenerationError) as context:
                generate_subtitle_file(
                    audio_path=audio_path,
                    script="   ",
                    output_dir=temp_dir,
                    align_words=lambda path, script: (),
                    subtitle_writer=_write_test_srt,
                )

            self.assertEqual(str(context.exception), "Subtitle script is required.")

    def test_strict_whisperx_mode_fails_on_whisperx_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            with patch("src.subtitles._align_words_with_whisperx", side_effect=SubtitleGenerationError("WhisperX exploded")):
                with self.assertRaises(SubtitleGenerationError) as context:
                    generate_subtitle_file(
                        audio_path=audio_path,
                        script="Hello world",
                        output_dir=temp_dir,
                        subtitle_writer=_write_test_srt,
                        alignment_mode="whisperx",
                    )

            self.assertEqual(str(context.exception), "WhisperX exploded")

    def test_strict_fallback_mode_skips_whisperx(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            with patch("src.subtitles._align_words_with_whisperx") as whisperx_align:
                with patch("src.subtitles._probe_audio_duration_seconds", return_value=2.0):
                    subtitle_path = generate_subtitle_file(
                        audio_path=audio_path,
                        script="Hello world again",
                        output_dir=temp_dir,
                        subtitle_writer=_write_test_srt,
                        alignment_mode="fallback",
                    )

            whisperx_align.assert_not_called()
            self.assertTrue(subtitle_path.exists())
            self.assertIn("Hello world again", subtitle_path.read_text(encoding="utf-8"))

    def test_whisperx_or_fallback_mode_falls_back_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            with patch("src.subtitles._align_words_with_whisperx", side_effect=SubtitleGenerationError("alignment broke")):
                with patch("src.subtitles._probe_audio_duration_seconds", return_value=2.4):
                    with self.assertLogs("src.subtitles", level="WARNING") as logs:
                        subtitle_path = generate_subtitle_file(
                            audio_path=audio_path,
                            script="Hello world. Another phrase.",
                            output_dir=temp_dir,
                            subtitle_writer=_write_test_srt,
                            alignment_mode="whisperx_or_fallback",
                        )

            self.assertTrue(subtitle_path.exists())
            self.assertIn("Subtitle backend switched to fallback", "\n".join(logs.output))
            self.assertIn("alignment broke", "\n".join(logs.output))

    def test_fallback_subtitles_still_produce_valid_non_empty_srt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            with patch("src.subtitles._probe_audio_duration_seconds", return_value=3.0):
                subtitle_path = generate_subtitle_file(
                    audio_path=audio_path,
                    script="One two three. Four five six.",
                    output_dir=temp_dir,
                    subtitle_writer=_write_test_srt,
                    alignment_mode="fallback",
                )

            subtitle_text = subtitle_path.read_text(encoding="utf-8")
            self.assertTrue(subtitle_text.strip())
            self.assertIn("-->", subtitle_text)
            self.assertIn("One two three.", subtitle_text)

    def test_fallback_timing_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")
            first_dir = temp_dir / "first"
            second_dir = temp_dir / "second"

            with patch("src.subtitles._probe_audio_duration_seconds", return_value=4.0):
                first_path = generate_subtitle_file(
                    audio_path=audio_path,
                    script="Fallback timing stays stable across reruns.",
                    output_dir=first_dir,
                    subtitle_writer=_write_test_srt,
                    alignment_mode="fallback",
                )
                second_path = generate_subtitle_file(
                    audio_path=audio_path,
                    script="Fallback timing stays stable across reruns.",
                    output_dir=second_dir,
                    subtitle_writer=_write_test_srt,
                    alignment_mode="fallback",
                )

            self.assertEqual(
                first_path.read_text(encoding="utf-8"),
                second_path.read_text(encoding="utf-8"),
            )

    def test_cpu_device_defaults_to_int8(self) -> None:
        with _reloaded_config_for_test(WHISPERX_DEVICE="cpu"):
            self.assertEqual(config_module.WHISPERX_COMPUTE_TYPE, "int8")

    def test_explicit_compute_type_override_is_respected(self) -> None:
        with _reloaded_config_for_test(
            WHISPERX_DEVICE="cuda",
            WHISPERX_COMPUTE_TYPE="float32",
        ):
            self.assertEqual(config_module.WHISPERX_COMPUTE_TYPE, "float32")

    def test_whisperx_load_model_receives_compute_type_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            fake_model = Mock()
            fake_model.transcribe.return_value = {
                "language": "uk",
                "segments": [{"text": "Hello world"}],
            }
            load_model = Mock(return_value=fake_model)
            whisperx_module = SimpleNamespace(
                load_model=load_model,
                load_align_model=Mock(return_value=("align-model", {"meta": True})),
                align=Mock(
                    return_value={
                        "word_segments": (
                            {"word": "Hello", "start": 0.0, "end": 0.4},
                            {"word": "world", "start": 0.4, "end": 0.9},
                        )
                    }
                ),
            )

            with patch.dict("sys.modules", {"whisperx": whisperx_module}):
                with _reloaded_config_for_test(
                    WHISPERX_DEVICE="cpu",
                    WHISPERX_COMPUTE_TYPE="int8",
                ):
                    aligned_words = _align_words_with_whisperx(audio_path, "Hello world")

            self.assertEqual(len(aligned_words), 2)
            self.assertEqual(load_model.call_args.kwargs["compute_type"], "int8")
            self.assertIn("compute_type", load_model.call_args.kwargs)

    def test_invalid_cpu_compute_type_fails_without_hidden_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            audio_path = temp_dir / "voiceover.wav"
            audio_path.write_bytes(b"audio")

            whisperx_module = SimpleNamespace()
            with patch.dict("sys.modules", {"whisperx": whisperx_module}):
                with _reloaded_config_for_test(
                    WHISPERX_DEVICE="cpu",
                    WHISPERX_COMPUTE_TYPE="float16",
                ):
                    with self.assertRaises(SubtitleGenerationError) as context:
                        _align_words_with_whisperx(audio_path, "Hello world")

            self.assertEqual(
                str(context.exception),
                "WHISPERX_COMPUTE_TYPE=float16 is not supported on CPU. Use int8.",
            )


def _write_test_srt(phrases, subtitle_path: Path) -> None:
    entries: list[str] = []
    for index, phrase in enumerate(phrases, start=1):
        entries.append(
            "\n".join(
                (
                    str(index),
                    f"{_format_timestamp(phrase.start_seconds)} --> {_format_timestamp(phrase.end_seconds)}",
                    phrase.text,
                )
            )
        )
    subtitle_path.write_text("\n\n".join(entries) + "\n", encoding="utf-8")


def _format_timestamp(total_seconds: float) -> str:
    milliseconds_total = max(0, int(round(total_seconds * 1000)))
    hours, remainder = divmod(milliseconds_total, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, milliseconds = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{milliseconds:03d}"


def _raise_alignment_error(audio_path: Path, script: str):
    del audio_path, script
    raise SubtitleGenerationError("WhisperX failed.")


@contextmanager
def _reloaded_config_for_test(**env: str):
    original_env = os.environ.copy()
    try:
        with patch.dict(os.environ, env, clear=True):
            yield importlib.reload(config_module)
    finally:
        with patch.dict(os.environ, original_env, clear=True):
            importlib.reload(config_module)


if __name__ == "__main__":
    unittest.main()
