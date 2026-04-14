from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ukrainian_tts_adapter import SUPPORTED_VOICES, UkrainianTtsError, generate_tts_wav


class UkrainianTtsAdapterTests(unittest.TestCase):
    def test_generate_tts_wav_raises_on_unsupported_voice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "out.wav"
            with self.assertRaises(UkrainianTtsError) as ctx:
                generate_tts_wav("Test script.", output_path, voice="unknown")

        self.assertIn("Unsupported TTS voice", str(ctx.exception))

    def test_generate_tts_wav_raises_on_missing_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "out.wav"
            with patch.dict("sys.modules", {"piper": None}):
                with self.assertRaises(UkrainianTtsError) as ctx:
                    generate_tts_wav("Test script.", output_path, voice="dmytro")

        self.assertIn("piper-tts", str(ctx.exception))

    def test_generate_tts_wav_calls_piper_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "out.wav"
            model_path = Path(tmp) / "model.onnx"
            model_path.write_bytes(b"fake-model")

            mock_voice = MagicMock()

            def fake_synthesize(text, wav_file):
                wav_file.setnchannels(1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(22050)
                wav_file.writeframes(b"\x00" * 100)

            mock_voice.synthesize_wav.side_effect = fake_synthesize
            mock_piper_voice_class = MagicMock()
            mock_piper_voice_class.load.return_value = mock_voice

            mock_piper = types.ModuleType("piper")
            mock_piper.PiperVoice = mock_piper_voice_class

            with patch.dict("sys.modules", {"piper": mock_piper}):
                with patch("src.config.ADVICE_PIPER_MODEL", str(model_path)):
                    generate_tts_wav("Test script.", output_path, voice="piper")

            mock_piper_voice_class.load.assert_called_once_with(str(model_path))
            mock_voice.synthesize_wav.assert_called_once()
            self.assertTrue(output_path.is_file())


if __name__ == "__main__":
    unittest.main()
