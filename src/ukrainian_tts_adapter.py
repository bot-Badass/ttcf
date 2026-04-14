from __future__ import annotations

import logging
import wave
from pathlib import Path
from typing import Final

from src import config

LOGGER = logging.getLogger(__name__)

# Voices supported by ukrainian-tts (espnet backend).
# For Piper backend, voice selection is done via ADVICE_PIPER_MODEL.
SUPPORTED_VOICES: Final[frozenset[str]] = frozenset({
    "dmytro", "lada", "oleksa", "tetiana", "mykyta",
    "piper",  # sentinel value meaning "use Piper backend"
})

_VOICE_ENUM_ATTR: Final[dict[str, str]] = {
    "dmytro": "Dmytro",
    "lada": "Lada",
    "oleksa": "Oleksa",
    "tetiana": "Tetiana",
    "mykyta": "Mykyta",
}


class UkrainianTtsError(Exception):
    """Raised when Ukrainian TTS generation fails."""


def generate_tts_wav(
    script: str,
    output_path: Path,
    voice: str = config.ADVICE_TTS_VOICE,
) -> Path:
    """Generate Ukrainian TTS audio.

    Uses Piper TTS (piper-tts pip package) as the primary backend.
    ADVICE_PIPER_MODEL must point to a downloaded .onnx model file.

    Download Ukrainian Piper model:
      python -m piper.download_voices uk_UA-lada-x_low
    Or manually:
      mkdir -p ~/.local/share/piper-voices/uk
      curl -L https://huggingface.co/rhasspy/piper-voices/resolve/v1.0.0/
        uk/uk_UA/lada/x_low/uk_UA-lada-x_low.onnx \
        -o ~/.local/share/piper-voices/uk/uk_UA-lada-x_low.onnx
      curl -L https://huggingface.co/rhasspy/piper-voices/resolve/v1.0.0/
        uk/uk_UA/lada/x_low/uk_UA-lada-x_low.onnx.json \
        -o ~/.local/share/piper-voices/uk/uk_UA-lada-x_low.onnx.json
      Then set in .env:
        ADVICE_PIPER_MODEL=/Users/you/.local/share/piper-voices/uk/uk_UA-lada-x_low.onnx
    """
    if voice not in SUPPORTED_VOICES:
        raise UkrainianTtsError(
            f"Unsupported TTS voice: {voice!r}. "
            f"Supported voices: {sorted(SUPPORTED_VOICES)}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Strip stress marks (+) before TTS — Piper x_low model reads them aloud.
    # Stress marks are intended for espnet/ukrainian-tts, not Piper.
    tts_script = script.replace("+", "")
    return _generate_with_piper(tts_script, output_path)


def _generate_with_piper(script: str, output_path: Path) -> Path:
    """Generate audio using piper-tts Python API."""
    try:
        from piper import PiperVoice  # type: ignore[import]
    except ImportError as exc:
        raise UkrainianTtsError(
            "piper-tts is not installed. "
            "Install: pip install piper-tts"
        ) from exc

    model_path = config.ADVICE_PIPER_MODEL
    if not model_path:
        raise UkrainianTtsError(
            "ADVICE_PIPER_MODEL is not set. "
            "Download a Ukrainian voice model and set the path in .env.\n"
            "Example: ADVICE_PIPER_MODEL=/Users/you/.local/share/piper-voices/uk/uk_UA-lada-x_low.onnx\n"
            "Download: python -m piper.download_voices uk_UA-lada-x_low"
        )

    model = Path(model_path)
    if not model.is_file():
        raise UkrainianTtsError(
            f"Piper model file not found: {model}\n"
            f"Download: python -m piper.download_voices uk_UA-lada-x_low"
        )

    try:
        piper_voice = PiperVoice.load(str(model))
        with wave.open(str(output_path), "wb") as wav_file:
            piper_voice.synthesize_wav(script, wav_file)
    except UkrainianTtsError:
        raise
    except Exception as exc:
        raise UkrainianTtsError(f"Piper TTS generation failed: {exc}") from exc

    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise UkrainianTtsError(f"Piper TTS output is empty or missing: {output_path}")

    LOGGER.info(
        "TTS generated (piper): model=%s length=%d output=%s",
        model.name,
        len(script),
        output_path,
    )
    return output_path
