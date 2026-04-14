from __future__ import annotations

import logging
import re
import subprocess
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from src import config


LOGGER = logging.getLogger(__name__)


class SubtitleGenerationError(Exception):
    """Raised when subtitle generation fails."""


@dataclass(frozen=True, slots=True)
class AlignedWord:
    text: str
    start_seconds: float
    end_seconds: float


@dataclass(frozen=True, slots=True)
class SubtitlePhrase:
    text: str
    start_seconds: float
    end_seconds: float


@dataclass(frozen=True, slots=True)
class ScriptToken:
    display: str
    normalized: str


WordAlignmentBoundary = Callable[[Path, str], Sequence[AlignedWord]]
SubtitleWriterBoundary = Callable[[Sequence[SubtitlePhrase], Path], None]


def generate_subtitle_file(
    audio_path: Path,
    script: str,
    output_dir: Path,
    *,
    align_words: WordAlignmentBoundary | None = None,
    subtitle_writer: SubtitleWriterBoundary | None = None,
    alignment_mode: str | None = None,
    max_words_per_phrase: int = config.SUBTITLE_MAX_WORDS_PER_PHRASE,
    max_chars_per_phrase: int = config.SUBTITLE_MAX_CHARS_PER_PHRASE,
    max_silence_gap_seconds: float = config.SUBTITLE_MAX_SILENCE_GAP_SECONDS,
) -> Path:
    normalized_script = script.strip()
    if not normalized_script:
        raise SubtitleGenerationError("Subtitle script is required.")
    if max_words_per_phrase <= 0:
        raise SubtitleGenerationError("Subtitle max words per phrase must be positive.")
    if max_chars_per_phrase <= 0:
        raise SubtitleGenerationError("Subtitle max chars per phrase must be positive.")
    if max_silence_gap_seconds < 0:
        raise SubtitleGenerationError("Subtitle max silence gap must be non-negative.")

    aligned_words = _align_words_for_subtitles(
        audio_path=audio_path,
        script=normalized_script,
        align_words=align_words,
        alignment_mode=alignment_mode,
    )
    if not aligned_words:
        raise SubtitleGenerationError("Subtitle alignment did not produce aligned words.")

    phrases = _group_aligned_words(
        aligned_words,
        max_words_per_phrase=max_words_per_phrase,
        max_chars_per_phrase=max_chars_per_phrase,
        max_silence_gap_seconds=max_silence_gap_seconds,
    )
    if not phrases:
        raise SubtitleGenerationError("Subtitle alignment did not produce subtitle phrases.")

    output_dir.mkdir(parents=True, exist_ok=True)
    subtitle_path = output_dir / "subtitles.srt"
    (subtitle_writer or _write_srt_with_pysubs2)(phrases, subtitle_path)
    return subtitle_path


def _align_words_for_subtitles(
    audio_path: Path,
    script: str,
    *,
    align_words: WordAlignmentBoundary | None,
    alignment_mode: str | None,
) -> tuple[AlignedWord, ...]:
    resolved_mode = _resolve_alignment_mode(alignment_mode)
    if resolved_mode == "fallback":
        return _align_words_with_fallback(audio_path, script)

    # Try faster-whisper first (no PyTorch required, works on Python 3.11).
    # Fall back to whisperx if faster-whisper is unavailable.
    try:
        return _align_words_with_faster_whisper(audio_path)
    except SubtitleGenerationError as fw_exc:
        if resolved_mode == "fallback":
            return _align_words_with_fallback(audio_path, script)
        LOGGER.debug("faster-whisper not available: %s — trying whisperx", fw_exc)

    whisperx_boundary = align_words or _align_words_with_whisperx
    try:
        return _align_words_with_script_text(
            tuple(whisperx_boundary(audio_path, script)),
            script,
        )
    except SubtitleGenerationError as exc:
        if resolved_mode != "whisperx_or_fallback":
            raise
        LOGGER.warning(
            "Subtitle backend switched to fallback for %s: %s",
            audio_path,
            exc,
        )
        return _align_words_with_fallback(audio_path, script)


def _resolve_alignment_mode(alignment_mode: str | None) -> str:
    mode = (alignment_mode or config.SUBTITLE_ALIGNMENT_MODE).strip()
    allowed_modes = {"whisperx", "fallback", "whisperx_or_fallback"}
    if mode not in allowed_modes:
        raise SubtitleGenerationError(
            "SUBTITLE_ALIGNMENT_MODE must be one of: "
            "whisperx, fallback, whisperx_or_fallback."
        )
    return mode


def _align_words_with_script_text(
    aligned_words: Sequence[AlignedWord],
    script: str,
) -> tuple[AlignedWord, ...]:
    if not aligned_words:
        raise SubtitleGenerationError("WhisperX did not return aligned words.")
    return _apply_script_text_to_aligned_words(aligned_words, script)


def _align_words_with_fallback(audio_path: Path, script: str) -> tuple[AlignedWord, ...]:
    duration_seconds = _probe_audio_duration_seconds(audio_path)
    script_tokens = _tokenize_script(script)
    if not script_tokens:
        raise SubtitleGenerationError("Subtitle script is required.")

    weights = [max(len(token.normalized), 1) for token in script_tokens]
    total_weight = sum(weights)
    if total_weight <= 0:
        raise SubtitleGenerationError("Fallback subtitle alignment could not weight script tokens.")

    aligned_words: list[AlignedWord] = []
    current_start = 0.0
    for index, token in enumerate(script_tokens):
        if index == len(script_tokens) - 1:
            current_end = duration_seconds
        else:
            token_duration = duration_seconds * (weights[index] / total_weight)
            current_end = current_start + token_duration
        aligned_words.append(
            AlignedWord(
                text=token.display,
                start_seconds=current_start,
                end_seconds=current_end,
            )
        )
        current_start = current_end

    return tuple(aligned_words)


def _probe_audio_duration_seconds(audio_path: Path) -> float:
    if not audio_path.exists():
        raise SubtitleGenerationError(
            f"Subtitle audio file does not exist: {audio_path}"
        )

    command = [
        config.FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(audio_path),
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=config.FFPROBE_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise SubtitleGenerationError("ffprobe is not installed.") from exc
    except subprocess.TimeoutExpired as exc:
        raise SubtitleGenerationError("ffprobe timed out while probing subtitle audio.") from exc

    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip() or "unknown ffprobe error"
        raise SubtitleGenerationError(
            f"ffprobe failed while probing subtitle audio: {stderr}"
        )

    try:
        duration_seconds = float(completed.stdout.strip())
    except ValueError as exc:
        raise SubtitleGenerationError(
            "ffprobe returned an unreadable subtitle audio duration."
        ) from exc
    if duration_seconds <= 0:
        raise SubtitleGenerationError(
            "ffprobe returned a non-positive subtitle audio duration."
        )
    return duration_seconds


def _align_words_with_faster_whisper(audio_path: Path) -> tuple[AlignedWord, ...]:
    """Transcribe with word-level timestamps using faster-whisper (no PyTorch).

    faster-whisper uses CTranslate2 and works on Python 3.11 without torch>=2.4.
    Returns word-level AlignedWord tuples directly from the transcription.
    Raises SubtitleGenerationError if faster-whisper is not installed or fails.
    """
    try:
        from faster_whisper import WhisperModel  # type: ignore[import]
    except ImportError as exc:
        raise SubtitleGenerationError(
            "faster-whisper is not installed. Install: pip install faster-whisper"
        ) from exc

    try:
        model = WhisperModel(
            config.WHISPERX_MODEL_SIZE,
            device=config.WHISPERX_DEVICE,
            compute_type=config.WHISPERX_COMPUTE_TYPE,
        )
        segments, _info = model.transcribe(
            str(audio_path),
            language="uk",
            word_timestamps=True,
        )
    except SubtitleGenerationError:
        raise
    except Exception as exc:
        raise SubtitleGenerationError(
            f"faster-whisper transcription failed: {exc}"
        ) from exc

    aligned_words: list[AlignedWord] = []
    for segment in segments:
        if segment.words is None:
            continue
        for word in segment.words:
            text = word.word.strip()
            if not text:
                continue
            aligned_words.append(
                AlignedWord(
                    text=text,
                    start_seconds=float(word.start),
                    end_seconds=float(word.end),
                )
            )

    if not aligned_words:
        raise SubtitleGenerationError(
            "faster-whisper did not return any word-level timestamps."
        )

    LOGGER.info(
        "faster-whisper aligned %d words for %s",
        len(aligned_words),
        audio_path.name,
    )
    return tuple(aligned_words)


def _align_words_with_whisperx(audio_path: Path, script: str) -> tuple[AlignedWord, ...]:
    try:
        import whisperx
    except ModuleNotFoundError as exc:
        raise SubtitleGenerationError("whisperx is not installed.") from exc

    try:
        compute_type = _resolve_whisperx_compute_type()
        model = whisperx.load_model(
            config.WHISPERX_MODEL_SIZE,
            device=config.WHISPERX_DEVICE,
            compute_type=compute_type,
        )
        transcription = model.transcribe(str(audio_path),batch_size=1, language="uk")
        language_code = str(transcription.get("language", "")).strip()
        if not language_code:
            raise SubtitleGenerationError("WhisperX did not return a language code.")
        align_model, metadata = whisperx.load_align_model(
            language_code=language_code,
            device=config.WHISPERX_DEVICE,
        )
        aligned_payload = whisperx.align(
            transcription.get("segments", ()),
            align_model,
            metadata,
            str(audio_path),
            config.WHISPERX_DEVICE,
            return_char_alignments=False,
        )
    except SubtitleGenerationError:
        raise
    except Exception as exc:
        LOGGER.error("WhisperX subtitle alignment failed for %s", audio_path)
        raise SubtitleGenerationError(f"WhisperX alignment failed: {exc}") from exc

    return _extract_aligned_words(aligned_payload)


def _resolve_whisperx_compute_type() -> str:
    compute_type = config.WHISPERX_COMPUTE_TYPE.strip()
    if not compute_type:
        raise SubtitleGenerationError("WHISPERX_COMPUTE_TYPE must not be blank.")
    if config.WHISPERX_DEVICE.strip().lower() == "cpu" and compute_type == "float16":
        raise SubtitleGenerationError(
            "WHISPERX_COMPUTE_TYPE=float16 is not supported on CPU. Use int8."
        )
    return compute_type


def _extract_aligned_words(payload: Mapping[str, Any]) -> tuple[AlignedWord, ...]:
    word_entries: list[Mapping[str, Any]] = []
    top_level_words = payload.get("word_segments")
    if isinstance(top_level_words, Sequence) and not isinstance(top_level_words, (str, bytes)):
        word_entries.extend(
            entry for entry in top_level_words if isinstance(entry, Mapping)
        )
    else:
        segments = payload.get("segments")
        if isinstance(segments, Sequence) and not isinstance(segments, (str, bytes)):
            for segment in segments:
                if not isinstance(segment, Mapping):
                    continue
                words = segment.get("words")
                if isinstance(words, Sequence) and not isinstance(words, (str, bytes)):
                    word_entries.extend(entry for entry in words if isinstance(entry, Mapping))

    aligned_words: list[AlignedWord] = []
    previous_word: AlignedWord | None = None
    for entry in word_entries:
        text = " ".join(str(entry.get("word", entry.get("text", ""))).split()).strip()
        if not text:
            raise SubtitleGenerationError("WhisperX returned a word without text.")
        start_seconds = entry.get("start")
        end_seconds = entry.get("end")
        if start_seconds is None or end_seconds is None:
            raise SubtitleGenerationError("WhisperX returned a word without timestamps.")
        try:
            start_value = float(start_seconds)
            end_value = float(end_seconds)
        except (TypeError, ValueError) as exc:
            raise SubtitleGenerationError("WhisperX returned unreadable word timestamps.") from exc
        if end_value <= start_value:
            raise SubtitleGenerationError("WhisperX returned non-increasing word timestamps.")
        current_word = AlignedWord(
            text=text,
            start_seconds=start_value,
            end_seconds=end_value,
        )
        if previous_word is not None:
            if current_word.start_seconds < previous_word.start_seconds:
                raise SubtitleGenerationError("WhisperX returned non-monotonic word start times.")
            if current_word.end_seconds < previous_word.end_seconds:
                raise SubtitleGenerationError("WhisperX returned non-monotonic word end times.")
        aligned_words.append(current_word)
        previous_word = current_word

    return tuple(aligned_words)


def _apply_script_text_to_aligned_words(
    aligned_words: Sequence[AlignedWord],
    script: str,
) -> tuple[AlignedWord, ...]:
    script_tokens = _tokenize_script(script)
    if not script_tokens:
        raise SubtitleGenerationError("Subtitle script is required.")

    canonical_words: list[AlignedWord] = []
    search_index = 0
    for aligned_word in aligned_words:
        aligned_normalized = _normalize_for_comparison(aligned_word.text)
        if not aligned_normalized:
            raise SubtitleGenerationError("WhisperX returned an unusable word for script matching.")

        matched_display = aligned_word.text
        matched_index = _find_matching_script_token(
            script_tokens=script_tokens,
            search_index=search_index,
            aligned_normalized=aligned_normalized,
        )
        if matched_index is not None:
            matched_display = script_tokens[matched_index].display
            search_index = matched_index + 1

        canonical_words.append(
            AlignedWord(
                text=matched_display,
                start_seconds=aligned_word.start_seconds,
                end_seconds=aligned_word.end_seconds,
            )
        )

    if not canonical_words:
        raise SubtitleGenerationError("WhisperX did not return aligned words.")
    return tuple(canonical_words)


def _group_aligned_words(
    aligned_words: Sequence[AlignedWord],
    *,
    max_words_per_phrase: int,
    max_chars_per_phrase: int,
    max_silence_gap_seconds: float,
) -> tuple[SubtitlePhrase, ...]:
    if not aligned_words:
        return ()

    phrases: list[SubtitlePhrase] = []
    current_words = [aligned_words[0]]

    for next_word in aligned_words[1:]:
        if _should_break_before_next_word(
            current_words=current_words,
            next_word=next_word,
            max_words_per_phrase=max_words_per_phrase,
            max_chars_per_phrase=max_chars_per_phrase,
            max_silence_gap_seconds=max_silence_gap_seconds,
        ):
            phrases.append(_build_phrase(current_words))
            current_words = [next_word]
            continue

        current_words.append(next_word)

    if current_words:
        phrases.append(_build_phrase(current_words))

    return tuple(phrases)


def _build_phrase(words: Sequence[AlignedWord]) -> SubtitlePhrase:
    if not words:
        raise SubtitleGenerationError("Subtitle phrase words are required.")
    phrase_text = " ".join(word.text for word in words).strip()
    if not phrase_text:
        raise SubtitleGenerationError("Subtitle phrase text is required.")
    return SubtitlePhrase(
        text=phrase_text,
        start_seconds=words[0].start_seconds,
        end_seconds=words[-1].end_seconds,
    )


def _should_break_before_next_word(
    *,
    current_words: Sequence[AlignedWord],
    next_word: AlignedWord,
    max_words_per_phrase: int,
    max_chars_per_phrase: int,
    max_silence_gap_seconds: float,
) -> bool:
    if not current_words:
        return False
    if _has_pause_gap(current_words[-1], next_word, max_silence_gap_seconds):
        return True
    if _ends_with_strong_punctuation(current_words[-1].text) and len(current_words) >= 2:
        return True
    if _ends_with_weak_punctuation(current_words[-1].text) and len(current_words) >= 3:
        return True
    proposed_words = (*current_words, next_word)
    proposed_text = " ".join(item.text for item in proposed_words)
    return (
        len(proposed_words) > max_words_per_phrase
        or len(proposed_text) > max_chars_per_phrase
    )


def _has_pause_gap(current_word: AlignedWord, next_word: AlignedWord, threshold: float) -> bool:
    return next_word.start_seconds - current_word.end_seconds > threshold


def _ends_with_strong_punctuation(text: str) -> bool:
    stripped_text = text.rstrip()
    return bool(stripped_text) and stripped_text[-1] in ".!?"


def _ends_with_weak_punctuation(text: str) -> bool:
    stripped_text = text.rstrip()
    return bool(stripped_text) and stripped_text[-1] in ",;"


def _find_matching_script_token(
    *,
    script_tokens: Sequence[ScriptToken],
    search_index: int,
    aligned_normalized: str,
) -> int | None:
    for index in range(search_index, len(script_tokens)):
        if script_tokens[index].normalized == aligned_normalized:
            return index
    return None


def _tokenize_script(script: str) -> tuple[ScriptToken, ...]:
    tokens = []
    for raw_token in re.findall(r"\S+", script):
        # Strip stress marks used by TTS engines (+ before stressed vowel).
        # These must not appear in subtitle display text.
        display_raw = unicodedata.normalize("NFKC", raw_token.strip())
        display = display_raw.replace("+", "")
        normalized = _normalize_for_comparison(display)
        if normalized:
            tokens.append(ScriptToken(display=display, normalized=normalized))
    return tuple(tokens)


def _normalize_for_comparison(text: str) -> str:
    normalized_text = unicodedata.normalize("NFKC", text)
    stripped_text = re.sub(r"^[^\w]+|[^\w]+$", "", normalized_text, flags=re.UNICODE)
    return " ".join(stripped_text.casefold().split())


def _write_srt_with_pysubs2(phrases: Sequence[SubtitlePhrase], subtitle_path: Path) -> None:
    try:
        import pysubs2
    except ModuleNotFoundError as exc:
        raise SubtitleGenerationError("pysubs2 is not installed.") from exc

    subs = pysubs2.SSAFile()
    for phrase in phrases:
        subs.events.append(
            pysubs2.SSAEvent(
                start=int(round(phrase.start_seconds * 1000)),
                end=int(round(phrase.end_seconds * 1000)),
                text=phrase.text,
            )
        )

    try:
        subs.save(str(subtitle_path), format_="srt")
    except Exception as exc:
        raise SubtitleGenerationError(f"Failed to save subtitle file: {exc}") from exc
