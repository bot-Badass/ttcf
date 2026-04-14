from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from src import config

LOGGER = logging.getLogger(__name__)


class TranslationError(Exception):
    """Raised when translation fails."""


@dataclass(slots=True)
class TranslationUsage:
    characters_used: int = 0


DeepLClientFactory = Callable[[str, str], Any]
SleepBoundary = Callable[[float], None]


def estimate_characters(text: str) -> int:
    return len(text)


def translate_text_deepl(
    text: str,
    api_key: str | None = config.DEEPL_API_KEY,
    server_url: str = config.DEEPL_SERVER_URL,
    max_chars_per_request: int = config.DEEPL_MAX_CHARS_PER_REQUEST,
    max_retries: int = config.DEEPL_MAX_RETRIES,
    retry_base_delay_seconds: float = config.DEEPL_RETRY_BASE_DELAY_SECONDS,
    usage: TranslationUsage | None = None,
    client_factory: DeepLClientFactory | None = None,
    sleep_func: SleepBoundary | None = None,
) -> str:
    translations = translate_texts_deepl(
        texts=(text,),
        api_key=api_key,
        server_url=server_url,
        max_chars_per_request=max_chars_per_request,
        max_retries=max_retries,
        retry_base_delay_seconds=retry_base_delay_seconds,
        usage=usage,
        client_factory=client_factory,
        sleep_func=sleep_func,
    )
    return translations[0]


def translate_texts_deepl(
    texts: Sequence[str],
    api_key: str | None = config.DEEPL_API_KEY,
    server_url: str = config.DEEPL_SERVER_URL,
    max_chars_per_request: int = config.DEEPL_MAX_CHARS_PER_REQUEST,
    max_retries: int = config.DEEPL_MAX_RETRIES,
    retry_base_delay_seconds: float = config.DEEPL_RETRY_BASE_DELAY_SECONDS,
    usage: TranslationUsage | None = None,
    client_factory: DeepLClientFactory | None = None,
    sleep_func: SleepBoundary | None = None,
) -> list[str]:
    normalized_texts = _normalize_texts(texts)
    _validate_translation_config(
        api_key=api_key,
        max_chars_per_request=max_chars_per_request,
        max_retries=max_retries,
        retry_base_delay_seconds=retry_base_delay_seconds,
    )

    usage_tracker = usage or TranslationUsage()
    client = _build_deepl_client(
        api_key=api_key,
        server_url=server_url,
        client_factory=client_factory,
    )
    batched_segments = _build_segment_batches(
        texts=normalized_texts,
        max_chars_per_request=max_chars_per_request,
    )
    translated_segments_by_index: dict[int, list[str]] = {
        index: []
        for index in range(len(normalized_texts))
    }
    sleeper = sleep_func or (lambda seconds: None)

    for batch in batched_segments:
        source_texts = [segment_text for _, segment_text in batch]
        translated_segments = _translate_batch_with_retry(
            client=client,
            texts=source_texts,
            max_retries=max_retries,
            retry_base_delay_seconds=retry_base_delay_seconds,
            sleep_func=sleeper,
        )

        batch_chars = 0
        for (index, source_text), translated_text in zip(batch, translated_segments, strict=True):
            translated_segments_by_index[index].append(translated_text)
            batch_chars += estimate_characters(source_text)

        usage_tracker.characters_used += batch_chars
        LOGGER.info(
            "DeepL translation batch complete | texts=%s | characters_used=%s | total_characters_used=%s",
            len(batch),
            batch_chars,
            usage_tracker.characters_used,
        )

    return [
        "".join(translated_segments_by_index[index])
        for index in range(len(normalized_texts))
    ]


def translate_to_ukrainian(
    text: str,
    api_key: str | None = config.DEEPL_API_KEY,
    server_url: str = config.DEEPL_SERVER_URL,
    max_chars_per_request: int = config.DEEPL_MAX_CHARS_PER_REQUEST,
    max_retries: int = config.DEEPL_MAX_RETRIES,
    retry_base_delay_seconds: float = config.DEEPL_RETRY_BASE_DELAY_SECONDS,
    usage: TranslationUsage | None = None,
    client_factory: DeepLClientFactory | None = None,
    sleep_func: SleepBoundary | None = None,
) -> str:
    return translate_text_deepl(
        text=text,
        api_key=api_key,
        server_url=server_url,
        max_chars_per_request=max_chars_per_request,
        max_retries=max_retries,
        retry_base_delay_seconds=retry_base_delay_seconds,
        usage=usage,
        client_factory=client_factory,
        sleep_func=sleep_func,
    )


def _normalize_texts(texts: Sequence[str]) -> list[str]:
    if not texts:
        raise TranslationError("Text to translate is required.")

    normalized_texts: list[str] = []
    for text in texts:
        stripped_text = text.strip()
        if not stripped_text:
            raise TranslationError("Text to translate is required.")
        normalized_texts.append(stripped_text)

    return normalized_texts


def _validate_translation_config(
    *,
    api_key: str | None,
    max_chars_per_request: int,
    max_retries: int,
    retry_base_delay_seconds: float,
) -> None:
    if not api_key:
        raise TranslationError("DeepL API key is not configured.")
    if max_chars_per_request <= 0:
        raise TranslationError("DEEPL_MAX_CHARS_PER_REQUEST must be greater than zero.")
    if max_retries <= 0:
        raise TranslationError("DEEPL_MAX_RETRIES must be greater than zero.")
    if retry_base_delay_seconds < 0:
        raise TranslationError("DEEPL_RETRY_BASE_DELAY_SECONDS must not be negative.")


def _build_deepl_client(
    *,
    api_key: str | None,
    server_url: str,
    client_factory: DeepLClientFactory | None,
) -> Any:
    if api_key is None:
        raise TranslationError("DeepL API key is not configured.")

    if client_factory is not None:
        return client_factory(api_key, server_url)

    try:
        deepl_module = importlib.import_module("deepl")
    except ImportError as exc:
        raise TranslationError("deepl package is not installed.") from exc

    client_class = getattr(deepl_module, "Translator", None) or getattr(
        deepl_module, "DeepLClient", None
    )
    if client_class is None:
        raise TranslationError("deepl package does not expose a translation client.")

    try:
        return client_class(api_key, server_url=server_url)
    except TypeError:
        return client_class(api_key)


def _build_segment_batches(
    *,
    texts: Sequence[str],
    max_chars_per_request: int,
) -> list[list[tuple[int, str]]]:
    pending_segments: list[tuple[int, str]] = []
    for index, text in enumerate(texts):
        pending_segments.extend(
            (index, segment)
            for segment in _split_text_into_chunks(text, max_chars_per_request)
        )

    batches: list[list[tuple[int, str]]] = []
    current_batch: list[tuple[int, str]] = []
    current_batch_chars = 0

    for item in pending_segments:
        _, segment_text = item
        segment_chars = estimate_characters(segment_text)
        if current_batch and current_batch_chars + segment_chars > max_chars_per_request:
            batches.append(current_batch)
            current_batch = []
            current_batch_chars = 0

        current_batch.append(item)
        current_batch_chars += segment_chars

    if current_batch:
        batches.append(current_batch)

    return batches


def _split_text_into_chunks(text: str, max_chars_per_request: int) -> list[str]:
    if estimate_characters(text) <= max_chars_per_request:
        return [text]

    chunks: list[str] = []
    remaining_text = text
    while remaining_text:
        chunk = remaining_text[:max_chars_per_request]
        if len(remaining_text) > max_chars_per_request:
            split_index = chunk.rfind(" ")
            if split_index > 0:
                chunk = chunk[:split_index]
            # If there is no usable space boundary, keep the full character slice.
            # Python string slicing is Unicode-aware, so this does not split bytes.

        chunk = chunk.strip()
        if not chunk:
            chunk = remaining_text[:max_chars_per_request]

        chunks.append(chunk)
        remaining_text = remaining_text[len(chunk):].lstrip()

    return chunks


def _translate_batch_with_retry(
    *,
    client: Any,
    texts: Sequence[str],
    max_retries: int,
    retry_base_delay_seconds: float,
    sleep_func: SleepBoundary,
) -> list[str]:
    last_error: Exception | None = None

    for attempt in range(max_retries):
        try:
            raw_result = client.translate_text(
                list(texts),
                target_lang="UK",
            )
            return _normalize_translation_result(raw_result, expected_count=len(texts))
        except Exception as exc:
            last_error = exc
            if not _is_retryable_error(exc) or attempt == max_retries - 1:
                raise _map_translation_error(exc) from exc

            delay_seconds = retry_base_delay_seconds * (2 ** attempt)
            LOGGER.warning(
                "Retrying DeepL translation | attempt=%s | delay_seconds=%s | error=%s",
                attempt + 1,
                delay_seconds,
                exc,
            )
            sleep_func(delay_seconds)

    if last_error is not None:
        raise _map_translation_error(last_error) from last_error

    raise TranslationError("DeepL translation failed unexpectedly.")


def _normalize_translation_result(raw_result: Any, expected_count: int) -> list[str]:
    if isinstance(raw_result, Sequence) and not isinstance(raw_result, (str, bytes, bytearray)):
        results = list(raw_result)
    else:
        results = [raw_result]

    if len(results) != expected_count:
        raise TranslationError("Unexpected DeepL translation response.")

    translated_texts: list[str] = []
    for result in results:
        translated_text = str(getattr(result, "text", "")).strip()
        if not translated_text:
            raise TranslationError("DeepL translation response was empty.")
        translated_texts.append(translated_text)

    return translated_texts


def _is_retryable_error(exc: Exception) -> bool:
    if isinstance(exc, OSError):
        return True

    status_code = getattr(exc, "status_code", None)
    if status_code == 429:
        return True

    return exc.__class__.__name__ in {
        "TooManyRequestsException",
        "ConnectionException",
    }


def _map_translation_error(exc: Exception) -> TranslationError:
    if isinstance(exc, TranslationError):
        return exc
    if isinstance(exc, OSError):
        return TranslationError("DeepL network error.")

    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        return TranslationError(f"DeepL API error: HTTP {status_code}")

    return TranslationError(f"DeepL API error: {exc}")
