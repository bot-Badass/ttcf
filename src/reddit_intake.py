from __future__ import annotations

import inspect
import json
import logging
import subprocess
import tempfile
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Mapping, Protocol, Sequence
from urllib.parse import urlencode
import urllib.request

from respeecher import Respeecher, RespeecherEnvironment

from src import config
from src.content_meta import build_content_meta, ensure_content_meta_file
from src.processor import PROCESSED_STATUS, ProcessorResult
from src.publisher import (
    PublishQueueError,
    PublishQueueResult,
    PublishQueueStore,
    build_publish_candidate,
)
from src.render import RenderError, render_story_video
from src.subtitles import SubtitleGenerationError, generate_subtitle_file
from src.translator import TranslationError, estimate_characters, translate_text_deepl
from src.utils import compute_sha256
LOGGER = logging.getLogger(__name__)

CANDIDATE_ACCEPTED_STATUS: Final[str] = "accepted"
CANDIDATE_REJECTED_STATUS: Final[str] = "rejected"
CANDIDATE_FAILED_STATUS: Final[str] = "failed"
PENDING_REVIEW_STATUS: Final[str] = "pending_review"
APPROVED_STATUS: Final[str] = "approved"
REJECTED_STATUS: Final[str] = "rejected"
RESPEECHER_PROVIDER: Final[str] = "respeecher"
REDDIT_FETCH_LISTINGS: Final[tuple[str, ...]] = ("hot", "top")
PROCESS_CALLBACK_PREFIX: Final[str] = "process:"
UKRAINIAN_SCRIPT_PREFIX: Final[str] = "[UK]"
PLACEHOLDER_VOICE_IDS: Final[frozenset[str]] = frozenset(
    {
        "<voice_id>",
        "voice_id",
        "your_voice_id",
        "your-voice-id",
        "replace-me",
        "changeme",
        "todo",
        "default",
        "test",
    }
)


class RedditIntakeError(Exception):
    """Base exception for Reddit intake failures."""


class StoryReviewTelegramSendError(RedditIntakeError):
    """Raised when Telegram send fails after one or more reviews were already sent."""

    def __init__(self, message: str, sent_story_ids: Sequence[str] = ()) -> None:
        super().__init__(message)
        self.sent_story_ids = tuple(sent_story_ids)


@dataclass(frozen=True, slots=True)
class StoryCandidate:
    post_id: str
    subreddit: str
    title: str
    body: str
    score: int
    url: str
    created_utc: float


@dataclass(frozen=True, slots=True)
class CandidatePreparationResult:
    post_id: str | None
    status: str
    reason: str
    candidate: StoryCandidate | None


@dataclass(frozen=True, slots=True)
class CandidateBatchResult:
    results: tuple[CandidatePreparationResult, ...]


@dataclass(frozen=True, slots=True)
class PipelineRunResult:
    fetched: int
    accepted: int
    persisted: int
    sent_to_telegram: int
    processed_updates: int
    translated: int
    rendered: int
    enqueued: int
    stage_errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class StoryIntakeStage:
    story_id: str
    source_title: str
    source_body: str
    subreddit: str
    url: str
    score: int
    created_utc: float


@dataclass(frozen=True, slots=True)
class StoryModerationStage:
    selected_for_processing: bool
    operator_rewritten_text: str | None
    script_saved_at: str | None


@dataclass(frozen=True, slots=True)
class StoryProductionStage:
    translated_script_uk: str | None
    translation_characters_used: int | None = None


@dataclass(frozen=True, slots=True)
class TelegramReviewMetadata:
    chat_id: str | None
    instruction_message_id: int | None


@dataclass(frozen=True, slots=True)
class StoredStoryReview:
    intake: StoryIntakeStage
    moderation: StoryModerationStage
    production: StoryProductionStage
    telegram: TelegramReviewMetadata
    status: str
    sent_to_telegram: bool
    saved_at: str

    @property
    def story_id(self) -> str:
        return self.intake.story_id

    @property
    def source_title(self) -> str:
        return self.intake.source_title

    @property
    def source_body(self) -> str:
        return self.intake.source_body

    @property
    def subreddit(self) -> str:
        return self.intake.subreddit

    @property
    def url(self) -> str:
        return self.intake.url

    @property
    def score(self) -> int:
        return self.intake.score

    @property
    def created_utc(self) -> float:
        return self.intake.created_utc

    @property
    def selected_for_processing(self) -> bool:
        return self.moderation.selected_for_processing

    @property
    def operator_rewritten_text(self) -> str | None:
        return self.moderation.operator_rewritten_text

    @property
    def script_saved_at(self) -> str | None:
        return self.moderation.script_saved_at

    @property
    def translated_script_uk(self) -> str | None:
        return self.production.translated_script_uk

    @property
    def translation_characters_used(self) -> int | None:
        return self.production.translation_characters_used


@dataclass(frozen=True, slots=True)
class StoryReviewStats:
    total: int
    pending_review: int
    approved: int
    rejected: int
    sent_to_telegram: int
    eligible_to_send: int


@dataclass(frozen=True, slots=True)
class ApprovedStoryPackage:
    story_id: str
    source_title: str
    adapted_script: str
    tts_provider: str
    voice_id: str
    background_video_path: Path
    output_basename: str


@dataclass(frozen=True, slots=True)
class RenderedInboxAsset:
    story_id: str
    bundle_dir: Path
    output_path: Path
    metadata_path: Path | None
    audio_path: Path
    subtitle_path: Path | None


FetchBoundary = Callable[[], Iterable[StoryCandidate | Mapping[str, Any]]]
HttpPostBoundary = Callable[[str, bytes, Mapping[str, str]], bytes]
RedditJsonGetBoundary = Callable[[str, Mapping[str, str]], Mapping[str, Any]]
TelegramInstructionBoundary = Callable[[str], int | None]
class TtsBoundary(Protocol):
    def __call__(
        self,
        script: str,
        provider_name: str,
        voice_id: str,
        output_dir: Path,
        *,
        pre_resolved_voice_id: str | None = None,
    ) -> Path: ...


SubtitleBoundary = Callable[[Path, str, Path], Path]
RenderBoundary = Callable[[Path, Path, Path, Path | None], Path]
RespeecherClientFactory = Callable[[str], Any]
TranslateBoundary = Callable[[str], tuple[str, int]]

_UNSET: Final[object] = object()


def fetch_live_story_candidates(
    subreddits: Sequence[str] = config.REDDIT_ALLOWED_SUBREDDITS,
    limit_per_listing: int = config.REDDIT_FETCH_LIMIT_PER_LISTING,
    listings: Sequence[str] = REDDIT_FETCH_LISTINGS,
    fetch_json: RedditJsonGetBoundary | None = None,
) -> tuple[StoryCandidate, ...]:
    json_get = fetch_json or _default_reddit_json_get
    headers = {"User-Agent": config.REDDIT_USER_AGENT}
    candidates_by_id: dict[str, StoryCandidate] = {}

    for subreddit in subreddits:
        for listing in listings:
            url = _build_reddit_listing_url(
                subreddit=subreddit,
                listing=listing,
                limit=limit_per_listing,
            )
            try:
                payload = json_get(url, headers)
            except OSError as exc:
                LOGGER.error("Failed to fetch Reddit listing for r/%s (%s)", subreddit, listing)
                raise RedditIntakeError(
                    f"Failed to fetch Reddit listing: r/{subreddit} {listing}"
                ) from exc

            for candidate in _normalize_reddit_listing(payload, subreddit=subreddit):
                candidates_by_id.setdefault(candidate.post_id, candidate)

    return tuple(candidates_by_id.values())


def prepare_story_candidates(
    fetch_candidates: FetchBoundary,
    allowed_subreddits: Sequence[str] = config.REDDIT_ALLOWED_SUBREDDITS,
    minimum_score: int = config.REDDIT_MIN_SCORE,
    minimum_combined_length: int = config.REDDIT_MIN_COMBINED_TEXT_LENGTH,
) -> CandidateBatchResult:
    return CandidateBatchResult(
        results=tuple(
            _process_single_candidate(
                raw_candidate=raw_candidate,
                allowed_subreddits=allowed_subreddits,
                minimum_score=minimum_score,
                minimum_combined_length=minimum_combined_length,
            )
            for raw_candidate in fetch_candidates()
        )
    )


def filter_story_candidate(
    candidate: StoryCandidate,
    allowed_subreddits: Sequence[str] = config.REDDIT_ALLOWED_SUBREDDITS,
    minimum_score: int = config.REDDIT_MIN_SCORE,
    minimum_combined_length: int = config.REDDIT_MIN_COMBINED_TEXT_LENGTH,
) -> tuple[bool, str | None]:
    if candidate.subreddit not in allowed_subreddits:
        return False, f"Subreddit is not allowed: {candidate.subreddit}"

    if candidate.score < minimum_score:
        return False, f"Score is below threshold: {candidate.score}"

    if not candidate.body.strip():
        return False, "Body is required."

    combined_length = len(candidate.title.strip()) + len(candidate.body.strip())
    if combined_length < minimum_combined_length:
        return False, f"Combined title/body length is below threshold: {combined_length}"

    return True, None


def format_telegram_approval_message(
    candidate: StoryCandidate,
    excerpt_length: int = config.REDDIT_APPROVAL_EXCERPT_LENGTH,
) -> str:
    excerpt = _build_excerpt(candidate.body, excerpt_length=excerpt_length)
    return "\n".join(
        (
            f"Candidate ID: {candidate.post_id}",
            f"Subreddit: r/{candidate.subreddit}",
            f"Score: {candidate.score}",
            f"Title: {candidate.title}",
            f"Excerpt: {excerpt}",
            f"Source URL: {candidate.url}",
        )
    )


def build_process_callback_data(story_id: str) -> str:
    normalized_story_id = story_id.strip()
    if not normalized_story_id:
        raise RedditIntakeError("story_id is required for Telegram process callback.")
    return f"{PROCESS_CALLBACK_PREFIX}{normalized_story_id}"


def send_telegram_approval_message(
    message: str,
    bot_token: str | None = None,
    chat_id: str | None = None,
    reply_markup: Mapping[str, Any] | None = None,
    http_post: HttpPostBoundary | None = None,
) -> Mapping[str, Any]:
    resolved_bot_token = bot_token or config.TELEGRAM_BOT_TOKEN
    resolved_chat_id = chat_id or config.TELEGRAM_CHAT_ID

    if not resolved_bot_token:
        raise RedditIntakeError("Telegram bot token is not configured.")

    if not resolved_chat_id:
        raise RedditIntakeError("Telegram chat id is not configured.")

    request_payload: dict[str, Any] = {"chat_id": resolved_chat_id, "text": message}
    if reply_markup is not None:
        request_payload["reply_markup"] = reply_markup

    payload = json.dumps(request_payload).encode("utf-8")
    headers = {"Content-Type": "application/json; charset=utf-8"}
    sender = http_post or _default_http_post

    try:
        response_bytes = sender(
            f"https://api.telegram.org/bot{resolved_bot_token}/sendMessage",
            payload,
            headers,
        )
    except OSError as exc:
        LOGGER.error("Failed to send Telegram approval message.")
        raise RedditIntakeError("Failed to send Telegram approval message.") from exc

    try:
        response_payload = json.loads(response_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RedditIntakeError("Unexpected Telegram response payload.") from exc

    if not isinstance(response_payload, Mapping):
        raise RedditIntakeError("Unexpected Telegram response payload.")

    result_payload = response_payload.get("result")
    if not isinstance(result_payload, Mapping):
        raise RedditIntakeError("Unexpected Telegram response payload.")

    return result_payload


def send_story_reviews_to_telegram(
    reviews: Sequence[StoredStoryReview],
    http_post: HttpPostBoundary | None = None,
) -> tuple[str, ...]:
    sent_story_ids: list[str] = []

    for review in reviews:
        try:
            send_telegram_approval_message(
                message=_build_story_review_telegram_message(review),
                reply_markup=_build_process_reply_markup(review.story_id),
                http_post=http_post,
            )
        except RedditIntakeError as exc:
            raise StoryReviewTelegramSendError(str(exc), sent_story_ids) from exc
        sent_story_ids.append(review.story_id)

    return tuple(sent_story_ids)


def _default_translate_adapter(text: str) -> tuple[str, int]:
    normalized_text = text.strip()
    translated_text = translate_text_deepl(normalized_text)
    return translated_text, estimate_characters(normalized_text)


def _translate_with_usage(
    story_id: str,
    text: str,
    *,
    translate: TranslateBoundary,
) -> tuple[str, int]:
    try:
        translation_result = translate(text)
    except TranslationError as exc:
        raise RedditIntakeError(str(exc)) from exc

    if not isinstance(translation_result, tuple) or len(translation_result) != 2:
        raise RedditIntakeError(
            "Translate boundary must return a (translated_text, chars_used) tuple."
        )

    translated_text, chars_used = translation_result
    if not isinstance(translated_text, str) or not translated_text.strip():
        raise RedditIntakeError("Translated script is empty.")
    if not isinstance(chars_used, int):
        raise RedditIntakeError(
            "Translate boundary must return a (translated_text, chars_used) tuple."
        )
    if chars_used < 0:
        raise RedditIntakeError("Translated character usage must be non-negative.")

    LOGGER.info(
        "Translation completed: story_id=%s chars=%d length=%d",
        story_id,
        chars_used,
        len(translated_text),
    )
    return translated_text, chars_used


def send_telegram_instruction_message(
    message: str,
    bot_token: str | None = None,
    chat_id: str | None = None,
    http_post: HttpPostBoundary | None = None,
) -> int | None:
    result_payload = send_telegram_approval_message(
        message=message,
        bot_token=bot_token,
        chat_id=chat_id,
        http_post=http_post,
    )
    message_id = result_payload.get("message_id")
    if message_id is None:
        return None

    try:
        return int(message_id)
    except (TypeError, ValueError) as exc:
        raise RedditIntakeError("Unexpected Telegram message_id in response.") from exc


def validate_story_review_state(record: StoredStoryReview) -> None:
    if record.operator_rewritten_text and not record.selected_for_processing:
        raise RedditIntakeError(
            f"Invalid story review state for {record.story_id}: rewritten text requires selected_for_processing=True"
        )

    if record.translated_script_uk and not record.operator_rewritten_text:
        raise RedditIntakeError(
            f"Invalid story review state for {record.story_id}: translated_script_uk requires operator_rewritten_text"
        )
    if (
        record.translation_characters_used is not None
        and not record.translated_script_uk
    ):
        raise RedditIntakeError(
            f"Invalid story review state for {record.story_id}: translation_characters_used requires translated_script_uk"
        )


def save_story_candidates_for_review(
    candidates: Sequence[StoryCandidate],
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
) -> tuple[StoredStoryReview, ...]:
    saved_records: list[StoredStoryReview] = []
    existing_records = {record.story_id: record for record in list_story_reviews(store_path=store_path)}

    for candidate in candidates:
        existing_record = existing_records.get(candidate.post_id)
        status = (
            existing_record.status
            if existing_record is not None
            and existing_record.status in {APPROVED_STATUS, REJECTED_STATUS}
            else PENDING_REVIEW_STATUS
        )
        sent_to_telegram = (
            existing_record.sent_to_telegram
            if existing_record is not None
            else False
        )
        selected_for_processing = (
            existing_record.selected_for_processing
            if existing_record is not None
            else False
        )
        telegram = (
            existing_record.telegram
            if existing_record is not None
            else TelegramReviewMetadata(chat_id=None, instruction_message_id=None)
        )
        operator_rewritten_text = (
            existing_record.operator_rewritten_text
            if existing_record is not None
            else None
        )
        translated_script_uk = (
            existing_record.translated_script_uk
            if existing_record is not None
            else None
        )
        translation_characters_used = (
            existing_record.translation_characters_used
            if existing_record is not None
            else None
        )
        script_saved_at = (
            existing_record.script_saved_at
            if existing_record is not None
            else None
        )
        record = StoredStoryReview(
            intake=StoryIntakeStage(
                story_id=candidate.post_id,
                source_title=candidate.title,
                source_body=candidate.body,
                subreddit=candidate.subreddit,
                url=candidate.url,
                score=candidate.score,
                created_utc=candidate.created_utc,
            ),
            moderation=StoryModerationStage(
                selected_for_processing=selected_for_processing,
                operator_rewritten_text=operator_rewritten_text,
                script_saved_at=script_saved_at,
            ),
            production=StoryProductionStage(
                translated_script_uk=translated_script_uk,
                translation_characters_used=translation_characters_used,
            ),
            telegram=telegram,
            status=status,
            sent_to_telegram=sent_to_telegram,
            saved_at=_utc_timestamp_now(),
        )
        existing_records[record.story_id] = record
        saved_records.append(record)

    _write_story_reviews(store_path, tuple(existing_records.values()))
    return tuple(saved_records)


def list_story_reviews(
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    status: str | None = None,
    sent_to_telegram: bool | None = None,
) -> tuple[StoredStoryReview, ...]:
    records = _load_story_reviews(store_path)
    return tuple(
        record
        for record in records
        if (status is None or record.status == status)
        and (
            sent_to_telegram is None
            or record.sent_to_telegram is sent_to_telegram
        )
    )


def get_story_review_stats(
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
) -> StoryReviewStats:
    records = _load_story_reviews(store_path)
    pending_review_count = _safe_sum(
        record.status == PENDING_REVIEW_STATUS for record in records
    )
    approved_count = _safe_sum(record.status == APPROVED_STATUS for record in records)
    rejected_count = _safe_sum(record.status == REJECTED_STATUS for record in records)
    sent_count = _safe_sum(record.sent_to_telegram for record in records)
    eligible_count = _safe_sum(
        record.status == PENDING_REVIEW_STATUS and not record.sent_to_telegram
        for record in records
    )

    return StoryReviewStats(
        total=len(records),
        pending_review=pending_review_count,
        approved=approved_count,
        rejected=rejected_count,
        sent_to_telegram=sent_count,
        eligible_to_send=eligible_count,
    )


def get_story_review(
    story_id: str,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
) -> StoredStoryReview | None:
    for record in _load_story_reviews(store_path):
        if record.story_id == story_id:
            return record
    return None


def update_story_review_status(
    story_id: str,
    status: str,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
) -> StoredStoryReview:
    _validate_review_status(status)
    return _update_story_review_record(
        story_id=story_id,
        store_path=store_path,
        updater=lambda record: _replace_story_review(
            record,
            status=status,
            sent_to_telegram=False,
            saved_at=_utc_timestamp_now(),
        ),
    )


def send_pending_story_reviews_to_telegram(
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
) -> tuple[str, ...]:
    records = list(_load_story_reviews(store_path))
    eligible_reviews = tuple(
        record
        for record in records
        if record.status == PENDING_REVIEW_STATUS and not record.sent_to_telegram
    )
    if not eligible_reviews:
        return ()

    try:
        sent_story_ids = send_story_reviews_to_telegram(eligible_reviews)
    except StoryReviewTelegramSendError as exc:
        sent_story_ids = exc.sent_story_ids
        if sent_story_ids:
            _write_story_reviews(
                store_path,
                _mark_story_reviews_as_sent(records, sent_story_ids),
            )
        raise

    _write_story_reviews(
        store_path,
        _mark_story_reviews_as_sent(records, sent_story_ids),
    )
    return sent_story_ids


def handle_telegram_process_callback(
    callback_data: str,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    chat_id: str | None = None,
    send_instruction_message: TelegramInstructionBoundary | None = None,
) -> StoredStoryReview:
    story_id = _parse_process_callback_data(callback_data)
    sender = send_instruction_message or (
        lambda message: send_telegram_instruction_message(message=message, chat_id=chat_id)
    )

    record = get_story_review(story_id=story_id, store_path=store_path)
    if record is None:
        raise RedditIntakeError(f"Story review not found: {story_id}")

    updated_record = mark_story_selected_for_processing(
        story_id=record.story_id,
        store_path=store_path,
        chat_id=(chat_id or record.telegram.chat_id),
    )

    try:
        instruction_message_id = sender(
            "\n".join(
                (
                    f"Story selected for processing: {record.story_id}",
                    "Reply to this message with the rewritten script.",
                    "To send an English script for auto-translation: reply with plain text.",
                    "To send a ready Ukrainian script (no translation): start with [UK]",
                    "Example: [UK] Одного дня я зрозумів, що все змінилось...",
                )
            )
        )
    except RedditIntakeError as exc:
        LOGGER.error(
            "Telegram instruction send failed after selection for story %s: %s",
            record.story_id,
            exc,
        )
        return updated_record

    return mark_story_selected_for_processing(
        story_id=record.story_id,
        store_path=store_path,
        chat_id=(chat_id or record.telegram.chat_id),
        instruction_message_id=instruction_message_id,
    )


def mark_story_selected_for_processing(
    story_id: str,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    chat_id: str | None = None,
    instruction_message_id: int | None = None,
) -> StoredStoryReview:
    normalized_story_id = story_id.strip()
    if not normalized_story_id:
        raise RedditIntakeError("story_id is required.")
    return _update_story_review_record(
        story_id=normalized_story_id,
        store_path=store_path,
        updater=lambda record: _replace_story_review(
            record,
            selected_for_processing=True,
            telegram=_replace_telegram_review_metadata(
                record.telegram,
                chat_id=chat_id if chat_id is not None else _UNSET,
                instruction_message_id=(
                    instruction_message_id
                    if instruction_message_id is not None
                    else _UNSET
                ),
            ),
            saved_at=_utc_timestamp_now(),
        ),
    )


def save_operator_rewrite(
    story_id: str,
    text: str,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
) -> StoredStoryReview:
    normalized_story_id = story_id.strip()
    normalized_text = text.strip()
    if not normalized_story_id:
        raise RedditIntakeError("story_id is required.")
    if not normalized_text:
        raise RedditIntakeError("rewritten_text is required.")

    def _updater(record: StoredStoryReview) -> StoredStoryReview:
        if not record.selected_for_processing:
            raise RedditIntakeError(
                f"Story must be selected for processing before saving a script: {normalized_story_id}"
            )
        if record.operator_rewritten_text:
            raise RedditIntakeError(
                f"Rewritten script already exists for story: {normalized_story_id}"
            )
        return _replace_story_review(
            record,
            operator_rewritten_text=normalized_text,
            script_saved_at=_utc_timestamp_now(),
            saved_at=_utc_timestamp_now(),
        )

    return _update_story_review_record(
        story_id=normalized_story_id,
        store_path=store_path,
        updater=_updater,
    )


def set_translated_script(
    story_id: str,
    translated_text: str,
    translation_characters_used: int | None = None,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
) -> StoredStoryReview:
    normalized_story_id = story_id.strip()
    normalized_text = translated_text.strip()
    if not normalized_story_id:
        raise RedditIntakeError("story_id is required.")
    if not normalized_text:
        raise RedditIntakeError("translated_text is required.")
    if translation_characters_used is not None and translation_characters_used < 0:
        raise RedditIntakeError("translation_characters_used must be non-negative.")

    def _updater(record: StoredStoryReview) -> StoredStoryReview:
        if not record.operator_rewritten_text:
            raise RedditIntakeError(
                f"Story must have rewritten text before saving a translated script: {normalized_story_id}"
            )
        if record.translated_script_uk:
            raise RedditIntakeError(
                f"Translated script already exists for story: {normalized_story_id}"
            )
        return _replace_story_review(
            record,
            translated_script_uk=normalized_text,
            translation_characters_used=translation_characters_used,
            saved_at=_utc_timestamp_now(),
        )

    return _update_story_review_record(
        story_id=normalized_story_id,
        store_path=store_path,
        updater=_updater,
    )


def save_operator_rewritten_text(
    story_id: str,
    rewritten_text: str,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
) -> StoredStoryReview:
    return save_operator_rewrite(
        story_id=story_id,
        text=rewritten_text,
        store_path=store_path,
    )


def translate_story_review_to_ukrainian(
    story_id: str,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    translator: TranslateBoundary = _default_translate_adapter,
) -> StoredStoryReview:
    normalized_story_id = story_id.strip()
    if not normalized_story_id:
        raise RedditIntakeError("story_id is required.")

    review = get_story_review(
        story_id=normalized_story_id,
        store_path=store_path,
    )
    if review is None:
        raise RedditIntakeError(f"Story review not found: {normalized_story_id}")
    if not review.selected_for_processing:
        raise RedditIntakeError(
            f"Story must be selected for processing before translation: {normalized_story_id}"
        )
    if not review.operator_rewritten_text:
        raise RedditIntakeError(
            f"Story must have rewritten text before translation: {normalized_story_id}"
        )
    if review.translated_script_uk:
        raise RedditIntakeError(
            f"Translated script already exists for story: {normalized_story_id}"
        )

    translated_text, chars_used = _translate_with_usage(
        normalized_story_id,
        review.operator_rewritten_text,
        translate=translator,
    )

    return set_translated_script(
        story_id=normalized_story_id,
        translated_text=translated_text,
        translation_characters_used=chars_used,
        store_path=store_path,
    )


def translate_story_to_ukrainian(
    story_id: str,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    translator: TranslateBoundary = _default_translate_adapter,
) -> StoredStoryReview:
    return translate_story_review_to_ukrainian(
        story_id=story_id,
        store_path=store_path,
        translator=translator,
    )


# Translation-stage boundary kept isolated from intake/moderation transitions.


def is_story_ready_for_render(review: StoredStoryReview) -> bool:
    return (
        review.status == APPROVED_STATUS
        and review.selected_for_processing
        and bool(review.operator_rewritten_text)
        and bool(review.translated_script_uk)
    )


def process_operator_reply_and_translate(
    story_id: str,
    operator_text: str,
    *,
    translate: TranslateBoundary = _default_translate_adapter,
    store_path: Path | None = None,
) -> StoredStoryReview:
    resolved_store_path = store_path or config.REDDIT_REVIEW_STORE_PATH
    rewritten_review = save_operator_rewritten_text(
        story_id=story_id,
        rewritten_text=operator_text,
        store_path=resolved_store_path,
    )

    translated_text, chars_used = _translate_with_usage(
        rewritten_review.story_id,
        rewritten_review.operator_rewritten_text or "",
        translate=translate,
    )

    return set_translated_script(
        story_id=rewritten_review.story_id,
        translated_text=translated_text,
        translation_characters_used=chars_used,
        store_path=resolved_store_path,
    )


def bypass_translation_with_ukrainian_script(
    story_id: str,
    ukrainian_text: str,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
) -> StoredStoryReview:
    normalized_story_id = story_id.strip()
    normalized_text = ukrainian_text.strip()
    if not normalized_story_id:
        raise RedditIntakeError("story_id is required.")
    if not normalized_text:
        raise RedditIntakeError("Ukrainian script body is required after [UK] prefix.")

    def _updater(record: StoredStoryReview) -> StoredStoryReview:
        if not record.selected_for_processing:
            raise RedditIntakeError(
                f"Story must be selected for processing before saving a script: {normalized_story_id}"
            )
        if record.translated_script_uk:
            raise RedditIntakeError(
                f"Translated script already exists for story: {normalized_story_id}"
            )
        if record.operator_rewritten_text:
            raise RedditIntakeError(
                f"Rewritten script already exists for story: {normalized_story_id}"
            )

        timestamp = _utc_timestamp_now()
        return _replace_story_review(
            record,
            operator_rewritten_text=normalized_text,
            script_saved_at=timestamp,
            translated_script_uk=normalized_text,
            translation_characters_used=None,
            saved_at=timestamp,
        )

    updated_review = _update_story_review_record(
        story_id=normalized_story_id,
        store_path=store_path,
        updater=_updater,
    )
    LOGGER.info(
        "Ukrainian script bypassed translation: story_id=%s length=%d",
        normalized_story_id,
        len(normalized_text),
    )
    return updated_review


def process_telegram_update(
    update_payload: Mapping[str, Any],
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    send_instruction_message: TelegramInstructionBoundary | None = None,
    translate: TranslateBoundary = _default_translate_adapter,
) -> StoredStoryReview:
    callback_query = update_payload.get("callback_query")
    if isinstance(callback_query, Mapping):
        callback_data = str(callback_query.get("data", "")).strip()
        message_payload = callback_query.get("message")
        chat_id = _extract_chat_id(message_payload) if isinstance(message_payload, Mapping) else None
        return handle_telegram_process_callback(
            callback_data=callback_data,
            store_path=store_path,
            chat_id=chat_id,
            send_instruction_message=send_instruction_message,
        )

    message_payload = update_payload.get("message")
    if isinstance(message_payload, Mapping):
        return ingest_telegram_rewrite_reply(
            message_payload=message_payload,
            store_path=store_path,
            translate=translate,
        )

    raise RedditIntakeError("Unsupported Telegram update payload.")


def ingest_telegram_rewrite_reply(
    message_payload: Mapping[str, Any],
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    translate: TranslateBoundary = _default_translate_adapter,
) -> StoredStoryReview:
    rewritten_text = str(message_payload.get("text", "")).strip()
    if not rewritten_text:
        raise RedditIntakeError("Telegram rewritten script reply is empty.")

    chat_id = _extract_chat_id(message_payload)
    reply_to_message = message_payload.get("reply_to_message")
    if not isinstance(reply_to_message, Mapping):
        raise RedditIntakeError("Telegram reply cannot be mapped to a story.")

    reply_message_id = reply_to_message.get("message_id")
    try:
        normalized_reply_message_id = int(reply_message_id)
    except (TypeError, ValueError) as exc:
        raise RedditIntakeError("Telegram reply cannot be mapped to a story.") from exc

    for record in _load_story_reviews(store_path):
        if (
            record.telegram.instruction_message_id != normalized_reply_message_id
            or record.telegram.chat_id != chat_id
        ):
            continue

        if record.operator_rewritten_text:
            raise RedditIntakeError(
                f"Rewritten script already exists for story: {record.story_id}"
            )

        if rewritten_text.startswith(UKRAINIAN_SCRIPT_PREFIX):
            ukrainian_text = rewritten_text[len(UKRAINIAN_SCRIPT_PREFIX) :].strip()
            if not ukrainian_text:
                raise RedditIntakeError("Ukrainian script body is required after [UK] prefix.")
            return bypass_translation_with_ukrainian_script(
                story_id=record.story_id,
                ukrainian_text=ukrainian_text,
                store_path=store_path,
            )

        return process_operator_reply_and_translate(
            story_id=record.story_id,
            operator_text=rewritten_text,
            translate=translate,
            store_path=store_path,
        )

    raise RedditIntakeError("Telegram reply cannot be mapped to a story.")


def build_approved_story_package(
    story_id: str,
    source_title: str,
    adapted_script: str,
    tts_provider: str,
    voice_id: str,
    background_video_path: Path = config.REDDIT_BACKGROUND_VIDEO_TEMPLATE_PATH,
    output_basename: str | None = None,
) -> ApprovedStoryPackage:
    package = ApprovedStoryPackage(
        story_id=story_id.strip(),
        source_title=source_title.strip(),
        adapted_script=adapted_script.strip(),
        tts_provider=tts_provider.strip(),
        voice_id=voice_id.strip(),
        background_video_path=background_video_path,
        output_basename=(output_basename or story_id).strip(),
    )
    validate_approved_story_package(package)
    return package


def validate_approved_story_package(package: ApprovedStoryPackage) -> None:
    if not package.story_id:
        raise RedditIntakeError("story_id is required.")

    if not package.source_title:
        raise RedditIntakeError("source_title is required.")

    if not package.adapted_script:
        raise RedditIntakeError("adapted_script is required.")

    if package.tts_provider != RESPEECHER_PROVIDER:
        raise RedditIntakeError(f"Unsupported TTS provider: {package.tts_provider}")

    if not package.output_basename:
        raise RedditIntakeError("output_basename is required.")

    if package.background_video_path.suffix.lower() not in config.SUPPORTED_EXTENSIONS:
        raise RedditIntakeError(
            f"Unsupported background video extension: {package.background_video_path.suffix}"
        )

    _resolve_voice_id(package.voice_id)


def generate_tts_audio(
    script: str,
    provider_name: str,
    voice_id: str,
    output_dir: Path,
    api_key: str | None = None,
    client_factory: RespeecherClientFactory | None = None,
    pre_resolved_voice_id: str | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    if not script.strip():
        raise RedditIntakeError("TTS script is required.")

    if provider_name != RESPEECHER_PROVIDER:
        raise RedditIntakeError(f"Unsupported TTS provider: {provider_name}")

    return _generate_respeecher_tts_audio(
        script=script,
        voice_id=voice_id,
        output_dir=output_dir,
        api_key=api_key or config.RESPEECHER_API_KEY,
        client_factory=client_factory,
        pre_resolved_voice_id=pre_resolved_voice_id,
    )


def list_available_respeecher_voices(
    api_key: str | None = None,
    client_factory: RespeecherClientFactory | None = None,
) -> tuple[Mapping[str, Any], ...]:
    resolved_api_key = api_key or config.RESPEECHER_API_KEY
    if not resolved_api_key:
        raise RedditIntakeError("Respeecher API key is not configured.")

    try:
        client = _build_respeecher_client(
            api_key=resolved_api_key,
            client_factory=client_factory,
        )
        voices = client.voices.list()
    except Exception as exc:
        LOGGER.error("Respeecher voices listing failed.")
        raise RedditIntakeError(f"Respeecher voices listing failed: {exc}") from exc

    return tuple(_voice_to_mapping(voice) for voice in voices)


def resolve_respeecher_voice_id(
    voice_id: str | None,
    api_key: str | None = None,
    client_factory: RespeecherClientFactory | None = None,
) -> str:
    resolved_voice_id = _resolve_voice_id(voice_id or "")
    voices = list_available_respeecher_voices(
        api_key=api_key,
        client_factory=client_factory,
    )
    available_voice_ids = {
        str(voice["id"]).strip()
        for voice in voices
        if "id" in voice and str(voice["id"]).strip()
    }

    if resolved_voice_id not in available_voice_ids:
        raise RedditIntakeError(f"Respeecher voice_id is not available: {resolved_voice_id}")

    return resolved_voice_id


def render_approved_story(
    package: ApprovedStoryPackage,
    input_dir: Path = config.INPUT_DIR,
    tts_generator: TtsBoundary = generate_tts_audio,
    subtitle_generator: SubtitleBoundary = generate_subtitle_file,
    renderer: RenderBoundary | None = None,
    pre_resolved_voice_id: str | None = None,
) -> RenderedInboxAsset:
    validate_approved_story_package(package)

    if not package.background_video_path.is_file():
        raise RedditIntakeError(
            f"Background video template does not exist: {package.background_video_path}"
        )

    input_dir.mkdir(parents=True, exist_ok=True)
    bundle_dir = _build_render_bundle_dir(
        input_dir=input_dir,
        story_id=package.story_id,
    )
    bundle_dir.mkdir(parents=True, exist_ok=True)
    output_path = _build_render_output_path(
        input_dir=input_dir,
        story_id=package.story_id,
    )
    render_video = renderer or render_story_video

    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        audio_path = tts_generator(
            package.adapted_script,
            package.tts_provider,
            package.voice_id,
            temp_dir,
            pre_resolved_voice_id=pre_resolved_voice_id,
        )
        try:
            generated_subtitle_path = subtitle_generator(
                audio_path,
                package.adapted_script,
                temp_dir,
            )
        except SubtitleGenerationError as exc:
            raise RedditIntakeError(str(exc)) from exc

        subtitle_path = _build_render_subtitle_path(
            input_dir=input_dir,
            story_id=package.story_id,
        )
        _persist_generated_artifact(
            source_path=generated_subtitle_path,
            output_path=subtitle_path,
            artifact_name="subtitle",
        )
        temp_output_path = temp_dir / "video.mp4"
        try:
            render_video(
                package.background_video_path,
                audio_path,
                temp_output_path,
                subtitle_path,
            )
        except RenderError as exc:
            raise RedditIntakeError(str(exc)) from exc

        if not temp_output_path.exists():
            raise RedditIntakeError(
                f"Expected rendered output was not created: {temp_output_path}"
            )

        _persist_generated_artifact(
            source_path=temp_output_path,
            output_path=output_path,
            artifact_name="rendered output",
        )

        return RenderedInboxAsset(
            story_id=package.story_id,
            bundle_dir=bundle_dir,
            output_path=output_path,
            metadata_path=None,
            audio_path=audio_path,
            subtitle_path=subtitle_path,
        )


def render_story_review(
    story_id: str,
    background_video_path: Path,
    voice_id: str = "",
    output_basename: str | None = None,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    input_dir: Path = config.INPUT_DIR,
    tts_generator: TtsBoundary = generate_tts_audio,
    subtitle_generator: SubtitleBoundary = generate_subtitle_file,
    renderer: RenderBoundary | None = None,
    pre_resolved_voice_id: str | None = None,
) -> RenderedInboxAsset:
    review = get_story_review(story_id=story_id, store_path=store_path)
    if review is None:
        raise RedditIntakeError(f"Story review not found: {story_id}")

    if review.status != APPROVED_STATUS:
        raise RedditIntakeError(f"Story is not approved for rendering: {story_id}")
    if not review.translated_script_uk:
        raise RedditIntakeError(
            f"Translated Ukrainian script is required before rendering: {story_id}"
        )
    if not is_story_ready_for_render(review):
        raise RedditIntakeError(f"Story is not ready for rendering: {story_id}")

    package = build_approved_story_package(
        story_id=review.story_id,
        source_title=review.source_title,
        adapted_script=review.translated_script_uk,
        tts_provider=RESPEECHER_PROVIDER,
        voice_id=voice_id,
        background_video_path=background_video_path,
        output_basename=output_basename or review.story_id,
    )
    rendered_asset = render_approved_story(
        package=package,
        input_dir=input_dir,
        tts_generator=tts_generator,
        subtitle_generator=subtitle_generator,
        renderer=renderer,
        pre_resolved_voice_id=pre_resolved_voice_id,
    )
    try:
        content_meta = build_content_meta(
            source_title=review.source_title,
            description_source=review.translated_script_uk,
            subreddit=review.subreddit,
        )
    except ValueError as exc:
        raise RedditIntakeError(f"Failed to build content metadata: {exc}") from exc

    metadata_path = ensure_content_meta_file(
        bundle_dir=rendered_asset.bundle_dir,
        meta=content_meta,
        overwrite=False,
    )
    return replace(rendered_asset, metadata_path=metadata_path)


def render_and_enqueue_story_review(
    story_id: str,
    background_video_path: Path,
    *,
    voice_id: str = "",
    output_basename: str | None = None,
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    input_dir: Path = config.INPUT_DIR,
    publish_queue_store: PublishQueueStore | None = None,
    tts_generator: TtsBoundary = generate_tts_audio,
    subtitle_generator: SubtitleBoundary = generate_subtitle_file,
    renderer: RenderBoundary | None = None,
    pre_resolved_voice_id: str | None = None,
) -> tuple[RenderedInboxAsset, PublishQueueResult]:
    rendered_asset = render_story_review(
        story_id=story_id,
        background_video_path=background_video_path,
        voice_id=voice_id,
        output_basename=output_basename,
        store_path=store_path,
        input_dir=input_dir,
        tts_generator=tts_generator,
        subtitle_generator=subtitle_generator,
        renderer=renderer,
        pre_resolved_voice_id=pre_resolved_voice_id,
    )

    review = get_story_review(story_id=story_id, store_path=store_path)
    if review is None:
        raise RedditIntakeError(f"Story review not found: {story_id}")
    if not is_story_ready_for_render(review):
        raise RedditIntakeError(f"Story is not ready for rendering: {story_id}")

    resolved_queue_store = publish_queue_store or PublishQueueStore()
    if publish_queue_store is None:
        resolved_queue_store.initialize()

    thumbnail_path = _create_render_thumbnail_from_rendered_story(
        rendered_asset.output_path
    )
    processor_result = ProcessorResult(
        asset_id=rendered_asset.story_id,
        source_path=rendered_asset.output_path,
        status=PROCESSED_STATUS,
        output_files=(rendered_asset.output_path,),
        thumbnail_path=thumbnail_path,
        error_message=None,
    )

    candidate = build_publish_candidate(
        processor_result=processor_result,
        title=review.source_title,
    )
    queue_result = resolved_queue_store.enqueue_candidate(candidate)
    return rendered_asset, queue_result


def run_full_pipeline(
    *,
    background_video_path: Path,
    telegram_update_payloads: Sequence[Mapping[str, Any]] = (),
    store_path: Path = config.REDDIT_REVIEW_STORE_PATH,
    input_dir: Path = config.INPUT_DIR,
) -> PipelineRunResult:
    fetched = 0
    accepted = 0
    persisted = 0
    sent_to_telegram = 0
    processed_updates = 0
    translated = 0
    rendered = 0
    enqueued = 0
    stage_errors: list[str] = []

    try:
        live_candidates = fetch_live_story_candidates()
        fetched = len(live_candidates)
        preparation_result = prepare_story_candidates(
            fetch_candidates=lambda: live_candidates,
        )
        accepted_candidates = tuple(
            result.candidate
            for result in preparation_result.results
            if result.status == CANDIDATE_ACCEPTED_STATUS and result.candidate is not None
        )
        accepted = len(accepted_candidates)
        persisted_reviews = save_story_candidates_for_review(
            accepted_candidates,
            store_path=store_path,
        )
        persisted = len(persisted_reviews)
    except RedditIntakeError as exc:
        LOGGER.error("Reddit intake fetch/persist stage failed: %s", exc)
        stage_errors.append(f"fetch/persist: {exc}")

    try:
        sent_story_ids = send_pending_story_reviews_to_telegram(store_path=store_path)
        sent_to_telegram = len(sent_story_ids)
    except StoryReviewTelegramSendError as exc:
        sent_to_telegram = len(exc.sent_story_ids)
        LOGGER.error("Telegram approval send stage failed: %s", exc)
        stage_errors.append(f"send-approvals: {exc}")
    except RedditIntakeError as exc:
        LOGGER.error("Telegram approval send stage failed: %s", exc)
        stage_errors.append(f"send-approvals: {exc}")

    for update_payload in telegram_update_payloads:
        try:
            updated_review = process_telegram_update(
                update_payload,
                store_path=store_path,
                translate=_default_translate_adapter,
            )
        except RedditIntakeError as exc:
            LOGGER.error("Telegram update processing failed: %s", exc)
            stage_errors.append(f"process-update: {exc}")
            continue

        processed_updates += 1
        if updated_review.translated_script_uk:
            translated += 1

    for review in list_story_reviews(store_path=store_path):
        if (
            not review.selected_for_processing
            or not review.operator_rewritten_text
            or review.translated_script_uk is not None
        ):
            continue

        try:
            translate_story_review_to_ukrainian(
                story_id=review.story_id,
                store_path=store_path,
                translator=_default_translate_adapter,
            )
        except RedditIntakeError as exc:
            LOGGER.error("Story translation failed for %s: %s", review.story_id, exc)
            stage_errors.append(f"translate:{review.story_id}: {exc}")
            continue

        translated += 1

    publish_queue_store = PublishQueueStore()
    try:
        publish_queue_store.initialize()
    except PublishQueueError as exc:
        LOGGER.error("Publish queue initialization failed: %s", exc)
        stage_errors.append(f"publish-queue-init: {exc}")
        result = PipelineRunResult(
            fetched=fetched,
            accepted=accepted,
            persisted=persisted,
            sent_to_telegram=sent_to_telegram,
            processed_updates=processed_updates,
            translated=translated,
            rendered=rendered,
            enqueued=enqueued,
            stage_errors=tuple(stage_errors),
        )
        LOGGER.info("Reddit pipeline summary: %s", result)
        return result

    pre_resolved_voice_id: str | None = None
    ready_reviews = tuple(
        review
        for review in list_story_reviews(store_path=store_path, status=APPROVED_STATUS)
        if is_story_ready_for_render(review)
    )
    if ready_reviews:
        try:
            pre_resolved_voice_id = resolve_respeecher_voice_id("")
        except RedditIntakeError as exc:
            LOGGER.error("Respeecher voice resolution failed before render loop: %s", exc)
            stage_errors.append(f"voice-resolution: {exc}")
            ready_reviews = ()

    for review in ready_reviews:
        output_basename = review.story_id
        rendered_output_path = _build_render_output_path(
            input_dir=input_dir,
            story_id=review.story_id,
        )
        if publish_queue_store.get_item(review.story_id, rendered_output_path) is not None:
            continue

        try:
            _, queue_result = render_and_enqueue_story_review(
                story_id=review.story_id,
                background_video_path=background_video_path,
                output_basename=output_basename,
                store_path=store_path,
                input_dir=input_dir,
                publish_queue_store=publish_queue_store,
                pre_resolved_voice_id=pre_resolved_voice_id,
            )
        except (RedditIntakeError, PublishQueueError) as exc:
            LOGGER.error("Render/enqueue failed for %s: %s", review.story_id, exc)
            stage_errors.append(f"render-enqueue:{review.story_id}: {exc}")
            continue

        rendered += 1
        if queue_result.created:
            enqueued += 1

    result = PipelineRunResult(
        fetched=fetched,
        accepted=accepted,
        persisted=persisted,
        sent_to_telegram=sent_to_telegram,
        processed_updates=processed_updates,
        translated=translated,
        rendered=rendered,
        enqueued=enqueued,
        stage_errors=tuple(stage_errors),
    )
    LOGGER.info("Reddit pipeline summary: %s", result)
    return result


def _create_render_thumbnail_from_rendered_story(rendered_video_path: Path) -> Path:
    thumbnail_path = rendered_video_path.with_suffix(".jpg")
    command = [
        config.FFMPEG_BIN,
        "-y",
        "-ss",
        str(config.THUMBNAIL_CAPTURE_SECOND),
        "-i",
        str(rendered_video_path),
        "-map_metadata",
        "-1",
        "-frames:v",
        "1",
        str(thumbnail_path),
    ]
    _run_command(command=command, timeout_seconds=config.FFMPEG_TIMEOUT_SECONDS)
    if not thumbnail_path.exists():
        raise RedditIntakeError(f"Expected rendered thumbnail was not created: {thumbnail_path}")
    return thumbnail_path


def _build_render_bundle_dir(input_dir: Path, story_id: str) -> Path:
    return input_dir / story_id


def _build_render_output_path(input_dir: Path, story_id: str) -> Path:
    return _build_render_bundle_dir(input_dir=input_dir, story_id=story_id) / "video.mp4"


def _build_render_subtitle_path(input_dir: Path, story_id: str) -> Path:
    return _build_render_bundle_dir(input_dir=input_dir, story_id=story_id) / "subtitles.srt"


def _persist_generated_artifact(
    source_path: Path,
    output_path: Path,
    artifact_name: str,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not source_path.exists():
        raise RedditIntakeError(
            f"Expected {artifact_name} file was not created: {source_path}"
        )
    if output_path.exists():
        if compute_sha256(output_path) == compute_sha256(source_path):
            return
        output_path.unlink()

    source_path.replace(output_path)


def _generate_respeecher_tts_audio(
    script: str,
    voice_id: str,
    output_dir: Path,
    api_key: str | None,
    client_factory: RespeecherClientFactory | None,
    pre_resolved_voice_id: str | None,
) -> Path:
    if not api_key:
        raise RedditIntakeError("Respeecher API key is not configured.")

    resolved_voice_id = pre_resolved_voice_id or resolve_respeecher_voice_id(
        voice_id=voice_id,
        api_key=api_key,
        client_factory=client_factory,
    )

    try:
        client = _build_respeecher_client(
            api_key=api_key,
            client_factory=client_factory,
        )
        audio_stream = client.tts.bytes(
            transcript=script,
            voice={"id": resolved_voice_id},
        )
    except Exception as exc:
        LOGGER.error("Respeecher TTS generation failed.")
        raise RedditIntakeError(f"Respeecher TTS generation failed: {exc}") from exc

    audio_path = output_dir / "voiceover.wav"
    _write_audio_stream(audio_stream=audio_stream, output_path=audio_path)

    if not audio_path.exists():
        raise RedditIntakeError(f"Expected rendered audio output was not created: {audio_path}")

    return audio_path


def _build_reddit_listing_url(subreddit: str, listing: str, limit: int) -> str:
    params = {"limit": str(limit), "raw_json": "1"}
    if listing == "top":
        params["t"] = "week"

    return f"https://www.reddit.com/r/{subreddit}/{listing}.json?{urlencode(params)}"


def _default_reddit_json_get(url: str, headers: Mapping[str, str]) -> Mapping[str, Any]:
    request = urllib.request.Request(url, headers=dict(headers), method="GET")
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))


def _normalize_reddit_listing(
    payload: Mapping[str, Any],
    subreddit: str,
) -> tuple[StoryCandidate, ...]:
    listing_data = payload.get("data")
    if not isinstance(listing_data, Mapping):
        raise ValueError(f"Invalid Reddit listing payload for r/{subreddit}")

    children = listing_data.get("children")
    if not isinstance(children, Sequence):
        raise ValueError(f"Invalid Reddit listing children for r/{subreddit}")

    candidates: list[StoryCandidate] = []
    for child in children:
        if not isinstance(child, Mapping):
            continue
        child_data = child.get("data")
        if not isinstance(child_data, Mapping):
            continue

        permalink = str(child_data.get("permalink", "")).strip()
        post_subreddit = str(child_data.get("subreddit", subreddit)).strip()
        candidates.append(
            StoryCandidate(
                post_id=str(child_data.get("id", "")).strip(),
                subreddit=post_subreddit,
                title=str(child_data.get("title", "")).strip(),
                body=str(child_data.get("selftext", "")),
                score=int(child_data.get("score", 0)),
                url=f"https://www.reddit.com{permalink}" if permalink else "",
                created_utc=float(child_data.get("created_utc", 0.0)),
            )
        )

    return tuple(candidate for candidate in candidates if candidate.post_id)


def _run_command(command: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    try:
        completed_process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise RedditIntakeError(f"Command not found: {command[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RedditIntakeError(f"Command timed out: {command[0]}") from exc

    if completed_process.returncode != 0:
        stderr_output = completed_process.stderr.strip() or "Command returned non-zero exit code."
        raise RedditIntakeError(stderr_output)

    return completed_process


def _normalize_story_candidate(raw_candidate: StoryCandidate | Mapping[str, Any]) -> StoryCandidate:
    if isinstance(raw_candidate, StoryCandidate):
        return raw_candidate

    required_fields = (
        "post_id",
        "subreddit",
        "title",
        "body",
        "score",
        "url",
        "created_utc",
    )
    missing_fields = [field for field in required_fields if field not in raw_candidate]
    if missing_fields:
        raise ValueError(f"Missing candidate field(s): {', '.join(missing_fields)}")

    return StoryCandidate(
        post_id=str(raw_candidate["post_id"]).strip(),
        subreddit=str(raw_candidate["subreddit"]).strip(),
        title=str(raw_candidate["title"]).strip(),
        body=str(raw_candidate["body"]),
        score=int(raw_candidate["score"]),
        url=str(raw_candidate["url"]).strip(),
        created_utc=float(raw_candidate["created_utc"]),
    )


def _extract_candidate_id(raw_candidate: StoryCandidate | Mapping[str, Any]) -> str | None:
    if isinstance(raw_candidate, StoryCandidate):
        return raw_candidate.post_id

    candidate_id = raw_candidate.get("post_id")
    if candidate_id is None:
        return None

    return str(candidate_id)


def _build_candidate_preparation_result(
    candidate: StoryCandidate,
    is_allowed: bool,
    reason: str | None,
) -> CandidatePreparationResult:
    return CandidatePreparationResult(
        post_id=candidate.post_id,
        status=CANDIDATE_ACCEPTED_STATUS if is_allowed else CANDIDATE_REJECTED_STATUS,
        reason="Candidate accepted." if is_allowed else (reason or "Candidate rejected."),
        candidate=candidate,
    )


def _build_failed_candidate_preparation_result(
    raw_candidate: StoryCandidate | Mapping[str, Any],
    reason: str,
) -> CandidatePreparationResult:
    return CandidatePreparationResult(
        post_id=_extract_candidate_id(raw_candidate),
        status=CANDIDATE_FAILED_STATUS,
        reason=reason,
        candidate=None,
    )


def _process_single_candidate(
    raw_candidate: StoryCandidate | Mapping[str, Any],
    *,
    allowed_subreddits: Sequence[str],
    minimum_score: int,
    minimum_combined_length: int,
) -> CandidatePreparationResult:
    try:
        candidate = _normalize_story_candidate(raw_candidate)
        is_allowed, reason = filter_story_candidate(
            candidate=candidate,
            allowed_subreddits=allowed_subreddits,
            minimum_score=minimum_score,
            minimum_combined_length=minimum_combined_length,
        )
    except (TypeError, ValueError) as exc:
        LOGGER.error("Failed to normalize Reddit candidate: %s", exc)
        return _build_failed_candidate_preparation_result(raw_candidate, str(exc))

    result = _build_candidate_preparation_result(
        candidate=candidate,
        is_allowed=is_allowed,
        reason=reason,
    )
    if is_allowed and result.candidate is None:
        raise RuntimeError("Accepted candidate preparation result is missing candidate.")
    if isinstance(result.candidate, CandidatePreparationResult):
        raise RuntimeError("Candidate preparation result was wrapped more than once.")
    return result


def _validate_no_duplicate_parameters(func: Callable[..., Any]) -> None:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError) as exc:
        raise RedditIntakeError(
            f"Could not inspect function signature: {getattr(func, '__name__', repr(func))}"
        ) from exc

    parameter_names = list(signature.parameters.keys())
    duplicate_names = sorted(
        {
            name
            for name in parameter_names
            if parameter_names.count(name) > 1
        }
    )
    if duplicate_names:
        raise RedditIntakeError(
            f"Duplicate parameter names detected in {func.__name__}: {', '.join(duplicate_names)}"
        )


def _safe_sum(values: Iterable[int | bool]) -> int:
    if isinstance(values, (str, bytes, bytearray)):
        raise RuntimeError("safe_sum received an invalid iterable.")

    try:
        iterator = iter(values)
    except TypeError as exc:
        raise RuntimeError("safe_sum requires an iterable of integers or booleans.") from exc

    total = 0
    for value in iterator:
        if not isinstance(value, (int, bool)):
            raise RuntimeError("safe_sum received a non-numeric item.")
        total += int(value)

    return total


def _build_excerpt(text: str, excerpt_length: int) -> str:
    trimmed_text = " ".join(text.split())
    if len(trimmed_text) <= excerpt_length:
        return trimmed_text

    return trimmed_text[: excerpt_length - 3].rstrip() + "..."


def _default_http_post(url: str, payload: bytes, headers: Mapping[str, str]) -> bytes:
    request = urllib.request.Request(url, data=payload, headers=dict(headers), method="POST")
    with urllib.request.urlopen(request) as response:
        return response.read()


def _build_respeecher_client(
    api_key: str,
    client_factory: RespeecherClientFactory | None,
) -> Any:
    if client_factory is not None:
        return client_factory(api_key)

    return Respeecher(
        api_key=api_key,
        environment=RespeecherEnvironment.PUBLIC_UA_RT,
    )
def _resolve_voice_id(voice_id: str) -> str:
    normalized_voice_id = voice_id.strip()
    if normalized_voice_id:
        if _is_placeholder_voice_id(normalized_voice_id):
            raise RedditIntakeError("Placeholder voice_id is not allowed for respeecher.")
        return normalized_voice_id

    default_voice_id = config.RESPEECHER_DEFAULT_VOICE_ID
    if default_voice_id is None or not default_voice_id.strip():
        raise RedditIntakeError("voice_id is required for provider: respeecher")

    resolved_default = default_voice_id.strip()
    if _is_placeholder_voice_id(resolved_default):
        raise RedditIntakeError("Placeholder voice_id is not allowed for respeecher.")

    return resolved_default


def _is_placeholder_voice_id(voice_id: str) -> bool:
    normalized = voice_id.strip().lower()
    if not normalized:
        return False

    if normalized in PLACEHOLDER_VOICE_IDS:
        return True

    return normalized.startswith("<") and normalized.endswith(">")


def _voice_to_mapping(voice: Any) -> Mapping[str, Any]:
    voice_id = getattr(voice, "id", None)
    voice_name = getattr(voice, "name", None)
    if voice_id is None:
        raise RedditIntakeError("Unexpected Respeecher voices response.")

    return {
        "id": str(voice_id),
        "name": str(voice_name) if voice_name is not None else "<missing name>",
    }


def _write_audio_stream(audio_stream: Iterable[bytes], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    bytes_written = 0

    with output_path.open("wb") as output_file:
        for chunk in audio_stream:
            if not isinstance(chunk, (bytes, bytearray)):
                raise RedditIntakeError("Respeecher audio stream chunk was not bytes.")

            if not chunk:
                continue

            output_file.write(chunk)
            bytes_written += len(chunk)

    if bytes_written == 0:
        raise RedditIntakeError("Generated TTS audio was empty.")


def _update_story_review_record(
    story_id: str,
    store_path: Path,
    updater: Callable[[StoredStoryReview], StoredStoryReview],
) -> StoredStoryReview:
    records = list(_load_story_reviews(store_path))

    for index, record in enumerate(records):
        if record.story_id != story_id:
            continue

        updated_record = updater(record)
        records[index] = updated_record
        _write_story_reviews(store_path, tuple(records))
        return updated_record

    raise RedditIntakeError(f"Story review not found: {story_id}")


def _replace_story_review(
    record: StoredStoryReview,
    *,
    status: str | object = _UNSET,
    sent_to_telegram: bool | object = _UNSET,
    telegram: TelegramReviewMetadata | object = _UNSET,
    selected_for_processing: bool | object = _UNSET,
    operator_rewritten_text: str | None | object = _UNSET,
    script_saved_at: str | None | object = _UNSET,
    translated_script_uk: str | None | object = _UNSET,
    translation_characters_used: int | None | object = _UNSET,
    saved_at: str | object = _UNSET,
) -> StoredStoryReview:
    moderation = record.moderation
    if (
        selected_for_processing is not _UNSET
        or operator_rewritten_text is not _UNSET
        or script_saved_at is not _UNSET
    ):
        moderation = replace(
            moderation,
            selected_for_processing=(
                moderation.selected_for_processing
                if selected_for_processing is _UNSET
                else bool(selected_for_processing)
            ),
            operator_rewritten_text=(
                moderation.operator_rewritten_text
                if operator_rewritten_text is _UNSET
                else operator_rewritten_text
            ),
            script_saved_at=(
                moderation.script_saved_at
                if script_saved_at is _UNSET
                else script_saved_at
            ),
        )

    production = record.production
    if (
        translated_script_uk is not _UNSET
        or translation_characters_used is not _UNSET
    ):
        production = replace(
            production,
            translated_script_uk=(
                production.translated_script_uk
                if translated_script_uk is _UNSET
                else translated_script_uk
            ),
            translation_characters_used=(
                production.translation_characters_used
                if translation_characters_used is _UNSET
                else translation_characters_used
            ),
        )

    resolved_telegram = record.telegram if telegram is _UNSET else telegram

    updated_record = StoredStoryReview(
        intake=record.intake,
        moderation=moderation,
        production=production,
        telegram=resolved_telegram,
        status=record.status if status is _UNSET else str(status),
        sent_to_telegram=(
            record.sent_to_telegram
            if sent_to_telegram is _UNSET
            else bool(sent_to_telegram)
        ),
        saved_at=record.saved_at if saved_at is _UNSET else str(saved_at),
    )
    validate_story_review_state(updated_record)
    return updated_record


def _replace_telegram_review_metadata(
    metadata: TelegramReviewMetadata,
    *,
    chat_id: str | None | object = _UNSET,
    instruction_message_id: int | None | object = _UNSET,
) -> TelegramReviewMetadata:
    return replace(
        metadata,
        chat_id=metadata.chat_id if chat_id is _UNSET else chat_id,
        instruction_message_id=(
            metadata.instruction_message_id
            if instruction_message_id is _UNSET
            else instruction_message_id
        ),
    )


def _load_story_reviews(store_path: Path) -> tuple[StoredStoryReview, ...]:
    if not store_path.exists():
        return ()

    payload = json.loads(store_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise RedditIntakeError("Invalid Reddit review store format.")

    return tuple(_dict_to_story_review(item) for item in payload)


def _write_story_reviews(store_path: Path, reviews: tuple[StoredStoryReview, ...]) -> None:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    for review in reviews:
        validate_story_review_state(review)
    serialized_reviews = [
        asdict(review)
        for review in sorted(reviews, key=lambda review: review.story_id)
    ]
    store_path.write_text(
        json.dumps(serialized_reviews, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _dict_to_story_review(payload: Mapping[str, Any]) -> StoredStoryReview:
    if "intake" in payload:
        intake_payload = payload.get("intake")
        moderation_payload = payload.get("moderation")
        production_payload = payload.get("production")
        telegram_payload = payload.get("telegram")
        if not isinstance(intake_payload, Mapping):
            raise RedditIntakeError("Invalid Reddit review store format.")
        if not isinstance(moderation_payload, Mapping):
            raise RedditIntakeError("Invalid Reddit review store format.")
        if not isinstance(production_payload, Mapping):
            raise RedditIntakeError("Invalid Reddit review store format.")
        if telegram_payload is not None and not isinstance(telegram_payload, Mapping):
            raise RedditIntakeError("Invalid Reddit review store format.")

        record = StoredStoryReview(
            intake=StoryIntakeStage(
                story_id=str(intake_payload["story_id"]),
                source_title=str(intake_payload["source_title"]),
                source_body=str(intake_payload["source_body"]),
                subreddit=str(intake_payload["subreddit"]),
                url=str(intake_payload["url"]),
                score=int(intake_payload["score"]),
                created_utc=float(intake_payload["created_utc"]),
            ),
            moderation=StoryModerationStage(
                selected_for_processing=bool(moderation_payload.get("selected_for_processing", False)),
                operator_rewritten_text=_optional_string(
                    moderation_payload.get("operator_rewritten_text")
                ),
                script_saved_at=_optional_string(moderation_payload.get("script_saved_at")),
            ),
            production=StoryProductionStage(
                translated_script_uk=_optional_string(
                    production_payload.get("translated_script_uk")
                ),
                translation_characters_used=_optional_int(
                    production_payload.get("translation_characters_used")
                ),
            ),
            telegram=TelegramReviewMetadata(
                chat_id=_optional_string(
                    telegram_payload.get("chat_id") if isinstance(telegram_payload, Mapping) else payload.get("telegram_chat_id")
                ),
                instruction_message_id=_optional_int(
                    telegram_payload.get("instruction_message_id") if isinstance(telegram_payload, Mapping) else payload.get("telegram_instruction_message_id")
                ),
            ),
            status=str(payload["status"]),
            sent_to_telegram=bool(payload.get("sent_to_telegram", False)),
            saved_at=str(payload["saved_at"]),
        )
        validate_story_review_state(record)
        return record

    record = StoredStoryReview(
        intake=StoryIntakeStage(
            story_id=str(payload["story_id"]),
            source_title=str(payload["source_title"]),
            source_body=str(payload["source_body"]),
            subreddit=str(payload["subreddit"]),
            url=str(payload["url"]),
            score=int(payload["score"]),
            created_utc=float(payload["created_utc"]),
        ),
        moderation=StoryModerationStage(
            selected_for_processing=bool(payload.get("selected_for_processing", False)),
            operator_rewritten_text=_optional_string(payload.get("operator_rewritten_text")),
            script_saved_at=_optional_string(payload.get("script_saved_at")),
        ),
        production=StoryProductionStage(
            translated_script_uk=_optional_string(payload.get("translated_script_uk")),
            translation_characters_used=_optional_int(
                payload.get("translation_characters_used")
            ),
        ),
        telegram=TelegramReviewMetadata(
            chat_id=_optional_string(payload.get("telegram_chat_id")),
            instruction_message_id=_optional_int(payload.get("telegram_instruction_message_id")),
        ),
        status=str(payload["status"]),
        sent_to_telegram=bool(payload.get("sent_to_telegram", False)),
        saved_at=str(payload["saved_at"]),
    )
    validate_story_review_state(record)
    return record


def _validate_review_status(status: str) -> None:
    if status not in {PENDING_REVIEW_STATUS, APPROVED_STATUS, REJECTED_STATUS}:
        raise RedditIntakeError(f"Unsupported review status: {status}")


def _utc_timestamp_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _story_review_to_candidate(review: StoredStoryReview) -> StoryCandidate:
    return StoryCandidate(
        post_id=review.story_id,
        subreddit=review.subreddit,
        title=review.source_title,
        body=review.source_body,
        score=review.score,
        url=review.url,
        created_utc=review.created_utc,
    )


def _build_story_review_telegram_message(review: StoredStoryReview) -> str:
    return format_telegram_approval_message(_story_review_to_candidate(review))


def _parse_process_callback_data(callback_data: str) -> str:
    if not callback_data.startswith(PROCESS_CALLBACK_PREFIX):
        raise RedditIntakeError(f"Unsupported Telegram callback data: {callback_data}")

    story_id = callback_data.removeprefix(PROCESS_CALLBACK_PREFIX).strip()
    if not story_id:
        raise RedditIntakeError("Telegram process callback is missing story_id.")

    return story_id


def _build_process_reply_markup(story_id: str) -> Mapping[str, Any]:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "Process",
                    "callback_data": build_process_callback_data(story_id),
                }
            ]
        ]
    }


def _mark_story_reviews_as_sent(
    records: Sequence[StoredStoryReview],
    sent_story_ids: Sequence[str],
) -> tuple[StoredStoryReview, ...]:
    sent_story_id_set = set(sent_story_ids)
    if not sent_story_id_set:
        return tuple(records)

    saved_at = _utc_timestamp_now()
    updated_records: list[StoredStoryReview] = []
    for record in records:
        if record.story_id not in sent_story_id_set:
            updated_records.append(record)
            continue

        updated_records.append(
            _replace_story_review(
                record,
                sent_to_telegram=True,
                saved_at=saved_at,
            )
        )

    return tuple(updated_records)


def _validate_no_obvious_copy_paste_patterns() -> None:
    for func in (filter_story_candidate, _process_single_candidate):
        source = inspect.getsource(func)
        signature_source_lines: list[str] = []
        for line in source.splitlines():
            signature_source_lines.append(line.strip())
            if ") ->" in line or line.rstrip().endswith("):"):
                break

        signature_source = " ".join(signature_source_lines)
        if (
            signature_source.count("minimum_combined_length") > 1
            and func.__name__ == "_process_single_candidate"
        ):
            raise RedditIntakeError(
                "Obvious copy-paste pattern detected in _process_single_candidate signature."
            )


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None

    normalized_value = str(value).strip()
    return normalized_value or None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise RedditIntakeError("Invalid integer value in Reddit review store.") from exc


def _extract_chat_id(message_payload: Mapping[str, Any]) -> str | None:
    chat_payload = message_payload.get("chat")
    if not isinstance(chat_payload, Mapping):
        return None

    chat_id = chat_payload.get("id")
    if chat_id is None:
        return None

    return str(chat_id)


_validate_no_duplicate_parameters(filter_story_candidate)
_validate_no_duplicate_parameters(_process_single_candidate)
_validate_no_obvious_copy_paste_patterns()
