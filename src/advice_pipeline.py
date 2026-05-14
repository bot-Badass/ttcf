from __future__ import annotations

import dataclasses
import json
import logging
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Final

from src import config
from src.content_plan import (
    ContentPlanError,
    ContentTopic,
    TOPIC_RENDERED,
    TOPIC_SCRIPT_RECEIVED,
    advance_topic_index,
    get_current_topic_index,
    get_topic_by_id,
    list_topics,
    mark_topic_status,
)
from src.pexels_client import PexelsError
from src.pexels_client import download_background_video as pexels_download
from src.render import RenderError, render_story_video
from src.subtitles import SubtitleGenerationError, generate_subtitle_file
from src.ukrainian_tts_adapter import UkrainianTtsError, generate_tts_wav

LOGGER = logging.getLogger(__name__)

ADVICE_PENDING_STATUS: Final[str] = "pending_review"
ADVICE_APPROVED_STATUS: Final[str] = "approved"
ADVICE_REJECTED_STATUS: Final[str] = "rejected"


class AdvicePipelineError(Exception):
    """Raised when the advice pipeline fails."""


SubtitleBoundary = Callable[[Path, str, Path], Path]
RenderBoundary = Callable[[Path, Path, Path, "Path | None", "str | None"], Path]
VideoDownloaderBoundary = Callable[[Path, str], Path]


def extract_hook_sentence(script_text: str) -> str | None:
    """Return the first non-empty line between 10 and 80 characters."""
    for line in script_text.splitlines():
        stripped = line.strip()
        if 10 <= len(stripped) <= 80:
            return stripped
    return None


@dataclasses.dataclass(frozen=True, slots=True)
class AdviceScript:
    script_id: str
    topic: str
    topic_slug: str
    generated_script: str
    saved_at: str


@dataclasses.dataclass(frozen=True, slots=True)
class AdviceReview:
    script: AdviceScript
    status: str
    telegram_chat_id: str | None
    telegram_message_id: int | None
    updated_at: str


@dataclasses.dataclass(frozen=True, slots=True)
class AdviceRenderResult:
    script_id: str
    output_path: Path
    subtitle_path: Path
    audio_path: Path
    background_video_path: Path


@dataclasses.dataclass(frozen=True, slots=True)
class AdviceReviewStats:
    total: int
    pending_review: int
    approved: int
    rejected: int


@dataclasses.dataclass(frozen=True, slots=True)
class PostMeta:
    title: str
    description: str
    hashtags: tuple[str, ...]
    part_number: int
    total_parts: int
    series_title: str


_CATEGORY_TAGS: Final[dict[str, tuple[str, ...]]] = {
    "A": ("мобілізація", "повістка", "ТЦК", "відстрочка", "військовийоблік"),
    "B": ("українцівЄС", "тимчасовийзахист", "діаспора", "правауЄС"),
    "C": ("сімейнеправо", "розлучення", "спадщина", "опіка"),
    "D": ("компенсація", "майно", "ВПО", "відновлення"),
    "E": ("українціСША", "мігрантів", "правауСША", "еміграція"),
}
_GENERIC_TAGS: Final[tuple[str, ...]] = ("юрист", "право", "правоукраїна", "юридичнапорада")


def generate_post_meta(
    topic: ContentTopic,
    part_number: int,
    total_parts: int,
) -> PostMeta:
    """Generate publication metadata for a micro-series part."""
    raw_title = f"{topic.title} | Частина {part_number}/{total_parts}"
    title = raw_title[:100]

    description_parts: list[str] = []
    if topic.scenario:
        snippet = topic.scenario[:120].rstrip()
        description_parts.append(snippet if snippet.endswith(".") else snippet + ".")
    if topic.legal_facts:
        description_parts.append(str(topic.legal_facts[0]))
    description_parts.append(f"Підписуйся на серію «{topic.series_title}» 🔔")
    description = " ".join(description_parts)

    cat_tags = _CATEGORY_TAGS.get(topic.series_id, ())
    selected_cat = cat_tags[:3]
    needed_generic = 7 - len(selected_cat)
    hashtags = tuple(selected_cat) + _GENERIC_TAGS[:needed_generic]

    return PostMeta(
        title=title,
        description=description,
        hashtags=hashtags,
        part_number=part_number,
        total_parts=total_parts,
        series_title=topic.series_title,
    )


@dataclasses.dataclass(frozen=True, slots=True)
class AdvicePartScript:
    part_number: int
    total_parts: int
    script_text: str
    word_count: int


@dataclasses.dataclass(frozen=True, slots=True)
class AdviceMicroSeries:
    review_id: str
    topic_id: str
    series_title: str
    parts: tuple[AdvicePartScript, ...]
    background_video_path: Path
    parsed_at: str


_PARTS_HEADER_RE = re.compile(r"^PARTS:\s*(\d+)\s*$", re.MULTILINE)
_PART_MARKER_RE = re.compile(r"=== ЧАСТИНА \d+/\d+ ===")


def parse_micro_series_response(
    review_id: str,
    raw_response: str,
    topic: ContentTopic,
    min_words: int = config.ADVICE_SCRIPT_MIN_WORDS,
    min_parts: int = config.ADVICE_MICRO_SERIES_MIN_PARTS,
    max_parts: int = config.ADVICE_MICRO_SERIES_MAX_PARTS,
) -> AdviceMicroSeries:
    """Parse AI multi-script response into an AdviceMicroSeries.

    Expected format::

        PARTS: N

        === ЧАСТИНА 1/N ===
        [text]
        ...
        === ЧАСТИНА N/N ===
        [text]

    Raises AdvicePipelineError if the format is invalid, part counts mismatch,
    or any individual script block is shorter than min_words.
    background_video_path is not set here; use dataclasses.replace before rendering.
    """
    header_match = _PARTS_HEADER_RE.search(raw_response)
    if header_match is None:
        raise AdvicePipelineError(
            "AI response missing 'PARTS: N' header line. "
            "Cannot determine number of scripts."
        )

    declared_n = int(header_match.group(1))
    if declared_n < min_parts or declared_n > max_parts:
        raise AdvicePipelineError(
            f"Declared PARTS: {declared_n} is outside allowed range "
            f"[{min_parts}, {max_parts}]."
        )

    blocks = _PART_MARKER_RE.split(raw_response)
    # blocks[0] is the preamble containing the PARTS: N line; discard it
    script_blocks = [b.strip() for b in blocks[1:]]

    if len(script_blocks) != declared_n:
        raise AdvicePipelineError(
            f"Declared PARTS: {declared_n} but found {len(script_blocks)} script blocks."
        )

    parts: list[AdvicePartScript] = []
    for idx, text in enumerate(script_blocks, start=1):
        word_count = len(text.split())
        if word_count < min_words:
            raise AdvicePipelineError(
                f"Script block {idx}/{declared_n} is too short "
                f"({word_count} words, minimum {min_words}). "
                f"Можливо текст було обрізано при копіюванні — перевір що скопіював повну відповідь AI."
            )
        parts.append(
            AdvicePartScript(
                part_number=idx,
                total_parts=declared_n,
                script_text=text,
                word_count=word_count,
            )
        )

    LOGGER.info(
        "Micro-series parsed: review_id=%s parts=%d",
        review_id,
        declared_n,
    )
    return AdviceMicroSeries(
        review_id=review_id,
        topic_id=topic.topic_id,
        series_title=topic.series_title,
        parts=tuple(parts),
        background_video_path=Path(""),
        parsed_at=_utc_now(),
    )


def render_micro_series(
    micro_series: AdviceMicroSeries,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
    output_dir: Path = config.ADVICE_OUTPUT_DIR,
    tts_generator: Callable[[str, Path, str], Path] | None = None,
    subtitle_generator: SubtitleBoundary | None = None,
    renderer: RenderBoundary | None = None,
    hook_bg_override: str | None = None,
    hook_accent_override: str | None = None,
    hook_brand_override: str | None = None,
) -> tuple[AdviceRenderResult, ...]:
    """Render each part of the micro-series as a separate video.

    Background video is shared (already set in micro_series.background_video_path).
    Returns a tuple of AdviceRenderResult, one per successfully rendered part.
    Per-part failures are logged; if ALL parts fail, raises AdvicePipelineError.
    Bundle dir per part: output_dir / f"{review_id}_part{part_number}".
    """
    del store_path  # not used; retained for consistent signature pattern
    results: list[AdviceRenderResult] = []
    review_id = micro_series.review_id

    for part in micro_series.parts:
        part_id = f"{review_id}_part{part.part_number}"
        bundle_dir = output_dir / part_id
        bundle_dir.mkdir(parents=True, exist_ok=True)

        audio_path = bundle_dir / "voiceover.wav"
        try:
            (tts_generator or generate_tts_wav)(
                part.script_text,
                audio_path,
                config.ADVICE_TTS_VOICE,
            )
        except UkrainianTtsError as exc:
            LOGGER.error("TTS failed for part %d/%d: %s", part.part_number, part.total_parts, exc)
            continue

        try:
            subtitle_path = (subtitle_generator or generate_subtitle_file)(
                audio_path,
                part.script_text,
                bundle_dir,
            )
        except SubtitleGenerationError as exc:
            LOGGER.error(
                "Subtitle generation failed for part %d/%d: %s",
                part.part_number, part.total_parts, exc,
            )
            continue

        output_path = bundle_dir / "video.mp4"
        # Hook text: завжди береться перший сильний рядок зі скрипту цієї частини.
        hook_text = extract_hook_sentence(part.script_text) or micro_series.series_title
        try:
            (renderer or render_story_video)(
                micro_series.background_video_path,
                audio_path,
                output_path,
                subtitle_path,
                hook_text,
                part.part_number,
                part.total_parts,
                micro_series.topic_id[:1].upper() if micro_series.topic_id else None,
                hook_bg_override,
                hook_accent_override,
                hook_brand_override,
            )
        except RenderError as exc:
            LOGGER.error("Render failed for part %d/%d: %s", part.part_number, part.total_parts, exc)
            continue

        LOGGER.info(
            "Micro-series part rendered: review_id=%s part=%d/%d output=%s",
            review_id,
            part.part_number,
            part.total_parts,
            output_path,
        )
        results.append(
            AdviceRenderResult(
                script_id=part_id,
                output_path=output_path,
                subtitle_path=subtitle_path,
                audio_path=audio_path,
                background_video_path=micro_series.background_video_path,
            )
        )

    if not results:
        raise AdvicePipelineError(
            f"All {len(micro_series.parts)} parts failed to render for review_id={review_id}."
        )

    return tuple(results)


def receive_operator_scripts(
    review_id: str,
    raw_response: str,
    topic: ContentTopic,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
    plan_path: Path = config.CONTENT_PLAN_PATH,
    output_dir: Path = config.ADVICE_OUTPUT_DIR,
    video_downloader: VideoDownloaderBoundary | None = None,
    tts_generator: Callable[[str, Path, str], Path] | None = None,
    subtitle_generator: SubtitleBoundary | None = None,
    renderer: RenderBoundary | None = None,
    voice_mode: bool = config.VOICE_MODE,
    hook_bg_override: str | None = None,
    hook_accent_override: str | None = None,
    hook_brand_override: str | None = None,
    cta_overlay_path: str | None = None,
    cta_overlay_width: int = 380,
    cta_overlay_y: int = 100,
) -> tuple[AdviceRenderResult, ...] | AdviceVoiceSession:
    """Full pipeline for multi-script AI response.

    When voice_mode=False (TTS path):
      1. Parse raw_response into AdviceMicroSeries.
      2. Download ONE Pexels background video (shared across all parts).
      3. Render N videos via render_micro_series.
      4. Mark AdviceReview as approved.
      5. Mark content plan topic as rendered.
      Returns tuple of AdviceRenderResult (one per successfully rendered part).

    When voice_mode=True (voice path):
      1. Parse raw_response into AdviceMicroSeries.
      2. Download ONE Pexels background video.
      3. Create AdviceVoiceSession with all slots initialised to None.
      Returns the AdviceVoiceSession (rendering happens later via render_voice_session).

    Raises AdvicePipelineError on parse failure or if all parts fail to render.
    """
    review = _get_review_or_raise(review_id, store_path)
    if review.status != ADVICE_PENDING_STATUS:
        raise AdvicePipelineError(
            f"Cannot receive scripts for {review_id}: "
            f"status is {review.status!r}, expected {ADVICE_PENDING_STATUS!r}."
        )

    micro_series = parse_micro_series_response(review_id, raw_response, topic)

    shared_dir = output_dir / review_id
    shared_dir.mkdir(parents=True, exist_ok=True)
    background_video_path = shared_dir / "background.mp4"
    resolved_query = topic.pexels_query or config.ADVICE_PEXELS_QUERY
    LOGGER.info("Pexels query resolved: series=%s query=%s", topic.series_id, resolved_query)
    try:
        (video_downloader or pexels_download)(background_video_path, resolved_query)
    except PexelsError as exc:
        raise AdvicePipelineError(f"Pexels video download failed: {exc}") from exc

    micro_series = dataclasses.replace(micro_series, background_video_path=background_video_path)

    if voice_mode:
        session = create_voice_session(
            review_id=review_id,
            micro_series=micro_series,
            background_video_path=background_video_path,
            store_path=config.VOICE_SESSION_STORE_PATH,
            hook_bg_override=hook_bg_override,
            hook_accent_override=hook_accent_override,
            hook_brand_override=hook_brand_override,
            cta_overlay_path=cta_overlay_path,
            cta_overlay_width=cta_overlay_width,
            cta_overlay_y=cta_overlay_y,
        )
        LOGGER.info(
            "Voice session created via receive_operator_scripts: review_id=%s session_id=%s",
            review_id, session.session_id,
        )
        return session

    results = render_micro_series(
        micro_series,
        store_path=store_path,
        output_dir=output_dir,
        tts_generator=tts_generator,
        subtitle_generator=subtitle_generator,
        renderer=renderer,
        hook_bg_override=hook_bg_override,
        hook_accent_override=hook_accent_override,
        hook_brand_override=hook_brand_override,
    )

    now = _utc_now()
    updated_script = dataclasses.replace(
        review.script, generated_script=raw_response, saved_at=now
    )
    _update_advice_review(
        script_id=review_id,
        store_path=store_path,
        updater=lambda r: dataclasses.replace(
            r,
            script=updated_script,
            status=ADVICE_APPROVED_STATUS,
            updated_at=now,
        ),
    )

    try:
        mark_topic_status(
            topic.topic_id,
            TOPIC_RENDERED,
            review_id,
            plan_path,
            parts_count=len(micro_series.parts),
        )
    except ContentPlanError as exc:
        LOGGER.warning("Failed to mark topic status in content plan: %s", exc)

    LOGGER.info(
        "Micro-series receive complete: review_id=%s parts=%d rendered=%d",
        review_id,
        len(micro_series.parts),
        len(results),
    )
    return results


def issue_next_topic(
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
    plan_path: Path = config.CONTENT_PLAN_PATH,
) -> tuple[AdviceReview, ContentTopic]:
    """Get the next pending topic from the content plan and create an AdviceReview for it.

    Advances current_topic_index so repeated calls issue different topics even while
    the previous topic is still 'pending' in the plan awaiting a script.
    """
    try:
        all_topics = list_topics(plan_path)
        current_index = get_current_topic_index(plan_path)
    except ContentPlanError as exc:
        raise AdvicePipelineError(str(exc)) from exc

    next_topic: ContentTopic | None = None
    next_global_index = current_index
    for i in range(current_index, len(all_topics)):
        if all_topics[i].status == "pending":
            next_topic = all_topics[i]
            next_global_index = i + 1
            break

    if next_topic is None:
        raise AdvicePipelineError("Всі теми опрацьовані.")

    try:
        advance_topic_index(next_global_index, plan_path)
    except ContentPlanError as exc:
        raise AdvicePipelineError(str(exc)) from exc

    now = _utc_now()
    script = AdviceScript(
        script_id=uuid.uuid4().hex,
        topic=next_topic.title,
        topic_slug=_build_topic_slug(next_topic.title),
        generated_script="",
        saved_at=now,
    )
    review = AdviceReview(
        script=script,
        status=ADVICE_PENDING_STATUS,
        telegram_chat_id=None,
        telegram_message_id=None,
        updated_at=now,
    )

    existing_reviews = _load_advice_reviews(store_path)
    _write_advice_reviews(store_path, (*existing_reviews, review))

    LOGGER.info(
        "Advice topic issued: script_id=%s topic=%s",
        script.script_id,
        next_topic.title,
    )
    return review, next_topic


def issue_specific_topic(
    topic_id: str,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
    plan_path: Path = config.CONTENT_PLAN_PATH,
) -> tuple[AdviceReview, ContentTopic]:
    """Create an AdviceReview for the specific requested topic.

    Does NOT advance current_topic_index.
    Raises AdvicePipelineError if topic is not found or status != pending.
    """
    try:
        topic = get_topic_by_id(topic_id, plan_path)
    except ContentPlanError as exc:
        raise AdvicePipelineError(str(exc)) from exc

    if topic is None:
        raise AdvicePipelineError(f"Тему не знайдено: {topic_id}")

    if topic.status != "pending":
        raise AdvicePipelineError(
            f"Тема {topic_id} має статус {topic.status!r}, очікується 'pending'."
        )

    now = _utc_now()
    script = AdviceScript(
        script_id=uuid.uuid4().hex,
        topic=topic.title,
        topic_slug=_build_topic_slug(topic.title),
        generated_script="",
        saved_at=now,
    )
    review = AdviceReview(
        script=script,
        status=ADVICE_PENDING_STATUS,
        telegram_chat_id=None,
        telegram_message_id=None,
        updated_at=now,
    )

    existing_reviews = _load_advice_reviews(store_path)
    _write_advice_reviews(store_path, (*existing_reviews, review))

    LOGGER.info(
        "Specific advice topic issued: script_id=%s topic_id=%s topic=%s",
        script.script_id,
        topic_id,
        topic.title,
    )
    return review, topic


def receive_operator_script(
    script_id: str,
    script_text: str,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
    plan_path: Path = config.CONTENT_PLAN_PATH,
    min_words: int = config.ADVICE_SCRIPT_MIN_WORDS,
) -> AdviceReview:
    """Validate and save operator-supplied script text into the AdviceReview.

    Marks the corresponding content plan topic as 'script_received'.
    """
    word_count = len(script_text.split())
    if word_count < min_words:
        raise AdvicePipelineError(
            f"Скрипт занадто короткий ({word_count} слів, мінімум {min_words})."
        )

    review = _get_review_or_raise(script_id, store_path)
    if review.status != ADVICE_PENDING_STATUS:
        raise AdvicePipelineError(
            f"Cannot receive script for {script_id}: "
            f"status is {review.status!r}, expected {ADVICE_PENDING_STATUS!r}."
        )

    now = _utc_now()
    updated_script = dataclasses.replace(review.script, generated_script=script_text, saved_at=now)
    updated = _update_advice_review(
        script_id=script_id,
        store_path=store_path,
        updater=lambda r: dataclasses.replace(r, script=updated_script, updated_at=now),
    )

    try:
        topics = list_topics(plan_path)
        for topic in topics:
            if topic.title == review.script.topic:
                mark_topic_status(topic.topic_id, TOPIC_SCRIPT_RECEIVED, script_id, plan_path)
                break
    except ContentPlanError as exc:
        LOGGER.warning("Failed to mark topic status in content plan: %s", exc)

    LOGGER.info(
        "Operator script received: script_id=%s words=%d",
        script_id,
        word_count,
    )
    return updated


def approve_advice_script(
    script_id: str,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
    output_dir: Path = config.ADVICE_OUTPUT_DIR,
    tts_generator: Callable[[str, Path, str], Path] | None = None,
    video_downloader: VideoDownloaderBoundary | None = None,
    subtitle_generator: SubtitleBoundary | None = None,
    renderer: RenderBoundary | None = None,
) -> AdviceRenderResult:
    review = _get_review_or_raise(script_id, store_path)

    if review.status != ADVICE_PENDING_STATUS:
        raise AdvicePipelineError(
            f"Cannot approve script {script_id}: status is {review.status!r}, "
            f"expected {ADVICE_PENDING_STATUS!r}."
        )

    _update_advice_review(
        script_id=script_id,
        store_path=store_path,
        updater=lambda r: dataclasses.replace(
            r, status=ADVICE_APPROVED_STATUS, updated_at=_utc_now()
        ),
    )

    bundle_dir = output_dir / script_id
    bundle_dir.mkdir(parents=True, exist_ok=True)

    audio_path = bundle_dir / "voiceover.wav"
    try:
        (tts_generator or generate_tts_wav)(
            review.script.generated_script,
            audio_path,
            config.ADVICE_TTS_VOICE,
        )
    except UkrainianTtsError as exc:
        raise AdvicePipelineError(f"TTS failed: {exc}") from exc

    background_video_path = bundle_dir / "background.mp4"
    try:
        (video_downloader or pexels_download)(background_video_path, config.ADVICE_PEXELS_QUERY)
    except PexelsError as exc:
        raise AdvicePipelineError(f"Pexels video download failed: {exc}") from exc

    try:
        subtitle_path = (subtitle_generator or generate_subtitle_file)(
            audio_path,
            review.script.generated_script,
            bundle_dir,
        )
    except SubtitleGenerationError as exc:
        raise AdvicePipelineError(f"Subtitle generation failed: {exc}") from exc

    output_path = bundle_dir / "video.mp4"
    try:
        (renderer or render_story_video)(
            background_video_path,
            audio_path,
            output_path,
            subtitle_path,
        )
    except RenderError as exc:
        raise AdvicePipelineError(f"Render failed: {exc}") from exc

    LOGGER.info(
        "Advice render complete: script_id=%s output=%s",
        script_id,
        output_path,
    )
    return AdviceRenderResult(
        script_id=script_id,
        output_path=output_path,
        subtitle_path=subtitle_path,
        audio_path=audio_path,
        background_video_path=background_video_path,
    )


def reject_advice_script(
    script_id: str,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
) -> AdviceReview:
    review = _get_review_or_raise(script_id, store_path)

    if review.status != ADVICE_PENDING_STATUS:
        raise AdvicePipelineError(
            f"Cannot reject script {script_id}: status is {review.status!r}, "
            f"expected {ADVICE_PENDING_STATUS!r}."
        )

    updated = _update_advice_review(
        script_id=script_id,
        store_path=store_path,
        updater=lambda r: dataclasses.replace(
            r, status=ADVICE_REJECTED_STATUS, updated_at=_utc_now()
        ),
    )

    LOGGER.info("Advice script rejected: script_id=%s", script_id)
    return updated


def get_advice_review(
    script_id: str,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
) -> AdviceReview | None:
    reviews = _load_advice_reviews(store_path)
    for review in reviews:
        if review.script.script_id == script_id:
            return review
    return None


def get_advice_review_by_message_id(
    message_id: int,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
) -> AdviceReview | None:
    reviews = _load_advice_reviews(store_path)
    for review in reviews:
        if review.telegram_message_id == message_id:
            return review
    return None


def list_advice_reviews(
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
    status: str | None = None,
) -> tuple[AdviceReview, ...]:
    reviews = _load_advice_reviews(store_path)
    if status is None:
        return reviews
    return tuple(r for r in reviews if r.status == status)


def get_advice_stats(
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
) -> AdviceReviewStats:
    reviews = _load_advice_reviews(store_path)
    return AdviceReviewStats(
        total=len(reviews),
        pending_review=sum(1 for r in reviews if r.status == ADVICE_PENDING_STATUS),
        approved=sum(1 for r in reviews if r.status == ADVICE_APPROVED_STATUS),
        rejected=sum(1 for r in reviews if r.status == ADVICE_REJECTED_STATUS),
    )


def save_advice_telegram_message_id(
    script_id: str,
    chat_id: str,
    message_id: int,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
) -> AdviceReview:
    return _update_advice_review(
        script_id=script_id,
        store_path=store_path,
        updater=lambda r: dataclasses.replace(
            r,
            telegram_chat_id=chat_id,
            telegram_message_id=message_id,
            updated_at=_utc_now(),
        ),
    )


def _load_advice_reviews(store_path: Path) -> tuple[AdviceReview, ...]:
    if not store_path.is_file():
        return ()
    try:
        raw = json.loads(store_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise AdvicePipelineError(f"Failed to load advice review store: {exc}") from exc
    if not isinstance(raw, list):
        raise AdvicePipelineError("Advice review store must contain a JSON array.")
    return tuple(_deserialize_review(item) for item in raw)


def _write_advice_reviews(
    store_path: Path,
    reviews: tuple[AdviceReview, ...],
) -> None:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    sorted_reviews = tuple(sorted(reviews, key=lambda r: r.script.script_id))
    serialized = [dataclasses.asdict(r) for r in sorted_reviews]
    store_path.write_text(
        json.dumps(serialized, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _update_advice_review(
    script_id: str,
    store_path: Path,
    updater: Callable[[AdviceReview], AdviceReview],
) -> AdviceReview:
    reviews = _load_advice_reviews(store_path)
    updated_review: AdviceReview | None = None
    new_reviews: list[AdviceReview] = []
    for review in reviews:
        if review.script.script_id == script_id:
            updated_review = updater(review)
            new_reviews.append(updated_review)
        else:
            new_reviews.append(review)
    if updated_review is None:
        raise AdvicePipelineError(f"Advice review not found: {script_id}")
    _write_advice_reviews(store_path, tuple(new_reviews))
    return updated_review


def _get_review_or_raise(script_id: str, store_path: Path) -> AdviceReview:
    review = get_advice_review(script_id, store_path)
    if review is None:
        raise AdvicePipelineError(f"Advice review not found: {script_id}")
    return review


def _build_topic_slug(topic: str) -> str:
    lowered = topic.lower().strip()
    hyphenated = re.sub(r"\s+", "-", lowered)
    return re.sub(r"[^\w-]", "", hyphenated)


def _deserialize_review(data: dict) -> AdviceReview:
    script_data = data["script"]
    script = AdviceScript(
        script_id=script_data["script_id"],
        topic=script_data["topic"],
        topic_slug=script_data["topic_slug"],
        generated_script=script_data["generated_script"],
        saved_at=script_data["saved_at"],
    )
    return AdviceReview(
        script=script,
        status=data["status"],
        telegram_chat_id=data.get("telegram_chat_id"),
        telegram_message_id=data.get("telegram_message_id"),
        updated_at=data["updated_at"],
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Voice session dataclass and store
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True, slots=True)
class AdviceVoiceSession:
    session_id: str
    review_id: str
    topic_id: str
    micro_series: AdviceMicroSeries
    voice_files: tuple[Path | None, ...]
    part_message_ids: tuple[int | None, ...]
    background_video_path: Path
    created_at: str
    updated_at: str
    hook_bg_override: str | None = None
    hook_accent_override: str | None = None
    hook_brand_override: str | None = None
    cta_overlay_path: str | None = None
    cta_overlay_width: int = 380
    cta_overlay_y: int = 100


def _serialize_voice_session(s: AdviceVoiceSession) -> dict:
    return {
        "session_id": s.session_id,
        "review_id": s.review_id,
        "topic_id": s.topic_id,
        "micro_series": {
            "review_id": s.micro_series.review_id,
            "topic_id": s.micro_series.topic_id,
            "series_title": s.micro_series.series_title,
            "parts": [
                {
                    "part_number": p.part_number,
                    "total_parts": p.total_parts,
                    "script_text": p.script_text,
                    "word_count": p.word_count,
                }
                for p in s.micro_series.parts
            ],
            "background_video_path": str(s.micro_series.background_video_path),
            "parsed_at": s.micro_series.parsed_at,
        },
        "voice_files": [str(f) if f is not None else None for f in s.voice_files],
        "part_message_ids": list(s.part_message_ids),
        "background_video_path": str(s.background_video_path),
        "created_at": s.created_at,
        "updated_at": s.updated_at,
        "hook_bg_override": s.hook_bg_override,
        "hook_accent_override": s.hook_accent_override,
        "hook_brand_override": s.hook_brand_override,
        "cta_overlay_path": s.cta_overlay_path,
        "cta_overlay_width": s.cta_overlay_width,
        "cta_overlay_y": s.cta_overlay_y,
    }


def _deserialize_voice_session(data: dict) -> AdviceVoiceSession:
    ms_data = data["micro_series"]
    parts = tuple(
        AdvicePartScript(
            part_number=p["part_number"],
            total_parts=p["total_parts"],
            script_text=p["script_text"],
            word_count=p["word_count"],
        )
        for p in ms_data["parts"]
    )
    micro_series = AdviceMicroSeries(
        review_id=ms_data["review_id"],
        topic_id=ms_data["topic_id"],
        series_title=ms_data["series_title"],
        parts=parts,
        background_video_path=Path(ms_data["background_video_path"]),
        parsed_at=ms_data["parsed_at"],
    )
    voice_files: tuple[Path | None, ...] = tuple(
        Path(f) if f is not None else None for f in data["voice_files"]
    )
    part_message_ids: tuple[int | None, ...] = tuple(data["part_message_ids"])
    return AdviceVoiceSession(
        session_id=data["session_id"],
        review_id=data["review_id"],
        topic_id=data["topic_id"],
        micro_series=micro_series,
        voice_files=voice_files,
        part_message_ids=part_message_ids,
        background_video_path=Path(data["background_video_path"]),
        created_at=data["created_at"],
        updated_at=data["updated_at"],
        hook_bg_override=data.get("hook_bg_override"),
        hook_accent_override=data.get("hook_accent_override"),
        hook_brand_override=data.get("hook_brand_override"),
        cta_overlay_path=data.get("cta_overlay_path"),
        cta_overlay_width=data.get("cta_overlay_width", 380),
        cta_overlay_y=data.get("cta_overlay_y", 100),
    )


def _load_voice_sessions(store_path: Path) -> tuple[AdviceVoiceSession, ...]:
    if not store_path.is_file():
        return ()
    try:
        raw = json.loads(store_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise AdvicePipelineError(f"Failed to load voice session store: {exc}") from exc
    if not isinstance(raw, list):
        raise AdvicePipelineError("Voice session store must contain a JSON array.")
    return tuple(_deserialize_voice_session(item) for item in raw)


def _write_voice_sessions(
    store_path: Path,
    sessions: tuple[AdviceVoiceSession, ...],
) -> None:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = [_serialize_voice_session(s) for s in sessions]
    store_path.write_text(
        json.dumps(serialized, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _update_voice_session(
    session_id: str,
    store_path: Path,
    updater: Callable[[AdviceVoiceSession], AdviceVoiceSession],
) -> AdviceVoiceSession:
    sessions = _load_voice_sessions(store_path)
    updated: AdviceVoiceSession | None = None
    new_sessions: list[AdviceVoiceSession] = []
    for s in sessions:
        if s.session_id == session_id:
            updated = updater(s)
            new_sessions.append(updated)
        else:
            new_sessions.append(s)
    if updated is None:
        raise AdvicePipelineError(f"Voice session not found: {session_id}")
    _write_voice_sessions(store_path, tuple(new_sessions))
    return updated


# ---------------------------------------------------------------------------
# Voice session public API
# ---------------------------------------------------------------------------

def create_voice_session(
    review_id: str,
    micro_series: AdviceMicroSeries,
    background_video_path: Path,
    store_path: Path = config.VOICE_SESSION_STORE_PATH,
    hook_bg_override: str | None = None,
    hook_accent_override: str | None = None,
    hook_brand_override: str | None = None,
    cta_overlay_path: str | None = None,
    cta_overlay_width: int = 380,
    cta_overlay_y: int = 100,
) -> AdviceVoiceSession:
    """Create a new AdviceVoiceSession with all slots initialised to None."""
    now = _utc_now()
    n = len(micro_series.parts)
    session = AdviceVoiceSession(
        session_id=uuid.uuid4().hex,
        review_id=review_id,
        topic_id=micro_series.topic_id,
        micro_series=micro_series,
        voice_files=tuple(None for _ in range(n)),
        part_message_ids=tuple(None for _ in range(n)),
        background_video_path=background_video_path,
        created_at=now,
        updated_at=now,
        hook_bg_override=hook_bg_override,
        hook_accent_override=hook_accent_override,
        hook_brand_override=hook_brand_override,
        cta_overlay_path=cta_overlay_path,
        cta_overlay_width=cta_overlay_width,
        cta_overlay_y=cta_overlay_y,
    )
    existing = _load_voice_sessions(store_path)
    _write_voice_sessions(store_path, (*existing, session))
    LOGGER.info(
        "Voice session created: session_id=%s review_id=%s parts=%d",
        session.session_id, review_id, n,
    )
    return session


def save_part_message_id(
    session_id: str,
    part_number: int,
    message_id: int,
    store_path: Path = config.VOICE_SESSION_STORE_PATH,
) -> AdviceVoiceSession:
    """Record the Telegram message_id of a part script message."""
    def _updater(s: AdviceVoiceSession) -> AdviceVoiceSession:
        idx = part_number - 1
        new_ids = list(s.part_message_ids)
        new_ids[idx] = message_id
        return dataclasses.replace(s, part_message_ids=tuple(new_ids), updated_at=_utc_now())
    return _update_voice_session(session_id, store_path, _updater)


def save_part_voice(
    session_id: str,
    part_number: int,
    wav_path: Path,
    store_path: Path = config.VOICE_SESSION_STORE_PATH,
) -> AdviceVoiceSession:
    """Store the WAV path for a received voice recording."""
    def _updater(s: AdviceVoiceSession) -> AdviceVoiceSession:
        idx = part_number - 1
        new_files = list(s.voice_files)
        new_files[idx] = wav_path
        return dataclasses.replace(s, voice_files=tuple(new_files), updated_at=_utc_now())
    return _update_voice_session(session_id, store_path, _updater)


def get_voice_session(
    session_id: str,
    store_path: Path = config.VOICE_SESSION_STORE_PATH,
) -> AdviceVoiceSession | None:
    sessions = _load_voice_sessions(store_path)
    for s in sessions:
        if s.session_id == session_id:
            return s
    return None


def get_voice_session_by_part_message_id(
    message_id: int,
    store_path: Path = config.VOICE_SESSION_STORE_PATH,
) -> tuple[AdviceVoiceSession, int] | None:
    """Return (session, part_number) for the part that sent this Telegram message_id."""
    sessions = _load_voice_sessions(store_path)
    for s in sessions:
        for idx, mid in enumerate(s.part_message_ids):
            if mid == message_id:
                return s, idx + 1
    return None


def is_voice_session_complete(session: AdviceVoiceSession) -> bool:
    """Return True when every part has a recorded WAV file."""
    return all(f is not None for f in session.voice_files)


def convert_ogg_to_wav(ogg_path: Path, wav_path: Path) -> Path:
    """Convert OGG/OGA to 16 kHz mono PCM WAV via ffmpeg.

    Raises AdvicePipelineError on non-zero exit or missing output.
    """
    cmd = [
        config.FFMPEG_BIN, "-y",
        "-i", str(ogg_path),
        "-ar", "16000",
        "-ac", "1",
        "-c:a", "pcm_s16le",
        str(wav_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=120)
    except Exception as exc:
        raise AdvicePipelineError(f"ffmpeg invocation failed: {exc}") from exc
    if result.returncode != 0:
        raise AdvicePipelineError(
            f"ffmpeg exited with code {result.returncode}: "
            f"{result.stderr.decode('utf-8', errors='replace')}"
        )
    if not wav_path.is_file():
        raise AdvicePipelineError(
            f"ffmpeg succeeded but output file not found: {wav_path}"
        )
    return wav_path


def render_voice_session(
    session: AdviceVoiceSession,
    store_path: Path = config.ADVICE_REVIEW_STORE_PATH,
    voice_store_path: Path = config.VOICE_SESSION_STORE_PATH,
    plan_path: Path = config.CONTENT_PLAN_PATH,
    output_dir: Path = config.ADVICE_OUTPUT_DIR,
    subtitle_generator: SubtitleBoundary | None = None,
    renderer: RenderBoundary | None = None,
) -> tuple[AdviceRenderResult, ...]:
    """Render each part of the session using pre-recorded voice files (no TTS).

    Marks the AdviceReview as approved and the content plan topic as rendered
    when rendering completes successfully.
    """
    del voice_store_path  # retained for consistent signature pattern

    try:
        topic = get_topic_by_id(session.topic_id, plan_path)
    except ContentPlanError as exc:
        raise AdvicePipelineError(
            f"Content plan lookup failed for topic {session.topic_id}: {exc}"
        ) from exc
    if topic is None:
        raise AdvicePipelineError(f"Topic not found in content plan: {session.topic_id}")

    review_id = session.review_id
    micro_series = session.micro_series
    results: list[AdviceRenderResult] = []

    for part, wav_path in zip(micro_series.parts, session.voice_files):
        if wav_path is None:
            LOGGER.error(
                "Voice file missing for part %d/%d — skipping.",
                part.part_number, part.total_parts,
            )
            continue

        part_id = f"{review_id}_part{part.part_number}"
        bundle_dir = output_dir / part_id
        bundle_dir.mkdir(parents=True, exist_ok=True)

        try:
            subtitle_path = (subtitle_generator or generate_subtitle_file)(
                wav_path,
                part.script_text,
                bundle_dir,
            )
        except SubtitleGenerationError as exc:
            LOGGER.error(
                "Subtitle generation failed for part %d/%d: %s",
                part.part_number, part.total_parts, exc,
            )
            continue

        output_path = bundle_dir / "video.mp4"
        # Hook text: завжди береться перший сильний рядок зі скрипту цієї частини.
        # Кожна частина має власний мікрогачок — саме він має бути на hook frame.
        hook_text = extract_hook_sentence(part.script_text) or topic.title
        try:
            (renderer or render_story_video)(
                session.background_video_path,
                wav_path,
                output_path,
                subtitle_path,
                hook_text,
                part.part_number,
                part.total_parts,
                topic.series_id if topic is not None else None,
                session.hook_bg_override,
                session.hook_accent_override,
                session.hook_brand_override,
                session.cta_overlay_path,
                session.cta_overlay_width,
                session.cta_overlay_y,
            )
        except RenderError as exc:
            LOGGER.error(
                "Render failed for part %d/%d: %s",
                part.part_number, part.total_parts, exc,
            )
            continue

        LOGGER.info(
            "Voice session part rendered: review_id=%s part=%d/%d output=%s",
            review_id, part.part_number, part.total_parts, output_path,
        )
        results.append(
            AdviceRenderResult(
                script_id=part_id,
                output_path=output_path,
                subtitle_path=subtitle_path,
                audio_path=wav_path,
                background_video_path=session.background_video_path,
            )
        )

    if not results:
        raise AdvicePipelineError(
            f"All {len(micro_series.parts)} parts failed to render for review_id={review_id}."
        )

    now = _utc_now()
    try:
        _update_advice_review(
            script_id=review_id,
            store_path=store_path,
            updater=lambda r: dataclasses.replace(
                r, status=ADVICE_APPROVED_STATUS, updated_at=now,
            ),
        )
    except AdvicePipelineError as exc:
        LOGGER.warning("Failed to mark review as approved after voice render: %s", exc)

    try:
        mark_topic_status(
            session.topic_id,
            TOPIC_RENDERED,
            review_id,
            plan_path,
            parts_count=len(micro_series.parts),
        )
    except ContentPlanError as exc:
        LOGGER.warning("Failed to mark topic as rendered after voice render: %s", exc)

    LOGGER.info(
        "Voice session render complete: review_id=%s parts=%d rendered=%d",
        review_id, len(micro_series.parts), len(results),
    )
    return tuple(results)
