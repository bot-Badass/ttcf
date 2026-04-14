from __future__ import annotations

import json
import logging
import time
import urllib.request
import wave
from pathlib import Path
from typing import Any, Callable, Final, Mapping, Sequence

from src import config
from src.advice_pipeline import (
    ADVICE_PENDING_STATUS,
    AdvicePipelineError,
    AdviceMicroSeries,
    AdviceReview,
    AdviceReviewStats,
    AdviceVoiceSession,
    PostMeta,
    approve_advice_script,
    convert_ogg_to_wav,
    generate_post_meta,
    get_advice_review,
    get_advice_review_by_message_id,
    get_advice_stats,
    get_voice_session,
    get_voice_session_by_part_message_id,
    is_voice_session_complete,
    issue_next_topic,
    issue_specific_topic,
    list_advice_reviews,
    receive_operator_script,
    receive_operator_scripts,
    reject_advice_script,
    render_voice_session,
    save_advice_telegram_message_id,
    save_part_message_id,
    save_part_voice,
)
from src.content_plan import (
    ContentTopic,
    TOPIC_RENDERED,
    TOPIC_SCRIPT_RECEIVED,
    get_topic_by_id,
    list_topics,
)
from src.publisher import PublishCandidate, PublishQueueStore
from src.reddit_intake import (
    APPROVED_STATUS,
    RedditIntakeError,
    StoryReviewStats,
    StoryReviewTelegramSendError,
    fetch_live_story_candidates,
    get_story_review_stats,
    handle_telegram_process_callback,
    ingest_telegram_rewrite_reply,
    is_story_ready_for_render,
    list_story_reviews,
    prepare_story_candidates,
    render_and_enqueue_story_review,
    save_story_candidates_for_review,
    send_pending_story_reviews_to_telegram,
)

LOGGER = logging.getLogger(__name__)

HELP_TEXT: Final[str] = (
    "/plan - переглянути контент-план та керувати темами\n"
    "/status - show Reddit review store stats\n"
    "/fetch - fetch and persist Reddit candidates\n"
    "/list - list approved Reddit stories\n"
    "/queue - list queued publish items\n"
    "/help - show this help"
)
TELEGRAM_MAX_MESSAGE_LENGTH: Final[int] = 4096
RENDER_CALLBACK_PREFIX: Final[str] = "render:"


def _send_long_message(
    chat_id: str,
    text: str,
    reply_markup: Mapping[str, Any] | None,
    send_message: SendMessageBoundary,
) -> Mapping[str, Any]:
    """Send a message splitting it into <=4096-char chunks if needed.

    Telegram rejects messages longer than 4096 characters with HTTP 400.
    Only the LAST chunk receives the reply_markup so inline buttons appear
    at the end of the message sequence.
    Returns the result of the last send_message call.
    """
    if len(text) <= TELEGRAM_MAX_MESSAGE_LENGTH:
        return send_message(chat_id, text, reply_markup)

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= TELEGRAM_MAX_MESSAGE_LENGTH:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, TELEGRAM_MAX_MESSAGE_LENGTH)
        if split_at == -1:
            split_at = TELEGRAM_MAX_MESSAGE_LENGTH
        chunks.append(remaining[:split_at])
        remaining = remaining[split_at:].lstrip("\n")

    result: Mapping[str, Any] = {}
    for idx, chunk in enumerate(chunks):
        is_last = idx == len(chunks) - 1
        result = send_message(chat_id, chunk, reply_markup if is_last else None)
    return result


GetUpdatesBoundary = Callable[[int | None], Sequence[Mapping[str, Any]]]
SendMessageBoundary = Callable[[str, str, Mapping[str, Any] | None], Mapping[str, Any]]
SendVideoBoundary = Callable[[str, Path, str | None], Mapping[str, Any]]
SleepBoundary = Callable[[float], None]
EditMessageBoundary = Callable[[str, int, str, Mapping[str, Any] | None], Mapping[str, Any]]
AnswerCallbackQueryBoundary = Callable[[str, str | None], None]
DownloadFileBoundary = Callable[[str], bytes]

# chat_id → message_id of the "factory message" (plan UI message that gets edited in-place)
_factory_messages: dict[str, int] = {}

# chat_id → active voice session_id
_voice_sessions: dict[str, str] = {}

# chat_id → {part_number: message_id} of sent part-script messages
_part_script_msg_ids: dict[str, dict[int, int]] = {}

# chat_id → {part_number: (session_id, pending_wav_path)}
_pending_voice_confirmations: dict[str, dict[int, tuple[str, Path]]] = {}

# chat_id → active channel key ("law" or "finance")
_active_channel: dict[str, str] = {}


def _get_channel_profile(chat_id: str) -> dict:
    """Return the active channel profile dict for this chat."""
    key = _active_channel.get(chat_id, config.DEFAULT_CHANNEL)
    return config.CHANNEL_PROFILES.get(key, config.CHANNEL_PROFILES[config.DEFAULT_CHANNEL])


def _channel_plan_path(chat_id: str) -> Path:
    return _get_channel_profile(chat_id)["plan_path"]


def _channel_prompt_path(chat_id: str) -> Path:
    return _get_channel_profile(chat_id)["prompt_path"]


def run_polling_loop(
    *,
    background_video_path: Path | None,
    polling_interval_seconds: int = config.TELEGRAM_POLLING_INTERVAL_SECONDS,
    queue_store: PublishQueueStore | None = None,
    get_updates: GetUpdatesBoundary | None = None,
    send_message: SendMessageBoundary | None = None,
    send_video: SendVideoBoundary | None = None,
    sleep_func: SleepBoundary = time.sleep,
    max_cycles: int | None = None,
    edit_message: EditMessageBoundary | None = None,
    answer_callback: AnswerCallbackQueryBoundary | None = None,
    download_file: DownloadFileBoundary | None = None,
) -> None:
    resolved_queue_store = queue_store or PublishQueueStore()
    if queue_store is None:
        resolved_queue_store.initialize()

    updates_getter = get_updates or _default_get_updates
    message_sender = send_message or _default_send_message
    video_sender = send_video or _default_send_video
    message_editor = edit_message or _default_edit_message
    callback_answerer = answer_callback or _default_answer_callback
    file_downloader = download_file or _default_download_file

    next_offset: int | None = None
    cycles = 0
    while max_cycles is None or cycles < max_cycles:
        try:
            updates = updates_getter(next_offset)
        except Exception as exc:
            LOGGER.warning("Telegram polling request failed: %s", exc)
            sleep_func(polling_interval_seconds)
            cycles += 1
            continue

        for update in updates:
            update_id = _extract_update_id(update)
            if update_id is not None:
                next_offset = update_id + 1

            try:
                _process_update(
                    update=update,
                    background_video_path=background_video_path,
                    queue_store=resolved_queue_store,
                    send_message=message_sender,
                    send_video=video_sender,
                    edit_message=message_editor,
                    answer_callback=callback_answerer,
                    download_file=file_downloader,
                )
            except Exception as exc:
                LOGGER.error("Telegram update processing failed: update_id=%s error=%s", update_id, exc)
                chat_id = _extract_update_chat_id(update)
                if chat_id is not None:
                    _send_operator_error(
                        chat_id=chat_id,
                        message_sender=message_sender,
                        error_text=str(exc),
                    )

        sleep_func(polling_interval_seconds)
        cycles += 1


def _process_update(
    *,
    update: Mapping[str, Any],
    background_video_path: Path | None,
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
    edit_message: EditMessageBoundary,
    answer_callback: AnswerCallbackQueryBoundary,
    download_file: DownloadFileBoundary,
) -> None:
    message_payload = update.get("message")
    callback_query = update.get("callback_query")

    if isinstance(message_payload, Mapping):
        if _is_command_message(message_payload):
            _handle_command_message(
                message_payload=message_payload,
                queue_store=queue_store,
                send_message=send_message,
                send_video=send_video,
                edit_message=edit_message,
            )
            return
        if "reply_to_message" in message_payload:
            _handle_reply_message(
                message_payload=message_payload,
                queue_store=queue_store,
                send_message=send_message,
                send_video=send_video,
                edit_message=edit_message,
                download_file=download_file,
            )
            return

    if isinstance(callback_query, Mapping):
        _handle_callback_query(
            callback_query=callback_query,
            background_video_path=background_video_path,
            queue_store=queue_store,
            send_message=send_message,
            send_video=send_video,
            edit_message=edit_message,
            answer_callback=answer_callback,
        )
        return


def _handle_command_message(
    *,
    message_payload: Mapping[str, Any],
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
    edit_message: EditMessageBoundary,
) -> None:
    chat_id = _require_chat_id(message_payload)
    command = str(message_payload.get("text", "")).strip().split(maxsplit=1)[0]

    if command == "/plan":
        _handle_plan_command(
            message_payload=message_payload,
            send_message=send_message,
            edit_message=edit_message,
        )
        return

    if command == "/status":
        reddit_stats = get_story_review_stats()
        advice_stats = get_advice_stats()
        send_message(
            chat_id,
            _format_status_message(reddit_stats, advice_stats),
            None,
        )
        return

    if command == "/fetch":
        live_candidates = fetch_live_story_candidates()
        prepared = prepare_story_candidates(fetch_candidates=lambda: live_candidates)
        accepted_candidates = tuple(
            result.candidate
            for result in prepared.results
            if result.status == "accepted" and result.candidate is not None
        )
        persisted = save_story_candidates_for_review(accepted_candidates)
        try:
            sent_story_ids = send_pending_story_reviews_to_telegram()
        except StoryReviewTelegramSendError as exc:
            sent_story_ids = exc.sent_story_ids
            LOGGER.error("Telegram send partially failed during /fetch: %s", exc)
        except RedditIntakeError as exc:
            sent_story_ids = ()
            LOGGER.error("Telegram send failed during /fetch: %s", exc)
        send_message(
            chat_id,
            " | ".join(
                (
                    f"fetched={len(live_candidates)}",
                    f"accepted={len(accepted_candidates)}",
                    f"persisted={len(persisted)}",
                    f"sent_to_telegram={len(sent_story_ids)}",
                )
            ),
            None,
        )
        return

    if command == "/list":
        approved_reviews = list_story_reviews(status=APPROVED_STATUS)
        if not approved_reviews:
            send_message(chat_id, "No approved Reddit stories.", None)
            return
        lines = []
        for review in approved_reviews:
            lines.append(
                " | ".join(
                    (
                        f"story_id={review.story_id}",
                        f"ready={'yes' if is_story_ready_for_render(review) else 'no'}",
                        f"title={review.source_title}",
                    )
                )
            )
        send_message(chat_id, "\n".join(lines), None)
        return

    if command == "/queue":
        queued_items = queue_store.list_queued_items()
        if not queued_items:
            send_message(chat_id, "No queued publish items.", None)
            return
        lines = [
            " | ".join(
                (
                    f"asset_id={item.asset_id}",
                    f"title={item.title}",
                    f"status={item.status}",
                )
            )
            for item in queued_items
        ]
        send_message(chat_id, "\n".join(lines), None)
        return

    # Hidden fallbacks — not advertised in HELP_TEXT but still functional.
    if command == "/newtopic":
        _handle_newtopic_command(
            message_payload=message_payload,
            send_message=send_message,
        )
        return

    if command == "/scripts":
        _handle_scripts_command(
            message_payload=message_payload,
            queue_store=queue_store,
            send_message=send_message,
            send_video=send_video,
        )
        return

    if command == "/script":
        _handle_script_command(
            message_payload=message_payload,
            queue_store=queue_store,
            send_message=send_message,
            send_video=send_video,
        )
        return

    if command == "/approve":
        _handle_approve_command(
            message_payload=message_payload,
            queue_store=queue_store,
            send_message=send_message,
            send_video=send_video,
        )
        return

    if command == "/reject":
        _handle_reject_command(
            message_payload=message_payload,
            send_message=send_message,
        )
        return

    send_message(chat_id, HELP_TEXT, None)


def _handle_reply_message(
    *,
    message_payload: Mapping[str, Any],
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
    edit_message: EditMessageBoundary,
    download_file: DownloadFileBoundary,
) -> None:
    chat_id = _require_chat_id(message_payload)

    # Priority 1: voice reply — must be checked before text-reply routing.
    if "voice" in message_payload:
        _handle_voice_reply(
            message_payload=message_payload,
            send_message=send_message,
            download_file=download_file,
        )
        return

    # Priority 2: reply to an advice prompt message.
    reply_to = message_payload.get("reply_to_message")
    if isinstance(reply_to, Mapping):
        replied_message_id = _extract_message_id(reply_to)
        if replied_message_id is not None:
            advice_review = get_advice_review_by_message_id(replied_message_id)
            if advice_review is not None:
                raw_response = str(message_payload.get("text", "")).strip()
                review_id = advice_review.script.script_id
                topic = _find_topic_for_review(advice_review, chat_id)
                if topic is None:
                    send_message(
                        chat_id,
                        f"❌ Тему не знайдено: {advice_review.script.topic!r}",
                        None,
                    )
                    return
                _edit_factory(chat_id, *_build_rendering_message(topic), edit_message)
                try:
                    result = receive_operator_scripts(
                        review_id,
                        raw_response,
                        topic,
                        plan_path=_channel_plan_path(chat_id),
                    )
                except AdvicePipelineError as exc:
                    send_message(
                        chat_id,
                        f"❌ Помилка парсингу:\n{exc}\n\nПовна відповідь AI:\n{raw_response[:500]}",
                        None,
                    )
                    return
                if isinstance(result, AdviceVoiceSession):
                    # voice_mode=True — collect voice recordings part-by-part
                    _voice_sessions[chat_id] = result.session_id
                    _edit_factory(
                        chat_id, *_build_voice_collection_message(result, topic), edit_message
                    )
                    msg_id = _send_part_script_message(chat_id, result, 1, send_message)
                    if msg_id is not None:
                        _part_script_msg_ids.setdefault(chat_id, {})[1] = msg_id
                else:
                    # voice_mode=False — TTS rendered immediately
                    results = result
                    _edit_factory(
                        chat_id, *_build_done_message(topic, len(results)), edit_message
                    )
                    for part_idx, r in enumerate(results, start=1):
                        post_meta = generate_post_meta(topic, part_idx, len(results))
                        send_video(
                            chat_id,
                            r.output_path,
                            f"📺 Частина {part_idx}/{len(results)} | {topic.title}",
                        )
                        send_message(chat_id, _format_post_meta_message(post_meta), None)
                        candidate = PublishCandidate(
                            asset_id=r.script_id,
                            clip_path=r.output_path,
                            thumbnail_path=r.output_path,
                            title=post_meta.title,
                            caption=post_meta.description,
                            hashtags=post_meta.hashtags,
                        )
                        queue_store.enqueue_candidate(candidate)
                return

    # Priority 3: Existing Reddit reply flow.
    try:
        updated_review = ingest_telegram_rewrite_reply(message_payload)
    except RedditIntakeError as exc:
        if "cannot be mapped" in str(exc):
            send_message(
                chat_id,
                "Could not map this reply to a story. Please reply directly to the instruction message.",
                None,
            )
            return
        raise

    if (
        updated_review.translation_characters_used is None
        and updated_review.translated_script_uk
    ):
        confirmation = (
            f"Ukrainian script saved directly for story {updated_review.story_id}. "
            "No translation needed."
        )
    else:
        confirmation = f"Script saved and translated for story {updated_review.story_id}."

    send_message(chat_id, confirmation, None)

    if updated_review.translated_script_uk and updated_review.status == APPROVED_STATUS:
        send_message(
            chat_id,
            "Story is approved and ready to render.",
            _build_render_reply_markup(updated_review.story_id),
        )
        return

    if updated_review.translated_script_uk:
        send_message(
            chat_id,
            (
                f"Translation complete. Approve story {updated_review.story_id} "
                "with 'review --status approved' before rendering."
            ),
            None,
        )


def _handle_callback_query(
    *,
    callback_query: Mapping[str, Any],
    background_video_path: Path | None,
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
    edit_message: EditMessageBoundary,
    answer_callback: AnswerCallbackQueryBoundary,
) -> None:
    callback_data = str(callback_query.get("data", "")).strip()
    callback_id = str(callback_query.get("id", ""))
    message_payload = callback_query.get("message")
    if not isinstance(message_payload, Mapping):
        raise RedditIntakeError("Telegram callback message payload is missing.")
    chat_id = _require_chat_id(message_payload)

    # --- Advice plan UI callbacks ---

    if callback_data.startswith("series:"):
        series_id = callback_data.removeprefix("series:")
        text, markup = _build_series_topic_list_message(series_id, chat_id)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data.startswith("topic:"):
        topic_id = callback_data.removeprefix("topic:")
        topic = _get_topic_by_id_safe(topic_id, chat_id)
        if topic is None:
            send_message(chat_id, f"❌ Тему не знайдено: {topic_id}", None)
            answer_callback(callback_id, None)
            return
        text, markup = _build_topic_detail_message(topic)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data.startswith("prompt:"):
        topic_id = callback_data.removeprefix("prompt:")
        topic = _get_topic_by_id_safe(topic_id, chat_id)
        if topic is None:
            send_message(chat_id, f"❌ Тему не знайдено: {topic_id}", None)
            answer_callback(callback_id, None)
            return
        # Find existing pending review for this topic, or create one on demand.
        pending_reviews = list_advice_reviews(status=ADVICE_PENDING_STATUS)
        review = next(
            (r for r in reversed(pending_reviews) if r.script.topic == topic.title),
            None,
        )
        if review is None:
            try:
                review, topic = issue_specific_topic(
                    topic_id, plan_path=_channel_plan_path(chat_id)
                )
            except AdvicePipelineError as exc:
                send_message(chat_id, f"❌ {exc}", None)
                answer_callback(callback_id, None)
                return
        try:
            filled_prompt = _build_filled_prompt_text(topic, chat_id)
        except AdvicePipelineError as exc:
            send_message(chat_id, f"❌ {exc}", None)
            answer_callback(callback_id, None)
            return
        result = _send_long_message(chat_id, filled_prompt, None, send_message)
        msg_id = _extract_message_id(result)
        if msg_id is not None:
            save_advice_telegram_message_id(review.script.script_id, chat_id, msg_id)
        text, markup = _build_waiting_message(topic, review.script.script_id)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data.startswith("cancel:"):
        topic_id = callback_data.removeprefix("cancel:")
        topic = _get_topic_by_id_safe(topic_id, chat_id)
        series_id = topic.series_id if topic is not None else ""
        text, markup = _build_series_topic_list_message(series_id, chat_id)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data == "next_topic":
        try:
            _review, topic = issue_next_topic(plan_path=_channel_plan_path(chat_id))
        except AdvicePipelineError as exc:
            send_message(chat_id, f"❌ {exc}", None)
            answer_callback(callback_id, None)
            return
        text, markup = _build_topic_detail_message(topic)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data.startswith("channel:"):
        channel_key = callback_data.removeprefix("channel:")
        if channel_key in config.CHANNEL_PROFILES:
            _active_channel[chat_id] = channel_key
        text, markup = _build_plan_series_list_message(chat_id)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data == "back:channel":
        text, markup = _build_channel_picker_message(chat_id)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data == "back:plan":
        text, markup = _build_plan_series_list_message(chat_id)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data.startswith("back:"):
        series_id = callback_data.removeprefix("back:")
        text, markup = _build_series_topic_list_message(series_id, chat_id)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data == "close":
        # Only close if this callback is for the current factory message.
        # Stale callbacks from previous bot sessions are silently ignored.
        msg_id_in_callback = _extract_message_id(message_payload)
        current_factory_id = _factory_messages.get(chat_id)
        if current_factory_id is not None and msg_id_in_callback == current_factory_id:
            _edit_factory(chat_id, "Закрито ✓", {"inline_keyboard": []}, edit_message)
            _factory_messages.pop(chat_id, None)
        answer_callback(callback_id, None)
        return

    if callback_data.startswith("resend_part:"):
        parts = callback_data.removeprefix("resend_part:").split(":", 1)
        if len(parts) == 2:
            sid, n_str = parts
            try:
                part_n = int(n_str)
            except ValueError:
                answer_callback(callback_id, None)
                return
            session = get_voice_session(sid)
            if session is None:
                send_message(chat_id, f"❌ Сесію не знайдено: {sid}", None)
                answer_callback(callback_id, None)
                return
            msg_id = _send_part_script_message(chat_id, session, part_n, send_message)
            if msg_id is not None:
                _part_script_msg_ids.setdefault(chat_id, {})[part_n] = msg_id
        answer_callback(callback_id, None)
        return

    if callback_data.startswith("cancel_voice:"):
        sid = callback_data.removeprefix("cancel_voice:")
        session = get_voice_session(sid)
        if session is not None:
            topic = _get_topic_by_id_safe(session.topic_id)
            series_id = topic.series_id if topic is not None else ""
            text, markup = _build_series_topic_list_message(series_id)
        else:
            text, markup = _build_plan_series_list_message()
        _voice_sessions.pop(chat_id, None)
        _part_script_msg_ids.pop(chat_id, None)
        _edit_factory(chat_id, text, markup, edit_message)
        answer_callback(callback_id, None)
        return

    if callback_data.startswith("confirm_voice:"):
        parts = callback_data.removeprefix("confirm_voice:").split(":", 1)
        if len(parts) != 2:
            answer_callback(callback_id, None)
            return
        sid, n_str = parts
        try:
            part_n = int(n_str)
        except ValueError:
            answer_callback(callback_id, None)
            return
        entry = _pending_voice_confirmations.get(chat_id, {}).pop(part_n, None)
        if entry is None:
            send_message(chat_id, "❌ Підтвердження не знайдено. Запишіть голосове знову.", None)
            answer_callback(callback_id, None)
            return
        _session_id, pending_wav = entry
        session = get_voice_session(_session_id)
        if session is None:
            send_message(chat_id, f"❌ Сесію не знайдено: {_session_id}", None)
            answer_callback(callback_id, None)
            return
        review_id = session.review_id
        part_dir = config.ADVICE_OUTPUT_DIR / f"{review_id}_part{part_n}"
        final_wav = part_dir / "voiceover.wav"
        pending_wav.rename(final_wav)
        saved_session = save_part_voice(_session_id, part_n, final_wav)
        answer_callback(callback_id, "✅ Прийнято!")
        topic = _get_topic_by_id_safe(saved_session.topic_id)
        if not is_voice_session_complete(saved_session):
            if topic is not None:
                _edit_factory(
                    chat_id, *_build_voice_collection_message(saved_session, topic), edit_message
                )
            next_part = part_n + 1
            if next_part <= len(saved_session.micro_series.parts):
                msg_id = _send_part_script_message(chat_id, saved_session, next_part, send_message)
                if msg_id is not None:
                    _part_script_msg_ids.setdefault(chat_id, {})[next_part] = msg_id
        else:
            _render_and_send_voice_session(
                chat_id=chat_id,
                session=saved_session,
                queue_store=queue_store,
                send_message=send_message,
                send_video=send_video,
                edit_message=edit_message,
            )
        return

    if callback_data.startswith("retake_voice:"):
        parts = callback_data.removeprefix("retake_voice:").split(":", 1)
        if len(parts) != 2:
            answer_callback(callback_id, None)
            return
        sid, n_str = parts
        try:
            part_n = int(n_str)
        except ValueError:
            answer_callback(callback_id, None)
            return
        entry = _pending_voice_confirmations.get(chat_id, {}).pop(part_n, None)
        if entry is not None:
            _, pending_wav = entry
            try:
                pending_wav.unlink(missing_ok=True)
            except OSError as exc:
                LOGGER.warning("Failed to delete pending wav %s: %s", pending_wav, exc)
        session = get_voice_session(sid)
        if session is None:
            send_message(chat_id, f"❌ Сесію не знайдено: {sid}", None)
            answer_callback(callback_id, None)
            return
        msg_id = _send_part_script_message(chat_id, session, part_n, send_message)
        if msg_id is not None:
            _part_script_msg_ids.setdefault(chat_id, {})[part_n] = msg_id
        answer_callback(callback_id, "🔄 Надсилаю текст знову...")
        return

    # --- Reddit callbacks ---

    if callback_data.startswith("process:"):
        updated_review = handle_telegram_process_callback(
            callback_data=callback_data,
            chat_id=chat_id,
            send_instruction_message=lambda message: _extract_message_id(
                send_message(chat_id, message, None)
            ),
        )
        if updated_review.telegram.instruction_message_id is None:
            send_message(
                chat_id,
                f"Story {updated_review.story_id} was selected, but the instruction message could not be saved. Retry the action.",
                None,
            )
        answer_callback(callback_id, None)
        return

    if callback_data.startswith(RENDER_CALLBACK_PREFIX):
        story_id = callback_data.removeprefix(RENDER_CALLBACK_PREFIX).strip()
        if not story_id:
            raise RedditIntakeError("Render callback is missing a story_id.")
        if background_video_path is None:
            raise RedditIntakeError(
                "Render is not available: BACKGROUND_VIDEO_PATH is not configured. "
                "Restart the bot with --background-video-path or set BACKGROUND_VIDEO_PATH."
            )
        rendered_asset, queue_result = render_and_enqueue_story_review(
            story_id=story_id,
            background_video_path=background_video_path,
            publish_queue_store=queue_store,
        )
        send_message(
            chat_id,
            " | ".join(
                (
                    f"story_id={story_id}",
                    f"rendered={rendered_asset.output_path}",
                    f"queued_created={'yes' if queue_result.created else 'no'}",
                )
            ),
            None,
        )
        send_video(chat_id, rendered_asset.output_path, f"Rendered story {story_id}")
        answer_callback(callback_id, None)
        return

    answer_callback(callback_id, None)


# ---------------------------------------------------------------------------
# Advice plan UI — /plan command
# ---------------------------------------------------------------------------

def _handle_plan_command(
    *,
    message_payload: Mapping[str, Any],
    send_message: SendMessageBoundary,
    edit_message: EditMessageBoundary,
) -> None:
    chat_id = _require_chat_id(message_payload)
    # Show channel picker first so operator can switch channels on demand.
    text, markup = _build_channel_picker_message(chat_id)
    result = send_message(chat_id, text, markup)
    message_id = _extract_message_id(result)
    if message_id is not None:
        _factory_messages[chat_id] = message_id


# ---------------------------------------------------------------------------
# Advice plan UI — message builders
# ---------------------------------------------------------------------------

def _build_channel_picker_message(chat_id: str) -> tuple[str, Mapping[str, Any]]:
    """Show channel selector. Active channel is highlighted."""
    active_key = _active_channel.get(chat_id, config.DEFAULT_CHANNEL)
    rows: list[list[Mapping[str, Any]]] = []
    for key, profile in config.CHANNEL_PROFILES.items():
        marker = "✅ " if key == active_key else ""
        rows.append([{
            "text": f"{marker}{profile['label']}",
            "callback_data": f"channel:{key}",
        }])
    rows.append([{"text": "❌ Закрити", "callback_data": "close"}])
    active_label = config.CHANNEL_PROFILES[active_key]["label"]
    text = f"📁 Вибері канал\n\nЗараз активний: {active_label}"
    return text, {"inline_keyboard": rows}


def _build_plan_series_list_message(chat_id: str) -> tuple[str, Mapping[str, Any]]:
    plan_path = _channel_plan_path(chat_id)
    profile = _get_channel_profile(chat_id)
    try:
        topics = list_topics(plan_path)
    except Exception as exc:
        raise AdvicePipelineError(f"Не вдалося завантажити план: {exc}") from exc

    # Group by series_id, preserving order of first appearance.
    series_order: list[str] = []
    series_groups: dict[str, list[ContentTopic]] = {}
    for t in topics:
        if t.series_id not in series_groups:
            series_order.append(t.series_id)
            series_groups[t.series_id] = []
        series_groups[t.series_id].append(t)

    lines: list[str] = [f"📋 {profile['label']} — Контент-план\n"]
    keyboard_rows: list[list[Mapping[str, Any]]] = []

    for series_id in series_order:
        series_topics = series_groups[series_id]
        series_title = series_topics[0].series_title
        total = len(series_topics)
        rendered = sum(1 for t in series_topics if t.status == TOPIC_RENDERED)
        if rendered == total:
            emoji = "✅"
        elif rendered > 0:
            emoji = "⏳"
        else:
            emoji = "🆕"
        lines.append(f"{emoji} {series_id}: {series_title} ({rendered}/{total})")
        keyboard_rows.append([{
            "text": f"{emoji} {series_id}: {series_title} →",
            "callback_data": f"series:{series_id}",
        }])

    keyboard_rows.append([{
        "text": "🔄 Змінити канал",
        "callback_data": "back:channel",
    }, {"text": "❌ Закрити", "callback_data": "close"}])
    return "\n".join(lines), {"inline_keyboard": keyboard_rows}


def _build_series_topic_list_message(series_id: str, chat_id: str = "") -> tuple[str, Mapping[str, Any]]:
    plan_path = _channel_plan_path(chat_id) if chat_id else config.CONTENT_PLAN_PATH
    try:
        topics = list_topics(plan_path)
    except Exception as exc:
        raise AdvicePipelineError(f"Не вдалося завантажити план: {exc}") from exc

    series_topics = [t for t in topics if t.series_id == series_id]
    if not series_topics:
        return (
            f"Серія {series_id} не знайдена.",
            {"inline_keyboard": [[{"text": "❌ Закрити", "callback_data": "close"}]]},
        )

    series_title = series_topics[0].series_title
    lines: list[str] = [f"📂 Серія {series_id}: {series_title}\n"]
    keyboard_rows: list[list[Mapping[str, Any]]] = []

    for topic in series_topics:
        if topic.status == TOPIC_RENDERED:
            emoji = "✅"
        elif topic.status == TOPIC_SCRIPT_RECEIVED:
            emoji = "📝"
        else:
            emoji = "🆕"
        lines.append(f"{emoji} {topic.topic_id}: {topic.title}")
        # Button text truncated to 40 chars to keep UI readable.
        # callback_data is always short: "topic:A1" = 8 bytes max.
        btn_label = f"{emoji} {topic.topic_id}: {topic.title}"
        if len(btn_label) > 40:
            btn_label = btn_label[:37] + "..."
        keyboard_rows.append([{
            "text": btn_label,
            "callback_data": f"topic:{topic.topic_id}",
        }])

    keyboard_rows.append([
        {"text": "← Назад", "callback_data": "back:plan"},
        {"text": "❌ Закрити", "callback_data": "close"},
    ])
    return "\n".join(lines), {"inline_keyboard": keyboard_rows}


def _build_topic_detail_message(topic: ContentTopic) -> tuple[str, Mapping[str, Any]]:
    text = (
        f"📌 {topic.topic_id}: {topic.title}\n"
        f"📂 Серія {topic.series_id}: {topic.series_title}\n"
        f"👥 Аудиторія: {topic.audience}\n\n"
        "📋 Натисни «Показати промпт» щоб отримати готовий промпт для AI."
    )
    markup: Mapping[str, Any] = {
        "inline_keyboard": [
            [{"text": "📋 Показати промпт", "callback_data": f"prompt:{topic.topic_id}"}],
            [
                {"text": "← До серії", "callback_data": f"back:{topic.series_id}"},
                {"text": "❌ Закрити", "callback_data": "close"},
            ],
        ]
    }
    return text, markup


def _build_waiting_message(topic: ContentTopic, review_id: str) -> tuple[str, Mapping[str, Any]]:
    text = (
        f"⏳ Очікую скрипт\n\n"
        f"{topic.topic_id}: {topic.title}\n\n"
        f"ID: {review_id}\n\n"
        "Відповідай на повідомлення з промптом, вставивши відповідь від AI."
    )
    markup: Mapping[str, Any] = {
        "inline_keyboard": [
            [{"text": "🔄 Показати промпт знову", "callback_data": f"prompt:{topic.topic_id}"}],
            [{"text": "← Скасувати", "callback_data": f"cancel:{topic.topic_id}"}],
        ]
    }
    return text, markup


def _build_rendering_message(topic: ContentTopic) -> tuple[str, Mapping[str, Any]]:
    text = (
        f"🎬 Рендеримо мікросерію...\n\n"
        f"{topic.topic_id}: {topic.title}\n\n"
        "⏳ Завантажуємо відео, генеруємо озвучку, рендеримо...\n"
        "Це займе кілька хвилин."
    )
    return text, {"inline_keyboard": []}


def _build_done_message(topic: ContentTopic, parts_count: int) -> tuple[str, Mapping[str, Any]]:
    text = (
        f"✅ Готово!\n\n"
        f"{topic.topic_id}: {topic.title}\n"
        f"Мікросерія: {parts_count} відео\n\n"
        "Відео та метадані надіслані вище ↑"
    )
    markup: Mapping[str, Any] = {
        "inline_keyboard": [
            [{"text": "📋 Наступна тема →", "callback_data": "next_topic"}],
            [
                {"text": "↩ До плану", "callback_data": "back:plan"},
                {"text": "❌ Закрити", "callback_data": "close"},
            ],
        ]
    }
    return text, markup


def _build_voice_collection_message(
    session: AdviceVoiceSession,
    topic: ContentTopic,
) -> tuple[str, Mapping[str, Any]]:
    """Build STATE 4b factory message showing voice collection progress."""
    sid = session.session_id
    total = len(session.micro_series.parts)

    current_part: int | None = None
    for idx, vf in enumerate(session.voice_files):
        if vf is None:
            current_part = idx + 1
            break

    lines: list[str] = [
        "🎙 Озвучка мікросерії\n",
        f"📌 {topic.title}",
        f"Частин: {total}\n",
    ]

    for part in session.micro_series.parts:
        n = part.part_number
        vf = session.voice_files[n - 1]
        if vf is not None:
            duration = _wav_duration_str(vf)
            lines.append(f"✅ Частина {n}/{total} — озвучено ({duration})")
        elif n == current_part:
            lines.append(f"🎤 Частина {n}/{total} — очікую голосове...")
        else:
            lines.append(f"⏳ Частина {n}/{total} — очікує")

    if current_part is not None:
        lines.append(
            f"\nВідповідай голосовим на повідомлення з текстом частини {current_part} ↓"
        )

    keyboard_rows: list[list[Mapping[str, Any]]] = []
    if current_part is not None:
        keyboard_rows.append([{
            "text": f"🔄 Надіслати текст частини {current_part} знову",
            "callback_data": f"resend_part:{sid}:{current_part}",
        }])
    keyboard_rows.append([{
        "text": "← Скасувати",
        "callback_data": f"cancel_voice:{sid}",
    }])

    return "\n".join(lines), {"inline_keyboard": keyboard_rows}


def _wav_duration_str(wav_path: Path) -> str:
    try:
        with wave.open(str(wav_path), "rb") as wf:
            frames = wf.getnframes()
            rate = wf.getframerate()
            duration = frames / rate if rate > 0 else 0.0
            minutes = int(duration // 60)
            seconds = int(duration % 60)
            return f"{minutes}:{seconds:02d}"
    except Exception:
        return "?"


def _send_part_script_message(
    chat_id: str,
    session: AdviceVoiceSession,
    part_number: int,
    send_message: SendMessageBoundary,
) -> int | None:
    """Send the script text for a given part and persist the message_id."""
    part = session.micro_series.parts[part_number - 1]
    total = len(session.micro_series.parts)
    text = (
        f"🎤 Частина {part_number}/{total} — озвучуй цей текст голосовим:\n\n"
        f"{part.script_text}"
    )
    result = send_message(chat_id, text, None)
    msg_id = _extract_message_id(result)
    if msg_id is not None:
        save_part_message_id(session.session_id, part_number, msg_id)
    return msg_id


def _handle_voice_reply(
    *,
    message_payload: Mapping[str, Any],
    send_message: SendMessageBoundary,
    download_file: DownloadFileBoundary,
) -> None:
    """Handle an operator voice message that is a reply to a part-script message."""
    chat_id = _require_chat_id(message_payload)

    reply_to = message_payload.get("reply_to_message")
    if not isinstance(reply_to, Mapping):
        send_message(
            chat_id,
            "❌ Голосове повідомлення повинно бути відповіддю на текст частини.",
            None,
        )
        return

    replied_message_id = _extract_message_id(reply_to)
    if replied_message_id is None:
        send_message(chat_id, "❌ Не вдалося визначити повідомлення-ціль.", None)
        return

    lookup = get_voice_session_by_part_message_id(replied_message_id)
    if lookup is None:
        send_message(
            chat_id,
            "❌ Це голосове не відповідає жодній частині. "
            "Відповідай на повідомлення з текстом відповідної частини.",
            None,
        )
        return

    session, part_number = lookup
    review_id = session.review_id

    voice_payload = message_payload.get("voice")
    file_id = voice_payload.get("file_id") if isinstance(voice_payload, Mapping) else None
    if not file_id:
        send_message(chat_id, "❌ Не вдалося отримати файл голосового повідомлення.", None)
        return

    raw_bytes = download_file(str(file_id))

    part_dir = config.ADVICE_OUTPUT_DIR / f"{review_id}_part{part_number}"
    part_dir.mkdir(parents=True, exist_ok=True)
    oga_path = part_dir / "voice_raw.oga"
    oga_path.write_bytes(raw_bytes)

    pending_wav = part_dir / "voice_pending.wav"
    try:
        convert_ogg_to_wav(oga_path, pending_wav)
    except AdvicePipelineError as exc:
        send_message(chat_id, f"❌ Помилка конвертації аудіо: {exc}", None)
        return

    _pending_voice_confirmations.setdefault(chat_id, {})[part_number] = (
        session.session_id, pending_wav
    )
    _send_voice_confirmation_message(chat_id, session, part_number, pending_wav, send_message)


def _send_voice_confirmation_message(
    chat_id: str,
    session: AdviceVoiceSession,
    part_number: int,
    pending_wav: Path,
    send_message: SendMessageBoundary,
) -> None:
    total = len(session.micro_series.parts)
    duration = _wav_duration_str(pending_wav)
    sid = session.session_id
    text = (
        f"🎤 Частина {part_number}/{total} — запис отримано ({duration})\n\n"
        "Перевір голосове вище ↑ і підтвердь або перезапиши."
    )
    markup: Mapping[str, Any] = {
        "inline_keyboard": [
            [{"text": "✅ Прийняти та продовжити", "callback_data": f"confirm_voice:{sid}:{part_number}"}],
            [{"text": "🔄 Перезаписати", "callback_data": f"retake_voice:{sid}:{part_number}"}],
        ]
    }
    send_message(chat_id, text, markup)


def _render_and_send_voice_session(
    *,
    chat_id: str,
    session: AdviceVoiceSession,
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
    edit_message: EditMessageBoundary,
) -> None:
    """Render completed voice session and send all videos + metadata."""
    total = len(session.micro_series.parts)
    send_message(
        chat_id,
        f"🎬 Всі {total} частини озвучено. Починаю рендер — це займе ~{total * 8} хвилин. Чекай...",
        None,
    )
    topic = _get_topic_by_id_safe(session.topic_id)
    if topic is not None:
        _edit_factory(chat_id, *_build_rendering_message(topic), edit_message)

    try:
        results = render_voice_session(session)
    except AdvicePipelineError as exc:
        send_message(chat_id, f"❌ Помилка рендеру: {exc}", None)
        return

    total_rendered = len(results)
    if topic is not None:
        _edit_factory(chat_id, *_build_done_message(topic, total_rendered), edit_message)

    for part_idx, r in enumerate(results, start=1):
        if topic is not None:
            post_meta = generate_post_meta(topic, part_idx, total_rendered)
            send_video(
                chat_id,
                r.output_path,
                f"📺 Частина {part_idx}/{total_rendered} | {topic.title}",
            )
            send_message(chat_id, _format_post_meta_message(post_meta), None)
            candidate = PublishCandidate(
                asset_id=r.script_id,
                clip_path=r.output_path,
                thumbnail_path=r.output_path,
                title=post_meta.title,
                caption=post_meta.description,
                hashtags=post_meta.hashtags,
            )
            queue_store.enqueue_candidate(candidate)
        else:
            send_video(chat_id, r.output_path, f"📺 Частина {part_idx}/{total_rendered}")


def _format_post_meta_message(meta: PostMeta) -> str:
    hashtags_text = " ".join(f"#{tag}" for tag in meta.hashtags)
    return (
        f"📝 Метадані — Частина {meta.part_number}/{meta.total_parts}\n\n"
        f"📌 Заголовок:\n{meta.title}\n\n"
        f"📄 Опис:\n{meta.description}\n\n"
        f"🏷 Хештеги:\n{hashtags_text}"
    )


def _edit_factory(
    chat_id: str,
    text: str,
    markup: Mapping[str, Any] | None,
    edit_message: EditMessageBoundary,
) -> None:
    message_id = _factory_messages.get(chat_id)
    if message_id is None:
        return
    try:
        edit_message(chat_id, message_id, text, markup)
    except Exception as exc:
        LOGGER.warning("Factory message edit failed: %s", exc)


def _find_topic_for_review(review: AdviceReview, chat_id: str = "") -> ContentTopic | None:
    plan_path = _channel_plan_path(chat_id) if chat_id else config.CONTENT_PLAN_PATH
    try:
        for t in list_topics(plan_path):
            if t.title == review.script.topic:
                return t
    except Exception as exc:
        LOGGER.warning("Failed to load topics for review lookup: %s", exc)
    return None


def _get_topic_by_id_safe(topic_id: str, chat_id: str = "") -> ContentTopic | None:
    plan_path = _channel_plan_path(chat_id) if chat_id else config.CONTENT_PLAN_PATH
    try:
        return get_topic_by_id(topic_id, plan_path)
    except Exception as exc:
        LOGGER.warning("Failed to look up topic %s: %s", topic_id, exc)
    return None


def _build_filled_prompt_text(topic: ContentTopic, chat_id: str = "") -> str:
    """Build the filled script-generation prompt for a topic."""
    topic_block = (
        f"Назва: {topic.title}\n"
        f"Серія: {topic.series_title} (Частина {topic.part_number} з {topic.total_parts})\n"
        f"Хук-формула: {topic.hook_formula}\n"
        f"Аудиторія: {topic.audience}\n"
        f"Сценарій: {topic.scenario}\n"
        "Юрфакти:\n"
        + "\n".join(f"• {f}" for f in topic.legal_facts)
    )
    prompt_path = _channel_prompt_path(chat_id) if chat_id else config.SCRIPT_PROMPT_PATH
    try:
        prompt_template = prompt_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise AdvicePipelineError(
            f"Script prompt template not found at {prompt_path}. "
            "Run: python advice_content_cli.py init"
        ) from exc
    return (
        prompt_template
        .replace("{{TOPIC_BLOCK}}", topic_block)
        .replace("{{PART_CURRENT}}", str(topic.part_number))
        .replace("{{PART_TOTAL}}", str(topic.total_parts))
        .replace("{{SERIES_TITLE}}", topic.series_title)
    )


# ---------------------------------------------------------------------------
# Hidden text-command fallbacks (/newtopic, /script, /scripts, /approve, /reject)
# ---------------------------------------------------------------------------

def _handle_newtopic_command(
    *,
    message_payload: Mapping[str, Any],
    send_message: SendMessageBoundary,
) -> None:
    chat_id = _require_chat_id(message_payload)

    review, topic = issue_next_topic()
    script_id = review.script.script_id

    # Message A — topic preview card.
    preview = (
        f"\U0001f4cb \u0422\u0435\u043c\u0430 {topic.topic_id}: {topic.title}\n"
        f"\U0001f4c2 \u0421\u0435\u0440\u0456\u044f {topic.series_id}: {topic.series_title}\n"
        f"\U0001f4cd \u0427\u0430\u0441\u0442\u0438\u043d\u0430 "
        f"{topic.part_number} \u0437 {topic.total_parts}\n\n"
        "\U0001f4a1 \u0412\u0438\u043a\u043e\u0440\u0438\u0441\u0442\u0430\u0439 "
        f"/scripts {script_id} <\u0432\u0456\u0434\u043f\u043e\u0432\u0456\u0434\u044c> "
        "\u0434\u043b\u044f \u043c\u0456\u043a\u0440\u043e\u0441\u0435\u0440\u0456\u0457 "
        "(\u0440\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u043e\u0432\u0430\u043d\u043e)\n"
        "   \u0430\u0431\u043e "
        f"/script {script_id} <\u0442\u0435\u043a\u0441\u0442> "
        "\u0434\u043b\u044f \u043e\u0434\u043d\u043e\u0433\u043e \u0432\u0456\u0434\u0435\u043e\n\n"
        "\U0001f447 \u041d\u0430\u0441\u0442\u0443\u043f\u043d\u0435 "
        "\u043f\u043e\u0432\u0456\u0434\u043e\u043c\u043b\u0435\u043d\u043d\u044f "
        "\u043c\u0456\u0441\u0442\u0438\u0442\u044c \u0433\u043e\u0442\u043e\u0432\u0438\u0439 "
        "\u043f\u0440\u043e\u043c\u043f\u0442."
    )
    send_message(chat_id, preview, None)

    # Message B — filled prompt (operator copies this to ChatGPT/Claude).
    filled_prompt = _build_filled_prompt_text(topic)
    result = _send_long_message(chat_id, filled_prompt, None, send_message)
    message_id = _extract_message_id(result)
    if message_id is not None:
        save_advice_telegram_message_id(script_id, chat_id, message_id)


def _handle_script_command(
    *,
    message_payload: Mapping[str, Any],
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
) -> None:
    chat_id = _require_chat_id(message_payload)
    text = str(message_payload.get("text", "")).strip()
    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        send_message(chat_id, "Usage: /script <script_id> <script text>", None)
        return
    script_id = parts[1].strip()
    script_text = parts[2].strip()
    _process_script_submission(
        chat_id=chat_id,
        script_id=script_id,
        script_text=script_text,
        queue_store=queue_store,
        send_message=send_message,
        send_video=send_video,
    )


def _handle_scripts_command(
    *,
    message_payload: Mapping[str, Any],
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
) -> None:
    chat_id = _require_chat_id(message_payload)
    text = str(message_payload.get("text", "")).strip()
    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        send_message(chat_id, "Usage: /scripts <review_id> <AI \u0432\u0456\u0434\u043f\u043e\u0432\u0456\u0434\u044c>", None)
        return
    review_id = parts[1].strip()
    raw_response = parts[2].strip()
    _process_scripts_submission(
        chat_id=chat_id,
        review_id=review_id,
        raw_response=raw_response,
        queue_store=queue_store,
        send_message=send_message,
        send_video=send_video,
    )


def _process_scripts_submission(
    *,
    chat_id: str,
    review_id: str,
    raw_response: str,
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
) -> None:
    review = get_advice_review(review_id)
    if review is None:
        send_message(chat_id, f"\u274c review_id \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e: {review_id}", None)
        return

    topic = None
    try:
        for t in list_topics():
            if t.title == review.script.topic:
                topic = t
                break
    except Exception as exc:
        send_message(chat_id, f"\u274c \u041f\u043e\u043c\u0438\u043b\u043a\u0430 \u0437\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043d\u044f \u043f\u043b\u0430\u043d\u0443: {exc}", None)
        return

    if topic is None:
        send_message(
            chat_id,
            f"\u274c \u0422\u0435\u043c\u0443 \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e: {review.script.topic!r}",
            None,
        )
        return

    send_message(chat_id, "\u23f3 \u041f\u0430\u0440\u0441\u0438\u043c\u043e \u0432\u0456\u0434\u043f\u043e\u0432\u0456\u0434\u044c \u0442\u0430 \u043f\u043e\u0447\u0438\u043d\u0430\u0454\u043c\u043e \u0440\u0435\u043d\u0434\u0435\u0440...", None)

    try:
        result = receive_operator_scripts(review_id, raw_response, topic)
    except AdvicePipelineError as exc:
        send_message(
            chat_id,
            f"\u274c \u041f\u043e\u043c\u0438\u043b\u043a\u0430 \u043f\u0430\u0440\u0441\u0438\u043d\u0433\u0443:\n{exc}\n\n\u041f\u043e\u0432\u043d\u0430 \u0432\u0456\u0434\u043f\u043e\u0432\u0456\u0434\u044c AI:\n{raw_response[:500]}",
            None,
        )
        return

    if isinstance(result, AdviceVoiceSession):
        send_message(
            chat_id,
            f"\u2705 \u0421\u0435\u0441\u0456\u044e \u0441\u0442\u0432\u043e\u0440\u0435\u043d\u043e. "
            f"\u041d\u0430\u0434\u0456\u0448\u043b\u0438 \u0447\u0430\u0441\u0442\u0438\u043d\u0438 \u0433\u043e\u043b\u043e\u0441\u043e\u0432\u0438\u043c\u0438 \u043f\u043e\u0432\u0456\u0434\u043e\u043c\u043b\u0435\u043d\u043d\u044f\u043c\u0438.",
            None,
        )
        return

    results = result
    total = len(results)
    for idx, r in enumerate(results, start=1):
        post_meta = generate_post_meta(topic, idx, total)
        send_video(chat_id, r.output_path, f"📺 Частина {idx}/{total} | {topic.title}")
        send_message(chat_id, _format_post_meta_message(post_meta), None)
        candidate = PublishCandidate(
            asset_id=r.script_id,
            clip_path=r.output_path,
            thumbnail_path=r.output_path,
            title=post_meta.title,
            caption=post_meta.description,
            hashtags=post_meta.hashtags,
        )
        queue_store.enqueue_candidate(candidate)

    send_message(
        chat_id,
        f"\u2705 \u041c\u0456\u043a\u0440\u043e\u0441\u0435\u0440\u0456\u044f \u0433\u043e\u0442\u043e\u0432\u0430: {total} \u0432\u0456\u0434\u0435\u043e \u0432 \u0447\u0435\u0440\u0437\u0456 \u043f\u0443\u0431\u043b\u0456\u043a\u0430\u0446\u0456\u0457.",
        None,
    )


def _process_script_submission(
    *,
    chat_id: str,
    script_id: str,
    script_text: str,
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
) -> None:
    word_count = len(script_text.split())
    receive_operator_script(script_id, script_text)
    send_message(
        chat_id,
        f"\u2705 \u0421\u043a\u0440\u0438\u043f\u0442 \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e "
        f"({word_count} \u0441\u043b\u0456\u0432). \u041f\u043e\u0447\u0438\u043d\u0430\u044e \u0440\u0435\u043d\u0434\u0435\u0440...",
        None,
    )

    result = approve_advice_script(script_id)

    # Look up topic metadata for the video caption.
    review = get_advice_review(script_id)
    topic_title = review.script.topic if review is not None else script_id
    part = total = 1
    try:
        for t in list_topics():
            if review is not None and t.title == review.script.topic:
                part = t.part_number
                total = t.total_parts
                break
    except Exception:
        pass

    send_video(chat_id, result.output_path, f"\U0001f4fa {topic_title} | \u0427.{part}/{total}")

    candidate = PublishCandidate(
        asset_id=script_id,
        clip_path=result.output_path,
        thumbnail_path=result.output_path,
        title=topic_title,
        caption=None,
        hashtags=(),
    )
    queue_store.enqueue_candidate(candidate)
    send_message(
        chat_id,
        "\u2705 \u0412\u0456\u0434\u0435\u043e \u0433\u043e\u0442\u043e\u0432\u0435 \u0456 "
        "\u0432 \u0447\u0435\u0440\u0437\u0456 \u043f\u0443\u0431\u043b\u0456\u043a\u0430\u0446\u0456\u0457.",
        None,
    )


def _handle_approve_command(
    *,
    message_payload: Mapping[str, Any],
    queue_store: PublishQueueStore,
    send_message: SendMessageBoundary,
    send_video: SendVideoBoundary,
) -> None:
    chat_id = _require_chat_id(message_payload)
    text = str(message_payload.get("text", "")).strip()
    parts = text.split(maxsplit=1)
    script_id = parts[1].strip() if len(parts) > 1 else ""
    if not script_id:
        send_message(chat_id, "Usage: /approve <script_id>", None)
        return

    result = approve_advice_script(script_id)
    send_message(chat_id, f"Rendering advice video for {script_id}...", None)
    send_video(chat_id, result.output_path, f"Advice: {script_id}")

    review = get_advice_review(script_id)
    title = review.script.topic if review is not None else script_id
    candidate = PublishCandidate(
        asset_id=script_id,
        clip_path=result.output_path,
        thumbnail_path=result.output_path,
        title=title,
        caption=None,
        hashtags=(),
    )
    queue_store.enqueue_candidate(candidate)
    send_message(chat_id, f"Enqueued: {script_id}", None)


def _handle_reject_command(
    *,
    message_payload: Mapping[str, Any],
    send_message: SendMessageBoundary,
) -> None:
    chat_id = _require_chat_id(message_payload)
    text = str(message_payload.get("text", "")).strip()
    parts = text.split(maxsplit=1)
    script_id = parts[1].strip() if len(parts) > 1 else ""
    if not script_id:
        send_message(chat_id, "Usage: /reject <script_id>", None)
        return

    reject_advice_script(script_id)
    send_message(chat_id, f"Rejected: {script_id}", None)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _format_status_message(
    stats: StoryReviewStats,
    advice_stats: AdviceReviewStats,
) -> str:
    reddit_part = " | ".join(
        (
            f"total={stats.total}",
            f"pending_review={stats.pending_review}",
            f"approved={stats.approved}",
            f"rejected={stats.rejected}",
            f"sent_to_telegram={stats.sent_to_telegram}",
            f"eligible_to_send={stats.eligible_to_send}",
        )
    )
    advice_part = " | ".join(
        (
            f"advice_total={advice_stats.total}",
            f"advice_pending={advice_stats.pending_review}",
            f"advice_approved={advice_stats.approved}",
        )
    )
    return f"{reddit_part}\n{advice_part}"


def _build_render_reply_markup(story_id: str) -> Mapping[str, Any]:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "Render",
                    "callback_data": f"{RENDER_CALLBACK_PREFIX}{story_id}",
                }
            ]
        ]
    }


def _send_operator_error(
    *,
    chat_id: str,
    message_sender: SendMessageBoundary,
    error_text: str,
) -> None:
    try:
        message_sender(chat_id, f"Error: {error_text}", None)
    except Exception:
        LOGGER.exception("Failed to send Telegram error acknowledgment to chat %s", chat_id)


# ---------------------------------------------------------------------------
# Default boundary implementations
# ---------------------------------------------------------------------------

def _default_get_updates(offset: int | None) -> Sequence[Mapping[str, Any]]:
    bot_token = _require_bot_token()
    query = f"&offset={offset}" if offset is not None else ""
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/getUpdates?timeout=30{query}",
        method="GET",
    )
    with urllib.request.urlopen(request) as response:
        payload = json.loads(response.read().decode("utf-8"))
    result = payload.get("result", ())
    if not isinstance(result, Sequence) or isinstance(result, (str, bytes)):
        raise RedditIntakeError("Unexpected Telegram getUpdates response payload.")
    return tuple(item for item in result if isinstance(item, Mapping))


def _default_send_message(
    chat_id: str,
    text: str,
    reply_markup: Mapping[str, Any] | None,
) -> Mapping[str, Any]:
    bot_token = _require_bot_token()
    request_payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_markup is not None:
        request_payload["reply_markup"] = reply_markup
    payload = json.dumps(request_payload).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        response_payload = json.loads(response.read().decode("utf-8"))
    result = response_payload.get("result")
    if not isinstance(result, Mapping):
        raise RedditIntakeError("Unexpected Telegram sendMessage response payload.")
    return result


def _default_send_video(
    chat_id: str,
    video_path: Path,
    caption: str | None,
) -> Mapping[str, Any]:
    if not video_path.is_file():
        raise RedditIntakeError(f"Telegram video file does not exist: {video_path}")

    bot_token = _require_bot_token()
    boundary = "ttcf-telegram-video"
    body = _build_multipart_body(
        boundary=boundary,
        fields={
            "chat_id": chat_id,
            "caption": caption or "",
        },
        file_field_name="video",
        file_path=video_path,
        file_content_type="video/mp4",
    )
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/sendVideo",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        response_payload = json.loads(response.read().decode("utf-8"))
    result = response_payload.get("result")
    if not isinstance(result, Mapping):
        raise RedditIntakeError("Unexpected Telegram sendVideo response payload.")
    return result


def _default_edit_message(
    chat_id: str,
    message_id: int,
    text: str,
    reply_markup: Mapping[str, Any] | None,
) -> Mapping[str, Any]:
    bot_token = _require_bot_token()
    request_payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }
    if reply_markup is not None:
        request_payload["reply_markup"] = reply_markup
    payload = json.dumps(request_payload).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/editMessageText",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
        result = response_payload.get("result")
        if not isinstance(result, Mapping):
            return {}
        return result
    except Exception as exc:
        # Telegram returns 400 when text is unchanged — treat as no-op.
        LOGGER.debug("editMessageText non-critical error: %s", exc)
        return {}


def _default_download_file(file_id: str) -> bytes:
    """GET /getFile to resolve the path, then download the file bytes."""
    bot_token = _require_bot_token()
    get_file_request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/getFile?file_id={file_id}",
        method="GET",
    )
    with urllib.request.urlopen(get_file_request) as response:
        payload = json.loads(response.read().decode("utf-8"))
    file_path = payload.get("result", {}).get("file_path")
    if not file_path:
        raise RedditIntakeError(f"Could not resolve file path for file_id={file_id!r}")
    download_request = urllib.request.Request(
        f"https://api.telegram.org/file/bot{bot_token}/{file_path}",
        method="GET",
    )
    with urllib.request.urlopen(download_request) as response:
        return response.read()


def _default_answer_callback(callback_query_id: str, text: str | None) -> None:
    bot_token = _require_bot_token()
    request_payload: dict[str, Any] = {"callback_query_id": callback_query_id}
    if text is not None:
        request_payload["text"] = text
    payload = json.dumps(request_payload).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            response.read()
    except Exception as exc:
        LOGGER.debug("answerCallbackQuery non-critical error: %s", exc)


def _build_multipart_body(
    *,
    boundary: str,
    fields: Mapping[str, str],
    file_field_name: str,
    file_path: Path,
    file_content_type: str,
) -> bytes:
    parts: list[bytes] = []
    for field_name, value in fields.items():
        parts.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{field_name}"\r\n\r\n'.encode("utf-8"),
                value.encode("utf-8"),
                b"\r\n",
            ]
        )

    parts.extend(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            (
                f'Content-Disposition: form-data; name="{file_field_name}"; '
                f'filename="{file_path.name}"\r\n'
            ).encode("utf-8"),
            f"Content-Type: {file_content_type}\r\n\r\n".encode("utf-8"),
            file_path.read_bytes(),
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    return b"".join(parts)


def _require_bot_token() -> str:
    if not config.TELEGRAM_BOT_TOKEN:
        raise RedditIntakeError("Telegram bot token is not configured.")
    return config.TELEGRAM_BOT_TOKEN


def _require_chat_id(message_payload: Mapping[str, Any]) -> str:
    chat_id = _extract_chat_id(message_payload)
    if chat_id is None:
        raise RedditIntakeError("Telegram chat_id is missing from update payload.")
    return chat_id


def _extract_chat_id(message_payload: Mapping[str, Any]) -> str | None:
    chat_payload = message_payload.get("chat")
    if not isinstance(chat_payload, Mapping):
        return None
    chat_id = chat_payload.get("id")
    if chat_id is None:
        return None
    return str(chat_id)


def _extract_update_id(update: Mapping[str, Any]) -> int | None:
    update_id = update.get("update_id")
    try:
        return int(update_id)
    except (TypeError, ValueError):
        return None


def _extract_message_id(result_payload: Mapping[str, Any]) -> int | None:
    message_id = result_payload.get("message_id")
    try:
        return int(message_id)
    except (TypeError, ValueError):
        return None


def _extract_update_chat_id(update: Mapping[str, Any]) -> str | None:
    message_payload = update.get("message")
    if isinstance(message_payload, Mapping):
        return _extract_chat_id(message_payload)
    callback_query = update.get("callback_query")
    if isinstance(callback_query, Mapping):
        callback_message = callback_query.get("message")
        if isinstance(callback_message, Mapping):
            return _extract_chat_id(callback_message)
    return None


def _is_command_message(message_payload: Mapping[str, Any]) -> bool:
    text = str(message_payload.get("text", "")).strip()
    return text.startswith("/")
