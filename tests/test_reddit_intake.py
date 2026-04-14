from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Callable, cast
from unittest.mock import ANY, patch

from src import config
from src import reddit_intake
from src.publisher import PublishQueueStore
from src.reddit_intake import (
    APPROVED_STATUS,
    CANDIDATE_ACCEPTED_STATUS,
    CANDIDATE_FAILED_STATUS,
    CANDIDATE_REJECTED_STATUS,
    PENDING_REVIEW_STATUS,
    REJECTED_STATUS,
    ApprovedStoryPackage,
    PipelineRunResult,
    RedditIntakeError,
    RESPEECHER_PROVIDER,
    StoryReviewTelegramSendError,
    StoryCandidate,
    StoryIntakeStage,
    StoryModerationStage,
    StoryProductionStage,
    StoredStoryReview,
    TelegramReviewMetadata,
    bypass_translation_with_ukrainian_script,
    build_approved_story_package,
    build_process_callback_data,
    fetch_live_story_candidates,
    filter_story_candidate,
    format_telegram_approval_message,
    generate_tts_audio,
    get_story_review,
    get_story_review_stats,
    handle_telegram_process_callback,
    ingest_telegram_rewrite_reply,
    is_story_ready_for_render,
    list_story_reviews,
    list_available_respeecher_voices,
    mark_story_selected_for_processing,
    prepare_story_candidates,
    process_telegram_update,
    process_operator_reply_and_translate,
    render_and_enqueue_story_review,
    render_approved_story,
    render_story_review,
    render_story_video,
    resolve_respeecher_voice_id,
    run_full_pipeline,
    save_operator_rewrite,
    save_story_candidates_for_review,
    save_operator_rewritten_text,
    set_translated_script,
    send_pending_story_reviews_to_telegram,
    send_story_reviews_to_telegram,
    send_telegram_approval_message,
    translate_story_to_ukrainian,
    translate_story_review_to_ukrainian,
    update_story_review_status,
    validate_story_review_state,
    _process_single_candidate,
    _safe_sum,
    _validate_no_duplicate_parameters,
)
from src.subtitles import SubtitleGenerationError
from src.translator import TranslationError


class RedditIntakeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.base_dir = Path(self.temp_dir.name)
        self.inbox_dir = self.base_dir / "inbox"
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.background_video_path = self.base_dir / "background.mp4"
        self.background_video_path.write_bytes(b"background-video")
        self.review_store_path = self.base_dir / "reddit_story_reviews.json"

    def test_fetch_live_story_candidates_normalizes_mocked_reddit_json(self) -> None:
        fetched_urls: list[str] = []
        fetched_headers: list[dict[str, str]] = []

        def fake_fetch_json(url: str, headers: dict[str, str]):
            fetched_urls.append(url)
            fetched_headers.append(headers)
            if "AmItheAsshole/hot" in url:
                return self._reddit_listing_payload(
                    subreddit="AmItheAsshole",
                    posts=[
                        {
                            "id": "story-001",
                            "title": "AITA example",
                            "selftext": "Body one",
                            "score": 1400,
                            "permalink": "/r/AmItheAsshole/comments/story001/example/",
                            "created_utc": 1_700_000_001.0,
                        }
                    ],
                )
            return self._reddit_listing_payload(subreddit="AmItheAsshole", posts=[])

        candidates = fetch_live_story_candidates(
            subreddits=("AmItheAsshole",),
            listings=("hot",),
            limit_per_listing=1,
            fetch_json=fake_fetch_json,
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].post_id, "story-001")
        self.assertEqual(candidates[0].subreddit, "AmItheAsshole")
        self.assertEqual(candidates[0].title, "AITA example")
        self.assertEqual(candidates[0].body, "Body one")
        self.assertEqual(candidates[0].score, 1400)
        self.assertEqual(
            candidates[0].url,
            "https://www.reddit.com/r/AmItheAsshole/comments/story001/example/",
        )
        self.assertEqual(fetched_urls[0], "https://www.reddit.com/r/AmItheAsshole/hot.json?limit=1&raw_json=1")
        self.assertEqual(fetched_headers[0], {"User-Agent": config.REDDIT_USER_AGENT})

    def test_candidate_filtering(self) -> None:
        accepted, accepted_reason = filter_story_candidate(self._build_candidate())
        low_score, low_score_reason = filter_story_candidate(
            self._build_candidate(score=config.REDDIT_MIN_SCORE - 1)
        )
        disallowed_subreddit, disallowed_subreddit_reason = filter_story_candidate(
            self._build_candidate(subreddit="AskReddit")
        )
        empty_body, empty_body_reason = filter_story_candidate(self._build_candidate(body="   "))
        short_text, short_text_reason = filter_story_candidate(
            self._build_candidate(body="too short"),
            minimum_combined_length=500,
        )

        self.assertTrue(accepted)
        self.assertIsNone(accepted_reason)
        self.assertFalse(low_score)
        self.assertIn("Score is below threshold", low_score_reason)
        self.assertFalse(disallowed_subreddit)
        self.assertIn("Subreddit is not allowed", disallowed_subreddit_reason)
        self.assertFalse(empty_body)
        self.assertEqual(empty_body_reason, "Body is required.")
        self.assertFalse(short_text)
        self.assertIn("Combined title/body length is below threshold", short_text_reason)

    def test_prepare_story_candidates_marks_bad_item_without_killing_batch(self) -> None:
        result = prepare_story_candidates(
            fetch_candidates=lambda: [
                self._build_candidate(post_id="ok-001"),
                {"post_id": "bad-001", "subreddit": "AmItheAsshole"},
            ],
        )

        self.assertEqual(len(result.results), 2)
        self.assertEqual(result.results[0].status, CANDIDATE_ACCEPTED_STATUS)
        self.assertEqual(result.results[1].status, CANDIDATE_FAILED_STATUS)
        self.assertIn("Missing candidate field(s)", result.results[1].reason)

    def test_prepare_story_candidates_marks_filtered_item_as_rejected(self) -> None:
        result = prepare_story_candidates(
            fetch_candidates=lambda: [self._build_candidate(score=config.REDDIT_MIN_SCORE - 1)],
        )

        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.results[0].status, CANDIDATE_REJECTED_STATUS)
        self.assertIn("Score is below threshold", result.results[0].reason)

    def test_prepare_story_candidates_no_nested_call_bug(self) -> None:
        candidate = self._build_candidate(post_id="prep-001")

        with patch(
            "src.reddit_intake._build_candidate_preparation_result",
            wraps=reddit_intake._build_candidate_preparation_result,
        ) as result_builder_mock:
            result = prepare_story_candidates(fetch_candidates=lambda: [candidate])

        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.results[0].status, CANDIDATE_ACCEPTED_STATUS)
        result_builder_mock.assert_called_once()

    def test_prepare_story_candidates_is_pure_and_single_pass(self) -> None:
        first_candidate = self._build_candidate(post_id="pure-001")
        second_candidate = self._build_candidate(post_id="pure-002")
        raw_candidates = (
            first_candidate,
            second_candidate,
            {"post_id": "bad-001", "subreddit": "AmItheAsshole"},
        )
        calls = 0
        original_processor = reddit_intake._process_single_candidate

        def fetch_candidates():
            for raw_candidate in raw_candidates:
                yield raw_candidate

        def counting_processor(*args, **kwargs):
            nonlocal calls
            calls += 1
            return original_processor(*args, **kwargs)

        with patch(
            "src.reddit_intake._process_single_candidate",
            side_effect=counting_processor,
        ) as process_mock:
            result = prepare_story_candidates(fetch_candidates=fetch_candidates)

        self.assertEqual(calls, 3)
        self.assertEqual(process_mock.call_count, 3)
        self.assertEqual(
            tuple(item.post_id for item in result.results),
            ("pure-001", "pure-002", "bad-001"),
        )
        self.assertEqual(
            tuple(item.status for item in result.results),
            (
                CANDIDATE_ACCEPTED_STATUS,
                CANDIDATE_ACCEPTED_STATUS,
                CANDIDATE_FAILED_STATUS,
            ),
        )
        self.assertEqual(len(result.results), 3)

    def test_stats_eligible_count_no_duplicate_tokens(self) -> None:
        pending_unsent = self._build_candidate(post_id="stats-eligible-001")
        pending_sent = self._build_candidate(post_id="stats-eligible-002")
        approved_unsent = self._build_candidate(post_id="stats-eligible-003")

        save_story_candidates_for_review(
            (pending_unsent, pending_sent, approved_unsent),
            store_path=self.review_store_path,
        )
        with patch(
            "src.reddit_intake.send_story_reviews_to_telegram",
            return_value=("stats-eligible-002",),
        ):
            send_pending_story_reviews_to_telegram(store_path=self.review_store_path)
        update_story_review_status(
            story_id="stats-eligible-003",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )

        stats = get_story_review_stats(store_path=self.review_store_path)

        self.assertEqual(stats.total, 3)
        self.assertEqual(stats.pending_review, 2)
        self.assertEqual(stats.sent_to_telegram, 1)
        self.assertEqual(stats.eligible_to_send, 1)

    def test_internal_sum_helper_rejects_invalid_input(self) -> None:
        with self.assertRaises(RuntimeError) as context:
            _safe_sum("11")

        self.assertEqual(str(context.exception), "safe_sum received an invalid iterable.")

    def test_duplicate_parameter_detection_fails(self) -> None:
        namespace: dict[str, object] = {}
        exec(
            "def duplicate_params(a, b):\n"
            "    return a + b\n",
            namespace,
        )
        duplicate_params = cast(Callable[..., object], namespace["duplicate_params"])

        class FakeParameters:
            def keys(self):
                return ["minimum_score", "minimum_score"]

        class FakeSignature:
            parameters = FakeParameters()

        with (
            patch("src.reddit_intake.inspect.signature", return_value=FakeSignature()),
            self.assertRaises(RedditIntakeError) as context,
        ):
            _validate_no_duplicate_parameters(duplicate_params)

        self.assertEqual(
            str(context.exception),
            "Duplicate parameter names detected in duplicate_params: minimum_score",
        )

    def test_no_duplicate_parameters_in_core_functions(self) -> None:
        _validate_no_duplicate_parameters(filter_story_candidate)
        _validate_no_duplicate_parameters(_process_single_candidate)

    def test_format_telegram_approval_message(self) -> None:
        candidate = self._build_candidate()

        message = format_telegram_approval_message(candidate, excerpt_length=40)

        self.assertIn(f"Candidate ID: {candidate.post_id}", message)
        self.assertIn(f"Subreddit: r/{candidate.subreddit}", message)
        self.assertIn(f"Score: {candidate.score}", message)
        self.assertIn(f"Title: {candidate.title}", message)
        self.assertIn(f"Source URL: {candidate.url}", message)
        self.assertIn("Excerpt:", message)

    def test_send_telegram_approval_message_is_mockable(self) -> None:
        captured_request: dict[str, object] = {}

        def fake_sender(url: str, payload: bytes, headers: dict[str, str]) -> bytes:
            captured_request["url"] = url
            captured_request["payload"] = json.loads(payload.decode("utf-8"))
            captured_request["headers"] = headers
            return b'{"ok": true, "result": {"message_id": 1001}}'

        send_telegram_approval_message(
            message="approve this story",
            bot_token="bot-token",
            chat_id="chat-id",
            http_post=fake_sender,
        )

        self.assertEqual(
            captured_request["payload"],
            {"chat_id": "chat-id", "text": "approve this story"},
        )
        self.assertEqual(
            captured_request["headers"],
            {"Content-Type": "application/json; charset=utf-8"},
        )

    def test_send_story_reviews_to_telegram_includes_inline_process_button(self) -> None:
        captured_request: dict[str, object] = {}
        candidate = self._build_candidate(post_id="story-inline-001")
        saved_record = save_story_candidates_for_review(
            candidates=(candidate,),
            store_path=self.review_store_path,
        )[0]

        def fake_sender(url: str, payload: bytes, headers: dict[str, str]) -> bytes:
            del url, headers
            captured_request["payload"] = json.loads(payload.decode("utf-8"))
            return b'{"ok": true, "result": {"message_id": 1002}}'

        with (
            patch("src.reddit_intake.config.TELEGRAM_BOT_TOKEN", "bot-token"),
            patch("src.reddit_intake.config.TELEGRAM_CHAT_ID", "chat-id"),
        ):
            sent_ids = send_story_reviews_to_telegram((saved_record,), http_post=fake_sender)

        self.assertEqual(sent_ids, ("story-inline-001",))
        self.assertEqual(
            captured_request["payload"]["reply_markup"]["inline_keyboard"][0][0]["text"],
            "Process",
        )
        self.assertEqual(
            captured_request["payload"]["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            build_process_callback_data("story-inline-001"),
        )

    def test_send_story_reviews_to_telegram_returns_sent_story_ids(self) -> None:
        sent_payloads: list[str] = []
        candidate = self._build_candidate(post_id="story-telegram-001")
        saved_record = save_story_candidates_for_review(
            candidates=(candidate,),
            store_path=self.review_store_path,
        )[0]

        def fake_sender(url: str, payload: bytes, headers: dict[str, str]) -> bytes:
            del url, headers
            sent_payloads.append(payload.decode("utf-8"))
            return b'{"ok": true, "result": {"message_id": 1003}}'

        with (
            patch("src.reddit_intake.config.TELEGRAM_BOT_TOKEN", "bot-token"),
            patch("src.reddit_intake.config.TELEGRAM_CHAT_ID", "chat-id"),
        ):
            sent_ids = send_story_reviews_to_telegram((saved_record,), http_post=fake_sender)

        self.assertEqual(sent_ids, ("story-telegram-001",))
        self.assertEqual(len(sent_payloads), 1)
        self.assertIn("story-telegram-001", sent_payloads[0])

    def test_no_double_send_on_telegram(self) -> None:
        send_calls = 0
        candidate = self._build_candidate(post_id="story-telegram-single-001")
        saved_record = save_story_candidates_for_review(
            candidates=(candidate,),
            store_path=self.review_store_path,
        )[0]

        def fake_sender(url: str, payload: bytes, headers: dict[str, str]) -> bytes:
            del url, payload, headers
            nonlocal send_calls
            send_calls += 1
            return b'{"ok": true, "result": {"message_id": 1004}}'

        with (
            patch("src.reddit_intake.config.TELEGRAM_BOT_TOKEN", "bot-token"),
            patch("src.reddit_intake.config.TELEGRAM_CHAT_ID", "chat-id"),
        ):
            sent_ids = send_story_reviews_to_telegram((saved_record,), http_post=fake_sender)

        self.assertEqual(sent_ids, ("story-telegram-single-001",))
        self.assertEqual(send_calls, 1)

    def test_story_review_local_persistence(self) -> None:
        candidate = self._build_candidate(post_id="story-store-001")

        saved_records = save_story_candidates_for_review(
            candidates=(candidate,),
            store_path=self.review_store_path,
        )
        approved_record = update_story_review_status(
            story_id="story-store-001",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )

        self.assertEqual(saved_records[0].status, PENDING_REVIEW_STATUS)
        self.assertFalse(saved_records[0].sent_to_telegram)
        self.assertEqual(approved_record.status, APPROVED_STATUS)
        self.assertFalse(approved_record.sent_to_telegram)
        self.assertTrue(self.review_store_path.exists())
        self.assertEqual(get_story_review("story-store-001", self.review_store_path), approved_record)
        self.assertEqual(
            list_story_reviews(store_path=self.review_store_path, status=APPROVED_STATUS),
            (approved_record,),
        )

    def test_old_json_records_load_with_new_fields_defaulted(self) -> None:
        self.review_store_path.write_text(
            json.dumps(
                [
                    {
                        "story_id": "legacy-story-001",
                        "source_title": "Legacy title",
                        "source_body": "Legacy body",
                        "subreddit": "AmItheAsshole",
                        "url": "https://reddit.example/posts/legacy-story-001",
                        "score": config.REDDIT_MIN_SCORE + 100,
                        "created_utc": 1700000000.0,
                        "status": PENDING_REVIEW_STATUS,
                        "saved_at": "2026-03-22T12:00:00Z",
                    }
                ]
            ),
            encoding="utf-8",
        )

        review = get_story_review("legacy-story-001", store_path=self.review_store_path)

        self.assertIsNotNone(review)
        self.assertFalse(review.sent_to_telegram)
        self.assertFalse(review.selected_for_processing)
        self.assertIsNone(review.operator_rewritten_text)
        self.assertIsNone(review.translated_script_uk)
        self.assertIsNone(review.translation_characters_used)
        self.assertIsNone(review.script_saved_at)

    def test_list_story_reviews_no_double_iteration(self) -> None:
        candidate = self._build_candidate(post_id="list-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)

        with patch(
            "src.reddit_intake._load_story_reviews",
            wraps=reddit_intake._load_story_reviews,
        ) as load_mock:
            reviews = list_story_reviews(
                store_path=self.review_store_path,
                status=PENDING_REVIEW_STATUS,
                sent_to_telegram=False,
            )

        self.assertEqual(len(reviews), 1)
        load_mock.assert_called_once_with(self.review_store_path)

    def test_validate_story_review_state_rejects_rewritten_text_without_selection(self) -> None:
        record = StoredStoryReview(
            intake=StoryIntakeStage(
                story_id="invalid-review-001",
                source_title="Title",
                source_body="Body",
                subreddit="AmItheAsshole",
                url="https://reddit.example/posts/invalid-review-001",
                score=config.REDDIT_MIN_SCORE + 100,
                created_utc=1700000000.0,
            ),
            moderation=StoryModerationStage(
                selected_for_processing=False,
                operator_rewritten_text="Manual rewrite",
                script_saved_at="2026-03-22T12:00:00Z",
            ),
            production=StoryProductionStage(
                translated_script_uk=None,
                translation_characters_used=None,
            ),
            telegram=TelegramReviewMetadata(
                chat_id=None,
                instruction_message_id=None,
            ),
            status=PENDING_REVIEW_STATUS,
            sent_to_telegram=False,
            saved_at="2026-03-22T12:00:00Z",
        )

        with self.assertRaises(RedditIntakeError) as context:
            validate_story_review_state(record)

        self.assertEqual(
            str(context.exception),
            "Invalid story review state for invalid-review-001: rewritten text requires selected_for_processing=True",
        )

    def test_validate_story_review_state_rejects_translated_script_without_rewritten_text(self) -> None:
        record = StoredStoryReview(
            intake=StoryIntakeStage(
                story_id="invalid-review-002",
                source_title="Title",
                source_body="Body",
                subreddit="AmItheAsshole",
                url="https://reddit.example/posts/invalid-review-002",
                score=config.REDDIT_MIN_SCORE + 100,
                created_utc=1700000000.0,
            ),
            moderation=StoryModerationStage(
                selected_for_processing=True,
                operator_rewritten_text=None,
                script_saved_at=None,
            ),
            production=StoryProductionStage(
                translated_script_uk="Перекладений текст",
                translation_characters_used=42,
            ),
            telegram=TelegramReviewMetadata(
                chat_id=None,
                instruction_message_id=None,
            ),
            status=PENDING_REVIEW_STATUS,
            sent_to_telegram=False,
            saved_at="2026-03-22T12:00:00Z",
        )

        with self.assertRaises(RedditIntakeError) as context:
            validate_story_review_state(record)

        self.assertEqual(
            str(context.exception),
            "Invalid story review state for invalid-review-002: translated_script_uk requires operator_rewritten_text",
        )

    def test_invalid_usage_state_fails(self) -> None:
        record = StoredStoryReview(
            intake=StoryIntakeStage(
                story_id="invalid-review-003",
                source_title="Title",
                source_body="Body",
                subreddit="AmItheAsshole",
                url="https://reddit.example/posts/invalid-review-003",
                score=config.REDDIT_MIN_SCORE + 100,
                created_utc=1700000000.0,
            ),
            moderation=StoryModerationStage(
                selected_for_processing=True,
                operator_rewritten_text="Manual rewrite",
                script_saved_at="2026-03-22T12:00:00Z",
            ),
            production=StoryProductionStage(
                translated_script_uk=None,
                translation_characters_used=55,
            ),
            telegram=TelegramReviewMetadata(
                chat_id=None,
                instruction_message_id=None,
            ),
            status=PENDING_REVIEW_STATUS,
            sent_to_telegram=False,
            saved_at="2026-03-22T12:00:00Z",
        )

        with self.assertRaises(RedditIntakeError) as context:
            validate_story_review_state(record)

        self.assertEqual(
            str(context.exception),
            "Invalid story review state for invalid-review-003: translation_characters_used requires translated_script_uk",
        )

    def test_translate_before_rewrite_fails(self) -> None:
        candidate = self._build_candidate(post_id="story-translate-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-translate-001",
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            set_translated_script(
                story_id="story-translate-001",
                translated_text="Перекладений текст без рерайту.",
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Story must have rewritten text before saving a translated script: story-translate-001",
        )

    def test_double_rewrite_fails(self) -> None:
        candidate = self._build_candidate(post_id="story-rewrite-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-rewrite-001",
            store_path=self.review_store_path,
        )
        save_operator_rewrite(
            story_id="story-rewrite-001",
            text="First rewrite.",
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            save_operator_rewrite(
                story_id="story-rewrite-001",
                text="Second rewrite.",
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Rewritten script already exists for story: story-rewrite-001",
        )

    def test_double_translate_fails(self) -> None:
        candidate = self._build_candidate(post_id="story-translate-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-translate-002",
            store_path=self.review_store_path,
        )
        save_operator_rewrite(
            story_id="story-translate-002",
            text="Prepared rewritten text.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-translate-002",
            translated_text="Перший переклад.",
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            set_translated_script(
                story_id="story-translate-002",
                translated_text="Другий переклад.",
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Translated script already exists for story: story-translate-002",
        )

    def test_valid_full_flow_selection_rewrite_translate(self) -> None:
        candidate = self._build_candidate(post_id="story-flow-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)

        selected_record = mark_story_selected_for_processing(
            story_id="story-flow-001",
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            instruction_message_id=9025,
        )
        rewritten_record = save_operator_rewrite(
            story_id="story-flow-001",
            text="This is the rewritten English script.",
            store_path=self.review_store_path,
        )
        translated_record = set_translated_script(
            story_id="story-flow-001",
            translated_text="Це перекладений український сценарій.",
            store_path=self.review_store_path,
        )

        self.assertTrue(selected_record.selected_for_processing)
        self.assertEqual(
            rewritten_record.operator_rewritten_text,
            "This is the rewritten English script.",
        )
        self.assertEqual(
            translated_record.translated_script_uk,
            "Це перекладений український сценарій.",
        )

    def test_translate_story_review_to_ukrainian_full_flow(self) -> None:
        candidate = self._build_candidate(post_id="story-flow-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-flow-002",
            store_path=self.review_store_path,
        )
        save_operator_rewrite(
            story_id="story-flow-002",
            text="This is the operator rewritten English script.",
            store_path=self.review_store_path,
        )

        translated_record = translate_story_review_to_ukrainian(
            story_id="story-flow-002",
            store_path=self.review_store_path,
            translator=lambda text: (f"UA::{text}", len(text)),
        )

        self.assertEqual(
            translated_record.translated_script_uk,
            "UA::This is the operator rewritten English script.",
        )
        self.assertEqual(
            translated_record.translation_characters_used,
            len("This is the operator rewritten English script."),
        )

    def test_translate_story_review_to_ukrainian_fails_when_story_is_not_selected(self) -> None:
        candidate = self._build_candidate(post_id="story-flow-003")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)

        with self.assertRaises(RedditIntakeError) as context:
            translate_story_review_to_ukrainian(
                story_id="story-flow-003",
                store_path=self.review_store_path,
                translator=lambda text: (f"UA::{text}", len(text)),
            )

        self.assertEqual(
            str(context.exception),
            "Story must be selected for processing before translation: story-flow-003",
        )

    def test_translate_story_review_to_ukrainian_requires_rewrite(self) -> None:
        candidate = self._build_candidate(post_id="story-flow-004")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-flow-004",
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            translate_story_review_to_ukrainian(
                story_id="story-flow-004",
                store_path=self.review_store_path,
                translator=lambda text: (f"UA::{text}", len(text)),
            )

        self.assertEqual(
            str(context.exception),
            "Story must have rewritten text before translation: story-flow-004",
        )

    def test_translate_story_review_to_ukrainian_fails_when_translation_already_exists(self) -> None:
        candidate = self._build_candidate(post_id="story-flow-005")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-flow-005",
            store_path=self.review_store_path,
        )
        save_operator_rewrite(
            story_id="story-flow-005",
            text="This is the operator rewritten English script.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-flow-005",
            translated_text="Вже перекладено.",
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            translate_story_review_to_ukrainian(
                story_id="story-flow-005",
                store_path=self.review_store_path,
                translator=lambda text: (f"UA::{text}", len(text)),
            )

        self.assertEqual(
            str(context.exception),
            "Translated script already exists for story: story-flow-005",
        )

    def test_translate_story_review_to_ukrainian_fails_for_unknown_story(self) -> None:
        with self.assertRaises(RedditIntakeError) as context:
            translate_story_review_to_ukrainian(
                story_id="missing-story-001",
                store_path=self.review_store_path,
                translator=lambda text: (f"UA::{text}", len(text)),
            )

        self.assertEqual(
            str(context.exception),
            "Story review not found: missing-story-001",
        )

    def test_empty_store_stats(self) -> None:
        stats = get_story_review_stats(store_path=self.review_store_path)

        self.assertEqual(stats.total, 0)
        self.assertEqual(stats.pending_review, 0)
        self.assertEqual(stats.approved, 0)
        self.assertEqual(stats.rejected, 0)
        self.assertEqual(stats.sent_to_telegram, 0)
        self.assertEqual(stats.eligible_to_send, 0)

    def test_mixed_status_store_stats(self) -> None:
        pending_candidate = self._build_candidate(post_id="story-stats-001")
        approved_candidate = self._build_candidate(post_id="story-stats-002")
        rejected_candidate = self._build_candidate(post_id="story-stats-003")

        save_story_candidates_for_review(
            (pending_candidate, approved_candidate, rejected_candidate),
            store_path=self.review_store_path,
        )
        update_story_review_status(
            story_id="story-stats-002",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )
        update_story_review_status(
            story_id="story-stats-003",
            status=REJECTED_STATUS,
            store_path=self.review_store_path,
        )

        stats = get_story_review_stats(store_path=self.review_store_path)

        self.assertEqual(stats.total, 3)
        self.assertEqual(stats.pending_review, 1)
        self.assertEqual(stats.approved, 1)
        self.assertEqual(stats.rejected, 1)

    def test_sent_vs_eligible_counts_are_correct(self) -> None:
        first_candidate = self._build_candidate(post_id="story-stats-004")
        second_candidate = self._build_candidate(post_id="story-stats-005")

        save_story_candidates_for_review(
            (first_candidate, second_candidate),
            store_path=self.review_store_path,
        )

        with (
            patch(
                "src.reddit_intake.send_story_reviews_to_telegram",
                side_effect=StoryReviewTelegramSendError(
                    "Telegram send failed.",
                    sent_story_ids=("story-stats-004",),
                ),
            ),
            self.assertRaises(StoryReviewTelegramSendError),
        ):
            send_pending_story_reviews_to_telegram(
                store_path=self.review_store_path,
            )

        stats = get_story_review_stats(store_path=self.review_store_path)

        self.assertEqual(stats.total, 2)
        self.assertEqual(stats.pending_review, 2)
        self.assertEqual(stats.sent_to_telegram, 1)
        self.assertEqual(stats.eligible_to_send, 1)

    def test_approved_and_rejected_items_are_never_eligible_to_send(self) -> None:
        approved_candidate = self._build_candidate(post_id="story-stats-006")
        rejected_candidate = self._build_candidate(post_id="story-stats-007")

        save_story_candidates_for_review(
            (approved_candidate, rejected_candidate),
            store_path=self.review_store_path,
        )
        update_story_review_status(
            story_id="story-stats-006",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )
        update_story_review_status(
            story_id="story-stats-007",
            status=REJECTED_STATUS,
            store_path=self.review_store_path,
        )

        stats = get_story_review_stats(store_path=self.review_store_path)

        self.assertEqual(stats.pending_review, 0)
        self.assertEqual(stats.eligible_to_send, 0)

    def test_send_approvals_marks_as_sent(self) -> None:
        candidate = self._build_candidate(post_id="story-send-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)

        with patch(
            "src.reddit_intake.send_story_reviews_to_telegram",
            side_effect=lambda reviews: tuple(review.story_id for review in reviews),
        ):
            sent_story_ids = send_pending_story_reviews_to_telegram(
                store_path=self.review_store_path,
            )

        updated_record = get_story_review("story-send-001", self.review_store_path)
        self.assertEqual(sent_story_ids, ("story-send-001",))
        self.assertIsNotNone(updated_record)
        self.assertTrue(updated_record.sent_to_telegram)

    def test_send_flow_writes_once(self) -> None:
        first_candidate = self._build_candidate(post_id="story-write-001")
        second_candidate = self._build_candidate(post_id="story-write-002")
        save_story_candidates_for_review(
            (first_candidate, second_candidate),
            store_path=self.review_store_path,
        )

        with (
            patch(
                "src.reddit_intake.send_story_reviews_to_telegram",
                return_value=("story-write-001", "story-write-002"),
            ) as send_mock,
            patch(
                "src.reddit_intake._write_story_reviews",
                wraps=reddit_intake._write_story_reviews,
            ) as write_mock,
        ):
            sent_story_ids = send_pending_story_reviews_to_telegram(
                store_path=self.review_store_path,
            )

        self.assertEqual(sent_story_ids, ("story-write-001", "story-write-002"))
        send_mock.assert_called_once()
        write_mock.assert_called_once()

    def test_save_does_not_reset_sent_flag(self) -> None:
        candidate = self._build_candidate(post_id="story-save-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        with patch(
            "src.reddit_intake.send_story_reviews_to_telegram",
            side_effect=lambda reviews: tuple(review.story_id for review in reviews),
        ):
            send_pending_story_reviews_to_telegram(
                store_path=self.review_store_path,
            )

        saved_records = save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        updated_record = get_story_review("story-save-001", self.review_store_path)

        self.assertTrue(saved_records[0].sent_to_telegram)
        self.assertIsNotNone(updated_record)
        self.assertTrue(updated_record.sent_to_telegram)

    def test_partial_telegram_send_marks_only_successful_items(self) -> None:
        first_candidate = self._build_candidate(post_id="story-partial-001")
        second_candidate = self._build_candidate(post_id="story-partial-002")
        save_story_candidates_for_review(
            (first_candidate, second_candidate),
            store_path=self.review_store_path,
        )

        with (
            patch(
                "src.reddit_intake.send_story_reviews_to_telegram",
                side_effect=StoryReviewTelegramSendError(
                    "Telegram send failed.",
                    sent_story_ids=("story-partial-001",),
                ),
            ),
            self.assertRaises(StoryReviewTelegramSendError),
        ):
            send_pending_story_reviews_to_telegram(
                store_path=self.review_store_path,
            )

        first_record = get_story_review("story-partial-001", self.review_store_path)
        second_record = get_story_review("story-partial-002", self.review_store_path)
        self.assertIsNotNone(first_record)
        self.assertIsNotNone(second_record)
        self.assertTrue(first_record.sent_to_telegram)
        self.assertFalse(second_record.sent_to_telegram)

    def test_no_regression_in_send_logic(self) -> None:
        first_candidate = self._build_candidate(post_id="story-batch-001")
        second_candidate = self._build_candidate(post_id="story-batch-002")
        save_story_candidates_for_review(
            (first_candidate, second_candidate),
            store_path=self.review_store_path,
        )

        with patch(
            "src.reddit_intake.send_story_reviews_to_telegram",
            return_value=("story-batch-001", "story-batch-002"),
        ) as send_mock:
            sent_story_ids = send_pending_story_reviews_to_telegram(
                store_path=self.review_store_path,
            )

        sent_reviews = send_mock.call_args.args[0]
        self.assertEqual(tuple(review.story_id for review in sent_reviews), ("story-batch-001", "story-batch-002"))
        self.assertEqual(sent_story_ids, ("story-batch-001", "story-batch-002"))

    def test_process_callback_marks_selected_for_processing(self) -> None:
        candidate = self._build_candidate(post_id="story-process-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        sent_messages: list[str] = []

        def fake_instruction_sender(message: str) -> int:
            sent_messages.append(message)
            return 9001

        updated_record = handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-process-001"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=fake_instruction_sender,
        )

        stored_record = get_story_review("story-process-001", self.review_store_path)
        self.assertTrue(updated_record.selected_for_processing)
        self.assertIsNotNone(stored_record)
        self.assertTrue(stored_record.selected_for_processing)
        self.assertEqual(stored_record.status, PENDING_REVIEW_STATUS)
        self.assertEqual(stored_record.telegram.chat_id, "telegram-chat-001")
        self.assertEqual(stored_record.telegram.instruction_message_id, 9001)
        self.assertEqual(len(sent_messages), 1)
        self.assertIn("Reply to this message", sent_messages[0])

    def test_handle_telegram_process_callback_saves_selection_before_instruction_send(self) -> None:
        candidate = self._build_candidate(post_id="story-process-save-first-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)

        with self.assertLogs("src.reddit_intake", level="ERROR") as logs:
            updated_record = handle_telegram_process_callback(
                callback_data=build_process_callback_data("story-process-save-first-001"),
                store_path=self.review_store_path,
                chat_id="telegram-chat-001",
                send_instruction_message=lambda message: (_ for _ in ()).throw(
                    RedditIntakeError("telegram send failed")
                ),
            )

        stored_record = get_story_review("story-process-save-first-001", self.review_store_path)
        self.assertTrue(updated_record.selected_for_processing)
        self.assertIsNotNone(stored_record)
        self.assertTrue(stored_record.selected_for_processing)
        self.assertIsNone(stored_record.telegram.instruction_message_id)
        self.assertIn("Telegram instruction send failed after selection", "\n".join(logs.output))

    def test_rewritten_script_cannot_be_saved_before_selection(self) -> None:
        candidate = self._build_candidate(post_id="story-script-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)

        with self.assertRaises(RedditIntakeError) as context:
            save_operator_rewritten_text(
                story_id="story-script-001",
                rewritten_text="A manually rewritten English script.",
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Story must be selected for processing before saving a script: story-script-001",
        )

    def test_rewritten_script_save_persists_operator_text(self) -> None:
        candidate = self._build_candidate(post_id="story-script-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-script-002"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9002,
        )

        updated_record = save_operator_rewritten_text(
            story_id="story-script-002",
            rewritten_text="This is the rewritten English script.",
            store_path=self.review_store_path,
        )

        stored_record = get_story_review("story-script-002", self.review_store_path)
        self.assertEqual(
            updated_record.operator_rewritten_text,
            "This is the rewritten English script.",
        )
        self.assertIsNone(updated_record.translated_script_uk)
        self.assertIsNotNone(updated_record.script_saved_at)
        self.assertIsNotNone(stored_record)
        self.assertEqual(
            stored_record.operator_rewritten_text,
            "This is the rewritten English script.",
        )

    def test_reply_update_saves_rewritten_text_into_correct_story(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-001"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9010,
        )

        updated_record = ingest_telegram_rewrite_reply(
            message_payload={
                "message_id": 9100,
                "chat": {"id": "telegram-chat-001"},
                "text": "This is the Telegram reply with the rewritten script.",
                "reply_to_message": {"message_id": 9010},
            },
            store_path=self.review_store_path,
            translate=lambda text: ("Це перекладений український сценарій.", len(text)),
        )

        self.assertEqual(
            updated_record.operator_rewritten_text,
            "This is the Telegram reply with the rewritten script.",
        )
        self.assertEqual(
            updated_record.translated_script_uk,
            "Це перекладений український сценарій.",
        )

    def test_process_telegram_update_routes_reply_message_and_saves_text(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-001b")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-001b"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9014,
        )

        updated_record = process_telegram_update(
            {
                "message": {
                    "message_id": 9104,
                    "chat": {"id": "telegram-chat-001"},
                    "text": "Processed through the Telegram update entrypoint.",
                    "reply_to_message": {"message_id": 9014},
                }
            },
            store_path=self.review_store_path,
            translate=lambda text: ("Оновлення Telegram теж перекладається.", len(text)),
        )

        self.assertEqual(
            updated_record.operator_rewritten_text,
            "Processed through the Telegram update entrypoint.",
        )
        self.assertEqual(
            updated_record.translated_script_uk,
            "Оновлення Telegram теж перекладається.",
        )

    def test_operator_reply_triggers_translation(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-translate-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-translate-001"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9020,
        )

        updated_record = ingest_telegram_rewrite_reply(
            message_payload={
                "message_id": 9120,
                "chat": {"id": "telegram-chat-001"},
                "text": "This rewritten script should auto-translate.",
                "reply_to_message": {"message_id": 9020},
            },
            store_path=self.review_store_path,
            translate=lambda text: (
                "Цей переписаний сценарій перекладено автоматично.",
                len(text),
            ),
        )

        stored_record = get_story_review("story-reply-translate-001", self.review_store_path)
        self.assertIsNotNone(stored_record)
        self.assertEqual(
            updated_record.operator_rewritten_text,
            "This rewritten script should auto-translate.",
        )
        self.assertEqual(
            updated_record.translated_script_uk,
            "Цей переписаний сценарій перекладено автоматично.",
        )
        self.assertEqual(
            stored_record.translated_script_uk if stored_record is not None else None,
            "Цей переписаний сценарій перекладено автоматично.",
        )

    def test_bypass_translation_saves_ukrainian_text_directly(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-uk-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-reply-uk-001",
            store_path=self.review_store_path,
        )

        with patch("src.reddit_intake.translate_text_deepl") as translate_mock:
            updated_record = bypass_translation_with_ukrainian_script(
                story_id="story-reply-uk-001",
                ukrainian_text="Готовий український текст.",
                store_path=self.review_store_path,
            )

        translate_mock.assert_not_called()
        self.assertEqual(updated_record.operator_rewritten_text, "Готовий український текст.")
        self.assertEqual(updated_record.translated_script_uk, "Готовий український текст.")
        self.assertIsNone(updated_record.translation_characters_used)

    def test_bypass_translation_fails_if_story_not_selected(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-uk-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)

        with self.assertRaises(RedditIntakeError) as context:
            bypass_translation_with_ukrainian_script(
                story_id="story-reply-uk-002",
                ukrainian_text="Готовий український текст.",
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Story must be selected for processing before saving a script: story-reply-uk-002",
        )

    def test_bypass_translation_fails_if_rewrite_already_exists(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-uk-003")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-reply-uk-003",
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id="story-reply-uk-003",
            rewritten_text="Existing rewrite.",
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            bypass_translation_with_ukrainian_script(
                story_id="story-reply-uk-003",
                ukrainian_text="Готовий український текст.",
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Rewritten script already exists for story: story-reply-uk-003",
        )

    def test_bypass_translation_fails_if_translated_script_already_exists(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-uk-004")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-reply-uk-004",
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id="story-reply-uk-004",
            rewritten_text="Existing rewrite.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-reply-uk-004",
            translated_text="Вже збережений переклад.",
            translation_characters_used=21,
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            bypass_translation_with_ukrainian_script(
                story_id="story-reply-uk-004",
                ukrainian_text="Готовий український текст.",
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Translated script already exists for story: story-reply-uk-004",
        )

    def test_bypass_translation_fails_if_body_empty_after_prefix(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-uk-005")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-reply-uk-005",
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            bypass_translation_with_ukrainian_script(
                story_id="story-reply-uk-005",
                ukrainian_text="   ",
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Ukrainian script body is required after [UK] prefix.",
        )

    def test_ingest_telegram_rewrite_reply_routes_uk_prefix_to_bypass(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-uk-006")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-uk-006"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9030,
        )

        with patch("src.reddit_intake.translate_text_deepl") as translate_mock:
            updated_record = ingest_telegram_rewrite_reply(
                message_payload={
                    "message_id": 9130,
                    "chat": {"id": "telegram-chat-001"},
                    "text": "[UK] Готовий текст.",
                    "reply_to_message": {"message_id": 9030},
                },
                store_path=self.review_store_path,
            )

        translate_mock.assert_not_called()
        self.assertEqual(updated_record.translated_script_uk, "Готовий текст.")
        self.assertIsNone(updated_record.translation_characters_used)

    def test_ingest_telegram_rewrite_reply_routes_plain_text_to_deepl(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-uk-007")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-uk-007"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9031,
        )
        translate_calls: list[str] = []

        updated_record = ingest_telegram_rewrite_reply(
            message_payload={
                "message_id": 9131,
                "chat": {"id": "telegram-chat-001"},
                "text": "Plain English rewrite.",
                "reply_to_message": {"message_id": 9031},
            },
            store_path=self.review_store_path,
            translate=lambda text: translate_calls.append(text) or ("Звичайний переклад.", len(text)),
        )

        self.assertEqual(translate_calls, ["Plain English rewrite."])
        self.assertEqual(updated_record.translated_script_uk, "Звичайний переклад.")

    def test_uk_prefix_with_empty_body_raises_in_ingest_reply(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-uk-008")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-uk-008"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9032,
        )

        with self.assertRaises(RedditIntakeError) as context:
            ingest_telegram_rewrite_reply(
                message_payload={
                    "message_id": 9132,
                    "chat": {"id": "telegram-chat-001"},
                    "text": "[UK]   ",
                    "reply_to_message": {"message_id": 9032},
                },
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Ukrainian script body is required after [UK] prefix.",
        )

    def test_double_reply_fails(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-double-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-double-001"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9021,
        )

        ingest_telegram_rewrite_reply(
            message_payload={
                "message_id": 9121,
                "chat": {"id": "telegram-chat-001"},
                "text": "First rewritten script.",
                "reply_to_message": {"message_id": 9021},
            },
            store_path=self.review_store_path,
            translate=lambda text: ("Перший переклад.", len(text)),
        )

        with self.assertRaises(RedditIntakeError) as context:
            ingest_telegram_rewrite_reply(
                message_payload={
                    "message_id": 9122,
                    "chat": {"id": "telegram-chat-001"},
                    "text": "Second rewritten script.",
                    "reply_to_message": {"message_id": 9021},
                },
                store_path=self.review_store_path,
                translate=lambda text: ("Другий переклад.", len(text)),
            )

        self.assertEqual(
            str(context.exception),
            "Rewritten script already exists for story: story-reply-double-001",
        )

    def test_translation_failure_does_not_corrupt_state(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-translate-fail-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-translate-fail-001"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9022,
        )

        def failing_translate(text: str) -> tuple[str, int]:
            raise TranslationError("DeepL request failed.")

        with self.assertRaises(RedditIntakeError) as context:
            process_operator_reply_and_translate(
                story_id="story-reply-translate-fail-001",
                operator_text="This rewritten script should remain saved.",
                translate=failing_translate,
                store_path=self.review_store_path,
            )

        stored_record = get_story_review(
            "story-reply-translate-fail-001",
            self.review_store_path,
        )
        self.assertEqual(str(context.exception), "DeepL request failed.")
        self.assertIsNotNone(stored_record)
        self.assertEqual(
            stored_record.operator_rewritten_text if stored_record is not None else None,
            "This rewritten script should remain saved.",
        )
        self.assertIsNone(
            stored_record.translated_script_uk if stored_record is not None else None,
        )
        self.assertIsNone(
            stored_record.translation_characters_used if stored_record is not None else None,
        )

    def test_translation_usage_is_logged(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-usage-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-usage-001"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9023,
        )

        with self.assertLogs("src.reddit_intake", level="INFO") as captured_logs:
            updated_record = process_operator_reply_and_translate(
                story_id="story-reply-usage-001",
                operator_text="This rewritten script tracks usage.",
                translate=lambda text: ("Цей сценарій перекладено.", 37),
                store_path=self.review_store_path,
            )

        self.assertEqual(updated_record.translated_script_uk, "Цей сценарій перекладено.")
        self.assertIn(
            (
                "Translation completed: story_id=story-reply-usage-001 chars=37 "
                f"length={len('Цей сценарій перекладено.')}"
            ),
            "\n".join(captured_logs.output),
        )

    def test_translation_usage_is_persisted(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-usage-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-reply-usage-002",
            store_path=self.review_store_path,
        )

        updated_record = process_operator_reply_and_translate(
            story_id="story-reply-usage-002",
            operator_text="This rewrite persists translation usage.",
            translate=lambda text: ("Цей переклад зберігає usage.", 41),
            store_path=self.review_store_path,
        )

        stored_record = get_story_review("story-reply-usage-002", self.review_store_path)
        self.assertEqual(updated_record.translation_characters_used, 41)
        self.assertIsNotNone(stored_record)
        self.assertEqual(
            stored_record.translation_characters_used if stored_record is not None else None,
            41,
        )

    def test_translation_usage_none_when_not_translated(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-usage-003")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-usage-003"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9024,
        )

        stored_record = get_story_review("story-reply-usage-003", self.review_store_path)
        self.assertIsNotNone(stored_record)
        self.assertIsNone(
            stored_record.translation_characters_used if stored_record is not None else None,
        )

    def test_translation_returns_tuple_contract(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-contract-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-reply-contract-001",
            store_path=self.review_store_path,
        )

        updated_record = process_operator_reply_and_translate(
            story_id="story-reply-contract-001",
            operator_text="Tuple contract translation.",
            translate=lambda text: ("Контракт виконано.", 26),
            store_path=self.review_store_path,
        )

        self.assertEqual(updated_record.translated_script_uk, "Контракт виконано.")
        self.assertEqual(updated_record.translation_characters_used, 26)

    def test_invalid_translate_boundary_fails(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-contract-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-reply-contract-002",
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            process_operator_reply_and_translate(
                story_id="story-reply-contract-002",
                operator_text="This rewrite gets a bad translator contract.",
                translate=lambda text: cast(object, "not-a-tuple"),  # type: ignore[arg-type]
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Translate boundary must return a (translated_text, chars_used) tuple.",
        )

    def test_empty_reply_fails(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-002"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9011,
        )

        with self.assertRaises(RedditIntakeError) as context:
            ingest_telegram_rewrite_reply(
                message_payload={
                    "message_id": 9101,
                    "chat": {"id": "telegram-chat-001"},
                    "text": "   ",
                    "reply_to_message": {"message_id": 9011},
                },
                store_path=self.review_store_path,
            )

        self.assertEqual(str(context.exception), "Telegram rewritten script reply is empty.")

    def test_unrelated_reply_is_rejected(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-003")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-003"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9012,
        )

        with self.assertRaises(RedditIntakeError) as context:
            ingest_telegram_rewrite_reply(
                message_payload={
                    "message_id": 9102,
                    "chat": {"id": "telegram-chat-999"},
                    "text": "Wrong chat reply",
                    "reply_to_message": {"message_id": 9012},
                },
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Telegram reply cannot be mapped to a story.",
        )

    def test_already_saved_rewritten_text_is_rejected(self) -> None:
        candidate = self._build_candidate(post_id="story-reply-004")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-reply-004"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9013,
        )
        save_operator_rewritten_text(
            story_id="story-reply-004",
            rewritten_text="Initial rewritten text.",
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            ingest_telegram_rewrite_reply(
                message_payload={
                    "message_id": 9103,
                    "chat": {"id": "telegram-chat-001"},
                    "text": "Second rewritten text.",
                    "reply_to_message": {"message_id": 9013},
                },
                store_path=self.review_store_path,
            )

        self.assertEqual(
            str(context.exception),
            "Rewritten script already exists for story: story-reply-004",
        )

    def test_approved_story_package_validation_accepts_respeecher(self) -> None:
        package = build_approved_story_package(
            story_id="story-001",
            source_title="Original post title",
            adapted_script="Це адаптований текст для української озвучки.",
            tts_provider=RESPEECHER_PROVIDER,
            voice_id="olesia-media",
            background_video_path=self.background_video_path,
            output_basename="reddit_story_001",
        )

        self.assertIsInstance(package, ApprovedStoryPackage)
        self.assertEqual(package.story_id, "story-001")
        self.assertEqual(package.tts_provider, RESPEECHER_PROVIDER)

    def test_package_validation_rejects_placeholder_or_empty_voice_id_without_default(self) -> None:
        with patch("src.reddit_intake.config.RESPEECHER_DEFAULT_VOICE_ID", None):
            with self.assertRaises(RedditIntakeError) as empty_context:
                build_approved_story_package(
                    story_id="story-002",
                    source_title="Original post title",
                    adapted_script="Це адаптований текст для української озвучки.",
                    tts_provider=RESPEECHER_PROVIDER,
                    voice_id="",
                    background_video_path=self.background_video_path,
                )

        self.assertEqual(str(empty_context.exception), "voice_id is required for provider: respeecher")

        with self.assertRaises(RedditIntakeError) as placeholder_context:
            build_approved_story_package(
                story_id="story-003",
                source_title="Original post title",
                adapted_script="Це адаптований текст для української озвучки.",
                tts_provider=RESPEECHER_PROVIDER,
                voice_id="<voice_id>",
                background_video_path=self.background_video_path,
            )

        self.assertEqual(
            str(placeholder_context.exception),
            "Placeholder voice_id is not allowed for respeecher.",
        )

    def test_package_validation_accepts_empty_voice_id_when_default_is_configured(self) -> None:
        with patch("src.reddit_intake.config.RESPEECHER_DEFAULT_VOICE_ID", "olesia-media"):
            package = build_approved_story_package(
                story_id="story-004",
                source_title="Original post title",
                adapted_script="Це адаптований текст для української озвучки.",
                tts_provider=RESPEECHER_PROVIDER,
                voice_id="",
                background_video_path=self.background_video_path,
            )

        self.assertEqual(package.voice_id, "")

    def test_list_available_respeecher_voices_uses_sdk_boundary(self) -> None:
        voices = list_available_respeecher_voices(
            api_key="respeecher-key",
            client_factory=lambda api_key: self._fake_client(
                voices=[self._voice("olesia-media", "Olesia Media")],
            ),
        )

        self.assertEqual(voices, ({"id": "olesia-media", "name": "Olesia Media"},))

    def test_voice_resolution_against_sdk_responses(self) -> None:
        resolved_voice_id = resolve_respeecher_voice_id(
            voice_id="olesia-media",
            api_key="respeecher-key",
            client_factory=lambda api_key: self._fake_client(
                voices=[
                    self._voice("olesia-media", "Olesia Media"),
                    self._voice("oleksandr-radio", "Oleksandr Radio"),
                ],
            ),
        )

        self.assertEqual(resolved_voice_id, "olesia-media")

    def test_generation_fails_if_selected_voice_id_is_absent_from_available_voices(self) -> None:
        with self.assertRaises(RedditIntakeError) as context:
            generate_tts_audio(
                script="Це тест української озвучки.",
                provider_name=RESPEECHER_PROVIDER,
                voice_id="missing-voice",
                output_dir=self.base_dir / "tts-missing-voice",
                api_key="respeecher-key",
                client_factory=lambda api_key: self._fake_client(
                    voices=[self._voice("olesia-media", "Olesia Media")],
                ),
            )

        self.assertEqual(
            str(context.exception),
            "Respeecher voice_id is not available: missing-voice",
        )

    def test_generation_succeeds_and_writes_wav_from_sdk_bytes(self) -> None:
        tts_output_dir = self.base_dir / "tts-respeecher"

        audio_path = generate_tts_audio(
            script="Це тест української озвучки.",
            provider_name=RESPEECHER_PROVIDER,
            voice_id="olesia-media",
            output_dir=tts_output_dir,
            api_key="respeecher-key",
            client_factory=lambda api_key: self._fake_client(
                voices=[self._voice("olesia-media", "Olesia Media")],
                audio_chunks=[b"wav-", b"bytes"],
            ),
        )

        self.assertEqual(audio_path, tts_output_dir / "voiceover.wav")
        self.assertTrue(audio_path.exists())
        self.assertEqual(audio_path.read_bytes(), b"wav-bytes")

    def test_generation_uses_pre_resolved_voice_id_without_listing_voices_again(self) -> None:
        tts_output_dir = self.base_dir / "tts-pre-resolved"

        with patch("src.reddit_intake.resolve_respeecher_voice_id") as resolve_voice_mock:
            audio_path = generate_tts_audio(
                script="Це тест української озвучки.",
                provider_name=RESPEECHER_PROVIDER,
                voice_id="olesia-media",
                output_dir=tts_output_dir,
                api_key="respeecher-key",
                pre_resolved_voice_id="olesia-media",
                client_factory=lambda api_key: self._fake_client(
                    voices=[self._voice("olesia-media", "Olesia Media")],
                    audio_chunks=[b"wav-", b"bytes"],
                ),
            )

        resolve_voice_mock.assert_not_called()
        self.assertEqual(audio_path.read_bytes(), b"wav-bytes")

    def test_render_approved_story_passes_pre_resolved_voice_id_to_custom_tts_generator(self) -> None:
        package = ApprovedStoryPackage(
            story_id="story-render-pre-resolved-001",
            source_title="Title",
            adapted_script="Готовий український сценарій.",
            output_basename="story-render-pre-resolved-001",
            background_video_path=self.background_video_path,
            tts_provider=RESPEECHER_PROVIDER,
            voice_id="olesia-media",
        )
        observed_pre_resolved_voice_ids: list[str | None] = []

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id
            observed_pre_resolved_voice_ids.append(pre_resolved_voice_id)
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:02,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        render_approved_story(
            package=package,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
            pre_resolved_voice_id="olesia-media",
        )

        self.assertEqual(observed_pre_resolved_voice_ids, ["olesia-media"])

    def test_sdk_tts_failure_is_explicit(self) -> None:
        with self.assertRaises(RedditIntakeError) as context:
            generate_tts_audio(
                script="Це тест української озвучки.",
                provider_name=RESPEECHER_PROVIDER,
                voice_id="olesia-media",
                output_dir=self.base_dir / "tts-sdk-failure",
                api_key="respeecher-key",
                client_factory=lambda api_key: self._fake_client(
                    voices=[self._voice("olesia-media", "Olesia Media")],
                    tts_error=RuntimeError("sdk boom"),
                ),
            )

        self.assertEqual(
            str(context.exception),
            "Respeecher TTS generation failed: sdk boom",
        )

    def test_invalid_sdk_voice_shape_fails_clearly(self) -> None:
        class InvalidVoice:
            def __init__(self) -> None:
                self.name = "No Id"

        with self.assertRaises(RedditIntakeError) as context:
            list_available_respeecher_voices(
                api_key="respeecher-key",
                client_factory=lambda api_key: self._fake_client(voices=[InvalidVoice()]),
            )

        self.assertEqual(str(context.exception), "Unexpected Respeecher voices response.")

    def test_unsupported_provider_fails_clearly(self) -> None:
        with self.assertRaises(RedditIntakeError) as context:
            generate_tts_audio(
                script="Це тест української озвучки.",
                provider_name="unsupported_provider",
                voice_id="olesia-media",
                output_dir=self.base_dir / "tts-unsupported",
            )

        self.assertEqual(str(context.exception), "Unsupported TTS provider: unsupported_provider")

    def test_missing_respeecher_api_key_fails_clearly(self) -> None:
        with patch("src.reddit_intake.config.RESPEECHER_API_KEY", None):
            with self.assertRaises(RedditIntakeError) as context:
                generate_tts_audio(
                    script="Це тест української озвучки.",
                    provider_name=RESPEECHER_PROVIDER,
                    voice_id="olesia-media",
                    output_dir=self.base_dir / "tts-missing-key",
                )

        self.assertEqual(str(context.exception), "Respeecher API key is not configured.")

    def test_render_story_review_uses_mocked_tts_and_render_boundaries(self) -> None:
        candidate = self._build_candidate(post_id="story-render-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        handle_telegram_process_callback(
            callback_data=build_process_callback_data("story-render-001"),
            store_path=self.review_store_path,
            chat_id="telegram-chat-001",
            send_instruction_message=lambda message: 9020,
        )
        save_operator_rewritten_text(
            story_id="story-render-001",
            rewritten_text="This is the approved rewritten English script.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-render-001",
            translated_text="Це підготовлений український сценарій для рендеру.",
            store_path=self.review_store_path,
        )
        update_story_review_status(
            story_id="story-render-001",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )
        call_log: list[str] = []

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del provider_name, voice_id, pre_resolved_voice_id
            call_log.append(script)
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path
            self.assertIsNotNone(subtitle_path)
            self.assertTrue(subtitle_path.exists())
            call_log.append("render")
            output_path.write_bytes(b"rendered-video")
            return output_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text(
                f"1\n00:00:00,000 --> 00:00:05,000\n{script}\n",
                encoding="utf-8",
            )
            return subtitle_path

        result = render_story_review(
            story_id="story-render-001",
            background_video_path=self.background_video_path,
            voice_id="olesia-media",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
        )

        self.assertEqual(result.bundle_dir, self.inbox_dir / "story-render-001")
        self.assertEqual(result.output_path, self.inbox_dir / "story-render-001" / "video.mp4")
        self.assertTrue(result.output_path.exists())
        self.assertEqual(result.metadata_path, self.inbox_dir / "story-render-001" / "meta.json")
        self.assertTrue(result.metadata_path.exists())
        self.assertEqual(result.subtitle_path, self.inbox_dir / "story-render-001" / "subtitles.srt")
        self.assertTrue(result.subtitle_path.exists())

    def test_render_creates_bundle_directory(self) -> None:
        candidate = self._build_candidate(post_id="story-render-bundle-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-render-bundle-001",
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id="story-render-bundle-001",
            rewritten_text="Ready English script.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-render-bundle-001",
            translated_text="Готовий український сценарій для bundle тесту.",
            translation_characters_used=41,
            store_path=self.review_store_path,
        )
        update_story_review_status(
            story_id="story-render-bundle-001",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id, pre_resolved_voice_id
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:04,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        result = render_story_review(
            story_id="story-render-bundle-001",
            background_video_path=self.background_video_path,
            voice_id="olesia-media",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
        )

        self.assertTrue(result.bundle_dir.is_dir())
        self.assertEqual(result.output_path, result.bundle_dir / "video.mp4")
        self.assertEqual(result.metadata_path, result.bundle_dir / "meta.json")
        self.assertEqual(result.subtitle_path, result.bundle_dir / "subtitles.srt")

    def test_video_and_meta_in_same_folder(self) -> None:
        ready_review = self._make_ready_review("story-render-bundle-002")

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id, pre_resolved_voice_id
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:04,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        result = render_story_review(
            story_id=ready_review.story_id,
            background_video_path=self.background_video_path,
            voice_id="olesia-media",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
        )

        self.assertEqual(result.output_path.parent, result.bundle_dir)
        self.assertEqual(result.metadata_path.parent, result.bundle_dir)
        self.assertEqual(result.subtitle_path.parent, result.bundle_dir)

    def test_bundle_path_structure(self) -> None:
        ready_review = self._make_ready_review("story-render-bundle-003")

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id, pre_resolved_voice_id
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:04,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        result = render_story_review(
            story_id=ready_review.story_id,
            background_video_path=self.background_video_path,
            voice_id="olesia-media",
            output_basename="ignored-basename",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
        )

        self.assertEqual(result.bundle_dir, self.inbox_dir / ready_review.story_id)
        self.assertEqual(result.output_path, self.inbox_dir / ready_review.story_id / "video.mp4")
        self.assertEqual(result.metadata_path, self.inbox_dir / ready_review.story_id / "meta.json")
        self.assertEqual(result.subtitle_path, self.inbox_dir / ready_review.story_id / "subtitles.srt")

    def test_meta_file_created(self) -> None:
        ready_review = self._make_ready_review("story-render-meta-001")

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id, pre_resolved_voice_id
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:04,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        result = render_story_review(
            story_id=ready_review.story_id,
            background_video_path=self.background_video_path,
            voice_id="olesia-media",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
        )

        self.assertIsNotNone(result.metadata_path)
        self.assertTrue(result.metadata_path.exists())

    def test_meta_structure_valid(self) -> None:
        ready_review = self._make_ready_review("story-render-meta-002")

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id, pre_resolved_voice_id
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:04,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        result = render_story_review(
            story_id=ready_review.story_id,
            background_video_path=self.background_video_path,
            voice_id="olesia-media",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
        )

        metadata_payload = json.loads(result.metadata_path.read_text(encoding="utf-8"))
        self.assertEqual(set(metadata_payload.keys()), {"title", "description", "hashtags"})
        self.assertIsInstance(metadata_payload["hashtags"], list)
        self.assertTrue(metadata_payload["title"])
        self.assertTrue(metadata_payload["description"])

    def test_meta_not_overwritten_if_exists(self) -> None:
        ready_review = self._make_ready_review("story-render-meta-003")
        bundle_dir = self.inbox_dir / ready_review.story_id
        bundle_dir.mkdir(parents=True, exist_ok=True)
        existing_meta_path = bundle_dir / "meta.json"
        existing_meta_path.write_text(
            json.dumps(
                {
                    "title": "Manual title",
                    "description": "Manual description",
                    "hashtags": ["#manual"],
                }
            )
            + "\n",
            encoding="utf-8",
        )

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id, pre_resolved_voice_id
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:04,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        result = render_story_review(
            story_id=ready_review.story_id,
            background_video_path=self.background_video_path,
            voice_id="olesia-media",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
        )

        self.assertEqual(result.metadata_path, existing_meta_path)
        self.assertEqual(
            json.loads(existing_meta_path.read_text(encoding="utf-8")),
            {
                "title": "Manual title",
                "description": "Manual description",
                "hashtags": ["#manual"],
            },
        )

    def test_rerun_does_not_duplicate_files(self) -> None:
        ready_review = self._make_ready_review("story-render-rerun-001")

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id, pre_resolved_voice_id
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:01,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        first_result = render_story_review(
            story_id=ready_review.story_id,
            background_video_path=self.background_video_path,
            voice_id="olesia-media",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
        )
        second_result = render_story_review(
            story_id=ready_review.story_id,
            background_video_path=self.background_video_path,
            voice_id="olesia-media",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            tts_generator=fake_tts_generator,
            subtitle_generator=fake_subtitle_generator,
            renderer=fake_renderer,
        )

        bundle_files = sorted(path.name for path in first_result.bundle_dir.iterdir())
        self.assertEqual(first_result.bundle_dir, second_result.bundle_dir)
        self.assertEqual(first_result.output_path, second_result.output_path)
        self.assertEqual(bundle_files, ["meta.json", "subtitles.srt", "video.mp4"])

    def test_render_includes_subtitles(self) -> None:
        subtitle_path = self.base_dir / "subtitles.srt"
        subtitle_path.write_text("1\n00:00:00,000 --> 00:00:01,000\nSubtitle\n", encoding="utf-8")
        audio_path = self.base_dir / "voiceover.wav"
        audio_path.write_bytes(b"audio")
        output_path = self.base_dir / "rendered.mp4"
        captured_command: list[str] = []

        def fake_run_command(command: list[str], timeout_seconds: int):
            del timeout_seconds
            captured_command[:] = command
            return type(
                "CompletedProcess",
                (),
                {"returncode": 0, "stdout": "", "stderr": ""},
            )()

        with (
            patch("src.render._probe_media_duration", return_value=5.0),
            patch("src.render._run_command", side_effect=fake_run_command),
        ):
            render_story_video(
                background_video_path=self.background_video_path,
                audio_path=audio_path,
                output_path=output_path,
                subtitle_path=subtitle_path,
            )

        self.assertIn("-vf", captured_command)
        self.assertTrue(
            any(
                argument.startswith("subtitles=") and "subtitles.srt" in argument
                for argument in captured_command
            )
        )

    def test_subtitle_generation_failure_propagates(self) -> None:
        ready_review = self._make_ready_review("story-render-subtitle-fail-001")

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id, pre_resolved_voice_id
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def failing_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script, output_dir
            raise SubtitleGenerationError("WhisperX alignment failed.")

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, output_path, subtitle_path
            self.fail("Renderer should not be called when subtitle generation fails.")

        with self.assertRaises(RedditIntakeError) as context:
            render_story_review(
                story_id=ready_review.story_id,
                background_video_path=self.background_video_path,
                voice_id="olesia-media",
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
                tts_generator=fake_tts_generator,
                subtitle_generator=failing_subtitle_generator,
                renderer=fake_renderer,
            )

        self.assertEqual(str(context.exception), "WhisperX alignment failed.")

    def test_approved_translated_story_is_render_ready(self) -> None:
        candidate = self._build_candidate(post_id="story-ready-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-ready-001",
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id="story-ready-001",
            rewritten_text="Approved rewritten English script.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-ready-001",
            translated_text="Готовий український сценарій.",
            translation_characters_used=31,
            store_path=self.review_store_path,
        )
        approved_record = update_story_review_status(
            story_id="story-ready-001",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )

        self.assertTrue(is_story_ready_for_render(approved_record))

    def test_approved_story_without_translated_text_is_not_render_ready(self) -> None:
        candidate = self._build_candidate(post_id="story-ready-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-ready-002",
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id="story-ready-002",
            rewritten_text="Approved rewritten English script.",
            store_path=self.review_store_path,
        )
        approved_record = update_story_review_status(
            story_id="story-ready-002",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )

        self.assertFalse(is_story_ready_for_render(approved_record))

    def test_render_story_review_fails_when_translated_script_is_missing(self) -> None:
        candidate = self._build_candidate(post_id="story-render-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        update_story_review_status(
            story_id="story-render-002",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            render_story_review(
                story_id="story-render-002",
                background_video_path=self.background_video_path,
                voice_id="olesia-media",
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(
            str(context.exception),
            "Translated Ukrainian script is required before rendering: story-render-002",
        )

    def test_render_story_review_fails_when_story_is_not_approved(self) -> None:
        candidate = self._build_candidate(post_id="story-render-003")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-render-003",
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id="story-render-003",
            rewritten_text="Ready English script.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-render-003",
            translated_text="Готовий український сценарій.",
            translation_characters_used=28,
            store_path=self.review_store_path,
        )

        with self.assertRaises(RedditIntakeError) as context:
            render_story_review(
                story_id="story-render-003",
                background_video_path=self.background_video_path,
                voice_id="olesia-media",
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(
            str(context.exception),
            "Story is not approved for rendering: story-render-003",
        )

    def test_render_story_review_fails_when_story_is_not_render_ready(self) -> None:
        candidate = self._build_candidate(post_id="story-render-004")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-render-004",
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id="story-render-004",
            rewritten_text="Ready English script.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-render-004",
            translated_text="Український сценарій для перевірки ready gate.",
            translation_characters_used=35,
            store_path=self.review_store_path,
        )
        update_story_review_status(
            story_id="story-render-004",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )

        with (
            patch("src.reddit_intake.is_story_ready_for_render", return_value=False),
            self.assertRaises(RedditIntakeError) as context,
        ):
            render_story_review(
                story_id="story-render-004",
                background_video_path=self.background_video_path,
                voice_id="olesia-media",
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(
            str(context.exception),
            "Story is not ready for rendering: story-render-004",
        )

    def test_render_and_enqueue_story_review_succeeds_for_render_ready_story(self) -> None:
        candidate = self._build_candidate(post_id="story-render-enqueue-001")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-render-enqueue-001",
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id="story-render-enqueue-001",
            rewritten_text="This is the approved rewritten English script.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-render-enqueue-001",
            translated_text="Це підготовлений український сценарій для render-enqueue.",
            translation_characters_used=58,
            store_path=self.review_store_path,
        )
        update_story_review_status(
            story_id="story-render-enqueue-001",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )
        queue_store = PublishQueueStore(self.base_dir / "publish_queue.sqlite3")
        queue_store.initialize()

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del provider_name, voice_id, pre_resolved_voice_id
            self.assertEqual(
                script,
                "Це підготовлений український сценарій для render-enqueue.",
            )
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:04,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        def fake_thumbnail_helper(rendered_video_path: Path) -> Path:
            thumbnail_path = rendered_video_path.with_suffix(".jpg")
            thumbnail_path.write_bytes(b"thumbnail")
            return thumbnail_path

        with patch(
            "src.reddit_intake._create_render_thumbnail_from_rendered_story",
            side_effect=fake_thumbnail_helper,
        ):
            rendered_asset, queue_result = render_and_enqueue_story_review(
                story_id="story-render-enqueue-001",
                background_video_path=self.background_video_path,
                voice_id="olesia-media",
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
                publish_queue_store=queue_store,
                tts_generator=fake_tts_generator,
                subtitle_generator=fake_subtitle_generator,
                renderer=fake_renderer,
            )

        self.assertTrue(rendered_asset.output_path.exists())
        self.assertTrue(rendered_asset.subtitle_path.exists())
        self.assertTrue(queue_result.created)
        self.assertEqual(queue_result.item.asset_id, "story-render-enqueue-001")
        self.assertEqual(queue_result.item.clip_path, rendered_asset.output_path)
        self.assertEqual(len(queue_store.list_queued_items()), 1)

    def test_render_and_enqueue_story_review_fails_for_non_ready_story(self) -> None:
        candidate = self._build_candidate(post_id="story-render-enqueue-002")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)

        with self.assertRaises(RedditIntakeError) as context:
            render_and_enqueue_story_review(
                story_id="story-render-enqueue-002",
                background_video_path=self.background_video_path,
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(
            str(context.exception),
            "Story is not approved for rendering: story-render-enqueue-002",
        )

    def test_render_and_enqueue_story_review_rerun_does_not_create_duplicate_queue_item(self) -> None:
        candidate = self._build_candidate(post_id="story-render-enqueue-003")
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id="story-render-enqueue-003",
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id="story-render-enqueue-003",
            rewritten_text="This is the approved rewritten English script.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id="story-render-enqueue-003",
            translated_text="Це підготовлений український сценарій для повторного enqueue.",
            translation_characters_used=61,
            store_path=self.review_store_path,
        )
        update_story_review_status(
            story_id="story-render-enqueue-003",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )
        queue_store = PublishQueueStore(self.base_dir / "publish_queue-rerun.sqlite3")
        queue_store.initialize()

        def fake_tts_generator(
            script: str,
            provider_name: str,
            voice_id: str,
            output_dir: Path,
            *,
            pre_resolved_voice_id: str | None = None,
        ) -> Path:
            del script, provider_name, voice_id, pre_resolved_voice_id
            audio_path = output_dir / "voiceover.wav"
            audio_path.write_bytes(b"wav-audio")
            return audio_path

        def fake_renderer(
            background_video_path: Path,
            audio_path: Path,
            output_path: Path,
            subtitle_path: Path | None,
        ) -> Path:
            del background_video_path, audio_path, subtitle_path
            output_path.write_bytes(b"rendered-video")
            return output_path

        def fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
            del audio_path, script
            subtitle_path = output_dir / "subtitles.srt"
            subtitle_path.write_text("1\n00:00:00,000 --> 00:00:04,000\nSubtitle\n", encoding="utf-8")
            return subtitle_path

        def fake_thumbnail_helper(rendered_video_path: Path) -> Path:
            thumbnail_path = rendered_video_path.with_suffix(".jpg")
            thumbnail_path.write_bytes(b"thumbnail")
            return thumbnail_path

        with patch(
            "src.reddit_intake._create_render_thumbnail_from_rendered_story",
            side_effect=fake_thumbnail_helper,
        ):
            _, first_queue_result = render_and_enqueue_story_review(
                story_id="story-render-enqueue-003",
                background_video_path=self.background_video_path,
                voice_id="olesia-media",
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
                publish_queue_store=queue_store,
                tts_generator=fake_tts_generator,
                subtitle_generator=fake_subtitle_generator,
                renderer=fake_renderer,
            )
            _, second_queue_result = render_and_enqueue_story_review(
                story_id="story-render-enqueue-003",
                background_video_path=self.background_video_path,
                voice_id="olesia-media",
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
                publish_queue_store=queue_store,
                tts_generator=fake_tts_generator,
                subtitle_generator=fake_subtitle_generator,
                renderer=fake_renderer,
            )

        self.assertTrue(first_queue_result.created)
        self.assertFalse(second_queue_result.created)
        self.assertEqual(len(queue_store.list_queued_items()), 1)

    def test_run_full_pipeline_runs_end_to_end(self) -> None:
        self._make_ready_review("story-pipeline-ready-001")

        fetched_candidate = self._build_candidate(post_id="story-pipeline-fetched-001")
        processed_review = StoredStoryReview(
            intake=StoryIntakeStage(
                story_id="story-pipeline-updated-001",
                source_title="Updated title",
                source_body="Updated body",
                subreddit="TIFU",
                url="https://reddit.example/posts/story-pipeline-updated-001",
                score=config.REDDIT_MIN_SCORE + 10,
                created_utc=1700000100.0,
            ),
            moderation=StoryModerationStage(
                selected_for_processing=True,
                operator_rewritten_text="Rewritten text",
                script_saved_at="2026-03-22T12:00:00Z",
            ),
            production=StoryProductionStage(
                translated_script_uk="Перекладений текст",
                translation_characters_used=14,
            ),
            telegram=TelegramReviewMetadata(chat_id="chat-001", instruction_message_id=1001),
            status=PENDING_REVIEW_STATUS,
            sent_to_telegram=False,
            saved_at="2026-03-22T12:00:00Z",
        )
        render_result = type(
            "RenderResult",
            (),
            {
                "story_id": "story-pipeline-ready-001",
                "output_path": self.inbox_dir / "story-pipeline-ready-001" / "video.mp4",
            },
        )()
        queue_item = type("QueueItem", (), {"asset_id": "story-pipeline-ready-001"})()
        queue_result = type("QueueResult", (), {"created": True, "item": queue_item})()

        class FakeQueueStore:
            def initialize(self) -> None:
                return None

            def get_item(self, asset_id: str, clip_path: Path):
                del asset_id, clip_path
                return None

        with (
            patch(
                "src.reddit_intake.fetch_live_story_candidates",
                return_value=(fetched_candidate,),
            ),
            patch(
                "src.reddit_intake.send_pending_story_reviews_to_telegram",
                return_value=("story-pipeline-fetched-001",),
            ),
            patch(
                "src.reddit_intake.process_telegram_update",
                return_value=processed_review,
            ),
            patch(
                "src.reddit_intake.PublishQueueStore",
                return_value=FakeQueueStore(),
            ),
            patch(
                "src.reddit_intake.resolve_respeecher_voice_id",
                return_value="olesia-media",
            ),
            patch(
                "src.reddit_intake.render_and_enqueue_story_review",
                return_value=(render_result, queue_result),
            ) as render_enqueue_mock,
        ):
            result = run_full_pipeline(
                background_video_path=self.background_video_path,
                telegram_update_payloads=({"update_id": 1},),
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(
            result,
            PipelineRunResult(
                fetched=1,
                accepted=1,
                persisted=1,
                sent_to_telegram=1,
                processed_updates=1,
                translated=1,
                rendered=1,
                enqueued=1,
                stage_errors=(),
            ),
        )
        render_enqueue_mock.assert_called_once()

    def test_run_full_pipeline_returns_stage_errors_when_fetch_fails(self) -> None:
        class FakeQueueStore:
            def initialize(self) -> None:
                return None

            def get_item(self, asset_id: str, clip_path: Path):
                del asset_id, clip_path
                return None

        with (
            patch(
                "src.reddit_intake.fetch_live_story_candidates",
                side_effect=RedditIntakeError("fetch failed"),
            ),
            patch("src.reddit_intake.send_pending_story_reviews_to_telegram", return_value=()),
            patch("src.reddit_intake.PublishQueueStore", return_value=FakeQueueStore()),
        ):
            result = run_full_pipeline(
                background_video_path=self.background_video_path,
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(result.stage_errors, ("fetch/persist: fetch failed",))

    def test_run_full_pipeline_partial_failures_do_not_stop_pipeline(self) -> None:
        self._make_ready_review("story-pipeline-ready-002")
        untranslated_candidate = self._build_candidate(post_id="story-pipeline-untranslated-001")
        save_story_candidates_for_review(
            (untranslated_candidate,),
            store_path=self.review_store_path,
        )
        mark_story_selected_for_processing(
            story_id="story-pipeline-untranslated-001",
            store_path=self.review_store_path,
        )
        save_operator_rewrite(
            story_id="story-pipeline-untranslated-001",
            text="Rewrite waiting for translation.",
            store_path=self.review_store_path,
        )

        render_result = type(
            "RenderResult",
            (),
            {
                "story_id": "story-pipeline-ready-002",
                "output_path": self.inbox_dir / "story-pipeline-ready-002" / "video.mp4",
            },
        )()
        queue_item = type("QueueItem", (), {"asset_id": "story-pipeline-ready-002"})()
        queue_result = type("QueueResult", (), {"created": True, "item": queue_item})()

        class FakeQueueStore:
            def initialize(self) -> None:
                return None

            def get_item(self, asset_id: str, clip_path: Path):
                del asset_id, clip_path
                return None

        with (
            patch(
                "src.reddit_intake.fetch_live_story_candidates",
                return_value=(self._build_candidate(post_id="story-pipeline-fetch-002"),),
            ),
            patch(
                "src.reddit_intake.send_pending_story_reviews_to_telegram",
                side_effect=StoryReviewTelegramSendError(
                    "Telegram stage failed.",
                    sent_story_ids=("story-pipeline-fetch-002",),
                ),
            ),
            patch(
                "src.reddit_intake.translate_story_review_to_ukrainian",
                side_effect=RedditIntakeError("DeepL failed."),
            ) as translate_mock,
            patch(
                "src.reddit_intake.PublishQueueStore",
                return_value=FakeQueueStore(),
            ),
            patch(
                "src.reddit_intake.resolve_respeecher_voice_id",
                return_value="olesia-media",
            ),
            patch(
                "src.reddit_intake.render_and_enqueue_story_review",
                return_value=(render_result, queue_result),
            ) as render_enqueue_mock,
        ):
            result = run_full_pipeline(
                background_video_path=self.background_video_path,
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(result.sent_to_telegram, 1)
        self.assertEqual(result.translated, 0)
        self.assertEqual(result.rendered, 1)
        self.assertEqual(result.enqueued, 1)
        translate_mock.assert_called_once_with(
            story_id="story-pipeline-untranslated-001",
            store_path=self.review_store_path,
            translator=reddit_intake._default_translate_adapter,
        )
        render_enqueue_mock.assert_called_once()

    def test_run_full_pipeline_no_duplicates_on_rerun(self) -> None:
        self._make_ready_review("story-pipeline-ready-003")

        class FakeQueueItem:
            def __init__(self, asset_id: str, clip_path: Path) -> None:
                self.asset_id = asset_id
                self.clip_path = clip_path

        class FakeQueueResult:
            def __init__(self, item: FakeQueueItem, created: bool) -> None:
                self.item = item
                self.created = created

        class FakeQueueStore:
            def __init__(self) -> None:
                self.items: dict[tuple[str, Path], FakeQueueItem] = {}

            def initialize(self) -> None:
                return None

            def get_item(self, asset_id: str, clip_path: Path):
                return self.items.get((asset_id, clip_path))

        fake_queue_store = FakeQueueStore()

        def fake_render_and_enqueue(**kwargs):
            story_id = kwargs["story_id"]
            clip_path = kwargs["input_dir"] / story_id / "video.mp4"
            queue_store = kwargs["publish_queue_store"]
            item = FakeQueueItem(asset_id=story_id, clip_path=clip_path)
            queue_store.items[(story_id, clip_path)] = item
            render_result = type(
                "RenderResult",
                (),
                {"story_id": story_id, "bundle_dir": clip_path.parent, "output_path": clip_path, "metadata_path": clip_path.parent / "meta.json"},
            )()
            return render_result, FakeQueueResult(item=item, created=True)

        with (
            patch("src.reddit_intake.fetch_live_story_candidates", return_value=()),
            patch("src.reddit_intake.send_pending_story_reviews_to_telegram", return_value=()),
            patch("src.reddit_intake.PublishQueueStore", return_value=fake_queue_store),
            patch("src.reddit_intake.resolve_respeecher_voice_id", return_value="olesia-media"),
            patch(
                "src.reddit_intake.render_and_enqueue_story_review",
                side_effect=fake_render_and_enqueue,
            ) as render_enqueue_mock,
        ):
            first_result = run_full_pipeline(
                background_video_path=self.background_video_path,
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )
            second_result = run_full_pipeline(
                background_video_path=self.background_video_path,
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(first_result.rendered, 1)
        self.assertEqual(first_result.enqueued, 1)
        self.assertEqual(second_result.rendered, 0)
        self.assertEqual(second_result.enqueued, 0)
        self.assertEqual(render_enqueue_mock.call_count, 1)

    def test_run_full_pipeline_translation_only_runs_when_needed(self) -> None:
        untranslated_candidate = self._build_candidate(post_id="story-pipeline-translate-001")
        translated_candidate = self._build_candidate(post_id="story-pipeline-translate-002")
        save_story_candidates_for_review(
            (untranslated_candidate, translated_candidate),
            store_path=self.review_store_path,
        )
        for story_id in ("story-pipeline-translate-001", "story-pipeline-translate-002"):
            mark_story_selected_for_processing(
                story_id=story_id,
                store_path=self.review_store_path,
            )
            save_operator_rewrite(
                story_id=story_id,
                text=f"Rewrite for {story_id}.",
                store_path=self.review_store_path,
            )
        set_translated_script(
            story_id="story-pipeline-translate-002",
            translated_text="Вже перекладено.",
            translation_characters_used=13,
            store_path=self.review_store_path,
        )

        class FakeQueueStore:
            def initialize(self) -> None:
                return None

            def get_item(self, asset_id: str, clip_path: Path):
                del asset_id, clip_path
                return None

        with (
            patch("src.reddit_intake.fetch_live_story_candidates", return_value=()),
            patch("src.reddit_intake.send_pending_story_reviews_to_telegram", return_value=()),
            patch(
                "src.reddit_intake.translate_story_review_to_ukrainian",
                side_effect=lambda story_id, store_path, translator: get_story_review(
                    story_id,
                    store_path=store_path,
                ),
            ) as translate_mock,
            patch("src.reddit_intake.PublishQueueStore", return_value=FakeQueueStore()),
            patch("src.reddit_intake.render_and_enqueue_story_review") as render_enqueue_mock,
        ):
            result = run_full_pipeline(
                background_video_path=self.background_video_path,
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(result.translated, 1)
        translate_mock.assert_called_once_with(
            story_id="story-pipeline-translate-001",
            store_path=self.review_store_path,
            translator=reddit_intake._default_translate_adapter,
        )
        render_enqueue_mock.assert_not_called()

    def test_run_full_pipeline_render_only_runs_when_ready(self) -> None:
        ready_review = self._make_ready_review("story-pipeline-render-001")
        pending_candidate = self._build_candidate(post_id="story-pipeline-render-002")
        save_story_candidates_for_review((pending_candidate,), store_path=self.review_store_path)
        update_story_review_status(
            story_id="story-pipeline-render-002",
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )

        render_result = type(
            "RenderResult",
            (),
            {"story_id": ready_review.story_id, "output_path": self.inbox_dir / "story-pipeline-render-001" / "video.mp4"},
        )()
        queue_item = type("QueueItem", (), {"asset_id": ready_review.story_id})()
        queue_result = type("QueueResult", (), {"created": True, "item": queue_item})()

        class FakeQueueStore:
            def initialize(self) -> None:
                return None

            def get_item(self, asset_id: str, clip_path: Path):
                del asset_id, clip_path
                return None

        with (
            patch("src.reddit_intake.fetch_live_story_candidates", return_value=()),
            patch("src.reddit_intake.send_pending_story_reviews_to_telegram", return_value=()),
            patch("src.reddit_intake.PublishQueueStore", return_value=FakeQueueStore()),
            patch("src.reddit_intake.resolve_respeecher_voice_id", return_value="olesia-media"),
            patch(
                "src.reddit_intake.render_and_enqueue_story_review",
                return_value=(render_result, queue_result),
            ) as render_enqueue_mock,
        ):
            result = run_full_pipeline(
                background_video_path=self.background_video_path,
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(result.rendered, 1)
        self.assertEqual(result.enqueued, 1)
        render_enqueue_mock.assert_called_once_with(
            story_id="story-pipeline-render-001",
            background_video_path=self.background_video_path,
            output_basename="story-pipeline-render-001",
            store_path=self.review_store_path,
            input_dir=self.inbox_dir,
            publish_queue_store=ANY,
            pre_resolved_voice_id=ANY,
        )

    def test_run_full_pipeline_resolves_voice_once_before_render_loop(self) -> None:
        self._make_ready_review("story-pipeline-voice-001")
        self._make_ready_review("story-pipeline-voice-002")

        class FakeQueueStore:
            def initialize(self) -> None:
                return None

            def get_item(self, asset_id: str, clip_path: Path):
                del asset_id, clip_path
                return None

        render_result = type(
            "RenderResult",
            (),
            {"story_id": "story-pipeline-voice-001", "output_path": self.inbox_dir / "story-pipeline-voice-001" / "video.mp4"},
        )()
        queue_item = type("QueueItem", (), {"asset_id": "story-pipeline-voice-001"})()
        queue_result = type("QueueResult", (), {"created": True, "item": queue_item})()

        with (
            patch("src.reddit_intake.fetch_live_story_candidates", return_value=()),
            patch("src.reddit_intake.send_pending_story_reviews_to_telegram", return_value=()),
            patch("src.reddit_intake.PublishQueueStore", return_value=FakeQueueStore()),
            patch("src.reddit_intake.resolve_respeecher_voice_id", return_value="olesia-media") as resolve_voice_mock,
            patch(
                "src.reddit_intake.render_and_enqueue_story_review",
                return_value=(render_result, queue_result),
            ) as render_enqueue_mock,
        ):
            result = run_full_pipeline(
                background_video_path=self.background_video_path,
                store_path=self.review_store_path,
                input_dir=self.inbox_dir,
            )

        self.assertEqual(result.rendered, 2)
        resolve_voice_mock.assert_called_once_with("")
        self.assertEqual(render_enqueue_mock.call_count, 2)
        for call in render_enqueue_mock.call_args_list:
            self.assertEqual(call.kwargs["pre_resolved_voice_id"], "olesia-media")

    def _build_candidate(
        self,
        post_id: str = "abc123",
        subreddit: str = "AmItheAsshole",
        title: str = "AITA for telling my roommate the truth?",
        body: str = (
            "I moved in six months ago and things have been getting steadily worse. "
            "Yesterday everything finally boiled over and now everyone is upset."
        ),
        score: int = config.REDDIT_MIN_SCORE + 100,
        url: str = "https://reddit.example/posts/abc123",
        created_utc: float = 1_700_000_000.0,
    ) -> StoryCandidate:
        return StoryCandidate(
            post_id=post_id,
            subreddit=subreddit,
            title=title,
            body=body,
            score=score,
            url=url,
            created_utc=created_utc,
        )

    def _make_ready_review(self, story_id: str) -> StoredStoryReview:
        candidate = self._build_candidate(post_id=story_id)
        save_story_candidates_for_review((candidate,), store_path=self.review_store_path)
        mark_story_selected_for_processing(
            story_id=story_id,
            store_path=self.review_store_path,
        )
        save_operator_rewritten_text(
            story_id=story_id,
            rewritten_text=f"Rewrite for {story_id}.",
            store_path=self.review_store_path,
        )
        set_translated_script(
            story_id=story_id,
            translated_text=f"Український сценарій для {story_id}.",
            translation_characters_used=32,
            store_path=self.review_store_path,
        )
        return update_story_review_status(
            story_id=story_id,
            status=APPROVED_STATUS,
            store_path=self.review_store_path,
        )

    def _reddit_listing_payload(
        self,
        subreddit: str,
        posts: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "data": {
                "children": [
                    {
                        "data": {
                            "subreddit": subreddit,
                            **post,
                        }
                    }
                    for post in posts
                ]
            }
        }

    def _voice(self, voice_id: str, name: str):
        class FakeVoice:
            def __init__(self, voice_id: str, name: str) -> None:
                self.id = voice_id
                self.name = name

        return FakeVoice(voice_id=voice_id, name=name)

    def _fake_client(
        self,
        voices: list[object],
        audio_chunks: list[bytes] | None = None,
        tts_error: Exception | None = None,
    ):
        class FakeVoices:
            def __init__(self, voices: list[object]) -> None:
                self._voices = voices

            def list(self):
                return self._voices

        class FakeTts:
            def __init__(
                self,
                audio_chunks: list[bytes] | None,
                tts_error: Exception | None,
            ) -> None:
                self._audio_chunks = audio_chunks or []
                self._tts_error = tts_error

            def bytes(self, **kwargs):
                del kwargs
                if self._tts_error is not None:
                    raise self._tts_error
                return iter(self._audio_chunks)

        class FakeClient:
            def __init__(
                self,
                voices: list[object],
                audio_chunks: list[bytes] | None,
                tts_error: Exception | None,
            ) -> None:
                self.voices = FakeVoices(voices)
                self.tts = FakeTts(audio_chunks, tts_error)

        return FakeClient(voices=voices, audio_chunks=audio_chunks, tts_error=tts_error)


if __name__ == "__main__":
    unittest.main()
