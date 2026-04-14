from __future__ import annotations

import dataclasses
import unittest
from pathlib import Path
from unittest.mock import patch

import src.telegram_bot as _bot_module
from src.advice_pipeline import (
    ADVICE_PENDING_STATUS,
    AdviceMicroSeries,
    AdvicePartScript,
    AdvicePipelineError,
    AdviceRenderResult,
    AdviceReview,
    AdviceReviewStats,
    AdviceScript,
    AdviceVoiceSession,
)
from src.content_plan import ContentTopic
from src.reddit_intake import (
    APPROVED_STATUS,
    PENDING_REVIEW_STATUS,
    RedditIntakeError,
    StoryCandidate,
    StoryIntakeStage,
    StoryModerationStage,
    StoryProductionStage,
    StoryReviewStats,
    StoredStoryReview,
    TelegramReviewMetadata,
)
from src.telegram_bot import run_polling_loop


class TelegramBotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.background_video_path = Path("/tmp/background.mp4")

    def test_polling_loop_continues_after_per_update_exception(self) -> None:
        processed_updates: list[int] = []
        sent_messages: list[tuple[str, str]] = []

        def fake_get_updates(offset: int | None):
            del offset
            return (
                {"update_id": 1, "message": {"chat": {"id": "chat-1"}, "text": "/status"}},
                {"update_id": 2, "message": {"chat": {"id": "chat-2"}, "text": "/status"}},
            )

        def fake_send_message(chat_id: str, text: str, reply_markup):
            del reply_markup
            sent_messages.append((chat_id, text))
            return {"message_id": 1}

        with patch(
            "src.telegram_bot._process_update",
            side_effect=[
                RuntimeError("boom"),
                processed_updates.append(2),
            ],
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=fake_get_updates,
                send_message=fake_send_message,
                send_video=lambda chat_id, video_path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        self.assertEqual(processed_updates, [2])
        self.assertEqual(sent_messages, [("chat-1", "Error: boom")])

    def test_status_command_returns_formatted_stats(self) -> None:
        sent_messages: list[tuple[str, str]] = []

        with (
            patch(
                "src.telegram_bot.get_story_review_stats",
                return_value=StoryReviewStats(
                    total=12,
                    pending_review=7,
                    approved=3,
                    rejected=2,
                    sent_to_telegram=5,
                    eligible_to_send=4,
                ),
            ),
            patch(
                "src.telegram_bot.get_advice_stats",
                return_value=AdviceReviewStats(
                    total=2,
                    pending_review=1,
                    approved=1,
                    rejected=0,
                ),
            ),
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {"update_id": 1, "message": {"chat": {"id": "chat-1"}, "text": "/status"}},
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append((chat_id, text)) or {"message_id": 1},
                send_video=lambda chat_id, video_path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        self.assertEqual(
            sent_messages,
            [(
                "chat-1",
                "total=12 | pending_review=7 | approved=3 | rejected=2 | sent_to_telegram=5 | eligible_to_send=4\n"
                "advice_total=2 | advice_pending=1 | advice_approved=1",
            )],
        )

    def test_fetch_command_sends_to_telegram_after_persisting(self) -> None:
        sent_messages: list[tuple[str, str]] = []
        candidate = StoryCandidate(
            post_id="story-fetch-001",
            subreddit="TIFU",
            title="Title",
            body="Body",
            score=7001,
            url="https://reddit.example/story-fetch-001",
            created_utc=1700000000.0,
        )
        prepared = type(
            "Prepared",
            (),
            {
                "results": (
                    type(
                        "PreparedResult",
                        (),
                        {"status": "accepted", "candidate": candidate},
                    )(),
                )
            },
        )()

        with (
            patch("src.telegram_bot.fetch_live_story_candidates", return_value=(candidate,)),
            patch("src.telegram_bot.prepare_story_candidates", return_value=prepared),
            patch("src.telegram_bot.save_story_candidates_for_review", return_value=(object(),)),
            patch(
                "src.telegram_bot.send_pending_story_reviews_to_telegram",
                return_value=("story-fetch-001", "story-fetch-002"),
            ) as send_pending_mock,
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {"update_id": 1, "message": {"chat": {"id": "chat-1"}, "text": "/fetch"}},
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append((chat_id, text)) or {"message_id": 1},
                send_video=lambda chat_id, video_path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        send_pending_mock.assert_called_once_with()
        self.assertEqual(
            sent_messages,
            [("chat-1", "fetched=1 | accepted=1 | persisted=1 | sent_to_telegram=2")],
        )

    def test_fetch_command_reports_zero_sent_on_telegram_failure(self) -> None:
        sent_messages: list[tuple[str, str]] = []
        candidate = StoryCandidate(
            post_id="story-fetch-003",
            subreddit="TIFU",
            title="Title",
            body="Body",
            score=7001,
            url="https://reddit.example/story-fetch-003",
            created_utc=1700000000.0,
        )
        prepared = type(
            "Prepared",
            (),
            {
                "results": (
                    type(
                        "PreparedResult",
                        (),
                        {"status": "accepted", "candidate": candidate},
                    )(),
                )
            },
        )()

        with (
            patch("src.telegram_bot.fetch_live_story_candidates", return_value=(candidate,)),
            patch("src.telegram_bot.prepare_story_candidates", return_value=prepared),
            patch("src.telegram_bot.save_story_candidates_for_review", return_value=(object(),)),
            patch(
                "src.telegram_bot.send_pending_story_reviews_to_telegram",
                side_effect=RedditIntakeError("telegram boom"),
            ),
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {"update_id": 1, "message": {"chat": {"id": "chat-1"}, "text": "/fetch"}},
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append((chat_id, text)) or {"message_id": 1},
                send_video=lambda chat_id, video_path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        self.assertEqual(
            sent_messages,
            [("chat-1", "fetched=1 | accepted=1 | persisted=1 | sent_to_telegram=0")],
        )

    def test_unmapped_reply_sends_error_message_back_to_chat(self) -> None:
        sent_messages: list[tuple[str, str]] = []

        with patch(
            "src.telegram_bot.ingest_telegram_rewrite_reply",
            side_effect=RedditIntakeError("Telegram reply cannot be mapped to a story."),
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {
                        "update_id": 1,
                        "message": {
                            "chat": {"id": "chat-1"},
                            "text": "rewritten script",
                            "reply_to_message": {"message_id": 123},
                        },
                    },
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append((chat_id, text)) or {"message_id": 1},
                send_video=lambda chat_id, video_path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        self.assertEqual(
            sent_messages,
            [
                (
                    "chat-1",
                    "Could not map this reply to a story. Please reply directly to the instruction message.",
                )
            ],
        )

    def test_reply_message_sends_render_button_for_approved_translated_story(self) -> None:
        sent_messages: list[tuple[str, str, object | None]] = []

        with patch(
            "src.telegram_bot.ingest_telegram_rewrite_reply",
            return_value=self._review(
                status=APPROVED_STATUS,
                translated_script_uk="Переклад готовий.",
                operator_rewritten_text=None,
                translation_characters_used=24,
            ),
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {
                        "update_id": 1,
                        "message": {
                            "chat": {"id": "chat-1"},
                            "text": "rewritten script",
                            "reply_to_message": {"message_id": 123},
                        },
                    },
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append(
                    (chat_id, text, reply_markup)
                )
                or {"message_id": 1},
                send_video=lambda chat_id, video_path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        self.assertEqual(sent_messages[0], ("chat-1", "Script saved and translated for story story-001.", None))
        self.assertEqual(sent_messages[1][0], "chat-1")
        self.assertEqual(sent_messages[1][1], "Story is approved and ready to render.")
        self.assertEqual(
            sent_messages[1][2],
            {"inline_keyboard": [[{"text": "Render", "callback_data": "render:story-001"}]]},
        )

    def test_reply_message_sends_approval_reminder_for_translated_pending_story(self) -> None:
        sent_messages: list[tuple[str, str, object | None]] = []

        with patch(
            "src.telegram_bot.ingest_telegram_rewrite_reply",
            return_value=self._review(
                status=PENDING_REVIEW_STATUS,
                translated_script_uk="Переклад готовий.",
                operator_rewritten_text=None,
                translation_characters_used=24,
            ),
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {
                        "update_id": 1,
                        "message": {
                            "chat": {"id": "chat-1"},
                            "text": "rewritten script",
                            "reply_to_message": {"message_id": 123},
                        },
                    },
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append(
                    (chat_id, text, reply_markup)
                )
                or {"message_id": 1},
                send_video=lambda chat_id, video_path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        self.assertEqual(sent_messages[0], ("chat-1", "Script saved and translated for story story-001.", None))
        self.assertEqual(
            sent_messages[1],
            (
                "chat-1",
                "Translation complete. Approve story story-001 with 'review --status approved' before rendering.",
                None,
            ),
        )

    def test_reply_with_uk_prefix_sends_no_translation_confirmation(self) -> None:
        sent_messages: list[tuple[str, str, object | None]] = []

        with patch(
            "src.telegram_bot.ingest_telegram_rewrite_reply",
            return_value=self._review(
                status=PENDING_REVIEW_STATUS,
                translated_script_uk="Готовий український текст.",
                operator_rewritten_text="Готовий український текст.",
                translation_characters_used=None,
            ),
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {
                        "update_id": 1,
                        "message": {
                            "chat": {"id": "chat-1"},
                            "text": "[UK] Готовий український текст.",
                            "reply_to_message": {"message_id": 123},
                        },
                    },
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append(
                    (chat_id, text, reply_markup)
                )
                or {"message_id": 1},
                send_video=lambda chat_id, video_path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        self.assertEqual(
            sent_messages[0],
            (
                "chat-1",
                "Ukrainian script saved directly for story story-001. No translation needed.",
                None,
            ),
        )
        self.assertEqual(
            sent_messages[1],
            (
                "chat-1",
                "Translation complete. Approve story story-001 with 'review --status approved' before rendering.",
                None,
            ),
        )

    def test_reply_without_uk_prefix_sends_translated_confirmation(self) -> None:
        sent_messages: list[tuple[str, str, object | None]] = []

        with patch(
            "src.telegram_bot.ingest_telegram_rewrite_reply",
            return_value=self._review(
                status=PENDING_REVIEW_STATUS,
                translated_script_uk="Перекладений текст.",
                operator_rewritten_text="English rewrite.",
                translation_characters_used=42,
            ),
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {
                        "update_id": 1,
                        "message": {
                            "chat": {"id": "chat-1"},
                            "text": "English rewrite.",
                            "reply_to_message": {"message_id": 123},
                        },
                    },
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append(
                    (chat_id, text, reply_markup)
                )
                or {"message_id": 1},
                send_video=lambda chat_id, video_path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        self.assertEqual(
            sent_messages[0],
            ("chat-1", "Script saved and translated for story story-001.", None),
        )

    def test_render_callback_without_background_path_sends_error_message(self) -> None:
        sent_messages: list[tuple[str, str]] = []

        run_polling_loop(
            background_video_path=None,
            queue_store=self._fake_queue_store(),
            get_updates=lambda offset: (
                {
                    "update_id": 1,
                    "callback_query": {
                        "data": "render:story-001",
                        "message": {"chat": {"id": "chat-1"}},
                    },
                },
            ),
            send_message=lambda chat_id, text, reply_markup: sent_messages.append((chat_id, text))
            or {"message_id": 1},
            send_video=lambda chat_id, video_path, caption: {"message_id": 2},
            sleep_func=lambda seconds: None,
            max_cycles=1,
        )

        self.assertEqual(
            sent_messages,
            [
                (
                    "chat-1",
                    "Error: Render is not available: BACKGROUND_VIDEO_PATH is not configured. Restart the bot with --background-video-path or set BACKGROUND_VIDEO_PATH.",
                )
            ],
        )

    # 4i
    def test_scripts_command_parses_and_renders_all_parts(self) -> None:
        sent_messages: list[tuple[str, str]] = []
        sent_videos: list[tuple[str, Path]] = []
        enqueued: list[object] = []

        fake_results = tuple(
            AdviceRenderResult(
                script_id=f"rev-001_part{i}",
                output_path=Path(f"/tmp/part{i}.mp4"),
                subtitle_path=Path(f"/tmp/part{i}.srt"),
                audio_path=Path(f"/tmp/part{i}.wav"),
                background_video_path=Path("/tmp/bg.mp4"),
            )
            for i in range(1, 4)
        )

        fake_review = AdviceReview(
            script=AdviceScript(
                script_id="rev-001",
                topic="Тема 1",
                topic_slug="tema-1",
                generated_script="",
                saved_at="2026-01-01T00:00:00Z",
            ),
            status=ADVICE_PENDING_STATUS,
            telegram_chat_id=None,
            telegram_message_id=None,
            updated_at="2026-01-01T00:00:00Z",
        )

        class FakeQueueStore:
            def initialize(self) -> None:
                pass

            def list_queued_items(self):
                return ()

            def enqueue_candidate(self, candidate) -> None:
                enqueued.append(candidate)

        fake_topic = type("T", (), {
            "title": "Тема 1",
            "topic_id": "A1",
            "series_id": "A",
            "series_title": "Test Series",
            "scenario": "Scenario",
            "legal_facts": ("Fact 1",),
            "part_number": 1,
            "total_parts": 3,
            "status": "pending",
        })()

        with (
            patch("src.telegram_bot.get_advice_review", return_value=fake_review),
            patch("src.telegram_bot.list_topics", return_value=(fake_topic,)),
            patch("src.telegram_bot.receive_operator_scripts", return_value=fake_results),
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=FakeQueueStore(),
                get_updates=lambda offset: (
                    {
                        "update_id": 1,
                        "message": {
                            "chat": {"id": "chat-1"},
                            "text": "/scripts rev-001 PARTS: 3\n=== ЧАСТИНА 1/3 ===\ntext",
                        },
                    },
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append((chat_id, text)) or {"message_id": 1},
                send_video=lambda chat_id, path, caption: sent_videos.append((chat_id, path)) or {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        self.assertEqual(len(sent_videos), 3)
        self.assertEqual(len(enqueued), 3)

    # 4j
    def test_scripts_command_on_parse_error_sends_error_message(self) -> None:
        sent_messages: list[tuple[str, str]] = []

        fake_review = AdviceReview(
            script=AdviceScript(
                script_id="rev-002",
                topic="Тема 2",
                topic_slug="tema-2",
                generated_script="",
                saved_at="2026-01-01T00:00:00Z",
            ),
            status=ADVICE_PENDING_STATUS,
            telegram_chat_id=None,
            telegram_message_id=None,
            updated_at="2026-01-01T00:00:00Z",
        )

        fake_topic_2 = type("T", (), {
            "title": "Тема 2",
            "topic_id": "A2",
            "series_id": "A",
            "series_title": "Test Series",
            "scenario": "Scenario",
            "legal_facts": ("Fact 1",),
            "part_number": 1,
            "total_parts": 3,
            "status": "pending",
        })()

        with (
            patch("src.telegram_bot.get_advice_review", return_value=fake_review),
            patch("src.telegram_bot.list_topics", return_value=(fake_topic_2,)),
            patch(
                "src.telegram_bot.receive_operator_scripts",
                side_effect=AdvicePipelineError("PARTS line missing"),
            ),
        ):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {
                        "update_id": 1,
                        "message": {
                            "chat": {"id": "chat-1"},
                            "text": "/scripts rev-002 broken AI response",
                        },
                    },
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append((chat_id, text)) or {"message_id": 1},
                send_video=lambda chat_id, path, caption: {"message_id": 2},
                sleep_func=lambda seconds: None,
                max_cycles=1,
            )

        error_messages = [t for _, t in sent_messages if "PARTS line missing" in t]
        self.assertEqual(len(error_messages), 1)
        self.assertIn("broken AI response", error_messages[0])

    @staticmethod
    def _fake_queue_store():
        class FakeQueueStore:
            def initialize(self) -> None:
                return None

            def list_queued_items(self):
                return ()

        return FakeQueueStore()

    @staticmethod
    def _review(
        *,
        status: str,
        translated_script_uk: str | None,
        operator_rewritten_text: str | None,
        translation_characters_used: int | None = None,
    ) -> StoredStoryReview:
        return StoredStoryReview(
            intake=StoryIntakeStage(
                story_id="story-001",
                source_title="Title",
                source_body="Body",
                subreddit="TIFU",
                url="https://reddit.example/story-001",
                score=7001,
                created_utc=1700000000.0,
            ),
            moderation=StoryModerationStage(
                selected_for_processing=True,
                operator_rewritten_text=operator_rewritten_text,
                script_saved_at="2026-03-25T12:00:00Z",
            ),
            production=StoryProductionStage(
                translated_script_uk=translated_script_uk,
                translation_characters_used=translation_characters_used,
            ),
            telegram=TelegramReviewMetadata(chat_id="chat-1", instruction_message_id=123),
            status=status,
            sent_to_telegram=True,
            saved_at="2026-03-25T12:00:00Z",
        )


def _make_content_topic(
    topic_id: str = "A1",
    series_id: str = "A",
    series_title: str = "Серія А",
    title: str = "Тема А1",
    part_number: int = 1,
    total_parts: int = 2,
    status: str = "pending",
) -> ContentTopic:
    return ContentTopic(
        topic_id=topic_id,
        series_id=series_id,
        series_title=series_title,
        part_number=part_number,
        total_parts=total_parts,
        title=title,
        hook_formula="Hook",
        audience="Aud",
        scenario="Scen",
        legal_facts=(),
        status=status,
        script_id=None,
        script_saved_at=None,
    )


class AdvicePlanUITests(unittest.TestCase):
    def setUp(self) -> None:
        self.background_video_path = Path("/tmp/background.mp4")
        _bot_module._factory_messages.clear()

    def tearDown(self) -> None:
        _bot_module._factory_messages.clear()

    # 9d
    def test_plan_command_sends_series_list_with_buttons(self) -> None:
        sent_messages: list[tuple[str, str, object]] = []

        fake_topics = (
            _make_content_topic(topic_id="A1", series_id="A", series_title="Серія А"),
            _make_content_topic(topic_id="B1", series_id="B", series_title="Серія Б"),
        )

        with patch("src.telegram_bot.list_topics", return_value=fake_topics):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {"update_id": 1, "message": {"chat": {"id": "chat-1"}, "text": "/plan"}},
                ),
                send_message=lambda chat_id, text, reply_markup: sent_messages.append(
                    (chat_id, text, reply_markup)
                ) or {"message_id": 77},
                send_video=lambda *a: {"message_id": 2},
                sleep_func=lambda s: None,
                max_cycles=1,
            )

        self.assertEqual(len(sent_messages), 1)
        _, _, markup = sent_messages[0]
        all_cb_data = [
            btn["callback_data"]
            for row in markup["inline_keyboard"]
            for btn in row
        ]
        self.assertIn("series:A", all_cb_data)
        self.assertIn("series:B", all_cb_data)
        self.assertEqual(_bot_module._factory_messages.get("chat-1"), 77)

    # 9e
    def test_series_callback_edits_factory_message(self) -> None:
        edited: list[tuple] = []
        _bot_module._factory_messages["chat-1"] = 42

        fake_topics = (
            _make_content_topic(topic_id="A1", series_id="A", title="Тема А1"),
            _make_content_topic(topic_id="A2", series_id="A", title="Тема А2"),
        )

        with patch("src.telegram_bot.list_topics", return_value=fake_topics):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {
                        "update_id": 1,
                        "callback_query": {
                            "id": "cq-1",
                            "data": "series:A",
                            "message": {"chat": {"id": "chat-1"}},
                        },
                    },
                ),
                send_message=lambda *a: {"message_id": 1},
                send_video=lambda *a: {"message_id": 2},
                edit_message=lambda *args: edited.append(args),
                answer_callback=lambda *a: None,
                sleep_func=lambda s: None,
                max_cycles=1,
            )

        self.assertEqual(len(edited), 1)
        _chat_id, msg_id, text, _markup = edited[0]
        self.assertEqual(msg_id, 42)
        self.assertIn("Тема А1", text)

    # 9f
    def test_topic_callback_issues_review_and_edits_to_detail(self) -> None:
        edited: list[tuple] = []
        _bot_module._factory_messages["chat-1"] = 10

        fake_topic = _make_content_topic(topic_id="A1", series_id="A", title="Тема А1")
        fake_review = AdviceReview(
            script=AdviceScript(
                script_id="rev-123",
                topic="Тема А1",
                topic_slug="tema-a1",
                generated_script="",
                saved_at="2026-01-01T00:00:00Z",
            ),
            status=ADVICE_PENDING_STATUS,
            telegram_chat_id=None,
            telegram_message_id=None,
            updated_at="2026-01-01T00:00:00Z",
        )

        with patch("src.telegram_bot.issue_specific_topic", return_value=(fake_review, fake_topic)):
            run_polling_loop(
                background_video_path=self.background_video_path,
                queue_store=self._fake_queue_store(),
                get_updates=lambda offset: (
                    {
                        "update_id": 1,
                        "callback_query": {
                            "id": "cq-2",
                            "data": "topic:A1",
                            "message": {"chat": {"id": "chat-1"}},
                        },
                    },
                ),
                send_message=lambda *a: {"message_id": 1},
                send_video=lambda *a: {"message_id": 2},
                edit_message=lambda *args: edited.append(args),
                answer_callback=lambda *a: None,
                sleep_func=lambda s: None,
                max_cycles=1,
            )

        self.assertEqual(len(edited), 1)
        _chat_id, _msg_id, _text, markup = edited[0]
        all_cb_data = [
            btn["callback_data"]
            for row in markup["inline_keyboard"]
            for btn in row
        ]
        self.assertIn("prompt:A1", all_cb_data)

    # 9g
    def test_close_callback_edits_to_closed(self) -> None:
        edited: list[tuple] = []
        _bot_module._factory_messages["chat-1"] = 55

        run_polling_loop(
            background_video_path=self.background_video_path,
            queue_store=self._fake_queue_store(),
            get_updates=lambda offset: (
                {
                    "update_id": 1,
                    "callback_query": {
                        "id": "cq-3",
                        "data": "close",
                        "message": {"chat": {"id": "chat-1"}, "message_id": 55},
                    },
                },
            ),
            send_message=lambda *a: {"message_id": 1},
            send_video=lambda *a: {"message_id": 2},
            edit_message=lambda *args: edited.append(args),
            answer_callback=lambda *a: None,
            sleep_func=lambda s: None,
            max_cycles=1,
        )

        self.assertEqual(len(edited), 1)
        _chat_id, msg_id, _text, markup = edited[0]
        self.assertEqual(msg_id, 55)
        self.assertEqual(markup, {"inline_keyboard": []})

    # 9h
    def test_done_state_has_next_topic_button(self) -> None:
        from src.telegram_bot import _build_done_message

        topic = _make_content_topic()
        _text, markup = _build_done_message(topic, 3)

        all_cb_data = [
            btn["callback_data"]
            for row in markup["inline_keyboard"]
            for btn in row
        ]
        self.assertIn("next_topic", all_cb_data)

    @staticmethod
    def _fake_queue_store():
        class FakeQueueStore:
            def initialize(self) -> None:
                return None

            def list_queued_items(self):
                return ()

            def enqueue_candidate(self, candidate) -> None:
                pass

        return FakeQueueStore()


def _make_voice_session(
    session_id: str = "sess-1",
    review_id: str = "rev-1",
    topic_id: str = "A1",
    n_parts: int = 3,
    voice_files: tuple | None = None,
    part_message_ids: tuple | None = None,
) -> AdviceVoiceSession:
    parts = tuple(
        AdvicePartScript(
            part_number=i,
            total_parts=n_parts,
            script_text=f"Текст частини {i} " + "слово " * 30,
            word_count=30 + 3,
        )
        for i in range(1, n_parts + 1)
    )
    ms = AdviceMicroSeries(
        review_id=review_id,
        topic_id=topic_id,
        series_title="Test Series",
        parts=parts,
        background_video_path=Path("/tmp/bg.mp4"),
        parsed_at="2026-01-01T00:00:00Z",
    )
    return AdviceVoiceSession(
        session_id=session_id,
        review_id=review_id,
        topic_id=topic_id,
        micro_series=ms,
        voice_files=voice_files if voice_files is not None else tuple(None for _ in range(n_parts)),
        part_message_ids=part_message_ids if part_message_ids is not None else tuple(None for _ in range(n_parts)),
        background_video_path=Path("/tmp/bg.mp4"),
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )


class VoicePipelineBotTests(unittest.TestCase):
    def setUp(self) -> None:
        _bot_module._factory_messages.clear()
        _bot_module._voice_sessions.clear()
        _bot_module._part_script_msg_ids.clear()
        _bot_module._pending_voice_confirmations.clear()

    @staticmethod
    def _fake_queue_store():
        class FakeQueueStore:
            def initialize(self) -> None:
                return None

            def list_queued_items(self):
                return ()

            def enqueue_candidate(self, candidate) -> None:
                pass

        return FakeQueueStore()

    # 10k
    def test_voice_reply_routes_to_handle_voice_reply(self) -> None:
        """A reply message with a 'voice' field must route to _handle_voice_reply."""
        called: list[bool] = []

        with patch("src.telegram_bot._handle_voice_reply", side_effect=lambda **kw: called.append(True)):
            _bot_module._handle_reply_message(
                message_payload={
                    "chat": {"id": "chat-1"},
                    "voice": {"file_id": "abc123", "duration": 3},
                    "reply_to_message": {"message_id": 99},
                },
                queue_store=self._fake_queue_store(),
                send_message=lambda *a, **kw: {"message_id": 1},
                send_video=lambda *a, **kw: {"message_id": 2},
                edit_message=lambda *a, **kw: {},
                download_file=lambda fid: b"fake-audio",
            )

        self.assertEqual(len(called), 1)

    # 10l
    def test_voice_reply_unknown_part_sends_error_message(self) -> None:
        """Voice reply to a non-tracked message_id → send ❌ error."""
        sent: list[str] = []

        with patch(
            "src.telegram_bot.get_voice_session_by_part_message_id",
            return_value=None,
        ):
            _bot_module._handle_voice_reply(
                message_payload={
                    "chat": {"id": "chat-1"},
                    "voice": {"file_id": "abc123"},
                    "reply_to_message": {"message_id": 42},
                },
                send_message=lambda chat_id, text, markup: sent.append(text) or {"message_id": 1},
                download_file=lambda fid: b"fake-audio",
            )

        self.assertTrue(any("❌" in m for m in sent))

    # 10m
    def test_voice_collection_message_shows_progress(self) -> None:
        """Part 1 done, part 2 current, part 3 future → correct emoji per part."""
        from src.telegram_bot import _build_voice_collection_message
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            wav1 = Path(tmp) / "p1.wav"
            # Write a minimal valid WAV header so _wav_duration_str doesn't fail.
            wav1.write_bytes(b"RIFF\x24\x00\x00\x00WAVEfmt \x10\x00\x00\x00\x01\x00\x01\x00\x80\x3e\x00\x00\x00\x7d\x00\x00\x02\x00\x10\x00data\x00\x00\x00\x00")
            session = _make_voice_session(
                n_parts=3,
                voice_files=(wav1, None, None),
                part_message_ids=(101, None, None),
            )
            topic = _make_content_topic(title="Тема тест")

            text, markup = _build_voice_collection_message(session, topic)

        self.assertIn("✅", text)
        self.assertIn("🎤", text)
        self.assertIn("⏳", text)
        lines_with_check = [l for l in text.splitlines() if "✅" in l]
        lines_with_mic = [l for l in text.splitlines() if "🎤" in l]
        lines_with_wait = [l for l in text.splitlines() if "⏳" in l]
        self.assertEqual(len(lines_with_check), 1)
        self.assertIn("1/3", lines_with_check[0])
        self.assertEqual(len(lines_with_mic), 1)
        self.assertIn("2/3", lines_with_mic[0])
        self.assertEqual(len(lines_with_wait), 1)
        self.assertIn("3/3", lines_with_wait[0])

    # 10n
    def test_voice_reply_stores_pending_does_not_render(self) -> None:
        """Receiving voice reply stores pending confirmation; render is NOT triggered."""
        import tempfile

        render_called: list[bool] = []
        sent_texts: list[str] = []

        with tempfile.TemporaryDirectory() as tmp:
            session = _make_voice_session(
                session_id="sess-complete",
                n_parts=3,
                part_message_ids=(101, 102, 103),
            )

            _bot_module._factory_messages["chat-1"] = 55

            with (
                patch(
                    "src.telegram_bot.get_voice_session_by_part_message_id",
                    return_value=(session, 3),
                ),
                patch(
                    "src.telegram_bot.render_voice_session",
                    side_effect=lambda s, **kw: render_called.append(True),
                ),
                patch("src.telegram_bot.convert_ogg_to_wav"),
                patch.object(Path, "mkdir", return_value=None),
                patch.object(Path, "write_bytes", return_value=None),
            ):
                _bot_module._handle_voice_reply(
                    message_payload={
                        "chat": {"id": "chat-1"},
                        "voice": {"file_id": "fid-3"},
                        "reply_to_message": {"message_id": 103},
                    },
                    send_message=lambda chat_id, text, markup: sent_texts.append(text) or {"message_id": 10},
                    download_file=lambda fid: b"fake-audio",
                )

        self.assertFalse(render_called)
        self.assertIn(3, _bot_module._pending_voice_confirmations.get("chat-1", {}))
        self.assertTrue(any("запис отримано" in t for t in sent_texts))

    # 10o
    def test_resend_part_callback_sends_part_script_message(self) -> None:
        """'resend_part:{sid}:2' callback → send_message called with part 2 script text."""
        sent_texts: list[str] = []
        session = _make_voice_session(
            session_id="sess-resend",
            n_parts=3,
            part_message_ids=(101, 102, None),
        )

        _bot_module._factory_messages["chat-1"] = 55

        with (
            patch("src.telegram_bot.get_voice_session", return_value=session),
            patch("src.telegram_bot.save_part_message_id", return_value=session),
        ):
            _bot_module._handle_callback_query(
                callback_query={
                    "id": "cq-1",
                    "data": f"resend_part:sess-resend:2",
                    "message": {"chat": {"id": "chat-1"}, "message_id": 55},
                },
                background_video_path=None,
                queue_store=self._fake_queue_store(),
                send_message=lambda chat_id, text, markup: sent_texts.append(text) or {"message_id": 20},
                send_video=lambda *a, **kw: {"message_id": 2},
                edit_message=lambda *a, **kw: {},
                answer_callback=lambda *a, **kw: None,
            )

        self.assertTrue(any("Частина 2" in t for t in sent_texts))
        part2_script = session.micro_series.parts[1].script_text
        self.assertTrue(any(part2_script[:20] in t for t in sent_texts))

    # 12a
    def test_voice_reply_stores_pending_not_committed(self) -> None:
        """Voice received → pending stored, save_part_voice NOT called, confirmation sent."""
        import tempfile
        from typing import Any

        save_called: list[bool] = []
        sent_calls: list[tuple[str, Any]] = []

        with tempfile.TemporaryDirectory() as tmp:
            session = _make_voice_session(
                session_id="sess-12a",
                review_id="rev-12a",
                n_parts=2,
                part_message_ids=(10, None),
            )

            with (
                patch(
                    "src.telegram_bot.get_voice_session_by_part_message_id",
                    return_value=(session, 1),
                ),
                patch(
                    "src.telegram_bot.save_part_voice",
                    side_effect=lambda *a, **kw: save_called.append(True),
                ),
                patch("src.telegram_bot.convert_ogg_to_wav"),
                patch.object(Path, "mkdir", return_value=None),
                patch.object(Path, "write_bytes", return_value=None),
            ):
                _bot_module._handle_voice_reply(
                    message_payload={
                        "chat": {"id": "chat-12a"},
                        "voice": {"file_id": "fid-1"},
                        "reply_to_message": {"message_id": 10},
                    },
                    send_message=lambda chat_id, text, markup: sent_calls.append((text, markup)) or {"message_id": 1},
                    download_file=lambda fid: b"fake-audio",
                )

        self.assertFalse(save_called)
        self.assertIn(1, _bot_module._pending_voice_confirmations.get("chat-12a", {}))
        self.assertTrue(any("✅ Прийняти" in str(markup) for _, markup in sent_calls if markup))

    # 12b
    def test_confirm_voice_commits_and_sends_next_part(self) -> None:
        """confirm_voice:sid:1 → save_part_voice called; part 2 script sent; pending removed."""
        import tempfile

        save_called: list[bool] = []
        sent_texts: list[str] = []

        with tempfile.TemporaryDirectory() as tmp:
            pending_wav = Path(tmp) / "voice_pending.wav"
            pending_wav.write_bytes(b"fake")

            session = _make_voice_session(
                session_id="sess-12b",
                review_id="rev-12b",
                n_parts=2,
                part_message_ids=(10, None),
            )

            _bot_module._pending_voice_confirmations["chat-12b"] = {
                1: ("sess-12b", pending_wav)
            }

            with (
                patch(
                    "src.telegram_bot.get_voice_session",
                    return_value=session,
                ),
                patch(
                    "src.telegram_bot.save_part_voice",
                    side_effect=lambda *a, **kw: save_called.append(True) or session,
                ),
                patch(
                    "src.telegram_bot.is_voice_session_complete",
                    return_value=False,
                ),
                patch(
                    "src.telegram_bot._get_topic_by_id_safe",
                    return_value=None,
                ),
                patch("src.telegram_bot.save_part_message_id"),
                patch.object(Path, "rename", return_value=None),
            ):
                _bot_module._handle_callback_query(
                    callback_query={
                        "id": "cq-12b",
                        "data": "confirm_voice:sess-12b:1",
                        "message": {"chat": {"id": "chat-12b"}, "message_id": 55},
                    },
                    background_video_path=None,
                    queue_store=self._fake_queue_store(),
                    send_message=lambda chat_id, text, markup: sent_texts.append(text) or {"message_id": 20},
                    send_video=lambda *a, **kw: {"message_id": 2},
                    edit_message=lambda *a, **kw: {},
                    answer_callback=lambda *a, **kw: None,
                )

        self.assertTrue(save_called)
        self.assertTrue(any("Частина 2" in t for t in sent_texts))
        self.assertNotIn(1, _bot_module._pending_voice_confirmations.get("chat-12b", {}))

    # 12c
    def test_confirm_voice_triggers_render_when_last_part(self) -> None:
        """confirm on last part → render_voice_session called; send_video called."""
        import tempfile

        render_called: list[bool] = []
        video_sent: list[bool] = []

        with tempfile.TemporaryDirectory() as tmp:
            pending_wav = Path(tmp) / "voice_pending.wav"
            pending_wav.write_bytes(b"fake")
            video_path = Path(tmp) / "video.mp4"

            session = _make_voice_session(
                session_id="sess-12c",
                review_id="rev-12c",
                n_parts=2,
                part_message_ids=(10, 11),
            )

            fake_result = AdviceRenderResult(
                script_id="rev-12c_part1",
                output_path=video_path,
                subtitle_path=Path(tmp) / "subs.srt",
                audio_path=pending_wav,
                background_video_path=Path("/tmp/bg.mp4"),
            )

            _bot_module._pending_voice_confirmations["chat-12c"] = {
                2: ("sess-12c", pending_wav)
            }

            with (
                patch(
                    "src.telegram_bot.get_voice_session",
                    return_value=session,
                ),
                patch(
                    "src.telegram_bot.save_part_voice",
                    return_value=session,
                ),
                patch(
                    "src.telegram_bot.is_voice_session_complete",
                    return_value=True,
                ),
                patch(
                    "src.telegram_bot.render_voice_session",
                    side_effect=lambda s, **kw: render_called.append(True) or (fake_result,),
                ),
                patch(
                    "src.telegram_bot._get_topic_by_id_safe",
                    return_value=None,
                ),
                patch.object(Path, "rename", return_value=None),
            ):
                _bot_module._handle_callback_query(
                    callback_query={
                        "id": "cq-12c",
                        "data": "confirm_voice:sess-12c:2",
                        "message": {"chat": {"id": "chat-12c"}, "message_id": 55},
                    },
                    background_video_path=None,
                    queue_store=self._fake_queue_store(),
                    send_message=lambda *a, **kw: {"message_id": 20},
                    send_video=lambda chat_id, path, caption: video_sent.append(True) or {"message_id": 2},
                    edit_message=lambda *a, **kw: {},
                    answer_callback=lambda *a, **kw: None,
                )

        self.assertTrue(render_called)
        self.assertTrue(video_sent)

    # 12d
    def test_retake_voice_deletes_pending_and_resends_script(self) -> None:
        """retake with existing pending → file deleted; pending removed; script re-sent."""
        import tempfile

        sent_texts: list[str] = []

        with tempfile.TemporaryDirectory() as tmp:
            pending_wav = Path(tmp) / "voice_pending.wav"
            pending_wav.write_bytes(b"fake-audio-data")

            session = _make_voice_session(
                session_id="sess-12d",
                n_parts=2,
                part_message_ids=(10, None),
            )

            _bot_module._pending_voice_confirmations["chat-12d"] = {
                1: ("sess-12d", pending_wav)
            }

            with (
                patch("src.telegram_bot.get_voice_session", return_value=session),
                patch("src.telegram_bot.save_part_message_id"),
            ):
                _bot_module._handle_callback_query(
                    callback_query={
                        "id": "cq-12d",
                        "data": "retake_voice:sess-12d:1",
                        "message": {"chat": {"id": "chat-12d"}, "message_id": 55},
                    },
                    background_video_path=None,
                    queue_store=self._fake_queue_store(),
                    send_message=lambda chat_id, text, markup: sent_texts.append(text) or {"message_id": 20},
                    send_video=lambda *a, **kw: {"message_id": 2},
                    edit_message=lambda *a, **kw: {},
                    answer_callback=lambda *a, **kw: None,
                )

        self.assertFalse(pending_wav.exists())
        self.assertNotIn(1, _bot_module._pending_voice_confirmations.get("chat-12d", {}))
        part1_script = session.micro_series.parts[0].script_text
        self.assertTrue(any(part1_script[:20] in t for t in sent_texts))

    # 12e
    def test_confirm_voice_missing_pending_sends_error(self) -> None:
        """confirm_voice with no pending entry → ❌ error sent."""
        sent_texts: list[str] = []

        _bot_module._handle_callback_query(
            callback_query={
                "id": "cq-12e",
                "data": "confirm_voice:sess-12e:1",
                "message": {"chat": {"id": "chat-12e"}, "message_id": 55},
            },
            background_video_path=None,
            queue_store=self._fake_queue_store(),
            send_message=lambda chat_id, text, markup: sent_texts.append(text) or {"message_id": 1},
            send_video=lambda *a, **kw: {"message_id": 2},
            edit_message=lambda *a, **kw: {},
            answer_callback=lambda *a, **kw: None,
        )

        self.assertTrue(any("❌" in t for t in sent_texts))

    # 12f
    def test_retake_voice_when_no_pending_still_resends_script(self) -> None:
        """retake with no pending entry → no crash; script re-sent."""
        sent_texts: list[str] = []

        session = _make_voice_session(
            session_id="sess-12f",
            n_parts=2,
            part_message_ids=(10, None),
        )

        with (
            patch("src.telegram_bot.get_voice_session", return_value=session),
            patch("src.telegram_bot.save_part_message_id"),
        ):
            _bot_module._handle_callback_query(
                callback_query={
                    "id": "cq-12f",
                    "data": "retake_voice:sess-12f:1",
                    "message": {"chat": {"id": "chat-12f"}, "message_id": 55},
                },
                background_video_path=None,
                queue_store=self._fake_queue_store(),
                send_message=lambda chat_id, text, markup: sent_texts.append(text) or {"message_id": 20},
                send_video=lambda *a, **kw: {"message_id": 2},
                edit_message=lambda *a, **kw: {},
                answer_callback=lambda *a, **kw: None,
            )

        part1_script = session.micro_series.parts[0].script_text
        self.assertTrue(any(part1_script[:20] in t for t in sent_texts))


if __name__ == "__main__":
    unittest.main()
