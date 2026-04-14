from __future__ import annotations

import argparse
import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import call, patch

import reddit_intake_cli
from src import config
from src.reddit_intake import (
    PENDING_REVIEW_STATUS,
    PipelineRunResult,
    StoryCandidate,
    StoryIntakeStage,
    StoryModerationStage,
    StoryProductionStage,
    StoredStoryReview,
    StoryReviewStats,
    TelegramReviewMetadata,
)


class RedditIntakeCliTests(unittest.TestCase):
    def test_command_names_match_expected_operator_workflow(self) -> None:
        parser = reddit_intake_cli._build_parser()
        subparser_action = next(
            action
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )

        self.assertEqual(
            set(subparser_action.choices.keys()),
            {
                "fetch",
                "send-approvals",
                "review",
                "process-telegram-update",
                "translate",
                "stats",
                "list-approved",
                "list-ready",
                "render",
                "render-enqueue",
                "run-pipeline",
            },
        )

    def test_fetch_does_not_send_telegram_messages(self) -> None:
        candidate = self._candidate(post_id="reddit-001")

        with (
            patch("reddit_intake_cli.fetch_live_story_candidates", return_value=(candidate,)) as fetch_mock,
            patch(
                "reddit_intake_cli.save_story_candidates_for_review",
                return_value=(
                    self._review(
                        candidate,
                        status=PENDING_REVIEW_STATUS,
                        sent_to_telegram=False,
                    ),
                ),
            ) as save_mock,
            patch("reddit_intake_cli.send_pending_story_reviews_to_telegram") as send_mock,
        ):
            exit_code, stdout, stderr = self._run_cli("fetch")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("fetched=1", stdout)
        self.assertIn("accepted=1", stdout)
        self.assertIn("persisted=1", stdout)
        fetch_mock.assert_called_once()
        save_mock.assert_called_once()
        send_mock.assert_not_called()

    def test_send_approvals_does_not_fetch_reddit(self) -> None:
        pending_review = self._review(
            self._candidate(post_id="reddit-002"),
            status=PENDING_REVIEW_STATUS,
            sent_to_telegram=False,
        )

        with (
            patch("reddit_intake_cli.fetch_live_story_candidates") as fetch_mock,
            patch("reddit_intake_cli.save_story_candidates_for_review") as save_mock,
            patch(
                "reddit_intake_cli.list_story_reviews",
                side_effect=((pending_review,), (pending_review,)),
            ) as list_mock,
            patch(
                "reddit_intake_cli.send_pending_story_reviews_to_telegram",
                return_value=("reddit-002",),
            ) as send_mock,
        ):
            exit_code, stdout, stderr = self._run_cli("send-approvals")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("pending_review=1", stdout)
        self.assertIn("eligible=1", stdout)
        self.assertIn("telegram_sent=1", stdout)
        self.assertEqual(
            list_mock.call_args_list,
            [
                call(status=PENDING_REVIEW_STATUS),
                call(status=PENDING_REVIEW_STATUS, sent_to_telegram=False),
            ],
        )
        fetch_mock.assert_not_called()
        save_mock.assert_not_called()
        send_mock.assert_called_once()

    def test_send_approvals_skips_already_sent(self) -> None:
        already_sent = self._review(
            self._candidate(post_id="reddit-003"),
            status=PENDING_REVIEW_STATUS,
            sent_to_telegram=True,
        )
        eligible_review = self._review(
            self._candidate(post_id="reddit-004"),
            status=PENDING_REVIEW_STATUS,
            sent_to_telegram=False,
        )

        with (
            patch(
                "reddit_intake_cli.list_story_reviews",
                side_effect=((already_sent, eligible_review), (eligible_review,)),
            ),
            patch(
                "reddit_intake_cli.send_pending_story_reviews_to_telegram",
                return_value=("reddit-004",),
            ) as send_mock,
        ):
            exit_code, stdout, stderr = self._run_cli("send-approvals")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("pending_review=2", stdout)
        self.assertIn("eligible=1", stdout)
        self.assertIn("telegram_sent=1", stdout)
        send_mock.assert_called_once_with()

    def test_cli_output_shows_eligible_count(self) -> None:
        pending_review = self._review(
            self._candidate(post_id="reddit-005"),
            status=PENDING_REVIEW_STATUS,
            sent_to_telegram=False,
        )

        with (
            patch(
                "reddit_intake_cli.list_story_reviews",
                side_effect=((pending_review,), (pending_review,)),
            ),
            patch(
                "reddit_intake_cli.send_pending_story_reviews_to_telegram",
                return_value=("reddit-005",),
            ),
        ):
            exit_code, stdout, stderr = self._run_cli("send-approvals")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("eligible=1", stdout)

    def test_stats_command_prints_correct_summary(self) -> None:
        stats = StoryReviewStats(
            total=12,
            pending_review=7,
            approved=3,
            rejected=2,
            sent_to_telegram=5,
            eligible_to_send=4,
        )

        with patch("reddit_intake_cli.get_story_review_stats", return_value=stats):
            exit_code, stdout, stderr = self._run_cli("stats")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(
            stdout.strip(),
            "total=12 | pending_review=7 | approved=3 | rejected=2 | "
            "sent_to_telegram=5 | eligible_to_send=4",
        )

    def test_stats_does_not_call_fetch_or_send(self) -> None:
        stats = StoryReviewStats(
            total=1,
            pending_review=1,
            approved=0,
            rejected=0,
            sent_to_telegram=0,
            eligible_to_send=1,
        )

        with (
            patch("reddit_intake_cli.get_story_review_stats", return_value=stats) as stats_mock,
            patch("reddit_intake_cli.fetch_live_story_candidates") as fetch_mock,
            patch("reddit_intake_cli.send_pending_story_reviews_to_telegram") as send_mock,
        ):
            exit_code, stdout, stderr = self._run_cli("stats")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("total=1", stdout)
        stats_mock.assert_called_once_with()
        fetch_mock.assert_not_called()
        send_mock.assert_not_called()

    def test_stats_does_not_mutate_storage(self) -> None:
        stats = StoryReviewStats(
            total=2,
            pending_review=1,
            approved=1,
            rejected=0,
            sent_to_telegram=1,
            eligible_to_send=0,
        )

        with (
            patch("reddit_intake_cli.get_story_review_stats", return_value=stats),
            patch("reddit_intake_cli.save_story_candidates_for_review") as save_mock,
            patch("reddit_intake_cli.update_story_review_status") as update_mock,
            patch("reddit_intake_cli.send_pending_story_reviews_to_telegram") as send_mock,
        ):
            exit_code, stdout, stderr = self._run_cli("stats")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("sent_to_telegram=1", stdout)
        save_mock.assert_not_called()
        update_mock.assert_not_called()
        send_mock.assert_not_called()

    def test_process_telegram_update_command_works(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            update_path = Path(temp_dir_name) / "telegram_update.json"
            update_path.write_text(
                '{"message":{"message_id":9100,"chat":{"id":"telegram-chat-001"},"text":"Rewritten script","reply_to_message":{"message_id":9002}}}',
                encoding="utf-8",
            )
            updated_review = self._review(
                self._candidate(post_id="reddit-006"),
                status=PENDING_REVIEW_STATUS,
                sent_to_telegram=False,
                selected_for_processing=True,
                operator_rewritten_text="Rewritten script",
            )

            with patch(
                "reddit_intake_cli.process_telegram_update",
                return_value=updated_review,
            ) as process_update_mock:
                exit_code, stdout, stderr = self._run_cli(
                    "process-telegram-update",
                    "--update-file",
                    str(update_path),
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("story_id=reddit-006", stdout)
        self.assertIn("selected_for_processing=True", stdout)
        self.assertIn("rewritten_script_saved=True", stdout)
        process_update_mock.assert_called_once_with(
            {
                "message": {
                    "message_id": 9100,
                    "chat": {"id": "telegram-chat-001"},
                    "text": "Rewritten script",
                    "reply_to_message": {"message_id": 9002},
                }
            }
        )

    def test_translate_command_works(self) -> None:
        updated_review = self._review(
            self._candidate(post_id="reddit-007"),
            status=PENDING_REVIEW_STATUS,
            sent_to_telegram=False,
            selected_for_processing=True,
            operator_rewritten_text="Rewritten script",
            translated_script_uk="Перекладений текст",
        )

        with patch(
            "reddit_intake_cli.translate_story_review_to_ukrainian",
            return_value=updated_review,
        ) as translate_mock:
            exit_code, stdout, stderr = self._run_cli(
                "translate",
                "--story-id",
                "reddit-007",
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(stdout.strip(), "story_id=reddit-007 | translated=1")
        translate_mock.assert_called_once_with(story_id="reddit-007")

    def test_translate_command_surfaces_explicit_errors(self) -> None:
        with patch(
            "reddit_intake_cli.translate_story_review_to_ukrainian",
            side_effect=reddit_intake_cli.RedditIntakeError("Story must have rewritten text before translation: reddit-008"),
        ):
            exit_code, stdout, stderr = self._run_cli(
                "translate",
                "--story-id",
                "reddit-008",
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout, "")
        self.assertEqual(
            stderr.strip(),
            "Story must have rewritten text before translation: reddit-008",
        )

    def test_render_command_no_longer_accepts_adapted_script_file(self) -> None:
        with self.assertRaises(SystemExit) as context, redirect_stderr(io.StringIO()) as stderr:
            reddit_intake_cli.main(
                (
                    "render",
                    "--story-id",
                    "reddit-011",
                    "--adapted-script-file",
                    "/tmp/script.txt",
                    "--background-video-path",
                    "/tmp/background.mp4",
                )
            )

        self.assertEqual(context.exception.code, 2)
        self.assertIn("unrecognized arguments: --adapted-script-file", stderr.getvalue())

    def test_render_command_calls_render_story_review_without_external_text_file(self) -> None:
        result = type("RenderResult", (), {"output_path": Path("/tmp/reddit-012.mp4")})()

        with patch(
            "reddit_intake_cli.render_story_review",
            return_value=result,
        ) as render_mock:
            exit_code, stdout, stderr = self._run_cli(
                "render",
                "--story-id",
                "reddit-012",
                "--background-video-path",
                "/tmp/background.mp4",
                "--voice-id",
                "olesia-media",
                "--output-basename",
                "reddit_012_rendered",
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(stdout.strip(), "/tmp/reddit-012.mp4")
        render_mock.assert_called_once_with(
            story_id="reddit-012",
            background_video_path=Path("/tmp/background.mp4"),
            voice_id="olesia-media",
            output_basename="reddit_012_rendered",
        )

    def test_render_enqueue_calls_workflow_entrypoint(self) -> None:
        queue_item = type("QueueItem", (), {"asset_id": "reddit-015"})()
        queue_result = type("QueueResult", (), {"created": True, "item": queue_item})()
        render_result = type(
            "RenderResult",
            (),
            {"story_id": "reddit-015", "output_path": Path("/tmp/reddit-015.mp4")},
        )()

        with patch(
            "reddit_intake_cli.render_and_enqueue_story_review",
            return_value=(render_result, queue_result),
        ) as workflow_mock:
            exit_code, stdout, stderr = self._run_cli(
                "render-enqueue",
                "--story-id",
                "reddit-015",
                "--background-video-path",
                "/tmp/background.mp4",
                "--voice-id",
                "olesia-media",
                "--output-basename",
                "reddit_015_rendered",
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("story_id=reddit-015", stdout)
        self.assertIn("queued_created=yes", stdout)
        workflow_mock.assert_called_once_with(
            story_id="reddit-015",
            background_video_path=Path("/tmp/background.mp4"),
            voice_id="olesia-media",
            output_basename="reddit_015_rendered",
        )

    def test_render_enqueue_success_output_includes_queued_created(self) -> None:
        queue_item = type("QueueItem", (), {"asset_id": "reddit-016"})()
        queue_result = type("QueueResult", (), {"created": False, "item": queue_item})()
        render_result = type(
            "RenderResult",
            (),
            {"story_id": "reddit-016", "output_path": Path("/tmp/reddit-016.mp4")},
        )()

        with patch(
            "reddit_intake_cli.render_and_enqueue_story_review",
            return_value=(render_result, queue_result),
        ):
            exit_code, stdout, stderr = self._run_cli(
                "render-enqueue",
                "--story-id",
                "reddit-016",
                "--background-video-path",
                "/tmp/background.mp4",
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("queued_created=no", stdout)

    def test_render_enqueue_failure_is_surfaced_cleanly(self) -> None:
        with patch(
            "reddit_intake_cli.render_and_enqueue_story_review",
            side_effect=reddit_intake_cli.RedditIntakeError(
                "Story is not ready for rendering: reddit-017"
            ),
        ):
            exit_code, stdout, stderr = self._run_cli(
                "render-enqueue",
                "--story-id",
                "reddit-017",
                "--background-video-path",
                "/tmp/background.mp4",
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout, "")
        self.assertEqual(stderr.strip(), "Story is not ready for rendering: reddit-017")

    def test_list_approved_includes_translated_visibility_marker(self) -> None:
        translated_review = self._review(
            self._candidate(post_id="reddit-009"),
            status="approved",
            sent_to_telegram=True,
            selected_for_processing=True,
            operator_rewritten_text="Rewrite",
            translated_script_uk="Переклад",
        )
        untranslated_review = self._review(
            self._candidate(post_id="reddit-010"),
            status="approved",
            sent_to_telegram=True,
            selected_for_processing=True,
            operator_rewritten_text="Rewrite",
            translated_script_uk=None,
        )

        with patch(
            "reddit_intake_cli.list_story_reviews",
            return_value=(translated_review, untranslated_review),
        ):
            exit_code, stdout, stderr = self._run_cli("list-approved")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("story_id=reddit-009", stdout)
        self.assertIn("translated=yes", stdout)
        self.assertIn("story_id=reddit-010", stdout)
        self.assertIn("translated=no", stdout)

    def test_list_ready_prints_only_render_ready_stories(self) -> None:
        ready_review = self._review(
            self._candidate(post_id="reddit-013"),
            status="approved",
            sent_to_telegram=True,
            selected_for_processing=True,
            operator_rewritten_text="Rewrite",
            translated_script_uk="Переклад",
        )
        not_ready_review = self._review(
            self._candidate(post_id="reddit-014"),
            status="approved",
            sent_to_telegram=True,
            selected_for_processing=True,
            operator_rewritten_text="Rewrite",
            translated_script_uk=None,
        )

        with patch(
            "reddit_intake_cli.list_story_reviews",
            return_value=(ready_review, not_ready_review),
        ):
            exit_code, stdout, stderr = self._run_cli("list-ready")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("story_id=reddit-013", stdout)
        self.assertIn("ready=yes", stdout)
        self.assertNotIn("story_id=reddit-014", stdout)

    def test_list_ready_prints_empty_message_when_no_ready_stories(self) -> None:
        with patch(
            "reddit_intake_cli.list_story_reviews",
            return_value=(),
        ):
            exit_code, stdout, stderr = self._run_cli("list-ready")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(stdout.strip(), "No render-ready Reddit stories.")

    def test_run_pipeline_command_works(self) -> None:
        result = PipelineRunResult(
            fetched=3,
            accepted=2,
            persisted=2,
            sent_to_telegram=1,
            processed_updates=2,
            translated=2,
            rendered=1,
            enqueued=1,
        )

        with patch(
            "reddit_intake_cli.run_full_pipeline",
            return_value=result,
        ) as run_mock:
            exit_code, stdout, stderr = self._run_cli(
                "run-pipeline",
                "--background-video-path",
                "/tmp/background.mp4",
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(
            stdout.strip(),
            "fetched=3 | accepted=2 | persisted=2 | sent=1 | "
            "processed_updates=2 | translated=2 | rendered=1 | enqueued=1",
        )
        run_mock.assert_called_once_with(
            background_video_path=Path("/tmp/background.mp4"),
            telegram_update_payloads=(),
        )

    def test_run_pipeline_update_files_correctly_processed(self) -> None:
        result = PipelineRunResult(
            fetched=0,
            accepted=0,
            persisted=0,
            sent_to_telegram=0,
            processed_updates=2,
            translated=0,
            rendered=0,
            enqueued=0,
        )

        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            update_one = temp_dir / "update-one.json"
            update_two = temp_dir / "update-two.json"
            update_one.write_text('{"update_id": 1001}', encoding="utf-8")
            update_two.write_text('{"update_id": 1002}', encoding="utf-8")

            with patch(
                "reddit_intake_cli.run_full_pipeline",
                return_value=result,
            ) as run_mock:
                exit_code, stdout, stderr = self._run_cli(
                    "run-pipeline",
                    "--background-video-path",
                    "/tmp/background.mp4",
                    "--update-file",
                    str(update_one),
                    "--update-file",
                    str(update_two),
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("processed_updates=2", stdout)
        run_mock.assert_called_once_with(
            background_video_path=Path("/tmp/background.mp4"),
            telegram_update_payloads=(
                {"update_id": 1001},
                {"update_id": 1002},
            ),
        )

    def _candidate(self, post_id: str) -> StoryCandidate:
        return StoryCandidate(
            post_id=post_id,
            subreddit="TIFU",
            title="Reddit story title",
            body="A" * 300,
            score=config.REDDIT_MIN_SCORE + 100,
            url=f"https://www.reddit.com/r/TIFU/comments/{post_id}",
            created_utc=1710000000.0,
        )

    def _review(
        self,
        candidate: StoryCandidate,
        status: str,
        sent_to_telegram: bool,
        selected_for_processing: bool = False,
        telegram_chat_id: str | None = None,
        telegram_instruction_message_id: int | None = None,
        operator_rewritten_text: str | None = None,
        translated_script_uk: str | None = None,
    ) -> StoredStoryReview:
        return StoredStoryReview(
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
                script_saved_at="2026-03-22T12:05:00+00:00" if operator_rewritten_text else None,
            ),
            production=StoryProductionStage(
                translated_script_uk=translated_script_uk,
            ),
            status=status,
            sent_to_telegram=sent_to_telegram,
            telegram=TelegramReviewMetadata(
                chat_id=telegram_chat_id,
                instruction_message_id=telegram_instruction_message_id,
            ),
            saved_at="2026-03-22T12:00:00+00:00",
        )

    def _run_cli(self, *args: str) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch("reddit_intake_cli.config.validate_runtime_config"),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            exit_code = reddit_intake_cli.main(args)
        return exit_code, stdout.getvalue(), stderr.getvalue()


if __name__ == "__main__":
    unittest.main()
