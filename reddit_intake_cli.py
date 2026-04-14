from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from src import config
from src.reddit_intake import (
    APPROVED_STATUS,
    PENDING_REVIEW_STATUS,
    REJECTED_STATUS,
    RedditIntakeError,
    fetch_live_story_candidates,
    get_story_review_stats,
    is_story_ready_for_render,
    list_story_reviews,
    prepare_story_candidates,
    process_telegram_update,
    run_full_pipeline,
    render_and_enqueue_story_review,
    render_story_review,
    save_story_candidates_for_review,
    send_pending_story_reviews_to_telegram,
    translate_story_review_to_ukrainian,
    update_story_review_status,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        _validate_runtime_config_for_subcommand(args.subcommand)
        return args.command(args)
    except (RedditIntakeError, RuntimeError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operate the Reddit intake workflow.")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    fetch_parser = subparsers.add_parser(
        "fetch",
        help="Fetch live Reddit candidates, filter them, and persist accepted ones locally.",
    )
    fetch_parser.add_argument(
        "--limit-per-listing",
        type=int,
        default=config.REDDIT_FETCH_LIMIT_PER_LISTING,
    )
    fetch_parser.set_defaults(command=_fetch_command)

    send_approvals_parser = subparsers.add_parser(
        "send-approvals",
        help="Send locally stored pending-review Reddit stories to Telegram.",
    )
    send_approvals_parser.set_defaults(command=_send_approvals_command)

    review_parser = subparsers.add_parser(
        "review",
        help="Mark a stored Reddit story review as approved or rejected.",
    )
    review_parser.add_argument("--story-id", required=True)
    review_parser.add_argument(
        "--status",
        required=True,
        choices=(APPROVED_STATUS, REJECTED_STATUS, PENDING_REVIEW_STATUS),
    )
    review_parser.set_defaults(command=_review_command)

    process_update_parser = subparsers.add_parser(
        "process-telegram-update",
        help="Process one Telegram callback or reply update payload from a local JSON file.",
    )
    process_update_parser.add_argument("--update-file", type=Path, required=True)
    process_update_parser.set_defaults(command=_process_telegram_update_command)

    translate_parser = subparsers.add_parser(
        "translate",
        help="Translate one rewritten Reddit story into Ukrainian and store it.",
    )
    translate_parser.add_argument("--story-id", required=True)
    translate_parser.set_defaults(command=_translate_command)

    stats_parser = subparsers.add_parser(
        "stats",
        help="Show a read-only summary of the local Reddit review store.",
    )
    stats_parser.set_defaults(command=_stats_command)

    list_approved_parser = subparsers.add_parser(
        "list-approved",
        help="List locally approved Reddit stories.",
    )
    list_approved_parser.set_defaults(command=_list_approved_command)

    list_ready_parser = subparsers.add_parser(
        "list-ready",
        help="List locally approved Reddit stories that are ready for render.",
    )
    list_ready_parser.set_defaults(command=_list_ready_command)

    render_parser = subparsers.add_parser(
        "render",
        help="Render one approved Reddit story into data/inbox/.",
    )
    render_parser.add_argument("--story-id", required=True)
    render_parser.add_argument("--background-video-path", type=Path, required=True)
    render_parser.add_argument("--voice-id", default="")
    render_parser.add_argument("--output-basename", default=None)
    render_parser.set_defaults(command=_render_command)

    render_enqueue_parser = subparsers.add_parser(
        "render-enqueue",
        help="Render one ready Reddit story and enqueue it for publishing.",
    )
    render_enqueue_parser.add_argument("--story-id", required=True)
    render_enqueue_parser.add_argument("--background-video-path", type=Path, required=True)
    render_enqueue_parser.add_argument("--voice-id", default="")
    render_enqueue_parser.add_argument("--output-basename", default=None)
    render_enqueue_parser.set_defaults(command=_render_enqueue_command)

    run_pipeline_parser = subparsers.add_parser(
        "run-pipeline",
        help="Run the full Reddit intake pipeline in one command.",
    )
    run_pipeline_parser.add_argument("--background-video-path", type=Path, required=True)
    run_pipeline_parser.add_argument(
        "--update-file",
        type=Path,
        action="append",
        default=[],
    )
    run_pipeline_parser.set_defaults(command=_run_pipeline_command)

    return parser


def _validate_runtime_config_for_subcommand(subcommand: str) -> None:
    if subcommand == "send-approvals":
        config.validate_runtime_config("telegram")
        return

    if subcommand == "translate":
        config.validate_runtime_config("translation")
        return

    if subcommand in {"render", "render-enqueue"}:
        config.validate_runtime_config("tts")
        return

    if subcommand == "run-pipeline":
        config.validate_runtime_config("telegram", "translation", "tts")


def _fetch_command(args: argparse.Namespace) -> int:
    live_candidates = fetch_live_story_candidates(limit_per_listing=args.limit_per_listing)
    prepared_candidates = prepare_story_candidates(fetch_candidates=lambda: live_candidates)
    accepted_candidates = tuple(
        result.candidate
        for result in prepared_candidates.results
        if result.status == "accepted" and result.candidate is not None
    )

    persisted_reviews = save_story_candidates_for_review(accepted_candidates)

    print(
        " | ".join(
            (
                f"fetched={len(live_candidates)}",
                f"accepted={len(accepted_candidates)}",
                f"persisted={len(persisted_reviews)}",
            )
        )
    )
    return 0


def _send_approvals_command(args: argparse.Namespace) -> int:
    del args
    pending_reviews = list_story_reviews(status=PENDING_REVIEW_STATUS)
    eligible_reviews = list_story_reviews(
        status=PENDING_REVIEW_STATUS,
        sent_to_telegram=False,
    )
    sent_candidate_ids = send_pending_story_reviews_to_telegram()

    print(
        " | ".join(
            (
                f"pending_review={len(pending_reviews)}",
                f"eligible={len(eligible_reviews)}",
                f"telegram_sent={len(sent_candidate_ids)}",
            )
        )
    )
    return 0


def _review_command(args: argparse.Namespace) -> int:
    updated_review = update_story_review_status(story_id=args.story_id, status=args.status)
    print(f"story_id={updated_review.story_id} status={updated_review.status}")
    return 0


def _process_telegram_update_command(args: argparse.Namespace) -> int:
    update_payload = json.loads(args.update_file.read_text(encoding="utf-8"))
    updated_review = process_telegram_update(update_payload)
    print(
        " | ".join(
            (
                f"story_id={updated_review.story_id}",
                f"selected_for_processing={updated_review.selected_for_processing}",
                f"rewritten_script_saved={bool(updated_review.operator_rewritten_text)}",
            )
        )
    )
    return 0


def _stats_command(args: argparse.Namespace) -> int:
    del args
    stats = get_story_review_stats()
    print(
        " | ".join(
            (
                f"total={stats.total}",
                f"pending_review={stats.pending_review}",
                f"approved={stats.approved}",
                f"rejected={stats.rejected}",
                f"sent_to_telegram={stats.sent_to_telegram}",
                f"eligible_to_send={stats.eligible_to_send}",
            )
        )
    )
    return 0


def _list_approved_command(args: argparse.Namespace) -> int:
    del args
    approved_reviews = list_story_reviews(status=APPROVED_STATUS)
    if not approved_reviews:
        print("No approved Reddit stories.")
        return 0

    for review in approved_reviews:
        print(
            " | ".join(
                (
                    f"story_id={review.story_id}",
                    f"subreddit=r/{review.subreddit}",
                    f"title={review.source_title}",
                    f"url={review.url}",
                    f"status={review.status}",
                    f"translated={'yes' if review.translated_script_uk else 'no'}",
                )
            )
        )

    return 0


def _list_ready_command(args: argparse.Namespace) -> int:
    del args
    ready_reviews = tuple(
        review
        for review in list_story_reviews(status=APPROVED_STATUS)
        if is_story_ready_for_render(review)
    )
    if not ready_reviews:
        print("No render-ready Reddit stories.")
        return 0

    for review in ready_reviews:
        print(
            " | ".join(
                (
                    f"story_id={review.story_id}",
                    f"subreddit=r/{review.subreddit}",
                    f"title={review.source_title}",
                    f"status={review.status}",
                    "ready=yes",
                )
            )
        )

    return 0


def _render_command(args: argparse.Namespace) -> int:
    result = render_story_review(
        story_id=args.story_id,
        background_video_path=args.background_video_path,
        voice_id=args.voice_id,
        output_basename=args.output_basename,
    )
    print(result.output_path)
    return 0


def _render_enqueue_command(args: argparse.Namespace) -> int:
    render_result, queue_result = render_and_enqueue_story_review(
        story_id=args.story_id,
        background_video_path=args.background_video_path,
        voice_id=args.voice_id,
        output_basename=args.output_basename,
    )
    print(
        " | ".join(
            (
                f"story_id={render_result.story_id}",
                f"rendered={render_result.output_path}",
                f"queued_created={'yes' if queue_result.created else 'no'}",
            )
        )
    )
    return 0


def _translate_command(args: argparse.Namespace) -> int:
    updated_review = translate_story_review_to_ukrainian(story_id=args.story_id)
    print(f"story_id={updated_review.story_id} | translated=1")
    return 0


def _run_pipeline_command(args: argparse.Namespace) -> int:
    update_payloads = tuple(
        json.loads(update_file.read_text(encoding="utf-8"))
        for update_file in args.update_file
    )
    result = run_full_pipeline(
        background_video_path=args.background_video_path,
        telegram_update_payloads=update_payloads,
    )
    print(
        " | ".join(
            (
                f"fetched={result.fetched}",
                f"accepted={result.accepted}",
                f"persisted={result.persisted}",
                f"sent={result.sent_to_telegram}",
                f"processed_updates={result.processed_updates}",
                f"translated={result.translated}",
                f"rendered={result.rendered}",
                f"enqueued={result.enqueued}",
            )
        )
    )
    return 1 if result.stage_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
