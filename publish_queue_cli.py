from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from src import config
from src.publisher import (
    PublishQueueError,
    PublishQueueStore,
    QueueItemNotFoundError,
    build_dry_run_payload,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    _validate_runtime_config_for_subcommand(args.subcommand)
    queue_store = PublishQueueStore(args.queue_path)
    queue_store.initialize()

    try:
        return args.command(queue_store, args)
    except (PublishQueueError, QueueItemNotFoundError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the local publish queue.")
    parser.add_argument(
        "--queue-path",
        type=Path,
        default=config.PUBLISH_QUEUE_PATH,
        help="Path to the local publish queue SQLite database.",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    list_parser = subparsers.add_parser("list", help="List queued publish items.")
    list_parser.set_defaults(command=_list_command)

    publish_parser = subparsers.add_parser("publish", help="Mark a queue item as published.")
    publish_parser.add_argument("--asset-id", required=True)
    publish_parser.add_argument("--clip-path", type=Path, required=True)
    publish_parser.set_defaults(command=_publish_command)

    fail_parser = subparsers.add_parser("fail", help="Mark a queue item as failed.")
    fail_parser.add_argument("--asset-id", required=True)
    fail_parser.add_argument("--clip-path", type=Path, required=True)
    fail_parser.add_argument("--error-message", required=True)
    fail_parser.set_defaults(command=_fail_command)

    payload_parser = subparsers.add_parser(
        "payload",
        help="Show the dry-run payload for a queued publish item.",
    )
    payload_parser.add_argument("--asset-id", required=True)
    payload_parser.add_argument("--clip-path", type=Path, required=True)
    payload_parser.set_defaults(command=_payload_command)

    return parser


def _validate_runtime_config_for_subcommand(subcommand: str) -> None:
    del subcommand
    # Publish queue operations are local-only and intentionally require no
    # Telegram, TTS, or translation runtime config groups.


def _list_command(queue_store: PublishQueueStore, args: argparse.Namespace) -> int:
    del args
    queued_items = queue_store.list_queued_items()
    if not queued_items:
        print("No queued items.")
        return 0

    for item in queued_items:
        print(
            " | ".join(
                (
                    f"asset_id={item.asset_id}",
                    f"clip_path={item.clip_path}",
                    f"title={item.title}",
                    f"status={item.status}",
                    f"created_at={item.created_at}",
                )
            )
        )
    return 0


def _publish_command(queue_store: PublishQueueStore, args: argparse.Namespace) -> int:
    item = queue_store.mark_item_as_published(
        asset_id=args.asset_id,
        clip_path=args.clip_path,
    )
    print(
        f"Marked published: asset_id={item.asset_id} clip_path={item.clip_path} status={item.status}"
    )
    return 0


def _fail_command(queue_store: PublishQueueStore, args: argparse.Namespace) -> int:
    item = queue_store.mark_item_as_failed(
        asset_id=args.asset_id,
        clip_path=args.clip_path,
        error_message=args.error_message,
    )
    print(
        f"Marked failed: asset_id={item.asset_id} clip_path={item.clip_path} status={item.status}"
    )
    return 0


def _payload_command(queue_store: PublishQueueStore, args: argparse.Namespace) -> int:
    item = queue_store.get_item(asset_id=args.asset_id, clip_path=args.clip_path)
    if item is None:
        raise QueueItemNotFoundError(f"Queue item not found: {args.asset_id} {args.clip_path}")

    payload = build_dry_run_payload(item)
    print(f"local_video_path={payload.local_video_path}")
    print(f"local_thumbnail_path={payload.local_thumbnail_path}")
    print(f"title={payload.title}")
    print(f"caption={payload.caption}")
    print(f"hashtags={', '.join(payload.hashtags) if payload.hashtags else '(none)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
