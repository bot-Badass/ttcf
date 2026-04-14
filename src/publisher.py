from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, Sequence

from src import config
from src.processor import PROCESSED_STATUS, ProcessorResult


LOGGER = logging.getLogger(__name__)
PUBLISH_QUEUE_TABLE_NAME: Final[str] = "publish_queue"
QUEUED_STATUS: Final[str] = "queued"
PUBLISHED_STATUS: Final[str] = "published"
FAILED_STATUS: Final[str] = "failed"
UPDATEABLE_FIELDS: Final[frozenset[str]] = frozenset({"status", "error_message"})
__all__ = [
    "FAILED_STATUS",
    "PUBLISHED_STATUS",
    "QUEUED_STATUS",
    "PublishCandidate",
    "PublishPayload",
    "PublishQueueError",
    "PublishQueueItem",
    "PublishQueueResult",
    "PublishQueueStore",
    "QueueItemNotFoundError",
    "build_dry_run_payload",
    "build_publish_candidate",
]


class PublishQueueError(Exception):
    """Base exception for publish queue failures."""


class QueueItemNotFoundError(PublishQueueError):
    """Raised when a queue item cannot be found."""


@dataclass(frozen=True, slots=True)
class PublishCandidate:
    asset_id: str
    clip_path: Path
    thumbnail_path: Path
    title: str
    caption: str | None
    hashtags: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PublishQueueItem:
    asset_id: str
    clip_path: Path
    thumbnail_path: Path
    title: str
    caption: str
    hashtags: tuple[str, ...]
    status: str
    created_at: str
    error_message: str | None


@dataclass(frozen=True, slots=True)
class PublishQueueResult:
    item: PublishQueueItem
    created: bool


@dataclass(frozen=True, slots=True)
class PublishPayload:
    local_video_path: Path
    local_thumbnail_path: Path
    title: str
    caption: str
    hashtags: tuple[str, ...]


class PublishQueueStore:
    """SQLite-backed local queue for future publishing work."""

    def __init__(self, queue_path: Path = config.PUBLISH_QUEUE_PATH) -> None:
        self.queue_path = queue_path

    def initialize(self) -> None:
        self.queue_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with self._connect() as connection:
                connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PUBLISH_QUEUE_TABLE_NAME} (
                        asset_id TEXT NOT NULL,
                        clip_path TEXT NOT NULL,
                        thumbnail_path TEXT NOT NULL,
                        title TEXT NOT NULL,
                        caption TEXT NOT NULL,
                        hashtags TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        error_message TEXT,
                        UNIQUE(asset_id, clip_path)
                    )
                    """
                )
            LOGGER.info("Publish queue store initialized at %s", self.queue_path)
        except sqlite3.Error as exc:
            LOGGER.exception("Failed to initialize publish queue store at %s", self.queue_path)
            raise PublishQueueError(
                f"Failed to initialize publish queue store: {self.queue_path}"
            ) from exc

    def enqueue_candidate(self, candidate: PublishCandidate) -> PublishQueueResult:
        normalized_candidate = _normalize_candidate(candidate)

        try:
            with self._connect() as connection:
                connection.execute(
                    f"""
                    INSERT INTO {PUBLISH_QUEUE_TABLE_NAME} (
                        asset_id,
                        clip_path,
                        thumbnail_path,
                        title,
                        caption,
                        hashtags,
                        status,
                        created_at,
                        error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_candidate.asset_id,
                        str(normalized_candidate.clip_path),
                        str(normalized_candidate.thumbnail_path),
                        normalized_candidate.title,
                        normalized_candidate.caption,
                        _serialize_hashtags(normalized_candidate.hashtags),
                        QUEUED_STATUS,
                        _utc_timestamp_now(),
                        None,
                    ),
                )
        except sqlite3.IntegrityError:
            existing_item = self.get_item(
                asset_id=normalized_candidate.asset_id,
                clip_path=normalized_candidate.clip_path,
            )
            if existing_item is None:
                raise PublishQueueError(
                    "Duplicate publish queue enqueue detected but existing item could not be loaded."
                )

            LOGGER.info(
                "Publish queue item already exists for asset_id=%s clip_path=%s",
                normalized_candidate.asset_id,
                normalized_candidate.clip_path,
            )
            return PublishQueueResult(item=existing_item, created=False)
        except sqlite3.Error as exc:
            LOGGER.exception(
                "Failed to enqueue publish candidate asset_id=%s clip_path=%s",
                normalized_candidate.asset_id,
                normalized_candidate.clip_path,
            )
            raise PublishQueueError(
                f"Failed to enqueue publish candidate: {normalized_candidate.asset_id}"
            ) from exc

        created_item = self.get_item(
            asset_id=normalized_candidate.asset_id,
            clip_path=normalized_candidate.clip_path,
        )
        if created_item is None:
            raise PublishQueueError(
                "Publish queue item insert succeeded but created item could not be loaded."
            )

        LOGGER.info(
            "Enqueued publish item asset_id=%s clip_path=%s",
            normalized_candidate.asset_id,
            normalized_candidate.clip_path,
        )
        return PublishQueueResult(item=created_item, created=True)

    def list_queued_items(self) -> tuple[PublishQueueItem, ...]:
        return self._list_items_by_status(status=QUEUED_STATUS)

    def mark_item_as_published(self, asset_id: str, clip_path: Path) -> PublishQueueItem:
        self._update_item(
            asset_id=asset_id,
            clip_path=clip_path,
            status=PUBLISHED_STATUS,
            error_message=None,
        )
        item = self.get_item(asset_id=asset_id, clip_path=clip_path)
        if item is None:
            raise QueueItemNotFoundError(f"Queue item not found: {asset_id} {clip_path}")
        return item

    def mark_item_as_failed(
        self,
        asset_id: str,
        clip_path: Path,
        error_message: str,
    ) -> PublishQueueItem:
        if not error_message.strip():
            raise PublishQueueError("error_message is required when marking a queue item as failed.")

        self._update_item(
            asset_id=asset_id,
            clip_path=clip_path,
            status=FAILED_STATUS,
            error_message=error_message,
        )
        item = self.get_item(asset_id=asset_id, clip_path=clip_path)
        if item is None:
            raise QueueItemNotFoundError(f"Queue item not found: {asset_id} {clip_path}")
        return item

    def get_item(self, asset_id: str, clip_path: Path) -> PublishQueueItem | None:
        try:
            with self._connect() as connection:
                row = connection.execute(
                    f"""
                    SELECT * FROM {PUBLISH_QUEUE_TABLE_NAME}
                    WHERE asset_id = ? AND clip_path = ?
                    """,
                    (asset_id, str(clip_path)),
                ).fetchone()
        except sqlite3.Error as exc:
            LOGGER.exception(
                "Failed to load publish queue item asset_id=%s clip_path=%s",
                asset_id,
                clip_path,
            )
            raise PublishQueueError(
                f"Failed to load publish queue item: {asset_id} {clip_path}"
            ) from exc

        if row is None:
            return None

        return _row_to_publish_queue_item(row)

    def _list_items_by_status(self, status: str) -> tuple[PublishQueueItem, ...]:
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    f"""
                    SELECT * FROM {PUBLISH_QUEUE_TABLE_NAME}
                    WHERE status = ?
                    ORDER BY created_at ASC, asset_id ASC, clip_path ASC
                    """,
                    (status,),
                ).fetchall()
        except sqlite3.Error as exc:
            LOGGER.exception("Failed to list publish queue items for status=%s", status)
            raise PublishQueueError(f"Failed to list publish queue items for status: {status}") from exc

        return tuple(_row_to_publish_queue_item(row) for row in rows)

    def _update_item(
        self,
        asset_id: str,
        clip_path: Path,
        status: str,
        error_message: str | None,
    ) -> None:
        if "status" not in UPDATEABLE_FIELDS or "error_message" not in UPDATEABLE_FIELDS:
            raise PublishQueueError("Publish queue update fields are misconfigured.")

        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    f"""
                    UPDATE {PUBLISH_QUEUE_TABLE_NAME}
                    SET status = ?, error_message = ?
                    WHERE asset_id = ? AND clip_path = ?
                    """,
                    (status, error_message, asset_id, str(clip_path)),
                )
        except sqlite3.Error as exc:
            LOGGER.exception(
                "Failed to update publish queue item asset_id=%s clip_path=%s",
                asset_id,
                clip_path,
            )
            raise PublishQueueError(
                f"Failed to update publish queue item: {asset_id} {clip_path}"
            ) from exc

        if cursor.rowcount != 1:
            raise QueueItemNotFoundError(f"Queue item not found: {asset_id} {clip_path}")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.queue_path)
        connection.row_factory = sqlite3.Row
        return connection


def build_publish_candidate(
    processor_result: ProcessorResult,
    title: str,
    caption: str | None = None,
    hashtags: Sequence[str] = (),
) -> PublishCandidate:
    if processor_result.status != PROCESSED_STATUS:
        raise PublishQueueError(
            f"Cannot enqueue processor result with status {processor_result.status}."
        )

    if not processor_result.output_files:
        raise PublishQueueError("Processed asset must include at least one clip output.")

    if processor_result.thumbnail_path is None:
        raise PublishQueueError("Processed asset must include a thumbnail path.")

    normalized_title = title.strip()
    if not normalized_title:
        raise PublishQueueError("title is required for a publish candidate.")

    normalized_caption = caption.strip() if caption is not None else ""
    if not normalized_caption:
        normalized_caption = normalized_title

    return PublishCandidate(
        asset_id=processor_result.asset_id,
        clip_path=processor_result.output_files[0],
        thumbnail_path=processor_result.thumbnail_path,
        title=normalized_title,
        caption=normalized_caption,
        hashtags=tuple(hashtags),
    )


def build_dry_run_payload(queue_item: PublishQueueItem) -> PublishPayload:
    return PublishPayload(
        local_video_path=queue_item.clip_path,
        local_thumbnail_path=queue_item.thumbnail_path,
        title=queue_item.title,
        caption=queue_item.caption,
        hashtags=queue_item.hashtags,
    )


def _normalize_candidate(candidate: PublishCandidate) -> PublishCandidate:
    if not candidate.title.strip():
        raise PublishQueueError("title is required for a publish candidate.")

    if not candidate.clip_path.exists():
        raise PublishQueueError(f"clip_path does not exist: {candidate.clip_path}")

    if not candidate.thumbnail_path.exists():
        raise PublishQueueError(f"thumbnail_path does not exist: {candidate.thumbnail_path}")

    normalized_caption = candidate.caption.strip() if candidate.caption is not None else ""
    if not normalized_caption:
        normalized_caption = candidate.title

    normalized_hashtags = tuple(tag.strip() for tag in candidate.hashtags if tag.strip())

    return PublishCandidate(
        asset_id=candidate.asset_id,
        clip_path=candidate.clip_path,
        thumbnail_path=candidate.thumbnail_path,
        title=candidate.title.strip(),
        caption=normalized_caption,
        hashtags=normalized_hashtags,
    )


def _row_to_publish_queue_item(row: sqlite3.Row) -> PublishQueueItem:
    return PublishQueueItem(
        asset_id=row["asset_id"],
        clip_path=Path(row["clip_path"]),
        thumbnail_path=Path(row["thumbnail_path"]),
        title=row["title"],
        caption=row["caption"],
        hashtags=_deserialize_hashtags(row["hashtags"]),
        status=row["status"],
        created_at=row["created_at"],
        error_message=row["error_message"],
    )


def _serialize_hashtags(hashtags: Sequence[str]) -> str:
    return json.dumps(list(hashtags))


def _deserialize_hashtags(hashtags: str) -> tuple[str, ...]:
    return tuple(str(tag) for tag in json.loads(hashtags))


def _utc_timestamp_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
