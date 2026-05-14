from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from src import config
from src.publisher import (
    PublishQueueStore,
    PublishQueueItem,
    QUEUED_STATUS,
    PUBLISHED_STATUS,
    FAILED_STATUS,
    PUBLISH_QUEUE_TABLE_NAME,
)


@dataclass
class EnrichedQueueItem:
    item: PublishQueueItem
    thumb_exists: bool
    video_exists: bool
    video_size_mb: float | None
    channel_key: str


def _guess_channel(item: PublishQueueItem) -> str:
    path_str = str(item.clip_path).lower()
    if "moneyua" in item.title.lower() or "finance" in path_str:
        return "finance"
    plan_path = str(config.CHANNEL_PROFILES["finance"]["plan_path"])
    return "law"


def get_all_items(status_filter: str = "queued") -> list[EnrichedQueueItem]:
    if not config.PUBLISH_QUEUE_PATH.is_file():
        return []
    result: list[EnrichedQueueItem] = []
    try:
        with sqlite3.connect(config.PUBLISH_QUEUE_PATH) as conn:
            conn.row_factory = sqlite3.Row
            if status_filter == "all":
                rows = conn.execute(
                    f"SELECT * FROM {PUBLISH_QUEUE_TABLE_NAME} ORDER BY created_at DESC"
                ).fetchall()
            else:
                rows = conn.execute(
                    f"SELECT * FROM {PUBLISH_QUEUE_TABLE_NAME} WHERE status=? ORDER BY created_at DESC",
                    (status_filter,),
                ).fetchall()
    except sqlite3.Error:
        return []

    for row in rows:
        clip = Path(row["clip_path"])
        thumb = Path(row["thumbnail_path"])
        size_mb: float | None = None
        if clip.is_file():
            size_mb = round(clip.stat().st_size / 1024 / 1024, 1)

        row_dict = dict(row)
        item = PublishQueueItem(
            asset_id=row_dict["asset_id"],
            clip_path=clip,
            thumbnail_path=thumb,
            title=row_dict["title"],
            caption=row_dict["caption"],
            hashtags=tuple(row_dict["hashtags"].split(",")) if row_dict["hashtags"] else (),
            status=row_dict["status"],
            created_at=row_dict["created_at"],
            error_message=row_dict.get("error_message"),
        )
        result.append(EnrichedQueueItem(
            item=item,
            thumb_exists=thumb.is_file(),
            video_exists=clip.is_file(),
            video_size_mb=size_mb,
            channel_key=_guess_channel(item),
        ))
    return result


def mark_published(asset_id: str) -> bool:
    if not config.PUBLISH_QUEUE_PATH.is_file():
        return False
    try:
        with sqlite3.connect(config.PUBLISH_QUEUE_PATH) as conn:
            conn.execute(
                f"UPDATE {PUBLISH_QUEUE_TABLE_NAME} SET status=? WHERE asset_id=?",
                (PUBLISHED_STATUS, asset_id),
            )
        return True
    except sqlite3.Error:
        return False


def delete_item(asset_id: str) -> bool:
    if not config.PUBLISH_QUEUE_PATH.is_file():
        return False
    try:
        with sqlite3.connect(config.PUBLISH_QUEUE_PATH) as conn:
            conn.execute(
                f"DELETE FROM {PUBLISH_QUEUE_TABLE_NAME} WHERE asset_id=?",
                (asset_id,),
            )
        return True
    except sqlite3.Error:
        return False


def get_queue_asset_ids() -> set[str]:
    """Return asset_ids of all queued (not yet published) items."""
    if not config.PUBLISH_QUEUE_PATH.is_file():
        return set()
    try:
        with sqlite3.connect(config.PUBLISH_QUEUE_PATH) as conn:
            rows = conn.execute(
                f"SELECT asset_id FROM {PUBLISH_QUEUE_TABLE_NAME} WHERE status=?",
                (QUEUED_STATUS,),
            ).fetchall()
        return {row[0] for row in rows}
    except sqlite3.Error:
        return set()


def mark_all_published() -> int:
    """Mark all queued items as published. Returns count of updated rows."""
    if not config.PUBLISH_QUEUE_PATH.is_file():
        return 0
    try:
        with sqlite3.connect(config.PUBLISH_QUEUE_PATH) as conn:
            cur = conn.execute(
                f"UPDATE {PUBLISH_QUEUE_TABLE_NAME} SET status=? WHERE status=?",
                (PUBLISHED_STATUS, QUEUED_STATUS),
            )
        return cur.rowcount
    except sqlite3.Error:
        return 0


def get_queue_stats() -> dict[str, int]:
    if not config.PUBLISH_QUEUE_PATH.is_file():
        return {"queued": 0, "published": 0, "failed": 0}
    try:
        with sqlite3.connect(config.PUBLISH_QUEUE_PATH) as conn:
            rows = conn.execute(
                f"SELECT status, COUNT(*) as cnt FROM {PUBLISH_QUEUE_TABLE_NAME} GROUP BY status"
            ).fetchall()
        result = {"queued": 0, "published": 0, "failed": 0}
        for status, cnt in rows:
            if status in result:
                result[status] = cnt
        return result
    except sqlite3.Error:
        return {"queued": 0, "published": 0, "failed": 0}
