from __future__ import annotations

import dataclasses
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from src import config

LOGGER = logging.getLogger(__name__)

TOPIC_PENDING: Final[str] = "pending"
TOPIC_SCRIPT_RECEIVED: Final[str] = "script_received"
TOPIC_RENDERED: Final[str] = "rendered"


class ContentPlanError(Exception):
    """Raised when content plan operations fail."""


@dataclasses.dataclass(frozen=True, slots=True)
class ContentTopic:
    topic_id: str
    series_id: str
    series_title: str
    part_number: int
    total_parts: int
    title: str
    hook_formula: str
    audience: str
    scenario: str
    legal_facts: tuple[str, ...]
    status: str
    script_id: str | None
    script_saved_at: str | None
    parts_count: int | None = None
    pexels_query: str = ""


def get_next_pending_topic(
    plan_path: Path = config.CONTENT_PLAN_PATH,
) -> ContentTopic | None:
    for topic in list_topics(plan_path):
        if topic.status == TOPIC_PENDING:
            return topic
    return None


def mark_topic_status(
    topic_id: str,
    status: str,
    script_id: str | None = None,
    plan_path: Path = config.CONTENT_PLAN_PATH,
    parts_count: int | None = None,
) -> ContentTopic:
    plan_data = _load_plan_raw(plan_path)
    found_topic: dict | None = None
    found_series: dict | None = None
    for series in plan_data["series"]:
        for topic_dict in series["topics"]:
            if topic_dict["topic_id"] == topic_id:
                topic_dict["status"] = status
                if script_id is not None:
                    topic_dict["script_id"] = script_id
                    topic_dict["script_saved_at"] = _utc_now()
                if parts_count is not None:
                    topic_dict["parts_count"] = parts_count
                found_topic = topic_dict
                found_series = series
                break
        if found_topic is not None:
            break
    if found_topic is None:
        raise ContentPlanError(f"Topic not found: {topic_id}")
    _write_plan_raw(plan_path, plan_data)
    return _deserialize_topic(found_topic, found_series)


def get_topic_by_id(
    topic_id: str,
    plan_path: Path = config.CONTENT_PLAN_PATH,
) -> ContentTopic | None:
    plan_data = _load_plan_raw(plan_path)
    for series in plan_data["series"]:
        for topic_dict in series["topics"]:
            if topic_dict["topic_id"] == topic_id:
                return _deserialize_topic(topic_dict, series)
    return None


def list_topics(
    plan_path: Path = config.CONTENT_PLAN_PATH,
) -> tuple[ContentTopic, ...]:
    plan_data = _load_plan_raw(plan_path)
    result: list[ContentTopic] = []
    for series in plan_data["series"]:
        for topic_dict in series["topics"]:
            result.append(_deserialize_topic(topic_dict, series))
    return tuple(result)


def advance_topic_index(
    to_index: int,
    plan_path: Path = config.CONTENT_PLAN_PATH,
) -> None:
    plan_data = _load_plan_raw(plan_path)
    plan_data["current_topic_index"] = to_index
    _write_plan_raw(plan_path, plan_data)


def get_current_topic_index(
    plan_path: Path = config.CONTENT_PLAN_PATH,
) -> int:
    plan_data = _load_plan_raw(plan_path)
    return int(plan_data.get("current_topic_index", 0))


def _load_plan_raw(plan_path: Path) -> dict:
    if not plan_path.is_file():
        raise ContentPlanError(
            f"Content plan not found at {plan_path}. "
            "Run: python advice_content_cli.py init"
        )
    try:
        return json.loads(plan_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ContentPlanError(f"Failed to load content plan: {exc}") from exc


def _write_plan_raw(plan_path: Path, plan_data: dict) -> None:
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(
        json.dumps(plan_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _deserialize_topic(topic_dict: dict, series: dict) -> ContentTopic:
    raw_facts = topic_dict.get("legal_facts", [])
    legal_facts = tuple(str(f) for f in raw_facts) if isinstance(raw_facts, list) else ()
    # part_number and total_parts are optional — derive from position if missing.
    topics_list = series.get("topics", [])
    auto_part = next(
        (i + 1 for i, t in enumerate(topics_list) if t.get("topic_id") == topic_dict.get("topic_id")),
        1,
    )
    total = series.get("total_parts", len(topics_list)) or len(topics_list)
    return ContentTopic(
        topic_id=topic_dict["topic_id"],
        series_id=series["series_id"],
        series_title=series["title"],
        part_number=int(topic_dict.get("part_number", auto_part)),
        total_parts=int(total),
        title=topic_dict["title"],
        hook_formula=topic_dict.get("hook_formula", ""),
        audience=topic_dict.get("audience", ""),
        scenario=topic_dict.get("scenario", ""),
        legal_facts=legal_facts,
        status=topic_dict.get("status", TOPIC_PENDING),
        script_id=topic_dict.get("script_id"),
        script_saved_at=topic_dict.get("script_saved_at"),
        parts_count=topic_dict.get("parts_count"),
        pexels_query=series.get("pexels_query", ""),
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
