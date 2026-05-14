from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src import config
from src.content_plan import list_topics, ContentTopic, TOPIC_PENDING, TOPIC_RENDERED
from src.advice_pipeline import _load_voice_sessions


CHANNEL_PLAN_MAP: dict[str, Path] = {
    "law": config.CHANNEL_PROFILES["law"]["plan_path"],
    "finance": config.CHANNEL_PROFILES["finance"]["plan_path"],
}

CHANNEL_LABELS: dict[str, str] = {
    "law": "DontPanicLaw",
    "finance": "MoneyUA",
}


@dataclass
class EnrichedTopic:
    topic: ContentTopic
    channel_key: str
    channel_label: str
    has_active_session: bool = False
    has_render: bool = False


def _build_rendered_topic_ids() -> set[str]:
    """Return topic_ids that have at least one rendered video on disk."""
    sessions = _load_voice_sessions(config.VOICE_SESSION_STORE_PATH)
    rendered: set[str] = set()
    for s in sessions:
        for i in range(len(s.micro_series.parts)):
            part_dir = config.ADVICE_OUTPUT_DIR / f"{s.review_id}_part{i + 1}"
            if (part_dir / "video.mp4").is_file():
                rendered.add(s.topic_id)
                break
    return rendered


def get_all_topics(
    channel_filter: str = "all",
    status_filter: str = "all",
    series_filter: str = "all",
    active_session_ids: set[str] | None = None,
) -> list[EnrichedTopic]:
    active_ids = active_session_ids or set()
    rendered_ids = _build_rendered_topic_ids()
    result: list[EnrichedTopic] = []

    channels = (
        [(channel_filter, CHANNEL_PLAN_MAP[channel_filter])]
        if channel_filter != "all" and channel_filter in CHANNEL_PLAN_MAP
        else list(CHANNEL_PLAN_MAP.items())
    )

    for channel_key, plan_path in channels:
        if not plan_path.is_file():
            continue
        for topic in list_topics(plan_path):
            if status_filter != "all" and topic.status != status_filter:
                continue
            if series_filter != "all" and topic.series_id.upper() != series_filter.upper():
                continue
            result.append(EnrichedTopic(
                topic=topic,
                channel_key=channel_key,
                channel_label=CHANNEL_LABELS[channel_key],
                has_active_session=topic.topic_id in active_ids,
                has_render=topic.topic_id in rendered_ids,
            ))

    return result


def get_plan_stats() -> dict[str, int]:
    stats = {"pending": 0, "rendered": 0, "in_progress": 0, "total": 0}
    for channel_key, plan_path in CHANNEL_PLAN_MAP.items():
        if not plan_path.is_file():
            continue
        for topic in list_topics(plan_path):
            stats["total"] += 1
            if topic.status == TOPIC_PENDING:
                stats["pending"] += 1
            elif topic.status == TOPIC_RENDERED:
                stats["rendered"] += 1
            else:
                stats["in_progress"] += 1
    return stats


def get_series_ids(channel_filter: str = "all") -> list[str]:
    seen: list[str] = []
    channels = (
        [(channel_filter, CHANNEL_PLAN_MAP[channel_filter])]
        if channel_filter != "all" and channel_filter in CHANNEL_PLAN_MAP
        else list(CHANNEL_PLAN_MAP.items())
    )
    for _, plan_path in channels:
        if not plan_path.is_file():
            continue
        for topic in list_topics(plan_path):
            if topic.series_id not in seen:
                seen.append(topic.series_id)
    return seen
