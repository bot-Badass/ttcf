from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.content_plan import (
    ContentPlanError,
    ContentTopic,
    TOPIC_PENDING,
    TOPIC_SCRIPT_RECEIVED,
    get_next_pending_topic,
    get_topic_by_id,
    list_topics,
    mark_topic_status,
)


def _write_plan(plan_path: Path, topics: list[dict], current_index: int = 0) -> None:
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(
        json.dumps(
            {
                "series": [
                    {
                        "series_id": "A",
                        "title": "Test Series",
                        "total_parts": len(topics),
                        "topics": topics,
                    }
                ],
                "current_topic_index": current_index,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _make_topic_dict(
    topic_id: str,
    part_number: int,
    title: str = "Test Topic",
    status: str = TOPIC_PENDING,
) -> dict:
    return {
        "topic_id": topic_id,
        "part_number": part_number,
        "title": title,
        "hook_formula": "Test hook",
        "audience": "Test audience",
        "scenario": "Test scenario",
        "legal_facts": ["Fact 1", "Fact 2"],
        "status": status,
        "script_id": None,
        "script_saved_at": None,
    }


class ContentPlanTests(unittest.TestCase):
    def test_get_next_pending_returns_first_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_plan(
                plan_path,
                [
                    _make_topic_dict("A1", 1, "Topic One", status=TOPIC_SCRIPT_RECEIVED),
                    _make_topic_dict("A2", 2, "Topic Two", status=TOPIC_PENDING),
                    _make_topic_dict("A3", 3, "Topic Three", status=TOPIC_PENDING),
                ],
            )

            topic = get_next_pending_topic(plan_path)

        self.assertIsNotNone(topic)
        self.assertEqual(topic.topic_id, "A2")
        self.assertEqual(topic.title, "Topic Two")

    def test_get_next_pending_returns_none_when_all_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_plan(
                plan_path,
                [
                    _make_topic_dict("A1", 1, status=TOPIC_SCRIPT_RECEIVED),
                    _make_topic_dict("A2", 2, status="rendered"),
                ],
            )

            topic = get_next_pending_topic(plan_path)

        self.assertIsNone(topic)

    def test_mark_topic_status_updates_and_persists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_plan(
                plan_path,
                [_make_topic_dict("A1", 1, status=TOPIC_PENDING)],
            )

            updated = mark_topic_status(
                "A1",
                TOPIC_SCRIPT_RECEIVED,
                script_id="abc123",
                plan_path=plan_path,
            )

            reloaded = get_topic_by_id("A1", plan_path)

        self.assertEqual(updated.status, TOPIC_SCRIPT_RECEIVED)
        self.assertEqual(updated.script_id, "abc123")
        self.assertIsNotNone(reloaded)
        self.assertEqual(reloaded.status, TOPIC_SCRIPT_RECEIVED)
        self.assertEqual(reloaded.script_id, "abc123")

    def test_load_plan_returns_all_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_plan(
                plan_path,
                [
                    _make_topic_dict("A1", 1, "Alpha"),
                    _make_topic_dict("A2", 2, "Beta"),
                    _make_topic_dict("A3", 3, "Gamma"),
                ],
            )

            topics = list_topics(plan_path)

        self.assertEqual(len(topics), 3)
        self.assertEqual(topics[0].title, "Alpha")
        self.assertEqual(topics[1].title, "Beta")
        self.assertEqual(topics[2].title, "Gamma")
        # Verify dataclass fields
        self.assertIsInstance(topics[0], ContentTopic)
        self.assertEqual(topics[0].series_id, "A")
        self.assertEqual(topics[0].total_parts, 3)
        self.assertEqual(topics[0].legal_facts, ("Fact 1", "Fact 2"))


if __name__ == "__main__":
    unittest.main()
