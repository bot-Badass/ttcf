from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import patch

import src.myth_session as myth_session


class TestMythSession(unittest.TestCase):
    def setUp(self) -> None:
        self._patcher = patch.object(
            myth_session,
            "MYTH_SESSION_PATH",
            Path(self.id().replace(".", "_") + ".json"),
        )
        self._patcher.start()
        # Ensure clean state
        if myth_session.MYTH_SESSION_PATH.is_file():
            myth_session.MYTH_SESSION_PATH.unlink()

    def tearDown(self) -> None:
        if myth_session.MYTH_SESSION_PATH.is_file():
            myth_session.MYTH_SESSION_PATH.unlink()
        self._patcher.stop()

    def test_set_and_get_pending(self) -> None:
        myth_session.set_pending("chat1", "mfo-court")
        self.assertEqual(myth_session.get_pending("chat1"), "mfo-court")

    def test_clear_pending(self) -> None:
        myth_session.set_pending("chat1", "mfo-court")
        myth_session.clear_pending("chat1")
        self.assertIsNone(myth_session.get_pending("chat1"))

    def test_multiple_chats(self) -> None:
        myth_session.set_pending("chat1", "slug-a")
        myth_session.set_pending("chat2", "slug-b")
        self.assertEqual(myth_session.get_pending("chat1"), "slug-a")
        self.assertEqual(myth_session.get_pending("chat2"), "slug-b")
        myth_session.clear_pending("chat1")
        self.assertIsNone(myth_session.get_pending("chat1"))
        self.assertEqual(myth_session.get_pending("chat2"), "slug-b")

    def test_get_pending_missing(self) -> None:
        self.assertIsNone(myth_session.get_pending("nonexistent"))

    def test_clear_missing_is_noop(self) -> None:
        myth_session.clear_pending("nonexistent")
        self.assertIsNone(myth_session.get_pending("nonexistent"))

    def test_overwrite_pending(self) -> None:
        myth_session.set_pending("chat1", "old-slug")
        myth_session.set_pending("chat1", "new-slug")
        self.assertEqual(myth_session.get_pending("chat1"), "new-slug")

    def test_persists_as_json(self) -> None:
        myth_session.set_pending("chat1", "mfo-court")
        raw = json.loads(myth_session.MYTH_SESSION_PATH.read_text(encoding="utf-8"))
        self.assertEqual(raw, {"chat1": "mfo-court"})


if __name__ == "__main__":
    unittest.main()
