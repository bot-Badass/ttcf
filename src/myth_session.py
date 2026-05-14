from __future__ import annotations

import json
from pathlib import Path

from src import config

MYTH_SESSION_PATH: Path = config.DATA_DIR / "manifests" / "myth_pending.json"


def set_pending(chat_id: str, slug: str) -> None:
    data = _load()
    data[chat_id] = slug
    _save(data)


def get_pending(chat_id: str) -> str | None:
    return _load().get(chat_id)


def clear_pending(chat_id: str) -> None:
    data = _load()
    data.pop(chat_id, None)
    _save(data)


def _load() -> dict[str, str]:
    if not MYTH_SESSION_PATH.is_file():
        return {}
    try:
        return json.loads(MYTH_SESSION_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save(data: dict[str, str]) -> None:
    MYTH_SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
    MYTH_SESSION_PATH.write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )
