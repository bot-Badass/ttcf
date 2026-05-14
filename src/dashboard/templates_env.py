from __future__ import annotations

from pathlib import Path
from fastapi.templating import Jinja2Templates
from src.dashboard.services.queue_service import get_queue_stats

_dir = str(Path(__file__).parent / "templates")
templates = Jinja2Templates(directory=_dir)


def _queue_badge() -> int | None:
    n = get_queue_stats()["queued"]
    return n if n > 0 else None


templates.env.globals["queue_badge"] = _queue_badge
