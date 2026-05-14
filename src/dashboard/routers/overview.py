from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from src.dashboard.templates_env import templates
from pathlib import Path

from src.dashboard.services.plan_service import get_plan_stats
from src.dashboard.services.session_service import get_all_sessions, get_session_stats
from src.dashboard.services.queue_service import get_queue_stats, get_all_items
from src.dashboard.services.render_service import get_all_renders, get_render_stats

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def overview(request: Request):
    active_sessions = [s for s in get_all_sessions() if not s.is_complete]
    recent_queue = get_all_items("queued")[:5]
    recent_renders = get_all_renders(limit=5)
    return templates.TemplateResponse(request, "overview.html", {
        "page": "overview",
        "plan_stats": get_plan_stats(),
        "session_stats": get_session_stats(),
        "queue_stats": get_queue_stats(),
        "render_stats": get_render_stats(),
        "active_sessions": active_sessions,
        "recent_queue": recent_queue,
        "recent_renders": recent_renders,
    })
