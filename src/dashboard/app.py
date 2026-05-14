from __future__ import annotations

from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.dashboard.routers import overview, plan, sessions, queue, renders, settings
from src.dashboard.services.queue_service import get_queue_stats

app = FastAPI(title="TTCF Dashboard", docs_url=None, redoc_url=None)

_static_dir = Path(__file__).parent / "static"
_static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

# Shared Jinja2 env — inject queue_badge into every response
_templates_dir = Path(__file__).parent / "templates"
_shared_templates = Jinja2Templates(directory=str(_templates_dir))


@app.middleware("http")
async def inject_globals(request: Request, call_next):
    response = await call_next(request)
    return response


def _make_templates() -> Jinja2Templates:
    t = Jinja2Templates(directory=str(_templates_dir))

    def queue_badge_global():
        stats = get_queue_stats()
        return stats["queued"] if stats["queued"] > 0 else None

    t.env.globals["queue_badge"] = queue_badge_global()
    return t


# Expose a factory so routers can refresh badge each request
app.state.templates_dir = str(_templates_dir)

app.include_router(overview.router)
app.include_router(plan.router)
app.include_router(sessions.router)
app.include_router(queue.router)
app.include_router(renders.router)
app.include_router(settings.router)
