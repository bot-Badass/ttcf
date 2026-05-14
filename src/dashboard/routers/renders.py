from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, Response
from src.dashboard.templates_env import templates
from pathlib import Path

from src.dashboard.services.render_service import get_all_renders, enqueue_render
from src.dashboard.services.queue_service import get_queue_asset_ids

router = APIRouter()


@router.get("/renders", response_class=HTMLResponse)
async def renders_page(request: Request, q: str = ""):
    renders = get_all_renders(limit=50, search=q)
    queued_ids = get_queue_asset_ids()
    return templates.TemplateResponse(request, "renders.html", {
        "page": "renders",
        "renders": renders,
        "queued_ids": queued_ids,
        "search_q": q,
    })


@router.post("/api/renders/{asset_id}/enqueue")
async def enqueue_render_item(asset_id: str):
    ok, msg = enqueue_render(asset_id)
    return JSONResponse({"ok": ok, "message": msg})


@router.get("/media/video/{path:path}")
async def serve_video(path: str):
    video_path = Path("/") / path
    if video_path.is_file():
        return FileResponse(str(video_path), media_type="video/mp4")
    return Response(status_code=404)


@router.get("/media/thumb/{path:path}")
async def serve_thumb(path: str):
    thumb_path = Path("/") / path
    if thumb_path.is_file():
        return FileResponse(str(thumb_path), media_type="image/jpeg")
    return Response(status_code=404)
