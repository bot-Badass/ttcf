from __future__ import annotations

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from src.dashboard.templates_env import templates
from pathlib import Path

from src.dashboard.services.queue_service import (
    get_all_items,
    mark_published,
    delete_item,
    get_queue_stats,
    mark_all_published,
)

router = APIRouter()


@router.get("/queue", response_class=HTMLResponse)
async def queue_page(
    request: Request,
    status: str = Query("queued"),
):
    items = get_all_items(status_filter=status)
    stats = get_queue_stats()
    return templates.TemplateResponse(request, "queue.html", {
        "page": "queue",
        "items": items,
        "stats": stats,
        "filter_status": status,
    })


@router.post("/api/queue/{asset_id}/published", response_class=HTMLResponse)
async def mark_item_published(request: Request, asset_id: str):
    success = mark_published(asset_id)
    return templates.TemplateResponse(request, "partials/toast.html", {
        "success": success,
        "message": "Позначено як опубліковано" if success else "Помилка оновлення",
    })


@router.post("/api/queue/{asset_id}/delete", response_class=HTMLResponse)
async def delete_queue_item(request: Request, asset_id: str):
    success = delete_item(asset_id)
    return templates.TemplateResponse(request, "partials/toast.html", {
        "success": success,
        "message": "Видалено з черги" if success else "Помилка видалення",
    })


@router.post("/api/queue/bulk/publish-all")
async def bulk_publish_all():
    count = mark_all_published()
    return JSONResponse({"ok": True, "count": count})
