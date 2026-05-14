from __future__ import annotations

import asyncio
import json
from fastapi import APIRouter, Request, Query, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
from src.dashboard.templates_env import templates
from pathlib import Path, PurePosixPath

from src.dashboard.services.session_service import (
    get_all_sessions,
    start_render,
    get_render_snapshot,
    send_part_prompt,
    save_uploaded_audio,
    refresh_session_background,
    generate_hook_preview,
    mark_session_published,
)

router = APIRouter()


@router.get("/sessions", response_class=HTMLResponse)
async def sessions_page(
    request: Request,
    status: str = Query("rendered"),
):
    sessions = get_all_sessions(status_filter=status)
    return templates.TemplateResponse(request, "sessions.html", {
        "page": "sessions",
        "sessions": sessions,
        "filter_status": status,
    })


@router.post("/api/sessions/{session_id}/mark-published")
async def mark_published_endpoint(session_id: str):
    ok, msg = mark_session_published(session_id)
    return JSONResponse({"ok": ok, "message": msg})


@router.post("/api/sessions/{review_id}/render")
async def trigger_render(review_id: str):
    started = start_render(review_id)
    return JSONResponse({"status": "started" if started else "already_running"})


@router.get("/api/sessions/{review_id}/log/stream")
async def render_log_sse(review_id: str):
    """SSE stream: sends full log snapshot every 1s until finished."""
    async def event_generator():
        sent_len = 0
        while True:
            log, finished = get_render_snapshot(review_id)
            new_text = log[sent_len:]
            if new_text:
                # Escape newlines for SSE data field
                escaped = new_text.replace("\n", "\\n")
                yield f"data: {json.dumps({'text': escaped, 'done': finished})}\n\n"
                sent_len = len(log)
            elif finished:
                yield f"data: {json.dumps({'text': '', 'done': True})}\n\n"
                break
            await asyncio.sleep(0.8)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/api/sessions/{session_id}/refresh-background")
async def refresh_background(session_id: str):
    from fastapi.concurrency import run_in_threadpool
    ok, msg, bg_path = await run_in_threadpool(refresh_session_background, session_id)
    video_url = None
    if ok and bg_path:
        video_url = "/media/video/" + bg_path.lstrip("/")
    return JSONResponse({"ok": ok, "message": msg, "video_url": video_url})


@router.get("/api/sessions/{session_id}/parts/{part_number}/hook-preview")
async def hook_preview(session_id: str, part_number: int):
    from fastapi.concurrency import run_in_threadpool
    png = await run_in_threadpool(generate_hook_preview, session_id, part_number)
    if png is None:
        return Response(status_code=500)
    return Response(content=png, media_type="image/png")


@router.post("/api/sessions/{session_id}/parts/{part_number}/send-prompt")
async def send_prompt_to_telegram(session_id: str, part_number: int):
    ok, msg = send_part_prompt(session_id, part_number)
    return JSONResponse({"ok": ok, "message": msg})


@router.post("/api/sessions/{session_id}/parts/{part_number}/upload-audio")
async def upload_part_audio(
    session_id: str,
    part_number: int,
    file: UploadFile = File(...),
):
    ext = PurePosixPath(file.filename or "voice.ogg").suffix or ".ogg"
    audio_bytes = await file.read()
    ok, msg = save_uploaded_audio(session_id, part_number, audio_bytes, ext)
    return JSONResponse({"ok": ok, "message": msg})
