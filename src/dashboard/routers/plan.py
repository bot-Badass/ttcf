from __future__ import annotations

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from src.dashboard.templates_env import templates
from pathlib import Path

from src.dashboard.services.plan_service import get_all_topics, get_series_ids, CHANNEL_PLAN_MAP
from src.dashboard.services.session_service import get_active_topic_ids
from src import config

router = APIRouter()


@router.get("/plan", response_class=HTMLResponse)
async def plan_page(
    request: Request,
    channel: str = Query("all"),
    status: str = Query("all"),
    series: str = Query("all"),
):
    active_ids = get_active_topic_ids()
    topics = get_all_topics(
        channel_filter=channel,
        status_filter=status,
        series_filter=series,
        active_session_ids=active_ids,
    )
    series_ids = get_series_ids(channel)
    return templates.TemplateResponse(request, "plan.html", {
        "page": "plan",
        "topics": topics,
        "series_ids": series_ids,
        "filter_channel": channel,
        "filter_status": status,
        "filter_series": series,
        "total": len(topics),
    })


class CreateSessionBody(BaseModel):
    channel_key: str
    script_text: str


@router.post("/api/plan/{topic_id}/create-session")
async def create_session_from_plan(topic_id: str, body: CreateSessionBody):
    from src.advice_pipeline import (
        issue_specific_topic,
        receive_operator_scripts,
        AdvicePipelineError,
    )
    from src.content_plan import get_topic_by_id

    channel_key = body.channel_key
    plan_path = CHANNEL_PLAN_MAP.get(channel_key)
    if plan_path is None or not plan_path.is_file():
        return JSONResponse({"ok": False, "message": f"Канал '{channel_key}' не знайдено"})

    channel_profile = config.CHANNEL_PROFILES.get(channel_key, {})

    def _run() -> dict:
        try:
            review, topic = issue_specific_topic(
                topic_id,
                plan_path=plan_path,
            )
        except AdvicePipelineError as exc:
            return {"ok": False, "message": str(exc)}

        try:
            session = receive_operator_scripts(
                review_id=review.script.script_id,
                raw_response=body.script_text,
                topic=topic,
                voice_mode=True,
                hook_bg_override=channel_profile.get("hook_bg"),
                hook_accent_override=channel_profile.get("hook_accent"),
                hook_brand_override=channel_profile.get("hook_brand") or None,
                cta_overlay_path=channel_profile.get("cta_overlay_path"),
                cta_overlay_width=int(channel_profile.get("cta_overlay_width", 380)),
                cta_overlay_y=int(channel_profile.get("cta_overlay_y", 100)),
            )
        except AdvicePipelineError as exc:
            return {"ok": False, "message": str(exc)}

        bg_path = str(session.background_video_path)
        video_url = "/media/video/" + bg_path.lstrip("/")
        return {
            "ok": True,
            "session_id": session.session_id,
            "message": "Сесію створено",
            "bg_video_url": video_url,
        }

    result = await run_in_threadpool(_run)
    return JSONResponse(result)


@router.get("/api/plan/topics", response_class=HTMLResponse)
async def plan_topics_partial(
    request: Request,
    channel: str = Query("all"),
    status: str = Query("all"),
    series: str = Query("all"),
):
    active_ids = get_active_topic_ids()
    topics = get_all_topics(
        channel_filter=channel,
        status_filter=status,
        series_filter=series,
        active_session_ids=active_ids,
    )
    return templates.TemplateResponse(request, "partials/topic_rows.html", {
        "topics": topics,
        "total": len(topics),
    })
