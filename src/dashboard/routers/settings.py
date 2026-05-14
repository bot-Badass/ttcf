from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from src.dashboard.templates_env import templates
from pathlib import Path

from src import config

router = APIRouter()

_SECRET_VARS = {
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "PEXELS_API_KEY",
    "DEEPL_API_KEY", "RESPEECHER_API_KEY",
}

_ENV_VARS = [
    ("TELEGRAM_BOT_TOKEN", config.TELEGRAM_BOT_TOKEN),
    ("TELEGRAM_CHAT_ID", config.TELEGRAM_CHAT_ID),
    ("PEXELS_API_KEY", config.PEXELS_API_KEY),
    ("DEEPL_API_KEY", config.DEEPL_API_KEY),
    ("RESPEECHER_API_KEY", config.RESPEECHER_API_KEY),
    ("ADVICE_TTS_VOICE", config.ADVICE_TTS_VOICE),
    ("WHISPERX_DEVICE", config.WHISPERX_DEVICE),
    ("WHISPERX_MODEL_SIZE", config.WHISPERX_MODEL_SIZE),
    ("VOICE_MODE", str(config.VOICE_MODE)),
]

_PATH_VARS = [
    ("INPUT_DIR", config.INPUT_DIR),
    ("APPROVED_DIR", config.APPROVED_DIR),
    ("PROCESSED_DIR", config.PROCESSED_DIR),
    ("REJECTED_DIR", config.REJECTED_DIR),
    ("LOG_DIR", config.LOG_DIR),
    ("ADVICE_OUTPUT_DIR", config.ADVICE_OUTPUT_DIR),
    ("MANIFEST_PATH", config.MANIFEST_PATH),
    ("PUBLISH_QUEUE_PATH", config.PUBLISH_QUEUE_PATH),
    ("VOICE_SESSION_STORE_PATH", config.VOICE_SESSION_STORE_PATH),
]


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    env_vars = [
        {
            "name": name,
            "set": bool(value and str(value).strip()),
            "value": None if name in _SECRET_VARS else value,
            "is_secret": name in _SECRET_VARS,
        }
        for name, value in _ENV_VARS
    ]
    path_vars = [
        {
            "name": name,
            "path": str(path),
            "exists": path.exists(),
        }
        for name, path in _PATH_VARS
    ]
    channels = [
        {
            "key": key,
            "label": profile["label"],
            "hook_bg": profile.get("hook_bg", ""),
            "hook_accent": profile.get("hook_accent", ""),
            "hook_brand": profile.get("hook_brand", ""),
            "plan_path": str(profile["plan_path"]),
            "plan_exists": Path(str(profile["plan_path"])).is_file(),
        }
        for key, profile in config.CHANNEL_PROFILES.items()
    ]
    return templates.TemplateResponse(request, "settings.html", {
        "page": "settings",
        "env_vars": env_vars,
        "path_vars": path_vars,
        "channels": channels,
    })
