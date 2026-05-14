from __future__ import annotations

import json
import logging
import random
import subprocess
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Mapping

from src import config

_BLACKLIST_PATH = config.DATA_DIR / "used_backgrounds.json"


def _load_blacklist() -> dict[str, list[int]]:
    if _BLACKLIST_PATH.is_file():
        try:
            return json.loads(_BLACKLIST_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_blacklist(blacklist: dict[str, list[int]]) -> None:
    _BLACKLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    _BLACKLIST_PATH.write_text(json.dumps(blacklist, ensure_ascii=False, indent=2), encoding="utf-8")

LOGGER = logging.getLogger(__name__)


class PexelsError(Exception):
    """Raised when a Pexels API request fails."""


HttpGetBoundary = Callable[[str, Mapping[str, str]], bytes]
HttpDownloadBoundary = Callable[[str], bytes]


def download_background_video(
    output_path: Path,
    query: str = config.ADVICE_PEXELS_QUERY,
    min_duration: int = config.ADVICE_PEXELS_VIDEO_MIN_DURATION,
    api_key: str | None = None,
    http_get: HttpGetBoundary | None = None,
    http_download: HttpDownloadBoundary | None = None,
    extra_exclude_ids: set[int] | None = None,
) -> int | None:
    # Local fallback: if ADVICE_LOCAL_BACKGROUND_VIDEO is set, skip Pexels entirely.
    local_bg = config.ADVICE_LOCAL_BACKGROUND_VIDEO
    if local_bg:
        local_path = Path(local_bg)
        if not local_path.is_file():
            raise PexelsError(
                f"ADVICE_LOCAL_BACKGROUND_VIDEO file not found: {local_path}"
            )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        import shutil
        shutil.copy2(local_path, output_path)
        LOGGER.info("Background video copied from local fallback: %s", local_path)
        return None

    resolved_key = api_key or config.PEXELS_API_KEY
    if not resolved_key:
        raise PexelsError(
            "PEXELS_API_KEY is not configured. "
            "Get a free key at pexels.com/api and set PEXELS_API_KEY."
        )

    encoded_query = urllib.parse.quote(query)
    # Randomise the page to get varied results across calls.
    page = random.randint(1, 3)
    url = f"https://api.pexels.com/videos/search?query={encoded_query}&per_page=40&orientation=portrait&page={page}"
    headers: Mapping[str, str] = {"Authorization": resolved_key}

    try:
        response_bytes = (http_get or _default_http_get)(url, headers)
    except PexelsError:
        raise
    except OSError as exc:
        raise PexelsError(f"Pexels API request failed: {exc}") from exc

    try:
        response_data = json.loads(response_bytes)
    except (json.JSONDecodeError, ValueError) as exc:
        raise PexelsError(f"Pexels returned invalid JSON: {exc}") from exc

    videos = response_data.get("videos", [])
    if not isinstance(videos, list):
        raise PexelsError("Pexels response missing 'videos' list.")

    blacklist = _load_blacklist()
    used_ids: set[int] = set(blacklist.get(query, []))
    if extra_exclude_ids:
        used_ids |= extra_exclude_ids

    video_link, video_id = _pick_hd_portrait_link(videos, min_duration, used_ids)
    if video_link is None:
        raise PexelsError(
            f"No suitable portrait HD video found on Pexels "
            f"for query={query!r} min_duration={min_duration}s."
        )

    try:
        video_bytes = (http_download or _default_http_download)(video_link)
    except PexelsError:
        raise
    except OSError as exc:
        raise PexelsError(f"Failed to download Pexels video: {exc}") from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write raw download to a temp file, then transcode to target format.
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        tmp_path.write_bytes(video_bytes)
        if not tmp_path.is_file() or tmp_path.stat().st_size == 0:
            raise PexelsError(f"Downloaded Pexels video is empty: {video_link}")
        _transcode_background_video(tmp_path, output_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    # Record used video_id in blacklist.
    if video_id is not None:
        if video_id in used_ids:
            # Fallback to full pool occurred — all videos were used; reset and start fresh.
            blacklist[query] = [video_id]
        else:
            entry = blacklist.get(query, [])
            entry.append(video_id)
            blacklist[query] = entry
        _save_blacklist(blacklist)
        LOGGER.info(
            "Pexels video downloaded and transcoded: query=%s video_id=%d size=%dMB output=%s",
            query, video_id,
            output_path.stat().st_size // (1024 * 1024),
            output_path,
        )
    else:
        LOGGER.info(
            "Pexels video downloaded and transcoded: query=%s size=%dMB output=%s",
            query,
            output_path.stat().st_size // (1024 * 1024),
            output_path,
        )
    return video_id


def _transcode_background_video(input_path: Path, output_path: Path) -> None:
    """Transcode Pexels video to portrait 1080x1920, CRF 28, muted.

    Target: ~10-25 MB for a 15-30 sec portrait clip.
    Audio is stripped — the pipeline overlays its own TTS audio track.
    """
    command = [
        config.FFMPEG_BIN,
        "-y",
        "-i", str(input_path),
        "-vf", "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920",
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "28",
        "-an",                  # strip audio — TTS added later by render.py
        "-movflags", "+faststart",
        "-pix_fmt", "yuv420p",
        str(output_path),
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=config.FFMPEG_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise PexelsError("ffmpeg not found — cannot transcode background video.") from exc
    except subprocess.TimeoutExpired as exc:
        raise PexelsError("ffmpeg timed out while transcoding background video.") from exc

    if result.returncode != 0:
        stderr = result.stderr.strip() or "ffmpeg returned non-zero exit code."
        raise PexelsError(f"Background video transcode failed: {stderr}")

    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise PexelsError(f"Transcoded background video is empty: {output_path}")


def _pick_hd_portrait_link(
    videos: list[Any],
    min_duration: int,
    exclude_ids: set[int] | None = None,
) -> tuple[str, int] | tuple[None, None]:
    """Pick a random HD portrait video link, skipping already-used IDs.

    Returns (link, video_id) or (None, None) if nothing eligible.
    If all results are blacklisted, falls back to the full pool.
    """
    exclude = exclude_ids or set()

    def _collect(vids: list[Any], skip_ids: set[int]) -> list[tuple[str, int]]:
        eligible: list[tuple[str, int]] = []
        for video in vids:
            if not isinstance(video, dict):
                continue
            video_id = video.get("id")
            if not isinstance(video_id, int):
                continue
            if video_id in skip_ids:
                continue
            duration = video.get("duration", 0)
            if not isinstance(duration, (int, float)) or duration < min_duration:
                continue
            for vf in video.get("video_files", []):
                if not isinstance(vf, dict):
                    continue
                if vf.get("quality") != "hd":
                    continue
                link = vf.get("link")
                if not link:
                    continue
                w = vf.get("width") or 0
                h = vf.get("height") or 0
                if h >= w:
                    eligible.append((str(link), video_id))
                    break
        return eligible

    candidates = _collect(videos, exclude)
    if not candidates:
        # All results are blacklisted — fall back to full pool and reset later.
        candidates = _collect(videos, set())
    if not candidates:
        return None, None
    return random.choice(candidates)


_DEFAULT_HEADERS: Mapping[str, str] = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}


def _default_http_get(url: str, headers: Mapping[str, str]) -> bytes:
    merged = {**_DEFAULT_HEADERS, **headers}
    request = urllib.request.Request(url, headers=merged, method="GET")
    with urllib.request.urlopen(request) as response:
        return response.read()


def _default_http_download(url: str) -> bytes:
    request = urllib.request.Request(url, headers=dict(_DEFAULT_HEADERS), method="GET")
    with urllib.request.urlopen(request) as response:
        return response.read()
