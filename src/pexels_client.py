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
) -> Path:
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
        return output_path

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

    video_link = _pick_hd_portrait_link(videos, min_duration)
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

    LOGGER.info(
        "Pexels video downloaded and transcoded: query=%s size=%dMB output=%s",
        query,
        output_path.stat().st_size // (1024 * 1024),
        output_path,
    )
    return output_path


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


def _pick_hd_portrait_link(videos: list[Any], min_duration: int) -> str | None:
    """Pick a random suitable HD portrait video link from results.

    Collects all eligible links first, then picks one at random to ensure
    variety across consecutive calls with the same query.
    """
    eligible_links: list[str] = []
    for video in videos:
        if not isinstance(video, dict):
            continue
        duration = video.get("duration", 0)
        if not isinstance(duration, (int, float)) or duration < min_duration:
            continue
        video_files = video.get("video_files", [])
        if not isinstance(video_files, list):
            continue
        for vf in video_files:
            if not isinstance(vf, dict):
                continue
            if vf.get("quality") != "hd":
                continue
            link = vf.get("link")
            if not link:
                continue
            width = vf.get("width") or 0
            height = vf.get("height") or 0
            if height >= width:
                eligible_links.append(str(link))
                break  # one link per video is enough

    if not eligible_links:
        return None
    return random.choice(eligible_links)


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
