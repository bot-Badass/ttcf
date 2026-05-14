from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src import config
from src.publisher import PublishCandidate, PublishQueueStore


@dataclass
class RenderEntry:
    asset_id: str
    video_path: Path
    thumb_path: Path | None
    size_mb: float
    created_ts: float
    topic_id: str
    part_number: int | None
    channel_key: str


def _guess_channel_from_path(path: Path) -> str:
    parts = path.parts
    for part in parts:
        if "moneyua" in part.lower() or "finance" in part.lower():
            return "finance"
    return "law"


def _parse_part_number(stem: str) -> int | None:
    if "_part" in stem:
        try:
            return int(stem.split("_part")[-1].split("_")[0])
        except ValueError:
            pass
    return None


def _parse_topic_id(directory: Path) -> str:
    name = directory.name
    if "_part" in name:
        name = name.split("_part")[0]
    return name[-8:] if len(name) > 8 else name


def get_all_renders(limit: int = 50, search: str = "") -> list[RenderEntry]:
    output_dir = config.ADVICE_OUTPUT_DIR
    if not output_dir.is_dir():
        return []

    entries: list[RenderEntry] = []
    for video_path in output_dir.rglob("video.mp4"):
        try:
            stat = video_path.stat()
        except OSError:
            continue

        bundle_dir = video_path.parent
        thumb = bundle_dir / "thumb.jpg"
        if not thumb.is_file():
            thumb_candidates = list(bundle_dir.glob("*.jpg"))
            thumb = thumb_candidates[0] if thumb_candidates else None

        entries.append(RenderEntry(
            asset_id=bundle_dir.name,
            video_path=video_path,
            thumb_path=thumb if (thumb and thumb.is_file()) else None,
            size_mb=round(stat.st_size / 1024 / 1024, 1),
            created_ts=stat.st_mtime,
            topic_id=_parse_topic_id(bundle_dir),
            part_number=_parse_part_number(bundle_dir.name),
            channel_key=_guess_channel_from_path(bundle_dir),
        ))

    entries.sort(key=lambda e: e.created_ts, reverse=True)
    if search:
        q = search.lower()
        entries = [e for e in entries if q in e.topic_id.lower() or q in e.asset_id.lower()]
    return entries[:limit]


_render_index: dict[str, RenderEntry] = {}


def _build_index() -> None:
    for r in get_all_renders(limit=1000):
        _render_index[r.asset_id] = r


def enqueue_render(asset_id: str) -> tuple[bool, str]:
    if not _render_index:
        _build_index()
    entry = _render_index.get(asset_id)
    if entry is None:
        # Try fresh scan
        for r in get_all_renders(limit=1000):
            _render_index[r.asset_id] = r
        entry = _render_index.get(asset_id)
    if entry is None:
        return False, "Рендер не знайдено"
    if not entry.video_path.is_file():
        return False, "Файл відео не існує"

    thumb = entry.thumb_path or entry.video_path.parent / "thumb.jpg"
    candidate = PublishCandidate(
        asset_id=asset_id,
        clip_path=entry.video_path,
        thumbnail_path=thumb,
        title=f"{entry.topic_id} ч.{entry.part_number}" if entry.part_number else entry.topic_id,
        caption=None,
        hashtags=(),
    )
    try:
        store = PublishQueueStore()
        store.initialize()
        store.enqueue_candidate(candidate)
        return True, "Додано в чергу"
    except Exception as exc:
        return False, f"Помилка: {exc}"


def get_render_stats() -> dict[str, int | float]:
    renders = get_all_renders(limit=1000)
    total_size = sum(r.size_mb for r in renders)
    return {
        "total": len(renders),
        "total_size_mb": round(total_size, 1),
    }
