from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Final


@dataclasses.dataclass(frozen=True, slots=True)
class ContentMeta:
    title: str
    description: str
    hashtags: tuple[str, ...]


_DEFAULT_HASHTAGS: Final[tuple[str, ...]] = (
    "reddit",
    "redditstories",
    "storytime",
    "tiktok",
    "україна",
)

_SUBREDDIT_HASHTAGS: Final[dict[str, tuple[str, ...]]] = {
    "AmItheAsshole": ("AITA", "стосунки", "конфлікт"),
    "BestofRedditorUpdates": ("оновлення", "драма", "redditdrama"),
    "offmychest": ("сповідь", "зізнання", "думки"),
    "relationships": ("стосунки", "кохання", "поради"),
    "TIFU": ("помилка", "випадок", "сьогодні"),
}

_DESCRIPTION_MAX_CHARS: Final[int] = 280
_TITLE_MAX_CHARS: Final[int] = 100


def build_content_meta(
    source_title: str,
    description_source: str,
    subreddit: str,
) -> ContentMeta:
    title = source_title.strip()[:_TITLE_MAX_CHARS]
    if not title:
        raise ValueError("source_title must not be empty.")

    raw_description = description_source.strip()
    if not raw_description:
        raise ValueError("description_source must not be empty.")
    description = raw_description[:_DESCRIPTION_MAX_CHARS]

    subreddit_tags = _SUBREDDIT_HASHTAGS.get(subreddit, ())
    hashtags = subreddit_tags + _DEFAULT_HASHTAGS

    return ContentMeta(
        title=title,
        description=description,
        hashtags=hashtags,
    )


def ensure_content_meta_file(
    bundle_dir: Path,
    meta: ContentMeta,
    overwrite: bool = True,
) -> Path:
    meta_path = bundle_dir / "meta.json"
    if meta_path.exists() and not overwrite:
        return meta_path
    payload = {
        "title": meta.title,
        "description": meta.description,
        "hashtags": list(meta.hashtags),
    }
    meta_path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    return meta_path
