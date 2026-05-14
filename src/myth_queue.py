from __future__ import annotations

import re
from pathlib import Path

from src import config


def list_unvoiced() -> list[tuple[str, str]]:
    """Return (slug, channel_key) for every myth dir that has script.txt but no voiceover.wav."""
    if not config.MYTH_DATA_DIR.is_dir():
        return []
    result: list[tuple[str, str]] = []
    for myth_dir in sorted(config.MYTH_DATA_DIR.iterdir()):
        if not myth_dir.is_dir():
            continue
        if not (myth_dir / "script.txt").is_file():
            continue
        if (myth_dir / "voiceover.wav").is_file():
            continue
        slug = myth_dir.name
        result.append((slug, slug_to_channel(slug)))
    return result


def slug_to_channel(slug: str) -> str:
    """Detect channel key from slug prefix. finance_* → finance, else → law."""
    return "finance" if slug.startswith("finance_") else "law"


def slug_to_title(slug: str) -> str:
    """Extract first text line from script.txt as human-readable title (max 55 chars).
    Falls back to slug if script not found."""
    script_path = config.MYTH_DATA_DIR / slug / "script.txt"
    if not script_path.is_file():
        return slug
    for line in script_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if (
            stripped
            and not stripped.startswith("##")
            and not stripped.startswith("PARTS:")
            and not stripped.startswith("**")
        ):
            clean = re.sub(r"\*\*(.+?)\*\*", r"\1", stripped)
            return clean[:55] + ("..." if len(clean) > 55 else "")
    return slug
