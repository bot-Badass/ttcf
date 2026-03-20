from __future__ import annotations

import hashlib
from pathlib import Path

from src import config


def compute_sha256(file_path: Path) -> str:
    """Return the content hash used as the canonical asset identity."""
    digest = hashlib.sha256()
    with file_path.open("rb") as file_handle:
        while chunk := file_handle.read(config.FILE_HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()
