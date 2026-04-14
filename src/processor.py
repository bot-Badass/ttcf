from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Final


PROCESSED_STATUS: Final[str] = "processed"
FAILED_STATUS: Final[str] = "failed"


@dataclass(frozen=True, slots=True)
class ProcessorResult:
    asset_id: str
    source_path: Path
    status: str
    output_files: tuple[Path, ...]
    thumbnail_path: Path | None
    error_message: str | None
