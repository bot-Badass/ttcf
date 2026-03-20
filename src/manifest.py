from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Sequence

from src import config


LOGGER = logging.getLogger(__name__)
ASSETS_TABLE_NAME: Final[str] = "assets"
UPDATEABLE_FIELDS: Final[frozenset[str]] = frozenset(
    {"status", "output_files", "error_message"}
)


class ManifestError(Exception):
    """Base exception for manifest storage failures."""


class DuplicateAssetError(ManifestError):
    """Base exception for duplicate asset insert failures."""


class DuplicateAssetIdError(DuplicateAssetError):
    """Raised when an asset with the same asset_id already exists."""


class DuplicateSha256Error(DuplicateAssetError):
    """Raised when an asset with the same sha256 already exists."""


class AssetNotFoundError(ManifestError):
    """Raised when an asset update targets a missing record."""


@dataclass(frozen=True, slots=True)
class AssetRecord:
    asset_id: str
    input_path: Path
    source_type: str
    source_path_or_url: str
    rights_status: str
    ingest_time: str
    sha256: str
    duration: float | None
    status: str
    output_files: tuple[str, ...]
    error_message: str | None


class ManifestStore:
    """SQLite-backed manifest storage for pipeline assets."""

    def __init__(self, manifest_path: Path = config.MANIFEST_PATH) -> None:
        self.manifest_path = manifest_path

    def initialize(self) -> None:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with self._connect() as connection:
                connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {ASSETS_TABLE_NAME} (
                        asset_id TEXT PRIMARY KEY,
                        input_path TEXT NOT NULL,
                        source_type TEXT NOT NULL,
                        source_path_or_url TEXT NOT NULL,
                        rights_status TEXT NOT NULL,
                        ingest_time TEXT NOT NULL,
                        sha256 TEXT NOT NULL UNIQUE,
                        duration REAL,
                        status TEXT NOT NULL,
                        output_files TEXT NOT NULL,
                        error_message TEXT
                    )
                    """
                )
            LOGGER.info("Manifest store initialized at %s", self.manifest_path)
        except sqlite3.Error as exc:
            LOGGER.exception("Failed to initialize manifest store at %s", self.manifest_path)
            raise ManifestError(f"Failed to initialize manifest store: {self.manifest_path}") from exc

    def insert_asset(self, record: AssetRecord) -> None:
        try:
            with self._connect() as connection:
                connection.execute(
                    f"""
                    INSERT INTO {ASSETS_TABLE_NAME} (
                        asset_id,
                        input_path,
                        source_type,
                        source_path_or_url,
                        rights_status,
                        ingest_time,
                        sha256,
                        duration,
                        status,
                        output_files,
                        error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.asset_id,
                        str(record.input_path),
                        record.source_type,
                        record.source_path_or_url,
                        record.rights_status,
                        record.ingest_time,
                        record.sha256,
                        record.duration,
                        record.status,
                        self._serialize_output_files(record.output_files),
                        record.error_message,
                    ),
                )
            LOGGER.info("Inserted asset record asset_id=%s sha256=%s", record.asset_id, record.sha256)
        except sqlite3.IntegrityError as exc:
            error_message = str(exc)

            if f"{ASSETS_TABLE_NAME}.asset_id" in error_message:
                LOGGER.warning("Duplicate asset_id detected: asset_id=%s", record.asset_id)
                raise DuplicateAssetIdError(
                    f"Asset with asset_id {record.asset_id} already exists."
                ) from exc

            if f"{ASSETS_TABLE_NAME}.sha256" in error_message:
                LOGGER.warning("Duplicate sha256 detected: sha256=%s", record.sha256)
                raise DuplicateSha256Error(
                    f"Asset with sha256 {record.sha256} already exists."
                ) from exc

            LOGGER.exception("Integrity error inserting asset_id=%s", record.asset_id)
            raise ManifestError(f"Failed to insert asset record: {record.asset_id}") from exc
        except sqlite3.Error as exc:
            LOGGER.exception("SQLite error inserting asset_id=%s", record.asset_id)
            raise ManifestError(f"Failed to insert asset record: {record.asset_id}") from exc

    def is_duplicate_sha256(self, sha256: str) -> bool:
        return self.get_asset_by_sha256(sha256) is not None

    def get_asset(self, asset_id: str) -> AssetRecord | None:
        try:
            with self._connect() as connection:
                row = connection.execute(
                    f"SELECT * FROM {ASSETS_TABLE_NAME} WHERE asset_id = ?",
                    (asset_id,),
                ).fetchone()
        except sqlite3.Error as exc:
            LOGGER.exception("Failed to load asset_id=%s", asset_id)
            raise ManifestError(f"Failed to load asset record: {asset_id}") from exc

        if row is None:
            return None

        return self._row_to_asset_record(row)

    def get_asset_by_sha256(self, sha256: str) -> AssetRecord | None:
        try:
            with self._connect() as connection:
                row = connection.execute(
                    f"SELECT * FROM {ASSETS_TABLE_NAME} WHERE sha256 = ?",
                    (sha256,),
                ).fetchone()
        except sqlite3.Error as exc:
            LOGGER.exception("Failed to load asset by sha256=%s", sha256)
            raise ManifestError(f"Failed to load asset record by sha256: {sha256}") from exc

        if row is None:
            return None

        return self._row_to_asset_record(row)

    def update_status(self, asset_id: str, status: str) -> None:
        self._update_field(asset_id=asset_id, field_name="status", value=status)

    def update_output_files(self, asset_id: str, output_files: Sequence[str | Path]) -> None:
        serialized_output_files = self._serialize_output_files(output_files)
        self._update_field(
            asset_id=asset_id,
            field_name="output_files",
            value=serialized_output_files,
        )

    def update_error_message(self, asset_id: str, error_message: str | None) -> None:
        self._update_field(
            asset_id=asset_id,
            field_name="error_message",
            value=error_message,
        )

    def _update_field(self, asset_id: str, field_name: str, value: str | None) -> None:
        if field_name not in UPDATEABLE_FIELDS:
            LOGGER.error("Attempted update of non-whitelisted field: %s", field_name)
            raise ManifestError(f"Field is not updateable: {field_name}")

        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    f"UPDATE {ASSETS_TABLE_NAME} SET {field_name} = ? WHERE asset_id = ?",
                    (value, asset_id),
                )
        except sqlite3.Error as exc:
            LOGGER.exception("Failed to update %s for asset_id=%s", field_name, asset_id)
            raise ManifestError(
                f"Failed to update {field_name} for asset record: {asset_id}"
            ) from exc

        if cursor.rowcount != 1:
            LOGGER.error("Asset not found during %s update: asset_id=%s", field_name, asset_id)
            raise AssetNotFoundError(f"Asset not found: {asset_id}")

        LOGGER.info("Updated %s for asset_id=%s", field_name, asset_id)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.manifest_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _row_to_asset_record(self, row: sqlite3.Row) -> AssetRecord:
        return AssetRecord(
            asset_id=row["asset_id"],
            input_path=Path(row["input_path"]),
            source_type=row["source_type"],
            source_path_or_url=row["source_path_or_url"],
            rights_status=row["rights_status"],
            ingest_time=row["ingest_time"],
            sha256=row["sha256"],
            duration=row["duration"],
            status=row["status"],
            output_files=self._deserialize_output_files(row["output_files"]),
            error_message=row["error_message"],
        )

    def _serialize_output_files(self, output_files: Sequence[str | Path]) -> str:
        return json.dumps([str(path) for path in output_files])

    def _deserialize_output_files(self, output_files: str) -> tuple[str, ...]:
        loaded_output_files = json.loads(output_files)
        return tuple(str(path) for path in loaded_output_files)
