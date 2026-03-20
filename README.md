# ttcf

Local-first short-video processing pipeline for approved media inputs.

## Purpose

This project ingests approved source media, validates required metadata and media readability, processes valid assets into vertical short-video outputs, and persists processing state in a SQLite manifest.

Current pipeline stages:
- `ingest.py`: collect local inputs and optional approved-source manifest entries
- `validator.py`: validate provided assets and move invalid files to `data/rejected/`
- `processor.py`: create vertical processed clips and thumbnails with `ffmpeg`
- `manifest.py`: persist asset state in SQLite
- `orchestrator.py`: wire the stages together
- `run.py`: real CLI entrypoint

## Directory structure

```text
src/
  ingest.py
  validator.py
  processor.py
  manifest.py
  orchestrator.py
  config.py
  utils.py

data/
  inbox/
  approved/
  processed/
  rejected/
  manifests/
  logs/

tests/
run.py
requirements.txt
```

Runtime data layout:
- `data/inbox/`: local source files to ingest
- `data/approved/`: copied approved files that passed ingest
- `data/rejected/`: files rejected during validation
- `data/processed/<asset_id>/`: processed clip and thumbnail per asset
- `data/manifests/manifest.sqlite3`: SQLite manifest store
- `data/logs/startup.log`: pipeline log file

## Prerequisites

- Python 3.11+
- `ffmpeg`
- `ffprobe`

Check the media tools:

```bash
ffmpeg -version
ffprobe -version
```

## Setup

1. Create and activate a Python environment if you want one.
2. Install dependencies if needed.

`requirements.txt` is intentionally minimal because the current pipeline uses the standard library only.

## How to run tests

Run the full suite:

```bash
python -m unittest discover -s tests -v
```

You can also run a single module, for example:

```bash
python -m unittest tests.test_orchestrator -v
```

## How to run the pipeline

Put supported source files into `data/inbox/`, then run:

```bash
python run.py
```

Example supported extensions are currently:
- `.mp4`
- `.mov`
- `.m4v`
- `.mkv`

`run.py` will:
- ensure required directories exist
- configure file logging to `data/logs/startup.log`
- initialize the manifest store
- run ingest -> validate -> process
- print a final pipeline summary

Exit codes:
- `0`: no processing failures
- `1`: one or more processing failures, or an unexpected runtime error

## Input expectations

### Local inbox

Files discovered directly in `data/inbox/`:
- are treated as `source_type="local_file"`
- are assigned `rights_status="owned"`
- use the filename stem as `title`
- skip hidden files such as `.gitkeep` and `.DS_Store`

Unsupported extensions are skipped during ingest and do not move forward.

### Optional approved-source JSON

If you add `data/approved_sources.json`, it must contain a list of items with:
- `source_type`
- `source_path_or_url`
- `rights_status`
- `title`

Example:

```json
[
  {
    "source_type": "local_file",
    "source_path_or_url": "/absolute/path/to/video.mp4",
    "rights_status": "licensed",
    "title": "Example clip"
  }
]
```

Malformed JSON is reported as a batch error. A bad item does not kill the whole batch.

## What appears in each directory

After a normal run:
- valid ingested media is copied into `data/approved/`
- invalid media is moved into `data/rejected/`
- processed output is written into `data/processed/<asset_id>/`

Example processed output:

```text
data/processed/<asset_id>/
  clip_001.mp4
  thumb.jpg
```

## Manifest persistence

The manifest is a SQLite database at `data/manifests/manifest.sqlite3`.

The orchestrator persists processor outcomes after processing:
- successful processing writes `status="processed"`
- failed processing writes `status="failed"`
- successful processing stores generated clip paths in `output_files`
- failed processing stores an empty `output_files`
- processor errors are stored in `error_message`

Tracked fields currently include:
- `asset_id`
- `input_path`
- `source_type`
- `source_path_or_url`
- `rights_status`
- `ingest_time`
- `sha256`
- `duration`
- `status`
- `output_files`
- `error_message`

## Asset identity and rerun behavior

Asset identity is content-based:
- `asset_id` is the SHA-256 of the approved local file bytes
- manifest `sha256` uses the same value

Current rerun behavior:
- rerunning the same asset content keeps the same `asset_id`
- rerunning the same asset updates the existing manifest row instead of creating a duplicate logical record
- if the same file already exists in `data/approved/` with identical content, ingest skips the duplicate copy
- processing outputs remain under `data/processed/<asset_id>/`

## Logging behavior

Runtime logging:
- goes to `data/logs/startup.log`
- includes module logger names for easier debugging
- includes the final pipeline summary from `run.py`

Test logging:
- is quieter by default because the package installs a `NullHandler`
- useful error logging is still preserved when the CLI entrypoint configures logging

## Current limitations

- remote downloads are not implemented
- no uploader exists
- no scheduling exists
- no manifest writes happen before processing outcomes are available
- validator requires `ffprobe` at runtime
- processor requires `ffmpeg` and `ffprobe` at runtime
- processor currently produces one clip and one thumbnail per asset
- no deduplication policy exists yet for different files with the same destination name but different content beyond failing ingest
