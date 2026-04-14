PYTHON ?= python

.PHONY: doctor test run queue-list queue-payload queue-publish queue-fail respeecher-voices

doctor:
	@$(PYTHON) --version
	@command -v ffmpeg >/dev/null 2>&1 || { echo "ffmpeg is not installed or not on PATH"; exit 1; }
	@ffmpeg -version | sed -n '1p'
	@command -v ffprobe >/dev/null 2>&1 || { echo "ffprobe is not installed or not on PATH"; exit 1; }
	@ffprobe -version | sed -n '1p'

test:
	$(PYTHON) -m unittest discover -s tests -v

run:
	$(PYTHON) run.py

queue-list:
	@$(PYTHON) publish_queue_cli.py list

queue-payload:
	@if [ -z "$(ASSET_ID)" ]; then echo "ASSET_ID is required"; exit 1; fi
	@if [ -z "$(CLIP_PATH)" ]; then echo "CLIP_PATH is required"; exit 1; fi
	@$(PYTHON) publish_queue_cli.py payload --asset-id "$(ASSET_ID)" --clip-path "$(CLIP_PATH)"

queue-publish:
	@if [ -z "$(ASSET_ID)" ]; then echo "ASSET_ID is required"; exit 1; fi
	@if [ -z "$(CLIP_PATH)" ]; then echo "CLIP_PATH is required"; exit 1; fi
	@$(PYTHON) publish_queue_cli.py publish --asset-id "$(ASSET_ID)" --clip-path "$(CLIP_PATH)"

queue-fail:
	@if [ -z "$(ASSET_ID)" ]; then echo "ASSET_ID is required"; exit 1; fi
	@if [ -z "$(CLIP_PATH)" ]; then echo "CLIP_PATH is required"; exit 1; fi
	@if [ -z "$(ERROR_MESSAGE)" ]; then echo "ERROR_MESSAGE is required"; exit 1; fi
	@$(PYTHON) publish_queue_cli.py fail --asset-id "$(ASSET_ID)" --clip-path "$(CLIP_PATH)" --error-message "$(ERROR_MESSAGE)"

respeecher-voices:
	@$(PYTHON) respeecher_cli.py voices
