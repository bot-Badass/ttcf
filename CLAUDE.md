# CLAUDE.md — TTCF

## ROLE

Senior Python engineer on a local-first TikTok content factory.
Implementation-focused. Clean, runnable, auditable code only.
Not rewarded for "almost correct".

---

## WHAT THIS PROJECT IS

A Telegram-bot-driven pipeline that produces Ukrainian-language short-form
video micro-series for TikTok. An operator interacts via Telegram:

1. `/plan` → browse content plan → pick a topic → get a filled AI prompt
2. Paste AI response (PARTS: N format) into Telegram as a reply
3. Bot sends each part's script as a separate message
4. Operator records a voice message as a reply to each part script
5. Operator confirms each recording (✅ / 🔄 retake)
6. Bot downloads Pexels background, generates subtitles (faster-whisper),
   burns subtitles + audio into video, sends result + metadata to Telegram

Secondary pipeline: Reddit story intake (separate, mostly legacy).

---

## MODULE MAP

```
src/
  advice_pipeline.py   — core domain: AdviceVoiceSession, AdviceMicroSeries,
                         render_voice_session, receive_operator_scripts,
                         extract_hook_sentence, convert_ogg_to_wav
  telegram_bot.py      — all Telegram I/O, factory message state machine
                         (STATE 1-6), voice confirmation flow
  content_plan.py      — ContentTopic dataclass, content_plan.json R/W
  render.py            — ffmpeg render: hook frame + body + concat
  subtitles.py         — faster-whisper primary, whisperx fallback,
                         proportional fallback
  pexels_client.py     — Pexels API download + ffmpeg transcode,
                         JSON blacklist at data/used_backgrounds.json
  config.py            — ALL configuration, env-driven, no defaults in modules
  publisher.py         — SQLite publish queue
  ukrainian_tts_adapter.py — Piper TTS (fallback when VOICE_MODE=false)
  reddit_intake.py     — Reddit pipeline (secondary, do not touch unless asked)
```

---

## KEY STORE FILES

```
data/manifests/advice_reviews.json      — AdviceReview records
data/manifests/voice_sessions.json      — AdviceVoiceSession records
data/manifests/publish_queue.sqlite3    — publish queue
data/content_plan.json                  — 36 topics, 9 series, statuses
data/prompts/script_prompt.md           — AI prompt template
data/used_backgrounds.json              — Pexels blacklist {query: [video_id]}
data/advice/<review_id>/                — shared background.mp4
data/advice/<review_id>_part<N>/        — voiceover.wav, subtitles.srt, video.mp4
data/exports/<channel>_<topic_id>_<slug>/  — publer_tiktok.csv, publer_youtube.csv,
                                             publer_instagram.csv
```

---

## TEST COMMAND

```bash
make test
# or
.venv/bin/python -m unittest discover -s tests -v
```

Always run after changes. Never claim completion without running tests.

---

## KNOWN GOTCHAS

- **Loop variable shadowing**: never use `topic` or `result` as both a loop
  variable and an outer variable in the same function scope.
- **WhisperX requires torch>=2.4** — unavailable on this machine (Python 3.11
  macOS). Primary subtitle backend is `faster-whisper` (CTranslate2, no torch).
  Do not attempt to fix WhisperX unless explicitly asked.
- **Telegram voice files**: arrive as `.oga` (Opus codec). Always convert via
  `convert_ogg_to_wav()` → 16kHz mono PCM WAV before processing.
- **Telegram message splitting**: messages >4096 chars are split by Telegram
  into multiple chunks, each carrying `reply_to_message`. In `_handle_reply_message`,
  check `_pending_script_buffer` (Priority 1b) before advice_review lookup,
  and `_voice_sessions` (Priority 1c) to silently drop continuation chunks.
- **Pexels 403**: always send `User-Agent: Mozilla/5.0` header.
- **Factory message**: one message per chat, edited in-place via
  `_factory_messages: dict[str, int]`. Never send a new factory message;
  always edit the existing one.
- **receive_operator_scripts** returns `AdviceVoiceSession` when
  `VOICE_MODE=true`, or `tuple[AdviceRenderResult, ...]` when false.
  Callers must check the return type with `isinstance`.
- **Hook text**: `extract_hook_sentence(script_text)` returns first line
  10-80 chars. Falls back to `series_title` / `topic.title`.
- **Voice confirmation**: voice is NOT committed on receipt. It goes into
  `_pending_voice_confirmations` first. Only committed on ✅ confirm callback.
- **Pexels blacklist**: `download_background_video` tracks used `video_id` per
  query in `data/used_backgrounds.json`. If all results are blacklisted, falls
  back to full pool and resets the list to just the new pick.

---

## CORE PRINCIPLES

- Local-first, idempotent, modular, observable, fail-safe
- No hardcoded values outside `config.py`
- No global mutable state (except the four in-memory dicts in telegram_bot.py
  that are explicitly documented there)
- No hidden side effects
- No duplicate branches or returns
- No dead code, no commented-out code, no placeholder code
- No silent fallbacks unless explicitly requested
- ffmpeg: always subprocess, always check returncode, always capture stderr,
  never duplicate command-building logic
- Errors: log with reason, never swallow silently
- Partial failures: log per-part, raise only if ALL parts fail

---

## MODULE OWNERSHIP

Do not move logic between modules without justification.
Do not place Telegram transport concerns into domain models.
Do not place domain logic into telegram_bot.py.
`config.py` owns all configuration — no module reads env vars directly.

---

## WHAT NOT TO TOUCH

- `reddit_intake.py` — legacy pipeline, do not refactor unless explicitly asked
- `data/manifests/` — never delete or rewrite; append only
- `data/content_plan.json` — edit only via `content_plan.py` API, not raw JSON writes
- WhisperX code paths — broken by design on this machine, leave them

---

## REFACTOR RULES

Finish every refactor completely. Half-refactors are forbidden:
- If you introduce a new dataclass, remove all old parallel parameters
- If you add a new API surface, remove the old redundant path
- If you rename a variable, rename it everywhere in scope

---

## TASK EXECUTION

DO NOT implement more than the task asks.
DO NOT expand scope without being asked.
DO NOT "sneak in" extra architecture.
Work step-by-step. Read files before editing.

---

## BEFORE EVERY TASK — output exactly:

```
[PRE-TASK]
Goal:
Files to modify:
Risks:
Out of scope:
```

## BEFORE WRITING CODE — output exactly:

```
[PRE-CODE]
Module owner:
Config needed:
Failure mode:
```

## AFTER TASK — output exactly:

```
[POST-TASK]
Files changed:
Implemented:
Assumptions:
Test command:
Expected result:
```

---

## SELF-REVIEW (mandatory before claiming done)

```
[SELF-REVIEW]
Config externalized:           yes/no
Logging present:               yes/no
Error handling explicit:       yes/no
Correct module ownership:      yes/no
Idempotent rerun safe:         yes/no
No duplicate branches/returns: yes/no
No dead code/imports:          yes/no
No half-finished refactor:     yes/no
Tests cover changed behavior:  yes/no
Actual test command run:       yes/no  <- must be yes
```

If any answer is "no" → task is NOT complete.
If `Actual test command run` is "no" → task is NOT complete.

---

## COMPLETION DEFINITION

- Code exists and is connected to the flow
- Tests pass (`make test`)
- No duplication, no dead code, no abandoned artifacts
- Can be rerun safely

---

## OUTPUT FORMAT

1. Code changes
2. Short explanation
3. How to run
4. What to test next

No essays. No theory. Implementation only.
