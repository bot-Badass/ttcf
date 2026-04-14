from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.advice_pipeline import (
    ADVICE_APPROVED_STATUS,
    ADVICE_PENDING_STATUS,
    ADVICE_REJECTED_STATUS,
    AdviceMicroSeries,
    AdvicePartScript,
    AdvicePipelineError,
    AdviceReview,
    AdviceScript,
    AdviceVoiceSession,
    approve_advice_script,
    convert_ogg_to_wav,
    create_voice_session,
    extract_hook_sentence,
    generate_post_meta,
    get_advice_review,
    get_advice_stats,
    get_voice_session,
    get_voice_session_by_part_message_id,
    is_voice_session_complete,
    issue_next_topic,
    issue_specific_topic,
    list_advice_reviews,
    parse_micro_series_response,
    receive_operator_script,
    receive_operator_scripts,
    reject_advice_script,
    render_micro_series,
    render_voice_session,
    save_advice_telegram_message_id,
    save_part_message_id,
    save_part_voice,
)
from src.content_plan import (
    TOPIC_RENDERED,
    TOPIC_SCRIPT_RECEIVED,
    ContentTopic,
    get_topic_by_id,
    mark_topic_status,
)


def _write_dummy_plan(plan_path: Path, num_topics: int = 6) -> None:
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    topics = [
        {
            "topic_id": f"A{i}",
            "part_number": i,
            "title": f"Тема {i}",
            "hook_formula": "Hook",
            "audience": "Audience",
            "scenario": "Scenario",
            "legal_facts": ["Fact 1"],
            "status": "pending",
            "script_id": None,
            "script_saved_at": None,
        }
        for i in range(1, num_topics + 1)
    ]
    plan_path.write_text(
        json.dumps(
            {
                "series": [
                    {
                        "series_id": "A",
                        "title": "Test Series",
                        "total_parts": num_topics,
                        "topics": topics,
                    }
                ],
                "current_topic_index": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _fake_tts_generator(script: str, output_path: Path, voice: str) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(b"fake-wav")
    return output_path


def _fake_video_downloader(output_path: Path, query: str = "") -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(b"fake-mp4")
    return output_path


def _fake_subtitle_generator(audio_path: Path, script: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    subtitle_path = output_dir / "subtitles.srt"
    subtitle_path.write_text("1\n00:00:00,000 --> 00:00:02,000\nTest\n", encoding="utf-8")
    return subtitle_path


def _fake_renderer(bg: Path, audio: Path, output: Path, subtitle: Path | None, hook_text: str | None = None) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"fake-rendered-mp4")
    return output


def _make_pending_review(store_path: Path, plan_path: Path) -> AdviceReview:
    review, _ = issue_next_topic(store_path=store_path, plan_path=plan_path)
    return review


class AdvicePipelineTests(unittest.TestCase):
    # 5e
    def test_issue_next_topic_creates_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)

            review, topic = issue_next_topic(store_path=store_path, plan_path=plan_path)

            self.assertEqual(review.status, ADVICE_PENDING_STATUS)
            self.assertEqual(review.script.generated_script, "")
            self.assertEqual(review.script.topic, topic.title)
            self.assertTrue(store_path.is_file())
            reviews = list_advice_reviews(store_path=store_path)
            self.assertEqual(len(reviews), 1)

    # 5f
    def test_receive_operator_script_validates_min_words(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            script_id = review.script.script_id

            with self.assertRaises(AdvicePipelineError) as ctx:
                receive_operator_script(
                    script_id,
                    "Занадто короткий скрипт.",
                    store_path=store_path,
                    plan_path=plan_path,
                    min_words=100,
                )

        self.assertIn("100", str(ctx.exception))

    # 5g
    def test_receive_operator_script_saves_and_marks_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            script_id = review.script.script_id
            long_script = "слово " * 110

            updated = receive_operator_script(
                script_id,
                long_script,
                store_path=store_path,
                plan_path=plan_path,
                min_words=100,
            )

            self.assertEqual(updated.script.generated_script, long_script)
            self.assertEqual(updated.status, ADVICE_PENDING_STATUS)

            # Content plan topic should be marked script_received
            topic = get_topic_by_id("A1", plan_path)
            self.assertIsNotNone(topic)
            self.assertEqual(topic.status, TOPIC_SCRIPT_RECEIVED)
            self.assertEqual(topic.script_id, script_id)

    def test_approve_advice_script_calls_tts_and_render(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            output_dir = Path(tmp) / "advice"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            script_id = review.script.script_id

            result = approve_advice_script(
                script_id,
                store_path=store_path,
                output_dir=output_dir,
                tts_generator=_fake_tts_generator,
                video_downloader=_fake_video_downloader,
                subtitle_generator=_fake_subtitle_generator,
                renderer=_fake_renderer,
            )

            self.assertTrue(result.output_path.is_file())
            self.assertEqual(result.script_id, script_id)

    def test_approve_advice_script_fails_for_non_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            output_dir = Path(tmp) / "advice"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            script_id = review.script.script_id

            approve_advice_script(
                script_id,
                store_path=store_path,
                output_dir=output_dir,
                tts_generator=_fake_tts_generator,
                video_downloader=_fake_video_downloader,
                subtitle_generator=_fake_subtitle_generator,
                renderer=_fake_renderer,
            )

            with self.assertRaises(AdvicePipelineError) as ctx:
                approve_advice_script(
                    script_id,
                    store_path=store_path,
                    output_dir=output_dir,
                    tts_generator=_fake_tts_generator,
                    video_downloader=_fake_video_downloader,
                    subtitle_generator=_fake_subtitle_generator,
                    renderer=_fake_renderer,
                )

        self.assertIn(ADVICE_APPROVED_STATUS, str(ctx.exception))

    def test_reject_advice_script_updates_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            script_id = review.script.script_id

            rejected = reject_advice_script(script_id, store_path=store_path)

            self.assertEqual(rejected.status, ADVICE_REJECTED_STATUS)
            stored = get_advice_review(script_id, store_path=store_path)
            self.assertIsNotNone(stored)
            self.assertEqual(stored.status, ADVICE_REJECTED_STATUS)

    def test_get_advice_stats_counts_correctly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            output_dir = Path(tmp) / "advice"
            _write_dummy_plan(plan_path, num_topics=6)

            r1 = _make_pending_review(store_path, plan_path)
            r2 = _make_pending_review(store_path, plan_path)
            r3 = _make_pending_review(store_path, plan_path)
            r4 = _make_pending_review(store_path, plan_path)

            approve_advice_script(
                r3.script.script_id,
                store_path=store_path,
                output_dir=output_dir,
                tts_generator=_fake_tts_generator,
                video_downloader=_fake_video_downloader,
                subtitle_generator=_fake_subtitle_generator,
                renderer=_fake_renderer,
            )
            reject_advice_script(r4.script.script_id, store_path=store_path)

            stats = get_advice_stats(store_path=store_path)

        self.assertEqual(stats.total, 4)
        self.assertEqual(stats.pending_review, 2)
        self.assertEqual(stats.approved, 1)
        self.assertEqual(stats.rejected, 1)

    def test_save_advice_telegram_message_id_persists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            script_id = review.script.script_id

            updated = save_advice_telegram_message_id(
                script_id, "chat-1", 9999, store_path=store_path
            )

            self.assertEqual(updated.telegram_message_id, 9999)
            self.assertEqual(updated.telegram_chat_id, "chat-1")
            stored = get_advice_review(script_id, store_path=store_path)
            self.assertIsNotNone(stored)
            self.assertEqual(stored.telegram_message_id, 9999)


def _make_valid_raw_response(n: int, words_per_part: int = 110) -> str:
    lines = [f"PARTS: {n}", ""]
    for i in range(1, n + 1):
        lines.append(f"=== ЧАСТИНА {i}/{n} ===")
        lines.append(("слово " * words_per_part).strip())
        lines.append("")
    return "\n".join(lines)


def _make_dummy_topic(plan_path: Path) -> object:
    from src.content_plan import list_topics
    return list_topics(plan_path)[0]


class AdviceMicroSeriesParseTests(unittest.TestCase):
    # 4a
    def test_parse_micro_series_response_valid_3_parts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            topic = _make_dummy_topic(plan_path)
            raw = _make_valid_raw_response(3)
            result = parse_micro_series_response("rev-001", raw, topic, min_words=100)
        self.assertIsInstance(result, AdviceMicroSeries)
        self.assertEqual(len(result.parts), 3)
        for i, part in enumerate(result.parts, start=1):
            self.assertEqual(part.part_number, i)
            self.assertEqual(part.total_parts, 3)
            self.assertGreaterEqual(part.word_count, 100)

    # 4b
    def test_parse_micro_series_response_valid_5_parts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            topic = _make_dummy_topic(plan_path)
            raw = _make_valid_raw_response(5)
            result = parse_micro_series_response("rev-002", raw, topic, min_words=100)
        self.assertEqual(len(result.parts), 5)
        self.assertIsInstance(result.parts[0], AdvicePartScript)

    # 4c
    def test_parse_micro_series_response_wrong_part_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            topic = _make_dummy_topic(plan_path)
            # Declare 4 but only include 3 blocks
            raw = "PARTS: 4\n\n" + "\n".join(
                f"=== ЧАСТИНА {i}/4 ===\n" + ("слово " * 110).strip()
                for i in range(1, 4)
            )
            with self.assertRaises(AdvicePipelineError) as ctx:
                parse_micro_series_response("rev-003", raw, topic, min_words=100)
        self.assertIn("3", str(ctx.exception))

    # 4d
    def test_parse_micro_series_response_part_too_short(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            topic = _make_dummy_topic(plan_path)
            raw = (
                "PARTS: 3\n\n"
                "=== ЧАСТИНА 1/3 ===\n" + ("слово " * 110).strip() + "\n\n"
                "=== ЧАСТИНА 2/3 ===\nТільки п'ять слів тут є\n\n"
                "=== ЧАСТИНА 3/3 ===\n" + ("слово " * 110).strip()
            )
            with self.assertRaises(AdvicePipelineError) as ctx:
                parse_micro_series_response("rev-004", raw, topic, min_words=100)
        self.assertIn("2/3", str(ctx.exception))

    # 4e
    def test_parse_micro_series_response_missing_parts_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            topic = _make_dummy_topic(plan_path)
            raw = (
                "=== ЧАСТИНА 1/3 ===\n" + ("слово " * 110).strip() + "\n\n"
                "=== ЧАСТИНА 2/3 ===\n" + ("слово " * 110).strip() + "\n\n"
                "=== ЧАСТИНА 3/3 ===\n" + ("слово " * 110).strip()
            )
            with self.assertRaises(AdvicePipelineError) as ctx:
                parse_micro_series_response("rev-005", raw, topic, min_words=100)
        self.assertIn("PARTS", str(ctx.exception))


class AdviceMicroSeriesRenderTests(unittest.TestCase):
    def _make_micro_series(
        self,
        review_id: str,
        n: int,
        background_video_path: Path,
        plan_path: Path,
    ) -> AdviceMicroSeries:
        topic = _make_dummy_topic(plan_path)
        raw = _make_valid_raw_response(n)
        import dataclasses
        ms = parse_micro_series_response(review_id, raw, topic, min_words=100)
        return dataclasses.replace(ms, background_video_path=background_video_path)

    # 4f
    def test_render_micro_series_calls_tts_per_part(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            output_dir = Path(tmp) / "advice"
            bg = Path(tmp) / "bg.mp4"
            bg.parent.mkdir(parents=True, exist_ok=True)
            bg.write_bytes(b"fake-bg")

            tts_calls: list[str] = []

            def counting_tts(script: str, path: Path, voice: str) -> Path:
                tts_calls.append(script[:10])
                return _fake_tts_generator(script, path, voice)

            renderer_calls: list[Path] = []

            def counting_renderer(bg_path: Path, audio: Path, out: Path, sub: Path | None, hook: str | None = None) -> Path:
                renderer_calls.append(out)
                return _fake_renderer(bg_path, audio, out, sub)

            ms = self._make_micro_series("rev-f", 3, bg, plan_path)
            store_path = Path(tmp) / "store.json"
            results = render_micro_series(
                ms,
                store_path=store_path,
                output_dir=output_dir,
                tts_generator=counting_tts,
                subtitle_generator=_fake_subtitle_generator,
                renderer=counting_renderer,
            )

        self.assertEqual(len(tts_calls), 3)
        self.assertEqual(len(renderer_calls), 3)
        self.assertEqual(len(results), 3)

    # 4g
    def test_render_micro_series_partial_failure_returns_successful_parts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            output_dir = Path(tmp) / "advice"
            bg = Path(tmp) / "bg.mp4"
            bg.write_bytes(b"fake-bg")

            call_count = {"n": 0}

            def failing_on_part2(bg_path: Path, audio: Path, out: Path, sub: Path | None, hook: str | None = None) -> Path:
                call_count["n"] += 1
                if call_count["n"] == 2:
                    from src.render import RenderError
                    raise RenderError("simulated failure on part 2")
                return _fake_renderer(bg_path, audio, out, sub)

            ms = self._make_micro_series("rev-g", 3, bg, plan_path)
            store_path = Path(tmp) / "store.json"
            results = render_micro_series(
                ms,
                store_path=store_path,
                output_dir=output_dir,
                tts_generator=_fake_tts_generator,
                subtitle_generator=_fake_subtitle_generator,
                renderer=failing_on_part2,
            )

        self.assertEqual(len(results), 2)

    # 4h
    def test_render_micro_series_all_fail_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            output_dir = Path(tmp) / "advice"
            bg = Path(tmp) / "bg.mp4"
            bg.write_bytes(b"fake-bg")

            def always_fail(bg_path: Path, audio: Path, out: Path, sub: Path | None, hook: str | None = None) -> Path:
                from src.render import RenderError
                raise RenderError("always fails")

            ms = self._make_micro_series("rev-h", 3, bg, plan_path)
            store_path = Path(tmp) / "store.json"
            with self.assertRaises(AdvicePipelineError) as ctx:
                render_micro_series(
                    ms,
                    store_path=store_path,
                    output_dir=output_dir,
                    tts_generator=_fake_tts_generator,
                    subtitle_generator=_fake_subtitle_generator,
                    renderer=always_fail,
                )
        self.assertIn("rev-h", str(ctx.exception))


def _make_topic(
    topic_id: str = "A1",
    series_id: str = "A",
    series_title: str = "Мобілізація",
    title: str = "Як уникнути мобілізації",
    scenario: str = "Сценарій про мобілізацію",
    legal_facts: tuple[str, ...] = ("Факт 1",),
    part_number: int = 1,
    total_parts: int = 4,
    status: str = "pending",
) -> ContentTopic:
    return ContentTopic(
        topic_id=topic_id,
        series_id=series_id,
        series_title=series_title,
        part_number=part_number,
        total_parts=total_parts,
        title=title,
        hook_formula="Hook",
        audience="Audience",
        scenario=scenario,
        legal_facts=legal_facts,
        status=status,
        script_id=None,
        script_saved_at=None,
    )


class GeneratePostMetaTests(unittest.TestCase):
    # 9a
    def test_generate_post_meta_series_A(self) -> None:
        topic = _make_topic(series_id="A", part_number=1, total_parts=4)

        meta = generate_post_meta(topic, 1, 4)

        self.assertIn("мобілізація", meta.hashtags)
        self.assertIn("юрист", meta.hashtags)
        self.assertLessEqual(len(meta.hashtags), 7)
        self.assertIn("1/4", meta.title)

    # 9b
    def test_generate_post_meta_truncates_title(self) -> None:
        topic = _make_topic(title="А" * 200)

        meta = generate_post_meta(topic, 1, 4)

        self.assertLessEqual(len(meta.title), 100)

    # 9c
    def test_generate_post_meta_description_has_cta(self) -> None:
        topic = _make_topic()

        meta = generate_post_meta(topic, 1, 4)

        self.assertIn("Підписуйся", meta.description)


class IssueSpecificTopicTests(unittest.TestCase):
    # 9i
    def test_issue_specific_topic_creates_review_for_requested_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)

            # Mark A1 as rendered so it is no longer pending.
            mark_topic_status("A1", TOPIC_RENDERED, plan_path=plan_path)

            # A2 is still pending — should work without error.
            review, topic = issue_specific_topic(
                "A2", store_path=store_path, plan_path=plan_path
            )

            self.assertEqual(review.script.topic, topic.title)
            self.assertEqual(review.status, ADVICE_PENDING_STATUS)

    # 9j
    def test_issue_specific_topic_raises_for_non_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)

            mark_topic_status("A1", TOPIC_RENDERED, plan_path=plan_path)

            with self.assertRaises(AdvicePipelineError):
                issue_specific_topic("A1", store_path=store_path, plan_path=plan_path)


def _make_micro_series(
    review_id: str = "r1",
    n_parts: int = 3,
    background_video_path: Path | None = None,
) -> AdviceMicroSeries:
    parts = tuple(
        AdvicePartScript(
            part_number=i,
            total_parts=n_parts,
            script_text=" ".join(["слово"] * 120),
            word_count=120,
        )
        for i in range(1, n_parts + 1)
    )
    return AdviceMicroSeries(
        review_id=review_id,
        topic_id="A1",
        series_title="Test Series",
        parts=parts,
        background_video_path=background_video_path or Path("background.mp4"),
        parsed_at="2026-01-01T00:00:00Z",
    )


class VoiceSessionTests(unittest.TestCase):
    # 10a
    def test_create_voice_session_initializes_all_slots_as_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "voice_sessions.json"
            ms = _make_micro_series(n_parts=3)
            session = create_voice_session("r1", ms, Path(tmp) / "bg.mp4", store_path=store_path)

            self.assertEqual(len(session.voice_files), 3)
            self.assertTrue(all(f is None for f in session.voice_files))
            self.assertEqual(len(session.part_message_ids), 3)
            self.assertTrue(all(m is None for m in session.part_message_ids))

    # 10b
    def test_save_part_voice_updates_correct_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "voice_sessions.json"
            ms = _make_micro_series(n_parts=3)
            session = create_voice_session("r1", ms, Path(tmp) / "bg.mp4", store_path=store_path)
            wav = Path(tmp) / "part2.wav"
            wav.write_bytes(b"fake-wav")

            updated = save_part_voice(session.session_id, 2, wav, store_path=store_path)

            self.assertIsNone(updated.voice_files[0])
            self.assertEqual(updated.voice_files[1], wav)
            self.assertIsNone(updated.voice_files[2])

    # 10c
    def test_is_voice_session_complete_false_when_partial(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "voice_sessions.json"
            ms = _make_micro_series(n_parts=3)
            session = create_voice_session("r1", ms, Path(tmp) / "bg.mp4", store_path=store_path)
            wav1 = Path(tmp) / "p1.wav"
            wav1.write_bytes(b"w")
            session = save_part_voice(session.session_id, 1, wav1, store_path=store_path)
            wav2 = Path(tmp) / "p2.wav"
            wav2.write_bytes(b"w")
            session = save_part_voice(session.session_id, 2, wav2, store_path=store_path)

            self.assertFalse(is_voice_session_complete(session))

    # 10d
    def test_is_voice_session_complete_true_when_all_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "voice_sessions.json"
            ms = _make_micro_series(n_parts=3)
            session = create_voice_session("r1", ms, Path(tmp) / "bg.mp4", store_path=store_path)
            for i in range(1, 4):
                wav = Path(tmp) / f"p{i}.wav"
                wav.write_bytes(b"w")
                session = save_part_voice(session.session_id, i, wav, store_path=store_path)

            self.assertTrue(is_voice_session_complete(session))

    # 10e
    def test_convert_ogg_to_wav_produces_wav_file(self) -> None:
        import shutil
        if not shutil.which("ffmpeg"):
            self.skipTest("ffmpeg not available")

        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            ogg_path = Path(tmp) / "silence.ogg"
            # Generate 1-second silence OGG via ffmpeg.
            result = subprocess.run(
                [
                    "ffmpeg", "-y",
                    "-f", "lavfi",
                    "-i", "anullsrc=r=16000:cl=mono",
                    "-t", "1",
                    "-c:a", "libvorbis",
                    str(ogg_path),
                ],
                capture_output=True,
                timeout=30,
            )
            if result.returncode != 0 or not ogg_path.is_file():
                self.skipTest("ffmpeg could not generate test OGG")

            wav_path = Path(tmp) / "output.wav"
            convert_ogg_to_wav(ogg_path, wav_path)

            self.assertTrue(wav_path.is_file())
            import wave
            with wave.open(str(wav_path), "rb") as wf:
                self.assertEqual(wf.getframerate(), 16000)
                self.assertEqual(wf.getnchannels(), 1)

    # 10f
    def test_get_voice_session_by_part_message_id_returns_correct_part(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "voice_sessions.json"
            ms = _make_micro_series(n_parts=3)
            session = create_voice_session("r1", ms, Path(tmp) / "bg.mp4", store_path=store_path)
            save_part_message_id(session.session_id, 1, 100, store_path=store_path)
            save_part_message_id(session.session_id, 2, 200, store_path=store_path)
            save_part_message_id(session.session_id, 3, 300, store_path=store_path)

            result = get_voice_session_by_part_message_id(200, store_path=store_path)

            self.assertIsNotNone(result)
            found_session, found_part = result
            self.assertEqual(found_session.session_id, session.session_id)
            self.assertEqual(found_part, 2)

    # 10g
    def test_save_part_message_id_persists_to_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "voice_sessions.json"
            ms = _make_micro_series(n_parts=3)
            session = create_voice_session("r1", ms, Path(tmp) / "bg.mp4", store_path=store_path)

            save_part_message_id(session.session_id, 1, 999, store_path=store_path)

            reloaded = get_voice_session(session.session_id, store_path=store_path)
            self.assertIsNotNone(reloaded)
            self.assertEqual(reloaded.part_message_ids[0], 999)

    # 10h
    def test_render_voice_session_uses_wav_files_not_tts(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            voice_store_path = Path(tmp) / "voice_sessions.json"
            plan_path = Path(tmp) / "content_plan.json"
            output_dir = Path(tmp) / "advice"
            _write_dummy_plan(plan_path, num_topics=3)

            # Create a pending review so render_voice_session can update it.
            review = _make_pending_review(store_path, plan_path)
            review_id = review.script.script_id

            bg = Path(tmp) / "bg.mp4"
            bg.write_bytes(b"fake-bg")
            ms = _make_micro_series(review_id=review_id, n_parts=3, background_video_path=bg)
            session = create_voice_session(
                review_id, ms, bg, store_path=voice_store_path
            )
            for i in range(1, 4):
                wav = Path(tmp) / f"p{i}.wav"
                wav.write_bytes(b"fake-wav")
                session = save_part_voice(
                    session.session_id, i, wav, store_path=voice_store_path
                )

            renderer_calls: list[Path] = []

            def counting_renderer(
                bg_path: Path, audio: Path, out: Path,
                sub: Path | None, hook: str | None = None,
            ) -> Path:
                renderer_calls.append(out)
                return _fake_renderer(bg_path, audio, out, sub)

            with patch("src.advice_pipeline.generate_tts_wav") as mock_tts:
                results = render_voice_session(
                    session,
                    store_path=store_path,
                    voice_store_path=voice_store_path,
                    plan_path=plan_path,
                    output_dir=output_dir,
                    subtitle_generator=_fake_subtitle_generator,
                    renderer=counting_renderer,
                )

            mock_tts.assert_not_called()
            self.assertEqual(len(renderer_calls), 3)
            self.assertEqual(len(results), 3)

    # 10i
    def test_receive_operator_scripts_voice_mode_returns_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            output_dir = Path(tmp) / "advice"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            review_id = review.script.script_id
            topic = _make_dummy_topic(plan_path)
            raw = _make_valid_raw_response(3)

            result = receive_operator_scripts(
                review_id,
                raw,
                topic,
                store_path=store_path,
                plan_path=plan_path,
                output_dir=output_dir,
                video_downloader=_fake_video_downloader,
                voice_mode=True,
            )

            self.assertIsInstance(result, AdviceVoiceSession)
            self.assertEqual(result.review_id, review_id)
            self.assertEqual(len(result.voice_files), 3)
            self.assertTrue(all(f is None for f in result.voice_files))

    # 10j
    def test_receive_operator_scripts_tts_mode_returns_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            output_dir = Path(tmp) / "advice"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            review_id = review.script.script_id
            topic = _make_dummy_topic(plan_path)
            raw = _make_valid_raw_response(3)

            result = receive_operator_scripts(
                review_id,
                raw,
                topic,
                store_path=store_path,
                plan_path=plan_path,
                output_dir=output_dir,
                video_downloader=_fake_video_downloader,
                tts_generator=_fake_tts_generator,
                subtitle_generator=_fake_subtitle_generator,
                renderer=_fake_renderer,
                voice_mode=False,
            )

            self.assertIsInstance(result, tuple)
            self.assertEqual(len(result), 3)


class ExtractHookSentenceTests(unittest.TestCase):
    # 11a
    def test_extract_hook_sentence_returns_first_valid_line(self) -> None:
        script = "short\nThis is a valid hook sentence here.\nAnother line."
        result = extract_hook_sentence(script)
        self.assertEqual(result, "This is a valid hook sentence here.")

    # 11b
    def test_extract_hook_sentence_skips_short_lines(self) -> None:
        script = "tiny\nok\nThis line is long enough to qualify as a hook."
        result = extract_hook_sentence(script)
        self.assertEqual(result, "This line is long enough to qualify as a hook.")

    # 11c
    def test_extract_hook_sentence_skips_long_lines(self) -> None:
        too_long = "x" * 81
        valid = "This sentence is exactly right for a hook."
        script = f"{too_long}\n{valid}"
        result = extract_hook_sentence(script)
        self.assertEqual(result, valid)

    # 11d
    def test_extract_hook_sentence_returns_none_when_no_match(self) -> None:
        script = "hi\n" + "x" * 81
        result = extract_hook_sentence(script)
        self.assertIsNone(result)

    # 11e
    def test_render_micro_series_uses_extracted_hook_for_part1(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            output_dir = Path(tmp) / "advice"
            bg = Path(tmp) / "bg.mp4"
            bg.write_bytes(b"fake-bg")

            hook_texts: list[str | None] = []

            def capturing_renderer(
                bg_path: Path, audio: Path, out: Path,
                sub: Path | None, hook: str | None = None,
            ) -> Path:
                hook_texts.append(hook)
                return _fake_renderer(bg_path, audio, out, sub)

            # Build a micro-series where part 1 has a valid hook sentence
            valid_hook = "Це перше речення є достатньо довгим."
            parts = tuple(
                AdvicePartScript(
                    part_number=i,
                    total_parts=3,
                    script_text=valid_hook + "\n" + " ".join(["слово"] * 120) if i == 1 else " ".join(["слово"] * 120),
                    word_count=120,
                )
                for i in range(1, 4)
            )
            ms = AdviceMicroSeries(
                review_id="rev-11e",
                topic_id="A1",
                series_title="Test Series",
                parts=parts,
                background_video_path=bg,
                parsed_at="2026-01-01T00:00:00Z",
            )
            store_path = Path(tmp) / "store.json"
            render_micro_series(
                ms,
                store_path=store_path,
                output_dir=output_dir,
                tts_generator=_fake_tts_generator,
                subtitle_generator=_fake_subtitle_generator,
                renderer=capturing_renderer,
            )

        self.assertEqual(hook_texts[0], valid_hook)

    # 11f
    def test_render_micro_series_falls_back_to_series_title_when_no_hook(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            output_dir = Path(tmp) / "advice"
            bg = Path(tmp) / "bg.mp4"
            bg.write_bytes(b"fake-bg")

            hook_texts: list[str | None] = []

            def capturing_renderer(
                bg_path: Path, audio: Path, out: Path,
                sub: Path | None, hook: str | None = None,
            ) -> Path:
                hook_texts.append(hook)
                return _fake_renderer(bg_path, audio, out, sub)

            # All lines are too short or too long to produce a hook
            no_hook_script = "hi\n" + "x" * 81 + "\n" + " ".join(["слово"] * 120)
            parts = (
                AdvicePartScript(
                    part_number=1,
                    total_parts=1,
                    script_text=no_hook_script,
                    word_count=120,
                ),
            )
            ms = AdviceMicroSeries(
                review_id="rev-11f",
                topic_id="A1",
                series_title="Fallback Title",
                parts=parts,
                background_video_path=bg,
                parsed_at="2026-01-01T00:00:00Z",
            )
            store_path = Path(tmp) / "store.json"
            render_micro_series(
                ms,
                store_path=store_path,
                output_dir=output_dir,
                tts_generator=_fake_tts_generator,
                subtitle_generator=_fake_subtitle_generator,
                renderer=capturing_renderer,
            )

        self.assertEqual(hook_texts[0], "Fallback Title")

    # 11g
    def test_render_voice_session_uses_extracted_hook_for_part1(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            voice_store_path = Path(tmp) / "voice_sessions.json"
            plan_path = Path(tmp) / "content_plan.json"
            output_dir = Path(tmp) / "advice"
            _write_dummy_plan(plan_path, num_topics=3)

            review = _make_pending_review(store_path, plan_path)
            review_id = review.script.script_id

            bg = Path(tmp) / "bg.mp4"
            bg.write_bytes(b"fake-bg")

            valid_hook = "Це перше речення достатньо довге для хука."
            parts = tuple(
                AdvicePartScript(
                    part_number=i,
                    total_parts=3,
                    script_text=valid_hook + "\n" + " ".join(["слово"] * 120) if i == 1 else " ".join(["слово"] * 120),
                    word_count=120,
                )
                for i in range(1, 4)
            )
            ms = AdviceMicroSeries(
                review_id=review_id,
                topic_id="A1",
                series_title="Test Series",
                parts=parts,
                background_video_path=bg,
                parsed_at="2026-01-01T00:00:00Z",
            )
            session = create_voice_session(review_id, ms, bg, store_path=voice_store_path)
            for i in range(1, 4):
                wav = Path(tmp) / f"p{i}.wav"
                wav.write_bytes(b"fake-wav")
                session = save_part_voice(session.session_id, i, wav, store_path=voice_store_path)

            hook_texts: list[str | None] = []

            def capturing_renderer(
                bg_path: Path, audio: Path, out: Path,
                sub: Path | None, hook: str | None = None,
            ) -> Path:
                hook_texts.append(hook)
                return _fake_renderer(bg_path, audio, out, sub)

            render_voice_session(
                session,
                store_path=store_path,
                voice_store_path=voice_store_path,
                plan_path=plan_path,
                output_dir=output_dir,
                subtitle_generator=_fake_subtitle_generator,
                renderer=capturing_renderer,
            )

        self.assertEqual(hook_texts[0], valid_hook)

    # 11h
    def test_pexels_query_resolved_from_topic_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            output_dir = Path(tmp) / "advice"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            review_id = review.script.script_id
            topic = _make_dummy_topic(plan_path)

            from src.content_plan import ContentTopic
            topic_with_query = ContentTopic(
                topic_id=topic.topic_id,
                series_id=topic.series_id,
                series_title=topic.series_title,
                part_number=topic.part_number,
                total_parts=topic.total_parts,
                title=topic.title,
                hook_formula=topic.hook_formula,
                audience=topic.audience,
                scenario=topic.scenario,
                legal_facts=topic.legal_facts,
                status=topic.status,
                script_id=topic.script_id,
                script_saved_at=topic.script_saved_at,
                pexels_query="military checkpoint soldier ukraine",
            )

            captured_queries: list[str] = []

            def capturing_downloader(output_path: Path, query: str) -> Path:
                captured_queries.append(query)
                return _fake_video_downloader(output_path, query)

            raw = _make_valid_raw_response(3)
            receive_operator_scripts(
                review_id,
                raw,
                topic_with_query,
                store_path=store_path,
                plan_path=plan_path,
                output_dir=output_dir,
                video_downloader=capturing_downloader,
                tts_generator=_fake_tts_generator,
                subtitle_generator=_fake_subtitle_generator,
                renderer=_fake_renderer,
                voice_mode=False,
            )

        self.assertEqual(captured_queries, ["military checkpoint soldier ukraine"])

    # 11i
    def test_pexels_query_falls_back_to_config_when_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "advice_reviews.json"
            plan_path = Path(tmp) / "content_plan.json"
            output_dir = Path(tmp) / "advice"
            _write_dummy_plan(plan_path)

            review = _make_pending_review(store_path, plan_path)
            review_id = review.script.script_id
            topic = _make_dummy_topic(plan_path)  # pexels_query="" by default

            from src import config as advice_config
            captured_queries: list[str] = []

            def capturing_downloader(output_path: Path, query: str) -> Path:
                captured_queries.append(query)
                return _fake_video_downloader(output_path, query)

            raw = _make_valid_raw_response(3)
            receive_operator_scripts(
                review_id,
                raw,
                topic,
                store_path=store_path,
                plan_path=plan_path,
                output_dir=output_dir,
                video_downloader=capturing_downloader,
                tts_generator=_fake_tts_generator,
                subtitle_generator=_fake_subtitle_generator,
                renderer=_fake_renderer,
                voice_mode=False,
            )

        self.assertEqual(captured_queries, [advice_config.ADVICE_PEXELS_QUERY])

    # 11j
    def test_content_topic_pexels_query_deserialized_from_series(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            import json
            plan_path = Path(tmp) / "content_plan.json"
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            plan_path.write_text(
                json.dumps({
                    "series": [{
                        "series_id": "A",
                        "title": "Test",
                        "pexels_query": "military checkpoint soldier ukraine",
                        "total_parts": 1,
                        "topics": [{
                            "topic_id": "A1",
                            "part_number": 1,
                            "title": "Topic",
                            "hook_formula": "",
                            "audience": "",
                            "scenario": "",
                            "legal_facts": [],
                            "status": "pending",
                        }],
                    }],
                    "current_topic_index": 0,
                }),
                encoding="utf-8",
            )
            from src.content_plan import list_topics
            topics = list_topics(plan_path)

        self.assertEqual(topics[0].pexels_query, "military checkpoint soldier ukraine")

    # 11k
    def test_content_topic_pexels_query_defaults_to_empty_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "content_plan.json"
            _write_dummy_plan(plan_path)
            from src.content_plan import list_topics
            topics = list_topics(plan_path)

        self.assertEqual(topics[0].pexels_query, "")


if __name__ == "__main__":
    unittest.main()
