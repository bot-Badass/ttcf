from __future__ import annotations

import unittest

from src.subtitles import AlignedWord
from src.myth_pipeline import (
    extract_voice_text,
    map_sections_to_timestamps,
    parse_myth_script,
)

SAMPLE_SCRIPT = """\
##bg: stressed man reading court letter
3 речі які всі думають про МФО — і всі три неправда.

##bg: worried man documents desk
Міф 1: МФО завжди виграє в суді.

##bg: judge courtroom signing
Це неправда. Є три умови за яких суд відхиляє позов.

##bg: person relief laptop bank
Реальність: якщо заявиш про строк давності — МФО програє.
"""


class ParseMythScriptTests(unittest.TestCase):
    def test_returns_correct_section_count(self) -> None:
        sections = parse_myth_script(SAMPLE_SCRIPT)
        self.assertEqual(len(sections), 4)

    def test_query_correct(self) -> None:
        sections = parse_myth_script(SAMPLE_SCRIPT)
        self.assertEqual(sections[0]["query"], "stressed man reading court letter")
        self.assertEqual(sections[2]["query"], "judge courtroom signing")

    def test_text_contains_spoken_words(self) -> None:
        sections = parse_myth_script(SAMPLE_SCRIPT)
        self.assertIn("МФО", sections[0]["text"])
        self.assertIn("Міф", sections[1]["text"])

    def test_no_bg_markers_in_text(self) -> None:
        sections = parse_myth_script(SAMPLE_SCRIPT)
        for section in sections:
            self.assertNotIn("##bg:", section["text"])

    def test_query_is_stripped(self) -> None:
        sections = parse_myth_script("##bg:  extra spaces  \ntext")
        self.assertEqual(sections[0]["query"], "extra spaces")

    def test_empty_script_returns_empty_list(self) -> None:
        self.assertEqual(parse_myth_script(""), [])

    def test_script_without_markers_returns_empty(self) -> None:
        self.assertEqual(parse_myth_script("just text\nmore text"), [])

    def test_section_without_text_not_emitted(self) -> None:
        # Two consecutive markers with no text between them
        script = "##bg: query one\n##bg: query two\nsome text"
        sections = parse_myth_script(script)
        self.assertEqual(len(sections), 1)
        self.assertEqual(sections[0]["query"], "query two")


class ExtractVoiceTextTests(unittest.TestCase):
    def test_removes_bg_lines(self) -> None:
        result = extract_voice_text(SAMPLE_SCRIPT)
        self.assertNotIn("##bg:", result)

    def test_keeps_spoken_content(self) -> None:
        result = extract_voice_text(SAMPLE_SCRIPT)
        self.assertIn("МФО", result)
        self.assertIn("Міф 1", result)
        self.assertIn("Реальність", result)

    def test_empty_script(self) -> None:
        self.assertEqual(extract_voice_text(""), "")

    def test_script_only_markers(self) -> None:
        script = "##bg: query one\n##bg: query two"
        self.assertEqual(extract_voice_text(script), "")


class MapSectionsToTimestampsTests(unittest.TestCase):
    def _sections(self) -> list[dict]:
        return parse_myth_script(SAMPLE_SCRIPT)

    def _words(self) -> list[AlignedWord]:
        return [
            AlignedWord(text="3",          start_seconds=0.0,  end_seconds=0.3),
            AlignedWord(text="речі",       start_seconds=0.3,  end_seconds=0.7),
            AlignedWord(text="МФО",        start_seconds=0.7,  end_seconds=1.1),
            AlignedWord(text="Міф",        start_seconds=5.0,  end_seconds=5.3),
            AlignedWord(text="1",          start_seconds=5.3,  end_seconds=5.5),
            AlignedWord(text="Це",         start_seconds=10.0, end_seconds=10.2),
            AlignedWord(text="неправда",   start_seconds=10.2, end_seconds=10.8),
            AlignedWord(text="Реальність", start_seconds=14.0, end_seconds=14.5),
        ]

    def test_section_count_preserved(self) -> None:
        result = map_sections_to_timestamps(self._sections(), self._words(), 20.0)
        self.assertEqual(len(result), 4)

    def test_start_times_non_decreasing(self) -> None:
        result = map_sections_to_timestamps(self._sections(), self._words(), 20.0)
        times = [s["start_time"] for s in result]
        self.assertEqual(times, sorted(times))

    def test_last_section_ends_at_total_duration(self) -> None:
        result = map_sections_to_timestamps(self._sections(), self._words(), 20.0)
        self.assertAlmostEqual(result[-1]["end_time"], 20.0)

    def test_section_end_equals_next_section_start(self) -> None:
        result = map_sections_to_timestamps(self._sections(), self._words(), 20.0)
        for i in range(len(result) - 1):
            self.assertAlmostEqual(result[i]["end_time"], result[i + 1]["start_time"])

    def test_first_section_starts_at_first_word(self) -> None:
        result = map_sections_to_timestamps(self._sections(), self._words(), 20.0)
        self.assertAlmostEqual(result[0]["start_time"], 0.0)

    def test_query_preserved_in_result(self) -> None:
        result = map_sections_to_timestamps(self._sections(), self._words(), 20.0)
        self.assertEqual(result[0]["query"], "stressed man reading court letter")

    def test_missing_word_falls_back_to_previous_end(self) -> None:
        # Word "Реальність" absent — section 4 should start at section 3's end_time
        words_without_last = self._words()[:-1]
        result = map_sections_to_timestamps(self._sections(), words_without_last, 20.0)
        self.assertAlmostEqual(result[3]["start_time"], result[2]["end_time"])

    def test_empty_sections_returns_empty(self) -> None:
        result = map_sections_to_timestamps([], self._words(), 20.0)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
