from __future__ import annotations

import unittest
from unittest.mock import patch

from src import config


class ConfigTests(unittest.TestCase):
    def test_missing_required_env_raises(self) -> None:
        with (
            patch.object(config, "TELEGRAM_BOT_TOKEN", None),
            patch.object(config, "TELEGRAM_CHAT_ID", "chat-id"),
        ):
            with self.assertRaises(RuntimeError) as context:
                config.validate_runtime_config("telegram")

        self.assertEqual(
            str(context.exception),
            "Missing required runtime environment variables: TELEGRAM_BOT_TOKEN",
        )

    def test_valid_env_passes(self) -> None:
        with (
            patch.object(config, "TELEGRAM_BOT_TOKEN", "bot-token"),
            patch.object(config, "TELEGRAM_CHAT_ID", "chat-id"),
            patch.object(config, "RESPEECHER_API_KEY", "respeecher-key"),
            patch.object(config, "DEEPL_API_KEY", "deepl-key"),
        ):
            config.validate_runtime_config("telegram", "tts", "translation")

    def test_optional_env_is_allowed(self) -> None:
        with (
            patch.object(config, "RESPEECHER_API_KEY", "respeecher-key"),
            patch.object(config, "RESPEECHER_DEFAULT_VOICE_ID", None),
        ):
            config.validate_runtime_config("tts")

    def test_unknown_runtime_group_fails_clearly(self) -> None:
        with self.assertRaises(RuntimeError) as context:
            config.validate_runtime_config("unknown-group")

        self.assertEqual(
            str(context.exception),
            "Unknown runtime config group: unknown-group",
        )

    def test_multiple_groups_deduplicate_missing_variable_reporting(self) -> None:
        with (
            patch.object(config, "TELEGRAM_BOT_TOKEN", None),
            patch.object(config, "TELEGRAM_CHAT_ID", "chat-id"),
        ):
            with self.assertRaises(RuntimeError) as context:
                config.validate_runtime_config("telegram", "telegram")

        self.assertEqual(
            str(context.exception),
            "Missing required runtime environment variables: TELEGRAM_BOT_TOKEN",
        )

    def test_blank_string_env_values_are_treated_as_missing(self) -> None:
        with (
            patch.object(config, "DEEPL_API_KEY", "   "),
        ):
            with self.assertRaises(RuntimeError) as context:
                config.validate_runtime_config("translation")

        self.assertEqual(
            str(context.exception),
            "Missing required runtime environment variables: DEEPL_API_KEY",
        )

    def test_render_env_surface_does_not_create_fake_runtime_requirements(self) -> None:
        with (
            patch.object(config, "RESPEECHER_API_KEY", "respeecher-key"),
            patch.object(config, "FFMPEG_PATH", ""),
            patch.object(config, "FFPROBE_BIN", ""),
            patch.object(config, "FFPROBE_TIMEOUT_SECONDS", 0),
            patch.object(config, "FFMPEG_TIMEOUT_SECONDS", 0),
        ):
            config.validate_runtime_config("tts")


if __name__ == "__main__":
    unittest.main()
