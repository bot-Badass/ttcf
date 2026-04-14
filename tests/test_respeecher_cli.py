from __future__ import annotations

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

import respeecher_cli


class RespeecherCliTests(unittest.TestCase):
    def test_voices_command_shows_available_voices(self) -> None:
        with (
            patch("respeecher_cli.config.RESPEECHER_API_KEY", "respeecher-key"),
            patch(
                "respeecher_cli.list_available_respeecher_voices",
                return_value=(
                    {"id": "voice-001", "name": "Voice One"},
                    {"id": "voice-002", "name": "Voice Two"},
                ),
            ),
        ):
            exit_code, stdout, stderr = self._run_cli("voices")

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("Checking Respeecher voices via SDK environment: PUBLIC_UA_RT", stdout)
        self.assertIn("Success: fetched 2 voice(s)", stdout)
        self.assertIn("voice_id=voice-001 | name=Voice One", stdout)
        self.assertIn("voice_id=voice-002 | name=Voice Two", stdout)

    def test_voices_command_fails_clearly_when_api_key_missing(self) -> None:
        with patch("respeecher_cli.config.RESPEECHER_API_KEY", None):
            exit_code, stdout, stderr = self._run_cli("voices")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout, "")
        self.assertIn(
            "Missing required runtime environment variables: RESPEECHER_API_KEY",
            stderr,
        )

    def test_voices_command_surfaces_reddit_intake_error(self) -> None:
        with (
            patch("respeecher_cli.config.RESPEECHER_API_KEY", "respeecher-key"),
            patch(
                "respeecher_cli.list_available_respeecher_voices",
                side_effect=respeecher_cli.RedditIntakeError(
                    'Respeecher voices request failed with HTTP 403: {"detail":"forbidden"}'
                ),
            ),
        ):
            exit_code, stdout, stderr = self._run_cli("voices")

        self.assertEqual(exit_code, 1)
        self.assertIn("Checking Respeecher voices via SDK environment: PUBLIC_UA_RT", stdout)
        self.assertIn(
            'Respeecher voices request failed with HTTP 403: {"detail":"forbidden"}',
            stderr,
        )

    def _run_cli(self, *args: str) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = respeecher_cli.main(args)
        return exit_code, stdout.getvalue(), stderr.getvalue()


if __name__ == "__main__":
    unittest.main()
