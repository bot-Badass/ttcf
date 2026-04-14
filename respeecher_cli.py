from __future__ import annotations

import argparse
import sys
from typing import Sequence

from src import config
from src.reddit_intake import RedditIntakeError, list_available_respeecher_voices


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        config.validate_runtime_config("tts")
        return args.command(args)
    except (RedditIntakeError, RuntimeError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Respeecher diagnostics.")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    voices_parser = subparsers.add_parser(
        "voices",
        help="Verify access to the Respeecher voices endpoint and print available voices.",
    )
    voices_parser.set_defaults(command=_voices_command)

    return parser


def _voices_command(args: argparse.Namespace) -> int:
    del args
    print("Checking Respeecher voices via SDK environment: PUBLIC_UA_RT")
    voices = list_available_respeecher_voices()
    print(f"Success: fetched {len(voices)} voice(s)")

    if not voices:
        print("No voices returned.")
        return 0

    for voice in voices:
        voice_id = voice.get("id", "<missing id>")
        voice_name = voice.get("name", "<missing name>")
        print(f"voice_id={voice_id} | name={voice_name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
