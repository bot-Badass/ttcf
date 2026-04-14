from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import config
from src.pexels_client import PexelsError, download_background_video


def _make_response(videos: list) -> bytes:
    return json.dumps({"videos": videos}).encode("utf-8")


def _make_video_entry(duration: int, width: int, height: int, quality: str = "hd") -> dict:
    return {
        "duration": duration,
        "video_files": [
            {
                "quality": quality,
                "link": f"https://pexels.example/video_{duration}.mp4",
                "width": width,
                "height": height,
            }
        ],
    }


class PexelsClientTests(unittest.TestCase):
    def test_download_background_video_raises_when_api_key_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "bg.mp4"
            with patch.object(config, "PEXELS_API_KEY", None):
                with self.assertRaises(PexelsError) as ctx:
                    download_background_video(output_path, api_key=None)

        self.assertIn("PEXELS_API_KEY", str(ctx.exception))

    def test_download_background_video_raises_when_no_suitable_video(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "bg.mp4"

            def fake_http_get(url, headers):
                return _make_response([])

            with self.assertRaises(PexelsError) as ctx:
                download_background_video(
                    output_path,
                    api_key="test-key",
                    http_get=fake_http_get,
                    http_download=lambda url: b"",
                )

        self.assertIn("No suitable", str(ctx.exception))

    def test_download_background_video_writes_video_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "bg.mp4"

            def fake_http_get(url, headers):
                return _make_response([_make_video_entry(duration=30, width=720, height=1280)])

            def fake_http_download(url: str) -> bytes:
                return b"video-bytes"

            download_background_video(
                output_path,
                api_key="test-key",
                http_get=fake_http_get,
                http_download=fake_http_download,
            )

            self.assertEqual(output_path.read_bytes(), b"video-bytes")

    def test_download_background_video_filters_by_min_duration(self) -> None:
        selected_urls: list[str] = []

        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "bg.mp4"

            videos = [
                _make_video_entry(duration=5, width=720, height=1280),
                _make_video_entry(duration=30, width=720, height=1280),
            ]

            def fake_http_get(url, headers):
                return _make_response(videos)

            def fake_http_download(url: str) -> bytes:
                selected_urls.append(url)
                return b"video-data"

            download_background_video(
                output_path,
                min_duration=20,
                api_key="test-key",
                http_get=fake_http_get,
                http_download=fake_http_download,
            )

        self.assertEqual(len(selected_urls), 1)
        self.assertIn("30", selected_urls[0])


if __name__ == "__main__":
    unittest.main()
