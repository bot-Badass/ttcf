from __future__ import annotations

import unittest

from src.translator import (
    TranslationError,
    TranslationUsage,
    _split_text_into_chunks,
    estimate_characters,
    translate_text_deepl,
    translate_texts_deepl,
)


class TranslatorTests(unittest.TestCase):
    def test_batch_translation(self) -> None:
        captured_calls: list[tuple[list[str], dict[str, object]]] = []

        translations = translate_texts_deepl(
            texts=("Hello", "World"),
            api_key="deepl-key",
            client_factory=lambda api_key, server_url: self._fake_client(
                on_translate=lambda texts, kwargs: self._record_and_translate(
                    captured_calls,
                    texts,
                    kwargs,
                )
            ),
        )

        self.assertEqual(translations, ["UA::Hello", "UA::World"])
        self.assertEqual(len(captured_calls), 1)
        self.assertEqual(captured_calls[0][0], ["Hello", "World"])

    def test_usage_tracking(self) -> None:
        usage = TranslationUsage()

        translated = translate_text_deepl(
            text="Hello, world!",
            api_key="deepl-key",
            usage=usage,
            client_factory=lambda api_key, server_url: self._fake_client(),
        )

        self.assertEqual(translated, "UA::Hello, world!")
        self.assertEqual(usage.characters_used, estimate_characters("Hello, world!"))

    def test_retry_logic(self) -> None:
        sleep_calls: list[float] = []
        attempts = {"count": 0}

        def flaky_translate(texts: list[str], kwargs: dict[str, object]):
            del kwargs
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise self._http_error(429)
            return [self._result(f"UA::{text}") for text in texts]

        translations = translate_texts_deepl(
            texts=("Hello",),
            api_key="deepl-key",
            retry_base_delay_seconds=0.25,
            client_factory=lambda api_key, server_url: self._fake_client(
                on_translate=flaky_translate
            ),
            sleep_func=sleep_calls.append,
        )

        self.assertEqual(translations, ["UA::Hello"])
        self.assertEqual(attempts["count"], 2)
        self.assertEqual(sleep_calls, [0.25])

    def test_large_text_split(self) -> None:
        call_batches: list[list[str]] = []
        long_text = "A" * 25

        translated = translate_text_deepl(
            text=long_text,
            api_key="deepl-key",
            max_chars_per_request=10,
            client_factory=lambda api_key, server_url: self._fake_client(
                on_translate=lambda texts, kwargs: self._record_and_uppercase(
                    call_batches,
                    texts,
                    kwargs,
                )
            ),
        )

        self.assertEqual(translated, long_text.upper())
        self.assertEqual(call_batches, [["AAAAAAAAAA"], ["AAAAAAAAAA"], ["AAAAA"]])

    def test_auto_detect_language(self) -> None:
        captured_kwargs: list[dict[str, object]] = []

        translate_text_deepl(
            text="Hello",
            api_key="deepl-key",
            client_factory=lambda api_key, server_url: self._fake_client(
                on_translate=lambda texts, kwargs: self._record_kwargs(
                    captured_kwargs,
                    texts,
                    kwargs,
                )
            ),
        )

        self.assertEqual(len(captured_kwargs), 1)
        self.assertEqual(captured_kwargs[0]["target_lang"], "UK")
        self.assertNotIn("source_lang", captured_kwargs[0])

    def test_translate_text_deepl_fails_when_response_is_empty(self) -> None:
        with self.assertRaises(TranslationError) as context:
            translate_text_deepl(
                text="Hello, world!",
                api_key="deepl-key",
                client_factory=lambda api_key, server_url: self._fake_client(
                    on_translate=lambda texts, kwargs: [self._result("   ") for _ in texts]
                ),
            )

        self.assertEqual(str(context.exception), "DeepL translation response was empty.")

    def test_translate_text_deepl_fails_on_invalid_response(self) -> None:
        with self.assertRaises(TranslationError) as context:
            translate_text_deepl(
                text="Hello, world!",
                api_key="deepl-key",
                client_factory=lambda api_key, server_url: self._fake_client(
                    on_translate=lambda texts, kwargs: []
                ),
            )

        self.assertEqual(str(context.exception), "Unexpected DeepL translation response.")

    def test_translate_text_deepl_fails_when_api_key_missing(self) -> None:
        with self.assertRaises(TranslationError) as context:
            translate_text_deepl(
                text="Hello, world!",
                api_key=None,
            )

        self.assertEqual(str(context.exception), "DeepL API key is not configured.")

    def test_split_text_into_chunks_with_no_space_long_input_does_not_raise(self) -> None:
        long_text = "A" * 25

        chunks = _split_text_into_chunks(long_text, 10)

        self.assertEqual(chunks, ["AAAAAAAAAA", "AAAAAAAAAA", "AAAAA"])

    def test_split_text_into_chunks_preserves_unicode_characters(self) -> None:
        long_text = "ПривітСвіт🙂ПривітСвіт🙂"

        chunks = _split_text_into_chunks(long_text, 7)

        self.assertEqual("".join(chunks), long_text)
        self.assertTrue(all(isinstance(chunk, str) for chunk in chunks))

    def _record_and_translate(
        self,
        captured_calls: list[tuple[list[str], dict[str, object]]],
        texts: list[str],
        kwargs: dict[str, object],
    ) -> list[object]:
        captured_calls.append((list(texts), dict(kwargs)))
        return [self._result(f"UA::{text}") for text in texts]

    def _record_and_uppercase(
        self,
        call_batches: list[list[str]],
        texts: list[str],
        kwargs: dict[str, object],
    ) -> list[object]:
        del kwargs
        call_batches.append(list(texts))
        return [self._result(text.upper()) for text in texts]

    def _record_kwargs(
        self,
        captured_kwargs: list[dict[str, object]],
        texts: list[str],
        kwargs: dict[str, object],
    ) -> list[object]:
        del texts
        captured_kwargs.append(dict(kwargs))
        return [self._result("UA::Hello")]

    def _fake_client(self, on_translate=None):
        handler = on_translate or (
            lambda texts, kwargs: [self._result(f"UA::{text}") for text in texts]
        )

        class FakeClient:
            def translate_text(self, texts, **kwargs):
                return handler(list(texts), kwargs)

        return FakeClient()

    def _result(self, text: str):
        class FakeResult:
            def __init__(self, text: str) -> None:
                self.text = text

        return FakeResult(text)

    def _http_error(self, status_code: int):
        class FakeHttpError(Exception):
            def __init__(self, status_code: int) -> None:
                super().__init__(f"http {status_code}")
                self.status_code = status_code

        return FakeHttpError(status_code)


if __name__ == "__main__":
    unittest.main()
