#!/usr/bin/env python3
"""Integration regressions for AI provider-chain behavior."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app as app_module


class ProviderChainIntegrationTests(unittest.TestCase):
    @patch.object(app_module, "load_secrets")
    @patch.object(app_module, "load_config")
    def test_settings_html_never_contains_saved_secrets(self, mock_config, mock_secrets):
        secret_values = {
            "gemini_api_key": "gemini-render-secret",
            "openrouter_api_key": "openrouter-render-secret",
            "openai_compatible_api_key": "compatible-render-secret",
            "google_books_api_key": "google-render-secret",
            "bookdb_api_key": "skaldleita-render-secret",
        }
        mock_config.return_value = {**app_module.DEFAULT_CONFIG, **secret_values}
        mock_secrets.return_value = secret_values

        response = app_module.app.test_client().get("/settings")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        for value in secret_values.values():
            self.assertNotIn(value, html)
        self.assertEqual(html.count('data-has-key="true"'), len(secret_values))

    @patch.object(app_module, "load_secrets", return_value={})
    @patch.object(app_module, "call_ollama")
    def test_batch_schema_reaches_ollama(self, mock_ollama, _mock_secrets):
        mock_ollama.return_value = [
            {"item": "ITEM_1", "title": "Dune"},
            {"item": "ITEM_2", "title": "The Hobbit"},
        ]
        schema = {
            "type": "array",
            "items": {"type": "object"},
            "minItems": 2,
            "maxItems": 2,
        }

        result = app_module.call_text_provider_chain(
            "identify two books",
            {"text_provider_chain": ["ollama"]},
            response_schema=schema,
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(mock_ollama.call_args.kwargs["response_schema"], schema)

    @patch.object(app_module, "load_secrets", return_value={})
    @patch.object(app_module, "call_openai_compatible")
    def test_primary_provider_is_used_even_when_missing_from_legacy_chain(
        self,
        mock_compatible,
        _mock_secrets,
    ):
        mock_compatible.return_value = {"title": "Dune", "author": "Frank Herbert"}

        result = app_module.call_text_provider_chain("identify", {
            "ai_provider": "openai_compatible",
            "text_provider_chain": ["gemini", "openrouter"],
        })

        self.assertEqual(result["title"], "Dune")
        mock_compatible.assert_called_once()

    @patch.object(app_module, "_verify_and_correct_narrator", side_effect=lambda _path, result, _config: result)
    @patch.object(app_module, "_contribute_fingerprint_async")
    @patch.object(app_module, "transcribe_audio_local")
    @patch.object(app_module, "call_openai_compatible")
    @patch.object(app_module, "identify_audio_with_bookdb")
    @patch.object(app_module, "load_secrets", return_value={})
    def test_skaldleita_transcript_flows_to_configured_ai_fallback(
        self,
        _mock_secrets,
        mock_skaldleita,
        mock_compatible,
        mock_local_transcription,
        _mock_contribute,
        _mock_narrator,
    ):
        mock_skaldleita.return_value = {"transcript": "This is Dune by Frank Herbert"}
        mock_compatible.return_value = {"title": "Dune", "author": "Frank Herbert"}

        result = app_module.call_audio_provider_chain("sample.mp3", {
            "audio_provider_chain": ["bookdb", "openai_compatible"],
            "enable_fingerprinting": False,
        })

        self.assertEqual(result["title"], "Dune")
        mock_compatible.assert_called_once()
        self.assertIn("This is Dune by Frank Herbert", mock_compatible.call_args.args[0])
        mock_local_transcription.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
