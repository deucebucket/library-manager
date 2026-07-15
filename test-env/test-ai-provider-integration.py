#!/usr/bin/env python3
"""Integration regressions for AI provider-chain behavior."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app as app_module


class ProviderChainIntegrationTests(unittest.TestCase):
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
