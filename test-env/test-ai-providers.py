#!/usr/bin/env python3
"""Focused regression tests for self-hosted AI providers."""

import sys
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from library_manager.providers.ollama import call_ollama, get_ollama_models
from library_manager import config as config_module
from library_manager.providers.openai_compatible import (
    call_openai_compatible,
    get_openai_compatible_models,
    normalize_openai_compatible_url,
)


def response(status_code=200, payload=None):
    result = Mock()
    result.status_code = status_code
    result.json.return_value = payload or {}
    return result


class OpenAICompatibleProviderTests(unittest.TestCase):
    def test_normalizes_root_and_endpoint_urls(self):
        self.assertEqual(
            normalize_openai_compatible_url("http://llama:8080"),
            "http://llama:8080/v1",
        )
        self.assertEqual(
            normalize_openai_compatible_url("http://llama:8080/v1/chat/completions"),
            "http://llama:8080/v1",
        )

    @patch("library_manager.providers.openai_compatible.requests.get")
    def test_discovers_models_without_forcing_auth(self, mock_get):
        mock_get.return_value = response(payload={
            "data": [{"id": "local-a"}, {"id": "local-b"}],
        })

        models = get_openai_compatible_models({
            "openai_compatible_url": "http://llama:8080",
        })

        self.assertEqual(models, ["local-a", "local-b"])
        self.assertEqual(mock_get.call_args.args[0], "http://llama:8080/v1/models")
        self.assertNotIn("Authorization", mock_get.call_args.kwargs["headers"])

    @patch("library_manager.providers.openai_compatible.requests.post")
    def test_uses_configured_model_and_optional_key(self, mock_post):
        mock_post.return_value = response(payload={
            "choices": [{"message": {"content": '{"title":"Dune"}'}}],
        })

        result = call_openai_compatible(
            "identify",
            {
                "openai_compatible_url": "http://llama:8080/v1",
                "openai_compatible_model": "my-model",
                "openai_compatible_api_key": "secret",
            },
            parse_json_fn=lambda value: value,
        )

        self.assertEqual(result, '{"title":"Dune"}')
        request = mock_post.call_args
        self.assertEqual(request.args[0], "http://llama:8080/v1/chat/completions")
        self.assertEqual(request.kwargs["json"]["model"], "my-model")
        self.assertEqual(request.kwargs["json"]["max_tokens"], 2048)
        self.assertEqual(request.kwargs["headers"]["Authorization"], "Bearer secret")

    @patch("library_manager.providers.openai_compatible.requests.post")
    @patch("library_manager.providers.openai_compatible.requests.get")
    def test_auto_selects_the_only_discovered_model(self, mock_get, mock_post):
        mock_get.return_value = response(payload={"data": [{"id": "only-model"}]})
        mock_post.return_value = response(payload={
            "choices": [{"message": {"content": "ok"}}],
        })

        result = call_openai_compatible("identify", {
            "openai_compatible_url": "http://llama:8080/v1",
            "openai_compatible_model": "",
        })

        self.assertEqual(result, {"raw_response": "ok"})
        self.assertEqual(mock_post.call_args.kwargs["json"]["model"], "only-model")

    @patch("library_manager.providers.openai_compatible.requests.post")
    @patch("library_manager.providers.openai_compatible.requests.get")
    def test_does_not_choose_arbitrarily_from_multiple_models(self, mock_get, mock_post):
        mock_get.return_value = response(payload={
            "data": [{"id": "first"}, {"id": "second"}],
        })

        result = call_openai_compatible("identify", {
            "openai_compatible_url": "http://llama:8080/v1",
            "openai_compatible_model": "",
        })

        self.assertIsNone(result)
        mock_post.assert_not_called()


class OllamaProviderTests(unittest.TestCase):
    @patch("library_manager.providers.ollama.requests.get")
    def test_accepts_name_model_and_string_model_shapes(self, mock_get):
        mock_get.return_value = response(payload={
            "models": [
                {"name": "named:latest"},
                {"model": "model-field:latest"},
                "string-model:latest",
                {"name": "embedding-only", "capabilities": ["embedding"]},
                {"name": None},
            ],
        })

        models = get_ollama_models({"ollama_url": "http://ollama:11434/"})

        self.assertEqual(models, [
            "named:latest",
            "model-field:latest",
            "string-model:latest",
        ])

    @patch("library_manager.providers.ollama.requests.post")
    @patch("library_manager.providers.ollama.requests.get")
    def test_missing_model_never_falls_back_to_a_hardcoded_name(self, mock_get, mock_post):
        mock_get.return_value = response(payload={
            "models": [{"name": "first"}, {"name": "second"}],
        })

        result = call_ollama("identify", {
            "ollama_url": "http://ollama:11434",
            "ollama_model": "",
        })

        self.assertIsNone(result)
        mock_post.assert_not_called()

    @patch("library_manager.providers.ollama.requests.post")
    def test_generation_uses_json_mode_and_a_token_bound(self, mock_post):
        mock_post.return_value = response(payload={"response": '{"title":"Dune"}'})

        result = call_ollama("identify", {
            "ollama_url": "http://ollama:11434",
            "ollama_model": "local-model",
        })

        self.assertEqual(result, {"raw_response": '{"title":"Dune"}'})
        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["format"], "json")
        self.assertEqual(payload["options"]["num_predict"], 2048)

    @patch("library_manager.providers.ollama.requests.post")
    def test_generation_accepts_an_explicit_array_schema(self, mock_post):
        mock_post.return_value = response(payload={"response": '[]'})
        schema = {
            "type": "array",
            "items": {"type": "object"},
            "minItems": 2,
            "maxItems": 2,
        }

        result = call_ollama(
            "identify two books",
            {
                "ollama_url": "http://ollama:11434",
                "ollama_model": "local-model",
            },
            response_schema=schema,
        )

        self.assertEqual(result, {"raw_response": "[]"})
        self.assertEqual(mock_post.call_args.kwargs["json"]["format"], schema)


class ConfigSecretTests(unittest.TestCase):
    def test_provider_secrets_never_write_to_config_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            with patch.object(config_module, "CONFIG_PATH", config_path):
                config_module.save_config({
                    "ai_provider": "openai_compatible",
                    "openai_compatible_api_key": "local-secret",
                    "bookdb_api_key": "skaldleita-secret",
                    "gemini_api_key": "gemini-secret",
                    "openrouter_api_key": "openrouter-secret",
                })

            saved = json.loads(config_path.read_text())
            self.assertEqual(saved, {"ai_provider": "openai_compatible"})

    def test_secrets_file_is_owner_only_after_every_save(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = Path(temp_dir) / "secrets.json"
            secrets_path.write_text('{"old":"value"}')
            secrets_path.chmod(0o664)

            with patch.object(config_module, "SECRETS_PATH", secrets_path):
                config_module.save_secrets({"api_key": "secret"})

            saved = json.loads(secrets_path.read_text())
            mode = os.stat(secrets_path).st_mode & 0o777
            self.assertEqual(saved, {"api_key": "secret"})
            self.assertEqual(mode, 0o600)


if __name__ == "__main__":
    unittest.main(verbosity=2)
