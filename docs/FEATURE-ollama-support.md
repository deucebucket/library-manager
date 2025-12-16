# Feature: Ollama Self-Hosted AI Support

**Discussion:** #13
**Status:** Planned
**Requested by:** Community

## Problem

Users want to:
1. Avoid sending book titles to cloud AI services (privacy for "spicy" books)
2. Use their existing Ollama setup instead of paying for API calls
3. Keep everything self-hosted

## Solution

Add Ollama as an AI provider option alongside Gemini and OpenRouter.

## Implementation Plan

### 1. Config Changes

Add to DEFAULT_CONFIG:
```python
'ai_provider': 'gemini',  # Options: gemini, openrouter, ollama
'ollama_url': 'http://localhost:11434',
'ollama_model': 'llama3.2:3b',  # Good balance of speed/quality for 12GB GPU
```

### 2. Settings UI

Add "Ollama" option to AI Provider dropdown in Settings > AI Configuration:
- Ollama URL field (default: `http://localhost:11434`)
- Model selector (fetch available models from `/api/tags`)
- Connection test button

### 3. API Integration

Ollama uses OpenAI-compatible API:
```python
def call_ollama(prompt, model='llama3.2:3b'):
    response = requests.post(
        f"{ollama_url}/api/generate",
        json={
            "model": model,
            "prompt": prompt,
            "stream": False
        }
    )
    return response.json()['response']
```

### 4. Model Recommendations

For 12GB VRAM (RTX 3060):
- `llama3.2:3b` - Fast, good for simple book identification
- `mistral:7b` - Better reasoning, slower
- `gemma2:9b` - Google's model, good balance

For smaller VRAM (8GB):
- `llama3.2:1b`
- `phi3:mini`

### 5. Prompt Adjustments

May need to simplify prompts for smaller models:
- Current Gemini prompts are quite detailed
- Local models may need more explicit JSON formatting instructions
- Add JSON mode if model supports it

## Files to Modify

- `app.py`: Add Ollama provider, config options
- `templates/settings.html`: Add Ollama config UI
- `static/settings.js`: Handle new settings
- `CHANGELOG.md`: Document feature
- `README.md`: Document Ollama setup

## Docker Considerations

Users running Library Manager in Docker need to access host Ollama:
- Use `host.docker.internal:11434` on Docker Desktop
- Use `--add-host=host.docker.internal:host-gateway` on Linux

## Testing

- Test with llama3.2:3b (common setup)
- Test JSON parsing from Ollama responses
- Test connection failure handling
- Test Docker-to-host connectivity
