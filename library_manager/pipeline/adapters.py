"""Layer adapters -- uniform interface wrappers around existing layer functions.

Each adapter wraps one battle-tested layer function with a consistent interface
so the PipelineOrchestrator can run them generically. The adapters do NOT
reimplement any processing logic -- they translate between the orchestrator's
uniform call convention and each layer's specific function signature.

Adapter Pattern:
    Orchestrator -> adapter.run(config, deps) -> existing layer function -> (processed, resolved)
"""

import logging
from typing import Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class LayerAdapter:
    """Base adapter interface for pipeline layers.

    Every layer adapter must define a ``layer_id`` matching the registry
    and implement ``run()`` which delegates to the real layer function.

    Attributes:
        layer_id: Must match a registered LayerInfo.layer_id in the registry.
    """

    layer_id: str = ""

    def run(self, config: Dict, deps: Dict) -> Tuple[int, int]:
        """Run one batch cycle of this layer.

        Args:
            config: Current application configuration dict.
            deps: Dictionary of injected dependencies (get_db, load_config,
                  is_circuit_open, etc.). Each adapter picks what it needs.

        Returns:
            Tuple of (processed_count, resolved_count).
        """
        raise NotImplementedError(f"{self.__class__.__name__}.run() not implemented")

    def __repr__(self):
        return f"<{self.__class__.__name__} layer_id={self.layer_id!r}>"


class AudioIdAdapter(LayerAdapter):
    """Wraps process_layer_1_audio -- audio transcription + AI parsing.

    This is the primary identification method. It transcribes audiobook
    intros via Skaldleita/Whisper and identifies from narrator announcements.

    Dependencies required in deps dict:
        - process_layer_1_audio: The app.py wrapper that already has
          all app-level deps injected (get_db, identify_audio_with_bookdb, etc.)
    """

    layer_id = "audio_id"

    def run(self, config: Dict, deps: Dict) -> Tuple[int, int]:
        """Run one batch of audio identification.

        Delegates to the app-level process_layer_1_audio wrapper which
        already injects get_db, identify_audio_with_bookdb, transcribe_audio_intro,
        parse_transcript_with_ai, is_circuit_open, get_circuit_breaker,
        load_config, build_new_path, update_processing_status, and set_current_book.
        """
        process_fn = deps.get('process_layer_1_audio')
        if not process_fn:
            logger.warning(f"[{self.layer_id}] No process function provided, skipping")
            return 0, 0
        return process_fn(config)


class ApiLookupAdapter(LayerAdapter):
    """Wraps process_layer_1_api -- API database lookups.

    Queries Skaldleita, Audnexus, OpenLibrary, and Google Books to enrich
    book metadata. Faster and cheaper than AI verification.

    Dependencies required in deps dict:
        - process_layer_1_api: The app.py wrapper with get_db,
          gather_all_api_candidates, and set_current_book injected.
    """

    layer_id = "api_lookup"

    def run(self, config: Dict, deps: Dict) -> Tuple[int, int]:
        """Run one batch of API lookups."""
        process_fn = deps.get('process_layer_1_api')
        if not process_fn:
            logger.warning(f"[{self.layer_id}] No process function provided, skipping")
            return 0, 0
        return process_fn(config)


class AudioCreditsAdapter(LayerAdapter):
    """Wraps process_layer_3_audio -- AI audio clip analysis.

    Sends longer audio samples to Gemini AI for deeper analysis when
    transcription was unclear. This is an expensive layer.

    Dependencies required in deps dict:
        - process_layer_3_audio: The app.py wrapper with get_db,
          find_audio_files, analyze_audio_for_credits, auto_save_narrator,
          contribute_audio_extraction, and standardize_initials injected.
    """

    layer_id = "audio_credits"

    def run(self, config: Dict, deps: Dict) -> Tuple[int, int]:
        """Run one batch of audio credits analysis.

        Passes verification_layer=2 to process Layer 2 items (unclear L1 results).
        The underlying function checks the enable_audio_analysis config key.
        """
        process_fn = deps.get('process_layer_3_audio')
        if not process_fn:
            logger.warning(f"[{self.layer_id}] No process function provided, skipping")
            return 0, 0
        # audio_credits processes items at verification_layer=2
        return process_fn(config, verification_layer=2)


class AiVerifyAdapter(LayerAdapter):
    """Wraps process_queue -- AI verification of folder-based guesses.

    Uses AI to verify identification as a last resort. Folder names CAN be
    wrong, so confidence is set LOW for folder-derived identifications.

    Dependencies required in deps dict:
        - process_queue: The app.py wrapper with get_db, check_rate_limit,
          call_ai, detect_multibook_vs_chapters, auto_save_narrator,
          standardize_initials, extract_series_from_title, is_placeholder_author,
          build_new_path, is_drastic_author_change, verify_drastic_change,
          analyze_audio_for_credits, compare_book_folders, sanitize_path_component,
          extract_narrator_from_folder, build_metadata_for_embedding,
          embed_tags_for_path, BookProfile, audio_extensions, and set_current_book
          injected.
    """

    layer_id = "ai_verify"

    def run(self, config: Dict, deps: Dict) -> Tuple[int, int]:
        """Run one batch of AI verification.

        Passes verification_layer=4 because at this point in the pipeline,
        we're trusting folder names as a last resort.
        """
        process_fn = deps.get('process_queue')
        if not process_fn:
            logger.warning(f"[{self.layer_id}] No process function provided, skipping")
            return 0, 0
        return process_fn(config, verification_layer=4)


class SlRequeueAdapter(LayerAdapter):
    """Wraps process_sl_requeue_verification -- re-verify after nightly merge.

    Books with sl_requeue set had partial ID from Skaldleita. After the
    nightly database merge, we re-check to see if they're now fully identified.

    Dependencies required in deps dict:
        - process_sl_requeue_verification: The app.py wrapper with get_db
          and search_bookdb injected.
    """

    layer_id = "sl_requeue"

    def run(self, config: Dict, deps: Dict) -> Tuple[int, int]:
        """Run SL requeue verification (single pass, not a batch loop)."""
        process_fn = deps.get('process_sl_requeue_verification')
        if not process_fn:
            logger.debug(f"[{self.layer_id}] No process function provided, skipping")
            return 0, 0
        return process_fn(config)


def build_default_adapters():
    """Build the default set of layer adapters.

    Returns:
        List of LayerAdapter instances for all known layers.
    """
    return [
        AudioIdAdapter(),
        AudioCreditsAdapter(),
        SlRequeueAdapter(),
        ApiLookupAdapter(),
        AiVerifyAdapter(),
    ]


__all__ = [
    'LayerAdapter',
    'AudioIdAdapter',
    'ApiLookupAdapter',
    'AudioCreditsAdapter',
    'AiVerifyAdapter',
    'SlRequeueAdapter',
    'build_default_adapters',
]
