"""
Precog - Minority Report Consensus Voting System

Multiple sources vote on book identification. Majority wins, with source weighting.
Sources: Skaldleita (audio), metadata (ID3/JSON), APIs, AI, path parsing.

Named after the "precogs" in Minority Report - when all three agree, it's certain.
When they disagree, something needs human review.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

# Source weights - higher = more trusted
# Audio identification is most reliable (narrator literally says the title/author)
# Metadata from files is second (user may have tagged correctly)
# APIs are third (database lookups)
# AI is fourth (can hallucinate)
# Path parsing is least reliable (folders are often garbage)
SOURCE_WEIGHTS = {
    "skaldleita": 90,      # GPU Whisper + database match
    "skaldleita_audio": 85, # Audio transcription without DB match
    "metadata_id3": 80,     # ID3 tags from audio files
    "metadata_json": 75,    # metadata.json / .opf files
    "api_bookdb": 70,       # BookDB API lookup
    "api_audnexus": 68,     # Audnexus API
    "api_openlibrary": 65,  # OpenLibrary API
    "api_google": 62,       # Google Books API
    "api_hardcover": 60,    # Hardcover API
    "ai_gemini": 55,        # Gemini AI verification
    "ai_openrouter": 52,    # OpenRouter AI
    "ai_ollama": 50,        # Local Ollama
    "path_parsing": 30,     # Folder/filename parsing
}

# Minimum confidence thresholds
MIN_CONSENSUS_CONFIDENCE = 70  # Below this, flag for review
HIGH_CONFIDENCE_THRESHOLD = 85  # Above this, auto-accept
GENERIC_TITLE_THRESHOLD = 85   # Generic titles need higher consensus

# Generic titles that are prone to hallucination/mismatches
GENERIC_TITLES = {
    "match game", "the game", "game on", "end game", "final game",
    "the end", "the beginning", "new beginnings", "fresh start",
    "home", "coming home", "going home", "home again",
    "the choice", "choices", "decisions",
    "the list", "the plan", "the promise", "the secret",
    "forever", "always", "never", "maybe",
    "lost", "found", "broken", "fallen", "risen",
    "dark", "light", "shadow", "shadows",
    "fire", "ice", "storm", "rain",
    "book one", "book two", "book 1", "book 2",
    "part one", "part two", "part 1", "part 2",
    "chapter one", "chapter 1",
}


@dataclass
class SourceVote:
    """A vote from a single source."""
    source: str                    # Source identifier (e.g., "skaldleita", "api_bookdb")
    title: Optional[str] = None
    author: Optional[str] = None
    narrator: Optional[str] = None
    series: Optional[str] = None
    series_position: Optional[str] = None
    confidence: float = 0.0        # Source's own confidence (0-100)
    raw_data: Dict[str, Any] = field(default_factory=dict)  # Original response

    @property
    def weight(self) -> int:
        """Get the weight for this source."""
        return SOURCE_WEIGHTS.get(self.source, 40)

    @property
    def weighted_confidence(self) -> float:
        """Confidence adjusted by source weight."""
        return (self.confidence * self.weight) / 100


@dataclass
class ConsensusResult:
    """The result of consensus voting."""
    title: Optional[str] = None
    author: Optional[str] = None
    narrator: Optional[str] = None
    series: Optional[str] = None
    series_position: Optional[str] = None

    confidence: float = 0.0        # Overall consensus confidence (0-100)
    agreement_level: str = "none"  # "unanimous", "majority", "split", "none"
    needs_review: bool = False     # Flag for human review
    review_reason: Optional[str] = None

    winning_source: Optional[str] = None  # Which source "won"
    vote_breakdown: Dict[str, int] = field(default_factory=dict)  # How sources voted
    all_votes: List[SourceVote] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "title": self.title,
            "author": self.author,
            "narrator": self.narrator,
            "series": self.series,
            "series_position": self.series_position,
            "confidence": self.confidence,
            "agreement_level": self.agreement_level,
            "needs_review": self.needs_review,
            "review_reason": self.review_reason,
            "winning_source": self.winning_source,
            "vote_breakdown": self.vote_breakdown,
        }


class PrecogVoting:
    """
    Minority Report consensus voting system.

    Collects votes from multiple sources, weighs them, and determines consensus.
    When sources agree, we're confident. When they disagree, flag for review.
    """

    def __init__(self):
        self.votes: List[SourceVote] = []
        self._original_title: Optional[str] = None
        self._original_author: Optional[str] = None

    def set_original(self, title: Optional[str] = None, author: Optional[str] = None):
        """Set the original values for comparison (from path or existing metadata)."""
        self._original_title = title
        self._original_author = author

    def add_vote(self, vote: SourceVote):
        """Add a vote from a source."""
        if vote.source and (vote.title or vote.author):
            self.votes.append(vote)
            logger.debug(f"Precog: Added vote from {vote.source}: "
                        f"title='{vote.title}', author='{vote.author}', "
                        f"confidence={vote.confidence}, weight={vote.weight}")

    def add_vote_from_dict(self, source: str, data: Dict[str, Any], confidence: float = 70):
        """Convenience method to add a vote from a dictionary."""
        vote = SourceVote(
            source=source,
            title=data.get("title"),
            author=data.get("author"),
            narrator=data.get("narrator"),
            series=data.get("series"),
            series_position=data.get("series_position") or data.get("position"),
            confidence=confidence,
            raw_data=data,
        )
        self.add_vote(vote)

    def _normalize_text(self, text: Optional[str]) -> str:
        """Normalize text for comparison."""
        if not text:
            return ""
        # Lowercase, strip
        normalized = text.lower().strip()
        # Remove punctuation (dots, commas, etc.) - helps with "J.R.R." vs "JRR"
        normalized = re.sub(r'[^\w\s]', '', normalized)
        # Remove extra spaces
        normalized = " ".join(normalized.split())
        # Remove common prefixes/suffixes
        for prefix in ["the ", "a ", "an "]:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]
        return normalized

    def _expand_collapsed_initials(self, word: str) -> List[str]:
        """Expand collapsed initials like 'jrr' into ['j', 'r', 'r'].

        Only expands words that are 2-3 lowercase letters (post-normalization)
        and look like collapsed initials rather than short words.
        """
        # After normalization, "JRR" becomes "jrr". Only expand 2-3 char words
        # that are all letters (not real short words like "of", "an", "by").
        if len(word) in (2, 3) and word.isalpha():
            # Avoid expanding common short words
            common_short = {"of", "in", "on", "by", "to", "at", "or", "an", "is",
                            "it", "no", "so", "do", "my", "me", "we", "he", "if",
                            "up", "us", "am", "as", "be", "go", "ha", "hi", "ok",
                            "ox", "la", "le", "de", "du", "el", "al",
                            "the", "and", "for", "are", "but", "not", "you", "all",
                            "can", "had", "her", "was", "one", "our", "out", "has",
                            "his", "how", "its", "may", "new", "now", "old", "see",
                            "way", "who", "did", "get", "let", "say", "she", "too",
                            "use", "man", "day", "any", "few", "got", "him", "own",
                            "try", "run", "end", "far", "set", "big", "own", "put",
                            "red", "war", "van", "sir", "von", "mac", "don", "ben"}
            if word not in common_short:
                return list(word)
        return [word]

    def _is_initial_match(self, word1: str, word2: str) -> bool:
        """Check if one word is an initial of the other.

        Returns True if a single-character word matches the first letter of
        the other word (e.g., 'c' matches 'craig').
        """
        if len(word1) == 1 and len(word2) > 1:
            return word2.startswith(word1)
        if len(word2) == 1 and len(word1) > 1:
            return word1.startswith(word2)
        return False

    def _initial_aware_similarity(self, words1: List[str], words2: List[str]) -> float:
        """Calculate similarity between word lists with initial-aware matching.

        Single-letter initials match full words starting with that letter,
        weighted at 0.7 instead of 1.0 for exact matches. Collapsed initials
        like 'jrr' are expanded to ['j', 'r', 'r'] before comparison.
        """
        # Expand collapsed initials in both lists
        expanded1 = []
        for w in words1:
            expanded1.extend(self._expand_collapsed_initials(w))
        expanded2 = []
        for w in words2:
            expanded2.extend(self._expand_collapsed_initials(w))

        # Use the shorter list as the reference to match against the longer
        if len(expanded1) <= len(expanded2):
            shorter, longer = expanded1, expanded2
        else:
            shorter, longer = expanded2, expanded1

        score = 0.0
        used = set()  # Track which indices in longer list have been matched

        for sw in shorter:
            matched = False
            for i, lw in enumerate(longer):
                if i in used:
                    continue
                if sw == lw:
                    score += 1.0
                    used.add(i)
                    matched = True
                    break
                if self._is_initial_match(sw, lw):
                    score += 0.7
                    used.add(i)
                    matched = True
                    break
            # Unmatched words contribute nothing

        max_words = max(len(expanded1), len(expanded2))
        return score / max_words if max_words > 0 else 0

    def _texts_match(self, text1: Optional[str], text2: Optional[str], threshold: float = 0.8) -> bool:
        """Check if two texts match (fuzzy comparison)."""
        norm1 = self._normalize_text(text1)
        norm2 = self._normalize_text(text2)

        if not norm1 or not norm2:
            return False

        # Exact match after normalization
        if norm1 == norm2:
            return True

        # One contains the other
        if norm1 in norm2 or norm2 in norm1:
            return True

        # Word overlap check
        words1 = set(norm1.split())
        words2 = set(norm2.split())
        if not words1 or not words2:
            return False

        overlap = len(words1 & words2)
        max_words = max(len(words1), len(words2))
        similarity = overlap / max_words if max_words > 0 else 0

        if similarity >= threshold:
            return True

        # Initial-aware matching: handles "C Alanson" vs "Craig Alanson",
        # "JRR Tolkien" vs "J R R Tolkien", etc.
        initial_sim = self._initial_aware_similarity(norm1.split(), norm2.split())
        return initial_sim >= threshold

    def _is_generic_title(self, title: Optional[str]) -> bool:
        """Check if a title is generic and prone to mismatches."""
        if not title:
            return False
        normalized = self._normalize_text(title)
        # Only flag titles in our curated generic list
        # Don't use word count - catches legitimate short titles like "Dune", "IT", "The Road"
        return normalized in GENERIC_TITLES

    def _count_votes_for_field(self, field: str) -> Dict[str, float]:
        """
        Count weighted votes for a specific field.
        Returns dict of {value: weighted_vote_count}.
        """
        vote_counts: Dict[str, float] = defaultdict(float)

        for vote in self.votes:
            value = getattr(vote, field, None)
            if value:
                normalized = self._normalize_text(value)
                if normalized:
                    # Add weighted confidence
                    vote_counts[normalized] += vote.weighted_confidence

        return dict(vote_counts)

    def _find_consensus_value(self, field: str) -> tuple[Optional[str], float, str]:
        """
        Find the consensus value for a field.
        Returns (value, confidence, agreement_level).
        """
        vote_counts = self._count_votes_for_field(field)

        if not vote_counts:
            return None, 0, "none"

        # Sort by weighted votes
        sorted_votes = sorted(vote_counts.items(), key=lambda x: x[1], reverse=True)
        top_value, top_score = sorted_votes[0]

        # Calculate total votes
        total_votes = sum(vote_counts.values())

        # Determine agreement level
        if len(sorted_votes) == 1:
            agreement = "unanimous"
            confidence = min(100, top_score)
        elif len(sorted_votes) >= 2:
            second_score = sorted_votes[1][1]
            if top_score > total_votes * 0.66:
                agreement = "majority"
                confidence = (top_score / total_votes) * 100
            elif top_score > second_score * 1.5:
                agreement = "majority"
                confidence = (top_score / total_votes) * 80
            else:
                agreement = "split"
                confidence = (top_score / total_votes) * 60
        else:
            agreement = "none"
            confidence = 0

        # Find the original (non-normalized) value from the highest-weighted vote
        original_value = None
        for vote in sorted(self.votes, key=lambda v: v.weighted_confidence, reverse=True):
            field_value = getattr(vote, field, None)
            if field_value and self._normalize_text(field_value) == top_value:
                original_value = field_value
                break

        return original_value, confidence, agreement

    def calculate_consensus(self) -> ConsensusResult:
        """
        Calculate the consensus from all votes.

        Returns a ConsensusResult with the winning values and metadata.
        """
        if not self.votes:
            return ConsensusResult(
                needs_review=True,
                review_reason="No votes received from any source",
                agreement_level="none",
            )

        result = ConsensusResult()
        result.all_votes = self.votes.copy()

        # Calculate consensus for each field
        title, title_conf, title_agree = self._find_consensus_value("title")
        author, author_conf, author_agree = self._find_consensus_value("author")
        narrator, narrator_conf, narrator_agree = self._find_consensus_value("narrator")
        series, series_conf, series_agree = self._find_consensus_value("series")
        series_pos, pos_conf, pos_agree = self._find_consensus_value("series_position")

        result.title = title
        result.author = author
        result.narrator = narrator
        result.series = series
        result.series_position = series_pos

        # Overall confidence is weighted average of title and author (most important)
        if title_conf > 0 and author_conf > 0:
            result.confidence = (title_conf * 0.5 + author_conf * 0.5)
        elif title_conf > 0:
            result.confidence = title_conf * 0.7
        elif author_conf > 0:
            result.confidence = author_conf * 0.6
        else:
            result.confidence = 0

        # Determine overall agreement level
        agreements = [title_agree, author_agree]
        if "split" in agreements:
            result.agreement_level = "split"
        elif all(a == "unanimous" for a in agreements if a != "none"):
            result.agreement_level = "unanimous"
        elif "majority" in agreements:
            result.agreement_level = "majority"
        else:
            result.agreement_level = "none"

        # Find the winning source (highest weighted vote that matches consensus)
        for vote in sorted(self.votes, key=lambda v: v.weighted_confidence, reverse=True):
            if self._texts_match(vote.title, result.title) and self._texts_match(vote.author, result.author):
                result.winning_source = vote.source
                break

        # Build vote breakdown
        result.vote_breakdown = {}
        for vote in self.votes:
            key = f"{vote.title or '?'} by {vote.author or '?'}"
            if key not in result.vote_breakdown:
                result.vote_breakdown[key] = 0
            result.vote_breakdown[key] += 1

        # Determine if review is needed
        result.needs_review = False
        result.review_reason = None

        # Check: Generic title with low consensus
        if self._is_generic_title(result.title):
            if result.confidence < GENERIC_TITLE_THRESHOLD:
                result.needs_review = True
                result.review_reason = f"Generic title '{result.title}' needs higher consensus (got {result.confidence:.0f}%, need {GENERIC_TITLE_THRESHOLD}%)"

        # Check: Split votes
        if result.agreement_level == "split":
            result.needs_review = True
            result.review_reason = f"Sources disagree: {result.vote_breakdown}"

        # Check: Low overall confidence
        if result.confidence < MIN_CONSENSUS_CONFIDENCE:
            result.needs_review = True
            result.review_reason = result.review_reason or f"Low confidence ({result.confidence:.0f}%)"

        # Check: Only one source voted
        if len(self.votes) == 1:
            if result.confidence < HIGH_CONFIDENCE_THRESHOLD:
                result.needs_review = True
                result.review_reason = f"Single source ({self.votes[0].source}) with confidence {result.confidence:.0f}%"

        # Check: Drastic change from original (if we have one)
        if self._original_author and result.author:
            if not self._texts_match(self._original_author, result.author, threshold=0.3):
                # Author changed significantly - need higher confidence
                if result.confidence < 80:
                    result.needs_review = True
                    result.review_reason = f"Author change: '{self._original_author}' → '{result.author}'"

        logger.info(f"Precog consensus: '{result.title}' by '{result.author}' "
                   f"(confidence={result.confidence:.0f}%, agreement={result.agreement_level}, "
                   f"source={result.winning_source}, review={result.needs_review})")

        return result


def create_consensus_from_sources(
    skaldleita_result: Optional[Dict[str, Any]] = None,
    api_results: Optional[List[Dict[str, Any]]] = None,
    ai_result: Optional[Dict[str, Any]] = None,
    metadata_result: Optional[Dict[str, Any]] = None,
    path_result: Optional[Dict[str, Any]] = None,
    original_title: Optional[str] = None,
    original_author: Optional[str] = None,
) -> ConsensusResult:
    """
    Convenience function to create a consensus from various source results.

    Args:
        skaldleita_result: Result from Skaldleita audio identification
        api_results: List of results from API lookups (BookDB, Audnexus, etc.)
        ai_result: Result from AI verification
        metadata_result: Result from file metadata (ID3, JSON)
        path_result: Result from path/folder parsing
        original_title: Original title for comparison
        original_author: Original author for comparison

    Returns:
        ConsensusResult with the voting outcome
    """
    voting = PrecogVoting()
    voting.set_original(original_title, original_author)

    # Add Skaldleita vote (highest weight)
    if skaldleita_result:
        confidence = skaldleita_result.get("confidence", 80)
        if isinstance(confidence, (int, float)) and confidence <= 1:
            confidence = confidence * 100  # Convert 0-1 to 0-100
        voting.add_vote_from_dict("skaldleita", skaldleita_result, confidence)

    # Add API votes
    if api_results:
        for api_result in api_results:
            source = api_result.get("source", "api_bookdb")
            confidence = api_result.get("confidence", 70)
            voting.add_vote_from_dict(source, api_result, confidence)

    # Add AI vote
    if ai_result:
        source = ai_result.get("source", "ai_gemini")
        confidence = ai_result.get("confidence", 60)
        voting.add_vote_from_dict(source, ai_result, confidence)

    # Add metadata vote
    if metadata_result:
        source = metadata_result.get("source", "metadata_id3")
        confidence = metadata_result.get("confidence", 75)
        voting.add_vote_from_dict(source, metadata_result, confidence)

    # Add path parsing vote (lowest weight)
    if path_result:
        voting.add_vote_from_dict("path_parsing", path_result, 40)

    return voting.calculate_consensus()


# Export public API
__all__ = [
    "SourceVote",
    "ConsensusResult",
    "PrecogVoting",
    "create_consensus_from_sources",
    "SOURCE_WEIGHTS",
    "MIN_CONSENSUS_CONFIDENCE",
    "HIGH_CONFIDENCE_THRESHOLD",
    "GENERIC_TITLE_THRESHOLD",
]
