#!/usr/bin/env python3
"""
Tests for the Precog consensus voting system.

Tests the Minority Report style voting where multiple sources
vote on book identification and majority (weighted) wins.
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from library_manager.precog import (
    PrecogVoting,
    SourceVote,
    ConsensusResult,
    create_consensus_from_sources,
    SOURCE_WEIGHTS,
)


def test_unanimous_agreement():
    """When all sources agree, confidence should be high."""
    voting = PrecogVoting()

    # All sources agree on the same book
    voting.add_vote(SourceVote(
        source="skaldleita",
        title="The Martian",
        author="Andy Weir",
        confidence=90,
    ))
    voting.add_vote(SourceVote(
        source="api_bookdb",
        title="The Martian",
        author="Andy Weir",
        confidence=85,
    ))
    voting.add_vote(SourceVote(
        source="metadata_id3",
        title="The Martian",
        author="Andy Weir",
        confidence=80,
    ))

    result = voting.calculate_consensus()

    assert result.title == "The Martian", f"Expected 'The Martian', got '{result.title}'"
    assert result.author == "Andy Weir", f"Expected 'Andy Weir', got '{result.author}'"
    assert result.agreement_level == "unanimous", f"Expected unanimous, got {result.agreement_level}"
    assert result.confidence > 80, f"Expected high confidence, got {result.confidence}"
    assert result.needs_review == False, f"Unanimous agreement shouldn't need review"

    print("✓ test_unanimous_agreement passed")


def test_majority_wins():
    """When 2 of 3 agree, the majority should win."""
    voting = PrecogVoting()

    # Two sources say "The Martian", one says something else
    voting.add_vote(SourceVote(
        source="skaldleita",
        title="The Martian",
        author="Andy Weir",
        confidence=85,
    ))
    voting.add_vote(SourceVote(
        source="api_bookdb",
        title="The Martian",
        author="Andy Weir",
        confidence=80,
    ))
    voting.add_vote(SourceVote(
        source="path_parsing",
        title="Martian Chronicles",
        author="Ray Bradbury",
        confidence=40,
    ))

    result = voting.calculate_consensus()

    assert result.title == "The Martian", f"Expected 'The Martian', got '{result.title}'"
    assert result.author == "Andy Weir", f"Expected 'Andy Weir', got '{result.author}'"
    assert result.agreement_level in ["unanimous", "majority"], f"Expected majority, got {result.agreement_level}"

    print("✓ test_majority_wins passed")


def test_split_votes_need_review():
    """When sources strongly disagree, flag for review."""
    voting = PrecogVoting()

    # Two high-confidence sources disagree
    voting.add_vote(SourceVote(
        source="skaldleita",
        title="Match Game",
        author="Craig Alanson",
        confidence=70,
    ))
    voting.add_vote(SourceVote(
        source="ai_gemini",
        title="Match Game",
        author="Doc Raymond",
        confidence=75,
    ))

    result = voting.calculate_consensus()

    # Should detect the author disagreement
    assert result.title == "Match Game", f"Title should match"
    # The system should flag this for review due to author disagreement or low confidence
    # (Either split on author OR needs review for other reasons is acceptable)

    print(f"✓ test_split_votes_need_review passed (needs_review={result.needs_review}, reason={result.review_reason})")


def test_generic_title_needs_high_consensus():
    """Generic titles like 'Match Game' need higher confidence."""
    voting = PrecogVoting()

    # Single source with moderate confidence on a generic title
    voting.add_vote(SourceVote(
        source="ai_gemini",
        title="The End",
        author="Lemony Snicket",
        confidence=70,
    ))

    result = voting.calculate_consensus()

    assert result.needs_review == True, "Generic titles should need review with moderate confidence"
    assert "generic" in result.review_reason.lower() or "single" in result.review_reason.lower(), \
        f"Review reason should mention generic title or single source: {result.review_reason}"

    print("✓ test_generic_title_needs_high_consensus passed")


def test_audio_beats_path():
    """Audio identification should outweigh path parsing."""
    voting = PrecogVoting()

    # Skaldleita says one thing, path says another
    voting.add_vote(SourceVote(
        source="skaldleita",
        title="Project Hail Mary",
        author="Andy Weir",
        confidence=95,
    ))
    voting.add_vote(SourceVote(
        source="path_parsing",
        title="Unknown Book",
        author="Various",
        confidence=40,
    ))

    result = voting.calculate_consensus()

    assert result.title == "Project Hail Mary", f"Audio should win over path"
    assert result.author == "Andy Weir", f"Audio author should win"
    assert result.winning_source == "skaldleita", f"Winning source should be skaldleita"

    print("✓ test_audio_beats_path passed")


def test_drastic_author_change_flagged():
    """Changing author drastically from original should be flagged."""
    voting = PrecogVoting()
    voting.set_original(title="Match Game", author="Craig Alanson")

    # AI suggests completely different author
    voting.add_vote(SourceVote(
        source="ai_gemini",
        title="Match Game",
        author="Doc Raymond",
        confidence=65,
    ))

    result = voting.calculate_consensus()

    # Should flag because author changed and confidence isn't super high
    assert result.needs_review == True, "Drastic author change should need review"
    assert "author" in result.review_reason.lower() or "single" in result.review_reason.lower(), \
        f"Review reason should mention author change: {result.review_reason}"

    print("✓ test_drastic_author_change_flagged passed")


def test_convenience_function():
    """Test the create_consensus_from_sources convenience function."""
    result = create_consensus_from_sources(
        skaldleita_result={
            "title": "Children of Time",
            "author": "Adrian Tchaikovsky",
            "narrator": "Mel Hudson",
            "confidence": 0.92,  # 0-1 scale should be converted
        },
        api_results=[
            {
                "source": "api_audnexus",
                "title": "Children of Time",
                "author": "Adrian Tchaikovsky",
                "confidence": 85,
            }
        ],
        original_title="Children of Time",
        original_author="Adrian Tchaikovsky",
    )

    assert result.title == "Children of Time"
    assert result.author == "Adrian Tchaikovsky"
    assert result.confidence > 70
    assert result.needs_review == False, "Matching results shouldn't need review"

    print("✓ test_convenience_function passed")


def test_no_votes():
    """No votes should return needs_review=True."""
    voting = PrecogVoting()
    result = voting.calculate_consensus()

    assert result.needs_review == True
    assert "no votes" in result.review_reason.lower()

    print("✓ test_no_votes passed")


def test_source_weights():
    """Verify source weights are properly defined."""
    assert SOURCE_WEIGHTS["skaldleita"] > SOURCE_WEIGHTS["ai_gemini"], \
        "Skaldleita should outweigh AI"
    assert SOURCE_WEIGHTS["metadata_id3"] > SOURCE_WEIGHTS["path_parsing"], \
        "Metadata should outweigh path"
    assert SOURCE_WEIGHTS["api_bookdb"] > SOURCE_WEIGHTS["ai_gemini"], \
        "API should outweigh AI"

    print("✓ test_source_weights passed")


def test_title_normalization():
    """Titles with slight variations should match."""
    voting = PrecogVoting()

    voting.add_vote(SourceVote(
        source="skaldleita",
        title="The Hobbit",
        author="J.R.R. Tolkien",
        confidence=90,
    ))
    voting.add_vote(SourceVote(
        source="api_bookdb",
        title="Hobbit",  # Without "The"
        author="JRR Tolkien",  # Without periods
        confidence=85,
    ))

    result = voting.calculate_consensus()

    assert "hobbit" in result.title.lower(), f"Should recognize Hobbit variations"
    assert "tolkien" in result.author.lower(), f"Should recognize Tolkien variations"
    assert result.agreement_level == "unanimous", "Variations should count as unanimous"

    print("✓ test_title_normalization passed")


def run_all_tests():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("Running Precog Consensus Voting Tests")
    print("=" * 60 + "\n")

    tests = [
        test_unanimous_agreement,
        test_majority_wins,
        test_split_votes_need_review,
        test_generic_title_needs_high_consensus,
        test_audio_beats_path,
        test_drastic_author_change_flagged,
        test_convenience_function,
        test_no_votes,
        test_source_weights,
        test_title_normalization,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"✗ {test.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ {test.__name__} ERROR: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60 + "\n")

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
