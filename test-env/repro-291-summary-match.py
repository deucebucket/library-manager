#!/usr/bin/env python3
"""Reproduce Issue #291: third-party summary books accepted as title matches.

Part 1 (filter level): feed summary-style candidates through the exact
filter chain used by gather_all_api_candidates and show they pass.

Part 2 (live): run gather_all_api_candidates against the real APIs the way
the watch-folder / scan pipeline does, for a title known to attract summary
editions, and print what comes back.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from library_manager.utils.validation import is_garbage_match, is_garbage_author_match, is_summary_match

print("=" * 70)
print("PART 1: filter-level repro (no network)")
print("=" * 70)

# Candidates as returned by Google Books / BookDB for real searches.
# Third-party summary mills from the issue: IRB Media, Start Publishing Notes.
cases = [
    # (query_title, query_author, candidate_title, candidate_author)
    ("Atomic Habits", None, "Summary of Atomic Habits", "IRB Media"),
    ("Atomic Habits", None, "Summary: Atomic Habits: An Easy & Proven Way to Build Good Habits", "Start Publishing Notes"),
    ("Atomic Habits", None, "Atomic Habits", "James Clear"),  # legit control
    ("Project Hail Mary", None, "Summary of Project Hail Mary", "Instaread"),
    ("Project Hail Mary", None, "Project Hail Mary", "Andy Weir"),  # legit control
    # User genuinely owns the summary audiobook - must NOT be rejected
    ("Summary of Atomic Habits", None, "Summary of Atomic Habits", "IRB Media"),
]

bug_reproduced = False
for qt, qa, ct, ca in cases:
    # Mirror gather_all_api_candidates' acceptance logic:
    # rejected if garbage title match, (author hint present AND garbage author),
    # or summary/derivative match (Issue #291 fix)
    rejected_title = is_garbage_match(qt, ct)
    rejected_author = bool(qa) and is_garbage_author_match(qa, ca)
    rejected_summary = is_summary_match(qt, ct, ca)
    accepted = not rejected_title and not rejected_author and not rejected_summary
    verdict = "ACCEPTED" if accepted else "rejected"
    print(f"  query='{qt}' (author hint: {qa or 'none'})")
    print(f"    candidate: '{ct}' by '{ca}' -> {verdict}")
    is_summary = ('summary' in ct.lower()) or (ca and ca.lower() in ('irb media', 'start publishing notes', 'instaread'))
    query_is_summary = 'summary' in qt.lower()
    if accepted and is_summary and not query_is_summary:
        bug_reproduced = True
        print("    *** BUG: third-party summary accepted as a match ***")
    elif accepted and is_summary and query_is_summary:
        print("    OK: accepted because the source itself is a summary (intended exemption)")

print()
if bug_reproduced:
    print("PART 1 RESULT: BUG REPRODUCED - summary candidates pass the filter chain")
else:
    print("PART 1 RESULT: no summary candidate accepted (bug not reproduced)")

print()
print("=" * 70)
print("PART 2: live API repro via gather_all_api_candidates (as a user scan)")
print("=" * 70)
try:
    from app import gather_all_api_candidates
    config = {'preferred_language': 'en', 'strict_language_matching': False}
    for query in ["Atomic Habits", "Can't Hurt Me"]:
        print(f"\n  Searching: '{query}' (no author hint - worst case)")
        cands = gather_all_api_candidates(query, None, config)
        summaries = [c for c in cands if 'summary' in (c.get('title') or '').lower()]
        print(f"  {len(cands)} candidates returned, {len(summaries)} are summaries:")
        for c in cands:
            marker = " <-- SUMMARY" if 'summary' in (c.get('title') or '').lower() else ""
            print(f"    [{c.get('source')}] {c.get('author')} - {c.get('title')}{marker}")
except Exception as e:
    print(f"  live repro skipped/failed: {e}")
