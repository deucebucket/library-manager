#!/usr/bin/env python3
"""
Tests for language naming features:
- Issue #278: language as top-level folder (top_folder position)
- Issue #282: emoji flag language tags (emoji_flag format, {lang_flag} tag)
- Issue #279: ISO 639-2 three-letter codes (language_code_format)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tempfile
from pathlib import Path

from library_manager.utils.path_safety import (
    build_new_path,
    format_language_tag,
    lang_code_for_display,
    LANGUAGE_FLAGS,
    ISO_639_1_TO_2,
    ISO_639_2_TO_1,
)


def test_result(name, passed, details=""):
    status = "\033[92mPASS\033[0m" if passed else "\033[91mFAIL\033[0m"
    print(f"[{status}] {name}")
    if details and not passed:
        print(f"       {details}")
    return passed


def main():
    passed = 0
    failed = 0

    def check(name, cond, details=""):
        nonlocal passed, failed
        if test_result(name, cond, details):
            passed += 1
        else:
            failed += 1

    tmp = Path(tempfile.mkdtemp(prefix="lm-langtest-"))
    lib = tmp / "library"
    lib.mkdir()

    print("=" * 60)
    print("LANGUAGE FEATURE TESTS - Issues #278, #279, #282")
    print("=" * 60)

    # ==========================================
    # Issue #278: top-level language folder
    # ==========================================
    print("\n--- Issue #278: language_tag_position = top_folder ---")

    cfg_top = {
        'naming_format': 'author/title',
        'language_tag_enabled': True,
        'language_tag_position': 'top_folder',
        'preferred_language': 'en',
    }

    p = build_new_path(lib, "Brandon Sanderson", "Die Nebelgeborenen",
                       language="German", language_code="de", config=cfg_top)
    check("non-preferred book -> German/Author/Title",
          p is not None and p.relative_to(lib).parts == ("German", "Brandon Sanderson", "Die Nebelgeborenen"),
          f"got: {p}")

    p = build_new_path(lib, "Frank Herbert", "Dune",
                       language="English", language_code="en", config=cfg_top)
    check("preferred-language book still gets language folder (full partition)",
          p is not None and p.relative_to(lib).parts == ("English", "Frank Herbert", "Dune"),
          f"got: {p}")

    cfg_top_series = dict(cfg_top, series_grouping=True)
    p = build_new_path(lib, "Brandon Sanderson", "The Final Empire",
                       series="Mistborn", series_num=1,
                       language="German", language_code="de", config=cfg_top_series)
    check("series grouping preserved below language folder",
          p is not None and p.relative_to(lib).parts == (
              "German", "Brandon Sanderson", "Mistborn", "01 - The Final Empire"),
          f"got: {p}")

    cfg_top_flat = dict(cfg_top, naming_format='author - title')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_top_flat)
    check("flat 'author - title' format wrapped in language folder",
          p is not None and p.relative_to(lib).parts == ("German", "Frank Herbert - Der Wuestenplanet"),
          f"got: {p}")

    cfg_top_lf = dict(cfg_top, naming_format='author_lf/title')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_top_lf)
    check("author_lf/title format wrapped in language folder",
          p is not None and p.relative_to(lib).parts == ("German", "Herbert, Frank", "Der Wuestenplanet"),
          f"got: {p}")

    cfg_top_custom = dict(cfg_top, naming_format='custom',
                          custom_naming_template='{author}/{title}')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_top_custom)
    check("custom template wrapped in language folder",
          p is not None and p.relative_to(lib).parts == ("German", "Frank Herbert", "Der Wuestenplanet"),
          f"got: {p}")

    cfg_top_off = dict(cfg_top, language_tag_enabled=False, multilang_naming_mode='native')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_top_off)
    check("top_folder ignored when tagging not requested",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet"),
          f"got: {p}")

    p = build_new_path(lib, "Frank Herbert", "Dune", config=cfg_top)
    check("no language known -> plain path, no language folder",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Dune"),
          f"got: {p}")

    # Existing subfolder behavior must be unaffected
    cfg_sub = dict(cfg_top, language_tag_position='subfolder')
    p = build_new_path(lib, "Brandon Sanderson", "Die Nebelgeborenen",
                       language="German", language_code="de", config=cfg_sub)
    check("subfolder position still works (Author/German/Title)",
          p is not None and p.relative_to(lib).parts == (
              "Brandon Sanderson", "German", "Die Nebelgeborenen"),
          f"got: {p}")

    # ==========================================
    # Issue #282: emoji flag tags
    # ==========================================
    print("\n--- Issue #282: emoji_flag format and {lang_flag} template tag ---")

    check("format_language_tag de emoji_flag",
          format_language_tag('de', fmt='emoji_flag') == ' 🇩🇪',
          f"got: {format_language_tag('de', fmt='emoji_flag')!r}")
    check("format_language_tag en emoji_flag",
          format_language_tag('en', fmt='emoji_flag') == ' 🇬🇧',
          f"got: {format_language_tag('en', fmt='emoji_flag')!r}")
    check("unmapped code falls back to bracket_full name",
          format_language_tag('xx', fmt='emoji_flag') == ' (XX)',
          f"got: {format_language_tag('xx', fmt='emoji_flag')!r}")
    check("all LANGUAGE_NAMES codes have flags",
          all(c in LANGUAGE_FLAGS for c in
              ['en', 'de', 'fr', 'es', 'it', 'pt', 'nl', 'sv', 'no', 'da', 'fi',
               'pl', 'ru', 'ja', 'zh', 'ko', 'ar', 'he', 'hi', 'tr', 'cs', 'hu',
               'el', 'th', 'vi', 'uk', 'ro', 'id']))

    cfg_flag = {
        'naming_format': 'author/title',
        'language_tag_enabled': True,
        'language_tag_format': 'emoji_flag',
        'language_tag_position': 'after_title',
        'preferred_language': 'en',
    }
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_flag)
    check("emoji flag appended after title",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet 🇩🇪"),
          f"got: {p}")

    cfg_flag_custom = dict(cfg_flag, naming_format='custom',
                           custom_naming_template='{author}/{title} {lang_flag}')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_flag_custom)
    check("{lang_flag} in custom template",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet 🇩🇪"),
          f"got: {p}")

    cfg_flag_none = dict(cfg_flag_custom)
    p = build_new_path(lib, "Frank Herbert", "Dune",
                       config=cfg_flag_none)
    check("{lang_flag} empty when no language known (no trailing junk)",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Dune"),
          f"got: {p}")

    # ==========================================
    # Issue #279: ISO 639-2 three-letter codes
    # ==========================================
    print("\n--- Issue #279: language_code_format = iso639-2 ---")

    check("lang_code_for_display de -> ger",
          lang_code_for_display('de', 'iso639-2') == 'ger')
    check("lang_code_for_display default keeps 639-1",
          lang_code_for_display('de') == 'de')
    check("lang_code_for_display iso639-1 explicit",
          lang_code_for_display('de', 'iso639-1') == 'de')
    check("unknown code passes through",
          lang_code_for_display('xx', 'iso639-2') == 'xx')
    check("empty code passes through",
          lang_code_for_display('', 'iso639-2') == '')
    check("reverse map ger -> de",
          ISO_639_2_TO_1.get('ger') == 'de')
    check("reverse map round-trips all entries",
          all(ISO_639_2_TO_1[v] == k for k, v in ISO_639_1_TO_2.items()))

    check("bracket_code with iso639-2",
          format_language_tag('de', fmt='bracket_code', code_format='iso639-2') == ' [ger]',
          f"got: {format_language_tag('de', fmt='bracket_code', code_format='iso639-2')!r}")
    check("code format with iso639-2",
          format_language_tag('fr', fmt='code', code_format='iso639-2') == '_fre',
          f"got: {format_language_tag('fr', fmt='code', code_format='iso639-2')!r}")
    check("bracket_code default stays 639-1",
          format_language_tag('de', fmt='bracket_code') == ' [de]',
          f"got: {format_language_tag('de', fmt='bracket_code')!r}")
    check("bracket_full unaffected by code_format",
          format_language_tag('de', fmt='bracket_full', code_format='iso639-2') == ' (German)',
          f"got: {format_language_tag('de', fmt='bracket_full', code_format='iso639-2')!r}")

    cfg_6392 = {
        'naming_format': 'author/title',
        'language_tag_enabled': True,
        'language_tag_format': 'bracket_code',
        'language_tag_position': 'after_title',
        'language_code_format': 'iso639-2',
        'preferred_language': 'en',
    }
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_6392)
    check("folder tag uses three-letter code",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet [ger]"),
          f"got: {p}")

    cfg_6392_custom = dict(cfg_6392, naming_format='custom',
                           custom_naming_template='{author}/{title} [{lang_code}]')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_6392_custom)
    check("{lang_code} respects iso639-2 in custom template",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet [ger]"),
          f"got: {p}")

    cfg_6391_custom = dict(cfg_6392_custom, language_code_format='iso639-1')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_6391_custom)
    check("{lang_code} stays two-letter by default",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet [de]"),
          f"got: {p}")

    print("\n" + "=" * 60)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
