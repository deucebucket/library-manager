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


def _raises_value_error(fn):
    try:
        fn()
        return False
    except ValueError:
        return True


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

    # ==========================================
    # Issue #281: per-series language override/lock
    # ==========================================
    print("\n--- Issue #281: per-series language override ---")

    from library_manager import database as db
    from library_manager.pipeline.layer_audio_id import _resolve_metadata_language

    test_db = tmp / "series-lang.db"
    db.init_db(db_path=str(test_db))

    # --- DB CRUD ---
    db.set_series_language_override('Mistborn', 'de', db_path=str(test_db))
    check("set + get override",
          db.get_series_language_override('Mistborn', db_path=str(test_db)) == 'de')

    check("case-insensitive lookup",
          db.get_series_language_override('mistborn', db_path=str(test_db)) == 'de')
    check("whitespace/case-insensitive lookup",
          db.get_series_language_override('  MISTBORN ', db_path=str(test_db)) == 'de')

    db.set_series_language_override('mistborn', 'fr', db_path=str(test_db))
    all_overrides = db.get_all_series_language_overrides(db_path=str(test_db))
    check("update in place (no duplicate row)",
          len(all_overrides) == 1 and all_overrides[0]['language_code'] == 'fr',
          f"got: {all_overrides}")

    db.set_series_language_override('The Expanse', 'en', db_path=str(test_db))
    check("second series stored independently",
          db.get_series_language_override('The Expanse', db_path=str(test_db)) == 'en')

    check("unknown series returns None",
          db.get_series_language_override('Unknown Series', db_path=str(test_db)) is None)

    check("invalid language code rejected",
          _raises_value_error(lambda: db.set_series_language_override('X', 'xx', db_path=str(test_db))))

    check("'auto' removes the override",
          db.set_series_language_override('Mistborn', 'auto', db_path=str(test_db)) is None
          and db.get_series_language_override('Mistborn', db_path=str(test_db)) is None)

    check("delete returns True when removed",
          db.delete_series_language_override('the expanse', db_path=str(test_db)) is True)
    check("delete returns False when absent",
          db.delete_series_language_override('the expanse', db_path=str(test_db)) is False)

    # --- Resolution behavior ---
    def _audio_detector_must_not_run(*args, **kwargs):
        raise AssertionError("audio language detection should have been skipped")

    def _audio_detector_fr(*args, **kwargs):
        return {'language': 'fr'}

    old_db_path = db._db_path
    try:
        db.set_db_path(str(test_db))
        db.set_series_language_override('Mistborn', 'de')

        cfg_detect = {'detect_language_from_audio': True}
        lang = _resolve_metadata_language(
            tmp / "fake.m4b", "The Final Empire", cfg_detect,
            detect_audio_language_fn=_audio_detector_must_not_run,
            series_name='mistborn'
        )
        check("override is a hard override (audio detection skipped)", lang == 'de',
              f"got: {lang}")

        lang = _resolve_metadata_language(
            None, "The Final Empire", {},
            language_hint='en',
            series_name='Mistborn'
        )
        check("override beats explicit language hint", lang == 'de', f"got: {lang}")

        # auto/unset: normal detection still happens
        lang = _resolve_metadata_language(
            tmp / "fake.m4b", "The Final Empire", cfg_detect,
            detect_audio_language_fn=_audio_detector_fr,
            series_name='Other Series'
        )
        check("no override -> audio detection used", lang == 'fr', f"got: {lang}")

        lang = _resolve_metadata_language(
            None, "Der Wuestenplanet", {},
            series_name=None
        )
        check("no series -> title detection still runs", lang == 'de', f"got: {lang}")
    finally:
        db.set_db_path(old_db_path)

    # ==========================================
    # Issue #280: bilingual/multi-language tagging
    # ==========================================
    print("\n--- Issue #280: bilingual/multi-language tagging ---")

    import json as _json
    from library_manager.models.book_profile import (
        BookProfile, save_book_profile, load_book_profile,
    )
    from library_manager.models import book_profile as bp_module
    from library_manager.utils.path_safety import format_languages_tag

    # --- Profile: languages list set/get + primary back-compat ---
    profile = BookProfile()
    check("empty profile has no languages", profile.get_languages() == [])

    profile.language.value = 'de'
    check("get_languages falls back to primary language",
          profile.get_languages() == ['de'])

    profile.set_languages(['de', 'en'])
    check("set_languages stores ordered list",
          profile.languages == ['de', 'en'])
    check("set_languages syncs primary language",
          profile.language.value == 'de')

    profile.set_languages(['EN', 'en', ' fr '])
    check("set_languages normalizes and dedupes",
          profile.languages == ['en', 'fr'] and profile.language.value == 'en')

    # finalize() populates languages from the detected primary
    profile2 = BookProfile()
    profile2.language.add_source('audio', 'fr')
    profile2.finalize()
    check("finalize populates languages with primary",
          profile2.languages == ['fr'] and profile2.get_languages() == ['fr'])

    # to_dict/from_dict round-trip
    profile.set_languages(['de', 'en'])
    restored = BookProfile.from_dict(profile.to_dict())
    check("profile dict round-trip keeps languages",
          restored.languages == ['de', 'en'] and restored.language.value == 'de')
    old_profile = BookProfile.from_dict({'language': {'value': 'es', 'confidence': 80, 'sources': ['ai']}})
    check("old profile JSON without languages still works",
          old_profile.languages == [] and old_profile.get_languages() == ['es'])

    # --- Persistence: books.languages column + profile JSON ---
    lang_db = tmp / "book-langs.db"
    db.init_db(db_path=str(lang_db))
    conn = db.get_db(db_path=str(lang_db))
    conn.execute("INSERT INTO books (path, current_author, current_title) VALUES (?, ?, ?)",
                 (str(tmp / "book1"), "Frank Herbert", "Der Wuestenplanet"))
    book_id = conn.execute("SELECT id FROM books WHERE path = ?", (str(tmp / "book1"),)).fetchone()[0]
    conn.commit()
    conn.close()

    db.set_book_languages(book_id, ['de', 'en'], db_path=str(lang_db))
    check("set/get book languages round-trip",
          db.get_book_languages(book_id, db_path=str(lang_db)) == ['de', 'en'])

    check("invalid language code rejected",
          _raises_value_error(lambda: db.set_book_languages(book_id, ['de', 'xx'], db_path=str(lang_db))))
    check("rejected update leaves previous list intact",
          db.get_book_languages(book_id, db_path=str(lang_db)) == ['de', 'en'])

    db.set_book_languages(book_id, [], db_path=str(lang_db))
    check("empty list clears languages",
          db.get_book_languages(book_id, db_path=str(lang_db)) == [])

    # Old rows: no languages column value -> fall back to profile JSON language
    conn = db.get_db(db_path=str(lang_db))
    conn.execute("UPDATE books SET profile = ? WHERE id = ?",
                 (_json.dumps({'language': {'value': 'pl', 'confidence': 90, 'sources': ['audio']}}), book_id))
    conn.commit()
    conn.close()
    check("get_book_languages falls back to profile language",
          db.get_book_languages(book_id, db_path=str(lang_db)) == ['pl'])

    # save/load_book_profile persists the list (column + profile JSON)
    old_db_path = db._db_path
    try:
        db.set_db_path(str(lang_db))
        bp_module.set_db_getter(db.get_db)
        profile3 = BookProfile()
        profile3.language.add_source('audio', 'de')
        profile3.finalize()
        profile3.set_languages(['de', 'en'])
        save_book_profile(book_id, profile3)
        loaded = load_book_profile(book_id)
        check("save/load_book_profile round-trips languages",
              loaded is not None and loaded.languages == ['de', 'en'])
        check("languages column written by save_book_profile",
              db.get_book_languages(book_id) == ['de', 'en'])
    finally:
        db.set_db_path(old_db_path)

    # --- API: POST /api/books/<id>/languages ---
    from app import app as flask_app
    client = flask_app.test_client()
    old_db_path = db._db_path
    try:
        db.set_db_path(str(lang_db))

        resp = client.post(f'/api/books/{book_id}/languages',
                           json={'languages': ['de', 'en']})
        check("POST sets languages", resp.status_code == 200 and
              resp.get_json().get('languages') == ['de', 'en'],
              f"got: {resp.status_code} {resp.get_json()}")

        resp = client.get(f'/api/books/{book_id}/languages')
        check("GET returns stored languages",
              resp.status_code == 200 and resp.get_json().get('languages') == ['de', 'en'],
              f"got: {resp.status_code} {resp.get_json()}")

        resp = client.post(f'/api/books/{book_id}/languages',
                           json={'languages': ['de', 'xx']})
        check("bad code rejected with 400",
              resp.status_code == 400 and resp.get_json().get('success') is False,
              f"got: {resp.status_code} {resp.get_json()}")
        check("rejected API call keeps previous list",
              db.get_book_languages(book_id, db_path=str(lang_db)) == ['de', 'en'])

        resp = client.post(f'/api/books/{book_id}/languages', json={'languages': []})
        check("empty list clears secondaries, falls back to primary",
              resp.status_code == 200 and resp.get_json().get('languages') == ['de'],
              f"got: {resp.status_code} {resp.get_json()}")

        resp = client.post(f'/api/books/{book_id}/languages', json={'languages': 'de'})
        check("non-list body rejected with 400", resp.status_code == 400)

        resp = client.post('/api/books/999999/languages', json={'languages': ['de']})
        check("unknown book returns 404", resp.status_code == 404)

        # Setting a new primary via API syncs the profile JSON language
        client.post(f'/api/books/{book_id}/languages', json={'languages': ['fr', 'en']})
        loaded = load_book_profile(book_id)
        check("API set syncs profile primary language",
              loaded is not None and loaded.language.value == 'fr'
              and loaded.languages == ['fr', 'en'],
              f"got: {loaded.language.value if loaded else None}")
    finally:
        db.set_db_path(old_db_path)

    # --- Naming: multi-language tags ---
    check("bracket_full joins names with ', '",
          format_languages_tag(['de', 'en'], fmt='bracket_full') == ' (German, English)',
          f"got: {format_languages_tag(['de', 'en'], fmt='bracket_full')!r}")
    check("bracket_code joins codes with ','",
          format_languages_tag(['de', 'en'], fmt='bracket_code') == ' [de,en]')
    check("bracket_code respects iso639-2",
          format_languages_tag(['de', 'en'], fmt='bracket_code', code_format='iso639-2') == ' [ger,eng]')
    check("emoji_flag joins flags with space",
          format_languages_tag(['de', 'en'], fmt='emoji_flag') == ' 🇩🇪 🇬🇧')
    check("single language identical to format_language_tag",
          format_languages_tag(['de'], fmt='bracket_full') == format_language_tag('de', fmt='bracket_full'))
    check("empty list gives empty tag", format_languages_tag([], fmt='bracket_full') == '')

    cfg_multi = {
        'naming_format': 'author/title',
        'language_tag_enabled': True,
        'language_tag_format': 'bracket_full',
        'language_tag_position': 'after_title',
        'preferred_language': 'en',
    }
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de",
                       languages=['de', 'en'], config=cfg_multi)
    check("multi-language folder tag (German, English)",
          p is not None and str(p.relative_to(lib)) == "Frank Herbert/Der Wuestenplanet (German, English)",
          f"got: {p}")

    cfg_multi_code = dict(cfg_multi, language_tag_format='bracket_code')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language_code="de", languages=['de', 'en'], config=cfg_multi_code)
    check("multi-language folder tag [de,en]",
          p is not None and str(p.relative_to(lib)) == "Frank Herbert/Der Wuestenplanet [de,en]",
          f"got: {p}")

    cfg_multi_6392 = dict(cfg_multi_code, language_code_format='iso639-2')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language_code="de", languages=['de', 'en'], config=cfg_multi_6392)
    check("multi-language folder tag [ger,eng]",
          p is not None and str(p.relative_to(lib)) == "Frank Herbert/Der Wuestenplanet [ger,eng]",
          f"got: {p}")

    cfg_multi_flag = dict(cfg_multi, language_tag_format='emoji_flag')
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language_code="de", languages=['de', 'en'], config=cfg_multi_flag)
    check("multi-language folder tag 🇩🇪 🇬🇧",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet 🇩🇪 🇬🇧"),
          f"got: {p}")

    # Single-language regression: byte-identical to pre-#280 behavior
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de",
                       languages=['de'], config=cfg_multi)
    check("single-entry languages list unchanged",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet (German)"),
          f"got: {p}")
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language="German", language_code="de", config=cfg_multi)
    check("no languages param unchanged",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet (German)"),
          f"got: {p}")

    # languages=None default does not affect other formats
    p = build_new_path(lib, "Frank Herbert", "Der Wuestenplanet",
                       language_code="de", languages=None, config=cfg_multi_code)
    check("languages=None identical to today",
          p is not None and p.relative_to(lib).parts == ("Frank Herbert", "Der Wuestenplanet [de]"),
          f"got: {p}")

    print("\n" + "=" * 60)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
