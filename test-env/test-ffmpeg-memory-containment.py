#!/usr/bin/env python3
"""Regression coverage for Issue #303 ffmpeg memory containment."""

import os
import re
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from library_manager import resource_limited_exec  # noqa: E402
from library_manager.file_validation import can_seek_to_end  # noqa: E402
from library_manager.utils.audio import (  # noqa: E402
    FFMPEG_MAX_SINGLE_ALLOCATION_BYTES,
    build_limited_ffmpeg_command,
    extract_audio_sample,
)


class TestFfmpegMemoryContainment(unittest.TestCase):
    def test_builder_wraps_ffmpeg_with_posix_memory_limit(self):
        with mock.patch("library_manager.utils.audio.os.name", "posix"):
            command = build_limited_ffmpeg_command(["-version"], memory_limit_mb=768)

        self.assertEqual(command[:3], [sys.executable, "-m", "library_manager.resource_limited_exec"])
        self.assertEqual(command[3:6], ["--memory-mb", "768", "--"])
        self.assertEqual(command[6:11], [
            "ffmpeg", "-nostdin", "-hide_banner", "-max_alloc",
            str(FFMPEG_MAX_SINGLE_ALLOCATION_BYTES),
        ])
        self.assertEqual(command[11:17], [
            "-threads", "2", "-filter_threads", "2", "-filter_complex_threads", "2",
        ])
        self.assertEqual(command[-1], "-version")

    def test_sample_extraction_selects_audio_only(self):
        failed = SimpleNamespace(returncode=1, stderr=b"expected failure")
        with mock.patch("library_manager.utils.audio.subprocess.run", return_value=failed) as run:
            self.assertIsNone(extract_audio_sample("pathological.m4b", duration_seconds=45))

        command = run.call_args.args[0]
        ffmpeg_index = command.index("ffmpeg")
        ffmpeg_command = command[ffmpeg_index:]
        self.assertIn("-vn", ffmpeg_command)
        self.assertIn("-sn", ffmpeg_command)
        self.assertIn("-dn", ffmpeg_command)
        self.assertIn("-map", ffmpeg_command)
        self.assertEqual(ffmpeg_command[ffmpeg_command.index("-map") + 1], "0:a:0")
        self.assertEqual(run.call_args.kwargs["timeout"], 60)

    def test_end_seek_validation_is_limited_and_audio_only(self):
        succeeded = SimpleNamespace(returncode=0)
        with mock.patch(
            "library_manager.file_validation.subprocess.run",
            return_value=succeeded,
        ) as run:
            self.assertTrue(can_seek_to_end("pathological.m4b"))

        command = run.call_args.args[0]
        ffmpeg_index = command.index("ffmpeg")
        ffmpeg_command = command[ffmpeg_index:]
        self.assertIn("-max_alloc", ffmpeg_command)
        self.assertIn("-sseof", ffmpeg_command)
        self.assertIn("-map", ffmpeg_command)
        self.assertEqual(ffmpeg_command[ffmpeg_command.index("-map") + 1], "0:a:0")
        self.assertIn("-vn", ffmpeg_command)
        self.assertIn("-sn", ffmpeg_command)
        self.assertIn("-dn", ffmpeg_command)

    def test_no_production_code_bypasses_ffmpeg_limiter(self):
        direct_ffmpeg = re.compile(r"[\"']ffmpeg[\"']\s*,")
        bypasses = []
        for source_path in [REPO_ROOT / "app.py", *(REPO_ROOT / "library_manager").rglob("*.py")]:
            if source_path.name == "audio.py":
                continue
            for line_number, line in enumerate(source_path.read_text().splitlines(), 1):
                if direct_ffmpeg.search(line):
                    bypasses.append(f"{source_path.relative_to(REPO_ROOT)}:{line_number}")

        self.assertEqual(bypasses, [], f"direct ffmpeg commands bypass limiter: {bypasses}")

    def test_launcher_applies_limit_before_exec(self):
        with (
            mock.patch.object(resource_limited_exec, "_apply_memory_limit") as apply_limit,
            mock.patch.object(resource_limited_exec.os, "execvp") as execvp,
        ):
            result = resource_limited_exec.main([
                "--memory-mb", "1024", "--", "ffmpeg", "-version",
            ])

        self.assertEqual(result, 127)
        apply_limit.assert_called_once_with(1024)
        execvp.assert_called_once_with("ffmpeg", ["ffmpeg", "-version"])

    @unittest.skipUnless(os.name == "posix", "RLIMIT_AS is POSIX-only")
    def test_real_ffmpeg_starts_under_limit(self):
        command = build_limited_ffmpeg_command(["-version"], memory_limit_mb=2048)
        result = subprocess.run(command, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))


if __name__ == "__main__":
    unittest.main()
