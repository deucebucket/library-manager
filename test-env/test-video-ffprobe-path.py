"""A missing ffprobe is retried while a discovered executable is cached."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from library_manager.utils import video  # noqa: E402


original_cache = video._FFPROBE
original_which = video.shutil.which
original_exists = video.os.path.exists
calls = []
answers = iter((None, "/tools/ffprobe"))
try:
    video._FFPROBE = None

    def which(_name):
        calls.append("probe")
        return next(answers)

    video.shutil.which = which
    video.os.path.exists = lambda _path: False
    assert video.ffprobe_path() is None
    assert video.ffprobe_path() == "/tools/ffprobe"
    assert video.ffprobe_path() == "/tools/ffprobe"
    assert calls == ["probe", "probe"]
finally:
    video._FFPROBE = original_cache
    video.shutil.which = original_which
    video.os.path.exists = original_exists

print("All ffprobe path tests passed.")
