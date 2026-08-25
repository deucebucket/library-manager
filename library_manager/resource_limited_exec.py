"""Execute a child command with a bounded address space on POSIX hosts."""

import argparse
import os
from typing import Optional, Sequence


def _apply_memory_limit(memory_mb: int) -> None:
    if os.name != "posix":
        return

    import resource

    limit_bytes = memory_mb * 1024 * 1024
    _soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_AS)
    if hard_limit != resource.RLIM_INFINITY:
        limit_bytes = min(limit_bytes, hard_limit)
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--memory-mb", type=int, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if args.memory_mb < 256:
        parser.error("--memory-mb must be at least 256")
    if not command:
        parser.error("a command is required after --")

    _apply_memory_limit(args.memory_mb)
    os.execvp(command[0], command)
    return 127  # pragma: no cover - os.execvp replaces this process


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
