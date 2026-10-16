"""Descriptor-bound exec helper for ACP-owned client terminals."""

from __future__ import annotations

import os
import sys


def main() -> int:
    if len(sys.argv) < 3:
        return 64
    try:
        directory_fd = int(sys.argv[1])
        if directory_fd < 0:
            return 64
        command = sys.argv[2]
        arguments = sys.argv[2:]
        os.fchdir(directory_fd)
        os.close(directory_fd)
        os.execvpe(command, arguments, dict(os.environ))
    except (OSError, ValueError):
        return 126
    return 126


if __name__ == "__main__":
    raise SystemExit(main())
