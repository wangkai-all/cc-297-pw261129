"""Command-line entry point.

    python3 -m strata serve [--root DIR] [--host H] [--port P] [--inline]

``--inline`` runs builds in threads inside the server process; the default
spawns a real worker process per build, which is what the crash / cancellation
guarantees are about.
"""
from __future__ import annotations

import argparse
import sys

from .server import serve


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="strata")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve", help="start the HTTP UI and API")
    s.add_argument("--root", default="data", help="data directory (default: data)")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8765)
    s.add_argument("--inline", action="store_true",
                   help="run builds in-process instead of spawning workers")
    args = p.parse_args(argv)
    if args.cmd == "serve":
        serve(args.root, args.host, args.port, inline_workers=args.inline)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
