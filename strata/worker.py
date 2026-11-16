"""Build worker process entry point.

Runs a single build and exits.  The worker never publishes anything itself: it
calls :func:`strata.engine.execute_build`, whose final act is one metadata
transaction that the server-side store validates against the build token and the
currently published pointer.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import traceback


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: worker <data_root> <build_id>", file=sys.stderr)
        return 2
    root, build_id = sys.argv[1], int(sys.argv[2])

    from strata.engine import execute_build
    from strata.store import Store

    store = Store(root)

    def term(signum, frame):
        # Cooperative cancellation: never exit while holding a write transaction,
        # so the build row records the cancellation cleanly.
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, term)

    crash_at = os.environ.get("STRATA_CRASH") or None
    try:
        result = execute_build(store, build_id, crash_at=crash_at)
    except SystemExit:
        raise
    except BaseException:
        traceback.print_exc()
        b = store.build(build_id)
        if b is not None and b.status in ("queued", "running"):
            store.finish_build(build_id, b.token, "crashed",
                               "worker raised: " + traceback.format_exc()[-1000:])
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
