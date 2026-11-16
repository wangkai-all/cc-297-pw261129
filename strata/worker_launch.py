"""Spawn and supervise build workers.

Each build runs in its own OS process so a crash (segfault, OOM kill, ``kill -9``)
cannot take down the web server, and so the server can prove a stale worker's
late result is rejected: publication is a metadata transaction gated on the
build's ``running`` state and on its secret ``token``.  A worker that dies leaves
the published pointer untouched.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from .store import Store


def worker_command(root: str, build_id: int) -> list[str]:
    here = Path(__file__).resolve().parent.parent
    return [sys.executable, "-c",
            "import sys; sys.path.insert(0, %r); "
            "from strata.worker import main; main()" % str(here),
            str(Path(root).resolve()), str(build_id)]


class WorkerLauncher:
    def __init__(self, store: Store):
        self.store = store
        self.procs: dict[int, subprocess.Popen] = {}

    def spawn(self, build_id: int) -> subprocess.Popen:
        cmd = worker_command(str(self.store.root), build_id)
        logdir = self.store.root / "logs"
        logdir.mkdir(exist_ok=True)
        log = open(logdir / f"build-{build_id}.log", "ab")
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                             close_fds=True, env=env)
        self.procs[build_id] = p
        return p

    def run_inline(self, build_id: int, **kwargs) -> dict:
        """Used by tests and by the ``--inline`` server mode."""
        from .engine import execute_build
        return execute_build(self.store, build_id, **kwargs)

    def wait(self, build_id: int, timeout: float = 120.0) -> Optional[int]:
        p = self.procs.get(build_id)
        if p is None:
            return None
        try:
            return p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

    def signal(self, build_id: int, sig: int = signal.SIGTERM) -> None:
        p = self.procs.get(build_id)
        if p and p.poll() is None:
            try:
                p.send_signal(sig)
            except ProcessLookupError:
                pass

    def reap(self) -> list[int]:
        done = []
        for bid, p in list(self.procs.items()):
            if p.poll() is not None:
                done.append(bid)
                self.procs.pop(bid, None)
        return done
