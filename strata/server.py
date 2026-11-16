"""HTTP API and static server (standard library only).

Routes
------
GET  /                              single-page UI
GET  /static/<file>                 assets
GET  /api/state                     everything the UI needs in one poll
GET  /api/model/<table>             source SQL + lineage + sample rows
PUT  /api/model/<table>             save draft model text (validated)
POST /api/edits                     create draft edit from published snapshot
GET  /api/edits/<id>                draft bodies (also PUT to overwrite one)
GET  /api/edits/<id>/graph          draft dependency graph incl. new tables
GET  /api/edits/<id>/plan           planned rebuild set, groups and wait/cost
GET  /api/compare?a=&b=             row-level diff of two snapshots
POST /api/edits/<id>/tables         save a table's body/kind/version in an edit
POST /api/edits/<id>/preview        run a preview build in a worker (no publish)
POST /api/edits/<id>/publish        build + publish (grouped|cutover)
POST /api/edits/<id>/discard        abandon a draft
POST /api/builds/<id>/cancel        cancel a running build
GET  /api/builds/<id>               build detail incl. plan, tables, result
POST /api/increments                append source rows
POST /api/reconcile                 build from current model text (no edit)
GET  /api/tables/<name>             rows of a published table from a snapshot
GET  /api/snapshots                 generation history
POST /api/rollback                  publish a new generation pointing at an old one
POST /api/analyze                   parse SQL + field-level impact (no build)
"""
from __future__ import annotations

import json
import os
import re
import shutil
import threading
import time
import traceback
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

from . import engine
from .sqlparse import ParseError, parse_script
from .store import NAME_RE, Store, now
from .worker_launch import WorkerLauncher

WEB_ROOT = Path(__file__).resolve().parent.parent / "web"
MAX_BODY = 4 << 20


class ApiError(Exception):
    def __init__(self, message: str, status: int = 400, extra: dict | None = None):
        super().__init__(message)
        self.message = message
        self.status = status
        self.extra = extra or {}


def json_safe(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: json_safe(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [json_safe(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "replace")
    return v


class App:
    def __init__(self, root: str | Path, *, inline_workers: bool = False):
        self.store = Store(root)
        self.launcher = WorkerLauncher(self.store)
        self.inline = inline_workers
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self.reaper = threading.Thread(target=self._reap_loop, daemon=True)
        self.reaper.start()

    # -- lifecycle --------------------------------------------------------
    def _reap_loop(self) -> None:
        while not self._stop.wait(3.0):
            if self._stop.is_set():
                break
            try:
                self.store.reap_builds(stale_after_seconds=20)
            except Exception:
                if os.environ.get("STRATA_DEBUG"):
                    traceback.print_exc()

    def close(self) -> None:
        self._stop.set()

    # -- helpers ----------------------------------------------------------
    def table_defs_from_edit(self, eid: int) -> dict[str, dict]:
        et = self.store.edit_tables(eid)
        return {n: {"version": t["version"], "kind": t["kind"], "body": t["body"]}
                for n, t in et.items()}

    def published_defs(self) -> dict[str, dict]:
        return {n: {"version": t.version, "kind": t.kind, "body": t.body}
                for n, t in self.store.snapshot_tables(
                    self.store.published_snapshot().id).items()}

    def start_build(self, eid: Optional[int], kind: str) -> dict:
        published = self.store.published_snapshot()
        # A build must start from the base the edit was authored against, not
        # from whatever is published now.  If someone else published in the
        # meantime, execute_build's expected-current check turns this into a
        # conflict instead of letting a stale draft overwrite their generation.
        if eid is not None:
            e = self.store.edit(eid)
            if e is None:
                raise ApiError("edit not found", 404)
            base_id = e["base_snapshot"]
        else:
            base_id = published.id
        with self.lock:
            running = [b for b in self.store.builds(limit=20)
                       if b.status in ("queued", "running")
                       and b.kind == "preview"]
            if kind == "preview" and running:
                raise ApiError("a preview is already running; wait for it to finish",
                               409)
            for old in self.store.builds(limit=50):
                # One build per edit: a second submit would publish the same
                # draft twice.  Explicitly finalise the stale one first.
                if (eid is not None and old.edit_id == eid
                        and old.status in ("queued", "running", "cancelling")):
                    self.store.cancel_build(old.id)
                    self.launcher.signal(old.id)
            b = self.store.create_build(eid, base_id, kind,
                                        self.store.max_source_seq())
        if self.inline:
            threading.Thread(target=self._run_inline, args=(b.id,), daemon=True).start()
        else:
            self.launcher.spawn(b.id)
        return {"build": b.id, "status": b.status, "base_snapshot": base_id,
                "mode": kind}

    def _run_inline(self, bid: int) -> None:
        try:
            engine.execute_build(self.store, bid)
        except Exception:
            traceback.print_exc()

    # ------------------------------------------------------------------
    # route handlers
    def state(self) -> dict:
        store = self.store
        snap = store.published_snapshot()
        tables = store.snapshot_tables(snap.id)
        source_defs = store.source_defs()
        source_counts = store.source_counts()
        down = {}
        try:
            models = engine.model_map({
                n: {"version": t.version, "kind": t.kind, "body": t.body}
                for n, t in tables.items()})
            down = engine.downstream_map(models)
            lv = engine.compute_levels(models)
        except ParseError as e:
            models, lv = {}, {}
        snapshots = store.snapshots(limit=30)
        edits = store.edits(limit=30)
        builds = store.builds(limit=30)
        return {
            "now": now(),
            "published": {
                "snapshot": snap.id,
                "generation": snap.generation,
                "watermark": snap.watermark,
                "created_at": snap.created_at,
                "mode": snap.mode,
            },
            "sources": [
                {"name": n, "columns": cols, "rows": source_counts.get(n, 0)}
                for n, cols in source_defs.items()
            ],
            "tables": [
                {
                    "name": t.name, "kind": t.kind, "version": t.version,
                    "schema": [c["name"] for c in t.schema],
                    "rows": t.row_count, "level": lv.get(t.name, 0),
                    "downstream": down.get(t.name, []),
                    "built_watermark": t.built_watermark,
                    "body": t.body,
                    "content_hash": t.content_hash,
                }
                for t in sorted(tables.values(), key=lambda x: (lv.get(x.name, 0),
                                                                x.name))
            ],
            "snapshots": [
                {"id": s.id, "generation": s.generation, "parent": s.parent_id,
                 "watermark": s.watermark, "mode": s.mode, "note": s.note,
                 "created_at": s.created_at,
                 "is_published": s.id == snap.id}
                for s in snapshots
            ],
            "edits": [
                {"id": e["id"], "status": e["status"], "base_snapshot": e["base_snapshot"],
                 "note": e["note"], "created_at": e["created_at"],
                 "updated_at": e["updated_at"],
                 "tables": sorted(store.edit_tables(e["id"]))}
                for e in edits
            ],
            "builds": [
                {"id": b.id, "kind": b.kind, "status": b.status,
                 "edit_id": b.edit_id, "created_at": b.created_at,
                 "finished_at": b.finished_at, "error": b.error,
                 "watermark": b.watermark}
                for b in builds
            ],
            "events": store.events(limit=40),
        }

    def analyze(self, payload: dict) -> dict:
        sql = (payload.get("sql") or "").strip()
        table = (payload.get("table") or "").strip()
        if not sql:
            raise ApiError("sql is required")
        if not table:
            raise ApiError("table is required")
        kind = payload.get("kind", "create")
        if kind not in ("create", "insert"):
            raise ApiError("kind must be 'create' or 'insert'")
        try:
            model = engine.compile_model(table, 1, kind, sql)
            engine.validate_insert_sources({table: model},
                                           set(self.store.source_defs()))
        except ParseError as e:
            raise ApiError(str(e), 422, {"parse_error": True})
        snap = self.store.published_snapshot()
        cur = self.store.snapshot_tables(snap.id)
        # The model universe is the draft being edited when one is named,
        # otherwise the newest draft, otherwise the published generation.  A
        # brand-new edit has the published bodies copied into it, so this also
        # works before anything has been published.
        universe = {n: {"version": t.version, "kind": t.kind, "body": t.body}
                    for n, t in cur.items()}
        eid = payload.get("edit_id")
        e = self.store.edit(int(eid)) if eid else None
        if e is None:
            drafts = [x for x in self.store.edits(limit=20)
                      if x["status"] == "draft"]
            e = self.store.edit(drafts[0]["id"]) if drafts else None
        if e is not None:
            universe = {n: {"version": t["version"], "kind": t["kind"],
                            "body": t["body"]}
                        for n, t in self.store.edit_tables(e["id"]).items()}
            if table not in universe and table in cur:
                universe[table] = {"version": cur[table].version,
                                   "kind": cur[table].kind,
                                   "body": cur[table].body}
        source_columns = {s: [c["name"] for c in cols]
                          for s, cols in self.store.source_defs().items()}
        try:
            models = engine.model_map(universe)
        except ParseError as exc:
            raise ApiError(str(exc), 422, {"parse_error": True})
        models[table] = model
        source_names = set(source_columns)
        unknown = sorted({t for m in models.values() for t in m.tables
                          if t not in models and t not in source_names
                          and t not in cur})
        if unknown:
            raise ApiError(
                "unknown source or table referenced: " + ", ".join(unknown)
                + ". Declare a source or add the upstream table first.",
                422, {"parse_error": True, "unknown": unknown})
        try:
            lv = engine.compute_levels(models)
        except ParseError as e:
            raise ApiError(str(e), 422, {"parse_error": True})
        lineage, edges = engine.column_lineage(models, source_columns)
        mine = lineage.get(table, {})
        affected = sorted(engine.downstream_closure(models, {table}) - {table})
        old = cur.get(table)
        old_cols = ([c["name"] for c in old.schema] if old is not None else
                    [i.alias for i in models[table].select.items]
                    if table in universe and table not in cur else [])
        new_cols = [i.alias for i in model.select.items]
        impact = {
            "table": table,
            "columns": [
                {"name": c, "from": mine.get(c, {}).get("from", []),
                 "via": mine.get(c, {}).get("via", [])}
                for c in new_cols
            ],
            "added_columns": [c for c in new_cols if c not in old_cols],
            "removed_columns": [c for c in old_cols if c not in new_cols],
            "downstream": affected,
            "levels": {n: lv[n] for n in affected + [table]},
            "chain_depth": max([lv.get(n, 0) for n in affected] or [0]),
            "edges": [e for e in edges if e["from"] == table or e["to"] == table],
            "sources": model.tables,
        }
        # Breakage: does any consumer select a column that no longer exists?
        problems = []
        schema_preview = dict(source_columns)
        for n in models:
            if n == table:
                schema_preview[n] = new_cols
            elif n in cur:
                schema_preview[n] = [c["name"] for c in cur[n].schema]
            elif n in universe:
                try:
                    schema_preview[n] = engine.compile_model(
                        n, 1, universe[n]["kind"], universe[n]["body"]
                    ).select.output_names()
                except ParseError:
                    schema_preview[n] = []
        problems = engine.consumer_breakage(models, schema_preview)
        return {"ok": True, "kind": kind, "target": model.stmt.target,
                "impact": impact, "problems": problems,
                "statement": model.stmt.kind,
                "sql": model.select.sql}

    def table_rows(self, name: str, snapshot: Optional[int],
                   limit: int = 50) -> dict:
        snap = self.store.snapshot(int(snapshot)) if snapshot else \
            self.store.published_snapshot()
        if not snap:
            raise ApiError("snapshot not found", 404)
        tables = self.store.snapshot_tables(snap.id)
        if name not in tables:
            raise ApiError(f"table {name!r} is not in snapshot {snap.id}", 404)
        t = tables[name]
        p = self.store.physical_path(t.physical)
        rows = engine.sample_rows(p, name, limit=max(1, min(limit, 500)))
        return {"snapshot": snap.id, "generation": snap.generation,
                "table": name, "kind": t.kind, "version": t.version,
                "schema": [c["name"] for c in t.schema],
                "row_count": t.row_count, "rows": rows, "body": t.body}

    def build_detail(self, bid: int) -> dict:
        b = self.store.build(bid)
        if not b:
            raise ApiError("build not found", 404)
        plan = self.store.build_plan(bid)
        tables = self.store.build_tables(bid)
        return {
            "id": b.id, "kind": b.kind, "status": b.status, "edit_id": b.edit_id,
            "base_snapshot": b.base_snapshot, "watermark": b.watermark,
            "created_at": b.created_at, "started_at": b.started_at,
            "finished_at": b.finished_at, "error": b.error,
            "result": b.result, "plan": plan, "tables": tables,
            "worker_pid": b.worker_pid,
        }

    def edit_graph(self, eid: int) -> dict:
        """Dependency graph of the draft, including unpublished models.

        The graph is computed from the draft bodies (so a brand-new table is
        visible before its first build), with published row counts attached
        where a table already exists.
        """
        store = self.store
        e = store.edit(eid)
        if e is None:
            raise ApiError("edit not found", 404)
        defs = {n: {"version": t["version"], "kind": t["kind"], "body": t["body"]}
                for n, t in store.edit_tables(eid).items()}
        try:
            models = engine.model_map(defs)
            levels = engine.compute_levels(models)
            down = engine.downstream_map(models)
        except ParseError as exc:
            raise ApiError(str(exc), 422, {"parse_error": True})
        pub = store.snapshot_tables(store.published_snapshot().id)
        base = store.snapshot_tables(e["base_snapshot"])
        rows = []
        for n in sorted(models, key=lambda x: (levels[x], x)):
            m = models[n]
            b = base.get(n)
            rows.append({
                "name": n, "kind": m.kind, "version": m.version,
                "level": levels[n],
                "columns": m.select.output_names(),
                "upstream": [u for u in m.tables if u in models],
                "reads_sources": [u for u in m.tables if u not in models],
                "downstream": down.get(n, []),
                "published_rows": pub[n].row_count if n in pub else None,
                "published_version": pub[n].version if n in pub else None,
                "changed": (b is None or b.body != m.body or b.kind != m.kind),
                "is_new": b is None,
            })
        return {"edit": e, "tables": rows, "base_snapshot": e["base_snapshot"],
                "published_snapshot": store.published_snapshot().id}

    def compare(self, a: int, b: int, limit: int = 20) -> dict:
        """Diff two snapshots table by table.

        The comparison is executed against both physical file sets, so it works
        for any two generations (including a rollback target) and does not rely
        on the current models still describing the old snapshot.
        """
        sa, sb = self.store.snapshot(a), self.store.snapshot(b)
        if sa is None or sb is None:
            raise ApiError("snapshot not found", 404)
        ta, tb = self.store.snapshot_tables(a), self.store.snapshot_tables(b)
        out: list[dict] = []
        for name in sorted(set(ta) | set(tb)):
            x, y = ta.get(name), tb.get(name)
            entry: dict[str, Any] = {
                "table": name,
                "only_in": None if x and y else ("a" if x else "b"),
                "version_a": x.version if x else None,
                "version_b": y.version if y else None,
                "rows_a": x.row_count if x else None,
                "rows_b": y.row_count if y else None,
                "body_changed": bool(x and y and x.body != y.body),
                "schema_a": [c["name"] for c in x.schema] if x else [],
                "schema_b": [c["name"] for c in y.schema] if y else [],
                "same_content": bool(x and y and x.content_hash == y.content_hash),
            }
            if x and y:
                added, removed = self._row_delta(x, y, name, limit)
                entry["rows_added"], entry["rows_removed"] = added, removed
            else:
                entry["rows_added"], entry["rows_removed"] = [], []
            out.append(entry)
        return {
            "a": {"snapshot": sa.id, "generation": sa.generation,
                  "watermark": sa.watermark, "mode": sa.mode, "note": sa.note,
                  "created_at": sa.created_at},
            "b": {"snapshot": sb.id, "generation": sb.generation,
                  "watermark": sb.watermark, "mode": sb.mode, "note": sb.note,
                  "created_at": sb.created_at},
            "tables": out,
            "differing": [e["table"] for e in out
                          if e["only_in"] or not e["same_content"]
                          or e["body_changed"]],
        }

    def _row_delta(self, x: Any, y: Any, name: str, limit: int
                   ) -> tuple[list[dict], list[dict]]:
        """Row-level delta between two immutable files of the same table.

        Uses SQLite's EXCEPT over all shared columns; this is a real execution
        against the two version files rather than a metadata-only comparison.
        """
        import sqlite3 as _sqlite
        shared = [c for c in (c["name"] for c in x.schema)
                  if c in {d["name"] for d in y.schema}]
        if not shared:
            return [], []
        cols = ", ".join(engine.quote(c) for c in shared)
        pa = self.store.physical_path(x.physical)
        pb = self.store.physical_path(y.physical)
        conn = _sqlite.connect(":memory:")
        try:
            conn.execute("ATTACH DATABASE ? AS a", (f"file:{pa}?mode=ro",))
            conn.execute("ATTACH DATABASE ? AS b", (f"file:{pb}?mode=ro",))
            q = engine.quote(name)
            add = [dict(zip(shared, r)) for r in conn.execute(
                f"SELECT {cols} FROM b.{q} EXCEPT SELECT {cols} FROM a.{q} "
                f"LIMIT {int(limit)}")]
            rem = [dict(zip(shared, r)) for r in conn.execute(
                f"SELECT {cols} FROM a.{q} EXCEPT SELECT {cols} FROM b.{q} "
                f"LIMIT {int(limit)}")]
            return add, rem
        except _sqlite.Error as e:
            raise ApiError(f"cannot compare table {name!r}: {e}", 409)
        finally:
            conn.close()

    def preview_diff(self, eid: int, build_id: int) -> dict:
        d = self.build_detail(build_id)
        if d["status"] not in ("ready", "failed", "cancelled", "conflict"):
            raise ApiError("preview is still running", 409)
        return d


# ---------------------------------------------------------------------------
# HTTP layer


class Handler(BaseHTTPRequestHandler):
    app: App
    server_version = "Strata/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("STRATA_DEBUG"):
            super().log_message(fmt, *args)

    # -- plumbing ---------------------------------------------------------
    def _send(self, status: int, body: bytes, ctype: str,
              extra: Optional[dict] = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, obj: Any, status: int = 200) -> None:
        body = json.dumps(json_safe(obj), ensure_ascii=False).encode()
        self._send(status, body, "application/json; charset=utf-8")

    def _error(self, status: int, message: str, **extra: Any) -> None:
        self._json({"ok": False, "error": message, **extra}, status)

    def _read_json(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0:
            return {}
        if n > MAX_BODY:
            raise ApiError("request body too large", 413)
        raw = self.rfile.read(n)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ApiError("body must be valid JSON", 400)
        if not isinstance(data, dict):
            raise ApiError("body must be a JSON object", 400)
        return data

    # -- routing ----------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        self._route("GET")

    def do_HEAD(self) -> None:  # noqa: N802
        self._route("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._route("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._route("PUT")

    def _route(self, method: str) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if method == "GET" and path == "/":
                return self._static("index.html")
            if method == "GET" and path.startswith("/static/"):
                return self._static(path[len("/static/"):])
            if not path.startswith("/api/"):
                return self._error(404, "not found")
            return self._api(method, path[len("/api/"):], query)
        except ApiError as e:
            self._error(e.status, e.message, **e.extra)
        except ParseError as e:
            self._error(422, str(e), parse_error=True)
        except KeyError as e:
            self._error(404, f"not found: {e}")
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:  # noqa: BLE001
            if os.environ.get("STRATA_DEBUG"):
                traceback.print_exc()
            self._error(500, f"{type(e).__name__}: {e}")

    def _static(self, rel: str) -> None:
        target = (WEB_ROOT / rel).resolve()
        if not str(target).startswith(str(WEB_ROOT.resolve())) or not target.is_file():
            return self._error(404, "not found")
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "text/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".svg": "image/svg+xml",
            ".png": "image/png",
            ".ico": "image/x-icon",
        }.get(target.suffix, "application/octet-stream")
        self._send(200, target.read_bytes(), ctype)

    def _api(self, method: str, path: str, q: dict) -> None:
        app = self.app
        store = app.store
        parts = [p for p in path.split("/") if p]

        def qint(key: str, default: Optional[int] = None) -> Optional[int]:
            v = q.get(key, [None])[0]
            if v is None or v == "":
                return default
            try:
                return int(v)
            except ValueError:
                raise ApiError(f"{key} must be an integer")

        if method == "GET" and parts == ["state"]:
            return self._json(app.state())
        if method == "GET" and parts == ["health"]:
            return self._json({"ok": True, "time": now()})
        if method == "GET" and parts == ["snapshots"]:
            return self._json({"snapshots": app.state()["snapshots"]})
        if method == "GET" and len(parts) == 2 and parts[0] == "tables":
            return self._json(app.table_rows(parts[1], qint("snapshot"),
                                             qint("limit", 50) or 50))
        if method == "GET" and len(parts) == 2 and parts[0] == "builds":
            return self._json(app.build_detail(int(parts[1])))
        if method == "GET" and parts == ["compare"]:
            a, b = qint("a"), qint("b")
            if a is None or b is None:
                raise ApiError("query parameters a and b (snapshot ids) are required")
            return self._json(app.compare(a, b, qint("limit", 20) or 20))
        if method == "GET" and len(parts) == 2 and parts[0] == "edits":
            e = store.edit(int(parts[1]))
            if not e:
                raise ApiError("edit not found", 404)
            return self._json({"edit": e, "tables": store.edit_tables(e["id"])})
        if method == "GET" and len(parts) == 2 and parts[0] == "model":
            return self._json(self._model(parts[1], qint("snapshot"),
                                          qint("edit_id")))
        if method == "POST" and parts == ["analyze"]:
            return self._json(app.analyze(self._read_json()))
        if method == "POST" and parts == ["edits"]:
            body = self._read_json()
            base = store.published_snapshot()
            eid = store.create_edit(base.id, body.get("note"))
            return self._json({"ok": True, "edit": store.edit(eid)}, 201)
        if method == "POST" and parts == ["increments", "demo"]:
            body = self._read_json()
            n = body.get("n", 5)
            try:
                n = max(1, min(int(n), 500))
            except (TypeError, ValueError):
                raise ApiError("n must be an integer")
            if "orders" not in store.source_defs():
                raise ApiError("the demo source 'orders' is not declared", 409)
            from .demo import make_increment
            res = store.append_rows(make_increment(store, n))
            return self._json({"ok": True, **res})
        if method == "POST" and parts == ["increments"]:
            body = self._read_json()
            batches = body.get("rows")
            if not isinstance(batches, dict):
                raise ApiError("rows must be an object of source -> [row, ...]")
            try:
                res = store.append_rows(batches)
            except ValueError as e:
                raise ApiError(str(e), 422)
            return self._json({"ok": True, **res})
        if method == "POST" and parts == ["reconcile"]:
            if not store.source_defs():
                raise ApiError("no sources declared", 409)
            return self._json({"ok": True, **app.start_build(None, "grouped")}, 202)
        if method == "POST" and parts == ["rollback"]:
            body = self._read_json()
            sid = body.get("snapshot")
            if not isinstance(sid, int):
                raise ApiError("snapshot (int) is required")
            # Refuse if any build is running: a rollback during a build would
            # leave that build's expected-current check stale anyway, but the UI
            # should see the reason explicitly.
            try:
                res = engine.rollback_to(store, sid, body.get("note"))
            except ValueError as e:
                raise ApiError(str(e), 409)
            return self._json({"ok": True, **res}, 201)
        if method == "POST" and parts == ["sources"]:
            body = self._read_json()
            name = body.get("name")
            cols = body.get("columns")
            if not isinstance(name, str) or not NAME_RE.match(name):
                raise ApiError("valid source name is required")
            if not isinstance(cols, list) or not cols:
                raise ApiError("columns must be a non-empty list")
            clean = []
            for c in cols:
                if not isinstance(c, dict) or not isinstance(c.get("name"), str) \
                        or not NAME_RE.match(c["name"]):
                    raise ApiError(f"invalid column definition: {c!r}")
                t = str(c.get("type", "TEXT")).upper()
                if t not in engine.TYPE_CASTS:
                    raise ApiError(f"unsupported column type {t!r}")
                clean.append({"name": c["name"], "type": t})
            if len({c["name"] for c in clean}) != len(clean):
                raise ApiError("duplicate column names")
            try:
                store.create_source(name, clean)
            except Exception as e:  # sqlite IntegrityError
                raise ApiError(f"source already exists: {name}", 409)
            return self._json({"ok": True, "source": {"name": name,
                                                      "columns": clean}}, 201)
        if method == "POST" and parts == ["seed"]:
            body = self._read_json()
            demo = body.get("demo", "orders")
            from .demo import seed_demo
            eid = seed_demo(store, demo)
            return self._json({"ok": True, "edit": store.edit(eid)}, 201)

        # /edits/<id>/...
        if len(parts) >= 3 and parts[0] == "edits":
            eid = int(parts[1])
            edit = store.edit(eid)
            if not edit:
                raise ApiError("edit not found", 404)
            action = parts[2]
            if method == "GET" and action == "graph":
                return self._json(app.edit_graph(eid))
            if method == "GET" and action == "plan":
                try:
                    plan = engine.compute_plan(
                        store, edit_id=eid, base_snapshot=edit["base_snapshot"],
                        table_defs=app.table_defs_from_edit(eid),
                        mode=q.get("mode", ["grouped"])[0])
                except engine.PlanError as e:
                    raise ApiError(str(e), 422)
                return self._json({"ok": True, "plan": plan.to_json()})
            if method in ("POST", "PUT") and action == "tables":
                body = self._read_json()
                name = body.get("name")
                sql = body.get("sql")
                kind = body.get("kind", "create")
                if not isinstance(name, str) or not NAME_RE.match(name):
                    raise ApiError("valid table name is required")
                if not isinstance(sql, str) or not sql.strip():
                    raise ApiError("sql is required")
                if kind not in ("create", "insert"):
                    raise ApiError("kind must be 'create' or 'insert'")
                try:
                    engine.compile_model(name, 1, kind, sql)
                    engine.validate_insert_sources(
                        {name: engine.compile_model(name, 1, kind, sql)},
                        set(store.source_defs()))
                except ParseError as e:
                    raise ApiError(str(e), 422, {"parse_error": True, "table": name})
                base_tables = store.snapshot_tables(edit["base_snapshot"])
                prev = base_tables.get(name)
                version = (prev.version + 1) if prev else 1
                try:
                    store.save_edit_table(eid, name, sql, kind, version)
                except ValueError as e:
                    raise ApiError(str(e), 409)
                return self._json({"ok": True, "table": name, "version": version})
            if method == "POST" and action == "delete":
                body = self._read_json()
                name = body.get("name")
                if not isinstance(name, str):
                    raise ApiError("name is required")
                base_tables = store.snapshot_tables(edit["base_snapshot"])
                if name not in base_tables:
                    raise ApiError("only tables added by this edit can be removed")
                store.remove_edit_table(eid, name)
                return self._json({"ok": True})
            if method == "POST" and action == "preview":
                table_defs = app.table_defs_from_edit(eid)
                try:
                    engine.compute_plan(store, edit_id=eid,
                                        base_snapshot=edit["base_snapshot"],
                                        table_defs=table_defs, mode="grouped")
                except engine.PlanError as e:
                    raise ApiError(str(e), 422)
                try:
                    res = app.start_build(eid, "preview")
                except engine.PlanError as e:
                    raise ApiError(str(e), 422)
                return self._json({"ok": True, **res}, 202)
            if method == "POST" and action == "rebase":
                try:
                    res = self._rebase(eid)
                except engine.PlanError as e:
                    raise ApiError(str(e), 422)
                return self._json({"ok": True, **res})
            if method == "POST" and action == "publish":
                body = self._read_json()
                mode = body.get("mode", "grouped")
                if mode not in ("grouped", "cutover"):
                    raise ApiError("mode must be 'grouped' or 'cutover'")
                if edit["status"] != "draft":
                    raise ApiError(f"edit is {edit['status']}", 409)
                # A stale draft is not an error at submit time: queue the build
                # against its own base so the worker reports an explicit
                # conflict (and the UI can offer "rebase"), rather than turning
                # a legitimate race into a bare 422.
                if edit["base_snapshot"] == store.published_snapshot().id:
                    try:
                        engine.compute_plan(store, edit_id=eid,
                                            base_snapshot=edit["base_snapshot"],
                                            table_defs=app.table_defs_from_edit(eid),
                                            mode=mode)
                    except engine.PlanError as e:
                        raise ApiError(str(e), 422)
                return self._json({"ok": True, **app.start_build(eid, mode)}, 202)
            if method == "POST" and action == "discard":
                if edit["status"] != "draft":
                    raise ApiError(f"edit is {edit['status']}; only a draft can "
                                   "be discarded", 409)
                store.set_edit_status(eid, "discarded")
                return self._json({"ok": True})
        if method == "POST" and len(parts) == 3 and parts[0] == "builds" \
                and parts[2] == "cancel":
            bid = int(parts[1])
            status = store.cancel_build(bid)
            self.app.launcher.signal(bid)
            return self._json({"ok": True, "status": status})
        raise ApiError("no such endpoint", 404)

    def _model(self, name: str, snapshot: Optional[int],
               edit_id: Optional[int] = None) -> dict:
        """One table's model for the editor.

        With ``edit_id`` the draft body is returned (so unsaved-edit context is
        correct); otherwise the published snapshot.  Lineage and the dependency
        graph are always computed from the same universe the body belongs to.
        """
        store = self.app.store
        e = store.edit(edit_id) if edit_id else None
        if edit_id and e is None:
            raise ApiError("edit not found", 404)
        source_defs = store.source_defs()
        source_columns = {s: [c["name"] for c in cols]
                          for s, cols in source_defs.items()}
        if e is not None:
            universe = {n: {"version": x["version"], "kind": x["kind"],
                            "body": x["body"]}
                        for n, x in store.edit_tables(e["id"]).items()}
            if name not in universe:
                raise ApiError(f"table {name!r} is not in this draft", 404)
            u = universe[name]
            models = engine.model_map(universe)
            lv = engine.compute_levels(models)
            down = engine.downstream_map(models)
            lineage, _ = engine.column_lineage(models, source_columns)
            return {
                "table": name, "kind": u["kind"], "version": u["version"],
                "body": u["body"],
                "schema": models[name].select.output_names(),
                "rows": None, "level": lv.get(name, 0),
                "lineage": lineage.get(name, {}),
                "upstream": [u2 for u2 in models[name].tables],
                "downstream": down.get(name, []),
                "edit_id": e["id"], "base_snapshot": e["base_snapshot"],
                "status": e["status"],
            }
        snap = store.snapshot(snapshot) if snapshot else store.published_snapshot()
        tables = store.snapshot_tables(snap.id)
        if name not in tables:
            raise ApiError("table not found in snapshot", 404)
        t = tables[name]
        models = engine.model_map({
            n: {"version": x.version, "kind": x.kind, "body": x.body}
            for n, x in tables.items()})
        lineage, _ = engine.column_lineage(models, source_columns)
        lv = engine.compute_levels(models)
        down = engine.downstream_map(models)
        return {
            "table": t.name, "kind": t.kind, "version": t.version, "body": t.body,
            "schema": [c["name"] for c in t.schema], "rows": t.row_count,
            "level": lv.get(name, 0),
            "lineage": lineage.get(name, {}),
            "upstream": [u for u in models[name].tables],
            "downstream": down.get(name, []),
            "snapshot": snap.id,
        }

    def _rebase(self, eid: int) -> dict:
        """Move a draft edit onto the current published snapshot.

        Tables whose published body is identical to the draft's base are taken
        from the current snapshot; tables the user edited keep their draft body
        but are revalidated and their target version is left for the next save.
        """
        store = self.app.store
        edit = store.edit(eid)
        if not edit:
            raise ApiError("edit not found", 404)
        if edit["status"] != "draft":
            raise ApiError(f"edit is {edit['status']}", 409)
        cur = store.published_snapshot()
        cur_tables = store.snapshot_tables(cur.id)
        draft = store.edit_tables(eid)
        base_tables = store.snapshot_tables(edit["base_snapshot"])
        merged: dict[str, dict] = {}
        conflicts: list[dict] = []
        for n, d in draft.items():
            b = base_tables.get(n)
            c = cur_tables.get(n)
            if b is not None and b.body == d["body"] and b.kind == d["kind"]:
                if c is None:
                    conflicts.append({"table": n,
                                      "reason": "table no longer published"})
                    continue
                merged[n] = {"version": d["version"], "kind": c.kind,
                             "body": c.body}
            else:
                if c is None:
                    merged[n] = {"version": d["version"], "kind": d["kind"],
                                 "body": d["body"]}
                    continue
                if c.body == d["body"] and c.kind == d["kind"]:
                    merged[n] = {"version": d["version"], "kind": d["kind"],
                                 "body": d["body"]}
                else:
                    conflicts.append({
                        "table": n,
                        "reason": "the table changed upstream and this edit also "
                                  "changed it",
                        "edit_body": d["body"], "published_body": c.body,
                    })
                    merged[n] = {"version": d["version"], "kind": d["kind"],
                                 "body": d["body"]}
        for n, c in cur_tables.items():
            if n not in merged:
                merged[n] = {"version": c.version, "kind": c.kind, "body": c.body}
        try:
            engine.validate_insert_sources(
                engine.model_map(merged), set(store.source_defs()))
            engine.compute_plan(store, edit_id=eid, base_snapshot=cur.id,
                                table_defs=merged, mode="grouped")
        except ParseError as e:
            raise ApiError(str(e), 422)
        store.update_edit_base(eid, cur.id, merged)
        return {"edit": store.edit(eid), "conflicts": conflicts,
                "base_snapshot": cur.id}


def make_server(root: str, host: str = "127.0.0.1", port: int = 8765,
                inline_workers: bool = False) -> ThreadingHTTPServer:
    app = App(root, inline_workers=inline_workers)
    handler = type("BoundHandler", (Handler,), {"app": app})
    srv = ThreadingHTTPServer((host, port), handler)
    srv.daemon_threads = True
    srv.app = app  # type: ignore[attr-defined]
    return srv


def serve(root: str, host: str = "127.0.0.1", port: int = 8765,
          inline_workers: bool = False) -> None:
    srv = make_server(root, host, port, inline_workers)
    app: App = srv.app  # type: ignore[attr-defined]
    print(f"strata listening on http://{host}:{port}  (data: {app.store.root})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.close()
        srv.server_close()
