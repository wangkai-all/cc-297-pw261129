"""Metadata store and source-data store.

Two SQLite databases:

* ``meta.db``  – sources, edits, builds, snapshots and their table mappings.
  Publication is a single ``BEGIN IMMEDIATE`` transaction on this file, which is
  what makes a generation switch atomic.
* ``sources.db`` – append-only source rows.  A row's ``seq`` is its arrival
  watermark; builds read a fixed ``seq <= H`` window, so increments that land
  while a build is running are simply not part of that build.

Every materialized table lives in its own SQLite file under ``generations/`` so a
build can write shadow files without touching what consumers are reading.
"""
from __future__ import annotations

import calendar
import json
import random
import re
import secrets
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

META_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sources(
  name TEXT PRIMARY KEY,
  columns_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  generation INTEGER NOT NULL UNIQUE,
  parent_id INTEGER,
  edit_id INTEGER,
  build_id INTEGER,
  watermark INTEGER NOT NULL,
  horizons_json TEXT NOT NULL DEFAULT '{}',
  mode TEXT NOT NULL,
  note TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshot_tables(
  snapshot_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  version INTEGER NOT NULL,
  kind TEXT NOT NULL,
  body TEXT NOT NULL,
  schema_json TEXT NOT NULL,
  physical TEXT NOT NULL,
  content_hash TEXT,
  row_count INTEGER,
  built_watermark INTEGER NOT NULL,
  PRIMARY KEY(snapshot_id, name)
);
CREATE TABLE IF NOT EXISTS edits(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  base_snapshot INTEGER NOT NULL,
  status TEXT NOT NULL,
  note TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS edit_tables(
  edit_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  version INTEGER NOT NULL,
  kind TEXT NOT NULL,
  body TEXT NOT NULL,
  PRIMARY KEY(edit_id, name)
);
CREATE TABLE IF NOT EXISTS builds(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  edit_id INTEGER,
  base_snapshot INTEGER NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  watermark INTEGER NOT NULL,
  token TEXT NOT NULL,
  worker_pid INTEGER,
  created_at TEXT NOT NULL,
  started_at TEXT,
  heartbeat_at TEXT,
  finished_at TEXT,
  error TEXT,
  result_json TEXT,
  tmp_dir TEXT,
  plan_json TEXT,
  publish_json TEXT
);

CREATE TABLE IF NOT EXISTS build_tables(
  build_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  level INTEGER NOT NULL,
  strategy TEXT NOT NULL,
  status TEXT NOT NULL,
  rows_in INTEGER,
  rows_out INTEGER,
  duration_ms INTEGER,
  physical TEXT,
  content_hash TEXT,
  error TEXT,
  PRIMARY KEY(build_id, name)
);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
"""

SOURCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_rows(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS source_rows_source_seq ON source_rows(source, seq);
CREATE TABLE IF NOT EXISTS source_counters(
  source TEXT PRIMARY KEY,
  last_seq INTEGER NOT NULL
);
"""

TERMINAL_STATUSES = {
    "published", "ready", "failed", "cancelled", "conflict", "crashed",
}


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def new_token() -> str:
    return secrets.token_hex(16)


@dataclass
class Snapshot:
    id: int
    generation: int
    parent_id: Optional[int]
    edit_id: Optional[int]
    build_id: Optional[int]
    watermark: int
    horizons: dict[str, int]
    mode: str
    note: Optional[str]
    created_at: str


@dataclass
class SnapshotTable:
    name: str
    version: int
    kind: str
    body: str
    schema: list[dict]
    physical: str
    content_hash: Optional[str]
    row_count: Optional[int]
    built_watermark: int


@dataclass
class Build:
    id: int
    edit_id: Optional[int]
    base_snapshot: int
    kind: str
    status: str
    watermark: int
    token: str
    worker_pid: Optional[int]
    created_at: str
    started_at: Optional[str]
    heartbeat_at: Optional[str]
    finished_at: Optional[str]
    error: Optional[str]
    result: Optional[dict]
    tmp_dir: Optional[str]


class Store:
    def __init__(self, data_root: str | Path):
        self.root = Path(data_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "generations").mkdir(exist_ok=True)
        (self.root / "tmp").mkdir(exist_ok=True)
        self.meta_path = self.root / "meta.db"
        self.sources_path = self.root / "sources.db"
        self._init()

    # -- connections ------------------------------------------------------
    def _connect(self, path: Path, readonly: bool = False) -> sqlite3.Connection:
        if readonly:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
        else:
            conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        if not readonly:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
        return conn

    def meta(self) -> sqlite3.Connection:
        return self._connect(self.meta_path)

    def sources(self) -> sqlite3.Connection:
        return self._connect(self.sources_path)

    def _init(self) -> None:
        c = self.meta()
        try:
            c.executescript(META_SCHEMA)
            c.execute(
                "INSERT OR IGNORE INTO meta(key,value) VALUES('schema_version','1')"
            )
            row = c.execute("SELECT value FROM meta WHERE key='published_snapshot'").fetchone()
            if not row:
                c.execute("BEGIN IMMEDIATE")
                cur = c.execute(
                    "INSERT INTO snapshots(generation,parent_id,edit_id,build_id,"
                    "watermark,mode,note,created_at) VALUES(0,NULL,NULL,NULL,0,"
                    "'initial','empty warehouse',?)",
                    (now(),),
                )
                sid = cur.lastrowid
                c.execute(
                    "INSERT INTO meta(key,value) VALUES('published_snapshot',?)",
                    (str(sid),),
                )
                c.execute("COMMIT")
        finally:
            c.close()
        s = self.sources()
        try:
            s.executescript(SOURCE_SCHEMA)
        finally:
            s.close()

    @contextmanager
    def write_txn(self) -> Iterator[sqlite3.Connection]:
        c = self.meta()
        try:
            c.execute("BEGIN IMMEDIATE")
            yield c
            c.execute("COMMIT")
        except BaseException:
            try:
                c.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            c.close()

    # -- events -----------------------------------------------------------
    def log(self, kind: str, payload: dict[str, Any]) -> None:
        with self.write_txn() as c:
            c.execute(
                "INSERT INTO events(at,kind,payload_json) VALUES(?,?,?)",
                (now(), kind, json.dumps(payload, ensure_ascii=False)),
            )

    def events(self, limit: int = 100) -> list[dict]:
        c = self.meta()
        try:
            rows = c.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [
                {"id": r["id"], "at": r["at"], "kind": r["kind"],
                 "payload": json.loads(r["payload_json"])}
                for r in rows
            ]
        finally:
            c.close()

    # -- meta keys --------------------------------------------------------
    def get_meta(self, key: str, default: Optional[str] = None) -> Optional[str]:
        c = self.meta()
        try:
            row = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row["value"] if row else default
        finally:
            c.close()

    # -- sources ----------------------------------------------------------
    def create_source(self, name: str, columns: list[dict]) -> None:
        if not NAME_RE.match(name):
            raise ValueError(f"invalid source name {name!r}")
        if any(c["name"] == "_src_seq" for c in columns):
            raise ValueError("_src_seq is reserved and cannot be a source column")
        cols_json = json.dumps(columns, ensure_ascii=False)
        with self.write_txn() as c:
            c.execute(
                "INSERT INTO sources(name,columns_json,created_at) VALUES(?,?,?)",
                (name, cols_json, now()),
            )

    def source_defs(self) -> dict[str, list[dict]]:
        c = self.meta()
        try:
            return {
                r["name"]: json.loads(r["columns_json"])
                for r in c.execute("SELECT * FROM sources ORDER BY name")
            }
        finally:
            c.close()

    def source_counts(self) -> dict[str, int]:
        c = self.sources()
        try:
            return {
                r["source"]: r["n"]
                for r in c.execute(
                    "SELECT source, COUNT(*) AS n FROM source_rows GROUP BY source"
                )
            }
        finally:
            c.close()

    def max_source_seq(self) -> int:
        c = self.sources()
        try:
            row = c.execute("SELECT COALESCE(MAX(seq),0) AS m FROM source_rows").fetchone()
            return int(row["m"])
        finally:
            c.close()

    def source_horizons(self) -> dict[str, int]:
        c = self.sources()
        try:
            rows = c.execute(
                "SELECT source, COALESCE(MAX(seq),0) AS m FROM source_rows "
                "GROUP BY source").fetchall()
            return {r["source"]: int(r["m"]) for r in rows}
        finally:
            c.close()

    def rows_between(self, source: str, lower: int, upper: int,
                     limit: Optional[int] = None) -> list[dict]:
        c = self.sources()
        try:
            q = ("SELECT seq,data FROM source_rows WHERE source=? AND seq>? "
                 "AND seq<=? ORDER BY seq")
            params: list[Any] = [source, lower, upper]
            if limit is not None:
                q += " LIMIT ?"
                params.append(limit)
            return [
                {"seq": r["seq"], "data": json.loads(r["data"])}
                for r in c.execute(q, params)
            ]
        finally:
            c.close()

    def rows_since(self, source: str, watermark: int, limit: Optional[int] = None) -> list[dict]:
        c = self.sources()
        try:
            q = "SELECT seq,data FROM source_rows WHERE source=? AND seq>? ORDER BY seq"
            params: list[Any] = [source, watermark]
            if limit is not None:
                q += " LIMIT ?"
                params.append(limit)
            return [
                {"seq": r["seq"], "data": json.loads(r["data"])}
                for r in c.execute(q, params)
            ]
        finally:
            c.close()

    def append_rows(self, batches: dict[str, list[dict]]) -> dict[str, Any]:
        """Append source rows atomically.  Returns allocated per-source seqs."""
        defs = self.source_defs()
        prepared: list[tuple[str, str]] = []
        for source, rows in batches.items():
            if source not in defs:
                raise ValueError(f"unknown source {source!r}")
            allowed = {c["name"] for c in defs[source]}
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError("each increment row must be a JSON object")
                unknown = set(row) - allowed
                if unknown:
                    raise ValueError(
                        f"unknown columns for source {source!r}: {sorted(unknown)}"
                    )
                prepared.append((source, json.dumps(row, ensure_ascii=False)))
        if not prepared:
            return {"inserted": 0, "watermark": self.max_source_seq(), "sources": {}}

        c = self.sources()
        try:
            c.execute("BEGIN IMMEDIATE")
            per: dict[str, list[int]] = {}
            for source, payload in prepared:
                cur = c.execute(
                    "INSERT INTO source_rows(source,data) VALUES(?,?)", (source, payload)
                )
                seq = int(cur.lastrowid)
                per.setdefault(source, []).append(seq)
                c.execute(
                    "INSERT INTO source_counters(source,last_seq) VALUES(?,?) "
                    "ON CONFLICT(source) DO UPDATE SET last_seq=excluded.last_seq",
                    (source, seq),
                )
            h = c.execute("SELECT MAX(seq) AS m FROM source_rows").fetchone()["m"]
            c.execute("COMMIT")
        except BaseException:
            try:
                c.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            c.close()
        self.log("increments", {"count": len(prepared),
                                "sources": {k: len(v) for k, v in per.items()},
                                "watermark": h})
        return {"inserted": len(prepared), "watermark": int(h),
                "sources": {k: len(v) for k, v in per.items()}}

    # -- snapshots --------------------------------------------------------
    def snapshot(self, sid: int) -> Optional[Snapshot]:
        c = self.meta()
        try:
            r = c.execute("SELECT * FROM snapshots WHERE id=?", (sid,)).fetchone()
            return self._snapshot_row(r) if r else None
        finally:
            c.close()

    @staticmethod
    def _snapshot_row(r: sqlite3.Row) -> Snapshot:
        return Snapshot(
            id=r["id"], generation=r["generation"], parent_id=r["parent_id"],
            edit_id=r["edit_id"], build_id=r["build_id"], watermark=r["watermark"],
            horizons=json.loads(r["horizons_json"] or "{}"),
            mode=r["mode"], note=r["note"], created_at=r["created_at"],
        )

    def published_snapshot(self) -> Snapshot:
        sid = int(self.get_meta("published_snapshot", "1") or 1)
        snap = self.snapshot(sid)
        assert snap is not None, "published snapshot pointer is dangling"
        return snap

    def snapshots(self, limit: int = 100) -> list[Snapshot]:
        c = self.meta()
        try:
            rows = c.execute(
                "SELECT * FROM snapshots ORDER BY generation DESC LIMIT ?", (limit,)
            ).fetchall()
            return [self._snapshot_row(r) for r in rows]
        finally:
            c.close()

    def snapshot_tables(self, sid: int) -> dict[str, SnapshotTable]:
        c = self.meta()
        try:
            rows = c.execute(
                "SELECT * FROM snapshot_tables WHERE snapshot_id=? ORDER BY name", (sid,)
            ).fetchall()
            return {
                r["name"]: SnapshotTable(
                    name=r["name"], version=r["version"], kind=r["kind"],
                    body=r["body"], schema=json.loads(r["schema_json"]),
                    physical=r["physical"], content_hash=r["content_hash"],
                    row_count=r["row_count"], built_watermark=r["built_watermark"],
                )
                for r in rows
            }
        finally:
            c.close()

    def physical_path(self, rel: str) -> Path:
        p = (self.root / rel).resolve()
        if not str(p).startswith(str(self.root) + "/"):
            raise ValueError("physical path escapes data root")
        return p

    # -- edits ------------------------------------------------------------
    def create_edit(self, base_snapshot: int, note: str | None = None) -> int:
        base = self.snapshot_tables(base_snapshot)
        with self.write_txn() as c:
            cur = c.execute(
                "INSERT INTO edits(base_snapshot,status,note,created_at,updated_at) "
                "VALUES(?,'draft',?,?,?)",
                (base_snapshot, note, now(), now()),
            )
            eid = int(cur.lastrowid)
            for name, t in base.items():
                c.execute(
                    "INSERT INTO edit_tables(edit_id,name,version,kind,body) "
                    "VALUES(?,?,?,?,?)",
                    (eid, name, t.version, t.kind, t.body),
                )
        return eid

    def edit(self, eid: int) -> Optional[dict]:
        c = self.meta()
        try:
            r = c.execute("SELECT * FROM edits WHERE id=?", (eid,)).fetchone()
            return dict(r) if r else None
        finally:
            c.close()

    def edits(self, limit: int = 50) -> list[dict]:
        c = self.meta()
        try:
            return [dict(r) for r in c.execute(
                "SELECT * FROM edits ORDER BY id DESC LIMIT ?", (limit,)
            )]
        finally:
            c.close()

    def edit_tables(self, eid: int) -> dict[str, dict]:
        c = self.meta()
        try:
            return {
                r["name"]: {
                    "name": r["name"], "version": r["version"],
                    "kind": r["kind"], "body": r["body"],
                }
                for r in c.execute(
                    "SELECT * FROM edit_tables WHERE edit_id=? ORDER BY name", (eid,)
                )
            }
        finally:
            c.close()

    def save_edit_table(self, eid: int, name: str, body: str, kind: str,
                        version: int) -> None:
        with self.write_txn() as c:
            row = c.execute("SELECT status FROM edits WHERE id=?", (eid,)).fetchone()
            if not row:
                raise KeyError(f"edit {eid} not found")
            if row["status"] != "draft":
                raise ValueError(f"edit {eid} is {row['status']} and cannot be modified")
            c.execute(
                "INSERT INTO edit_tables(edit_id,name,version,kind,body) "
                "VALUES(?,?,?,?,?) ON CONFLICT(edit_id,name) DO UPDATE SET "
                "version=excluded.version,kind=excluded.kind,body=excluded.body",
                (eid, name, version, kind, body),
            )
            c.execute("UPDATE edits SET updated_at=? WHERE id=?", (now(), eid))

    def set_edit_status(self, eid: int, status: str) -> None:
        if status not in ("draft", "discarded", "published", "superseded"):
            raise ValueError(f"invalid edit status {status!r}")
        with self.write_txn() as c:
            c.execute("UPDATE edits SET status=?,updated_at=? WHERE id=?",
                      (status, now(), eid))

    def remove_edit_table(self, eid: int, name: str) -> None:
        with self.write_txn() as c:
            r = c.execute("SELECT status FROM edits WHERE id=?", (eid,)).fetchone()
            if not r:
                raise KeyError(f"edit {eid} not found")
            if r["status"] != "draft":
                raise ValueError(f"edit is {r['status']}")
            c.execute("DELETE FROM edit_tables WHERE edit_id=? AND name=?",
                      (eid, name))
            c.execute("UPDATE edits SET updated_at=? WHERE id=?", (now(), eid))

    def update_edit_base(self, eid: int, base_snapshot: int,
                         tables: dict[str, dict]) -> None:
        with self.write_txn() as c:
            c.execute(
                "UPDATE edits SET base_snapshot=?,updated_at=? WHERE id=?",
                (base_snapshot, now(), eid),
            )
            c.execute(
                "DELETE FROM edit_tables WHERE edit_id=? AND name NOT IN "
                f"({','.join('?' * len(tables))})",
                (eid, *tables.keys()),
            ) if tables else c.execute("DELETE FROM edit_tables WHERE edit_id=?", (eid,))
            for name, t in tables.items():
                c.execute(
                    "INSERT INTO edit_tables(edit_id,name,version,kind,body) "
                    "VALUES(?,?,?,?,?) ON CONFLICT(edit_id,name) DO UPDATE SET "
                    "version=excluded.version,kind=excluded.kind,body=excluded.body",
                    (eid, name, t["version"], t["kind"], t["body"]),
                )

    # -- builds -----------------------------------------------------------
    def create_build(self, edit_id: Optional[int], base_snapshot: int, kind: str,
                     watermark: int) -> Build:
        token = new_token()
        with self.write_txn() as c:
            cur = c.execute(
                "INSERT INTO builds(edit_id,base_snapshot,kind,status,watermark,"
                "token,created_at) VALUES(?,?,?,'queued',?,?,?)",
                (edit_id, base_snapshot, kind, watermark, token, now()),
            )
            bid = int(cur.lastrowid)
        b = self.build(bid)
        assert b is not None
        return b

    def build_plan(self, bid: int) -> Optional[dict]:
        c = self.meta()
        try:
            r = c.execute("SELECT plan_json FROM builds WHERE id=?", (bid,)).fetchone()
            return json.loads(r["plan_json"]) if r and r["plan_json"] else None
        finally:
            c.close()

    def mark_edit_published(self, eid: int, snapshot_id: int) -> None:
        with self.write_txn() as c:
            c.execute("UPDATE edits SET status='published',updated_at=? WHERE id=?",
                      (now(), eid))
            c.execute("INSERT INTO events(at,kind,payload_json) VALUES(?,?,?)",
                      (now(), "edit-published",
                       json.dumps({"edit": eid, "snapshot": snapshot_id})))

    def cancel_build(self, bid: int) -> str:
        with self.write_txn() as c:
            r = c.execute("SELECT status FROM builds WHERE id=?", (bid,)).fetchone()
            if not r:
                raise KeyError(f"build {bid} not found")
            if r["status"] in ("published", "ready", "failed", "cancelled",
                               "conflict", "crashed"):
                return r["status"]
            new = "cancelled" if r["status"] == "queued" else "cancelling"
            c.execute("UPDATE builds SET status=? WHERE id=?", (new, bid))
            c.execute("INSERT INTO events(at,kind,payload_json) VALUES(?,?,?)",
                      (now(), "build-cancel-requested", json.dumps({"build": bid})))
            return new

    def supersede_running(self, base_snapshot: int,
                          exclude_build: Optional[int] = None) -> list[int]:
        """Cancels every queued build whose base is not the given snapshot.

        Called after a publish: a build started from an older generation would
        otherwise be able to contain a mixture of old and new model versions in
        its file set.  It is marked ``cancelling``; its worker notices at the next
        heartbeat and can never publish because publication requires
        ``running`` plus the expected-current check.
        """
        out: list[int] = []
        with self.write_txn() as c:
            rows = c.execute(
                "SELECT id, token FROM builds WHERE status IN ('queued','running','cancelling') "
                "AND base_snapshot <> ? AND id <> ?",
                (base_snapshot, exclude_build or -1)).fetchall()
            for r in rows:
                c.execute("UPDATE builds SET status='cancelling',error=? WHERE id=?",
                          ("superseded by a newer generation; rebuild from the "
                           "current snapshot", r["id"]))
                c.execute("INSERT INTO events(at,kind,payload_json) VALUES(?,?,?)",
                          (now(), "build-superseded",
                           json.dumps({"build": r["id"], "published": base_snapshot})))
                out.append(r["id"])
        return out



    def build(self, bid: int) -> Optional[Build]:
        c = self.meta()
        try:
            r = c.execute("SELECT * FROM builds WHERE id=?", (bid,)).fetchone()
            if not r:
                return None
            return Build(
                id=r["id"], edit_id=r["edit_id"], base_snapshot=r["base_snapshot"],
                kind=r["kind"], status=r["status"], watermark=r["watermark"],
                token=r["token"], worker_pid=r["worker_pid"],
                created_at=r["created_at"], started_at=r["started_at"],
                heartbeat_at=r["heartbeat_at"], finished_at=r["finished_at"],
                error=r["error"],
                result=json.loads(r["result_json"]) if r["result_json"] else None,
                tmp_dir=r["tmp_dir"],
            )
        finally:
            c.close()

    def builds(self, limit: int = 50) -> list[Build]:
        c = self.meta()
        try:
            rows = c.execute(
                "SELECT id FROM builds ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        finally:
            c.close()
        return [b for b in (self.build(r["id"]) for r in rows) if b]

    def build_tables(self, bid: int) -> list[dict]:
        c = self.meta()
        try:
            return [dict(r) for r in c.execute(
                "SELECT * FROM build_tables WHERE build_id=? ORDER BY level,name", (bid,)
            )]
        finally:
            c.close()

    def set_build_tables(self, bid: int, tables: Iterable[dict]) -> None:
        with self.write_txn() as c:
            c.execute("DELETE FROM build_tables WHERE build_id=?", (bid,))
            for t in tables:
                c.execute(
                    "INSERT INTO build_tables(build_id,name,level,strategy,status) "
                    "VALUES(?,?,?,?,'pending')",
                    (bid, t["name"], t["level"], t["strategy"]),
                )

    def update_build_table(self, bid: int, name: str, **fields: Any) -> None:
        allowed = {"status", "rows_in", "rows_out", "duration_ms", "physical",
                   "content_hash", "error"}
        sets = {k: v for k, v in fields.items() if k in allowed}
        if not sets:
            return
        sql = "UPDATE build_tables SET " + ",".join(f"{k}=?" for k in sets)
        with self.write_txn() as c:
            c.execute(sql + " WHERE build_id=? AND name=?",
                      (*sets.values(), bid, name))

    def claim_build(self, bid: int) -> Optional[Build]:
        """queued -> running.  Returns None unless the caller now owns it."""
        with self.write_txn() as c:
            cur = c.execute(
                "UPDATE builds SET status='running',started_at=COALESCE(started_at,?),"
                "heartbeat_at=? WHERE id=? AND status IN ('queued','running')",
                (now(), now(), bid),
            )
            owned = cur.rowcount == 1
        return self.build(bid) if owned else None

    def set_worker_pid(self, bid: int, pid: int) -> None:
        with self.write_txn() as c:
            c.execute("UPDATE builds SET worker_pid=? WHERE id=?", (pid, bid))

    def heartbeat(self, bid: int, token: str) -> bool:
        """Returns False if this build was cancelled / superseded."""
        with self.write_txn() as c:
            r = c.execute("SELECT status,token FROM builds WHERE id=?",
                          (bid,)).fetchone()
            if not r:
                return False
            if r["token"] != token:
                return False
            if r["status"] not in ("queued", "running"):
                return False
            c.execute("UPDATE builds SET status='running',heartbeat_at=? WHERE id=?",
                      (now(), bid))
            return True

    def build_cancelled(self, bid: int, token: str) -> bool:
        c = self.meta()
        try:
            r = c.execute("SELECT status,token FROM builds WHERE id=?",
                          (bid,)).fetchone()
            return bool(
                not r or r["token"] != token
                or r["status"] in ("cancelling", "cancelled")
            )
        finally:
            c.close()

    def finish_build(self, bid: int, token: str, status: str,
                     error: str | None = None, result: dict | None = None) -> bool:
        with self.write_txn() as c:
            r = c.execute("SELECT status,token FROM builds WHERE id=?",
                          (bid,)).fetchone()
            if not r or r["token"] != token:
                return False
            if r["status"] in ("cancelling", "cancelled") and status == "ready":
                status = "cancelled"
            c.execute(
                "UPDATE builds SET status=?,finished_at=?,heartbeat_at=?,error=?,"
                "result_json=? WHERE id=?",
                (status, now(), now(), error,
                 json.dumps(result, ensure_ascii=False) if result is not None else None,
                 bid),
            )
            c.execute("DELETE FROM build_tables WHERE build_id=? AND status='pending'",
                      (bid,))
            return True

    def publish_build(self, bid: int, token: str, generation: int,
                      watermark: int, tables: dict[str, dict],
                      note: str | None,
                      expected_current: Optional[int] = None,
                      horizons: Optional[dict[str, int]] = None) -> tuple[int, str]:
        """Atomically install a snapshot pointing at already-written files.

        Returns (snapshot_id, 'published') or (current_snapshot_id, reason) when
        the optimistic check fails.  Never partially installs.
        """
        with self.write_txn() as c:
            b = c.execute("SELECT * FROM builds WHERE id=?", (bid,)).fetchone()
            if not b or b["token"] != token:
                return 0, "build token mismatch"
            if b["status"] != "running":
                return 0, f"build is {b['status']}, not running"
            meta = c.execute(
                "SELECT value FROM meta WHERE key='published_snapshot'"
            ).fetchone()
            current = int(meta["value"]) if meta else 1
            expected = b["base_snapshot"] if expected_current is None else expected_current
            if current != expected:
                return current, "published snapshot moved since this build started"
            if b["edit_id"] is not None:
                edit = c.execute("SELECT * FROM edits WHERE id=?",
                                 (b["edit_id"],)).fetchone()
                if edit is None:
                    return 0, "edit no longer exists"
                if edit["status"] != "draft":
                    return 0, f"edit is {edit['status']}"
                if edit["base_snapshot"] != b["base_snapshot"]:
                    return 0, "edit was rebased after this build started"
            cur = c.execute(
                "INSERT INTO snapshots(generation,parent_id,edit_id,build_id,"
                "watermark,horizons_json,mode,note,created_at) "
                "VALUES(?,?,?,?,?,?,'publish',?,?)",
                (generation, current, b["edit_id"], bid, watermark,
                 json.dumps(horizons or {}, ensure_ascii=False), note, now()),
            )
            sid = int(cur.lastrowid)
            for name, t in tables.items():
                c.execute(
                    "INSERT INTO snapshot_tables(snapshot_id,name,version,kind,body,"
                    "schema_json,physical,content_hash,row_count,built_watermark) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (sid, name, t["version"], t["kind"], t["body"],
                     json.dumps(t["schema"], ensure_ascii=False), t["physical"],
                     t.get("content_hash"), t.get("row_count"),
                     t["built_watermark"]),
                )
            c.execute("UPDATE meta SET value=? WHERE key='published_snapshot'",
                      (str(sid),))
            c.execute("UPDATE builds SET publish_json=COALESCE(publish_json,'') || ? "
                      "WHERE id=?", (json.dumps({"snapshot": sid, "generation": generation}) + "\n", bid))
            c.execute(
                "INSERT INTO events(at,kind,payload_json) VALUES(?,?,?)",
                (now(), "publish", json.dumps(
                    {"build": bid, "snapshot": sid, "generation": generation,
                     "watermark": watermark}, ensure_ascii=False)),
            )
            return sid, "published"
    def create_rollback_snapshot(self, target_sid: int, expected_current: int,
                                 note: str | None) -> int:
        with self.write_txn() as c:
            meta = c.execute(
                "SELECT value FROM meta WHERE key='published_snapshot'"
            ).fetchone()
            current = int(meta["value"])
            if current != expected_current:
                raise ValueError("published snapshot moved; refresh and retry")
            gen_row = c.execute("SELECT MAX(generation) AS g FROM snapshots").fetchone()
            generation = int(gen_row["g"]) + 1
            cur = c.execute(
                "INSERT INTO snapshots(generation,parent_id,edit_id,build_id,"
                "watermark,horizons_json,mode,note,created_at) SELECT ?,?,NULL,NULL,"
                "watermark,horizons_json,'rollback',?,? FROM snapshots WHERE id=?",
                (generation, current, note, now(), target_sid),
            )
            sid = int(cur.lastrowid)
            c.execute(
                "INSERT INTO snapshot_tables SELECT ?,name,version,kind,body,"
                "schema_json,physical,content_hash,row_count,built_watermark "
                "FROM snapshot_tables WHERE snapshot_id=?",
                (sid, target_sid),
            )
            c.execute("UPDATE meta SET value=? WHERE key='published_snapshot'",
                      (str(sid),))
            c.execute(
                "INSERT INTO events(at,kind,payload_json) VALUES(?,?,?)",
                (now(), "rollback", json.dumps(
                    {"snapshot": sid, "target": target_sid}, ensure_ascii=False)),
            )
            return sid

    def reap_builds(self, stale_after_seconds: int = 60) -> list[dict]:
        """Mark dead workers' builds crashed/cancelled.  A crashed worker can
        never publish: publication is a DB transaction gated on 'running'."""
        import os
        reaped: list[dict] = []
        for b in self.builds(limit=200):
            if b.status not in ("queued", "running", "cancelling"):
                continue
            alive = False
            if b.worker_pid:
                try:
                    os.kill(b.worker_pid, 0)
                    alive = True
                except (ProcessLookupError, PermissionError):
                    alive = False
            if b.worker_pid and alive:
                continue
            if b.worker_pid is None:
                # spawn may not have registered the pid yet.  created_at is UTC;
                # calendar.timegm converts it without applying the local offset
                # (mktime does, and subtracting timezone again double-corrected
                # it, making every queued build look hours old on non-UTC hosts).
                age = time.time() - calendar.timegm(
                    time.strptime(b.created_at, "%Y-%m-%dT%H:%M:%SZ")
                )
                if age < stale_after_seconds and b.status != "cancelling":
                    continue
            new_status = "cancelled" if b.status in ("cancelling", "cancelled") else "crashed"
            err = ("build worker exited before publishing"
                   if new_status == "crashed" else "cancelled")
            with self.write_txn() as c:
                r = c.execute("SELECT status,token FROM builds WHERE id=?",
                              (b.id,)).fetchone()
                if not r or r["status"] not in ("queued", "running", "cancelling"):
                    continue
                if r["token"] != b.token:
                    continue
                c.execute(
                    "UPDATE builds SET status=?,finished_at=?,error=? WHERE id=?",
                    (new_status, now(), err, b.id),
                )
            reaped.append({"build": b.id, "status": new_status})
            self.cleanup_build_dir(b)
        return reaped

    def cleanup_build_dir(self, b: Build) -> None:
        """Remove a finished build's scratch space.

        Generation files are kept while any snapshot still points at them, so
        rollback and time-travel reads keep working; they are only deleted when
        the build failed or was cancelled before publishing.
        """
        import shutil
        if b.tmp_dir:
            p = self.physical_path(b.tmp_dir)
            if p.exists():
                shutil.rmtree(p, ignore_errors=True)
        gdir = self.root / "generations" / f"build-{b.id}"
        if not gdir.exists():
            return
        c = self.meta()
        try:
            used = c.execute(
                "SELECT COUNT(*) AS n FROM snapshot_tables WHERE physical LIKE ?",
                (f"generations/build-{b.id}/%",),
            ).fetchone()["n"]
        finally:
            c.close()
        if used:
            return
        # Keep a failed build's files for post-mortem debugging.
        if b.status == "failed":
            return
        shutil.rmtree(gdir, ignore_errors=True)
