"""Dependency analysis, proof-carrying planning and execution of a model set.

Consistency model
-----------------
The unit readers see is a *snapshot*: immutable metadata mapping each logical
table to a physical SQLite file plus the SQL body that produced it.  A snapshot
is installed by one ``BEGIN IMMEDIATE`` transaction that moves the single
``published_snapshot`` pointer.  A reader resolves that pointer once, so it can
only ever read tables from one generation -- there is no moment at which two
consumers, or one consumer across two queries, see a half-switched warehouse.

Builds never write into files a snapshot points at.  Every rebuilt table is a
new file under ``generations/build-<id>/``; publication just points at it.

Publish modes
-------------
``cutover``
    Rebuild every table in the transitive downstream closure of every changed
    model, then publish one generation.  Predictable and always safe, but the
    new generation cannot appear until the *slowest, deepest* member of the
    longest chain has finished.  A 12-table chain therefore exposes no new data
    for 12 rebuilds even if the change only touched its root column.

``grouped`` (default)
    Rebuild in level order and carry a *proof* at each edge.  After a table is
    rebuilt, its schema and logical content hash are compared with the currently
    published version.  If they are unchanged, every downstream table whose only
    reason to rebuild was that dependency is dropped from the run and keeps its
    published file -- the edit is provably compatible across that edge.  The
    remaining work is split into weakly-connected *families*; each family is
    published as its own atomic generation as soon as it is ready.  Families
    share no path, so publishing them in any order is sound, and a long chain in
    family A no longer blocks family B.  The price is several generations during
    one rollout; each is individually coherent, and readers can pin either the
    pre-rollout or any published generation.

Every publication is additionally guarded by a *binding check*: after the
shadow files are ready, each model's SQL is executed with ``LIMIT 0`` against
the exact set of files that would be published.  A column removed upstream and
still referenced downstream fails here, before the pointer moves.

Incremental models (``INSERT INTO ... SELECT``) are append-only: they read a
fixed source watermark window, materialize the previous published file plus the
new rows, and may never change their output schema.  Rows that arrive while a
build is running are outside its horizon and are picked up by the next run;
they can never be half-applied.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from .sqlparse import ParseError, Statement, parse_script
from .store import NAME_RE, SnapshotTable, Store, now

SOURCE_SEQ_COL = "_src_seq"
SOURCE_DB_ALIAS = "_strata_sources"
TYPE_CASTS = {"INTEGER": "INTEGER", "INT": "INTEGER", "REAL": "REAL",
              "DOUBLE": "REAL", "FLOAT": "REAL", "TEXT": "TEXT"}


# ---------------------------------------------------------------------------
# models


@dataclass
class Model:
    name: str
    version: int
    kind: str            # 'create' | 'insert'
    body: str
    stmt: Statement
    tables: list[str] = field(default_factory=list)   # upstream logical names

    @property
    def select(self):
        return self.stmt.select


def compile_model(name: str, version: int, kind: str, body: str) -> Model:
    if not NAME_RE.match(name):
        raise ParseError(f"invalid table name {name!r}")
    stmts = parse_script(body)
    if len(stmts) != 1:
        raise ParseError(
            f"model {name!r}: a model body must be exactly one statement "
            f"({len(stmts)} found); split extra steps into their own tables"
        )
    stmt = stmts[0]
    if stmt.target != name:
        raise ParseError(
            f"model {name!r} must write its own table, but its statement targets "
            f"{stmt.target!r}"
        )
    if kind == "create" and stmt.kind != "create":
        raise ParseError(
            f"model {name!r} is a full model; use CREATE TABLE {name} AS SELECT ..."
        )
    if kind == "insert" and stmt.kind != "insert":
        raise ParseError(
            f"model {name!r} is an incremental model; use INSERT INTO {name} SELECT ..."
        )
    tables = stmt.select.source_tables()
    if name in tables:
        raise ParseError(f"model {name!r} reads itself; cycles are not allowed")
    return Model(name, version, kind, body, stmt, tables)


def model_map(table_defs: dict[str, dict]) -> dict[str, Model]:
    return {
        n: compile_model(n, t["version"], t["kind"], t["body"])
        for n, t in table_defs.items()
    }


def validate_insert_sources(models: dict[str, Model],
                            source_names: set[str]) -> None:
    """Incremental models are only allowed to read declared sources.

    This is what makes the append proof simple: an increment consumes exactly the
    source window it is given, so replaying it later against a newer window
    cannot rewrite previously appended rows.  Reading an upstream table (whose
    append history is already collapsed) would make the increment's result depend
    on build timing; that is rejected rather than silently mis-computed.
    """
    for n, m in models.items():
        if m.kind != "insert":
            continue
        bad = [t for t in m.tables if t not in source_names]
        if bad:
            raise ParseError(
                f"incremental model {n!r} reads {', '.join(bad)}; an INSERT model "
                "may read only declared sources (use a full CREATE model downstream)"
            )
        if not m.tables:
            raise ParseError(f"incremental model {n!r} reads no source")
        if m.select.is_aggregate or m.select.group_by:
            raise ParseError(
                f"incremental model {n!r} uses aggregation; appending re-aggregated "
                "results would duplicate groups. Use a full CREATE model and let "
                "the planner rebuild it, or append row-level facts only."
            )


# ---------------------------------------------------------------------------
# graph helpers


def downstream_map(models: dict[str, Model]) -> dict[str, list[str]]:
    down: dict[str, list[str]] = {n: [] for n in models}
    for m in models.values():
        for u in m.tables:
            if u in down and m.name not in down[u]:
                down[u].append(m.name)
    return down


def compute_levels(models: dict[str, Model]) -> dict[str, int]:
    memo: dict[str, int] = {}
    visiting: set[str] = set()

    def visit(n: str) -> int:
        if n in memo:
            return memo[n]
        if n in visiting:
            raise ParseError(f"dependency cycle involving table {n!r}")
        visiting.add(n)
        best = 0
        for u in models[n].tables:
            if u in models and u != n:
                best = max(best, visit(u) + 1)
        visiting.remove(n)
        memo[n] = best
        return best

    for n in models:
        visit(n)
    return memo


def downstream_closure(models: dict[str, Model], roots: set[str]) -> set[str]:
    down = downstream_map(models)
    seen: set[str] = set()
    stack = list(roots)
    while stack:
        n = stack.pop()
        if n in seen or n not in models:
            continue
        seen.add(n)
        stack.extend(down[n])
    return seen


def source_closure(models: dict[str, Model], name: str,
                   sources: set[str]) -> set[str]:
    """Sources a table transitively reads."""
    seen_t: set[str] = set()
    out: set[str] = set()

    def visit(n: str) -> None:
        if n in seen_t or n not in models:
            return
        seen_t.add(n)
        for u in models[n].tables:
            if u in sources:
                out.add(u)
            else:
                visit(u)

    visit(name)
    return out


def weakly_connected(models: dict[str, Model], start: str) -> set[str]:
    adj: dict[str, set[str]] = {n: set() for n in models}
    for m in models.values():
        for u in m.tables:
            if u in models:
                adj[u].add(m.name)
                adj[m.name].add(u)
    seen = {start}
    stack = [start]
    while stack:
        n = stack.pop()
        for x in adj[n]:
            if x not in seen:
                seen.add(x)
                stack.append(x)
    return seen


def changed_models(old: dict[str, Model], new: dict[str, Model]) -> dict[str, str]:
    out: dict[str, str] = {}
    for n, m in new.items():
        if n not in old:
            out[n] = "new table"
        elif old[n].body != m.body or old[n].kind != m.kind:
            out[n] = "model changed"
    for n in old:
        if n not in new:
            raise ParseError(
                f"table {n!r} would disappear; dropping tables is not supported "
                "because consumers may still read them"
            )
    return out


def consumer_breakage(models: dict[str, Model],
                      schemas: dict[str, list[str]]) -> list[dict]:
    """Which models reference a column their inputs no longer expose.

    Used by the preview/impact endpoint before anything is built.
    """
    problems: list[dict] = []
    for n, m in models.items():
        for t in m.tables:
            if t not in schemas:
                continue
        try:
            m.select.output_lineage(schemas)
        except ParseError as e:
            problems.append({"table": n, "error": str(e)})
            continue
        schema = schemas
        for pred in m.select.where + m.select.having:
            for op in (pred.left, pred.right):
                if op.kind == "column" and op.ref is not None:
                    try:
                        m.select.resolve_ref(op.ref, schema)
                    except ParseError as e:
                        problems.append({"table": n, "error": str(e)})
                elif op.kind == "aggregate":
                    try:
                        m.select.resolve_ref(
                            __import__("strata.sqlparse", fromlist=["ColumnRef"])
                            .ColumnRef(None, op.arg), schema)
                    except ParseError as e:
                        problems.append({"table": n, "error": str(e)})
        for ref in m.select.group_by:
            try:
                m.select.resolve_ref(ref, schema)
            except ParseError as e:
                problems.append({"table": n, "error": str(e)})
        for j in m.select.joins:
            for ref in j.cond:
                try:
                    m.select.resolve_ref(ref, schema)
                except ParseError as e:
                    problems.append({"table": n, "error": str(e)})
    return problems


def column_lineage(models: dict[str, Model], source_columns: dict[str, list[str]]
                   ) -> tuple[dict[str, dict], list[dict]]:
    """table -> column -> {from: [source.col], via: [tables]}, plus edge list.

    ``via`` is the ordered chain of logical tables the value passes through,
    nearest-to-source first.
    """
    memo: dict[str, dict] = {}
    visiting: set[str] = set()
    edges: list[dict] = []

    def resolve(n: str) -> dict:
        if n in memo:
            return memo[n]
        if n in visiting:
            return {}
        visiting.add(n)
        m = models[n]
        schemas: dict[str, list[str]] = {k: list(v) for k, v in source_columns.items()}
        direct: dict[str, dict] = {}
        for t in m.tables:
            if t in models:
                sub = resolve(t)
                schemas[t] = list(sub.keys())
                for col, info in sub.items():
                    direct[f"{t}.{col}"] = info
                    edges.append({"from": t, "to": n, "column": col})
            elif t in source_columns:
                for c in source_columns[t]:
                    direct[f"{t}.{c}"] = {"from": [f"{t}.{c}"], "via": []}
                edges.append({"from": t, "to": n, "column": None})
        select = m.select
        out: dict[str, dict] = {}
        try:
            line = select.output_lineage(schemas)
        except ParseError:
            visiting.discard(n)
            return {c: {"from": [], "via": []} for c in
                    (i.alias for i in select.items if i.kind != "star")}
        for col, refs in line.items():
            physical: list[str] = []
            via: list[str] = []
            for ref in refs:
                info = direct.get(ref)
                if info is None:
                    continue
                for p in info["from"]:
                    if p not in physical:
                        physical.append(p)
                for v in info["via"] + [ref.split(".")[0]]:
                    if v not in via:
                        via.append(v)
            out[col] = {"from": physical, "via": via}
        visiting.discard(n)
        memo[n] = out
        return out

    full = {n: resolve(n) for n in models}
    uniq = {(e["from"], e["to"], e["column"]): e for e in edges}
    return full, list(uniq.values())


# ---------------------------------------------------------------------------
# physical I/O


def connect_out(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-20000")
    return conn


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def attach_inputs(conn: sqlite3.Connection, store: Store,
                  physicals: dict[str, str]) -> list[str]:
    attached = []
    for name, rel in sorted(physicals.items()):
        p = store.physical_path(rel)
        if not p.exists():
            raise RuntimeError(f"input table {name!r} has no file ({rel})")
        conn.execute(f"ATTACH DATABASE ? AS {quote(name)}", (str(p),))
        attached.append(name)
    return attached


def attach_sources(conn: sqlite3.Connection, store: Store,
                   source_defs: dict[str, list[dict]], source_names: set[str],
                   horizons: dict[str, int], horizon: int,
                   prior: Optional[int] = None) -> None:
    if not source_names:
        return
    conn.execute(f"ATTACH DATABASE ? AS {SOURCE_DB_ALIAS}",
                 (str(store.sources_path),))
    for s in sorted(source_names):
        cols = []
        for c in source_defs.get(s, []):
            expr = f"json_extract(data, '$.{c['name']}')"
            cast = TYPE_CASTS.get(str(c.get("type", "")).upper())
            if cast:
                expr = f"CAST({expr} AS {cast})"
            cols.append(f"{expr} AS {quote(c['name'])}")
        lower = prior if prior is not None else 0
        upper = horizons.get(s, horizon)
        sql = (
            f"CREATE TEMP VIEW {quote(s)} AS SELECT seq AS {SOURCE_SEQ_COL}, "
            f"{', '.join(cols)} FROM {SOURCE_DB_ALIAS}.source_rows "
            f"WHERE source = '{s}' AND seq > {int(lower)} AND seq <= {int(upper)}"
        )
        conn.execute(sql)


def logical_hash(conn: sqlite3.Connection, table: str,
                 db: str = "main") -> str:
    """Order-insensitive hash of schema + row multiset."""
    rows = conn.execute(f"PRAGMA {db}.table_info({quote(table)})").fetchall()
    cols = [r[1] for r in rows]
    h = hashlib.sha256()
    h.update(("|".join(cols)).encode())
    if not cols:
        return h.hexdigest()
    collist = ",".join(quote(c) for c in cols)
    qualified = f"{db}.{quote(table)}"
    for row in conn.execute(f"SELECT {collist} FROM {qualified}"):
        h.update(json.dumps(list(row), default=str).encode())
        h.update(b"\x1e")
    return h.hexdigest()


def file_sha(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sample_rows(path: Path, table: str, limit: int = 5) -> list[dict]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({quote(table)})")]
        rows = conn.execute(
            f"SELECT * FROM {quote(table)} LIMIT {int(limit)}").fetchall()
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# planning


class PlanError(ValueError):
    pass


@dataclass
class Plan:
    mode: str
    edit_id: Optional[int]
    base_snapshot: int
    base_generation: int
    watermark: int
    horizons: dict[str, int]
    changed: dict[str, str]
    levels: dict[str, int]
    reasons: dict[str, set[str]]
    groups: list[dict]
    tables: list[dict]
    source_new_rows: dict[str, int]
    explain: dict
    models: dict[str, Model]

    def to_json(self) -> dict:
        return {
            "mode": self.mode,
            "edit_id": self.edit_id,
            "base_snapshot": self.base_snapshot,
            "base_generation": self.base_generation,
            "watermark": self.watermark,
            "horizons": self.horizons,
            "changed": self.changed,
            "levels": self.levels,
            "reasons": {k: sorted(v) for k, v in self.reasons.items()},
            "groups": self.groups,
            "tables": self.tables,
            "source_new_rows": self.source_new_rows,
            "explain": self.explain,
        }


def compute_plan(store: Store, *, edit_id: Optional[int], base_snapshot: int,
                 table_defs: dict[str, dict], mode: str = "grouped",
                 force_rebuild_all: bool = False,
                 frozen_horizon: Optional[int] = None) -> Plan:
    if mode not in ("grouped", "cutover"):
        raise PlanError(f"unknown publish mode {mode!r}")
    base = store.published_snapshot()
    if base.id != base_snapshot:
        raise PlanError("base snapshot is no longer the published snapshot")
    base_tables = store.snapshot_tables(base_snapshot)
    source_defs = store.source_defs()
    source_names = set(source_defs)
    source_columns = {s: [c["name"] for c in cols] for s, cols in source_defs.items()}

    try:
        models = model_map(table_defs)
        validate_insert_sources(models, source_names)
    except ParseError as e:
        raise PlanError(str(e))
    old_models: dict[str, Model] = {}
    old_errors: dict[str, str] = {}
    for n, t in base_tables.items():
        try:
            old_models[n] = compile_model(n, t.version, t.kind, t.body)
            validate_insert_sources({n: old_models[n]}, source_names)
        except ParseError as e:
            old_errors[n] = str(e)

    unknown = sorted({t for m in models.values() for t in m.tables
                      if t not in models and t not in source_names and t not in base_tables})
    if unknown:
        raise PlanError(
            "unknown source or table referenced: " + ", ".join(unknown)
            + ". Declare a source or add the upstream table first."
        )

    try:
        levels = compute_levels(models)
    except ParseError as e:
        raise PlanError(str(e))
    try:
        changed = (changed_models(old_models, models) if not old_errors else {
            n: "model changed" for n in models
            if n not in old_models or n in old_errors})
    except ParseError as e:
        raise PlanError(str(e))
    # Freeze the source horizon.  Every model in this run reads only rows with
    # seq <= horizon; rows appended while the build runs are invisible to it and
    # are picked up by a later run.  ``frozen_horizon`` is the value advertised
    # when the build was queued, so a plan recomputed mid-build cannot widen it.
    current = store.source_horizons()
    horizon = max(current.values(), default=0)
    if frozen_horizon is not None:
        horizon = min(horizon, frozen_horizon)
    horizons = {src: min(v, horizon) for src, v in current.items()}
    new_rows = {s: len(store.rows_between(s, base.watermark, horizons.get(s, 0)))
                for s in source_names}
    src_closure = {n: source_closure(models, n, source_names) for n in models}

    reasons: dict[str, set[str]] = {}
    all_horizons = horizons
    for n, m in models.items():
        r: set[str] = set()
        if n in changed:
            r.add("changed")
        bt = base_tables.get(n)
        # A table is stale when any source it reads has rows beyond the point it
        # last consumed.  ``seq`` is globally monotone across sources, so one
        # per-table watermark is sufficient and families can advance at
        # different rates.
        since = bt.built_watermark if bt is not None else 0
        for s in sorted(src_closure[n]):
            if all_horizons.get(s, 0) > since:
                r.add(f"source:{s}")
        if force_rebuild_all:
            r.add("full refresh")
        if r:
            reasons[n] = r

    # Downstream propagation is added level by level; in grouped mode a
    # dependency reason is provisional and may be discharged by proof.
    down = downstream_map(models)
    propagated: dict[str, set[str]] = {n: set(r) for n, r in reasons.items()}
    for n in sorted(models, key=lambda x: levels[x]):
        if n not in propagated:
            continue
        for d in down[n]:
            propagated.setdefault(d, set()).add(f"dep:{n}")
    reasons = propagated

    needed = set(reasons)
    if mode == "cutover":
        groups = [{"id": 0, "tables": sorted(needed, key=lambda x: (levels[x], x)),
                   "max_level": max((levels[n] for n in needed), default=0)}]
    else:
        groups = []
        seen: set[str] = set()
        for n in sorted(needed, key=lambda x: (levels[x], x)):
            if n in seen:
                continue
            comp = weakly_connected(models, n) & needed
            # A family must be closed against the whole graph: if an edge leaves
            # the family, the two tables share a component by definition.
            groups.append({
                "id": len(groups),
                "tables": sorted(comp, key=lambda x: (levels[x], x)),
                "max_level": max(levels[x] for x in comp),
            })
            seen |= comp
        groups.sort(key=lambda g: (g["max_level"], g["tables"][0]))

    tables = []
    for n in sorted(models, key=lambda x: (levels[x], x)):
        if n not in reasons:
            continue
        tables.append({
            "name": n,
            "level": levels[n],
            "kind": models[n].kind,
            "version": models[n].version,
            "change": changed.get(n),
            "reasons": sorted(reasons[n]),
            "depends_on": [t for t in models[n].tables if t in models],
            "reads_sources": [t for t in models[n].tables if t in source_names],
            "estimated": True,
        })

    depth = max(levels.values(), default=0)
    rebuild_pred = len(needed)
    cutover_pred = len(downstream_closure(
        models, set(changed) | {n for n, r in
                                ((x, y) for x, y in reasons.items())
                                if any(k.startswith("source:") for k in r)}))
    explain = {
        "why": (
            "No model text changed; this run only consumes new source rows."
            if not changed else
            "Model text changed for: " + ", ".join(sorted(changed)) + "."
        ),
        "cutover": {
            "tables": cutover_pred,
            "max_level": depth,
            "wait": (
                "The whole new generation becomes visible only after the deepest "
                f"affected table (level {depth}) finishes; a reader waits for "
                f"every one of the {cutover_pred} rebuilds."
            ),
            "cost": (
                f"{cutover_pred} table rewrites, one generation switch, one "
                "failure domain."
            ),
        },
        "grouped": {
            "tables_upper_bound": rebuild_pred,
            "families": len(groups),
            "first_visible": (
                "the first family to finish (max level "
                f"{min((g['max_level'] for g in groups), default=0)})"
                if groups else "nothing to publish"),
            "publish_batches": [
                {"tables": g["tables"], "max_level": g["max_level"]}
                for g in groups
            ],
            "wait": (
                "Each family publishes as soon as it is ready; the first new "
                "data appears after the shallowest family, not after the longest "
                "chain. Tables whose rebuilt content hash and schema equal the "
                "published ones are proven compatible: their downstream is "
                "skipped and keeps its current file."
            ),
            "cost": (
                f"Up to {rebuild_pred} rebuilds, {len(groups)} generation "
                "switches; extra snapshots retain old files, so disk use grows "
                "with version retention rather than with chain length."
            ),
        },
        "incremental": (
            "Incremental (INSERT) models append only source rows newer than their "
            "own last build watermark"
            + ("" if not any(m.kind == "insert" for m in models.values()) else
               " (" + ", ".join(
                   f"{n}: {base_tables[n].built_watermark if n in base_tables else 0}"
                   for n, m in models.items() if m.kind == "insert") + ")")
            + ". This run consumes source rows up to seq "
            f"{horizon}; rows arriving during the build have a higher seq and are "
            "outside every model's window, so they are appended exactly once by "
            "the next run and never half-applied."
        ),
        "validation": (
            "Before each switch every model's SQL is bound against the exact "
            "files of the candidate generation; a mismatch aborts the switch."
        ),
    }

    return Plan(mode, edit_id, base_snapshot, base.generation, horizon, horizons,
                changed, levels, reasons, groups, tables, new_rows, explain,
                models)


# ---------------------------------------------------------------------------
# execution


class BuildCancelled(Exception):
    pass


@dataclass
class TableOutcome:
    name: str
    action: str                     # rebuilt | reused | skipped-proof
    level: int
    kind: str
    version: int
    body: str
    schema: list[dict]
    physical: str
    content_hash: Optional[str]
    row_count: Optional[int]
    built_watermark: int
    rows_in: int = 0
    duration_ms: int = 0
    changed_since_base: bool = False
    schema_changed: bool = False

    def meta(self) -> dict:
        return {
            "version": self.version, "kind": self.kind, "body": self.body,
            "schema": self.schema, "physical": self.physical,
            "content_hash": self.content_hash, "row_count": self.row_count,
            "built_watermark": self.built_watermark,
        }


def _build_one(store: Store, m: Model, base_table: Optional[SnapshotTable],
               physicals: dict[str, str], source_defs: dict[str, list[dict]],
               horizon: int, prior: int, out_dir: Path,
               src_horizons: Optional[dict[str, int]] = None,
               reseeded: bool = False) -> TableOutcome:
    t0 = time.time()
    path = out_dir / f"{m.name}.sqlite"
    if path.exists():
        path.unlink()
    out = connect_out(path)
    attached: list[str] = []
    rows_in = 0
    srcs = {t for t in m.tables if t in source_defs}
    try:
        upstream = {t: physicals[t] for t in m.tables if t in physicals}
        attached = attach_inputs(out, store, upstream)
        attach_sources(out, store, source_defs, srcs,
                       src_horizons or {}, horizon,
                       prior if (m.kind == "insert" and base_table is not None)
                       else None)
        sel_sql = m.select.sql
        target = f"main.{quote(m.name)}"
        if m.kind == "insert" and base_table is not None:
            want = [i.alias for i in m.select.items]
            have = [c["name"] for c in base_table.schema]
            if want != have:
                raise RuntimeError(
                    f"incremental model {m.name!r} would change its output schema "
                    f"{have} -> {want}. An INSERT model must keep the same columns "
                    "so already-appended rows stay valid; publish this change as a "
                    "full (CREATE) model instead.")
        if m.kind == "insert":
            if base_table is not None and not reseeded:
                out.execute('ATTACH DATABASE ? AS "_prev"',
                            (str(store.physical_path(base_table.physical)),))
                attached.append("_prev")
                cols = [c["name"] for c in base_table.schema]
                collist = ",".join(quote(c) for c in cols)
                out.execute(
                    f"CREATE TABLE {target} AS SELECT {collist} "
                    f"FROM \"_prev\".{quote(m.name)}")
                out.execute('DETACH DATABASE "_prev"')
                attached.remove("_prev")
                got = [r[1] for r in out.execute(
                    f"PRAGMA main.table_info({quote(m.name)})")]
                want = [i.alias for i in m.select.items]
                if got != want:
                    raise RuntimeError(
                        f"incremental model {m.name!r} output schema {got} does "
                        f"not match its SELECT list {want}; an append may not "
                        "change the shape of existing rows. Publish it as a full "
                        "(CREATE) model instead.")
                rows_in = sum(
                    out.execute(f"SELECT COUNT(*) FROM {quote(s)}").fetchone()[0]
                    for s in sorted(srcs))
                out.execute(
                    f"INSERT INTO {target} ({collist}) "
                    f"SELECT {collist} FROM ({sel_sql})")
            else:
                # First materialization of an incremental model, or one whose body
                # changed: seed it from the full source history so later appends
                # are complete and consistent with the new body.
                rows_in = sum(out.execute(
                    f"SELECT COUNT(*) FROM {quote(s)}").fetchone()[0]
                    for s in sorted(srcs))
                out.execute(
                    f"CREATE TABLE {target} AS SELECT * FROM ({sel_sql})")
        else:
            out.execute(
                f"CREATE TABLE {target} AS SELECT * FROM ({sel_sql})")
        schema = [{"name": r[1], "type": r[2]} for r in
                  out.execute(f"PRAGMA main.table_info({quote(m.name)})")]
        if any(c["name"] == SOURCE_SEQ_COL for c in schema):
            raise RuntimeError(
                f"model {m.name!r} outputs reserved column {SOURCE_SEQ_COL}")
        row_count = out.execute(
            f"SELECT COUNT(*) FROM {target}").fetchone()[0]
        if m.kind == "insert" and base_table is not None and not reseeded:
            if row_count < (base_table.row_count or 0):
                raise RuntimeError(
                    f"incremental model {m.name!r} would lose rows "
                    f"({base_table.row_count} -> {row_count}); switch aborted")
        ch = logical_hash(out, m.name)
        vac = Path(str(path) + ".vac")
        if vac.exists():
            vac.unlink()
        out.execute("VACUUM INTO ?", (str(vac),))
    finally:
        for name in attached:
            try:
                out.execute(f"DETACH DATABASE {quote(name)}")
            except sqlite3.Error:
                pass
        out.close()
    vac = Path(str(path) + ".vac")
    if vac.exists():
        vac.replace(path)
    base_schema = base_table.schema if base_table else []
    return TableOutcome(
        name=m.name, action="rebuilt", level=0, kind=m.kind, version=m.version,
        body=m.body, schema=schema,
        physical=str(path.relative_to(store.root)), content_hash=ch,
        row_count=int(row_count), built_watermark=horizon, rows_in=int(rows_in),
        duration_ms=int((time.time() - t0) * 1000),
        changed_since_base=bool(
            base_table is None or base_table.content_hash != ch),
        schema_changed=bool(base_table and base_schema != schema),
    )


def _binding_check(store: Store, models: dict[str, Model], physicals: dict[str, str],
                   source_defs: dict[str, list[dict]], horizon: int,
                   horizons: Optional[dict[str, int]] = None) -> list[str]:
    """Execute every *resolvable* model with LIMIT 0 against the candidate files.

    Models whose upstream tables are not part of this candidate (they belong to
    a later family that has not been built yet) are skipped; they are checked
    when their own family is ready to switch.
    """
    problems: list[str] = []
    conn = sqlite3.connect(":memory:", isolation_level=None)
    try:
        attach_inputs(conn, store, physicals)
        attach_sources(conn, store, source_defs,
                       {t for m in models.values() for t in m.tables
                        if t in source_defs}, horizons or {}, horizon)
        for n, m in models.items():
            if n not in physicals:
                continue
            if any(t in models and t not in physicals for t in m.tables):
                continue
            try:
                conn.execute(f"SELECT * FROM ({m.select.sql}) LIMIT 0")
            except sqlite3.Error as e:
                problems.append(f"{n}: {e}")
    finally:
        conn.close()
    return problems


def execute_build(store: Store, build_id: int, *, crash_at: Optional[str] = None,
                  progress: Optional[Callable[[dict], None]] = None
                  ) -> dict:
    b = store.build(build_id)
    if b is None:
        return {"ok": False, "error": "build not found"}
    if b.status in ("cancelling", "cancelled"):
        store.finish_build(build_id, b.token, "cancelled",
                           b.error or "build was cancelled before it started")
        return {"ok": False, "status": "cancelled",
                "error": "build was cancelled before it started"}
    if b.status not in ("queued", "running"):
        return {"ok": False, "status": b.status,
                "error": b.error or f"build is already {b.status}"}
    b = store.claim_build(build_id)
    if b is None:
        now_b = store.build(build_id)
        return {"ok": False,
                "status": now_b.status if now_b else "unknown",
                "error": "build was cancelled or is already final"}
    token = b.token
    store.set_worker_pid(build_id, os.getpid())
    out_dir = store.root / "generations" / f"build-{build_id}"
    tmp_dir = store.root / "tmp" / f"build-{build_id}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    def hb() -> None:
        if not store.heartbeat(build_id, token):
            raise BuildCancelled("cancelled or superseded before publication")

    def check_cancel() -> None:
        if store.build_cancelled(build_id, token):
            raise BuildCancelled("cancelled before publication")

    def note(stage: str, **kw: Any) -> None:
        if progress:
            progress({"stage": stage, **kw})

    try:
        if crash_at == "after_claim":
            os._exit(17)
        base = store.published_snapshot()
        if b.base_snapshot != base.id:
            return _finish(
                store, build_id, token, "conflict",
                "the published snapshot moved after this build was queued; "
                "rebase the edit on the current generation and rebuild")
        base_tables = store.snapshot_tables(base.id)
        if b.edit_id is not None:
            edit = store.edit(b.edit_id)
            if not edit:
                return _finish(store, build_id, token, "failed", "edit not found")
            if edit["status"] not in ("draft",):
                return _finish(store, build_id, token, "cancelled",
                               f"edit is {edit['status']}")
            et = store.edit_tables(b.edit_id)
            table_defs = {n: {"version": t["version"], "kind": t["kind"],
                              "body": t["body"]} for n, t in et.items()}
        else:
            table_defs = {n: {"version": t.version, "kind": t.kind, "body": t.body}
                          for n, t in base_tables.items()}

        mode = b.kind if b.kind in ("grouped", "cutover") else "grouped"
        plan = compute_plan(store, edit_id=b.edit_id, base_snapshot=base.id,
                            table_defs=table_defs, mode=mode,
                            frozen_horizon=b.watermark or None)
        with store.write_txn() as c:
            c.execute("UPDATE builds SET plan_json=? WHERE id=?",
                      (json.dumps(plan.to_json(), ensure_ascii=False), build_id))
        store.set_build_tables(build_id, [
            {"name": t["name"], "level": t["level"], "strategy": "pending"}
            for t in plan.tables])
        note("planned", plan=plan.to_json())
        if crash_at == "after_plan":
            os._exit(17)

        source_defs = store.source_defs()
        physicals: dict[str, str] = {n: t.physical for n, t in base_tables.items()}
        outcomes: dict[str, TableOutcome] = {
            n: TableOutcome(n, "reused", plan.levels.get(n, 0), t.kind, t.version,
                            t.body, t.schema, t.physical, t.content_hash,
                            t.row_count, t.built_watermark)
            for n, t in base_tables.items()
        }
        reasons = {n: set(r) for n, r in plan.reasons.items()}
        built_names: set[str] = set()
        published_groups: list[dict] = []
        expected_current = base.id
        current_generation = base.generation
        generation_index = 0
        published_any = False
        pruning_log: list[dict] = []
        down = downstream_map(plan.models)
        # The table map as currently published.  Updated after each family
        # switch so a later family never rolls back an earlier one.
        current_tables: dict[str, dict] = {
            n: {"version": t.version, "kind": t.kind, "body": t.body,
                "schema": t.schema, "physical": t.physical,
                "content_hash": t.content_hash, "row_count": t.row_count,
                "built_watermark": t.built_watermark}
            for n, t in base_tables.items()
        }

        def discharge(identical: str) -> list[str]:
            """Remove dep:<identical> reasons from downstream tables.

            Returns the tables that lost their last reason, i.e. that are now
            proven unnecessary to rebuild.
            """
            freed: list[str] = []
            stack = list(down.get(identical, []))
            seen: set[str] = set()
            key = f"dep:{identical}"
            while stack:
                d = stack.pop()
                if d in seen:
                    continue
                seen.add(d)
                rr = reasons.get(d)
                if rr is None:
                    continue
                rr.discard(key)
                if not rr:
                    del reasons[d]
                    freed.append(d)
                    stack.extend(down.get(d, []))
            return freed

        for idx, group in enumerate(plan.groups):
            if not group["tables"]:
                continue
            check_cancel()
            hb()
            group_rebuilt: list[str] = []
            group_skipped: list[str] = []
            group_levels = sorted({plan.levels[n] for n in group["tables"]})
            for level in group_levels:
                # Decide this level *before* building it: a table whose reasons
                # were all discharged by a proven-identical upstream keeps its
                # published file, and its own edges are discharged in turn so the
                # pruning cascades down the whole family in order.
                for n in sorted(n for n in group["tables"]
                                if plan.levels[n] == level and not reasons.get(n)):
                    group_skipped.append(n)
                    if n not in built_names:
                        store.update_build_table(build_id, n, status="reused-proof")
                        freed = discharge(n)
                        pruning_log.append({
                            "table": n, "level": level,
                            "proof": "no remaining reason; every upstream edge was "
                                     "proven compatible, published file reused",
                            "skipped_downstream": sorted(freed),
                        })
                level_tables = [n for n in group["tables"]
                                if plan.levels[n] == level and reasons.get(n)]
                for n in sorted(level_tables):
                    hb()
                    m = plan.models[n]
                    base_t = base_tables.get(n)
                    reseeded = (m.kind != "insert" or base_t is None
                                or base_t.kind != "insert"
                                or base_t.body != m.body)
                    prior = (base_t.built_watermark
                             if base_t is not None and not reseeded else 0)
                    # A "create" model is a pure function of its inputs, so it is
                    # always safe to recompute and compare.  An "insert" model
                    # whose body is unchanged appends to its prior file; one whose
                    # body changed is reseeded from the full source history.
                    o = _build_one(store, m, base_t, physicals,
                                   source_defs, plan.watermark, prior, out_dir,
                                   src_horizons=plan.horizons, reseeded=reseeded)
                    o.level = level
                    outcomes[n] = o
                    built_names.add(n)
                    if o.changed_since_base or o.schema_changed or base_t is None:
                        group_rebuilt.append(n)
                    physicals[n] = o.physical
                    store.update_build_table(
                        build_id, n, status="built", rows_in=o.rows_in,
                        rows_out=o.row_count, duration_ms=o.duration_ms,
                        physical=o.physical, content_hash=o.content_hash)
                    # Proof: a rebuilt table with the same schema and the same
                    # logical content as the published one cannot affect any
                    # consumer, so no downstream rebuild is justified by it.
                    if (mode == "grouped" and base_t is not None
                            and not o.changed_since_base and not o.schema_changed):
                        freed = discharge(n)
                        pruning_log.append({
                            "table": n,
                            "level": level,
                            "proof": "rebuilt with identical schema and identical "
                                     "logical content to the published generation",
                            "skipped_downstream": sorted(freed),
                        })
                    if crash_at == "mid_build" and level >= 1:
                        os._exit(17)

            # Candidate generation for this family: every published table, with
            # this family's fresh files substituted.  Other families untouched.
            candidate: dict[str, dict] = dict(current_tables)
            for n in group_rebuilt:
                candidate[n] = outcomes[n].meta()

            problems = _binding_check(
                store, plan.models,
                {n: c["physical"] for n, c in candidate.items()},
                source_defs, plan.watermark, plan.horizons)
            if problems:
                raise RuntimeError(
                    "candidate generation does not bind: " + "; ".join(problems[:5]))

            if b.kind == "preview":
                published_groups.append({
                    "id": group["id"], "tables": group["tables"],
                    "rebuilt": group_rebuilt, "skipped": group_skipped,
                    "snapshot": None,
                })
                continue

            if crash_at == "before_publish" and group_rebuilt and not published_any:
                os._exit(17)
            if not group_rebuilt:
                # nothing changed: no pointer movement needed
                published_groups.append({
                    "id": group["id"], "tables": group["tables"],
                    "rebuilt": [], "skipped": group_skipped,
                    "snapshot": None,
                    "note": "content identical; generation unchanged",
                })
                continue
            generation_index += 1
            current_generation += 1
            sid, reason = store.publish_build(
                build_id, token, current_generation, plan.watermark, candidate,
                f"build {build_id} family {idx + 1}/{len(plan.groups)}",
                expected_current, plan.horizons)
            if sid == 0:
                return _finish(store, build_id, token,
                               "cancelled" if "cancel" in reason else "failed",
                               f"not published: {reason}")
            if reason != "published":
                return _finish(store, build_id, token, "conflict", reason,
                               {"groups": published_groups})
            expected_current = sid
            current_tables = candidate
            store.supersede_running(sid, exclude_build=build_id)
            published_groups.append({
                "id": group["id"], "tables": group["tables"],
                "rebuilt": group_rebuilt, "skipped": group_skipped,
                "snapshot": sid, "generation": current_generation,
            })
            for n in group_rebuilt:
                store.update_build_table(build_id, n, status="published")
            if crash_at == "after_publish" and not published_any:
                os._exit(17)
            published_any = True

        if b.kind == "preview":
            diffs = _preview_diffs(store, plan, outcomes, base_tables, source_defs)
            result = {
                "mode": mode, "groups": published_groups,
                "diffs": diffs, "watermark": plan.watermark,
                "published_snapshot": base.id,
            }
            return _finish(store, build_id, token, "ready", None, result)

        result = {
            "mode": mode,
            "groups": published_groups,
            "pruned": pruning_log,
            "watermark": plan.watermark,
            "snapshot": expected_current,
            "generation": current_generation,
            "tables": [
                {"name": o.name, "action": o.action, "rows_in": o.rows_in,
                 "rows_out": o.row_count, "duration_ms": o.duration_ms,
                 "changed": o.changed_since_base, "schema_changed": o.schema_changed}
                for o in outcomes.values() if o.name in plan.models
            ],
        }
        if b.edit_id is not None:
            store.mark_edit_published(b.edit_id, expected_current)
        store.supersede_running(expected_current, exclude_build=build_id)
        return _finish(store, build_id, token, "published", None, result)
    except BuildCancelled as e:
        return _finish(store, build_id, token, "cancelled", str(e))
    except ParseError as e:
        return _finish(store, build_id, token, "failed", f"SQL error: {e}")
    except sqlite3.OperationalError as e:
        return _finish(store, build_id, token, "failed",
                       f"SQL error: {e}; no generation was switched")
    except Exception as e:  # noqa: BLE001
        import traceback
        return _finish(store, build_id, token, "failed",
                       f"{type(e).__name__}: {e}",
                       {"traceback": traceback.format_exc()[-4000:]})
    finally:
        b2 = store.build(build_id)
        if b2 is not None and b2.status not in ("queued", "running"):
            # Keep files any snapshot references (rollback reads them); drop
            # scratch space and superseded preview output.
            store.cleanup_build_dir(b2)


def _preview_diffs(store: Store, plan: Plan, outcomes: dict[str, TableOutcome],
                   base_tables: dict[str, SnapshotTable],
                   source_defs: dict[str, list[dict]]) -> list[dict]:
    diffs = []
    for n, o in outcomes.items():
        if n not in plan.reasons:
            continue
        bt = base_tables.get(n)
        p = store.physical_path(o.physical)
        entry = {
            "table": n,
            "action": "create" if bt is None else "replace",
            "old_rows": bt.row_count if bt else None,
            "new_rows": o.row_count,
            "schema_changed": o.schema_changed,
            "content_changed": o.changed_since_base,
            "old_schema": [c["name"] for c in bt.schema] if bt else [],
            "new_schema": [c["name"] for c in o.schema],
            "sample_new": sample_rows(p, n, 5),
            "old_sample": (sample_rows(store.physical_path(bt.physical), n, 5)
                           if bt else []),
        }
        diffs.append(entry)
    diffs.sort(key=lambda d: plan.levels.get(d["table"], 0))
    return diffs


def _finish(store: Store, bid: int, token: str, status: str,
            error: str | None, result: dict | None = None) -> dict:
    store.finish_build(bid, token, status, error, result)
    return {"ok": status in ("published", "ready"), "status": status,
            "error": error, "result": result}


# ---------------------------------------------------------------------------
# rollback


def rollback_to(store: Store, target_snapshot: int, note: str | None = None) -> dict:
    target = store.snapshot(target_snapshot)
    if not target:
        raise ValueError("snapshot not found")
    cur = store.published_snapshot()
    if target.id == cur.id:
        raise ValueError("snapshot is already published")
    # Files are immutable and never overwritten, so pointing back at an old
    # snapshot is safe.  Verify nothing in the target is missing first.
    for n, t in store.snapshot_tables(target.id).items():
        if not store.physical_path(t.physical).exists():
            raise ValueError(f"snapshot {target.id} is incomplete: {n} file missing")
    new_sid = store.create_rollback_snapshot(target.id, cur.id, note or
                                             f"rollback to generation {target.generation}")
    return {"snapshot": new_sid, "from": cur.id, "target": target.id}
