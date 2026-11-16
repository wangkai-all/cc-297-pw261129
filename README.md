# Strata — a versioned transformation warehouse with proof-carrying rollout

A self-contained implementation of the brief: a data team edits a small set of
SQL transformations on a web page, tables depend on one another, and changing an
upstream field must rebuild downstream layers **without ever letting a consumer
read two generations at once**.

Everything is Python 3.11 standard library (SQLite, `http.server`, worker
subprocesses) plus a dependency-free browser UI. No packages to install.

```bash
python3 -m strata serve --root data --port 8765
# open http://127.0.0.1:8765, then "Load demo warehouse"
```

Tests (stdlib `unittest`, 38 integration/unit cases):

```bash
python3 -m unittest discover -s tests -v
```

---

## 1. The consistency model

The unit readers see is a **snapshot** — an immutable metadata row mapping every
logical table to a physical SQLite file, a schema, a row count, the SQL body that
produced it, and the source watermark it consumed.

A snapshot is installed by **one `BEGIN IMMEDIATE` transaction** that moves a
single `published_snapshot` pointer. A reader resolves the pointer once, then
opens the files named by *that* snapshot. There is therefore no instant at which
a reader can see table A from generation *n* and table B from generation *n+1*:
either all of A/B are old, or all are new.

Build files are never written in place. Each build writes
`generations/build-<id>/<table>.sqlite` with `VACUUM INTO`, so old generations
stay readable forever (that is also what makes rollback and comparison cheap).
Publication only changes pointers; it never copies table data.

Metadata lives in `meta.db` (WAL, `synchronous=FULL` for the pointer move) and
source rows live in `sources.db` with a globally monotone `seq`.

What this guarantees, concretely:

| failure | mechanism |
|---|---|
| two edit branches change the same dependency | a build records the snapshot it was authored against; `publish_build` requires that snapshot to still be `published` (`expected_current`). The loser is marked `conflict`, nothing moves, and the UI offers **Rebase**. |
| cancellation, then a late old build tries to publish | publication requires `builds.status='running'` **and** an unguessable per-build token **and** the expected-current pointer. A cancelled build's status is no longer `running`, so its token is dead. |
| process dies before the switch completes | the pointer move is one SQLite transaction. Before it commits, nothing visible changed; after, the whole generation is visible. The reaper marks the dead worker `crashed`; a crashed build can never publish. |
| rows arrive during a rebuild | the build freezes a **per-source horizon** at claim time. Every source view is `seq > lower AND seq <= horizon`. New rows are invisible to this build and are consumed exactly once by the next run. |
| a candidate generation is internally incompatible | before every switch, each model's SQL is executed as `SELECT * FROM (<model sql>) LIMIT 0` against the exact candidate file set. A dangling column aborts the switch. |

The published generation is only one row of a linked list; each new generation
records its parent. Rollback publishes a **new** generation pointing at an old
snapshot's files (physical files are immutable), so a rollback is itself
auditable and reversible.

## 2. Cutover vs. grouped switching

The plan screen shows both modes with the actual affected counts, groups, wait
explanation and cost.

**Cutover** rebuilds the whole transitive downstream closure, then switches once.
It is simple, but the new generation appears only after the *deepest* affected
table finishes. If the graph has a 6-level chain and the edit changed the root,
readers wait for all six rebuilds before *any* of the new data is visible, even
though the shallow branches were ready long before. One switch, one failure
domain, no intermediate generations.

**Grouped** (default) rebuilds level by level and carries a **proof** at every
edge:

1. After rebuilding a table, compare its schema and *logical content hash*
   (order-insensitive over the full row multiset) with the currently published
   version.
2. If they are identical, no consumer can observe a difference. Remove
   `dep:<table>` from every downstream table's reasons; if a table loses its
   last reason it is skipped and its published file is reused, which discharges
   its own edges in turn — the pruning cascades down the chain in level order.
3. Split the remaining work into weakly-connected **families** (they share no
   path by construction) and publish each family as its own atomic generation
   when it finishes.

This is a *provable* compatibility argument rather than a heuristic: identity of
schema + full data means the edge cannot be observed. A long chain in family A
does not delay family B; the first new data usually appears after the shallowest
family, not after the longest chain.

The cost is more than one generation during a rollout, plus hash computation per
rebuilt table. Each intermediate generation is individually coherent — a reader
that resolves the pointer sees a complete warehouse, just with some families
already advanced and others not. That is a valid state because the families are
independent.

Both modes run the same binding check before each pointer move. A preview build
performs all the work and all the checks but never moves the pointer.

## 3. Incremental models and arrivals during a rebuild

`INSERT INTO <table> SELECT ...` declares an append-only model. It may read
**only** declared sources (no joins to other models, no aggregates, no
`GROUP BY`) and must keep an identical output schema; otherwise the build is
rejected with a precise message.

Each insert table stores `built_watermark`, the source horizon it has consumed.
A run reads rows in `(built_watermark, horizon]`, materializes a copy of the
previous published file plus those rows, and records the new watermark in the
new snapshot. When the model's body changes, it is re-seeded from the full source
history (never appended twice). Row loss is treated as an error.

Rows appended while the build runs necessarily have `seq > horizon`; they are
outside every model's window and are appended exactly once by the next run.
The frozen horizon is stored on the build row, so a long build cannot widen its
own input halfway through.

## 4. The SQL subset

One statement per model, `CREATE [OR REPLACE] TABLE ... AS SELECT ...` or
`INSERT INTO ... SELECT ...`.

* SELECT: columns, `col AS alias`, aggregates `COUNT/SUM/MIN/MAX/AVG`,
  `FROM t [AS x]`, plain `JOIN ... ON a.col = b.col`, `WHERE` with
  `AND`/`OR` predicates, `GROUP BY`, `HAVING` (requires `GROUP BY`).
* Identifiers `[A-Za-z_][A-Za-z0-9_]*`, string and integer literals.

Rejected **explicitly** with a `ParseError` (surfaced in the editor): `SELECT *`,
arithmetic/functions/expressions, `LIMIT`, `ORDER BY`, `DISTINCT`, subqueries,
CTEs, set operations, `LEFT JOIN`, `GROUPING SETS`, `HAVING` without
`GROUP BY`, `INSERT ... VALUES`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, column
definitions, backtick quoting, multiple statements per model, and unknown
identifiers with a list of available columns.

A rejected statement is never saved to a draft and never reaches a build.

## 5. The page

`web/` is a single-page console (no framework, no build step):

* **Dependency graph** by level, with kind, row count and watermark. In a draft
  it is built from the *draft* bodies, so brand-new tables are visible before
  they are ever built.
* **Model & impact** — edit SQL, save to draft, run `Analyze`. Impact shows
  output-column lineage (`output ← source.column`, with the `via` table chain),
  added/removed columns, every downstream consumer with its level, the longest
  path below the table, and — before anything is saved — consumers that would
  break because a selected column disappeared.
* **Preview** — a real worker build; shows per-table old/new row counts, schemas,
  and samples read from both physical files. Nothing is published.
* **Compare & rollback** — pick any two generations; the server runs
  `EXCEPT` in both directions against the two immutable files and returns real
  added/removed rows. **Roll back to A** publishes a new generation pointing at
  A's files.
* **Publish panel** — grouped/cutover choice, the plan (rebuild set, changed
  models, families, deepest level, new rows), the wait/cost explanation, first
  visible family and switch order, the rebase/discard actions, and a live build
  view (per-table status, progress, cancel).
* **Builds / events / generations** — recent history; every event is a real
  metadata transaction.

All displayed data comes from executed builds and metadata; the UI never
fabricates a diff, plan or count.

## 6. Project layout

```
strata/
  sqlparse.py     tokenizer, subset parser, checked AST, lineage primitives
  store.py        metadata schema, sources, snapshots, builds, publication txns
  engine.py       dependency graph, planning, proofs, execution, rollback
  worker.py       one build per OS process (token-gated publication)
  worker_launch.py spawn/wait/signal workers
  server.py       HTTP JSON API + static server
  demo.py         demo sources and eight models (chain + independent branch)
  __main__.py     `python3 -m strata serve`
tests/test_strata.py
web/index.html, web/style.css, web/app.js
```

## 7. API sketch

```
GET  /api/state                     everything the UI polls
GET  /api/edits/<id>/graph          draft dependency graph (incl. new tables)
GET  /api/edits/<id>/plan?mode=     rebuild set, groups, wait/cost
POST /api/edits/<id>/tables         validate + save a model into the draft
POST /api/analyze                   parse + field impact + consumer breakage
POST /api/edits/<id>/preview        real build, no publish (202 + build id)
POST /api/edits/<id>/publish        build + publish, grouped|cutover
POST /api/edits/<id>/rebase         move a stale draft onto the current generation
POST /api/builds/<id>/cancel        cooperative cancellation
GET  /api/builds/<id>               plan, per-table outcomes, result
GET  /api/compare?a=&b=             row-level diff of two snapshots
POST /api/rollback                  publish a new generation from an old snapshot
POST /api/increments                append source rows (validated)
```

## 8. Design notes / limits

* Snapshot metadata and files are retained; nothing is garbage-collected. Disk
  grows with generations, not with chain length, because files are shared across
  snapshots. A retention policy could delete unreferenced files safely.
* Grouped mode intentionally creates intermediate generations. If a consumer
  requires *all* families to appear simultaneously, cutover is the correct mode;
  this trade-off is shown in the plan panel.
* An `INSERT` model's source rows are identified by their global `seq`, not by
  business keys, so the system does not deduplicate rows a source re-sends.
* Worker crashes are detected by pid liveness and heartbeat age; a process that
  is alive but hung is only surfaced by its stale heartbeat, not killed.
