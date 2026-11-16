"""Unit and integration tests (stdlib unittest only).

Run: python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from strata import demo, engine  # noqa: E402
from strata.sqlparse import ParseError, parse_script  # noqa: E402
from strata.store import Store  # noqa: E402
from strata.worker_launch import worker_command  # noqa: E402


class TempStore(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix="strata-test-")
        self.store = Store(self.root)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    # helpers
    def seed(self) -> int:
        eid = demo.seed_demo(self.store)
        self.build(eid, "grouped")
        return eid

    def build(self, eid: int | None, mode: str = "grouped", **kwargs) -> dict:
        base = self.store.published_snapshot()
        b = self.store.create_build(eid, base.id, mode, self.store.max_source_seq())
        return engine.execute_build(self.store, b.id, **kwargs)

    def edit(self, changes: dict[str, tuple[str, str]], note: str = "") -> int:
        base = self.store.published_snapshot()
        eid = self.store.create_edit(base.id, note)
        for name, (kind, body) in changes.items():
            prev = self.store.snapshot_tables(base.id).get(name)
            version = (prev.version + 1) if prev else 1
            self.store.save_edit_table(eid, name, body, kind, version)
        return eid

    def add_paid(self, n: int) -> int:
        """Append n orders that are certainly paid; returns the watermark."""
        cur = self.store.source_counts().get("orders", 0)
        rows = [{"order_id": 100000 + cur + i, "customer_id": 1,
                 "region": "north", "amount": 10 + i, "status": "paid"}
                for i in range(n)]
        return self.store.append_rows({"orders": rows})["watermark"]

    def rows(self, table: str, snapshot: int | None = None) -> list[dict]:
        snap = (self.store.snapshot(snapshot) if snapshot
                else self.store.published_snapshot())
        t = self.store.snapshot_tables(snap.id)[table]
        return engine.sample_rows(self.store.physical_path(t.physical), table, 10000)


# ---------------------------------------------------------------------------
# parser


class ParserTests(unittest.TestCase):
    def test_accepts_supported_subset(self) -> None:
        stmts = parse_script(
            "CREATE TABLE t AS SELECT a, b.x AS y FROM a JOIN b ON a.id = b.id "
            "WHERE a.v > 3 AND b.w = 'q' GROUP BY a.v HAVING COUNT(id) > 1"
        )
        self.assertEqual(stmts[0].target, "t")
        s = stmts[0].select
        self.assertEqual([i.alias for i in s.items], ["a", "y"])
        self.assertEqual(len(s.joins), 1)
        self.assertEqual(len(s.where), 2)

    def test_accepts_insert_and_semicolon(self) -> None:
        stmts = parse_script(
            "INSERT INTO log SELECT id AS id FROM events WHERE kind = 'x';")
        self.assertEqual(stmts[0].kind, "insert")

    def test_comments_and_quotes(self) -> None:
        stmts = parse_script(
            "-- leading comment\nCREATE TABLE t AS SELECT id AS id FROM e "
            "WHERE name = 'a;b'")
        self.assertEqual(len(stmts), 1)
        self.assertIn("a;b", stmts[0].sql)

    def test_rejects_unsupported(self) -> None:
        bad = [
            "SELECT * FROM t",
            "CREATE TABLE t AS SELECT * FROM a",
            "CREATE TABLE t AS SELECT a.id FROM a JOIN b ON a.id > b.id",
            "CREATE TABLE t AS SELECT id AS id FROM a LIMIT 5",
            "CREATE TABLE t AS SELECT id AS id FROM a ORDER BY id",
            "CREATE TABLE t AS SELECT DISTINCT id AS id FROM a",
            "WITH x AS (SELECT id AS id FROM a) CREATE TABLE t AS SELECT id AS id FROM x",
            "CREATE TABLE t AS SELECT id AS id FROM a UNION SELECT id AS id FROM b",
            "CREATE TABLE t AS SELECT id + 1 AS id FROM a",
            "CREATE TABLE t AS SELECT abs(id) AS id FROM a",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "DROP TABLE t",
            "CREATE TABLE t (id INT)",
            "INSERT INTO t VALUES (1)",
            "CREATE TABLE t AS SELECT id AS id FROM a LEFT JOIN b ON a.id = b.id",
            "CREATE TABLE t AS SELECT id AS id FROM a GROUP BY GROUPING SETS (id)",
            "CREATE TABLE t AS SELECT id AS id FROM a HAVING COUNT(id) > 1",
            "CREATE TABLE t AS SELECT `id` AS id FROM a",
        ]
        for sql in bad:
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError):
                    parse_script(sql)

    def test_rejects_multiple_statements_per_model(self) -> None:
        with self.assertRaises(ParseError):
            parse_script("CREATE TABLE a AS SELECT id AS id FROM s;"
                         "CREATE TABLE b AS SELECT id AS id FROM a")

    def test_target_must_match_model(self) -> None:
        with self.assertRaises(ParseError):
            engine.compile_model("wrong", 1, "create",
                                 "CREATE TABLE right AS SELECT id AS id FROM s")

    def test_star_message_mentions_lineage(self) -> None:
        with self.assertRaises(ParseError) as cm:
            parse_script("CREATE TABLE t AS SELECT * FROM a")
        self.assertIn("lineage", str(cm.exception))


# ---------------------------------------------------------------------------
# lineage and impact


class LineageTests(TempStore):
    def test_column_lineage_through_chain(self) -> None:
        self.seed()
        models = engine.model_map({
            n: {"version": t.version, "kind": t.kind, "body": t.body}
            for n, t in self.store.snapshot_tables(
                self.store.published_snapshot().id).items()})
        source_columns = {s: [c["name"] for c in cols]
                          for s, cols in self.store.source_defs().items()}
        lineage, _ = engine.column_lineage(models, source_columns)
        self.assertEqual(lineage["exec_dashboard"]["revenue"]["from"],
                         ["orders.amount"])
        self.assertIn("clean_orders", lineage["exec_dashboard"]["revenue"]["via"])
        self.assertEqual(lineage["click_pages"]["clicks"]["from"],
                         ["clicks.customer_id"])

    def test_analyze_reports_removed_column_breakage(self) -> None:
        from strata.server import App
        self.seed()
        app = App(self.root, inline_workers=True)
        try:
            res = app.analyze({
                "table": "clean_orders",
                "kind": "create",
                "sql": "CREATE TABLE clean_orders AS "
                       "SELECT order_id AS order_id, customer_id AS customer_id, "
                       "region AS region FROM orders WHERE status = 'paid'",
            })
            self.assertTrue(res["ok"])
            self.assertIn("amount", res["impact"]["removed_columns"])
            self.assertTrue(any(p["table"] == "order_customers"
                                for p in res["problems"]))
        finally:
            app.close()

    def test_analyze_rejects_bad_sql(self) -> None:
        from strata.server import ApiError, App
        app = App(self.root, inline_workers=True)
        try:
            with self.assertRaises(ApiError):
                app.analyze({"table": "t", "kind": "create",
                             "sql": "CREATE TABLE t AS SELECT * FROM orders"})
        finally:
            app.close()


# ---------------------------------------------------------------------------
# publishing semantics


class PublishingTests(TempStore):
    def test_initial_publish_is_one_snapshot(self) -> None:
        self.seed()
        snap = self.store.published_snapshot()
        tables = self.store.snapshot_tables(snap.id)
        self.assertEqual(sorted(tables), sorted(m["name"] for m in demo.MODELS))
        for t in tables.values():
            self.assertTrue(self.store.physical_path(t.physical).exists())

    def test_readers_never_see_mixed_generations(self) -> None:
        """While a grouped rollout publishes families one at a time, a reader
        that pins a snapshot id sees a coherent set of files."""
        self.seed()
        base = self.store.published_snapshot()
        before = self._fingerprint(base.id)
        eid = self.edit({
            "clean_orders": ("create",
                "CREATE TABLE clean_orders AS SELECT order_id, customer_id, "
                "region, amount, status FROM orders WHERE status = 'paid' "
                "AND amount > 100"),
        })
        res = self.build(eid, "grouped")
        self.assertEqual(res["status"], "published", res.get("error"))
        mid = self.store.published_snapshot()
        after = self._fingerprint(mid.id)
        # The pinned old snapshot is still fully readable and unchanged.
        self.assertEqual(before, self._fingerprint(base.id))
        # Every table in the new snapshot resolves and matches its row count.
        for name, t in self.store.snapshot_tables(mid.id).items():
            self.assertTrue(self.store.physical_path(t.physical).exists(), name)
            got = len(self.rows(name, mid.id))
            self.assertEqual(got, t.row_count, name)
        self.assertNotEqual(before, after)

    def _fingerprint(self, sid: int) -> dict:
        return {n: (t.content_hash, t.physical, t.row_count)
                for n, t in self.store.snapshot_tables(sid).items()}

    def test_grouped_prunes_when_content_identical(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({
            "clean_orders": ("create",
                "-- reformatted only\nCREATE  TABLE clean_orders AS\n"
                "SELECT order_id, customer_id, region, amount, status\n"
                "FROM orders WHERE status = 'paid'"),
        })
        res = self.build(eid, "grouped")
        self.assertEqual(res["status"], "published", res.get("error"))
        self.assertEqual(res["result"]["groups"][0]["rebuilt"], [])
        rewritten = {p["table"] for p in res["result"]["pruned"]}
        self.assertIn("clean_orders", rewritten)
        self.assertIn("order_customers", rewritten)
        # pointer did not move because nothing changed
        self.assertEqual(self.store.published_snapshot().id, base.id)

    def test_cutover_rebuilds_whole_closure(self) -> None:
        self.seed()
        eid = self.edit({
            "clean_orders": ("create",
                "CREATE TABLE clean_orders AS SELECT order_id, customer_id, "
                "region, amount, status FROM orders WHERE status = 'paid' "
                "AND amount > 100"),
        })
        res = self.build(eid, "cutover")
        self.assertEqual(res["status"], "published", res.get("error"))
        rebuilt = res["result"]["groups"][0]["rebuilt"]
        self.assertEqual(set(rebuilt), {
            "clean_orders", "order_customers", "regional_totals",
            "segment_totals", "exec_dashboard", "segment_dashboard"})
        self.assertEqual(len(res["result"]["groups"]), 1)

    def test_independent_families_publish_separately(self) -> None:
        self.seed()
        eid = self.edit({
            "clean_orders": ("create",
                "CREATE TABLE clean_orders AS SELECT order_id, customer_id, "
                "region, amount, status FROM orders WHERE status = 'paid' "
                "AND amount > 100"),
            "click_pages": ("create",
                "CREATE TABLE click_pages AS SELECT page AS page, "
                "COUNT(customer_id) AS clicks FROM clicks "
                "WHERE page <> '/' GROUP BY page"),
        })
        res = self.build(eid, "grouped")
        self.assertEqual(res["status"], "published", res.get("error"))
        groups = [g for g in res["result"]["groups"] if g["snapshot"]]
        self.assertGreaterEqual(len(groups), 2)
        # The order-family and the click-family are never in the same snapshot.
        for g in groups:
            fam = set(g["tables"])
            self.assertFalse("click_pages" in fam and "clean_orders" in fam)

    def test_schema_break_aborts_before_pointer_moves(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({
            "clean_orders": ("create",
                "CREATE TABLE clean_orders AS SELECT order_id, customer_id, "
                "region FROM orders WHERE status = 'paid'"),
        })
        res = self.build(eid, "grouped")
        self.assertNotEqual(res["status"], "published")
        self.assertIn("SQL error", res.get("error") or "")
        self.assertEqual(self.store.published_snapshot().id, base.id)
        # Original downstream still readable.
        self.assertEqual(
            [c["name"] for c in
             self.store.snapshot_tables(base.id)["order_customers"].schema],
            ["order_id", "amount", "region", "segment"])

    def test_removal_of_table_is_rejected(self) -> None:
        eid = self.seed()
        base = self.store.published_snapshot()
        eid2 = self.store.create_edit(base.id)
        tables = self.store.edit_tables(eid2)
        for n, t in tables.items():
            if n != "exec_dashboard":
                self.store.save_edit_table(eid2, n, t["body"], t["kind"],
                                           t["version"])
        self.store.remove_edit_table(eid2, "exec_dashboard")
        with self.assertRaises(engine.PlanError):
            engine.compute_plan(
                self.store, edit_id=eid2, base_snapshot=base.id,
                table_defs={n: {"version": t["version"], "kind": t["kind"],
                                "body": t["body"]}
                            for n, t in self.store.edit_tables(eid2).items()})

    def test_cycle_rejected(self) -> None:
        self.seed()
        eid = self.edit({
            "exec_dashboard": ("create",
                "CREATE TABLE exec_dashboard AS SELECT region AS region, "
                "revenue AS revenue FROM seg_loop"),
            "seg_loop": ("create",
                "CREATE TABLE seg_loop AS SELECT region AS region, "
                "revenue AS revenue FROM exec_dashboard"),
        })
        with self.assertRaises(engine.PlanError):
            engine.compute_plan(
                self.store, edit_id=eid,
                base_snapshot=self.store.published_snapshot().id,
                table_defs={n: {"version": t["version"], "kind": t["kind"],
                                "body": t["body"]}
                            for n, t in self.store.edit_tables(eid).items()})
        res = self.build(eid, "grouped")
        self.assertNotEqual(res["status"], "published")


# ---------------------------------------------------------------------------
# incremental


class IncrementalTests(TempStore):
    def test_append_only_and_no_double_apply(self) -> None:
        self.seed()
        n0 = len(self.rows("order_log"))
        self.add_paid(8)
        b = self.store.create_build(None, self.store.published_snapshot().id,
                                    "grouped", self.store.max_source_seq())
        res = engine.execute_build(self.store, b.id)
        self.assertEqual(res["status"], "published", res.get("error"))
        n1 = len(self.rows("order_log"))
        self.assertEqual(n1, n0 + 8)
        # second run with no new rows must not append again
        b2 = self.store.create_build(None, self.store.published_snapshot().id,
                                     "grouped", self.store.max_source_seq())
        res2 = engine.execute_build(self.store, b2.id)
        self.assertEqual(res2["status"], "published", res2.get("error"))
        self.assertEqual(len(self.rows("order_log")), n1)

    def test_incremental_schema_change_rejected(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({
            "order_log": ("insert",
                "INSERT INTO order_log SELECT order_id AS order_id, "
                "amount AS amount FROM orders WHERE status = 'paid'"),
        })
        res = self.build(eid, "grouped")
        self.assertNotEqual(res["status"], "published")
        self.assertIn("schema", (res.get("error") or "").lower())
        self.assertEqual(self.store.published_snapshot().id, base.id)

    def test_incremental_cannot_aggregate_or_read_models(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        for body in (
            "INSERT INTO order_log SELECT region AS region, SUM(amount) AS total "
            "FROM orders GROUP BY region",
            "INSERT INTO order_log SELECT order_id AS order_id FROM clean_orders",
        ):
            eid = self.edit({"order_log": ("insert", body)})
            res = self.build(eid, "grouped")
            self.assertNotEqual(res["status"], "published", body)
            self.assertEqual(self.store.published_snapshot().id, base.id)

    def test_rows_arriving_during_build_are_not_lost_or_duplicated(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        before = len(self.rows("order_log"))
        self.add_paid(5)
        b = self.store.create_build(None, base.id, "grouped",
                                    self.store.max_source_seq())
        # simulate an arrival between build creation and execution
        horizon = b.watermark
        self.add_paid(4)
        res = engine.execute_build(self.store, b.id)
        self.assertEqual(res["status"], "published", res.get("error"))
        after_first = len(self.rows("order_log"))
        self.assertEqual(after_first, before + 5)
        b2 = self.store.create_build(None, self.store.published_snapshot().id,
                                     "grouped", self.store.max_source_seq())
        engine.execute_build(self.store, b2.id)
        self.assertEqual(len(self.rows("order_log")), before + 9)
        self.assertGreater(horizon, 0)


# ---------------------------------------------------------------------------
# concurrency, cancellation, crash, late results


class RobustnessTests(TempStore):
    def test_two_branches_same_dependency_one_wins(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        e1 = self.edit({"clean_orders": ("create",
            "CREATE TABLE clean_orders AS SELECT order_id, customer_id, region, "
            "amount, status FROM orders WHERE status = 'paid' AND amount > 50")})
        e2 = self.edit({"regional_totals": ("create",
            "CREATE TABLE regional_totals AS SELECT region AS region, "
            "SUM(amount) AS revenue, COUNT(order_id) AS n FROM order_customers "
            "WHERE amount > 60 GROUP BY region")})
        b1 = self.store.create_build(e1, base.id, "grouped",
                                     self.store.max_source_seq())
        b2 = self.store.create_build(e2, base.id, "grouped",
                                     self.store.max_source_seq())
        r1 = engine.execute_build(self.store, b1.id)
        self.assertEqual(r1["status"], "published")
        snap_after_1 = self.store.published_snapshot()
        # The second build was launched from the old base; it must not publish.
        r2 = engine.execute_build(self.store, b2.id)
        self.assertIn(r2["status"], ("conflict", "cancelling", "cancelled"))
        self.assertEqual(self.store.published_snapshot().id, snap_after_1.id)
        # edit 2 is still a draft and can be rebased + published.
        reb = self.store.snapshot_tables(snap_after_1.id)["clean_orders"]
        self.assertEqual(reb.row_count,
                         sum(1 for r in self.rows("clean_orders")
                             if r["amount"] > 50))

    def test_cancelled_build_never_publishes(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({"clean_orders": ("create",
            "CREATE TABLE clean_orders AS SELECT order_id, customer_id, region, "
            "amount, status FROM orders WHERE status = 'paid' AND amount > 400")})
        b = self.store.create_build(eid, base.id, "grouped",
                                    self.store.max_source_seq())
        self.store.cancel_build(b.id)
        res = engine.execute_build(self.store, b.id)
        self.assertEqual(res["status"], "cancelled")
        self.assertEqual(self.store.published_snapshot().id, base.id)
        self.assertEqual(self.store.edit(eid)["status"], "draft")

    def test_reaper_respects_utc_created_at(self) -> None:
        """A queued build younger than the stale threshold must survive a reap
        even when the host timezone is not UTC (the old code double-corrected)."""
        self.seed()
        base = self.store.published_snapshot()
        b = self.store.create_build(None, base.id, "grouped",
                                    self.store.max_source_seq())
        self.assertEqual(self.store.reap_builds(stale_after_seconds=60), [])
        self.assertEqual(self.store.build(b.id).status, "queued")
        # With a zero threshold a pid-less queued build is considered abandoned.
        reaped = self.store.reap_builds(stale_after_seconds=0)
        self.assertEqual(reaped[0]["status"], "crashed")
        self.assertEqual(self.store.published_snapshot().id, base.id)

    def test_cancel_during_running_build(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({"clean_orders": ("create",
            "CREATE TABLE clean_orders AS SELECT order_id, customer_id, region, "
            "amount, status FROM orders WHERE status = 'paid' AND amount > 400")})
        b = self.store.create_build(eid, base.id, "grouped",
                                    self.store.max_source_seq())
        self.assertEqual(self.store.claim_build(b.id).status, "running")
        self.assertEqual(self.store.cancel_build(b.id), "cancelling")
        res = engine.execute_build(self.store, b.id)
        self.assertEqual(res["status"], "cancelled")
        self.assertEqual(self.store.published_snapshot().id, base.id)

    def test_late_publish_from_stale_token_rejected(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({"clean_orders": ("create",
            "CREATE TABLE clean_orders AS SELECT order_id, customer_id, region, "
            "amount, status FROM orders WHERE status = 'paid' AND amount > 400")})
        b = self.store.create_build(eid, base.id, "grouped",
                                    self.store.max_source_seq())
        engine.execute_build(self.store, b.id)
        snap = self.store.published_snapshot()
        # Replaying the publish with the old token and old expected-current.
        sid, reason = self.store.publish_build(
            b.id, self.store.build(b.id).token, 99, 0, {}, "late", base.id)
        self.assertEqual(sid, 0)
        self.assertNotEqual(reason, "published")
        self.assertEqual(self.store.published_snapshot().id, snap.id)

    def test_worker_crash_before_publish_leaves_pointer(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({"clean_orders": ("create",
            "CREATE TABLE clean_orders AS SELECT order_id, customer_id, region, "
            "amount, status FROM orders WHERE status = 'paid' AND amount > 400")})
        b = self.store.create_build(eid, base.id, "grouped",
                                    self.store.max_source_seq())
        env = dict(os.environ, STRATA_CRASH="before_publish")
        p = subprocess.run(worker_command(self.root, b.id), env=env,
                           capture_output=True, text=True, timeout=120)
        self.assertEqual(p.returncode, 17, p.stderr)
        self.assertEqual(self.store.published_snapshot().id, base.id)
        self.assertEqual(self.store.build(b.id).status, "running")
        reaped = self.store.reap_builds(stale_after_seconds=0)
        self.assertEqual(reaped[0]["status"], "crashed")
        self.assertEqual(self.store.published_snapshot().id, base.id)
        self.assertEqual(self.store.edit(eid)["status"], "draft")

    def test_crash_mid_build_after_one_family(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({
            "clean_orders": ("create",
                "CREATE TABLE clean_orders AS SELECT order_id, customer_id, "
                "region, amount, status FROM orders WHERE status = 'paid' "
                "AND amount > 400"),
            "click_pages": ("create",
                "CREATE TABLE click_pages AS SELECT page AS page, "
                "COUNT(customer_id) AS clicks FROM clicks GROUP BY page "
                "HAVING COUNT(customer_id) > 0"),
        })
        b = self.store.create_build(eid, base.id, "grouped",
                                    self.store.max_source_seq())
        env = dict(os.environ, STRATA_CRASH="after_publish")
        p = subprocess.run(worker_command(self.root, b.id), env=env,
                           capture_output=True, text=True, timeout=120)
        self.assertEqual(p.returncode, 17, p.stderr)
        snap = self.store.published_snapshot()
        # One family may have switched; whatever the pointer says must be complete.
        tables = self.store.snapshot_tables(snap.id)
        self.assertEqual(len(tables), len(demo.MODELS))
        for name, t in tables.items():
            self.assertTrue(self.store.physical_path(t.physical).exists(), name)
        self.assertNotEqual(snap.id, base.id)
        self.store.reap_builds(stale_after_seconds=0)
        self.assertEqual(self.store.published_snapshot().id, snap.id)

    def test_rollback_switches_atomically(self) -> None:
        self.seed()
        first = self.store.published_snapshot()
        eid = self.edit({"clean_orders": ("create",
            "CREATE TABLE clean_orders AS SELECT order_id, customer_id, region, "
            "amount, status FROM orders WHERE status = 'paid' AND amount > 200")})
        self.build(eid, "cutover")
        second = self.store.published_snapshot()
        self.assertNotEqual(first.id, second.id)
        res = engine.rollback_to(self.store, first.id, "undo")
        third = self.store.published_snapshot()
        self.assertEqual(third.generation, second.generation + 1)
        self.assertEqual(
            {n: t.content_hash for n, t in
             self.store.snapshot_tables(third.id).items()},
            {n: t.content_hash for n, t in
             self.store.snapshot_tables(first.id).items()})
        # rollback target snapshot is still intact
        self.assertTrue(all(self.store.physical_path(t.physical).exists()
                            for t in self.store.snapshot_tables(first.id).values()))


# ---------------------------------------------------------------------------
# preview


class PreviewTests(TempStore):
    def test_preview_does_not_publish(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({"clean_orders": ("create",
            "CREATE TABLE clean_orders AS SELECT order_id, customer_id, region, "
            "amount, status FROM orders WHERE status = 'paid' AND amount > 500")})
        b = self.store.create_build(eid, base.id, "preview",
                                    self.store.max_source_seq())
        res = engine.execute_build(self.store, b.id)
        self.assertEqual(res["status"], "ready", res.get("error"))
        self.assertEqual(self.store.published_snapshot().id, base.id)
        diffs = {d["table"]: d for d in res["result"]["diffs"]}
        self.assertIn("clean_orders", diffs)
        self.assertIsNotNone(diffs["clean_orders"]["old_rows"])
        self.assertEqual(self.store.edit(eid)["status"], "draft")

    def test_preview_shows_schema_change(self) -> None:
        self.seed()
        base = self.store.published_snapshot()
        eid = self.edit({"click_pages": ("create",
            "CREATE TABLE click_pages AS SELECT page AS page, "
            "COUNT(customer_id) AS hits FROM clicks GROUP BY page")})
        b = self.store.create_build(eid, base.id, "preview",
                                    self.store.max_source_seq())
        res = engine.execute_build(self.store, b.id)
        self.assertEqual(res["status"], "ready", res.get("error"))
        d = {x["table"]: x for x in res["result"]["diffs"]}["click_pages"]
        self.assertTrue(d["schema_changed"])
        self.assertEqual(d["new_schema"], ["page", "hits"])


# ---------------------------------------------------------------------------
# HTTP API


class HttpTests(TempStore):
    def setUp(self) -> None:
        super().setUp()
        from strata.server import make_server
        self.server = make_server(self.root, "127.0.0.1", 0, inline_workers=True)
        self.port = self.server.server_address[1]
        import threading
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def req(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}",
                                   data=data, method=method,
                                   headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(r, timeout=30) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def wait_build(self, bid: int, timeout: float = 60) -> dict:
        t0 = time.time()
        while time.time() - t0 < timeout:
            _, d = self.req("GET", f"/api/builds/{bid}")
            if d["status"] in ("published", "ready", "failed", "cancelled",
                               "conflict", "crashed"):
                return d
            time.sleep(0.05)
        raise AssertionError("build did not finish")

    def test_full_workflow_over_http(self) -> None:
        code, d = self.req("POST", "/api/seed", {"demo": "orders"})
        self.assertEqual(code, 201, d)
        eid = d["edit"]["id"]

        code, plan = self.req("GET", f"/api/edits/{eid}/plan")
        self.assertEqual(code, 200, plan)
        self.assertTrue(plan["plan"]["tables"])

        code, pv = self.req("POST", f"/api/edits/{eid}/preview")
        self.assertEqual(code, 202, pv)
        d = self.wait_build(pv["build"])
        self.assertEqual(d["status"], "ready")
        self.assertTrue(d["result"]["diffs"])

        code, pub = self.req("POST", f"/api/edits/{eid}/publish", {"mode": "grouped"})
        self.assertEqual(code, 202, pub)
        d = self.wait_build(pub["build"])
        self.assertEqual(d["status"], "published", d.get("error"))

        code, state = self.req("GET", "/api/state")
        self.assertEqual(code, 200)
        self.assertEqual(len(state["tables"]), len(demo.MODELS))
        self.assertEqual(state["published"]["generation"], state["snapshots"][0]["generation"])

        code, rows = self.req("GET", "/api/tables/regional_totals?limit=10")
        self.assertEqual(code, 200, rows)
        self.assertTrue(rows["rows"])
        self.assertEqual(rows["row_count"], 4)

        # history and rollback
        code, snaps = self.req("GET", "/api/snapshots")
        self.assertGreaterEqual(len(snaps["snapshots"]), 2)
        first = [s for s in snaps["snapshots"] if s["generation"] == 0][0]
        code, rb = self.req("POST", "/api/rollback", {"snapshot": first["id"]})
        self.assertEqual(code, 201, rb)
        code, state2 = self.req("GET", "/api/state")
        self.assertEqual(state2["tables"], [])

    def test_analyze_and_reject_over_http(self) -> None:
        self.req("POST", "/api/seed", {})
        code, d = self.req("POST", "/api/analyze", {
            "table": "clean_orders", "kind": "create",
            "sql": "CREATE TABLE clean_orders AS SELECT order_id AS order_id FROM orders",
        })
        self.assertEqual(code, 200, d)
        self.assertIn("order_customers", d["impact"]["downstream"])
        code, d = self.req("POST", "/api/analyze", {
            "table": "clean_orders", "kind": "create",
            "sql": "CREATE TABLE clean_orders AS SELECT * FROM orders"})
        self.assertEqual(code, 422, d)
        self.assertTrue(d.get("parse_error"))

    def test_increment_and_reconcile_http(self) -> None:
        self.req("POST", "/api/seed", {})
        eid = self.req("GET", "/api/state")[1]["edits"][0]["id"]
        _, pub = self.req("POST", f"/api/edits/{eid}/publish", {"mode": "grouped"})
        self.assertEqual(self.wait_build(pub["build"])["status"], "published")
        code, inc = self.req("POST", "/api/increments", {"rows": {"orders": [
            {"order_id": 9001, "customer_id": 1, "region": "north",
             "amount": 42, "status": "paid"}]}})
        self.assertEqual(code, 200, inc)
        code, rec = self.req("POST", "/api/reconcile")
        self.assertEqual(code, 202, rec)
        d = self.wait_build(rec["build"])
        self.assertEqual(d["status"], "published", d.get("error"))
        code, rows = self.req("GET", "/api/tables/order_log?limit=500")
        self.assertTrue(any(r["order_id"] == 9001 for r in rows["rows"]))

    def test_edit_rebase_after_competing_publish(self) -> None:
        self.req("POST", "/api/seed", {})
        eid = self.req("GET", "/api/state")[1]["edits"][0]["id"]
        self.req("POST", f"/api/edits/{eid}/publish", {"mode": "grouped"})
        self.wait_build(self.req("GET", "/api/state")[1]["builds"][0]["id"])

        # Two fresh drafts touching different tables from the same base.
        e1 = self.req("POST", "/api/edits", {"note": "a"})[1]["edit"]["id"]
        e2 = self.req("POST", "/api/edits", {"note": "b"})[1]["edit"]["id"]
        code, _ = self.req("PUT", "/api/edits/%d/tables" % e1, {
            "name": "clean_orders", "kind": "create",
            "sql": "CREATE TABLE clean_orders AS SELECT order_id AS order_id, "
                   "customer_id AS customer_id, region AS region, amount AS amount, "
                   "status AS status FROM orders WHERE status = 'paid' "
                   "AND amount > 400"})
        self.assertEqual(code, 200)
        code, _ = self.req("PUT", "/api/edits/%d/tables" % e2, {
            "name": "segment_totals", "kind": "create",
            "sql": "CREATE TABLE segment_totals AS SELECT segment AS segment, "
                   "SUM(amount) AS revenue, COUNT(order_id) AS n FROM order_customers "
                   "WHERE amount > 1 GROUP BY segment"})
        self.assertEqual(code, 200)
        code, pub1 = self.req("POST", f"/api/edits/{e1}/publish", {"mode": "cutover"})
        d1 = self.wait_build(pub1["build"])
        self.assertEqual(d1["status"], "published")
        code, pub2 = self.req("POST", f"/api/edits/{e2}/publish", {"mode": "cutover"})
        self.assertEqual(code, 202)
        d2 = self.wait_build(pub2["build"])
        self.assertIn(d2["status"], ("conflict", "cancelling", "failed"))
        code, reb = self.req("POST", f"/api/edits/{e2}/rebase")
        self.assertEqual(code, 200, reb)
        code, pub3 = self.req("POST", f"/api/edits/{e2}/publish", {"mode": "cutover"})
        d3 = self.wait_build(pub3["build"])
        self.assertEqual(d3["status"], "published", d3.get("error"))

    def test_static_and_404(self) -> None:
        r = urllib.request.urlopen(f"http://127.0.0.1:{self.port}/", timeout=10)
        self.assertEqual(r.status, 200)
        self.assertIn(b"Strata", r.read())
        code, d = self.req("GET", "/api/nope")
        self.assertEqual(code, 404)

    def test_edit_graph_compare_and_rollback_http(self) -> None:
        """The UI's remaining read paths are real server executions."""
        self.req("POST", "/api/seed", {})
        eid = self.req("GET", "/api/state")[1]["edits"][0]["id"]

        code, g = self.req("GET", f"/api/edits/{eid}/graph")
        self.assertEqual(code, 200, g)
        self.assertEqual(len(g["tables"]), len(demo.MODELS))
        self.assertTrue(all(t["is_new"] for t in g["tables"]))

        code, _ = self.req("PUT", f"/api/edits/{eid}/tables", {
            "name": "north_orders", "kind": "create",
            "sql": "CREATE TABLE north_orders AS SELECT order_id AS order_id, "
                   "amount AS amount FROM clean_orders WHERE region = 'north'"})
        self.assertEqual(code, 200)
        code, g = self.req("GET", f"/api/edits/{eid}/graph")
        new = [t for t in g["tables"] if t["name"] == "north_orders"][0]
        self.assertEqual(new["level"], 1)
        self.assertTrue(new["is_new"])
        self.assertIn("clean_orders", new["upstream"])

        _, pub = self.req("POST", f"/api/edits/{eid}/publish", {"mode": "grouped"})
        self.assertEqual(self.wait_build(pub["build"])["status"], "published")
        st = self.req("GET", "/api/state")[1]
        snaps = st["snapshots"]
        newest, oldest = snaps[0]["id"], snaps[-1]["id"]

        code, cmp = self.req("GET", f"/api/compare?a={oldest}&b={newest}")
        self.assertEqual(code, 200, cmp)
        self.assertIn("north_orders", cmp["differing"])
        for t in cmp["tables"]:
            self.assertIn("rows_added", t)   # always present for the UI

        code, rb = self.req("POST", "/api/rollback",
                            {"snapshot": oldest, "note": "test"})
        self.assertEqual(code, 201, rb)
        st2 = self.req("GET", "/api/state")[1]
        self.assertEqual(st2["tables"], [])
        self.assertEqual(st2["published"]["generation"],
                         st["published"]["generation"] + 1)

        # Rolling back a rollback restores the full table set.
        code, roll = self.req("GET", f"/api/compare?a={newest}&b={oldest}")
        self.assertEqual(code, 200)
        code, rb2 = self.req("POST", "/api/rollback", {"snapshot": newest})
        self.assertEqual(code, 201, rb2)
        st3 = self.req("GET", "/api/state")[1]
        self.assertEqual(len(st3["tables"]), len(demo.MODELS) + 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
