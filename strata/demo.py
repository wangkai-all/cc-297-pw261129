"""Demo fixtures used by the UI and tests.

The default cohort is a 7-table dependency chain plus a small independent
branch, so the difference between whole-group cutover and grouped publication is
visible in the timing panel.
"""
from __future__ import annotations

import random
from typing import Iterable

from .store import Store

SCHEMAS: dict[str, list[dict]] = {
    "orders": [
        {"name": "order_id", "type": "INTEGER"},
        {"name": "customer_id", "type": "INTEGER"},
        {"name": "region", "type": "TEXT"},
        {"name": "amount", "type": "INTEGER"},
        {"name": "status", "type": "TEXT"},
    ],
    "customers": [
        {"name": "customer_id", "type": "INTEGER"},
        {"name": "segment", "type": "TEXT"},
    ],
    "clicks": [
        {"name": "click_id", "type": "INTEGER"},
        {"name": "customer_id", "type": "INTEGER"},
        {"name": "page", "type": "TEXT"},
    ],
}

MODELS: list[dict] = [
    {"name": "clean_orders", "kind": "create", "body":
        "CREATE TABLE clean_orders AS\n"
        "SELECT order_id, customer_id, region, amount, status\n"
        "FROM orders\n"
        "WHERE status = 'paid'"},
    {"name": "order_customers", "kind": "create", "body":
        "CREATE TABLE order_customers AS\n"
        "SELECT o.order_id AS order_id, o.amount AS amount, o.region AS region,\n"
        "       c.segment AS segment\n"
        "FROM clean_orders o JOIN customers c ON o.customer_id = c.customer_id"},
    {"name": "regional_totals", "kind": "create", "body":
        "CREATE TABLE regional_totals AS\n"
        "SELECT region AS region, SUM(amount) AS revenue, COUNT(order_id) AS n\n"
        "FROM order_customers\n"
        "GROUP BY region"},
    {"name": "segment_totals", "kind": "create", "body":
        "CREATE TABLE segment_totals AS\n"
        "SELECT segment AS segment, SUM(amount) AS revenue, COUNT(order_id) AS n\n"
        "FROM order_customers\n"
        "GROUP BY segment"},
    {"name": "exec_dashboard", "kind": "create", "body":
        "CREATE TABLE exec_dashboard AS\n"
        "SELECT region AS region, revenue AS revenue, n AS orders\n"
        "FROM regional_totals"},
    {"name": "segment_dashboard", "kind": "create", "body":
        "CREATE TABLE segment_dashboard AS\n"
        "SELECT segment AS segment, revenue AS revenue, n AS orders\n"
        "FROM segment_totals"},
    {"name": "click_pages", "kind": "create", "body":
        "CREATE TABLE click_pages AS\n"
        "SELECT page AS page, COUNT(customer_id) AS clicks\n"
        "FROM clicks\n"
        "GROUP BY page"},
    {"name": "order_log", "kind": "insert", "body":
        "INSERT INTO order_log\n"
        "SELECT order_id AS order_id, amount AS amount, region AS region\n"
        "FROM orders\n"
        "WHERE status = 'paid'"},
]


def _orders(n: int, start: int = 1) -> list[dict]:
    regions = ["north", "south", "east", "west"]
    rng = random.Random(7)
    return [
        {
            "order_id": i,
            "customer_id": (i % 20) + 1,
            "region": regions[i % 4],
            "amount": rng.randint(5, 500),
            "status": "paid" if i % 5 else "pending",
        }
        for i in range(start, start + n)
    ]


def _customers() -> list[dict]:
    return [{"customer_id": i, "segment": ["smb", "mid", "ent"][i % 3]}
            for i in range(1, 21)]


def _clicks(n: int, start: int = 1) -> list[dict]:
    pages = ["/", "/pricing", "/docs", "/blog"]
    return [{"click_id": i, "customer_id": (i % 20) + 1,
             "page": pages[i % 4]} for i in range(start, start + n)]


def seed_demo(store: Store, name: str = "orders") -> int:
    """Create sources, initial rows, and a draft edit holding the demo models."""
    existing = set(store.source_defs())
    for s, cols in SCHEMAS.items():
        if s not in existing:
            store.create_source(s, cols)
    store.append_rows({
        "orders": _orders(120),
        "customers": _customers(),
        "clicks": _clicks(60),
    })
    base = store.published_snapshot()
    eid = store.create_edit(base.id, "seed demo models")
    for m in MODELS:
        store.save_edit_table(eid, m["name"], m["body"], m["kind"], 1)
    return eid


def add_increment(store: Store, n_orders: int = 10) -> dict:
    n = store.source_counts().get("orders", 0)
    return store.append_rows({"orders": _orders(n_orders, start=n + 1)})


def make_increment(store: Store, n_orders: int = 5) -> dict:
    """Rows for the demo ``orders`` source, continuing its id sequence."""
    n = store.source_counts().get("orders", 0)
    return {"orders": _orders(n_orders, start=n + 1)}
