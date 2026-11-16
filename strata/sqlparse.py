"""SQL text utilities: tokenizing, statement splitting, parsing into a checked AST.

Supported grammar (deliberately small; everything else is rejected with a precise
ParseError):

  CREATE [OR REPLACE] TABLE <name> AS <select>
  INSERT INTO <name> [ (col, ...) ] <select>
  SELECT c1 [AS a1], ... FROM <name> [AS x] [JOIN <name> [AS y] ON <col> = <col>]...
  ... [WHERE <pred>] [GROUP BY <col>, ...] [HAVING <pred>]

  c := * | <col> | <col> AS <alias> | <agg>(<col>) [AS <alias>]
  agg := COUNT | SUM | MIN | MAX | AVG
  pred := <operand> <cmp> <operand> [AND|OR ...]
  operand := <col> | <literal> | <agg>(<col>)

  <name> := ident ('.' ident)?   -- database qualifier is accepted only when it
                                    matches the engine's `main` database.
  identifier := [A-Za-z_][A-Za-z0-9_]*

There are no subqueries, CTEs, window functions, set operations, expressions,
DISTINCT, LIMIT/OFFSET, ORDER BY, INSERT ... VALUES, DELETE, UPDATE or DDL other
than the two forms above.  Those are *rejected* rather than mis-parsed.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional


class ParseError(ValueError):
    """Raised for any syntax outside the supported subset."""


@dataclass(frozen=True)
class ColumnRef:
    table: Optional[str]   # qualifier, or None
    name: str

    def sql(self) -> str:
        return f"{self.table}.{self.name}" if self.table else self.name


@dataclass(frozen=True)
class Literal:
    value: object
    sql: str


@dataclass(frozen=True)
class Aggregate:
    func: str
    arg: str
    alias: str


@dataclass(frozen=True)
class SelectItem:
    kind: str                  # 'star' | 'column' | 'literal' | 'aggregate'
    name: str = ""             # source column / aggregate argument
    alias: str = ""            # output name (may equal name)
    func: str = ""
    ref: Optional[ColumnRef] = None


@dataclass(frozen=True)
class Join:
    table: str
    alias: str
    cond: tuple[ColumnRef, ColumnRef]   # (left, right) in textual order


@dataclass(frozen=True)
class Operand:
    kind: str                      # 'column' | 'literal' | 'aggregate'
    ref: Optional[ColumnRef] = None
    literal: Optional[Literal] = None
    func: str = ""
    arg: str = ""


@dataclass(frozen=True)
class Predicate:
    op: str
    left: Operand
    right: Operand
    conj: str = "AND"              # conjunction joining this predicate with the previous


@dataclass
class Select:
    items: list[SelectItem]
    from_table: str
    from_alias: str
    joins: list[Join] = field(default_factory=list)
    where: list[Predicate] = field(default_factory=list)
    group_by: list[ColumnRef] = field(default_factory=list)
    having: list[Predicate] = field(default_factory=list)
    sql: str = ""

    @property
    def is_aggregate(self) -> bool:
        return any(i.kind == "aggregate" for i in self.items)

    def source_tables(self) -> list[str]:
        """Distinct tables referenced by FROM/JOIN, in first-reference order."""
        out = [self.from_table]
        for j in self.joins:
            if j.table not in out:
                out.append(j.table)
        return out

    def resolve_ref(self, ref: ColumnRef, schema: dict[str, list[str]]) -> str:
        """Normalize a possibly-aliased column reference to `table.column`."""
        aliases = self.alias_map()
        if ref.table is not None:
            real = aliases.get(ref.table)
            if real is None:
                raise ParseError(
                    f"unknown table or alias {ref.table!r} in column {ref.sql()}")
            if real in schema and ref.name not in schema[real]:
                raise ParseError(
                    f"table {real!r} has no column {ref.name!r} "
                    f"(available: {', '.join(schema[real]) or 'none'})")
            return f"{real}.{ref.name}"
        candidates = [t for t in self.source_tables() if ref.name in schema.get(t, [])]
        if len(candidates) != 1:
            raise ParseError(
                f"column {ref.name!r} is ambiguous or unknown here: candidates "
                f"{candidates or 'none'} (qualify it as table.column)"
            )
        return f"{candidates[0]}.{ref.name}"

    def output_lineage(self, schema: dict[str, list[str]]) -> dict[str, list[str]]:
        """output column -> list of `real_table.column` it is derived from."""
        lineage: dict[str, list[str]] = {}
        for i in self.items:
            if i.kind == "column":
                if i.ref is None:
                    raise ParseError(f"column {i.name!r} has no resolvable source")
                lineage[i.alias] = [self.resolve_ref(i.ref, schema)]
            elif i.kind == "aggregate":
                lineage[i.alias] = [self.resolve_ref(ColumnRef(None, i.name), schema)]
            else:
                lineage[i.alias] = []
        return lineage

    def output_names(self) -> list[str]:
        out = []
        for i in self.items:
            if i.kind == "star":
                raise ParseError(
                    "SELECT * is not supported: downstream column lineage requires an "
                    "explicit column list. List the columns by name instead."
                )
            out.append(i.alias)
        return out

    def alias_map(self) -> dict[str, str]:
        """SQL alias -> real table name, including each table's own name."""
        out: dict[str, str] = {}
        out[self.from_alias] = self.from_table
        out[self.from_table] = self.from_table
        for j in self.joins:
            out[j.alias] = j.table
            out[j.table] = j.table
        return out


@dataclass
class Statement:
    kind: str                     # 'create' | 'insert'
    target: str
    select: Select
    sql: str = ""


# ---------------------------------------------------------------------------
# tokenizer

_TOKEN_RE = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<comment>--[^\n]*)
  | (?P<string>'(?:[^']|'')*')
  | (?P<number>-?\d+(?:\.\d+)?)
  | (?P<ident>"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)
  | (?P<op><=|>=|<>|!=|=|<|>)
  | (?P<punct>[(),;*.])
  | (?P<other>\S)
    """,
    re.VERBOSE,
)

AGGREGATES = {"COUNT", "SUM", "MIN", "MAX", "AVG"}
RESERVED = {
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "JOIN", "ON", "AS",
    "AND", "OR", "CREATE", "TABLE", "OR", "REPLACE", "INSERT", "INTO",
    "ORDER", "LIMIT", "UNION", "WITH", "DISTINCT", "LEFT", "RIGHT", "INNER",
    "OUTER", "CROSS", "FULL", "WINDOW", "OVER", "PARTITION", "VALUES", "DELETE",
    "UPDATE", "DROP", "ALTER", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST",
    "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "EXISTS", "INTERSECT",
    "EXCEPT", "OFFSET", "ASC", "DESC",
}
_STMT_KEYWORDS = {"CREATE", "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "WITH", "SELECT"}


@dataclass(frozen=True)
class Tok:
    kind: str
    text: str
    pos: int

    @property
    def upper(self) -> str:
        return self.text.upper()

    def is_kw(self, *words: str) -> bool:
        return self.kind == "ident" and self.upper in words


def tokenize(sql: str) -> list[Tok]:
    out: list[Tok] = []
    pos = 0
    n = len(sql)
    while pos < n:
        m = _TOKEN_RE.match(sql, pos)
        if not m:
            raise ParseError(f"unrecognized character at offset {pos}: {sql[pos]!r}")
        kind = m.lastgroup
        text = m.group()
        if kind not in ("ws", "comment"):
            if kind == "other":
                raise ParseError(f"unsupported token {text!r} at offset {pos}")
            out.append(Tok(kind, text, pos))
        pos = m.end()
    return out


def split_statements(sql: str) -> list[str]:
    """Split on semicolons that are not inside string literals or comments."""
    toks = tokenize(sql)
    stmts: list[str] = []
    start: Optional[int] = None
    end = 0
    for t in toks:
        if t.kind == "punct" and t.text == ";":
            if start is not None:
                stmts.append(sql[start:end])
            start = None
        else:
            if start is None:
                start = t.pos
            end = t.pos + len(t.text)
    if start is not None:
        stmts.append(sql[start:end])
    return [s.strip() for s in stmts if s.strip()]


class _Parser:
    def __init__(self, toks: list[Tok]):
        self.toks = toks
        self.i = 0

    def peek(self, *words: str) -> bool:
        return self.i < len(self.toks) and self.toks[self.i].is_kw(*words)

    def peek_kind(self, kind: str, text: Optional[str] = None) -> bool:
        if self.i >= len(self.toks) or self.toks[self.i].kind != kind:
            return False
        return text is None or self.toks[self.i].text == text

    def take(self) -> Tok:
        if self.i >= len(self.toks):
            raise ParseError("unexpected end of statement")
        t = self.toks[self.i]
        self.i += 1
        return t

    def expect_kw(self, word: str) -> Tok:
        if not self.peek(word):
            got = self.toks[self.i].text if self.i < len(self.toks) else "end of statement"
            raise ParseError(f"expected {word}, got {got!r}")
        return self.take()

    def expect_punct(self, ch: str) -> Tok:
        if not self.peek_kind("punct", ch):
            got = self.toks[self.i].text if self.i < len(self.toks) else "end of statement"
            raise ParseError(f"expected {ch!r}, got {got!r}")
        return self.take()

    def expect_ident(self, what: str = "identifier") -> str:
        t = self.take()
        if t.kind != "ident":
            raise ParseError(f"expected {what}, got {t.text!r}")
        name = t.text
        if name.startswith('"') and name.endswith('"'):
            return name[1:-1]
        if t.upper in _STMT_KEYWORDS:
            raise ParseError(f"reserved word {name!r} cannot be used as {what}")
        return name

    # -- names ------------------------------------------------------------
    def parse_name(self) -> tuple[str, str]:
        """Returns (qualifier, bare_name); qualifier == 'main' is treated as bare."""
        first = self.expect_ident("table name")
        if self.peek_kind("punct", "."):
            raise ParseError(
                "qualified table names are not supported in FROM/JOIN positions"
            )
        return "main", first

    # -- select -----------------------------------------------------------
    def parse_select(self) -> Select:
        first = self.toks[self.i]
        self.expect_kw("SELECT")
        if self.peek("DISTINCT"):
            raise ParseError("SELECT DISTINCT is not supported")
        items: list[SelectItem] = []
        while True:
            items.append(self.parse_select_item())
            if self.peek_kind("punct", ","):
                self.take()
                continue
            break
        if not self.peek("FROM"):
            got = self.toks[self.i].text if self.i < len(self.toks) else "end of statement"
            if self.i < len(self.toks) and self.toks[self.i].kind in ("op", "punct"):
                raise ParseError(
                    f"unsupported expression at {got!r}: arithmetic, CASE, CAST and "
                    "scalar functions are not supported; select plain columns or "
                    f"supported aggregates ({', '.join(sorted(AGGREGATES))})"
                )
            raise ParseError(
                f"expected FROM, got {got!r}; SELECT without FROM and subqueries "
                "are not supported"
            )
        self.take()
        from_table = self.expect_ident("table name")
        from_alias = from_table
        if self.peek("AS"):
            self.take()
            from_alias = self.expect_ident("table alias")
        elif (self.i < len(self.toks) and self.toks[self.i].kind == "ident"
              and not self.peek("WHERE", "GROUP", "HAVING", "JOIN", "ON", "ORDER",
                                "LIMIT", "UNION", "LEFT", "RIGHT", "INNER", "OUTER",
                                "CROSS", "FULL", "WHERE", "INTERSECT", "EXCEPT")):
            from_alias = self.take().text

        joins: list[Join] = []
        while True:
            if self.peek("LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS"):
                raise ParseError(
                    f"{self.toks[self.i].upper} JOIN is not supported; only plain "
                    "JOIN ... ON col = col is supported"
                )
            if self.peek("JOIN"):
                self.take()
                jt = self.expect_ident("joined table name")
                ja = jt
                if self.peek("AS"):
                    self.take()
                    ja = self.expect_ident("table alias")
                elif (self.i < len(self.toks) and self.toks[self.i].kind == "ident"
                      and not self.peek("ON", "WHERE", "GROUP", "HAVING", "JOIN",
                                        "ORDER", "LIMIT", "UNION")):
                    ja = self.take().text
                self.expect_kw("ON")
                left = self.parse_column()
                if not self.peek_kind("op", "="):
                    raise ParseError(
                        "JOIN condition must be an equality between two columns "
                        "(col = col); other operators and expressions are not supported"
                    )
                self.take()
                right = self.parse_column()
                joins.append(Join(jt, ja, (left, right)))
                continue
            break

        where = []
        if self.peek("WHERE"):
            self.take()
            where = self.parse_predicates(stop={"GROUP", "HAVING", "ORDER", "LIMIT",
                                                "UNION", "INTERSECT", "EXCEPT"})

        group_by: list[ColumnRef] = []
        if self.peek("GROUP"):
            self.take()
            self.expect_kw("BY")
            while True:
                group_by.append(self.parse_column())
                if self.peek_kind("punct", ","):
                    self.take()
                    continue
                break
            if self.peek("GROUP", "ROLLUP", "CUBE", "GROUPING", "SETS"):
                raise ParseError("GROUPING SETS / ROLLUP / CUBE are not supported")

        having = []
        if self.peek("HAVING"):
            if not group_by:
                raise ParseError(
                    "HAVING requires GROUP BY; filter individual rows with WHERE")
            self.take()
            having = self.parse_predicates(stop={"ORDER", "LIMIT", "UNION",
                                                  "INTERSECT", "EXCEPT"})
        if self.peek("ORDER", "LIMIT", "OFFSET"):
            raise ParseError(
                f"{self.toks[self.i].upper} is not supported; materialized tables "
                "have no guaranteed row order and no pagination"
            )
        if self.peek("UNION", "INTERSECT", "EXCEPT"):
            raise ParseError(f"{self.toks[self.i].upper} is not supported")
        if self.i != len(self.toks):
            got = self.toks[self.i].text
            raise ParseError(
                f"unexpected {got!r}: expressions (arithmetic, functions other "
                f"than {', '.join(sorted(AGGREGATES))}, CASE, casts) are not "
                "supported; select plain columns or supported aggregates"
            )
        end = self.toks[-1].pos + len(self.toks[-1].text)
        return Select(items, from_table, from_alias, joins, where, group_by, having,
                      sql=self_text(self.toks, first.pos, end))

    def parse_select_item(self) -> SelectItem:
        if self.peek_kind("punct", "*"):
            self.take()
            raise ParseError(
                "SELECT * is not supported: downstream column lineage requires an "
                "explicit column list. List the columns by name instead."
            )
        if self.i < len(self.toks) and self.toks[self.i].kind == "ident" \
                and self.toks[self.i].upper in AGGREGATES:
            if not (self.i + 1 < len(self.toks) and self.toks[self.i + 1].kind == "punct"
                    and self.toks[self.i + 1].text == "("):
                raise ParseError(
                    f"{self.toks[self.i].text} must be used as an aggregate function call"
                )
            func = self.take().upper
            self.take()
            arg = self.expect_ident("column name")
            self.expect_punct(")")
            alias = f"{func.lower()}_{arg}"
            if self.peek("AS"):
                self.take()
                alias = self.expect_ident("column alias")
            elif (self.i < len(self.toks) and self.toks[self.i].kind == "ident"
                  and not self.peek("FROM")):
                alias = self.take().text
            return SelectItem("aggregate", name=arg, alias=alias, func=func)
        if self.peek_kind("string") or self.peek_kind("number"):
            raise ParseError(
                "literal SELECT items are not supported; select a column or aggregate"
            )
        if self.i < len(self.toks) and self.toks[self.i].kind == "ident" \
                and not self.toks[self.i].is_kw("CASE", "CAST", "NULL"):
            col = self.parse_column_spec()
            if isinstance(col, Literal):
                raise ParseError("literal SELECT items are not supported")
            alias = col.name
            if self.peek("AS"):
                self.take()
                alias = self.expect_ident("column alias")
            elif (self.i < len(self.toks) and self.toks[self.i].kind == "ident"
                  and not self.peek("FROM")):
                alias = self.take().text
            return SelectItem("column", name=col.name, alias=alias, ref=col)
        raise ParseError(
            f"unsupported SELECT item starting at "
            f"{self.toks[self.i].text if self.i < len(self.toks) else 'end of statement'!r}"
        )

    def parse_column_spec(self) -> ColumnRef | Literal:
        if self.peek_kind("string") or self.peek_kind("number"):
            t = self.take()
            if t.kind == "string":
                return Literal(t.text[1:-1].replace("''", "'"), t.text)
            return Literal(float(t.text) if "." in t.text else int(t.text), t.text)
        if self.peek("NULL"):
            raise ParseError("NULL literals are not supported")
        return self.parse_column()

    def parse_column(self, allow_alias: bool = True) -> ColumnRef:
        # "table.column" arrives as ident, '.', ident
        first = self.expect_ident("column name")
        if self.peek_kind("punct", "."):
            if not allow_alias:
                raise ParseError("qualified columns are not allowed in this position")
            self.take()
            second = self.expect_ident("column name")
            return ColumnRef(first, second)
        return ColumnRef(None, first)

    # -- predicates -------------------------------------------------------
    def parse_predicates(self, stop: set[str]) -> list[Predicate]:
        preds: list[Predicate] = []
        conj = "AND"
        while True:
            if self.peek(*stop) or self.i >= len(self.toks):
                break
            left = self.parse_operand()
            if not self.peek_kind("op"):
                raise ParseError(
                    f"expected comparison operator, got "
                    f"{self.toks[self.i].text if self.i < len(self.toks) else 'end'!r}"
                )
            op = self.take().text
            right = self.parse_operand()
            preds.append(Predicate(op, left, right, conj))
            if self.peek("AND"):
                self.take()
                conj = "AND"
            elif self.peek("OR"):
                self.take()
                conj = "OR"
            else:
                break
        if not preds:
            raise ParseError("WHERE/HAVING requires at least one predicate")
        return preds

    def parse_operand(self) -> Operand:
        if self.i < len(self.toks) and self.toks[self.i].kind == "ident" \
                and self.toks[self.i].upper in AGGREGATES:
            func = self.take().upper
            self.expect_punct("(")
            arg = self.expect_ident("column name")
            self.expect_punct(")")
            return Operand("aggregate", func=func, arg=arg)
        if self.peek_kind("string") or self.peek_kind("number"):
            lit = self.parse_column_spec()
            assert isinstance(lit, Literal)
            return Operand("literal", literal=lit)
        if self.peek("NULL"):
            raise ParseError("NULL literals are not supported")
        col = self.parse_column()
        if self.i < len(self.toks) and self.toks[self.i].kind == "punct" \
                and self.toks[self.i].text == "(":
            raise ParseError(
                f"function {col.name!r} is not supported; supported aggregates are "
                f"{', '.join(sorted(AGGREGATES))}"
            )
        return Operand("column", ref=col)


def self_text(toks: list[Tok], start: int, end: int) -> str:
    """Reconstruct a normalized single-space rendering of tokens [start,end)."""
    return " ".join(t.text for t in toks if start <= t.pos < end)


def parse_statement(sql: str) -> Statement:
    toks = tokenize(sql)
    whole = " ".join(t.text for t in toks)
    p = _Parser(toks)
    if p.peek("CREATE"):
        p.take()
        if p.peek("OR"):
            p.take()
            if not p.peek("REPLACE"):
                raise ParseError("expected REPLACE after CREATE OR")
            p.take()
        p.expect_kw("TABLE")
        target = p.expect_ident("target table name")
        if p.peek_kind("punct", "("):
            raise ParseError(
                "CREATE TABLE ... (columns) is not supported; use "
                "CREATE TABLE name AS SELECT ..."
            )
        p.expect_kw("AS")
        sel = p.parse_select()
        return Statement("create", target, sel, whole)
    if p.peek("INSERT"):
        p.take()
        p.expect_kw("INTO")
        target = p.expect_ident("target table name")
        if p.peek("VALUES"):
            raise ParseError(
                "INSERT ... VALUES is not supported; use INSERT INTO name SELECT ..."
            )
        sel = p.parse_select()
        return Statement("insert", target, sel, whole)
    if p.peek("SELECT"):
        raise ParseError(
            "standalone SELECT is not supported; use CREATE TABLE ... AS SELECT"
        )
    if p.i < len(p.toks):
        w = p.toks[p.i].upper
        if w == "UPDATE":
            raise ParseError("UPDATE is not supported; tables are immutable versions")
        if w == "DELETE":
            raise ParseError("DELETE is not supported; create a new table version instead")
        if w == "DROP":
            raise ParseError("DROP is not supported; supersede the table with a new model")
        if w == "ALTER":
            raise ParseError("ALTER TABLE is not supported; create a new table version instead")
        if w == "WITH":
            raise ParseError("common table expressions (WITH) are not supported")
        raise ParseError(f"unsupported statement starting with {w}")
    raise ParseError("empty statement")


def parse_script(sql: str) -> list[Statement]:
    """Parse a model body.

    A model is exactly one statement: publishing several statements as one
    "table" would make the effect of an edit ambiguous and make lineage
    incomplete, so extra statements are rejected explicitly.  Use
    :func:`split_statements` when the caller really wants a multi-statement
    script (the parser itself still understands the individual statements).
    """
    stmts = [parse_statement(s) for s in split_statements(sql)]
    if len(stmts) > 1:
        raise ParseError(
            "a model body must contain exactly one statement; split extra "
            "steps into separate tables"
        )
    return stmts



