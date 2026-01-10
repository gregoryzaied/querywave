from __future__ import annotations

import re
from typing import Dict, List, Any, Optional


# -------------------------
# Regex patterns (MVP-safe)
# -------------------------

# FROM/JOIN table alias detection:
#   FROM employees e
#   FROM employees AS e
#   JOIN branches b
#   JOIN branches AS b
FROM_JOIN_RE = re.compile(
    r"\b(from|join)\s+([A-Za-z_][A-Za-z0-9_]*)\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*)\b",
    re.IGNORECASE,
)

# Qualified column reference: e.emp_id
QUAL_COL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")

# Phase 3C: alias.col OP literal  (simple literal support: 'text', 123, 12.3, null, true/false)
QUAL_COMP_LIT_RE = re.compile(
    r"\b(?P<a>[A-Za-z_][A-Za-z0-9_]*)\.(?P<c>[A-Za-z_][A-Za-z0-9_]*)\s*"
    r"(?P<op>>=|<=|<>|!=|=|>|<)\s*"
    r"(?P<lit>'[^']*'|\d+(?:\.\d+)?|null|true|false)\b",
    re.IGNORECASE,
)

# Phase 3C: alias.col LIKE '...'
QUAL_LIKE_RE = re.compile(
    r"\b(?P<a>[A-Za-z_][A-Za-z0-9_]*)\.(?P<c>[A-Za-z_][A-Za-z0-9_]*)\s+"
    r"(?P<op>like|ilike)\s+(?P<lit>'[^']*')",
    re.IGNORECASE,
)

# JOIN ON equality: ON e.branch_id = b.branch_id
ON_EQ_RE = re.compile(
    r"\bon\b\s+(?P<a1>[A-Za-z_][A-Za-z0-9_]*)\.(?P<c1>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
    r"(?P<a2>[A-Za-z_][A-Za-z0-9_]*)\.(?P<c2>[A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)

# Unqualified identifier tokens
IDENT_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")

SQL_KEYWORDS = {
    "select", "from", "join", "inner", "left", "right", "full", "outer", "cross",
    "on", "where", "group", "by", "order", "having", "limit", "offset", "fetch",
    "union", "all", "distinct", "as", "and", "or", "not", "null", "is", "in", "like",
    "ilike", "between", "case", "when", "then", "else", "end", "asc", "desc", "into",
    "true", "false"
}

SQL_FUNCTIONS = {
    "count", "sum", "avg", "min", "max", "coalesce", "now", "date_trunc", "lower", "upper"
}


# -------------------------
# Helpers
# -------------------------

def _lower(s: str) -> str:
    return (s or "").lower()


def _strip_strings(sql: str) -> str:
    """
    Remove single-quoted strings to reduce false identifier matches.
    (Not a full SQL lexer, but helps.)
    """
    return re.sub(r"'.*?(?<!\\)'", "''", sql, flags=re.S)


def build_alias_map(sql: str) -> Dict[str, str]:
    """
    Returns alias->table mapping (all lowercased)
    """
    alias_map: Dict[str, str] = {}
    for m in FROM_JOIN_RE.finditer(sql):
        table = _lower(m.group(2))
        alias = _lower(m.group(3))
        alias_map[alias] = table
    return alias_map


def normalize_type(db_type: str) -> str:
    """
    Map SQL types into coarse groups: numeric, text, boolean, datetime, other
    """
    t = (db_type or "").lower()

    if any(x in t for x in ["int", "serial", "numeric", "decimal", "real", "double", "float"]):
        return "numeric"

    if any(x in t for x in ["text", "varchar", "char", "uuid"]):
        return "text"

    if "bool" in t:
        return "boolean"

    if any(x in t for x in ["date", "time", "timestamp", "timestamptz"]):
        return "datetime"

    return "other"


def literal_type(lit: str) -> str:
    lit_l = (lit or "").lower()
    if lit_l == "null":
        return "null"
    if lit_l in ("true", "false"):
        return "boolean"
    if lit.startswith("'") and lit.endswith("'"):
        return "text"
    return "numeric"


# -------------------------
# Phase 3B: FK-aware JOIN validation
# -------------------------

def fk_join_check(sql: str, schema_json: dict, alias_map: Dict[str, str]) -> dict:
    """
    Validates JOIN ... ON a.col = b.col against schema_json foreign_keys.
    Returns:
      invalid_joins: list of join issues
      join_warnings: list of non-fatal warnings
    """
    tables = schema_json.get("tables", {}) or {}

    # FK edges: (from_table, from_col, to_table, to_col) all lower
    fk_edges = set()
    for t, meta in tables.items():
        t_l = _lower(t)
        for fk in (meta.get("foreign_keys") or []):
            fk_edges.add((t_l, _lower(fk.get("column")), _lower(fk.get("ref_table")), _lower(fk.get("ref_column"))))

    # PK map for optional warnings (pk=pk same name join)
    pk_map = { _lower(t): set(_lower(x) for x in (meta.get("primary_key") or [])) for t, meta in tables.items() }

    invalid: List[dict] = []
    warnings: List[str] = []

    for m in ON_EQ_RE.finditer(sql):
        a1 = _lower(m.group("a1"))
        c1 = _lower(m.group("c1"))
        a2 = _lower(m.group("a2"))
        c2 = _lower(m.group("c2"))

        if a1 not in alias_map or a2 not in alias_map:
            warnings.append(f"JOIN uses unknown alias in ON: {a1}.{c1} = {a2}.{c2}")
            continue

        t1 = _lower(alias_map[a1])
        t2 = _lower(alias_map[a2])

        direct_ok = (t1, c1, t2, c2) in fk_edges
        reverse_ok = (t2, c2, t1, c1) in fk_edges

        if direct_ok or reverse_ok:
            continue

        # PK=PK same-name join can be intentional but suspicious
        if (c1 in pk_map.get(t1, set())) and (c2 in pk_map.get(t2, set())) and (c1 == c2):
            warnings.append(
                f"JOIN {t1}.{c1} = {t2}.{c2} is PK=PK with no FK declared; confirm intended relationship."
            )
            continue

        invalid.append({
            "left": f"{t1}.{c1}",
            "right": f"{t2}.{c2}",
            "reason": "No FK relationship found for this join condition"
        })

    return {"invalid_joins": invalid, "join_warnings": warnings}


# -------------------------
# Phase 3: validate SQL against schema (3B + 3C)
# -------------------------

def validate_against_schema(sql: str, schema_json: dict) -> dict:
    sql_raw = sql or ""
    sql0 = _strip_strings(sql_raw)
    sql_l = sql0.lower()

    tables: Dict[str, Any] = (schema_json.get("tables", {}) or {})

    # Alias map
    alias_map = build_alias_map(sql0)

    # Detected tables from alias map values (best effort)
    tables_detected = sorted(set(alias_map.values()))
    missing_tables = [t for t in tables_detected if t not in { _lower(x) for x in tables.keys() }]

    # Build schema column sets + type maps
    cols_by_table = {
        _lower(t): set(_lower(c) for c in (meta.get("columns", {}) or {}).keys())
        for t, meta in tables.items()
    }
    type_by_table = {
        _lower(t): { _lower(c): normalize_type(tp) for c, tp in (meta.get("columns", {}) or {}).items() }
        for t, meta in tables.items()
    }

    # Qualified checks
    unknown_aliases: List[str] = []
    unknown_columns: List[dict] = []
    unknown_identifiers: List[str] = []

    qualified_refs = QUAL_COL_RE.findall(sql0)
    for a, c in qualified_refs:
        a_l = _lower(a)
        c_l = _lower(c)

        if a_l not in alias_map:
            # could be schema.table style or db.table; treat as unknown alias if not a known table
            if a_l not in cols_by_table and a_l not in unknown_aliases:
                unknown_aliases.append(a_l)
            continue

        t = alias_map[a_l]  # already lower
        if c_l not in cols_by_table.get(t, set()):
            unknown_columns.append({"table": t, "alias": a_l, "column": c_l})
            if c_l not in unknown_identifiers:
                unknown_identifiers.append(c_l)

    # Unqualified column resolution
    # Candidate tokens = identifiers that are not keywords, functions, aliases, or table names
    tokens = IDENT_RE.findall(sql0)
    candidates: List[str] = []
    known_tables = set(cols_by_table.keys())
    known_aliases = set(alias_map.keys())

    for tok in tokens:
        tok_l = _lower(tok)
        if tok_l in SQL_KEYWORDS or tok_l in SQL_FUNCTIONS:
            continue
        if tok_l in known_aliases:
            continue
        if tok_l in known_tables:
            continue
        # skip if it's a number-like token (IDENT_RE won't match digits-only, but keep safe)
        candidates.append(tok_l)

    detected_tables_cols = [(t, cols_by_table.get(t, set())) for t in tables_detected if t in cols_by_table]

    unqualified_columns_resolved: List[dict] = []
    ambiguous_unqualified_columns: List[dict] = []
    seen_unqualified = set()

    for tok_l in candidates:
        if tok_l in seen_unqualified:
            continue
        seen_unqualified.add(tok_l)

        # If used as part of alias.col already, skip
        if re.search(rf"\b[a-zA-Z_]\w*\.{re.escape(tok_l)}\b", sql_l):
            continue

        owners = [t for (t, cols) in detected_tables_cols if tok_l in cols]
        if len(owners) == 1:
            unqualified_columns_resolved.append({"column": tok_l, "table": owners[0]})
        elif len(owners) > 1:
            ambiguous_unqualified_columns.append({"column": tok_l, "tables": owners})

    # Unknown identifiers (best effort):
    # anything that is not a keyword/function/alias/table and not resolved as a column
    resolved_cols = {x["column"] for x in unqualified_columns_resolved}
    ambiguous_cols = {x["column"] for x in ambiguous_unqualified_columns}
    for tok_l in seen_unqualified:
        if tok_l in resolved_cols or tok_l in ambiguous_cols:
            continue
        # keep it light: don't add too much noise
        if tok_l not in unknown_identifiers:
            unknown_identifiers.append(tok_l)

    # ---------------------------
    # Phase 3C: Type-aware checks
    # ---------------------------
    type_mismatches: List[dict] = []

    def col_type(alias: str, col: str) -> Optional[str]:
        a = _lower(alias)
        c = _lower(col)
        if a not in alias_map:
            return None
        t = alias_map[a]
        return type_by_table.get(t, {}).get(c)

    # 1) Comparisons vs literals
    for m in QUAL_COMP_LIT_RE.finditer(sql0):
        a = m.group("a")
        c = m.group("c")
        op = m.group("op").lower()
        lit = m.group("lit")

        ct = col_type(a, c)
        if ct is None:
            continue

        lt = literal_type(lit)

        if lt == "null":
            continue

        if op in (">", "<", ">=", "<="):
            if ct != "numeric":
                type_mismatches.append({
                    "kind": "comparison",
                    "expr": f"{a}.{c} {op} {lit}",
                    "column_type": ct,
                    "literal_type": lt,
                    "reason": "Non-numeric column used with numeric comparison operator"
                })
            continue

        if op in ("=", "!=", "<>"):
            if ct in ("numeric", "text", "boolean") and lt in ("numeric", "text", "boolean"):
                if ct != lt:
                    type_mismatches.append({
                        "kind": "equality",
                        "expr": f"{a}.{c} {op} {lit}",
                        "column_type": ct,
                        "literal_type": lt,
                        "reason": "Column type does not match literal type"
                    })

    # 2) LIKE / ILIKE must be text
    for m in QUAL_LIKE_RE.finditer(sql0):
        a = m.group("a")
        c = m.group("c")
        op = m.group("op").lower()
        lit = m.group("lit")

        ct = col_type(a, c)
        if ct is None:
            continue

        if ct != "text":
            type_mismatches.append({
                "kind": "like",
                "expr": f"{a}.{c} {op} {lit}",
                "column_type": ct,
                "literal_type": "text",
                "reason": "LIKE/ILIKE used on non-text column"
            })

    # 3) Join type mismatch: ON a.col = b.col
    for m in ON_EQ_RE.finditer(sql0):
        a1 = m.group("a1")
        c1 = m.group("c1")
        a2 = m.group("a2")
        c2 = m.group("c2")

        t1 = col_type(a1, c1)
        t2 = col_type(a2, c2)

        if t1 is None or t2 is None:
            continue

        if t1 != t2 and t1 != "other" and t2 != "other":
            type_mismatches.append({
                "kind": "join",
                "expr": f"{a1}.{c1} = {a2}.{c2}",
                "left_type": t1,
                "right_type": t2,
                "reason": "Join compares different column types"
            })

    # Phase 3B: FK-aware join check
    fk = fk_join_check(sql0, schema_json, alias_map)

    # Notes
    notes: List[str] = []
    if missing_tables:
        notes.append("One or more referenced tables are missing from the schema.")
    if unknown_columns:
        notes.append("One or more qualified columns (alias.column) do not exist in the referenced table.")
    if unknown_aliases:
        notes.append("One or more aliases referenced in SQL were not defined in FROM/JOIN.")
    if ambiguous_unqualified_columns:
        notes.append("Some unqualified column names are ambiguous; prefer alias.column.")
    if fk["invalid_joins"]:
        notes.append("One or more JOIN conditions do not match any FK relationship.")
    if type_mismatches:
        notes.append("One or more comparisons/joins appear to use incompatible data types.")

    return {
        "tables_detected": tables_detected,
        "missing_tables": missing_tables,
        "alias_map": alias_map,
        "unknown_aliases": unknown_aliases,
        "unknown_columns": unknown_columns,
        "unqualified_columns_resolved": unqualified_columns_resolved,
        "ambiguous_unqualified_columns": ambiguous_unqualified_columns,
        "unknown_identifiers": unknown_identifiers,
        "invalid_joins": fk["invalid_joins"],
        "join_warnings": fk["join_warnings"],
        "type_mismatches": type_mismatches[:50],
        "notes": notes,
    }


# -------------------------
# Phase 3D: Clear error classification
# -------------------------

def classify_issue(validation: dict, question: str, schema_json: dict) -> dict:
    """
    class: 'schema_issue' | 'ai_issue' | 'user_issue' | 'ok'
    action: 'stop' | 'retry_ai' | 'ask_user'
    """

    # If nothing major is wrong
    hard_signals = (
        bool(validation.get("missing_tables")) or
        bool(validation.get("unknown_columns")) or
        bool(validation.get("unknown_aliases")) or
        bool(validation.get("invalid_joins")) or
        bool(validation.get("type_mismatches"))
    )
    if not hard_signals:
        return {"class": "ok", "reason": "No major validation issues detected.", "action": "stop"}

    q = (question or "").lower()

    # Missing tables -> schema issue (or AI hallucination). If user asked for them explicitly, it's schema; otherwise AI.
    if validation.get("missing_tables"):
        return {
            "class": "schema_issue",
            "reason": "SQL references table(s) not present in the uploaded schema.",
            "action": "stop"
        }

    # Unknown columns: decide schema vs AI (simple heuristic)
    if validation.get("unknown_columns"):
        unknown_cols = {c.get("column", "") for c in validation.get("unknown_columns", []) if isinstance(c, dict)}
        unknown_cols = {str(x).lower() for x in unknown_cols if x}
        if any(uc in q for uc in unknown_cols):
            return {
                "class": "schema_issue",
                "reason": "Requested column(s) appear not to exist in the schema.",
                "action": "stop"
            }
        return {
            "class": "ai_issue",
            "reason": "AI produced column names that do not exist in the schema.",
            "action": "retry_ai"
        }

    # Invalid joins: usually AI issue if schema has FKs
    if validation.get("invalid_joins"):
        return {
            "class": "ai_issue",
            "reason": "SQL join condition does not match any foreign key relationship.",
            "action": "retry_ai"
        }

    # Type mismatches: usually AI issue
    if validation.get("type_mismatches"):
        return {
            "class": "ai_issue",
            "reason": "SQL compares incompatible data types (e.g., TEXT vs numeric).",
            "action": "retry_ai"
        }

    # Unknown aliases: likely AI issue
    if validation.get("unknown_aliases"):
        return {
            "class": "ai_issue",
            "reason": "SQL references aliases that were never defined in FROM/JOIN.",
            "action": "retry_ai"
        }

    # Ambiguity / user intent issues (best effort)
    if validation.get("ambiguous_unqualified_columns"):
        return {
            "class": "user_issue",
            "reason": "Request leads to ambiguous columns; specify which table/field you mean.",
            "action": "ask_user"
        }

    return {"class": "ai_issue", "reason": "Validation failed in an unexpected way.", "action": "retry_ai"}
