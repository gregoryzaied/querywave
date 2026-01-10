# schema_parser.py
import re
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class ForeignKey:
    column: str
    ref_table: str
    ref_column: str


@dataclass
class TableMeta:
    name: str
    columns: Dict[str, str]
    primary_key: List[str]
    foreign_keys: List[ForeignKey]


CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>(?:\"[^\"]+\"|\w+)(?:\.(?:\"[^\"]+\"|\w+))?)\s*\((?P<body>.*?)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)

COLUMN_DEF_RE = re.compile(
    r"^\s*(?P<col>\"[^\"]+\"|\w+)\s+(?P<type>[a-zA-Z][\w\s\(\),]*)",
    re.IGNORECASE,
)

PK_RE = re.compile(r"PRIMARY\s+KEY\s*\((?P<cols>[^)]+)\)", re.IGNORECASE)

FK_RE = re.compile(
    r"FOREIGN\s+KEY\s*\((?P<col>[^)]+)\)\s+REFERENCES\s+(?P<ref_table>(?:\"[^\"]+\"|\w+)(?:\.(?:\"[^\"]+\"|\w+))?)\s*\((?P<ref_col>[^)]+)\)",
    re.IGNORECASE,
)


def _clean_ident(s: str) -> str:
    s = s.strip()
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s


def _split_top_level_commas(body: str) -> List[str]:
    parts, buf = [], []
    depth = 0
    in_str = False

    for i, ch in enumerate(body):
        if ch == "'" and (i == 0 or body[i - 1] != "\\"):
            in_str = not in_str
            buf.append(ch)
        elif not in_str:
            if ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth = max(0, depth - 1)
                buf.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        else:
            buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)

    return parts


def parse_schema_sql(schema_sql: str) -> Dict[str, TableMeta]:
    tables: Dict[str, TableMeta] = {}

    for m in CREATE_TABLE_RE.finditer(schema_sql):
        raw_name = m.group("name")
        body = m.group("body")

        name = _clean_ident(raw_name.split(".")[-1])

        columns = {}
        primary_key = []
        foreign_keys = []

        items = _split_top_level_commas(body)

        for item in items:
            item_stripped = item.strip()

            pk_match = PK_RE.search(item_stripped)
            if pk_match:
                cols = [_clean_ident(c) for c in pk_match.group("cols").split(",")]
                primary_key.extend([c for c in cols if c])
                continue

            fk_match = FK_RE.search(item_stripped)
            if fk_match:
                col = _clean_ident(fk_match.group("col").split(",")[0])
                ref_table = _clean_ident(fk_match.group("ref_table").split(".")[-1])
                ref_col = _clean_ident(fk_match.group("ref_col").split(",")[0])
                foreign_keys.append(ForeignKey(col, ref_table, ref_col))
                continue

            col_match = COLUMN_DEF_RE.match(item_stripped)
            if col_match:
                col = _clean_ident(col_match.group("col"))
                col_type = col_match.group("type").strip()

                col_type = re.split(
                    r"\s+(?:NOT\s+NULL|NULL|DEFAULT|PRIMARY\s+KEY|UNIQUE|REFERENCES)\b",
                    col_type,
                    flags=re.IGNORECASE,
                )[0].strip()

                columns[col] = col_type

                if re.search(r"\bPRIMARY\s+KEY\b", item_stripped, re.IGNORECASE):
                    primary_key.append(col)

                inline_ref = re.search(
                    r"\bREFERENCES\s+(?P<ref_table>(?:\"[^\"]+\"|\w+)(?:\.(?:\"[^\"]+\"|\w+))?)\s*\((?P<ref_col>[^)]+)\)",
                    item_stripped,
                    re.IGNORECASE,
                )
                if inline_ref:
                    ref_table = _clean_ident(inline_ref.group("ref_table").split(".")[-1])
                    ref_col = _clean_ident(inline_ref.group("ref_col").split(",")[0])
                    foreign_keys.append(ForeignKey(col, ref_table, ref_col))

        tables[name] = TableMeta(
            name=name,
            columns=columns,
            primary_key=list(dict.fromkeys(primary_key)),
            foreign_keys=foreign_keys,
        )

    return tables


def to_schema_json(tables: Dict[str, TableMeta]) -> dict:
    return {
        "tables": {
            tname: {
                "columns": tmeta.columns,
                "primary_key": tmeta.primary_key,
                "foreign_keys": [
                    {
                        "column": fk.column,
                        "ref_table": fk.ref_table,
                        "ref_column": fk.ref_column,
                    }
                    for fk in tmeta.foreign_keys
                ],
            }
            for tname, tmeta in tables.items()
        }
    }