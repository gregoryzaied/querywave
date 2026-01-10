# app.py
import json
import os
import time
import uuid
from pathlib import Path
import re

import secrets
from datetime import date

from fastapi import HTTPException, Request, Response
from starlette import status
from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
from openai import OpenAI, RateLimitError

from schema_parser import parse_schema_sql, to_schema_json
from validator import validate_against_schema, classify_issue

from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles


# ---------------------------------------------------------
# OpenAI client + deterministic retry wrapper
# ---------------------------------------------------------
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def call_openai_with_retry(fn, retries: int = 3, base_sleep: float = 1.0):
    last_err = None
    for i in range(retries):
        try:
            return fn()
        except RateLimitError as e:
            last_err = e
            time.sleep(base_sleep * (2 ** i))
    raise last_err


# ---------------------------------------------------------
# FastAPI + schema storage
# ---------------------------------------------------------
app = FastAPI(title="QueryWave MVP")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
SCHEMAS: dict[str, dict] = {}
SCHEMA_DIR = Path("schemas")
SCHEMA_DIR.mkdir(exist_ok=True)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, response: Response):
    cid = get_or_set_client_id(request, response)
    bucket = get_usage_bucket(cid)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "schemas_remaining": MAX_SCHEMAS_PER_DAY - bucket["schemas"],
            "generates_remaining": MAX_GENERATES_PER_DAY - bucket["generates"],
            "max_question_chars": MAX_QUESTION_CHARS,
        },
    )


# -------- Phase 5 stability limits --------
MAX_UPLOAD_MB = 1
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

MAX_TABLES = 200
MAX_TOTAL_COLUMNS = 5000
MAX_COLUMNS_PER_TABLE = 300

# -------- Phase 6 productization limits --------
MAX_QUESTION_CHARS = 500
MAX_SCHEMAS_PER_DAY = 5
MAX_GENERATES_PER_DAY = 50

# client tracking (no login needed)
CLIENT_COOKIE = "qw_client_id"
USAGE: dict[str, dict[str, dict[str, int]]] = {}  # {"YYYY-MM-DD": {cid: {"schemas":int,"generates":int}}}


def today_key() -> str:
    return date.today().isoformat()

def get_or_set_client_id(request: Request, response: Response) -> str:
    cid = request.cookies.get(CLIENT_COOKIE)
    if not cid:
        cid = secrets.token_urlsafe(16)
        # dev-safe cookie. (On deployment you may set secure=True + domain.)
        response.set_cookie(CLIENT_COOKIE, cid, httponly=True, samesite="lax")
    return cid

def get_usage_bucket(cid: str) -> dict:
    tk = today_key()
    day = USAGE.setdefault(tk, {})
    return day.setdefault(cid, {"schemas": 0, "generates": 0})

def require_limit(bucket: dict, field: str, limit: int, label: str):
    if bucket.get(field, 0) >= limit:
        raise HTTPException(
            status_code=429,
            detail=f"Usage limit reached: {label} ({limit}/day)."
        )


# ---------------------------------------------------------
# Schema summary for AI (PK / FK / NOT NULL / types)
# ---------------------------------------------------------
def schema_summary(schema_json: dict) -> str:
    tables = (schema_json or {}).get("tables", {}) or {}
    out: list[str] = []

    for table_name, meta in tables.items():
        meta = meta or {}
        cols = meta.get("columns", {}) or {}

        pk = set(meta.get("primary_key") or [])
        nn = set(meta.get("not_null") or [])
        fks = meta.get("foreign_keys") or []

        col_bits = []
        for col_name, col_meta in cols.items():
            if isinstance(col_meta, dict):
                col_type = col_meta.get("type") or col_meta.get("data_type") or "UNKNOWN"
            else:
                col_type = str(col_meta)

            flags = []
            if col_name in pk:
                flags.append("PK")
            if col_name in nn:
                flags.append("NN")

            flag_txt = f" [{' '.join(flags)}]" if flags else ""
            col_bits.append(f"{col_name}:{col_type}{flag_txt}")

        col_bits = col_bits[:40]

        out.append(f"TABLE {table_name}")
        out.append("  COLUMNS: " + ", ".join(col_bits) if col_bits else "  COLUMNS: (none)")

        if fks:
            fk_bits = []
            for fk in fks[:30]:
                c = fk.get("column") or fk.get("from_column")
                rt = fk.get("ref_table") or fk.get("to_table")
                rc = fk.get("ref_column") or fk.get("to_column")
                if c and rt and rc:
                    fk_bits.append(f"{table_name}.{c} -> {rt}.{rc}")
            if fk_bits:
                out.append("  FKS: " + "; ".join(fk_bits))

        out.append("")

    return "\n".join(out).strip()


# ---------------------------------------------------------
# Extract pure SQL from model output (SQL-only gate)
# ---------------------------------------------------------
def extract_sql(text: str) -> str:
    if not text:
        return ""

    t = text.strip()
    t = re.sub(r"^```(?:sql)?\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*```$", "", t)
    t = re.sub(r"^\s*sql\s*:\s*", "", t, flags=re.IGNORECASE)

    parts = [p.strip() for p in t.split(";") if p.strip()]
    if not parts:
        return ""

    sql = parts[0] + ";"

    # must start with SELECT or WITH
    if not re.match(r"^(select|with)\b", sql.strip(), re.IGNORECASE):
        return ""

    return sql


# ---------------------------------------------------------
# Product boundary enforcement: reject disallowed SQL
# ---------------------------------------------------------
def reject_disallowed_sql(sql: str):
    s = (sql or "").strip().lower()
    disallowed = ("create ", "drop ", "alter ", "truncate ", "grant ", "revoke ")
    if s.startswith(disallowed):
        raise ValueError("DDL / privileged statements are not supported.")
    # also block multiple statements defensively (extract_sql already takes first)
    if ";" in (sql or "").strip()[:-1]:
        raise ValueError("Multi-statement SQL is not supported.")


# ---------------------------------------------------------
# Phase 4A–C: primary AI SQL generator
# ---------------------------------------------------------
def ai_generate_sql(schema_json: dict, question: str) -> str:
    schema_text = schema_summary(schema_json)

    system = (
        "You are a highly accurate senior PostgreSQL engineer.\n"
        "Rules:\n"
        "- NEVER invent tables or columns.\n"
        "- ALWAYS use alias.column in joins.\n"
        "- Use explicit JOIN ... ON ... with foreign keys.\n"
        "- If ambiguous, make the smallest assumption and add a short SQL comment.\n"
        "- Output ONLY SQL. No markdown.\n"
    )

    user = (
        f"SCHEMA:\n{schema_text}\n\n"
        f"REQUEST:\n{question}\n\n"
        "Return one valid PostgreSQL query. SQL only."
    )

    resp = call_openai_with_retry(lambda: client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.0,
        max_tokens=300,
    ))

    raw = resp.choices[0].message.content
    sql = extract_sql(raw)

    if not sql:
        raise ValueError("Model did not return valid SQL.")

    reject_disallowed_sql(sql)
    return sql


# ---------------------------------------------------------
# Phase 4E: AI auto-fix using validation feedback
# ---------------------------------------------------------
def ai_fix_sql(schema_json: dict, question: str, bad_sql: str, validation: dict) -> str:
    schema_text = schema_summary(schema_json)

    system = (
        "You are a senior PostgreSQL engineer fixing an existing query.\n"
        "Rules:\n"
        "- Fix ONLY the issues listed in the validation report.\n"
        "- NEVER invent tables or columns.\n"
        "- Preserve the user's intent.\n"
        "- Use alias.column in joins.\n"
        "- Output ONLY SQL.\n"
    )

    user = (
        f"SCHEMA:\n{schema_text}\n\n"
        f"REQUEST:\n{question}\n\n"
        f"ORIGINAL SQL:\n{bad_sql}\n\n"
        f"VALIDATION REPORT:\n{json.dumps(validation, indent=2)}\n\n"
        "Return a corrected SQL query."
    )

    resp = call_openai_with_retry(lambda: client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.0,
        max_tokens=300,
    ))

    raw = resp.choices[0].message.content
    sql = extract_sql(raw)

    if not sql:
        raise ValueError("Model did not return SQL in fix step.")

    reject_disallowed_sql(sql)
    return sql


# ---------------------------------------------------------
# Heuristic fallback (Phase 2 last resort)
# ---------------------------------------------------------
def smart_sql_generator(schema_json: dict, question: str) -> str:
    q = question.lower()
    tables = schema_json.get("tables", {})

    if not tables:
        return "-- No tables parsed."

    if "employee" in q and "branch" in q:
        if "employees" in tables and "branches" in tables:
            emp_cols = tables["employees"]["columns"].keys()
            br_cols = tables["branches"]["columns"].keys()

            emp_select = [f"e.{c}" for c in ["emp_id", "first_name", "last_name"] if c in emp_cols]
            if not emp_select:
                emp_select = ["e.*"]

            br_name_col = "branch_name" if "branch_name" in br_cols else None

            select_parts = emp_select[:]
            if br_name_col:
                select_parts.append(f"b.{br_name_col} AS branch_name")

            return (
                "SELECT " + ", ".join(select_parts) + "\n"
                "FROM employees e\n"
                "JOIN branches b ON b.branch_id = e.branch_id\n"
                "LIMIT 50;"
            )

    first_table = list(tables.keys())[0]
    cols = list(tables[first_table]["columns"].keys())
    col_list = ", ".join(cols[:5]) if cols else "*"
    return f"SELECT {col_list} FROM {first_table} LIMIT 50;"


# ---------------------------------------------------------
# UX message helper (Phase 6 UX-level errors)
# ---------------------------------------------------------
def ux_message(classification: dict, validation: dict) -> str:
    cls = (classification or {}).get("class", "ai_issue")

    if cls == "schema_issue":
        return "Your schema looks incomplete or inconsistent. Re-upload the correct schema.sql."
    if cls == "user_issue":
        return "Your question is ambiguous. Specify the table/column or desired output more clearly."
    if cls == "system_issue":
        return "Temporary system issue (quota/timeout). Try again."
    # ai_issue default
    if (validation or {}).get("unknown_columns") or (validation or {}).get("unknown_identifiers"):
        return "The query referenced a missing column/identifier. It was corrected or flagged."
    if (validation or {}).get("type_mismatches"):
        return "A type mismatch was detected (e.g., comparing text to numbers). Adjust the condition."
    return "Query generated and validated."


# ---------------------------------------------------------
# Request models
# ---------------------------------------------------------
class GenerateRequest(BaseModel):
    schema_id: str
    question: str

class ValidateRequest(BaseModel):
    schema_id: str
    sql: str


# ---------------------------------------------------------
# Schema endpoints
# ---------------------------------------------------------
@app.post("/schema")
async def upload_schema(request: Request, response: Response, file: UploadFile = File(...)):
    cid = get_or_set_client_id(request, response)
    bucket = get_usage_bucket(cid)
    response.headers["X-RateLimit-Limit-Schemas"] = str(MAX_SCHEMAS_PER_DAY)
    response.headers["X-RateLimit-Remaining-Schemas"] = str(MAX_SCHEMAS_PER_DAY - bucket["schemas"])
    require_limit(bucket, "schemas", MAX_SCHEMAS_PER_DAY, "schema uploads")

    raw = await file.read()

    # 1) Reject upload if file is too large (bytes check)
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Schema file too large. Max allowed is {MAX_UPLOAD_MB} MB."
        )

    # Decode text
    text = raw.decode("utf-8", errors="ignore").strip()
    if not text:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Empty schema file. Please upload a schema.sql with CREATE TABLE statements."
        )

    # 2) Parse schema safely (do NOT persist unless this succeeds)
    try:
        tables = parse_schema_sql(text)
        schema_json = to_schema_json(tables)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to parse schema.sql: {type(e).__name__}: {str(e)}"
        )

    all_tables = schema_json.get("tables", {}) or {}
    if not all_tables:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No tables parsed. Ensure schema.sql includes valid CREATE TABLE statements."
        )

    # 3) Reject schema if too big (tables / total columns / columns-per-table)
    table_count = len(all_tables)
    if table_count > MAX_TABLES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Schema has too many tables ({table_count}). Max allowed is {MAX_TABLES}."
        )

    total_cols = 0
    for tname, tmeta in all_tables.items():
        cols = (tmeta.get("columns", {}) or {})
        col_count = len(cols)
        total_cols += col_count

        if col_count > MAX_COLUMNS_PER_TABLE:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Table '{tname}' has too many columns ({col_count}). Max per table is {MAX_COLUMNS_PER_TABLE}."
            )

        if total_cols > MAX_TOTAL_COLUMNS:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Schema has too many total columns ({total_cols}). Max allowed is {MAX_TOTAL_COLUMNS}."
            )

    # 4) Persist ONLY after all checks pass (atomic write)
    schema_id = str(uuid.uuid4())
    SCHEMAS[schema_id] = schema_json

    out_path = SCHEMA_DIR / f"{schema_id}.json"
    tmp_path = SCHEMA_DIR / f"{schema_id}.json.tmp"
    tmp_path.write_text(json.dumps(schema_json, ensure_ascii=False), encoding="utf-8")
    tmp_path.replace(out_path)

    # increment ONLY after success
    bucket["schemas"] += 1
    response.headers["X-RateLimit-Remaining-Schemas"] = str(MAX_SCHEMAS_PER_DAY - bucket["schemas"])

    request_id = str(uuid.uuid4())
    return {
        "status": "ok",
        "request_id": request_id,
        "schema_id": schema_id,
        "summary": {"tables": table_count, "columns": total_cols},
        "schema_preview": list(all_tables.keys())[:10],
    }


@app.get("/schema/{schema_id}")
async def get_schema(schema_id: str, request: Request, response: Response):
    cid = get_or_set_client_id(request, response)
    _ = get_usage_bucket(cid)  # ensure client_id exists, even if not rate-limited here
    request_id = str(uuid.uuid4())

    schema_json = SCHEMAS.get(schema_id)
    if not schema_json:
        p = SCHEMA_DIR / f"{schema_id}.json"
        if p.exists():
            schema_json = json.loads(p.read_text(encoding="utf-8"))
            SCHEMAS[schema_id] = schema_json
        else:
            return {
                "status": "error",
                "request_id": request_id,
                "classification": {"class": "schema_issue"},
                "message": "Unknown schema_id. Please upload schema.sql again.",
                "error": "unknown_schema_id",
            }

    return {
        "status": "ok",
        "request_id": request_id,
        "schema_id": schema_id,
        "schema": schema_json,
    }


# ---------------------------------------------------------
# Phase 4: SQL generation with validation + auto-fix loop
# + Phase 6: question length + usage limit + UX message
# ---------------------------------------------------------
@app.post("/generate")
async def generate(request: Request, response: Response, req: GenerateRequest):
    cid = get_or_set_client_id(request, response)
    bucket = get_usage_bucket(cid)
    response.headers["X-RateLimit-Limit-Generates"] = str(MAX_GENERATES_PER_DAY)
    response.headers["X-RateLimit-Remaining-Generates"] = str(MAX_GENERATES_PER_DAY - bucket["generates"])
    require_limit(bucket, "generates", MAX_GENERATES_PER_DAY, "query generations")
    request_id = str(uuid.uuid4())

    q = (req.question or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="Question is required.")
    if len(q) > MAX_QUESTION_CHARS:
        raise HTTPException(
            status_code=422,
            detail=f"Question too long. Max is {MAX_QUESTION_CHARS} characters."
        )

    schema_json = SCHEMAS.get(req.schema_id)

    if not schema_json:
        p = SCHEMA_DIR / f"{req.schema_id}.json"
        if p.exists():
            schema_json = json.loads(p.read_text(encoding="utf-8"))
            SCHEMAS[req.schema_id] = schema_json
        else:
            return {
                "status": "error",
                "request_id": request_id,
                "classification": {"class": "schema_issue"},
                "message": "Unknown schema_id. Please upload schema.sql again.",
                "error": "unknown_schema_id",
            }

    # 1) Initial AI generation
    try:
        sql = ai_generate_sql(schema_json, q)
    except RateLimitError as e:
        # AI unreachable → heuristics allowed
        sql = smart_sql_generator(schema_json, q)
        validation = validate_against_schema(sql, schema_json)
        classification = classify_issue(validation, q, schema_json)

        # increment usage for a successful response (even if fallback)
        bucket["generates"] += 1
        response.headers["X-RateLimit-Remaining-Generates"] = str(MAX_GENERATES_PER_DAY - bucket["generates"])

        return {
            "status": "ok",
            "request_id": request_id,
            "sql": sql,
            "validation": validation,
            "classification": classification,
            "message": ux_message(classification, validation),
            "note": f"AI unreachable; fallback used. Error: {type(e).__name__}: {str(e)}",
        }
    except Exception as e:
        bucket["generates"] += 1
        response.headers["X-RateLimit-Remaining-Generates"] = str(MAX_GENERATES_PER_DAY - bucket["generates"])

        return {
            "status": "error",
            "request_id": request_id,
            "sql": None,
            "validation": None,
            "classification": {"class": "ai_issue"},
            "message": "AI returned invalid output. Try again or simplify your question.",
            "error": f"AI returned invalid output: {type(e).__name__}: {str(e)}",
        }

    # 2) Validate first candidate
    validation = validate_against_schema(sql, schema_json)
    classification = classify_issue(validation, q, schema_json)
    issue_class = classification.get("class", "ai_issue")

    # 3) Auto-fix loop (AI issue only, max 2 retries)
    for _ in range(2):
        if issue_class != "ai_issue":
            break

        needs_fix = (
            bool(validation.get("missing_tables")) or
            bool(validation.get("unknown_columns")) or
            bool(validation.get("unknown_aliases")) or
            bool(validation.get("unknown_identifiers")) or
            bool(validation.get("invalid_joins")) or
            bool(validation.get("type_mismatches"))
        )
        if not needs_fix:
            break

        sql = ai_fix_sql(schema_json, q, sql, validation)
        validation = validate_against_schema(sql, schema_json)
        classification = classify_issue(validation, q, schema_json)
        issue_class = classification.get("class", "ai_issue")

    # increment usage on successful response
    bucket["generates"] += 1
    response.headers["X-RateLimit-Remaining-Generates"] = str(MAX_GENERATES_PER_DAY - bucket["generates"])

    return {
        "status": "ok",
        "request_id": request_id,
        "sql": sql,
        "validation": validation,
        "classification": classification,
        "message": ux_message(classification, validation),
    }


# ---------------------------------------------------------
# Manual validation endpoint
# ---------------------------------------------------------
@app.post("/validate")
async def validate(request: Request, response: Response, req: ValidateRequest):
    cid = get_or_set_client_id(request, response)
    _ = get_usage_bucket(cid)
    request_id = str(uuid.uuid4())
    
    # Add generate limits for UI consistency
    response.headers["X-RateLimit-Limit-Generates"] = str(MAX_GENERATES_PER_DAY)
    bucket = get_usage_bucket(cid)
    response.headers["X-RateLimit-Remaining-Generates"] = str(MAX_GENERATES_PER_DAY - bucket["generates"])


    schema_json = SCHEMAS.get(req.schema_id)

    if not schema_json:
        p = SCHEMA_DIR / f"{req.schema_id}.json"
        if p.exists():
            schema_json = json.loads(p.read_text(encoding="utf-8"))
            SCHEMAS[req.schema_id] = schema_json
        else:
            return {
                "status": "error",
                "request_id": request_id,
                "classification": {"class": "schema_issue"},
                "message": "Unknown schema_id. Please upload schema.sql again.",
                "error": "unknown_schema_id",
            }

    return {
        "status": "ok",
        "request_id": request_id,
        "schema_id": req.schema_id,
        "validation": validate_against_schema(req.sql, schema_json),
    }


@app.get("/_debug/usage")
async def debug_usage(request: Request, response: Response):
    cid = get_or_set_client_id(request, response)
    bucket = get_usage_bucket(cid)
    tk = today_key()

    return {
        "status": "ok",
        "date": tk,
        "client_id": cid,
        "usage": bucket,
        "limits": {
            "schemas_per_day": MAX_SCHEMAS_PER_DAY,
            "generates_per_day": MAX_GENERATES_PER_DAY,
            "max_question_chars": MAX_QUESTION_CHARS,
            "max_upload_mb": MAX_UPLOAD_MB,
            "max_tables": MAX_TABLES,
            "max_total_columns": MAX_TOTAL_COLUMNS,
            "max_columns_per_table": MAX_COLUMNS_PER_TABLE,
        },
    }