# QueryWave

QueryWave is a lightweight web tool that converts plain-English questions into safe, validated SQL queries using your own database schema.

It is designed for:
- junior SQL developers
- students learning SQL
- entry-level data analysts

---

## What QueryWave Does

1. You upload a `.sql` schema file (CREATE TABLE statements).
2. QueryWave parses and understands your schema.
3. You ask a question in plain English.
4. QueryWave generates:
   - a SQL query
   - a validation summary explaining what it did

The system enforces strict safety rules:
- No schema hallucination
- No destructive SQL (DROP / DELETE / ALTER)
- Clear errors for invalid input

---

## What QueryWave Does NOT Do

- It does NOT execute SQL against a database
- It does NOT guarantee business correctness
- It does NOT replace SQL knowledge

All generated SQL should be reviewed before use.

---

## Safety & Validation

QueryWave includes multiple validation layers:
- schema-aware SQL generation
- rejection of unsafe statements
- limits on query scope and complexity

If a question is ambiguous or unsafe, QueryWave will refuse or return a conservative result.

---

## Current Status

**Version:** v1.0  
**Stage:** Soft launch / early feedback

This is an early version focused on correctness, safety, and UX clarity.

---

Deployment note:
Static assets are cache-busted using query strings (e.g. app.js?v=1.2).
After JS/CSS changes, bump the version and redeploy to ensure users receive updates.

## Getting Started (Local)

```bash
python app.py
