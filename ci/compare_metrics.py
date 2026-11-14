"""Compare fixture-built metric tables in the CI schema against ci/baseline.json.

Comparison is exact. A model that was not rebuilt for this PR (table absent)
is skipped. An intended metric change must update baseline.json in the same PR.

    PYTHONPATH=scripts DBT_SCHEMA=ci_pr_1 python ci/compare_metrics.py
"""
import json
import os
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path


def _norm(v):
    s = str(v)
    try:
        return Decimal(s)  # 19.5 == 19.50
    except InvalidOperation:
        return s


def diff(expected, actual):
    """Both are lists of dicts in the same order. Returns a list of messages."""
    if len(expected) != len(actual):
        return [f"row count {len(actual)} != expected {len(expected)}"]
    out = []
    for i, (e, a) in enumerate(zip(expected, actual)):
        for col in sorted(set(e) | set(a)):
            if col not in e or col not in a or _norm(e[col]) != _norm(a[col]):
                out.append(f"row {i} {col}: got {a.get(col)!r}, expected {e.get(col)!r}")
    return out


def main():
    from psycopg2 import errors, sql

    from merge import connect

    schema = os.environ["DBT_SCHEMA"]
    baseline = json.loads((Path(__file__).parent / "baseline.json").read_text())
    failed = False
    conn = connect()
    conn.autocommit = True
    with conn.cursor() as cur:
        for model, expected in baseline.items():
            try:
                cur.execute(sql.SQL("SELECT * FROM {}.{} ORDER BY 1").format(sql.Identifier(schema), sql.Identifier(model)))
            except errors.UndefinedTable:
                print(f"SKIP {model}: not affected by this PR")
                continue
            cols = [d[0] for d in cur.description]
            problems = diff(expected, [dict(zip(cols, row)) for row in cur.fetchall()])
            print("FAIL" if problems else "PASS", model)
            for p in problems:
                print("  ", p)
            failed = failed or bool(problems)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
