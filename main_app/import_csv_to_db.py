from __future__ import annotations
from pathlib import Path
import re
import sqlite3
import pandas as pd

from typing import Optional, Iterable, Tuple, Dict

from main_app.constants import (
    PREFERRED_ENCODINGS,
    reg__1,
    reg__2,
    data_base__2,
    data_base__3,
    data_base__4,
    rename_map,
)
from main_app.db import init_db, get_conn


def sv(x) -> str:
    if isinstance(x, str):
        s = x.strip()
        return s
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    try:
        return str(x).strip()
    except Exception:
        return ""


def parse_date_ddmmyyyy(val: str) -> Optional[str]:
    v = val.strip().strip('"').strip()
    if not v or v in {"-", "—"}:
        return None
    m = re.match(reg__1, v)
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    return f"{yyyy}-{mm}-{dd}"


def parse_one_role_name(s) -> Optional[Tuple[str, str]]:
    s = sv(s)
    if not s:
        return None
    m = reg__2.match(s)
    if m:
        return sv(m.group(1)), sv(m.group(2))
    return "", s


def split_multi(value: Optional[str]) -> Iterable[str]:
    v = sv(value)
    if not v:
        return []
    return [p for p in (sv(x) for x in v.split(";")) if p]


def read_csv_auto(csv_path: Path) -> pd.DataFrame:
    last_err = None
    for enc in PREFERRED_ENCODINGS:
        try:
            return pd.read_csv(
                csv_path,
                encoding=enc,
                sep=None,
                engine="python",
                dtype=str,
                keep_default_na=False,
                na_filter=False
            )
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Не удалось прочитать {csv_path}: {last_err}")


def upsert_case(cur: sqlite3.Cursor, row: dict) -> int:
    cur.execute(data_base__2, (
            sv(row.get("court_name")),
            sv(row.get("case_number")),
            parse_date_ddmmyyyy(sv(row.get("registration_date"))),
            sv(row.get("type")),
            sv(row.get("description")),
        )
    )
    cur.execute(
        "SELECT id FROM cases WHERE court_name=? AND case_number=?",
        (sv(row.get("court_name")), sv(row.get("case_number")))
    )
    return int(cur.fetchone()[0])


def upsert_judge(cur: sqlite3.Cursor, name: str) -> int:
    name = name.strip()
    if not name:
        raise ValueError("empty judge name")
    cur.execute("INSERT INTO judges(name) VALUES (?) ON CONFLICT(name) DO NOTHING", (name,))
    cur.execute("SELECT id FROM judges WHERE name=?", (name,))
    return int(cur.fetchone()[0])

def link_case_judge(cur: sqlite3.Cursor, case_id: int, judge_id: int, role: str) -> None:
    cur.execute(data_base__3, (case_id, judge_id, (role or "").strip()))


def upsert_event(cur: sqlite3.Cursor, case_id: int, row: dict) -> None:
    stage_date = parse_date_ddmmyyyy(sv(row.get("stage_date"))) or ""
    cur.execute(data_base__4, (
        case_id,
        sv(row.get("case_proc")),
        stage_date,
        sv(row.get("stage_name")),
        sv(row.get("cause_result")),
        sv(row.get("cause_dep")),
    ))


def import_csv_to_db(csv_path: Path, db_path: Path) -> None:
    init_db(db_path)
    df = read_csv_auto(csv_path)

    with get_conn(db_path) as conn:
        cur = conn.cursor()
        cache_judges: Dict[str, int] = {}

        for _, r in df.iterrows():
            row = {k: r.get(k) if k in r else None for k in rename_map.keys()}

            if not row["court_name"] or not row["case_number"]:
                continue

            case_id = upsert_case(cur, row)

            if row.get("judge"):
                rn = parse_one_role_name(str(row["judge"]))
                if rn:
                    role, name = rn
                    if name:
                        if name not in cache_judges:
                            cache_judges[name] = upsert_judge(cur, name)
                        link_case_judge(cur, case_id, cache_judges[name], role)

            if row.get("judges"):
                for chunk in split_multi(str(row["judges"])):
                    rn = parse_one_role_name(chunk)
                    if rn:
                        role, name = rn
                        if name:
                            if name not in cache_judges:
                                cache_judges[name] = upsert_judge(cur, name)
                            link_case_judge(cur, case_id, cache_judges[name], role)

            upsert_event(cur, case_id, row)

        conn.commit()
