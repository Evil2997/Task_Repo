from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Tuple
import sqlite3
import pandas as pd

from main_app.constants import PREFERRED_ENCODINGS
from main_app.import_csv_to_db import sv


def export_cases_by_numbers(
    db_path: Path,
    input_cases_csv: Path,
    output_csv: Path,
    delimiter: str = ",",
) -> Path:
    """
    БОНУС: выгрузка данных по номерам дел.
    - Читает входной CSV (1 колонка: case_number).
    - Достаёт из SQLite дела, судей и последнее событие по каждому делу.
    - Пишет результат в CSV (UTF-8-SIG). Возвращает путь к выходному файлу.

    Выходные колонки:
      court_name, case_number, registration_date, type, description,
      reporting_judge, panel_judges,
      last_stage_date, last_stage_name, last_cause_result, last_cause_dep, last_case_proc,
      not_found  (0/1)
    """
    db_path = Path(db_path)
    input_cases_csv = Path(input_cases_csv)
    output_csv = Path(output_csv)

    last_err = None
    df_in = None
    for enc in PREFERRED_ENCODINGS:
        try:
            df_in = pd.read_csv(
                input_cases_csv,
                encoding=enc,
                dtype=str,
                keep_default_na=False,
                na_filter=False,
                sep=None,
                engine="python",
            )
            break
        except Exception as e:
            last_err = e
    if df_in is None:
        raise RuntimeError(f"Не удалось прочитать входной CSV: {input_cases_csv} ({last_err})")

    col = None
    for c in df_in.columns:
        if c.strip().lower() == "case_number":
            col = c
            break
    if col is None:
        col = df_in.columns[0]

    seen = set()
    input_numbers: List[str] = []
    for v in df_in[col].tolist():
        n = sv(v)
        if not n or n in seen:
            continue
        seen.add(n)
        input_numbers.append(n)

    if not input_numbers:
        pd.DataFrame(columns=[
            "court_name","case_number","registration_date","type","description",
            "reporting_judge","panel_judges",
            "last_stage_date","last_stage_name","last_cause_result","last_cause_dep","last_case_proc",
            "not_found"
        ]).to_csv(output_csv, index=False, encoding="utf-8-sig", sep=delimiter)
        return output_csv

    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        def batched(seq, size=500):
            for i in range(0, len(seq), size):
                yield seq[i:i+size]

        cases: Dict[int, dict] = {}
        num_to_caseids: Dict[str, List[int]] = {}
        for chunk in batched(input_numbers):
            q = f"""
            SELECT id, court_name, case_number, registration_date, type, description
            FROM cases
            WHERE case_number IN ({",".join("?"*len(chunk))})
            """
            for r in cur.execute(q, chunk):
                d = dict(r)
                cases[d["id"]] = d
                num_to_caseids.setdefault(d["case_number"], []).append(d["id"])

        case_judges: Dict[int, Dict[str, List[str]]] = {}
        if cases:
            ids = list(cases.keys())
            for chunk in batched(ids):
                q = f"""
                SELECT cj.case_id, j.name AS judge_name, cj.role
                FROM case_judges cj
                JOIN judges j ON j.id = cj.judge_id
                WHERE cj.case_id IN ({",".join("?"*len(chunk))})
                """
                for r in cur.execute(q, chunk):
                    cid = r["case_id"]
                    role = sv(r["role"])
                    name = sv(r["judge_name"])
                    bucket = case_judges.setdefault(cid, {"reporting": [], "panel": []})
                    if role.lower().startswith("суддя-доповідач"):
                        if name and name not in bucket["reporting"]:
                            bucket["reporting"].append(name)
                    else:
                        pair = (f"{role}: {name}" if role else name) if name else ""
                        if pair and pair not in bucket["panel"]:
                            bucket["panel"].append(pair)

        last_event: Dict[int, dict] = {}
        if cases:
            ids = list(cases.keys())
            for chunk in batched(ids):
                q = f"""
                SELECT case_id, case_proc, stage_date, stage_name, cause_result, cause_dep, id
                FROM case_events
                WHERE case_id IN ({",".join("?"*len(chunk))})
                """
                for r in cur.execute(q, chunk):
                    cid = r["case_id"]
                    sd = sv(r["stage_date"])
                    key = (sd if sd else "0000-00-00", int(r["id"]))
                    prev = last_event.get(cid)
                    if (prev is None) or ((key[0], key[1]) > (prev["_k0"], prev["_k1"])):
                        last_event[cid] = {
                            "last_case_proc": sv(r["case_proc"]),
                            "last_stage_date": sd,
                            "last_stage_name": sv(r["stage_name"]),
                            "last_cause_result": sv(r["cause_result"]),
                            "last_cause_dep": sv(r["cause_dep"]),
                            "_k0": key[0], "_k1": key[1],
                        }

        rows = []
        for num in input_numbers:
            caseids = num_to_caseids.get(num, [])
            if not caseids:
                rows.append({
                    "court_name": "", "case_number": num, "registration_date": "",
                    "type": "", "description": "",
                    "reporting_judge": "", "panel_judges": "",
                    "last_stage_date": "", "last_stage_name": "",
                    "last_cause_result": "", "last_cause_dep": "", "last_case_proc": "",
                    "not_found": 1,
                })
                continue

            for cid in caseids:
                base = cases[cid]
                judges = case_judges.get(cid, {"reporting": [], "panel": []})
                ev = last_event.get(cid, {})
                rows.append({
                    "court_name": sv(base.get("court_name")),
                    "case_number": sv(base.get("case_number")),
                    "registration_date": sv(base.get("registration_date")),
                    "type": sv(base.get("type")),
                    "description": sv(base.get("description")),
                    "reporting_judge": "; ".join(judges["reporting"]) if judges["reporting"] else "",
                    "panel_judges": "; ".join(judges["panel"]) if judges["panel"] else "",
                    "last_stage_date": sv(ev.get("last_stage_date", "")),
                    "last_stage_name": sv(ev.get("last_stage_name", "")),
                    "last_cause_result": sv(ev.get("last_cause_result", "")),
                    "last_cause_dep": sv(ev.get("last_cause_dep", "")),
                    "last_case_proc": sv(ev.get("last_case_proc", "")),
                    "not_found": 0,
                })

        out_df = pd.DataFrame(rows, columns=[
            "court_name","case_number","registration_date","type","description",
            "reporting_judge","panel_judges",
            "last_stage_date","last_stage_name","last_cause_result","last_cause_dep","last_case_proc",
            "not_found",
        ])
        out_df.to_csv(output_csv, index=False, encoding="utf-8-sig", sep=delimiter)
        return output_csv
    finally:
        conn.close()
