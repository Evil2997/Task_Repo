import re

PREFERRED_ENCODINGS = [
    "utf-8-sig",
    "cp1251",
    "windows-1251",
    "utf-8",
]
BASE_LIST_URL = "https://dsa.court.gov.ua/dsa/inshe/oddata/532/?page={page}"
header = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) PythonDownloader/1.0 (+requests)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

rename_map = {
    "court_name": "court_name",
    "case_number": "case_number",
    "case_proc": "case_proc",
    "registration_date": "registration_date",
    "judge": "judge",
    "judges": "judges",
    "participants": "participants",
    "stage_date": "stage_date",
    "stage_name": "stage_name",
    "cause_result": "cause_result",
    "cause_dep": "cause_dep",
    "type": "type",
    "description": "description",
}


reg__1 = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")
reg__2 = re.compile(r"^\s*([^:]+):\s*(.+?)\s*$")

data_base__1 = """
    CREATE TABLE IF NOT EXISTS cases (
        id                 INTEGER PRIMARY KEY,
        court_name         TEXT NOT NULL,
        case_number        TEXT NOT NULL,
        registration_date  DATE,
        type               TEXT,
        description        TEXT,
        UNIQUE(court_name, case_number)
    );

    CREATE TABLE IF NOT EXISTS judges (
        id     INTEGER PRIMARY KEY,
        name   TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS case_judges (
        id        INTEGER PRIMARY KEY,
        case_id   INTEGER NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
        judge_id  INTEGER NOT NULL REFERENCES judges(id) ON DELETE CASCADE,
        role      TEXT,
        UNIQUE(case_id, judge_id, role)
    );

    CREATE TABLE IF NOT EXISTS case_events (
        id            INTEGER PRIMARY KEY,
        case_id       INTEGER NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
        case_proc     TEXT,
        stage_date    TEXT,
        stage_name    TEXT,
        cause_result  TEXT,
        cause_dep     TEXT,
        UNIQUE(case_id, stage_date, stage_name, cause_result, cause_dep)
    );
"""

data_base__2 = """
    INSERT INTO cases (court_name, case_number, registration_date, type, description)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(court_name, case_number) DO UPDATE SET
        registration_date=excluded.registration_date
"""
data_base__3 = """
        INSERT INTO case_judges(case_id, judge_id, role)
        VALUES (?, ?, ?)
        ON CONFLICT(case_id, judge_id, role) DO NOTHING
    """


data_base__4 = """
        INSERT INTO case_events(case_id, case_proc, stage_date, stage_name, cause_result, cause_dep)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(case_id, stage_date, stage_name, cause_result, cause_dep) DO NOTHING
    """