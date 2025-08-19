from __future__ import annotations
import sqlite3
from pathlib import Path

from main_app.constants import data_base__1


def get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db(db_path: Path) -> None:
    conn = get_conn(db_path)
    cur = conn.cursor()

    cur.executescript(data_base__1)
    conn.commit()
    conn.close()
