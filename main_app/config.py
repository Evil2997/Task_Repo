import zipfile
from pathlib import Path
from typing import Iterable

import pandas as pd

from main_app.constants import PREFERRED_ENCODINGS


def extract_zip(zip_path: Path, extract_root: Path) -> Path:
    target_dir = extract_root / zip_path.stem
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(target_dir)
    return target_dir


def read_csv_with_encoding(csv_path: Path) -> pd.DataFrame:
    last_err = None
    for enc in PREFERRED_ENCODINGS:
        try:
            df = pd.read_csv(csv_path, encoding=enc, sep=None, engine="python")
            print(f"[OK] Прочитан CSV: {csv_path.name} (encoding={enc}, rows={len(df)})")
            return df
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Не удалось прочитать CSV {csv_path} с доступными кодировками. Последняя ошибка: {last_err}")


def iter_csv_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*.csv"):
        if p.is_file() and not p.name.startswith("._"):
            yield p
