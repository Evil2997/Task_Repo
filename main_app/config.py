import zipfile
from pathlib import Path
from typing import Iterable


def extract_zip(zip_path: Path, extract_root: Path) -> Path:
    target_dir = extract_root / zip_path.stem
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(target_dir)
    return target_dir


def iter_csv_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*.csv"):
        if p.is_file() and not p.name.startswith("._"):
            yield p
