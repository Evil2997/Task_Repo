import pathlib
from typing import Final

MAIN_DIR: Final[pathlib.Path] = pathlib.Path(__file__).resolve().parents[1]

INPUT_DIR = MAIN_DIR / "input_dir"
OUTPUT_DIR = MAIN_DIR / "output_dir"

DB_PATH = OUTPUT_DIR / "court_registry.db"
