import tempfile
from pathlib import Path

import requests

from main_app.config import extract_zip, iter_csv_files
from main_app.constants import header
from main_app.import_csv_to_db import import_csv_to_db
from main_app.paths import DB_PATH
from main_app.urls import download_file, iter_year_zip_links


def rospakovka(year: int, max_pages: int = 50, chunk_size: int = 10):
    SESSION = requests.Session()
    SESSION.headers.update(header)

    any_found = False
    for url in iter_year_zip_links(year, SESSION=SESSION, max_pages=max_pages):
        any_found = True
        try:
            with tempfile.TemporaryDirectory(prefix="court_zip_") as tmpdir:
                tmpdir_path = Path(tmpdir)

                # 1) скачали
                zip_path = download_file(url, tmpdir_path, SESSION=SESSION, chunk_size=chunk_size)

                # 2) распаковали
                extracted_root = tmpdir_path / "extracted"
                extracted_root.mkdir(parents=True, exist_ok=True)
                out_dir = extract_zip(zip_path, extracted_root)
                print(f"[✓] Распаковано: {zip_path.name} -> {out_dir}")

                # 3) импортировали все CSV
                for csv_file in iter_csv_files(out_dir):
                    try:
                        import_csv_to_db(csv_file, DB_PATH)
                        print(f"[DB] Импортировано в БД: {csv_file.name}")
                    except Exception as e:
                        print(f"[ERR] Импорт {csv_file.name} в БД: {e}")

                print(f"[CLEAN] Обработан и удалён: {zip_path.name}")

        except Exception as e:
            print(f"[ERR] Не удалось обработать {url}: {e}")

    if not any_found:
        print(f"[WARN] Не найдено архивов за {year}")
    else:
        print(f"Готово: распаковка и импорт CSV выполнены. БД: {DB_PATH}")