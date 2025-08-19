#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import tempfile
from pathlib import Path
from main_app.config import extract_zip, iter_csv_files
from main_app.ensure_year_archives_local import ensure_year_archives_local
from main_app.importer_one_csv import import_csv_to_db
from main_app.paths import DB_PATH


def rospakovka(zip_paths: list[Path], year: int):
    with tempfile.TemporaryDirectory(prefix=f"court_{year}_") as tmpdir:
        extracted_root = Path(tmpdir) / "extracted"
        extracted_root.mkdir(parents=True, exist_ok=True)
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        for zip_path in zip_paths:
            try:
                out_dir = extract_zip(zip_path, extracted_root)
                print(f"[✓] Распаковано: {zip_path.name} -> {out_dir}")
                for csv_file in iter_csv_files(out_dir):
                    try:
                        import_csv_to_db(csv_file, DB_PATH)
                        print(f"[DB] Импортировано в БД: {csv_file.name}")
                    except Exception as e:
                        print(f"[ERR] Импорт {csv_file.name} в БД: {e}")
            except Exception as e:
                print(f"[ERR] Ошибка распаковки {zip_path.name}: {e}")

    print(f"Готово: распаковка и импорт CSV выполнены. БД: {DB_PATH}")


def main(year: int = 2025) -> None:
    print(f"Проверяем локальные ZIP и, при необходимости, докачиваем за {year} год...")
    zip_paths = ensure_year_archives_local(year)
    if not zip_paths:
        print("ZIP-архивы за указанный год не найдены ни локально, ни онлайн.")
        return

    print(f"Готово. Локальных архивов за {year}: {len(zip_paths)}")
    for i, p in enumerate(zip_paths, 1):
        print(f"  {i:02d}. {p.name}")

    rospakovka(zip_paths=zip_paths, year=year)

if __name__ == "__main__":
    main()
