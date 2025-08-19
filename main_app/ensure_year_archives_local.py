from pathlib import Path
from urllib.parse import urlparse

import requests

from main_app.constants import header
from main_app.paths import INPUT_DIR
from main_app.urls import download_file, find_year_zip_links


def ensure_year_archives_local(year: int, max_pages: int = 50) -> list[Path]:
    SESSION = requests.Session()
    SESSION.headers.update(header)

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = INPUT_DIR / f"manifest_{year}.txt"

    local_by_name = {p.name: p for p in INPUT_DIR.glob(f"*{year}*.zip")}

    if manifest_path.exists():
        expected = [line.strip() for line in manifest_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        missing = [name for name in expected if name not in local_by_name]
        if not missing:
            return [local_by_name[name] for name in expected]

    links = find_year_zip_links(year, SESSION=SESSION, max_pages=max_pages)
    if not links:
        return list(local_by_name.values())

    needed_filenames = [Path(urlparse(u).path).name for u in links]

    downloaded_paths: list[Path] = []
    for url, filename in zip(links, needed_filenames):
        if filename in local_by_name:
            continue
        try:
            dst = download_file(url, INPUT_DIR, SESSION=SESSION)
            downloaded_paths.append(dst)
            local_by_name[dst.name] = dst
        except Exception as e:
            print(f"[ERR] Не удалось скачать {url}: {e}")

    manifest_path.write_text("\n".join(needed_filenames), encoding="utf-8")

    return [local_by_name[name] for name in needed_filenames if name in local_by_name]
