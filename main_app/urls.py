import time
from datetime import datetime
from pathlib import Path
from typing import Iterator
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from main_app.constants import BASE_LIST_URL, DATE_RE


def download_file(url: str, dest_dir: Path, SESSION, chunk_size) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(urlparse(url).path).name or f"archive_{int(time.time())}.zip"
    dest_path = dest_dir / filename

    with SESSION.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size*1024*1024):
                if chunk:
                    f.write(chunk)

    return dest_path


def extract_date_from_context(a_zip) -> str | None:
    node = a_zip
    for _ in range(6):
        if not node:
            break
        text = node.get_text(" ", strip=True)
        m = DATE_RE.search(text)
        if m:
            return m.group(1)
        node = node.parent
    sib = a_zip.find_previous(lambda tag: DATE_RE.search(tag.get_text() or ""))
    if sib:
        m = DATE_RE.search(sib.get_text())
        if m:
            return m.group(1)
    return None


def iter_year_zip_links(year: int, SESSION, max_pages: int = 50) -> Iterator[str]:
    seen = set()

    for page in range(1, max_pages + 1):
        url = BASE_LIST_URL.format(page=page)
        resp = SESSION.get(url, timeout=30)
        if resp.status_code != 200:
            print(f"[WARN] {url} -> HTTP {resp.status_code}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        page_links = 0
        page_years = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            path = (urlparse(href).path or "").lower()
            if not path.endswith(".zip"):
                continue

            full_url = urljoin(url, href)
            if full_url in seen:
                continue

            date_str = extract_date_from_context(a)
            if not date_str:
                continue

            d = datetime.strptime(date_str, "%d.%m.%Y").date()
            page_years.add(d.year)

            if d.year == year:
                seen.add(full_url)
                page_links += 1
                yield full_url

        if page_links > 0:
            print(f"[INFO] Страница {page}: найдено {page_links} файлов за {year}")
        else:
            print(f"[INFO] Страница {page}: ничего не найдено")

        if page_years and max(page_years) < year:
            print(f"[STOP] встретили {max(page_years)}, дальше страниц за {year} не будет")
            break

        if page_links == 0 and not page_years:
            break

        time.sleep(0.5)
