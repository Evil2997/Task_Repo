import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from main_app.constants import BASE_LIST_URL


def download_file(url: str, dest_dir: Path, SESSION) -> Path:
    """
    Скачивает файл по URL в dest_dir. Возвращает путь к локальному файлу.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(urlparse(url).path).name or f"archive_{int(time.time())}.zip"
    dest_path = dest_dir / filename

    with SESSION.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    return dest_path


def find_year_zip_links(year: int, SESSION, max_pages: int = 50) -> list[str]:
    """
    Проходит по страницам реестра и собирает ссылки на .zip,
    у которых в имени/ссылке встречается указанный год.
    """
    links: list[str] = []
    seen = set()

    for page in range(1, max_pages + 1):
        url = BASE_LIST_URL.format(page=page)
        resp = SESSION.get(url, timeout=30)
        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        page_links = 0

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = (a.get_text() or "").strip()
            if not href.lower().endswith(".zip"):
                continue

            if str(year) not in href and str(year) not in text:
                continue

            full_url = urljoin(url, href)
            if full_url in seen:
                continue

            links.append(full_url)
            seen.add(full_url)
            page_links += 1

        if page_links == 0:
            break

        time.sleep(0.5)

    return links
