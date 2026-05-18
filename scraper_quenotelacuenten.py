"""
Scraper de libros/PDFs para quenotelacuenten.org

Uso:
    python scraper_quenotelacuenten.py [--ocr] [--delay 1.5] [--max-pages 500]

Opciones:
    --ocr          Aplicar OCR a los PDFs descargados (requiere ocrmypdf)
    --delay N      Segundos entre peticiones HTTP (default: 1.5)
    --max-pages N  Límite de páginas a rastrear (default: sin límite)
    --dry-run      Solo muestra las URLs encontradas sin descargar
"""

import argparse
import csv
import hashlib
import logging
import os
import time
from collections import deque
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
BASE_URL = "https://www.quenotelacuenten.org/"
DEST_DIR = Path("pdfs_downloaded")
OCR_DIR = Path("pdfs_ocr")
INDEX_FILE = Path("index_libros.csv")
STATE_FILE = Path(".scraper_state.txt")   # URLs ya visitadas (permite reanudar)

BOOK_EXTENSIONS = {".pdf", ".epub", ".mobi", ".azw3", ".djvu"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

MAX_RETRIES = 4
BACKOFF_BASE = 2  # segundos (2, 4, 8, 16)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP con reintentos y backoff exponencial
# ---------------------------------------------------------------------------
def fetch(session: requests.Session, url: str, stream=False, timeout=20):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, stream=stream, timeout=timeout, headers=HEADERS)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                log.warning("Fallo definitivo %s: %s", url, exc)
                return None
            wait = BACKOFF_BASE ** attempt
            log.debug("Reintento %d/%d en %ds para %s", attempt, MAX_RETRIES, wait, url)
            time.sleep(wait)
    return None


# ---------------------------------------------------------------------------
# Extracción de links desde una página
# ---------------------------------------------------------------------------
def extract_links(soup: BeautifulSoup, page_url: str, base_domain: str):
    """Devuelve (book_links, internal_page_links)."""
    book_links = set()
    internal_links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue
        full = urljoin(page_url, href)
        parsed = urlparse(full)

        # Normalizar: quitar fragmento y trailing slash innecesario
        clean = parsed._replace(fragment="").geturl()

        ext = Path(parsed.path).suffix.lower()
        if ext in BOOK_EXTENSIONS:
            book_links.add(clean)
        elif parsed.netloc == base_domain and parsed.scheme in ("http", "https"):
            internal_links.add(clean)

    return book_links, internal_links


# ---------------------------------------------------------------------------
# Descarga de un fichero con barra de progreso
# ---------------------------------------------------------------------------
def download_file(session: requests.Session, url: str, dest: Path, delay: float) -> bool:
    if dest.exists():
        log.debug("Ya existe: %s", dest.name)
        return False  # False = no descargado (ya estaba)

    resp = fetch(session, url, stream=True)
    if resp is None:
        return False

    total = int(resp.headers.get("content-length", 0))
    tmp = dest.with_suffix(".part")
    try:
        with open(tmp, "wb") as f, tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=dest.name[:50],
            leave=False,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
        tmp.rename(dest)
        time.sleep(delay)
        return True
    except Exception as exc:
        log.error("Error descargando %s: %s", url, exc)
        tmp.unlink(missing_ok=True)
        return False


# ---------------------------------------------------------------------------
# OCR (opcional)
# ---------------------------------------------------------------------------
def run_ocr(input_path: Path, output_path: Path):
    try:
        import ocrmypdf
        ocrmypdf.ocr(input_path, output_path, deskew=True, progress_bar=False)
        log.info("OCR ok: %s", output_path.name)
    except Exception as exc:
        log.error("Error OCR %s: %s", input_path.name, exc)


# ---------------------------------------------------------------------------
# Nombre de fichero seguro y único basado en URL
# ---------------------------------------------------------------------------
def safe_filename(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name.replace(" ", "_") or "file"
    # Si hay colisión de nombre entre distintas URLs, añadir hash corto
    url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
    stem = Path(name).stem
    suffix = Path(name).suffix or ".bin"
    return f"{stem}_{url_hash}{suffix}"


# ---------------------------------------------------------------------------
# Estado persistente (para reanudar si se interrumpe)
# ---------------------------------------------------------------------------
def load_visited(state_file: Path) -> set:
    if not state_file.exists():
        return set()
    return set(state_file.read_text().splitlines())


def save_visited(state_file: Path, visited: set):
    state_file.write_text("\n".join(sorted(visited)))


# ---------------------------------------------------------------------------
# Escritura incremental del índice CSV
# ---------------------------------------------------------------------------
class CsvWriter:
    def __init__(self, path: Path):
        self.path = path
        self._is_new = not path.exists()
        self._fh = open(path, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(
            self._fh,
            fieldnames=["source_page", "file_url", "local_file", "ocr_file", "extension"],
        )
        if self._is_new:
            self._writer.writeheader()

    def write(self, row: dict):
        self._writer.writerow(row)
        self._fh.flush()

    def close(self):
        self._fh.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Scraper de libros quenotelacuenten.org")
    parser.add_argument("--ocr", action="store_true", help="Aplicar OCR a los PDFs")
    parser.add_argument("--delay", type=float, default=1.5, help="Segundos entre peticiones")
    parser.add_argument("--max-pages", type=int, default=0, help="Límite de páginas (0=sin límite)")
    parser.add_argument("--dry-run", action="store_true", help="Solo muestra URLs sin descargar")
    args = parser.parse_args()

    DEST_DIR.mkdir(exist_ok=True)
    if args.ocr:
        OCR_DIR.mkdir(exist_ok=True)

    base_domain = urlparse(BASE_URL).netloc
    visited = load_visited(STATE_FILE)
    queue = deque([BASE_URL])
    session = requests.Session()
    csv_writer = CsvWriter(INDEX_FILE)

    pages_scraped = 0
    total_downloaded = 0

    log.info("Iniciando scraping de %s", BASE_URL)
    log.info("Archivos buscados: %s", ", ".join(BOOK_EXTENSIONS))

    try:
        while queue:
            if args.max_pages and pages_scraped >= args.max_pages:
                log.info("Límite de %d páginas alcanzado.", args.max_pages)
                break

            current_url = queue.popleft()
            if current_url in visited:
                continue
            visited.add(current_url)

            log.info("[%d visitadas | cola: %d] %s", pages_scraped, len(queue), current_url)

            resp = fetch(session, current_url)
            if resp is None:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            book_links, page_links = extract_links(soup, current_url, base_domain)

            # Encolar páginas internas aún no visitadas
            for link in page_links:
                if link not in visited:
                    queue.append(link)

            # Descargar libros encontrados
            for book_url in book_links:
                ext = Path(urlparse(book_url).path).suffix.lower()
                filename = safe_filename(book_url)
                dest = DEST_DIR / filename

                if args.dry_run:
                    log.info("  [DRY-RUN] %s", book_url)
                    continue

                downloaded = download_file(session, book_url, dest, args.delay)
                if downloaded:
                    total_downloaded += 1
                    log.info("  Descargado: %s", filename)

                ocr_path = ""
                if args.ocr and ext == ".pdf" and dest.exists():
                    ocr_path = str(OCR_DIR / f"OCR_{filename}")
                    run_ocr(dest, Path(ocr_path))

                csv_writer.write({
                    "source_page": current_url,
                    "file_url": book_url,
                    "local_file": str(dest) if dest.exists() else "",
                    "ocr_file": ocr_path,
                    "extension": ext,
                })

            pages_scraped += 1
            time.sleep(args.delay)

            # Guardar estado cada 10 páginas
            if pages_scraped % 10 == 0:
                save_visited(STATE_FILE, visited)

    except KeyboardInterrupt:
        log.info("Interrumpido por el usuario.")
    finally:
        save_visited(STATE_FILE, visited)
        csv_writer.close()

    log.info("--- Fin ---")
    log.info("Páginas rastreadas : %d", pages_scraped)
    log.info("Archivos descargados: %d", total_downloaded)
    log.info("Índice guardado en : %s", INDEX_FILE)


if __name__ == "__main__":
    main()
