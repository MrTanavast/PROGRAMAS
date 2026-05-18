"""
Scraper de libros/PDFs para quenotelacuenten.org

El sitio usa JavaScript para renderizar el contenido, por lo que se
usa Playwright (navegador real) para obtener el HTML final.

Instalación:
    pip install playwright requests tqdm
    playwright install chromium

Uso:
    python scraper_quenotelacuenten.py [--delay 2] [--max-pages 500] [--dry-run]

Opciones:
    --delay N      Segundos entre páginas (default: 2)
    --max-pages N  Límite de páginas a rastrear (default: sin límite)
    --dry-run      Solo muestra las URLs encontradas sin descargar
    --headed       Abre el navegador visible (útil para depurar)
"""

import argparse
import csv
import hashlib
import logging
import time
from collections import deque
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
BASE_URL = "https://www.quenotelacuenten.org/"

# URLs de entrada — el scraper arranca desde todas ellas y sigue todos los links internos
SEED_URLS = [
    "https://www.quenotelacuenten.org/",
    "https://www.quenotelacuenten.org/libros-recomendados/",
    "https://www.quenotelacuenten.org/category/libros/",
    "https://www.quenotelacuenten.org/category/documentos/",
    "https://www.quenotelacuenten.org/sitemap.xml",
]

DEST_DIR = Path("pdfs_downloaded")
INDEX_FILE = Path("index_libros.csv")
STATE_FILE = Path(".scraper_state.txt")

BOOK_EXTENSIONS = {".pdf", ".epub", ".mobi", ".azw3", ".djvu", ".doc", ".docx", ".odt"}

DOWNLOAD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

MAX_RETRIES = 4
BACKOFF_BASE = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Extracción de links desde HTML renderizado
# ---------------------------------------------------------------------------
def extract_links(html: str, page_url: str, base_domain: str):
    """Devuelve (book_links, internal_page_links) usando html ya renderizado."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    book_links = set()
    internal_links = set()

    # Links normales <a href>
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue
        full = urljoin(page_url, href)
        parsed = urlparse(full)
        clean = parsed._replace(fragment="").geturl()
        ext = Path(parsed.path).suffix.lower()

        if ext in BOOK_EXTENSIONS:
            book_links.add(clean)
        elif parsed.netloc == base_domain and parsed.scheme in ("http", "https"):
            internal_links.add(clean)

    # Sitemap XML: extraer <loc> tags
    for loc in soup.find_all("loc"):
        url = loc.get_text(strip=True)
        parsed = urlparse(url)
        if parsed.netloc == base_domain and parsed.scheme in ("http", "https"):
            ext = Path(parsed.path).suffix.lower()
            if ext in BOOK_EXTENSIONS:
                book_links.add(url)
            else:
                internal_links.add(url)

    return book_links, internal_links


# ---------------------------------------------------------------------------
# Descarga de fichero con reintentos y barra de progreso
# ---------------------------------------------------------------------------
def download_file(session: requests.Session, url: str, dest: Path, delay: float) -> bool:
    if dest.exists():
        return False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, stream=True, timeout=30, headers=DOWNLOAD_HEADERS)
            resp.raise_for_status()
            break
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                log.warning("Fallo definitivo descargando %s: %s", url, exc)
                return False
            time.sleep(BACKOFF_BASE ** attempt)
    else:
        return False

    total = int(resp.headers.get("content-length", 0))
    tmp = dest.with_suffix(".part")
    try:
        with open(tmp, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True,
            desc=dest.name[:50], leave=False,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
        tmp.rename(dest)
        time.sleep(delay)
        return True
    except Exception as exc:
        log.error("Error guardando %s: %s", url, exc)
        tmp.unlink(missing_ok=True)
        return False


# ---------------------------------------------------------------------------
# Nombre de fichero único
# ---------------------------------------------------------------------------
def safe_filename(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name.replace(" ", "_") or "file"
    url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
    stem = Path(name).stem
    suffix = Path(name).suffix or ".bin"
    return f"{stem}_{url_hash}{suffix}"


# ---------------------------------------------------------------------------
# Estado persistente
# ---------------------------------------------------------------------------
def load_visited(state_file: Path) -> set:
    if not state_file.exists():
        return set()
    return set(state_file.read_text(encoding="utf-8").splitlines())


def save_visited(state_file: Path, visited: set):
    state_file.write_text("\n".join(sorted(visited)), encoding="utf-8")


# ---------------------------------------------------------------------------
# CSV incremental
# ---------------------------------------------------------------------------
class CsvWriter:
    def __init__(self, path: Path):
        is_new = not path.exists()
        self._fh = open(path, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(
            self._fh,
            fieldnames=["source_page", "file_url", "local_file", "extension"],
        )
        if is_new:
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
    parser = argparse.ArgumentParser(description="Scraper quenotelacuenten.org")
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--headed", action="store_true", help="Mostrar navegador")
    args = parser.parse_args()

    # Importar Playwright aquí para dar error claro si no está instalado
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: Playwright no está instalado.")
        print("Ejecuta:  pip install playwright  &&  playwright install chromium")
        return

    DEST_DIR.mkdir(exist_ok=True)
    base_domain = urlparse(BASE_URL).netloc
    visited = load_visited(STATE_FILE)
    queue = deque(url for url in SEED_URLS if url not in visited)
    session = requests.Session()
    csv_writer = CsvWriter(INDEX_FILE)

    pages_scraped = 0
    total_downloaded = 0

    log.info("Iniciando scraping de %s", BASE_URL)
    log.info("Formatos buscados: %s", ", ".join(sorted(BOOK_EXTENSIONS)))

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.headed)
        context = browser.new_context(
            user_agent=DOWNLOAD_HEADERS["User-Agent"],
            locale="es-ES",
        )
        page = context.new_page()

        try:
            while queue:
                if args.max_pages and pages_scraped >= args.max_pages:
                    log.info("Límite de %d páginas alcanzado.", args.max_pages)
                    break

                current_url = queue.popleft()
                if current_url in visited:
                    continue
                visited.add(current_url)

                log.info("[%d pág. | cola: %d] %s", pages_scraped, len(queue), current_url)

                try:
                    page.goto(current_url, wait_until="domcontentloaded", timeout=20000)
                    # Pequeña espera para que el JS inyecte los links
                    page.wait_for_timeout(1500)
                    html = page.content()
                except Exception as exc:
                    log.warning("Error cargando %s: %s", current_url, exc)
                    continue

                book_links, page_links = extract_links(html, current_url, base_domain)

                for link in page_links:
                    if link not in visited:
                        queue.append(link)

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
                        log.info("  + Descargado: %s", filename)

                    csv_writer.write({
                        "source_page": current_url,
                        "file_url": book_url,
                        "local_file": str(dest) if dest.exists() else "",
                        "extension": ext,
                    })

                pages_scraped += 1
                time.sleep(args.delay)

                if pages_scraped % 10 == 0:
                    save_visited(STATE_FILE, visited)

        except KeyboardInterrupt:
            log.info("Interrumpido por el usuario.")
        finally:
            browser.close()
            save_visited(STATE_FILE, visited)
            csv_writer.close()

    log.info("--- Fin ---")
    log.info("Páginas rastreadas : %d", pages_scraped)
    log.info("Archivos descargados: %d", total_downloaded)
    log.info("Índice guardado en : %s", INDEX_FILE)


if __name__ == "__main__":
    main()
