"""
Reintenta descargar los archivos fallidos del índice CSV.

Uso:
    python reintentar_fallidos.py [--csv index_libros.csv] [--delay 2]

Qué hace:
  1. Lee el CSV y localiza filas con local_file vacío (fallidos)
     MAS filas donde local_file tiene ruta pero el fichero no existe en disco
  2. Para URLs de archive.org (ia9xxxxx / ia8xxxxx), convierte al formato
     canónico  https://archive.org/download/<id>/<fichero>  que evita
     los servidores directos caídos
  3. Descarga con circuit breaker y timeout corto
  4. Actualiza el CSV al terminar
"""

import argparse
import hashlib
import logging
import re
import time
from pathlib import Path
from urllib.parse import urlparse, unquote

import pandas as pd
import requests
from tqdm import tqdm

# ---------------------------------------------------------------------------
DEST_DIR = Path("pdfs_downloaded")
CONNECT_TIMEOUT = 8
READ_TIMEOUT = 90
MAX_RETRIES = 3
BACKOFF_BASE = 2
CIRCUIT_BREAKER_THRESHOLD = 3

DOWNLOAD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_host_failures: dict = {}

# ---------------------------------------------------------------------------
# Conversión de URLs directas de archive.org al formato canónico
# ---------------------------------------------------------------------------
# URL directa:  https://ia902507.us.archive.org/6/items/ColeccionJauja/archivo.pdf
# URL canónica: https://archive.org/download/ColeccionJauja/archivo.pdf
_ARCHIVE_DIRECT = re.compile(
    r"https?://ia\d+[a-z]?\d*\.us\.archive\.org/\d+/items/([^/]+)/(.+)"
)

def to_canonical_archive_url(url: str) -> str:
    m = _ARCHIVE_DIRECT.match(url)
    if m:
        item_id, filename = m.group(1), m.group(2)
        return f"https://archive.org/download/{item_id}/{filename}"
    # URL genérica archive.org/download/... ya es canónica
    if "archive.org" in url and "/download/" in url:
        return url
    return url


# ---------------------------------------------------------------------------
# Nombre de fichero único (mismo algoritmo que el scraper principal)
# ---------------------------------------------------------------------------
def safe_filename(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name.replace(" ", "_") or "file"
    url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
    stem = Path(name).stem
    suffix = Path(name).suffix or ".bin"
    return f"{stem}_{url_hash}{suffix}"


# ---------------------------------------------------------------------------
# Descarga con circuit breaker
# ---------------------------------------------------------------------------
def download_file(session: requests.Session, url: str, dest: Path, delay: float) -> bool:
    if dest.exists():
        log.debug("Ya existe: %s", dest.name)
        return True

    host = urlparse(url).netloc
    if _host_failures.get(host, 0) >= CIRCUIT_BREAKER_THRESHOLD:
        log.warning("Host bloqueado (circuit breaker): %s", host)
        return False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(
                url, stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                headers=DOWNLOAD_HEADERS,
            )
            resp.raise_for_status()
            _host_failures[host] = 0
            break
        except requests.RequestException as exc:
            _host_failures[host] = _host_failures.get(host, 0) + 1
            if attempt == MAX_RETRIES or _host_failures[host] >= CIRCUIT_BREAKER_THRESHOLD:
                log.warning("Fallo definitivo %s: %s", url, exc)
                return False
            wait = BACKOFF_BASE ** attempt
            log.debug("Reintento %d en %ds...", attempt + 1, wait)
            time.sleep(wait)
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
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Reintenta descargas fallidas")
    parser.add_argument("--csv", default="index_libros.csv")
    parser.add_argument("--delay", type=float, default=1.5)
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        log.error("No se encuentra %s", csv_path)
        return

    DEST_DIR.mkdir(exist_ok=True)
    df = pd.read_csv(csv_path)

    # Filas a reintentar:
    # a) local_file vacío o NaN (fallidas)
    # b) local_file tiene ruta pero el fichero no existe en disco
    mask_empty = df["local_file"].isna() | (df["local_file"] == "")
    mask_missing = df["local_file"].notna() & (df["local_file"] != "") & \
                   df["local_file"].apply(lambda p: not Path(p).exists())

    pendientes = df[mask_empty | mask_missing].copy()
    log.info("Pendientes en CSV    : %d (vacíos: %d, ruta rota: %d)",
             len(pendientes), mask_empty.sum(), mask_missing.sum())

    if pendientes.empty:
        log.info("No hay nada pendiente. Todo descargado.")
        return

    session = requests.Session()
    ok = 0
    fail = 0

    for idx, row in pendientes.iterrows():
        original_url = str(row["file_url"])
        canonical_url = to_canonical_archive_url(original_url)

        if canonical_url != original_url:
            log.info("Archive.org canónico: %s", canonical_url)

        filename = safe_filename(original_url)
        dest = DEST_DIR / filename

        success = download_file(session, canonical_url, dest, args.delay)

        if success:
            df.at[idx, "local_file"] = str(dest)
            ok += 1
            log.info("  + OK: %s", filename)
        else:
            fail += 1

    # Guardar CSV actualizado
    df.to_csv(csv_path, index=False)

    log.info("--- Fin ---")
    log.info("Descargados ahora : %d", ok)
    log.info("Siguen fallando   : %d", fail)
    log.info("CSV actualizado   : %s", csv_path)


if __name__ == "__main__":
    main()
