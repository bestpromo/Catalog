import sys
import os
import subprocess
from datetime import datetime

def is_download_running():
    result = subprocess.run(
        ["pgrep", "-f", "DownloadOffers.py"],
        stdout=subprocess.PIPE
    )
    return result.returncode == 0

# Diretórios e configurações para log
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
LOGS_DIR = os.path.join(DATA_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOGS_DIR, f"{datetime.now().strftime('%d%m%Y')}-downloadOffers.log")

def log(msg, level="INFO"):
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    line = f"[{level}] {msg} ({now})"
    print(line)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")

if is_download_running():
    log("Processo abortado: DownloadOffers.py já está em execução.", level="WARNING")
    sys.exit(0)

# --- Carregue o .env ANTES de acessar as variáveis ---
from dotenv import load_dotenv
load_dotenv()

import csv
import gzip
import shutil
import requests

from datetime import datetime

# Diretórios e configurações
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
LISTS_DIR = os.path.join(DATA_DIR, 'list')
FILES_DIR = os.path.join(DATA_DIR, 'files')
LOGS_DIR = os.path.join(DATA_DIR, 'logs')

os.makedirs(LISTS_DIR, exist_ok=True)
os.makedirs(FILES_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

AWIN_API_KEY = os.getenv("AWIN_API_KEY")
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_LIST_URL = f"https://ui.awin.com/productdata-darwin-download/publisher/{AWIN_PUBLISHER_ID}/{AWIN_API_KEY}/1/feedList"
TODAY = datetime.now().strftime("%d-%m-%Y")
LIST_FILENAME = f"{TODAY}-datafeed.csv"
LIST_PATH = os.path.join(LISTS_DIR, LIST_FILENAME)
LOG_PATH = os.path.join(LOGS_DIR, f"{datetime.now().strftime('%d%m%Y')}-downloadOffers.log")

def log(msg, level="INFO"):
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    line = f"[{level}] {msg} ({now})"
    print(line)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")

def download_and_extract(url, dest_csv):
    gz_path = dest_csv + ".gz"
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(gz_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)
    with gzip.open(gz_path, 'rb') as f_in, open(dest_csv, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(gz_path)

def filter_active_stores(csv_path):
    active_stores = []
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row.get("Membership Status", "").strip().lower() == "active":
                active_stores.append(row)
    return active_stores

def download_store_catalogs(stores):
    from collections import defaultdict
    advertiser_count = defaultdict(int)
    for store in stores:
        advertiser_id = store["Advertiser ID"].strip()
        advertiser_count[advertiser_id] += 1

    advertiser_index = defaultdict(int)

    for store in stores:
        advertiser_id = store["Advertiser ID"].strip()
        advertiser_name = store["Advertiser Name"].strip().replace(" ", "_")
        url = store["URL"].strip()
        advertiser_index[advertiser_id] += 1
        count = advertiser_count[advertiser_id]

        if count == 1:
            file_name = f"{datetime.now().strftime('%d%m%Y')}-{advertiser_id}-{advertiser_name}.csv"
        else:
            file_name = f"{datetime.now().strftime('%d%m%Y')}-{advertiser_id}-{advertiser_name}-{advertiser_index[advertiser_id]}.csv"

        dest_csv = os.path.join(FILES_DIR, file_name)
        try:
            log(f"Downloading catalog: {advertiser_id} - {advertiser_name} ({advertiser_index[advertiser_id]}/{count})" if count > 1 else f"Downloading catalog: {advertiser_id} - {advertiser_name}")
            download_and_extract(url, dest_csv)
            log(f"Catalog saved at: {dest_csv}")
        except Exception as e:
            log(f"Error downloading catalog {advertiser_id} - {advertiser_name}: {e}", level="ERROR")

def format_time(seconds):
    minutes = int(seconds // 60)
    hours = int(minutes // 60)
    minutes = minutes % 60
    if hours > 0:
        return f"{hours}h {minutes}min"
    else:
        return f"{minutes}min"

def clear_all_files(directory):
    """Delete all files in the given directory (non-recursive)."""
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                log(f"Deleted old file: {file_path}")
            except Exception as e:
                log(f"Error deleting file {file_path}: {e}", level="ERROR")

def main():
    start = datetime.now()
    log("Starting AWIN ingestion process")

    # Remove ALL old files (not just .csv)
    clear_all_files(FILES_DIR)
    clear_all_files(LISTS_DIR)

    # 1. Download and extract the main list
    try:
        log("Downloading AWIN main list...")
        download_and_extract(AWIN_LIST_URL, LIST_PATH)
        log(f"Main list file downloaded and extracted: {LIST_PATH}")
    except Exception as e:
        log(f"Error downloading/extracting main list: {e}", level="ERROR")
        return

    # 2. Filter active stores
    active_stores = filter_active_stores(LIST_PATH)
    log(f"Active stores found: {len(active_stores)}")

    # 3. Download the catalogs of active stores
    download_store_catalogs(active_stores)

    end = datetime.now()
    total_time = (end - start).total_seconds()
    formatted_time = format_time(total_time)
    log("Process finished")
    log(f"Total execution time: {formatted_time}")

if __name__ == "__main__":
    main()