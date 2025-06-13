import sys
import os
import csv
import time
from datetime import datetime
from dotenv import load_dotenv

# Lockfile atômico para evitar execuções simultâneas (robusto para uso com cron)
LOCKFILE = '/tmp/importcsv.lock'
try:
    fd = os.open(LOCKFILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.write(fd, str(os.getpid()).encode())
    os.close(fd)
except FileExistsError:
    print("Processo abortado: ImportCsv.py já está em execução.")
    sys.exit(0)

import atexit
atexit.register(lambda: os.path.exists(LOCKFILE) and os.remove(LOCKFILE))

LOG_DIR = os.path.join(os.path.dirname(__file__), "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, f"{datetime.now():%d%m%Y}-importCsv.log")

def log(msg, icon="ℹ️"):
    log_line = f"{datetime.now():%d/%m/%Y %H:%M:%S} {icon} {msg}\n"
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(log_line)
    print(log_line, end="")

# Ajusta sys.path para importar connect_db corretamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from db import connect_db as get_connection

load_dotenv()

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50000))
AWIN_PARTNER_ID = os.getenv("AWIN_PARTNER_ID")
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(PROJECT_ROOT, 'data', 'files')
os.makedirs(CSV_DIR, exist_ok=True)

def format_elapsed(seconds):
    """Format elapsed time in a human-readable way."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {seconds}s"

def prepare_header(header):
    """Normalize header and ensure partner_id is present."""
    header = [col.replace(":", "_").lower() for col in header]
    if "partner_id" not in header:
        header = ["partner_id"] + header
    return header

def process_csv_file(filepath, conn, batch_size):
    """Process a single CSV file and import its data in batches."""
    start = time.time()
    log(f"Starting processing file: {os.path.basename(filepath)}", icon="📄")
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = prepare_header(next(reader))
        batch = []
        total = 0
        for row in reader:
            if "partner_id" not in header or len(row) == len(header) - 1:
                row = [AWIN_PARTNER_ID] + row

            # --- Ajuste para brand_name ---
            if "brand_name" in header:
                idx = header.index("brand_name")
                # Garante que a linha tem o mesmo número de colunas do header
                while len(row) < len(header):
                    row.append("")
                if not row[idx] or row[idx].strip() == "":
                    row[idx] = "Genérico"
            # --- Fim do ajuste ---

            batch.append(row)
            if len(batch) >= batch_size:
                copy_batch(conn, header, batch)
                total += len(batch)
                batch = []
        if batch:
            copy_batch(conn, header, batch)
            total += len(batch)
    elapsed = time.time() - start
    log(f"File {os.path.basename(filepath)} processed: {total} records in {format_elapsed(elapsed)}.", icon="✅")

def copy_batch(conn, header, batch):
    """Copy a batch of rows into the database using COPY."""
    from io import StringIO
    with conn.cursor() as cur:
        f = StringIO()
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(batch)
        f.seek(0)
        columns = ','.join([f'"{col}"' for col in header])
        cur.copy_expert(
            sql=f"COPY awin_catalog_import_temp ({columns}) FROM STDIN WITH CSV",
            file=f
        )
    conn.commit()

def prepare_table():
    """Run the script to create or truncate the import table."""
    sql_script_path = os.path.join(PROJECT_ROOT, 'sql', 'CreateOrTruncateStructure.py')
    log("Running CreateOrTruncateStructure.py to prepare table...", icon="🛠️")
    result = os.system(f"python3 {sql_script_path}")
    if result != 0:
        log("Erro ao executar CreateOrTruncateStructure.py", icon="❗")

def main():
    start_total = time.time()
    log("Process started.", icon="🚀")
    prepare_table()

    files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
    log(f"{len(files)} CSV files found to process.", icon="📦")
    with get_connection() as conn:
        for idx, filename in enumerate(files, 1):
            filepath = os.path.join(CSV_DIR, filename)
            log(f"({idx}/{len(files)}) Processing file: {filename}", icon="🔄")
            process_csv_file(filepath, conn, BATCH_SIZE)
    elapsed_total = time.time() - start_total
    log(f"Process finished. Total time: {format_elapsed(elapsed_total)}.", icon="🏁")

if __name__ == "__main__":
    main()
