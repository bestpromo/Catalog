# Catalog Project

This project imports CSV files from Awin into a PostgreSQL catalog structure.

## Structure

- `src/Awin/data/`: Place the original Awin CSV files here. Subfolders:
  - `files/`: All CSVs to be imported.
  - `logs/`: Log files generated by the scripts.
  - `list/`: Stores the downloaded Awin main list.
- `src/db.py`: Centralized PostgreSQL connection using variables from `.env`.
- `src/Awin/DownloadOffers.py`: Downloads and extracts Awin product data and lists.
- `src/Awin/sql/CreateOrTruncateStructure.py`: Creates or truncates the temporary import table (`awin_temp_import_catalog`).
- `src/Awin/ImportCsv.py`: Imports all CSVs from `data/files` into the temporary table.
- `.env`: Environment variables (not versioned).

## How to use

1. Create a `.env` file in the root with your database credentials and Awin info:
    ```
    DB_DATABASE=yourdb
    DB_USER=youruser
    DB_PASSWORD=yourpassword
    DB_HOST=localhost
    DB_PORT=5432
    AWIN_API_KEY=your_awin_api_key
    AWIN_PUBLISHER_ID=your_awin_publisher_id
    AWIN_PARTNER_ID=your_partner_id
    BATCH_SIZE=5000
    ```

2. Install dependencies:
    ```
    pip install -r requirements.txt
    ```

3. Download and prepare Awin offers:
    ```
    python src/Awin/DownloadOffers.py
    ```
    This will:
    - Clean up all files in `src/Awin/data/files` and `src/Awin/data/list`
    - Download the main Awin list and filter active stores
    - Download all active store catalogs into `src/Awin/data/files`

4. Import CSVs into the database:
    ```
    python src/Awin/ImportCsv.py
    ```
    This will:
    - Run the table creation/truncation script
    - Import all CSVs from `src/Awin/data/files` into the `awin_temp_import_catalog` table

## Notes

- The `.env` file **MUST NOT** be versioned.
- The folder `src/Awin/data/` and its contents are ignored by git.
- All logs are saved in `src/Awin/data/logs/` with one file per day.
- The import process automatically fills the `partner_id` column with the value from `AWIN_PARTNER_ID` in `.env`.
- Adapt scripts as needed for changes in CSV layout or business rules.