import sys
import os
import logging
import time
from datetime import datetime
import psycopg2.extras
import json
from dotenv import load_dotenv
from collections import OrderedDict
import re
import io
from multiprocessing import Pool, current_process
import gc

LOG_DIR = os.path.join(os.path.dirname(__file__), "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"{datetime.now().strftime('%d%m%Y')}-ImportStaging.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S"
)

# Ajusta sys.path para importar connect_db corretamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from db import connect_db as get_connection

# Load .env variables
load_dotenv()
BATCH_SIZE = int(os.getenv("AWIN_IMPORT_BATCH_SIZE", 1000))  # Total de ofertas a processar por execução
MAX_PROCESSES = 8  # Limite de processos simultâneos

def parse_price(value):
    if not value:
        return None
    value = re.sub(r'[^\d,.-]', '', value)
    if value.count(',') == 1 and value.count('.') == 0:
        value = value.replace(',', '.')
    elif value.count('.') > 1 and value.count(',') == 1:
        value = value.replace('.', '').replace(',', '.')
    try:
        return float(value)
    except Exception:
        return None

def build_prices(row):
    currency = row.get("currency")
    search_price = parse_price(row.get("search_price"))
    product_price_old = parse_price(row.get("product_price_old"))
    rrp_price = parse_price(row.get("rrp_price"))
    if rrp_price is not None:
        return OrderedDict([
            ("currency", currency),
            ("price", search_price),
            ("promotional_price", rrp_price)
        ])
    if product_price_old is not None and product_price_old != search_price:
        return OrderedDict([
            ("currency", currency),
            ("price", product_price_old),
            ("promotional_price", search_price)
        ])
    if search_price is not None:
        return OrderedDict([
            ("currency", currency),
            ("price", search_price),
            ("promotional_price", None)
        ])
    return OrderedDict([
        ("currency", currency),
        ("price", None),
        ("promotional_price", None)
    ])

def build_images(row):
    images = []
    order_fields = [
        ('merchant_image_url', 'Main thumb'),
        ('large_image', 'Large image'),
        ('alternate_image', 'Alternative 1'),
        ('aw_thumb_url', 'AW thumb'),
        ('alternate_image_two', 'Alternative 2'),
        ('alternate_image_three', 'Alternative 3'),
        ('alternate_image_four', 'Alternative 4')
    ]
    has_merchant_image_url = bool(row.get('merchant_image_url'))
    has_aw_thumb_url = bool(row.get('aw_thumb_url'))
    main_image_field_set = False
    current_display_order = 0

    for field_name, alt_text in order_fields:
        url = row.get(field_name)
        if url:
            is_main = False
            display_idx = current_display_order
            if not main_image_field_set:
                if field_name == 'merchant_image_url' and has_merchant_image_url:
                    is_main = True
                    main_image_field_set = True
                elif field_name == 'aw_thumb_url' and not has_merchant_image_url and has_aw_thumb_url:
                    is_main = True
                    main_image_field_set = True
            if is_main:
                display_idx = 0
                if current_display_order > 0:
                    for img in images:
                        if not img["main_image"]:
                            img["display_order"] += 1
            images.append({
                "alt": alt_text,
                "url": url,
                "main_image": is_main,
                "display_order": display_idx
            })
            if not is_main:
                current_display_order += 1
    if main_image_field_set and any(img["main_image"] for img in images):
        temp_order_map = {f_name: i for i, (f_name, _) in enumerate(order_fields)}
        def sort_key(image_item):
            original_field_index = -1
            for field_key, (fname, _) in enumerate(order_fields):
                if row.get(fname) == image_item["url"]:
                    original_field_index = field_key
                    break
            return (not image_item["main_image"], image_item["display_order"], original_field_index)
        images.sort(key=sort_key)
        for i, img in enumerate(images):
            img["display_order"] = i
    elif not images:
        return None
    return images

def build_raw_data(row):
    raw = []
    for i in range(1, 10):
        key = f'custom_{i}'
        if row.get(key):
            raw.append({"name": key, "value": row[key]})
    return raw if raw else None

def build_attributes(row):
    attrs = []
    for attr in ['colour', 'fashion_suitable_for', 'fashion_category', 'fashion_size', 'fashion_material']:
        if row.get(attr):
            attrs.append({"name": attr, "value": row[attr]})
    return attrs if attrs else None

def build_logistics(row):
    logistics = []
    for attr in ['delivery_weight', 'warranty', 'delivery_time', 'delivery_cost']:
        if row.get(attr):
            logistics.append({"name": attr, "value": row[attr]})
    return logistics if logistics else None

def build_stock_qty(row):
    try:
        if row.get('stock_quantity'):
            return int(row['stock_quantity'])
    except Exception:
        return None
    return None

def get_partner_names(cur):
    cur.execute("SELECT id, nome FROM public.partners")
    partners = cur.fetchall()
    return {p['id']: p['nome'] for p in partners}

def fetch_rows(cur, batch_size, offset):
    # Deduplicação no SQL (CTE com ROW_NUMBER)
    cur.execute(f"""
        WITH ranked AS (
            SELECT
                id, partner_id, merchant_id, merchant_product_id, merchant_name, aw_deep_link,
                merchant_deep_link, product_name, condition, product_short_description, description,
                brand_name, product_type, merchant_category, category_name, merchant_product_category_path,
                merchant_product_second_category, search_price, product_price_old, rrp_price, currency,
                merchant_image_url, large_image, alternate_image, aw_thumb_url, alternate_image_two,
                alternate_image_three, alternate_image_four, custom_1, custom_2, custom_3, custom_4,
                custom_5, custom_6, custom_7, custom_8, custom_9, product_gtin, mpn, ean, isbn, upc,
                stock_quantity, data_feed_id
            FROM public.awin_catalog_import_temp
            WHERE imported = false
        ),
        deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY partner_id, merchant_id, merchant_product_id
                       ORDER BY data_feed_id DESC
                   ) as rn
            FROM ranked
        )
        SELECT * FROM deduped
        WHERE rn = 1
        ORDER BY id
        LIMIT %s OFFSET %s
    """, (batch_size, offset))
    return cur.fetchall()

def create_temp_keys_table(cur, offer_keys):
    cur.execute("""
        CREATE TEMP TABLE temp_keys (
            partner_id INT,
            merchant_id INT,
            offer_merchant_id TEXT
        ) ON COMMIT DROP;
    """)
    if offer_keys:
        args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode("utf-8") for x in offer_keys)
        cur.execute(f"INSERT INTO temp_keys (partner_id, merchant_id, offer_merchant_id) VALUES {args_str}")

def fetch_existing_offers(cur):
    cur.execute("""
        SELECT s.partner_id, s.merchant_id, s.offer_merchant_id, s.staging_id, s.error_process, s.processed, s.reason_error
        FROM public.import_offers_staging s
        JOIN temp_keys t
          ON s.partner_id = t.partner_id
         AND s.merchant_id = t.merchant_id
         AND s.offer_merchant_id = t.offer_merchant_id
    """)
    return {(r['partner_id'], r['merchant_id'], r['offer_merchant_id']): r for r in cur.fetchall()}

def is_true(val):
    return val is True or val == 1 or val == 't' or val == 'True' or val == 'true'

def is_false(val):
    return val is False or val == 0 or val == 'f' or val == 'False' or val == 'false'

def prepare_insert_and_update(rows, existing_dict, partner_names):
    insert_values = []
    insert_columns = None
    update_processed_values = []
    update_error_values = []
    skipped_count = 0
    updated_processed_count = 0
    updated_error_count = 0

    base_new_record_keys = [
        "created_at", "import_batch_id", "processed", "error_process", "reason_error",
        "processed_at", "partner_id", "partner_name", "merchant_id", "merchant_name",
        "deep_link_url", "status", "merchant_deep_link_url", "is_adult", "sku",
        "offer_title", "\"condition\"", "offer_short_description", "offer_full_description",
        "offer_merchant_id", "brand_name", "product_type", "\"attributes\"", "merchant_category",
        "merchant_category_name", "category_path", "category_path_secondary", "prices",
        "logistics", "stock_qty", "images", "raw_data", "gtin", "mpn", "ean",
        "isbn", "upc"
    ]
    update_field_names = [
        "partner_id", "partner_name", "merchant_id", "merchant_name", "deep_link_url",
        "status", "merchant_deep_link_url", "is_adult", "sku", "offer_title",
        "condition", "offer_short_description", "offer_full_description", "offer_merchant_id",
        "brand_name", "product_type", "attributes", "merchant_category", "merchant_category_name",
        "category_path", "category_path_secondary", "prices", "logistics", "stock_qty",
        "images", "raw_data", "gtin", "mpn", "ean", "isbn", "upc"
    ]

    for row in rows:
        partner_id = row.get('partner_id')
        merchant_id = row.get('merchant_id')
        offer_merchant_id = row.get('merchant_product_id')
        existing = existing_dict.get((partner_id, merchant_id, offer_merchant_id))
        partner_name = partner_names.get(partner_id)

        insert_data = OrderedDict()
        insert_data["partner_id"] = partner_id
        insert_data["partner_name"] = partner_name
        insert_data["merchant_id"] = merchant_id
        insert_data["merchant_name"] = row.get('merchant_name')
        insert_data["deep_link_url"] = row.get('aw_deep_link')
        insert_data["status"] = True
        insert_data["merchant_deep_link_url"] = row.get('merchant_deep_link')
        insert_data["is_adult"] = False
        insert_data["sku"] = None
        insert_data["offer_title"] = row.get('product_name')
        insert_data["condition"] = row.get('condition')
        insert_data["offer_short_description"] = row.get('product_short_description')
        insert_data["offer_full_description"] = row.get('description')
        insert_data["offer_merchant_id"] = offer_merchant_id
        insert_data["brand_name"] = row.get('brand_name')
        insert_data["product_type"] = row.get('product_type')
        insert_data["attributes"] = json.dumps(build_attributes(row))
        insert_data["merchant_category"] = row.get('merchant_category')
        insert_data["merchant_category_name"] = row.get('category_name')
        insert_data["category_path"] = row.get('merchant_product_category_path')
        insert_data["category_path_secondary"] = row.get('merchant_product_second_category')
        insert_data["prices"] = json.dumps(build_prices(row))
        insert_data["logistics"] = json.dumps(build_logistics(row))
        insert_data["stock_qty"] = build_stock_qty(row)
        insert_data["images"] = json.dumps(build_images(row))
        insert_data["raw_data"] = json.dumps(build_raw_data(row))
        insert_data["gtin"] = row.get('product_gtin')
        insert_data["mpn"] = row.get('mpn')
        insert_data["ean"] = row.get('ean')
        insert_data["isbn"] = row.get('isbn')
        insert_data["upc"] = row.get('upc')

        if existing:
            # Se processed = true
            if is_true(existing['processed']):
                staging_id = existing['staging_id']
                update_batch_values = [insert_data[k] for k in update_field_names] + [False, False, staging_id]
                update_processed_values.append(tuple(update_batch_values))
                updated_processed_count += 1
            # Se processed = false e error_process = true
            elif is_false(existing['processed']) and is_true(existing['error_process']):
                staging_id = existing['staging_id']
                update_batch_values = [insert_data[k] for k in update_field_names] + [False, False, staging_id]
                update_error_values.append(tuple(update_batch_values))
                updated_error_count += 1
            # Se processed = false e error_process = false: pula
            elif is_false(existing['processed']) and is_false(existing['error_process']):
                skipped_count += 1
                continue
            else:
                skipped_count += 1
                continue
        else:
            insert_data_full_ordered = OrderedDict()
            insert_data_full_ordered["created_at"] = row.get('created_at') or datetime.now()
            insert_data_full_ordered["import_batch_id"] = None
            insert_data_full_ordered["processed"] = False
            insert_data_full_ordered["error_process"] = False
            insert_data_full_ordered["reason_error"] = None
            insert_data_full_ordered["processed_at"] = None
            for key in update_field_names:
                insert_data_full_ordered[key] = insert_data[key]
            if insert_columns is None:
                insert_columns = base_new_record_keys
            insert_values.append(list(insert_data_full_ordered.values()))
    return (
        insert_values, insert_columns,
        update_processed_values, update_error_values,
        skipped_count, updated_processed_count, updated_error_count,
        update_field_names
    )

def batch_update_processed_records(cur, update_processed_values, update_field_names):
    if not update_processed_values:
        return 0
    update_set_parts = []
    for k in update_field_names:
        sql_col_name = f'"{k}"' if k in ["condition", "attributes"] else k
        update_set_parts.append(f"{sql_col_name} = %s")
    update_set_clause = ', '.join(update_set_parts)
    update_sql = f"""
        UPDATE public.import_offers_staging
        SET {update_set_clause},
            processed = %s,
            error_process = %s
        WHERE staging_id = %s
    """
    cur.executemany(update_sql, update_processed_values)
    return len(update_processed_values)

def batch_update_error_records(cur, update_error_values, update_field_names):
    if not update_error_values:
        return 0
    update_set_parts = []
    for k in update_field_names:
        sql_col_name = f'"{k}"' if k in ["condition", "attributes"] else k
        update_set_parts.append(f"{sql_col_name} = %s")
    update_set_clause = ', '.join(update_set_parts)
    update_sql = f"""
        UPDATE public.import_offers_staging
        SET {update_set_clause},
            processed = %s,
            error_process = %s
        WHERE staging_id = %s
    """
    cur.executemany(update_sql, update_error_values)
    return len(update_error_values)

def batch_insert_copy(cur, insert_values, insert_columns):
    if not insert_values or not insert_columns:
        return 0
    sio = io.StringIO()
    for record_tuple in insert_values:
        line_values = []
        for val in record_tuple:
            if val is None:
                line_values.append('\\N')
            elif isinstance(val, bool):
                line_values.append('t' if val else 'f')
            elif isinstance(val, str):
                line_values.append(
                    val.replace('\\', '\\\\')
                       .replace('\t', '\\t')
                       .replace('\n', '\\n')
                       .replace('\r', '\\r')
                )
            elif isinstance(val, datetime):
                line_values.append(val.strftime('%Y-%m-%d %H:%M:%S.%f'))
            else:
                line_values.append(str(val))
        sio.write('\t'.join(line_values) + '\n')
    sio.seek(0)
    try:
        copy_sql = f"COPY public.import_offers_staging ({', '.join(insert_columns)}) FROM STDIN WITH (FORMAT text, DELIMITER '\t', NULL '\\N')"
        cur.copy_expert(sql=copy_sql, file=sio)
        return len(insert_values)
    except Exception as e:
        logging.error(f"Erro real durante a operação COPY: {e}")
        cur.connection.rollback()
        return 0
    finally:
        sio.close()

def mark_imported(cur, ids):
    if ids:
        cur.execute("UPDATE public.awin_catalog_import_temp SET imported = true WHERE id = ANY(%s)", (ids,))

def process_batch(args):
    offset, batch_per_process = args
    start_time = time.time()
    logging.info(f"[{current_process().name}] Start batch at offset {offset} (limit {batch_per_process})")
    with get_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        partner_names = get_partner_names(cur)
        rows = fetch_rows(cur, batch_per_process, offset)
        logging.info(f"[{current_process().name}] Fetched {len(rows)} rows from awin_catalog_import_temp.")

        if not rows:
            logging.info(f"[{current_process().name}] No rows to process.")
            return

        offer_keys = [(row['partner_id'], row['merchant_id'], row['merchant_product_id']) for row in rows]
        create_temp_keys_table(cur, offer_keys)
        existing_dict = fetch_existing_offers(cur)

        (
            insert_values, insert_columns,
            update_processed_values, update_error_values,
            skipped_count, updated_processed_count, updated_error_count,
            update_field_names
        ) = prepare_insert_and_update(rows, existing_dict, partner_names)

        if update_processed_values:
            batch_update_processed_records(cur, update_processed_values, update_field_names)
        if update_error_values:
            batch_update_error_records(cur, update_error_values, update_field_names)
        inserted_count = batch_insert_copy(cur, insert_values, insert_columns)
        ids_to_mark_imported = [row['id'] for row in rows]
        mark_imported(cur, ids_to_mark_imported)
        conn.commit()

        # Libere listas após uso para liberar RAM
        del rows, insert_values, update_processed_values, update_error_values, existing_dict
        gc.collect()

    elapsed = time.time() - start_time
    logging.info(f"[{current_process().name}] Batch finished. Time: {elapsed:.2f}s. Inserted: {inserted_count}, Updated processed: {updated_processed_count}, Updated error: {updated_error_count}, Skipped: {skipped_count}.")

def main():
    # Conta total de registros únicos a processar
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM public.awin_catalog_import_temp
                WHERE imported = false
                GROUP BY partner_id, merchant_id, merchant_product_id
            ) t
        """)
        total = cur.fetchone()[0]
    if total == 0:
        logging.info("No records to process.")
        return

    # Calcula o tamanho do batch de cada processo para que o total não ultrapasse BATCH_SIZE
    num_procs = min(MAX_PROCESSES, (BATCH_SIZE + 1) // 1)  # nunca mais que MAX_PROCESSES
    batch_per_process = BATCH_SIZE // num_procs
    remainder = BATCH_SIZE % num_procs

    offsets = []
    limits = []
    for i in range(num_procs):
        offset = i * batch_per_process
        if i < remainder:
            batch_this = batch_per_process + 1
            offset += i  # distribui o resto
        else:
            batch_this = batch_per_process
            offset += remainder
        offsets.append((offset, batch_this))

    with Pool(processes=num_procs) as pool:
        pool.map(process_batch, offsets)

    logging.info("Batch finished.")

if __name__ == "__main__":
    # Lockfile só no processo principal!
    LOCKFILE = '/tmp/importstaging.lock'
    try:
        fd = os.open(LOCKFILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        print("Processo abortado: ImportStaging.py já está em execução.")
        sys.exit(0)

    import atexit
    atexit.register(lambda: os.path.exists(LOCKFILE) and os.remove(LOCKFILE))

    main()
