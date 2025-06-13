import sys 
import os  
import logging
import time
from datetime import datetime
import json
import re
from unidecode import unidecode 
import uuid
from io import StringIO
from multiprocessing import Pool, current_process
import gc

# --- Path Setup para importar 'db' ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from db import connect_db as get_connection
except ImportError:
    print("CRITICAL: Could not import 'connect_db' from 'db.py'. Ensure db.py is in Batches/Catalog/src/ and sys.path is correct.")
    sys.exit(1)

import psycopg2
import psycopg2.extras

BATCH_SIZE = 500000
MAX_PROCESSES = 10

LOG_TYPES = ["error", "warning", "critical", "qty"]
LOG_DIR = os.path.join(os.path.dirname(__file__), "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"{datetime.now().strftime('%d%m%Y')}-ProcessStagingOffers.log")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] - %(filename)s:%(lineno)d - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

def custom_log(level, msg, *args, **kwargs):
    level = level.lower()
    if level in LOG_TYPES:
        if level == "qty":
            logging.info(f"[QTY][{current_process().name}] {msg}", *args, **kwargs)
        else:
            getattr(logging, level)(f"[{current_process().name}] {msg}", *args, **kwargs)

def copy_to_table(cur, table, columns, rows, truncate_map=None):
    if not rows:
        return
    def sanitize(val):
        if val is None:
            return ""
        return str(val).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', "'")
    f = StringIO()
    for row in rows:
        if truncate_map:
            row = tuple(
                (sanitize(v)[:truncate_map.get(idx, None)] if v is not None and idx in truncate_map else sanitize(v))
                for idx, v in enumerate(row)
            )
        else:
            row = tuple(sanitize(v) for v in row)
        f.write('\t'.join(row) + '\n')
    f.seek(0)
    columns_str = ','.join(columns)
    sql = f"COPY {table} ({columns_str}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')"
    cur.copy_expert(sql, f)

def normalize_for_slug(text):
    text = unidecode(str(text)).lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')

def build_store_cache(cur, staging_records):
    # Estratégia 4: Buscar apenas colunas necessárias
    unique_stores = set(
        (r['partner_id'], r['merchant_id'], r.get('merchant_name', f"Store_{r['partner_id']}_{r['merchant_id']}"))
        for r in staging_records
        if r.get('partner_id') and r.get('merchant_id')
    )
    if not unique_stores:
        return {}
    cur.execute("CREATE TEMP TABLE tmp_store (partner_id integer, merchant_id integer, name text) ON COMMIT DROP")
    copy_to_table(cur, "tmp_store", ["partner_id", "merchant_id", "name"], list(unique_stores))
    cur.execute("""
        INSERT INTO public.stores (partner_id, partner_merchant_id, name)
        SELECT partner_id, merchant_id, name FROM tmp_store
        ON CONFLICT (partner_id, partner_merchant_id) DO UPDATE SET name = EXCLUDED.name
    """)
    cur.connection.commit()
    cur.execute(
        "SELECT partner_id, partner_merchant_id, store_id FROM public.stores WHERE (partner_id, partner_merchant_id) IN %s",
        (tuple((pid, mid) for pid, mid, _ in unique_stores),)
    )
    return { (row['partner_id'], row['partner_merchant_id']): row['store_id'] for row in cur.fetchall() }

def build_offer_staging_rows(staging_records, brands_cache, stores_cache, cur=None):
    rows = []
    slug_set = set()
    batch_slugs = set()
    for r in staging_records:
        offer_title = r.get('offer_title')
        offer_merchant_id = r.get('offer_merchant_id')
        staging_id = r.get('staging_id')
        slug_parts = []
        if offer_title:
            slug_parts.append(offer_title)
        if offer_merchant_id:
            slug_parts.append(str(offer_merchant_id))
        base_slug_text = " ".join(slug_parts) if slug_parts else f"offer-staging-{staging_id}"
        original_slug_candidate = normalize_for_slug(base_slug_text) or "offer-" + str(uuid.uuid4())[:8]
        batch_slugs.add(original_slug_candidate[:255])
    existing_slugs = set()
    if cur and batch_slugs:
        cur.execute("SELECT url_slug FROM public.catalog_offer WHERE url_slug = ANY(%s)", (list(batch_slugs),))
        existing_slugs = set(row['url_slug'] for row in cur.fetchall())
    for r in staging_records:
        partner_id = r.get('partner_id')
        merchant_id = r.get('merchant_id')
        offer_merchant_id = r.get('offer_merchant_id')
        offer_title = r.get('offer_title')
        offer_short_description = r.get('offer_short_description')
        offer_full_description = r.get('offer_full_description')
        brand_name = r.get('brand_name')
        brand_id = brands_cache.get(brand_name)
        condition_val = str(r.get('condition', 'new')).lower()
        if condition_val not in ['new', 'used', 'refurbished']:
            condition_val = 'new'
        status_val = 'active' if r.get('status') is True else 'inactive'
        if status_val not in ['active', 'inactive', 'draft', 'out_of_stock']:
            status_val = 'inactive'
        gtin = r.get('gtin')
        mpn = r.get('mpn')
        deep_link_url = r.get('deep_link_url')
        is_adult = r.get('is_adult', False)
        sku = r.get('sku')
        staging_id = r.get('staging_id')
        created_at = r.get('created_at', datetime.now())
        store_id = stores_cache.get((partner_id, merchant_id))

        slug_parts = []
        if offer_title:
            slug_parts.append(offer_title)
        if offer_merchant_id:
            slug_parts.append(str(offer_merchant_id))
        base_slug_text = " ".join(slug_parts) if slug_parts else f"offer-staging-{staging_id}"
        original_slug_candidate = normalize_for_slug(base_slug_text) or "offer-" + str(uuid.uuid4())[:8]
        slug_candidate = original_slug_candidate[:255]
        attempt = 1
        while slug_candidate in slug_set or slug_candidate in existing_slugs:
            slug_candidate = (original_slug_candidate + f"-{attempt}")[:255]
            attempt += 1
        slug_set.add(slug_candidate)
        rows.append((
            partner_id,
            merchant_id,
            offer_merchant_id,
            offer_title,
            offer_short_description,
            offer_full_description,
            brand_id,
            condition_val,
            gtin,
            mpn,
            deep_link_url,
            is_adult,
            sku,
            status_val,
            slug_candidate,
            created_at,
            staging_id,
            store_id
        ))
    return rows

def process_offer_images_bulk(cur, offers_images_dict):
    all_images_to_insert = []
    for offer_id, images_json in offers_images_dict.items():
        if not images_json:
            continue
        try:
            images_data = json.loads(images_json) if isinstance(images_json, str) else images_json
        except Exception:
            continue
        if not isinstance(images_data, list):
            continue
        for i, img_obj in enumerate(images_data):
            if isinstance(img_obj, dict):
                image_link = img_obj.get('image_url') or img_obj.get('url')
                alt_text = img_obj.get('alt_text') or img_obj.get('alt')
                main_image = img_obj.get('main_image', False)
                display_order = img_obj.get('display_order', i)
                if image_link:
                    all_images_to_insert.append((
                        str(offer_id),
                        image_link,
                        alt_text,
                        main_image,
                        display_order
                    ))
    if all_images_to_insert:
        cur.execute("CREATE TEMP TABLE tmp_catalog_offer_image (offer_id uuid, image_url text, alt_text text, main_image boolean, display_order int) ON COMMIT DROP")
        copy_to_table(
            cur,
            "tmp_catalog_offer_image",
            ["offer_id", "image_url", "alt_text", "main_image", "display_order"],
            all_images_to_insert
        )
        cur.execute("""
            INSERT INTO public.catalog_offer_image (offer_id, image_url, alt_text, main_image, display_order)
            SELECT offer_id, image_url, alt_text, main_image, display_order FROM tmp_catalog_offer_image
        """)

def process_offer_pricing_bulk(cur, offers_pricing_dict):
    MAX_ALLOWED = 99999999.99  # numeric(10,2)
    def safe_price(val):
        try:
            f = float(val)
            if abs(f) > MAX_ALLOWED:
                return MAX_ALLOWED if f > 0 else -MAX_ALLOWED
            return round(f, 2)
        except Exception:
            return None
    all_prices_to_upsert = []
    for offer_id, prices_json in offers_pricing_dict.items():
        if not prices_json:
            continue
        try:
            pricing_data = json.loads(prices_json) if isinstance(prices_json, str) else prices_json
        except Exception:
            continue
        if not isinstance(pricing_data, dict):
            continue
        sale_price = safe_price(pricing_data.get('price') or pricing_data.get('sale_price'))
        promotional_price = safe_price(pricing_data.get('promotional_price'))
        original_price = safe_price(pricing_data.get('original_price'))
        if sale_price is None:
            continue
        all_prices_to_upsert.append((
            str(offer_id),
            sale_price,
            promotional_price,
            original_price,
            pricing_data.get('currency', 'BRL'),
            datetime.now()
        ))
    if all_prices_to_upsert:
        cur.execute("CREATE TEMP TABLE tmp_catalog_offer_pricing (offer_id uuid, sale_price numeric, promotional_price numeric, original_price numeric, currency text, updated_at timestamp) ON COMMIT DROP")
        copy_to_table(
            cur,
            "tmp_catalog_offer_pricing",
            ["offer_id", "sale_price", "promotional_price", "original_price", "currency", "updated_at"],
            all_prices_to_upsert
        )
        cur.execute("""
            INSERT INTO public.catalog_offer_pricing 
                (offer_id, sale_price, promotional_price, original_price, currency, updated_at)
            SELECT offer_id, sale_price, promotional_price, original_price, currency, updated_at
            FROM tmp_catalog_offer_pricing
            ON CONFLICT (offer_id) DO UPDATE SET
                sale_price = EXCLUDED.sale_price,
                promotional_price = EXCLUDED.promotional_price,
                original_price = EXCLUDED.original_price,
                currency = EXCLUDED.currency,
                updated_at = CURRENT_TIMESTAMP;
        """)

def process_offer_stock_bulk(cur, offers_stock_dict):
    all_stock_to_upsert = []
    for offer_id, stock_qty in offers_stock_dict.items():
        quantity = stock_qty if stock_qty is not None else 0
        all_stock_to_upsert.append((str(offer_id), quantity, datetime.now()))
    if all_stock_to_upsert:
        cur.execute("CREATE TEMP TABLE tmp_catalog_offer_stock (offer_id uuid, quantity int, updated_at timestamp) ON COMMIT DROP")
        copy_to_table(
            cur,
            "tmp_catalog_offer_stock",
            ["offer_id", "quantity", "updated_at"],
            all_stock_to_upsert
        )
        cur.execute("""
            INSERT INTO public.catalog_offer_stock (offer_id, quantity, updated_at)
            SELECT offer_id, quantity, updated_at FROM tmp_catalog_offer_stock
            ON CONFLICT (offer_id) DO UPDATE SET
                quantity = EXCLUDED.quantity,
                updated_at = CURRENT_TIMESTAMP;
        """)

def process_offer_logistics_bulk(cur, offers_logistics_dict):
    all_logistics_to_upsert = []
    for offer_id, logistics_json in offers_logistics_dict.items():
        if not logistics_json:
            continue
        try:
            logistics_data = json.loads(logistics_json) if isinstance(logistics_json, str) else logistics_json
        except Exception:
            continue
        if not isinstance(logistics_data, dict):
            continue
        all_logistics_to_upsert.append((
            str(offer_id),
            logistics_data.get('weight'),
            logistics_data.get('height'),
            logistics_data.get('width'),
            logistics_data.get('depth'),
            logistics_data.get('package_weight'),
            logistics_data.get('barcode'),
            logistics_data.get('fragile_offer', False),
            logistics_data.get('free_shipping', False),
            logistics_data.get('processing_time', 1),
            datetime.now()
        ))
    if all_logistics_to_upsert:
        cur.execute("CREATE TEMP TABLE tmp_catalog_logistics_info (offer_id uuid, weight numeric, height numeric, width numeric, depth numeric, package_weight numeric, barcode text, fragile_offer boolean, free_shipping boolean, processing_time int, updated_at timestamp) ON COMMIT DROP")
        copy_to_table(
            cur,
            "tmp_catalog_logistics_info",
            ["offer_id", "weight", "height", "width", "depth", "package_weight", "barcode", "fragile_offer", "free_shipping", "processing_time", "updated_at"],
            all_logistics_to_upsert
        )
        cur.execute("""
            INSERT INTO public.catalog_logistics_info (
                offer_id, weight, height, width, depth, package_weight, barcode, 
                fragile_offer, free_shipping, processing_time, updated_at
            )
            SELECT offer_id, weight, height, width, depth, package_weight, barcode, fragile_offer, free_shipping, processing_time, updated_at
            FROM tmp_catalog_logistics_info
            ON CONFLICT (offer_id) DO UPDATE SET
                weight = EXCLUDED.weight, height = EXCLUDED.height, width = EXCLUDED.width, depth = EXCLUDED.depth,
                package_weight = EXCLUDED.package_weight, barcode = EXCLUDED.barcode, fragile_offer = EXCLUDED.fragile_offer,
                free_shipping = EXCLUDED.free_shipping, processing_time = EXCLUDED.processing_time, updated_at = CURRENT_TIMESTAMP;
        """)

def build_attribute_caches(cur, offers_attributes_dict):
    all_attr_names = set()
    all_attr_value_pairs = set()
    # 1. Coleta todos os nomes e valores únicos
    for attributes_json in offers_attributes_dict.values():
        if not attributes_json:
            continue
        try:
            attributes_data = json.loads(attributes_json) if isinstance(attributes_json, str) else attributes_json
        except Exception:
            continue
        if isinstance(attributes_data, dict):
            items = attributes_data.items()
        elif isinstance(attributes_data, list):
            items = ((item.get('name'), item.get('value')) for item in attributes_data if isinstance(item, dict))
        else:
            continue
        for name, value in items:
            if name and value:
                all_attr_names.add(str(name))
                all_attr_value_pairs.add((str(name), str(value)[:255]))
    # 2. Bulk insert nomes de atributos
    attribute_names_cache = {}
    if all_attr_names:
        cur.execute("CREATE TEMP TABLE tmp_catalog_attribute_name (name text) ON COMMIT DROP")
        copy_to_table(cur, "tmp_catalog_attribute_name", ["name"], [(n,) for n in all_attr_names])
        cur.execute("""
            INSERT INTO public.catalog_attribute_name (name)
            SELECT name FROM tmp_catalog_attribute_name
            ON CONFLICT (name) DO NOTHING
        """)
        cur.connection.commit()
        cur.execute("SELECT name, attribute_id FROM public.catalog_attribute_name WHERE name = ANY(%s)", (list(all_attr_names),))
        for row in cur.fetchall():
            attribute_names_cache[row['name']] = row['attribute_id']
    # 3. Bulk insert valores de atributos
    attribute_values_cache = {}
    unique_attr_id_value_pairs = set()
    for name, value in all_attr_value_pairs:
        attr_id = attribute_names_cache.get(name)
        if attr_id:
            unique_attr_id_value_pairs.add((attr_id, value))
    if unique_attr_id_value_pairs:
        cur.execute("CREATE TEMP TABLE tmp_catalog_attribute_value (attribute_id uuid, value varchar(255)) ON COMMIT DROP")
        copy_to_table(cur, "tmp_catalog_attribute_value", ["attribute_id", "value"], list(unique_attr_id_value_pairs))
        cur.execute("""
            INSERT INTO public.catalog_attribute_value (attribute_id, value)
            SELECT attribute_id, value FROM tmp_catalog_attribute_value
            ON CONFLICT (attribute_id, value) DO NOTHING
        """)
        cur.connection.commit()
        attr_ids = [p[0] for p in unique_attr_id_value_pairs]
        attr_vals = [p[1] for p in unique_attr_id_value_pairs]
        cur.execute("""
            SELECT attribute_id, value, value_id
            FROM public.catalog_attribute_value
            WHERE (attribute_id, value) IN (
                SELECT unnest(%s::uuid[]), unnest(%s::varchar[])
            )
        """, (attr_ids, attr_vals))
        for row in cur.fetchall():
            attribute_values_cache[(row['attribute_id'], row['value'])] = row['value_id']
    return attribute_names_cache, attribute_values_cache

def process_offer_attributes_bulk(cur, offers_attributes_dict, attribute_names_cache, attribute_values_cache):
    all_attrs_to_insert = []
    for offer_id, attributes_json in offers_attributes_dict.items():
        if not attributes_json:
            continue
        try:
            attributes_data = json.loads(attributes_json) if isinstance(attributes_json, str) else attributes_json
        except Exception:
            continue
        # Suporta tanto dict quanto lista de dicts
        if isinstance(attributes_data, dict):
            items = attributes_data.items()
        elif isinstance(attributes_data, list):
            items = ((item.get('name'), item.get('value')) for item in attributes_data if isinstance(item, dict))
        else:
            continue
        for name, value in items:
            if not name or not value:
                continue
            attr_id = attribute_names_cache.get(str(name))
            value_id = attribute_values_cache.get((attr_id, str(value)[:255]))
            if attr_id and value_id:
                all_attrs_to_insert.append((str(offer_id), attr_id, value_id))
    if all_attrs_to_insert:
        cur.execute("""
            CREATE TEMP TABLE tmp_catalog_offer_attribute (
                offer_id uuid,
                attribute_id uuid,
                value_id uuid
            ) ON COMMIT DROP
        """)
        copy_to_table(
            cur,
            "tmp_catalog_offer_attribute",
            ["offer_id", "attribute_id", "value_id"],
            all_attrs_to_insert
        )
        cur.execute("""
            INSERT INTO public.catalog_offer_attribute (offer_id, attribute_id, value_id)
            SELECT offer_id, attribute_id, value_id FROM tmp_catalog_offer_attribute
            ON CONFLICT (offer_id, attribute_id) DO UPDATE SET value_id = EXCLUDED.value_id
        """)

def process_batch(staging_ids):
    # Recebe uma lista de staging_ids para processar
    script_start_time = time.time()
    conn = None
    total_processed_successfully = 0
    total_failed = 0
    brands_cache = {}
    stores_cache = {}
    try:
        conn = get_connection()
        if conn is None:
            custom_log("critical", "Failed to connect to the database. Exiting.")
            return
        errored_staging_ids = set()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if not staging_ids:
                return

            # Buscar apenas colunas necessárias para processamento
            cur.execute("""
                SELECT staging_id, partner_id, merchant_id, offer_merchant_id, offer_title, offer_short_description,
                       offer_full_description, brand_name, condition, gtin, mpn, deep_link_url, is_adult, sku, status,
                       created_at, images, prices, stock_qty, logistics, attributes
                FROM public.import_offers_staging
                WHERE staging_id = ANY(%s)
            """, (staging_ids,))
            staging_records = cur.fetchall()
            if not staging_records:
                return

            # --- Pré-processamento de caches em memória (brands, stores) ---
            unique_brand_names = list(set(r['brand_name'] for r in staging_records if r.get('brand_name')))
            if unique_brand_names:
                missing_brands = [name for name in unique_brand_names if name not in brands_cache]
                if missing_brands:
                    insert_brand_tuples = [(name,) for name in missing_brands]
                    cur.execute("CREATE TEMP TABLE tmp_catalog_brand (name text) ON COMMIT DROP")
                    copy_to_table(cur, "tmp_catalog_brand", ["name"], insert_brand_tuples)
                    cur.execute("""
                        INSERT INTO public.catalog_brand (name)
                        SELECT name FROM tmp_catalog_brand
                        ON CONFLICT (name) DO NOTHING
                    """)
                    cur.connection.commit()
                    cur.execute("SELECT name, brand_id FROM public.catalog_brand WHERE name = ANY(%s)", (missing_brands,))
                    for row in cur.fetchall():
                        brands_cache[row['name']] = row['brand_id']

            # --- PRÉ-PROCESSAMENTO DE STORES ---
            stores_cache = build_store_cache(cur, staging_records)

            # --- Bulk upsert em staging para ofertas ---
            offer_rows = build_offer_staging_rows(staging_records, brands_cache, stores_cache, cur)
            cur.execute("""
                CREATE TEMP TABLE tmp_catalog_offer (
                    partner_id integer,
                    merchant_id integer,
                    merchant_offer_id text,
                    title text,
                    short_description text,
                    full_description text,
                    brand_id uuid,
                    condition text,
                    offer_gtin text,
                    mpn text,
                    deep_link_url text,
                    adult boolean,
                    sku text,
                    status text,
                    url_slug text,
                    created_at timestamp,
                    staging_id integer,
                    store_id uuid
                ) ON COMMIT DROP
            """)
            copy_to_table(
                cur,
                "tmp_catalog_offer",
                [
                    "partner_id", "merchant_id", "merchant_offer_id", "title", "short_description",
                    "full_description", "brand_id", "condition", "offer_gtin", "mpn", "deep_link_url",
                    "adult", "sku", "status", "url_slug", "created_at", "staging_id", "store_id"
                ],
                offer_rows
            )
            step_time = time.time()
            try:
                cur.execute("""
                    WITH ins AS (
                        INSERT INTO public.catalog_offer (
                            partner_id, merchant_id, merchant_offer_id, title, short_description,
                            full_description, brand_id, "condition", offer_gtin, mpn, deep_link_url,
                            adult, sku, status, url_slug, created_at, updated_at, store_id
                        )
                        SELECT partner_id, merchant_id, merchant_offer_id, title, short_description,
                            full_description, brand_id, condition, offer_gtin, mpn, deep_link_url,
                            adult, sku, status, url_slug, created_at, CURRENT_TIMESTAMP, store_id
                        FROM tmp_catalog_offer
                        ON CONFLICT (partner_id, merchant_id, merchant_offer_id) DO UPDATE SET
                            title = EXCLUDED.title, short_description = EXCLUDED.short_description,
                            full_description = EXCLUDED.full_description, brand_id = EXCLUDED.brand_id,
                            "condition" = EXCLUDED."condition", offer_gtin = EXCLUDED.offer_gtin, mpn = EXCLUDED.mpn,
                            deep_link_url = EXCLUDED.deep_link_url, adult = EXCLUDED.adult, sku = EXCLUDED.sku, 
                            status = EXCLUDED.status, url_slug = EXCLUDED.url_slug, 
                            updated_at = CURRENT_TIMESTAMP, store_id = EXCLUDED.store_id
                        RETURNING offer_id, partner_id, merchant_id, merchant_offer_id
                    )
                    SELECT
                        ins.offer_id,
                        tco.staging_id
                    FROM ins
                    JOIN tmp_catalog_offer tco
                      ON ins.partner_id = tco.partner_id
                     AND ins.merchant_id = tco.merchant_id
                     AND ins.merchant_offer_id = tco.merchant_offer_id
                """)
            except psycopg2.Error as db_err:
                if db_err.pgcode == '23505' and 'url_slug' in str(db_err):
                    errored_staging_ids.update([r['staging_id'] for r in staging_records])
                    custom_log("critical", f"Erro de slug duplicado no batch: {db_err}")
                    conn.rollback()
                    return
                else:
                    raise
            offer_id_map = {}
            for row in cur.fetchall():
                offer_id_map[row['staging_id']] = row['offer_id']
            custom_log("qty", f"Tempo upsert ofertas: {time.time() - step_time:.2f}s")

            # Atualiza processed=true em lote
            if offer_id_map:
                cur.execute(
                    "UPDATE public.import_offers_staging SET processed = true, processed_at = NOW(), error_process = false, reason_error = NULL WHERE staging_id = ANY(%s)",
                    (list(offer_id_map.keys()),)
                )
            processed_in_this_batch = len(offer_id_map)
            failed_in_this_batch = len(staging_records) - processed_in_this_batch

            # --- Bulk insert de relacionamentos (pré-filtragem e lazy) ---
            offers_images_dict = {}
            offers_pricing_dict = {}
            offers_stock_dict = {}
            offers_logistics_dict = {}
            offers_attributes_dict = {}
            for record in staging_records:
                staging_id = record.get('staging_id')
                offer_id = offer_id_map.get(staging_id)
                images = record.get('images')
                prices = record.get('prices')
                stock_qty = record.get('stock_qty')
                logistics = record.get('logistics')
                attributes = record.get('attributes')
                if offer_id and images:
                    offers_images_dict[offer_id] = images
                if offer_id and prices:
                    offers_pricing_dict[offer_id] = prices
                if offer_id and stock_qty is not None:
                    offers_stock_dict[offer_id] = stock_qty
                if offer_id and logistics:
                    offers_logistics_dict[offer_id] = logistics
                if offer_id and attributes:
                    offers_attributes_dict[offer_id] = attributes

            if offers_images_dict:
                step_time = time.time()
                process_offer_images_bulk(cur, offers_images_dict)
                custom_log("qty", f"Tempo imagens: {time.time() - step_time:.2f}s")
            if offers_pricing_dict:
                step_time = time.time()
                process_offer_pricing_bulk(cur, offers_pricing_dict)
                custom_log("qty", f"Tempo preços: {time.time() - step_time:.2f}s")
            if offers_stock_dict:
                step_time = time.time()
                process_offer_stock_bulk(cur, offers_stock_dict)
                custom_log("qty", f"Tempo estoque: {time.time() - step_time:.2f}s")
            if offers_logistics_dict:
                step_time = time.time()
                process_offer_logistics_bulk(cur, offers_logistics_dict)
                custom_log("qty", f"Tempo logística: {time.time() - step_time:.2f}s")
            if offers_attributes_dict:
                step_time = time.time()
                attribute_names_cache, attribute_values_cache = build_attribute_caches(cur, offers_attributes_dict)
                process_offer_attributes_bulk(cur, offers_attributes_dict, attribute_names_cache, attribute_values_cache)
                custom_log("qty", f"Tempo atributos: {time.time() - step_time:.2f}s")

            # Marcar registros problemáticos como erro
            if errored_staging_ids:
                cur.execute(
                    "UPDATE public.import_offers_staging SET error_process = true, reason_error = %s WHERE staging_id = ANY(%s)",
                    ("Erro de slug duplicado ou outro erro irrecuperável", list(errored_staging_ids))
                )

            if conn and not conn.closed:
                conn.commit()
            total_processed_successfully += processed_in_this_batch
            total_failed += failed_in_this_batch

            del staging_records, offer_rows, offers_images_dict, offers_pricing_dict, offers_stock_dict, offers_logistics_dict, offers_attributes_dict, offer_id_map
            gc.collect()

    except Exception as e:
        custom_log("critical", f"Erro no processo {current_process().name}: {e}", exc_info=True)
        if conn and not conn.closed:
            try: conn.rollback()
            except Exception: pass
    finally:
        if conn and not conn.closed:
            conn.close()
        elapsed = time.time() - script_start_time
        custom_log(
            "qty",
            f"Processamento finalizado. Tempo total: {elapsed:.2f} segundos. "
            f"Ofertas processadas com sucesso: {total_processed_successfully}, "
            f"falhas: {total_failed}."
        )

def main():
    # Lockfile para evitar múltiplas execuções simultâneas
    LOCKFILE = '/tmp/processstaging.lock'
    try:
        fd = os.open(LOCKFILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        print("Processo abortado: ProcessStaging.py já está em execução.")
        sys.exit(0)

    import atexit
    atexit.register(lambda: os.path.exists(LOCKFILE) and os.remove(LOCKFILE))

    # Busca os staging_ids do próximo batch a processar
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT staging_id
            FROM public.import_offers_staging
            WHERE processed = false AND error_process = false
            ORDER BY staging_id
            LIMIT %s
        """, (BATCH_SIZE,))
        batch_rows = cur.fetchall()
        batch_ids = [row[0] for row in batch_rows]
    if not batch_ids:
        logging.info("No batch to process.")
        return

    # Divide o batch entre os processos
    num_procs = min(MAX_PROCESSES, len(batch_ids))
    chunk_size = (len(batch_ids) + num_procs - 1) // num_procs
    batch_chunks = [batch_ids[i*chunk_size:(i+1)*chunk_size] for i in range(num_procs)]

    # Pré-processa stores só para este batch
    with get_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT partner_id, merchant_id, merchant_name
            FROM public.import_offers_staging
            WHERE staging_id = ANY(%s)
        """, (batch_ids,))
        staging_records = cur.fetchall()
        build_store_cache(cur, staging_records)
        conn.commit()

    # Cada processo recebe sua lista de staging_ids
    with Pool(processes=num_procs) as pool:
        pool.map(process_batch, batch_chunks)

if __name__ == "__main__":
    main()

