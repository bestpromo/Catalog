import sys 
import os  
import logging
import time
from datetime import datetime
import json
import fcntl  # For file locking on Unix-like systems
import re
from collections import defaultdict
from unidecode import unidecode 
import uuid 

# --- Path Setup para importar 'db' ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from db import connect_db as get_connection
except ImportError:
    print("CRITICAL: Could not import 'connect_db' from 'db.py'. Ensure db.py is in Batches/Catalog/src/ and sys.path is correct.")
    sys.exit(1)

import psycopg2
import psycopg2.extras

# --- Configurações Globais ---
BATCH_SIZE = 10000
MAX_OFFERS_PER_SCRIPT_RUN = 50000 
LOCK_FILE_PATH = "/tmp/process_staging_offers.lock"
MAX_SLUG_GENERATION_ATTEMPTS = 10 

# --- Configuração do Logging ---
LOG_DIR = os.path.join(os.path.dirname(__file__), "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"{datetime.now().strftime('%d%m%Y')}-ProcessStagingOffers.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(filename)s:%(lineno)d - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Funções de Cache e "Get or Create" em Lote ---

def populate_brands_cache(cur, staging_records):
    brands_cache = {}
    unique_brand_names = list(set(r['brand_name'] for r in staging_records if r.get('brand_name')))

    if not unique_brand_names:
        return brands_cache

    insert_brand_tuples = [(name,) for name in unique_brand_names]
    try:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO public.catalog_brand (name) VALUES %s ON CONFLICT (name) DO NOTHING",
            insert_brand_tuples,
            page_size=len(insert_brand_tuples) 
        )
        cur.execute("SELECT name, brand_id FROM public.catalog_brand WHERE name = ANY(%s)", (unique_brand_names,))
        for row in cur.fetchall():
            brands_cache[row['name']] = row['brand_id']
        logging.info(f"Brands cache populated with {len(brands_cache)} items. Found/Created for {len(unique_brand_names)} unique names.")
    except psycopg2.Error as e:
        logging.error(f"Error populating brands cache: {e}")
    return brands_cache

def populate_stores_cache(cur, staging_records):
    stores_cache = {}
    unique_store_keys_data = list(set(
        (r['partner_id'], r['merchant_id'], r.get('merchant_name', f"Store_{r['partner_id']}_{r['merchant_id']}"))
        for r in staging_records
        if r.get('partner_id') is not None and r.get('merchant_id') is not None
    ))

    if not unique_store_keys_data:
        return stores_cache

    insert_store_tuples = [(pid, mid, name) for pid, mid, name in unique_store_keys_data]

    try:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO public.stores (partner_id, partner_merchant_id, name) VALUES %s
               ON CONFLICT (partner_id, partner_merchant_id) DO UPDATE SET name = EXCLUDED.name""",
            insert_store_tuples,
            page_size=len(insert_store_tuples)
        )
        store_lookup_keys = list(set((pid, mid) for pid, mid, name in unique_store_keys_data))
        if store_lookup_keys:
            for p_id, m_id in store_lookup_keys:
                cur.execute("SELECT store_id FROM public.stores WHERE partner_id = %s AND partner_merchant_id = %s", (p_id, m_id))
                row = cur.fetchone()
                if row:
                    stores_cache[(p_id, m_id)] = row['store_id']
        logging.info(f"Stores cache populated with {len(stores_cache)} items for {len(unique_store_keys_data)} unique store definitions.")
    except psycopg2.Error as e:
        logging.error(f"Error populating stores cache: {e}")
    return stores_cache


def populate_attributes_caches(cur, staging_records):
    attribute_names_cache = {}
    attribute_values_cache = {}
    all_attr_names = set()
    all_attr_value_pairs = set() 

    for r in staging_records:
        attributes_json = r.get('attributes')
        if attributes_json:
            try:
                attributes_data = json.loads(attributes_json) if isinstance(attributes_json, str) else attributes_json
                
                if isinstance(attributes_data, dict): # Handle existing dict format
                    for name, value in attributes_data.items():
                        if name and value: 
                            all_attr_names.add(str(name))
                            all_attr_value_pairs.add((str(name), str(value)))
                elif isinstance(attributes_data, list): # Handle new list of objects format
                    for item in attributes_data:
                        if isinstance(item, dict):
                            name = item.get('name')
                            value = item.get('value')
                            if name and value:
                                all_attr_names.add(str(name))
                                all_attr_value_pairs.add((str(name), str(value)))
                        else:
                            logging.warning(f"Staging_id {r.get('staging_id', 'N/A')}: Item in attributes list is not a dict: {item}")
                else:
                    logging.warning(f"Staging_id {r.get('staging_id', 'N/A')}: Attributes JSON is neither a dict nor a list after parsing. Type: {type(attributes_data)}, Data: {str(attributes_json)[:200]}")

            except json.JSONDecodeError:
                logging.warning(f"Staging_id {r.get('staging_id', 'N/A')}: Malformed attributes JSON: {str(attributes_json)[:200]}")
            except TypeError: # Catch if attributes_json is not a string or dict/list directly (e.g. a number)
                logging.warning(f"Staging_id {r.get('staging_id', 'N/A')}: Attributes JSON has unexpected type before parsing or is not valid for processing: {str(attributes_json)[:200]}")

    if all_attr_names:
        insert_attr_name_tuples = [(name,) for name in all_attr_names]
        try:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO public.catalog_attribute_name (name) VALUES %s ON CONFLICT (name) DO NOTHING",
                insert_attr_name_tuples,
                page_size=len(insert_attr_name_tuples)
            )
            cur.execute("SELECT name, attribute_id FROM public.catalog_attribute_name WHERE name = ANY(%s)", (list(all_attr_names),))
            for row in cur.fetchall():
                attribute_names_cache[row['name']] = row['attribute_id']
            logging.info(f"Attribute names cache populated with {len(attribute_names_cache)} items.")
        except psycopg2.Error as e:
            logging.error(f"Error populating attribute names cache: {e}")
            return {}, {} 

    unique_attr_id_value_str_pairs = set()
    for name_str, value_str in all_attr_value_pairs:
        attribute_id = attribute_names_cache.get(name_str)
        if attribute_id:
            unique_attr_id_value_str_pairs.add((attribute_id, value_str))
    
    if unique_attr_id_value_str_pairs:
        insert_attr_value_tuples = list(unique_attr_id_value_str_pairs) # List of (attribute_id, value_str)
        try:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO public.catalog_attribute_value (attribute_id, value) VALUES %s ON CONFLICT (attribute_id, value) DO NOTHING",
                insert_attr_value_tuples,
                page_size=len(insert_attr_value_tuples)
            )
            if insert_attr_value_tuples:
                param_attr_ids = [p[0] for p in insert_attr_value_tuples] # List of UUIDs
                param_attr_values = [p[1] for p in insert_attr_value_tuples] # List of strings

                # Query ajustada para usar JOIN em unnest com WITH ORDINALITY separados
                cur.execute("""
                    SELECT cav.attribute_id, cav.value, cav.value_id
                    FROM public.catalog_attribute_value cav
                    JOIN (
                        SELECT
                            ids.pid AS attribute_id_param,
                            vals.pval AS value_param
                        FROM
                            unnest(%(p_attr_ids)s::uuid[]) WITH ORDINALITY AS ids(pid, idx)
                        JOIN
                            unnest(%(p_attr_vals)s::text[]) WITH ORDINALITY AS vals(pval, idx)
                        ON ids.idx = vals.idx
                    ) AS input_params
                    ON cav.attribute_id = input_params.attribute_id_param AND cav.value = input_params.value_param;
                """, {
                    'p_attr_ids': param_attr_ids,
                    'p_attr_vals': param_attr_values
                })
                for row in cur.fetchall():
                    attribute_values_cache[(row['attribute_id'], row['value'])] = row['value_id']
            logging.info(f"Attribute values cache populated with {len(attribute_values_cache)} items.")
        except psycopg2.Error as e:
            logging.error(f"Error populating attribute values cache: {e}")
            # Para depuração adicional, se necessário:
            # logging.debug(f"Query params for attribute values cache: IDs: {param_attr_ids}, Values: {param_attr_values}", exc_info=True)

    return attribute_names_cache, attribute_values_cache

# --- Funções de Processamento de Oferta Individual (usando caches) ---

def normalize_for_slug(text_input, max_length=250):
    if not text_input:
        slug_base = "offer" 
    else:
        slug_base = str(text_input)

    slug = unidecode(slug_base)
    slug = slug.lower()    
    slug = re.sub(r'\s+', '-', slug)
    slug = re.sub(r'[^a-z0-9\-]', '', slug)
    slug = re.sub(r'--+', '-', slug) 
    slug = slug.strip('-')  

    if not slug:
        slug = "offer-" + str(uuid.uuid4())[:8]

    return slug[:max_length]

def process_offer_attributes_from_cache(cur, offer_id, attributes_json, attr_names_cache, attr_values_cache):
    if not attributes_json: 
        # If attributes_json is empty/None, ensure existing attributes are cleared
        try:
            cur.execute("DELETE FROM public.catalog_offer_attribute WHERE offer_id = %s", (offer_id,))
            logging.info(f"Offer_id {offer_id}: No attributes provided. Cleared existing attributes.")
        except psycopg2.Error as e:
            logging.error(f"Offer_id {offer_id}: Error clearing attributes when none provided: {e}")
            # Depending on desired behavior, you might want to raise this or just log
        return

    try:
        attributes_data = json.loads(attributes_json) if isinstance(attributes_json, str) else attributes_json
    except (json.JSONDecodeError, TypeError) as e:
        logging.error(f"Offer_id {offer_id}: Failed to decode/parse attributes JSON: {str(attributes_json)[:200]}. Error: {e}")
        return 

    # Always delete existing attributes for the offer before processing new ones
    try:
        cur.execute("DELETE FROM public.catalog_offer_attribute WHERE offer_id = %s", (offer_id,))
    except psycopg2.Error as e:
        logging.error(f"Offer_id {offer_id}: Error deleting existing attributes: {e}")
        raise # Re-raise as this is a critical step before inserting new ones

    attrs_to_insert = []
    
    if isinstance(attributes_data, dict): # Handle existing dict format
        for name_str, value_str in attributes_data.items():
            if not name_str or not value_str: continue
            
            attribute_id = attr_names_cache.get(str(name_str))
            if attribute_id:
                value_id = attr_values_cache.get((attribute_id, str(value_str)))
                if value_id:
                    attrs_to_insert.append((offer_id, attribute_id, value_id))
                else:
                    logging.warning(f"Offer_id {offer_id}: Value_id not found in cache for attr_id {attribute_id}, value '{value_str}' (dict format)")
            else:
                logging.warning(f"Offer_id {offer_id}: Attribute_id not found in cache for name '{name_str}' (dict format)")
    
    elif isinstance(attributes_data, list): # Handle new list of objects format
        for item in attributes_data:
            if isinstance(item, dict):
                name_str = item.get('name')
                value_str = item.get('value')
                if not name_str or not value_str: continue
                
                attribute_id = attr_names_cache.get(str(name_str))
                if attribute_id:
                    value_id = attr_values_cache.get((attribute_id, str(value_str)))
                    if value_id:
                        attrs_to_insert.append((offer_id, attribute_id, value_id))
                    else:
                        logging.warning(f"Offer_id {offer_id}: Value_id not found in cache for attr_id {attribute_id}, value '{value_str}' (list format, item: {item})")
                else:
                    logging.warning(f"Offer_id {offer_id}: Attribute_id not found in cache for name '{name_str}' (list format, item: {item})")
            else:
                logging.warning(f"Offer_id {offer_id}: Item in attributes list is not a dict: {item}")
    else:
        logging.warning(f"Offer_id {offer_id}: Attributes data is neither a dict nor a list after parsing. Type: {type(attributes_data)}. Data: {str(attributes_data)[:200]}. No attributes will be processed.")
        # Since we deleted attributes above, this means the offer will have no attributes if data is in an unsupported format.
        return
            
    if attrs_to_insert:
        try:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO public.catalog_offer_attribute (offer_id, attribute_id, value_id) VALUES %s",
                attrs_to_insert
            )
        except psycopg2.Error as e:
            logging.error(f"Offer_id {offer_id}: Error bulk inserting attributes: {e}")
            raise 


def process_offer_images(cur, offer_id, images_json):
    logging.info(f"Offer_id {offer_id}: Starting image processing. Received images_json type: {type(images_json)}")
    if isinstance(images_json, str):
        logging.info(f"Offer_id {offer_id}: images_json (string preview): '{images_json[:200]}'")
    elif images_json is not None:
        logging.info(f"Offer_id {offer_id}: images_json (non-string preview): {str(images_json)[:200]}")


    if not images_json:
        logging.info(f"Offer_id {offer_id}: images_json is None or empty. Skipping image processing.")
        return
    try:
        images_data = json.loads(images_json) if isinstance(images_json, str) else images_json
        logging.info(f"Offer_id {offer_id}: Successfully parsed images_json. Type of images_data: {type(images_data)}")
    except (json.JSONDecodeError, TypeError) as e:
        logging.error(f"Offer_id {offer_id}: Failed to decode/parse images JSON. Content: '{str(images_json)[:200]}'. Error: {e}")
        return

    if not isinstance(images_data, list):
        logging.warning(f"Offer_id {offer_id}: Expected images_data to be a list, but got {type(images_data)}. Content: {str(images_data)[:200]}. Skipping image processing.")
        return

    if not images_data: 
        logging.info(f"Offer_id {offer_id}: images_data is an empty list. Deleting existing images (if any) and skipping new image insertion.")
    
    images_to_insert = []
    for i, img_obj in enumerate(images_data):
        if isinstance(img_obj, dict):
            # Try to get 'image_url', fallback to 'url'
            image_link = img_obj.get('image_url') or img_obj.get('url')
            if image_link:
                images_to_insert.append((
                    offer_id,
                    image_link, # Use the found link
                    img_obj.get('alt_text') or img_obj.get('alt'), # Fallback for alt text as well
                    img_obj.get('main_image', False),
                    img_obj.get('display_order', i) 
                ))
            else:
                logging.warning(f"Offer_id {offer_id}: Missing 'image_url' or 'url' in image object at index {i}. Object: {str(img_obj)[:200]}")
        else:
            logging.warning(f"Offer_id {offer_id}: Invalid image object (not a dict) at index {i}. Object: {str(img_obj)[:200]}")

    try:
        logging.info(f"Offer_id {offer_id}: Deleting existing images from catalog_offer_image.")
        cur.execute("DELETE FROM public.catalog_offer_image WHERE offer_id = %s", (offer_id,))

        if images_to_insert:
            logging.info(f"Offer_id {offer_id}: Found {len(images_to_insert)} valid images to insert.")
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO public.catalog_offer_image (offer_id, image_url, alt_text, main_image, display_order) VALUES %s",
                images_to_insert
            )
            logging.info(f"Offer_id {offer_id}: Successfully inserted {len(images_to_insert)} images into catalog_offer_image.")
        else:
            logging.info(f"Offer_id {offer_id}: No valid images found in images_data to insert into catalog_offer_image (after delete).")

    except psycopg2.Error as e:
        logging.error(f"Offer_id {offer_id}: DB Error during image processing (delete or insert): {e}", exc_info=True)
        raise 
    except Exception as e_gen:
        logging.error(f"Offer_id {offer_id}: Unexpected non-DB error during image processing logic: {e_gen}", exc_info=True)
        raise psycopg2.Error(f"Unexpected error in image processing for offer {offer_id}: {e_gen}") from e_gen


def process_offer_pricing(cur, offer_id, prices_json): 
    if not prices_json: return
    try:
        pricing_data = json.loads(prices_json) if isinstance(prices_json, str) else prices_json
    except (json.JSONDecodeError, TypeError) as e:
        logging.error(f"Offer_id {offer_id}: Failed to decode/parse prices JSON: {prices_json}. Error: {e}")
        return
    if not isinstance(pricing_data, dict): return

    sale_price = pricing_data.get('price') or pricing_data.get('sale_price')
    if sale_price is None: 
        logging.warning(f"Offer_id {offer_id}: No sale_price or price found in pricing_data. Skipping pricing insert.")
        return
    
    try:
        cur.execute(
            """INSERT INTO public.catalog_offer_pricing 
               (offer_id, sale_price, promotional_price, original_price, currency, updated_at)
               VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
               ON CONFLICT (offer_id) DO UPDATE SET
                 sale_price = EXCLUDED.sale_price, promotional_price = EXCLUDED.promotional_price,
                 original_price = EXCLUDED.original_price, currency = EXCLUDED.currency, updated_at = CURRENT_TIMESTAMP;""",
            (offer_id, sale_price, pricing_data.get('promotional_price'), pricing_data.get('original_price'), pricing_data.get('currency', 'BRL'))
        )
    except psycopg2.Error as e:
        logging.error(f"Offer_id {offer_id}: Error upserting pricing: {e}")
        raise


def process_offer_stock(cur, offer_id, stock_qty_staging): 
    quantity = stock_qty_staging if stock_qty_staging is not None else 0
    try:
        cur.execute(
            """INSERT INTO public.catalog_offer_stock (offer_id, quantity, updated_at)
               VALUES (%s, %s, CURRENT_TIMESTAMP) ON CONFLICT (offer_id) DO UPDATE SET
                 quantity = EXCLUDED.quantity, updated_at = CURRENT_TIMESTAMP;""",
            (offer_id, quantity)
        )
    except psycopg2.Error as e:
        logging.error(f"Offer_id {offer_id}: Error upserting stock: {e}")
        raise


def process_offer_logistics(cur, offer_id, logistics_json):
    if not logistics_json: return
    try:
        logistics_data = json.loads(logistics_json) if isinstance(logistics_json, str) else logistics_json
    except (json.JSONDecodeError, TypeError) as e:
        logging.error(f"Offer_id {offer_id}: Failed to decode/parse logistics JSON: {logistics_json}. Error: {e}")
        return
    if not isinstance(logistics_data, dict): return

    try:
        cur.execute(
            """
            INSERT INTO public.catalog_logistics_info (
                offer_id, weight, height, width, depth, package_weight, barcode, 
                fragile_offer, free_shipping, processing_time, updated_at
            ) VALUES (
                %(offer_id)s, %(weight)s, %(height)s, %(width)s, %(depth)s, %(package_weight)s, %(barcode)s,
                %(fragile_offer)s, %(free_shipping)s, %(processing_time)s, CURRENT_TIMESTAMP
            )
            ON CONFLICT (offer_id) DO UPDATE SET
                weight = EXCLUDED.weight, height = EXCLUDED.height, width = EXCLUDED.width, depth = EXCLUDED.depth,
                package_weight = EXCLUDED.package_weight, barcode = EXCLUDED.barcode, fragile_offer = EXCLUDED.fragile_offer,
                free_shipping = EXCLUDED.free_shipping, processing_time = EXCLUDED.processing_time, updated_at = CURRENT_TIMESTAMP;
            """,
            {
                'offer_id': offer_id,
                'weight': logistics_data.get('weight'),
                'height': logistics_data.get('height'),
                'width': logistics_data.get('width'),
                'depth': logistics_data.get('depth'), 
                'package_weight': logistics_data.get('package_weight'),
                'barcode': logistics_data.get('barcode'),
                'fragile_offer': logistics_data.get('fragile_offer', False),
                'free_shipping': logistics_data.get('free_shipping', False),
                'processing_time': logistics_data.get('processing_time', 1)
            }
        )
    except psycopg2.Error as e:
        logging.error(f"Offer_id {offer_id}: Error upserting logistics info: {e}")
        raise


def process_staging_record(cur, staging_row, caches, conn):
    staging_id = staging_row['staging_id']
    logging.debug(f"Processing staging_id: {staging_id}")

    brands_cache = caches['brands']
    attr_names_cache = caches['attr_names']
    attr_values_cache = caches['attr_values']
    
    offer_id_uuid = None 

    try:
        brand_id = brands_cache.get(staging_row.get('brand_name'))
        condition_from_staging = staging_row.get('condition')
        condition_val = 'new' 
        if condition_from_staging is not None:
            condition_val = str(condition_from_staging).lower() 
        if condition_val not in ['new', 'used', 'refurbished']:
            condition_val = 'new'
        
        offer_status_staging = staging_row.get('status') 
        catalog_offer_status = 'active' if offer_status_staging is True else 'inactive'
        if catalog_offer_status not in ['active', 'inactive', 'draft', 'out_of_stock']: 
             catalog_offer_status = 'inactive'

        offer_title = staging_row.get('offer_title')
        offer_merchant_id = staging_row.get('offer_merchant_id')
        
        slug_parts = []
        if offer_title:
            slug_parts.append(offer_title)
        if offer_merchant_id:
            slug_parts.append(offer_merchant_id)
        
        if slug_parts:
            base_slug_text = " ".join(slug_parts)
        else:
            base_slug_text = f"offer-staging-{staging_row['staging_id']}"

        original_slug_candidate = normalize_for_slug(base_slug_text)
        
        if not original_slug_candidate:
            original_slug_candidate = "offer-" + str(uuid.uuid4())[:8]

        offer_upsert_successful = False
        current_slug_to_try = ""

        for attempt in range(MAX_SLUG_GENERATION_ATTEMPTS):
            if attempt == 0:
                current_slug_to_try = original_slug_candidate
            else:
                current_slug_to_try = f"{original_slug_candidate}-{attempt}"[:255]

            try:
                cur.execute(
                    """
                    INSERT INTO public.catalog_offer (
                        partner_id, merchant_id, merchant_offer_id, title, short_description,
                        full_description, brand_id, "condition", offer_gtin, mpn, deep_link_url,
                        adult, sku, status, url_slug, updated_at, created_at
                    ) VALUES (
                        %(partner_id)s, %(merchant_id)s, %(offer_merchant_id)s, %(offer_title)s, %(offer_short_description)s,
                        %(offer_full_description)s, %(brand_id)s, %(condition)s, %(gtin)s, %(mpn)s, %(deep_link_url)s,
                        %(is_adult)s, %(sku)s, %(status)s, %(url_slug)s, CURRENT_TIMESTAMP, %(created_at)s
                    )
                    ON CONFLICT (partner_id, merchant_id, merchant_offer_id) DO UPDATE SET
                        title = EXCLUDED.title, short_description = EXCLUDED.short_description,
                        full_description = EXCLUDED.full_description, brand_id = EXCLUDED.brand_id,
                        "condition" = EXCLUDED."condition", offer_gtin = EXCLUDED.offer_gtin, mpn = EXCLUDED.mpn,
                        deep_link_url = EXCLUDED.deep_link_url, adult = EXCLUDED.adult, sku = EXCLUDED.sku, 
                        status = EXCLUDED.status, url_slug = EXCLUDED.url_slug, 
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING offer_id, created_at; 
                    """, 
                    {
                        'partner_id': staging_row.get('partner_id'), 
                        'merchant_id': staging_row.get('merchant_id'),
                        'offer_merchant_id': staging_row.get('offer_merchant_id'), 
                        'offer_title': staging_row.get('offer_title'),
                        'offer_short_description': staging_row.get('offer_short_description'),
                        'offer_full_description': staging_row.get('offer_full_description'), 
                        'brand_id': brand_id,
                        'condition': condition_val,
                        'gtin': staging_row.get('gtin'), 
                        'mpn': staging_row.get('mpn'),
                        'deep_link_url': staging_row.get('deep_link_url'), 
                        'is_adult': staging_row.get('is_adult', False),
                        'sku': staging_row.get('sku'), 
                        'status': catalog_offer_status,
                        'url_slug': current_slug_to_try,
                        'created_at': staging_row.get('created_at', datetime.now()) 
                    }
                )
                result_offer_tuple = cur.fetchone()
                if result_offer_tuple and result_offer_tuple['offer_id']:
                    offer_id_uuid = result_offer_tuple['offer_id']
                    logging.info(f"Staging_id {staging_id}: Successfully upserted offer with slug '{current_slug_to_try}' (attempt {attempt + 1}). Offer ID: {offer_id_uuid}")
                    offer_upsert_successful = True
                    break 
                else:
                    logging.error(f"Staging_id {staging_id}: Offer upsert with slug '{current_slug_to_try}' (attempt {attempt+1}) did not return an offer_id.")
                    if conn and not conn.closed: conn.rollback()


            except psycopg2.errors.UniqueViolation as uv_err:
                if conn and not conn.closed: conn.rollback()
                logging.debug(f"Staging_id {staging_id}: Rolled back transaction due to UniqueViolation for slug '{current_slug_to_try}'.")
                
                constraint_name = getattr(uv_err.diag, 'constraint_name', '')
                if constraint_name == 'catalog_offer_url_slug_key':
                    logging.warning(f"Staging_id {staging_id}: Slug '{current_slug_to_try}' (attempt {attempt + 1}/{MAX_SLUG_GENERATION_ATTEMPTS}) caused 'catalog_offer_url_slug_key' violation.")
                    if attempt == MAX_SLUG_GENERATION_ATTEMPTS - 1:
                        logging.error(f"Staging_id {staging_id}: All {MAX_SLUG_GENERATION_ATTEMPTS} attempts for slug failed. Last: '{current_slug_to_try}'.")
                else: 
                    logging.error(f"Staging_id {staging_id}: Non-slug UniqueViolation. Slug: '{current_slug_to_try}'. Constraint: '{constraint_name}'. Error: {uv_err}")
                    raise uv_err
            
            except psycopg2.Error as db_err_slug_loop:
                if conn and not conn.closed: conn.rollback()
                logging.error(f"Staging_id {staging_id}: DB error during slug attempt {attempt + 1} with slug '{current_slug_to_try}': {db_err_slug_loop}")
                raise db_err_slug_loop

        if not offer_upsert_successful:
            reason_error_final = f"Failed to upsert offer after {MAX_SLUG_GENERATION_ATTEMPTS} slug attempts or other issue. Last slug tried: '{current_slug_to_try}'."
            logging.error(f"Staging_id {staging_id}: {reason_error_final}")
            if conn and not conn.closed and conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                conn.rollback()
            cur.execute( "UPDATE public.import_offers_staging SET error_process = true, reason_error = %s, processed_at = NOW() WHERE staging_id = %s", (str(reason_error_final)[:1000], staging_id))
            if conn and not conn.closed: conn.commit()
            return False

        process_offer_pricing(cur, offer_id_uuid, staging_row.get('prices'))
        process_offer_stock(cur, offer_id_uuid, staging_row.get('stock_qty'))
        process_offer_images(cur, offer_id_uuid, staging_row.get('images'))
        process_offer_attributes_from_cache(cur, offer_id_uuid, staging_row.get('attributes'), attr_names_cache, attr_values_cache)
        process_offer_logistics(cur, offer_id_uuid, staging_row.get('logistics'))

        cur.execute(
            "UPDATE public.import_offers_staging SET processed = true, processed_at = NOW(), error_process = false, reason_error = NULL WHERE staging_id = %s",
            (staging_id,)
        )
        return True

    except psycopg2.Error as db_err:
        logging.error(f"Outer DB error in process_staging_record for staging_id {staging_id}: {db_err}")
        if conn and not conn.closed: 
            try: conn.rollback()
            except psycopg2.Error: pass
        try:
            cur.execute("UPDATE public.import_offers_staging SET error_process = true, reason_error = %s, processed_at = NOW() WHERE staging_id = %s", (f"Outer DB Error: {str(db_err)[:1000]}", staging_id))
            if conn and not conn.closed: conn.commit()
        except Exception as e_mark_err:
            logging.critical(f"Staging_id {staging_id}: FAILED TO MARK AS ERROR after outer DB error: {e_mark_err}. Original: {db_err}")
    except Exception as e:
        logging.error(f"Outer generic error in process_staging_record for staging_id {staging_id}: {e}", exc_info=True)
        if conn and not conn.closed: 
            try: conn.rollback()
            except psycopg2.Error: pass
        try:
            cur.execute("UPDATE public.import_offers_staging SET error_process = true, reason_error = %s, processed_at = NOW() WHERE staging_id = %s", (f"Outer Generic Error: {str(e)[:1000]}", staging_id))
            if conn and not conn.closed: conn.commit()
        except Exception as e_mark_err:
            logging.critical(f"Staging_id {staging_id}: FAILED TO MARK AS ERROR after outer generic error: {e_mark_err}. Original: {e}")
            
    return False


def main():
    script_start_time = time.time()
    logging.info(f"ProcessStagingOffers script started. BATCH_SIZE={BATCH_SIZE}, MAX_OFFERS_PER_SCRIPT_RUN={MAX_OFFERS_PER_SCRIPT_RUN or 'Unlimited'}")

    lock_fp = None
    try:
        lock_fp = open(LOCK_FILE_PATH, 'w')
        fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.warning("Another instance is already running. Exiting.")
        if lock_fp: lock_fp.close()
        return
    except Exception as e:
        logging.error(f"Error acquiring lock: {e}")
        if lock_fp: lock_fp.close()
        return

    conn = None
    total_processed_successfully = 0
    total_failed = 0
    
    try:
        conn = get_connection()
        if conn is None:
            logging.critical("Failed to connect to the database. Exiting.")
            return

        while True:
            limit_for_this_batch = BATCH_SIZE
            if MAX_OFFERS_PER_SCRIPT_RUN is not None:
                remaining_allowed = MAX_OFFERS_PER_SCRIPT_RUN - (total_processed_successfully + total_failed) 
                if remaining_allowed <= 0:
                    logging.info(f"MAX_OFFERS_PER_SCRIPT_RUN ({MAX_OFFERS_PER_SCRIPT_RUN}) reached or exceeded. No more offers will be processed.")
                    break
                limit_for_this_batch = min(BATCH_SIZE, remaining_allowed)

            batch_start_time = time.time()
            processed_in_this_batch = 0
            failed_in_this_batch = 0
            
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    "SELECT * FROM public.import_offers_staging WHERE processed = false AND error_process = false ORDER BY staging_id LIMIT %s",
                    (limit_for_this_batch,)
                )
                staging_records = cur.fetchall()

                if not staging_records:
                    logging.info("No new records in staging to process.")
                    break 

                logging.info(f"Fetched {len(staging_records)} records for this batch (limit was {limit_for_this_batch}).")

                try:
                    logging.info("Populating caches for the batch...")
                    brands_cache = populate_brands_cache(cur, staging_records)
                    stores_cache = populate_stores_cache(cur, staging_records) 
                    attr_names_cache, attr_values_cache = populate_attributes_caches(cur, staging_records)
                    
                    caches = {
                        'brands': brands_cache, 'stores': stores_cache,
                        'attr_names': attr_names_cache, 'attr_values': attr_values_cache
                    }
                    if conn and not conn.closed: conn.commit() 
                    logging.info("Caches populated and committed.")
                except Exception as cache_err:
                    logging.error(f"Critical error populating caches: {cache_err}. Rolling back and skipping batch.", exc_info=True)
                    if conn and not conn.closed: conn.rollback()
                    break 

                for record in staging_records:
                    staging_id_for_log = record.get('staging_id', 'UNKNOWN_ID')
                    try:
                        if process_staging_record(cur, record, caches, conn):
                            processed_in_this_batch += 1
                            if conn and not conn.closed:
                                conn.commit()
                                logging.debug(f"Main loop: Committed successfully for staging_id {staging_id_for_log}")
                            else:
                                logging.error(f"Main loop: Connection not available to commit successful staging_id {staging_id_for_log}")
                                break 
                        else:
                            failed_in_this_batch += 1
                            logging.debug(f"Main loop: Staging_id {staging_id_for_log} failed, error marking handled by process_staging_record.")

                    except Exception as e_loop: 
                        logging.error(f"Critical error in main processing loop for staging_id {staging_id_for_log}: {e_loop}. Rolling back.", exc_info=True)
                        failed_in_this_batch += 1 
                        if conn and not conn.closed:
                            try: conn.rollback()
                            except psycopg2.Error as rb_err_main_loop:
                                logging.error(f"Rollback failed in main loop exception handler for staging_id {staging_id_for_log}: {rb_err_main_loop}")
                        
                        try: 
                            if conn and not conn.closed:
                                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as err_cur_main:
                                    err_cur_main.execute(
                                        "UPDATE public.import_offers_staging SET error_process = true, reason_error = %s, processed_at = NOW() WHERE staging_id = %s",
                                        (f"Main Loop Error: {str(e_loop)[:1000]}", staging_id_for_log)
                                    )
                                conn.commit() 
                        except Exception as e_mark_err:
                            logging.critical(f"Failed to mark staging_id {staging_id_for_log} as error after main loop exception: {e_mark_err}")
                
                total_processed_successfully += processed_in_this_batch
                total_failed += failed_in_this_batch
                batch_duration = time.time() - batch_start_time
                logging.info(f"Batch finished. Processed: {processed_in_this_batch}, Failed: {failed_in_this_batch}. Duration: {batch_duration:.2f}s")
                logging.info(f"Total processed in this run so far: {total_processed_successfully}, Total Failed: {total_failed}")

                if MAX_OFFERS_PER_SCRIPT_RUN is not None and (total_processed_successfully + total_failed) >= MAX_OFFERS_PER_SCRIPT_RUN:
                    logging.info(f"Reached or exceeded MAX_OFFERS_PER_SCRIPT_RUN ({MAX_OFFERS_PER_SCRIPT_RUN}) considering all attempts. Stopping.")
                    break

                if len(staging_records) < limit_for_this_batch : 
                    logging.info("Processed all available records from staging (fetched less than batch limit).")
                    break
    except psycopg2.Error as db_err_main:
        logging.critical(f"Main database error: {db_err_main}", exc_info=True)
        if conn and not conn.closed: 
            try: conn.rollback()
            except psycopg2.Error: pass
    except Exception as e_main:
        logging.critical(f"Main script error: {e_main}", exc_info=True)
        if conn and not conn.closed:
            try: conn.rollback()
            except psycopg2.Error: pass 
    finally:
        if conn and not conn.closed:
            conn.close()
            logging.info("Database connection closed.")
        elif conn and conn.closed:
             logging.info("Database connection was already closed.")
        else:
            logging.info("No active database connection to close.")
        
        if lock_fp:
            fcntl.flock(lock_fp.fileno(), fcntl.LOCK_UN)
            lock_fp.close()
            try:
                if os.path.exists(LOCK_FILE_PATH): 
                    os.remove(LOCK_FILE_PATH)
            except OSError as e_rm_lock:
                logging.warning(f"Could not remove lock file {LOCK_FILE_PATH}: {e_rm_lock}")
            logging.info("Lock released.")
        
        script_duration = time.time() - script_start_time
        logging.info(f"ProcessStagingOffers script finished. Total Processed: {total_processed_successfully}, Total Failed: {total_failed}. Total Duration: {script_duration:.2f}s")

if __name__ == "__main__":
    main()

