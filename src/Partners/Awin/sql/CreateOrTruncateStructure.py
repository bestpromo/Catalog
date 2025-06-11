import os
import sys

# Ajusta sys.path para importar connect_db corretamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
from src.db import connect_db

CREATE_TABLE_SQL = """
CREATE TABLE awin_catalog_import_temp (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    imported BOOLEAN DEFAULT FALSE,
    partner_id INTEGER,
    aw_deep_link TEXT,
    product_name TEXT,
    aw_product_id TEXT,
    merchant_product_id TEXT,
    merchant_image_url TEXT,
    description TEXT,
    merchant_category TEXT,
    search_price TEXT,
    merchant_name TEXT,
    merchant_id INTEGER,
    category_name TEXT,
    category_id TEXT,
    aw_image_url TEXT,
    currency TEXT,
    store_price TEXT,
    delivery_cost TEXT,
    merchant_deep_link TEXT,
    "language" TEXT,
    last_updated TEXT,
    display_price TEXT,
    data_feed_id TEXT,
    brand_name TEXT,
    brand_id TEXT,
    colour TEXT,
    product_short_description TEXT,
    specifications TEXT,
    "condition" TEXT,
    product_model TEXT,
    model_number TEXT,
    dimensions TEXT,
    keywords TEXT,
    promotional_text TEXT,
    product_type TEXT,
    commission_group TEXT,
    merchant_product_category_path TEXT,
    merchant_product_second_category TEXT,
    merchant_product_third_category TEXT,
    rrp_price TEXT,
    saving TEXT,
    savings_percent TEXT,
    base_price TEXT,
    base_price_amount TEXT,
    base_price_text TEXT,
    product_price_old TEXT,
    delivery_restrictions TEXT,
    delivery_weight TEXT,
    warranty TEXT,
    terms_of_contract TEXT,
    delivery_time TEXT,
    in_stock BOOLEAN,
    stock_quantity TEXT,
    valid_from TEXT,
    valid_to TEXT,
    is_for_sale BOOLEAN,
    web_offer BOOLEAN,
    pre_order BOOLEAN,
    stock_status TEXT,
    size_stock_status TEXT,
    size_stock_amount TEXT,
    merchant_thumb_url TEXT,
    large_image TEXT,
    alternate_image TEXT,
    aw_thumb_url TEXT,
    alternate_image_two TEXT,
    alternate_image_three TEXT,
    alternate_image_four TEXT,
    reviews TEXT,
    average_rating TEXT,
    rating TEXT,
    number_available TEXT,
    custom_1 TEXT,
    custom_2 TEXT,
    custom_3 TEXT,
    custom_4 TEXT,
    custom_5 TEXT,
    custom_6 TEXT,
    custom_7 TEXT,
    custom_8 TEXT,
    custom_9 TEXT,
    ean TEXT,
    isbn TEXT,
    upc TEXT,
    mpn TEXT,
    parent_product_id TEXT,
    product_gtin TEXT,
    basket_link TEXT,
    fashion_suitable_for TEXT,
    fashion_category TEXT,
    fashion_size TEXT,
    fashion_material TEXT,
    fashion_pattern TEXT,
    fashion_swatch TEXT
);

CREATE INDEX idx_awin_imported ON awin_catalog_import_temp(imported);
CREATE INDEX idx_awin_partner_id ON awin_catalog_import_temp(partner_id);
CREATE INDEX idx_awin_merchant_id ON awin_catalog_import_temp(merchant_id);
CREATE INDEX idx_awin_merchant_product_id ON awin_catalog_import_temp(merchant_product_id);
CREATE INDEX idx_awin_aw_product_id ON awin_catalog_import_temp(aw_product_id);
CREATE INDEX idx_awin_last_updated ON awin_catalog_import_temp(last_updated);
"""

def table_exists(cur, table_name: str) -> bool:
    """
    Verifica se uma tabela existe no schema public.
    """
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    return cur.fetchone()[0]

def truncate_table(cur, table_name: str):
    """
    Limpa a tabela e reseta o id.
    """
    cur.execute(f"TRUNCATE TABLE public.{table_name} RESTART IDENTITY;")

def create_table_and_indexes(cur):
    """
    Cria a tabela temporária e seus índices.
    """
    cur.execute(CREATE_TABLE_SQL)

def main():
    """
    Cria ou limpa a tabela awin_catalog_import_temp conforme necessário.
    """
    table_name = "awin_catalog_import_temp"
    with connect_db() as conn:
        with conn.cursor() as cur:
            if table_exists(cur, table_name):
                truncate_table(cur, table_name)
                print(f"Tabela {table_name} já existia. Dados apagados e id reiniciado.")
            else:
                create_table_and_indexes(cur)
                print(f"Tabela {table_name} criada com sucesso.")
        conn.commit()

if __name__ == "__main__":
    main()