# -*- coding: utf-8 -*-
import requests
import csv
import brotli
import gzip
import zlib
import json
import time
import pandas as pd
import re
import os
import logging
import io
from dotenv import load_dotenv

load_dotenv()

base_url = "https://www.bigbasket.com/listing-svc/v2/products?type=pc&slug={slug}&page={page}"

# Add this import at the top of your scraper.py
from pymongo import MongoClient

# MongoDB connection setup
mongo_client = MongoClient(os.getenv("MONGO_URI"))
db = mongo_client[os.getenv("MONGODB_NAME")]
collection = db[os.getenv("MONGOCOL_NAME")]

def setup_logger(socketio=None):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Clear previous handlers
    logger.handlers = []

    # Create a default stream handler for stdout
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(stream_handler)

    # If using socketio in the future
    if socketio:
        socketio_handler = SocketIOHandler(socketio)
        socketio_handler.setLevel(logging.INFO)
        socketio_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(socketio_handler)
        
def create_session():
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-GB,en;q=0.8",
        "Content-Type": "application/json",
    }
    session = requests.Session()
    session.get("https://www.bigbasket.com/", headers=headers)
    session.headers.update(headers)
    return session

def decode_response(response):
    encoding = response.headers.get("Content-Encoding", "")
    try:
        if "br" in encoding:
            return brotli.decompress(response.content).decode()
        elif "gzip" in encoding:
            return gzip.decompress(response.content).decode()
        elif "deflate" in encoding:
            return zlib.decompress(response.content).decode()
        else:
            return response.text
    except Exception as e:
        logging.error("Decoding error: %s", e)
        return response.text

def process_csv(file_path):
    df = pd.read_csv(file_path)
    df['hsn'] = df.apply(lambda row: row['child_id'] if pd.notna(row['child_id']) else row['parent_id'], axis=1)
    df['hsn'] = df['hsn'].astype('Int64')

    def extract_discount(discount_text):
        if pd.isna(discount_text): return 0
        if '%' in discount_text: return int(discount_text.split('%')[0])
        if '₹' in discount_text: return 0
        return 0

    df['price'] = df['mrp']
    df['discount'] = df['discount_text'].apply(extract_discount)

    def split_weight(weight):
        if pd.isna(weight): return pd.Series([None, None])
        match = re.match(r'^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$', str(weight).strip())
        if match: return pd.Series([float(match.group(1)), match.group(2)])
        if re.match(r'^[a-zA-Z]+$', str(weight)): return pd.Series([None, str(weight)])
        return pd.Series([None, None])

    df[['quantity', 'unit']] = df['weight'].apply(split_weight)
    df = df[df['quantity'].notnull()]
    df.drop(['parent_id', 'child_id', 'weight', 'mrp', 'discount_text'], axis=1, inplace=True)

    # Save to MongoDB
    records = df.to_dict(orient='records')
    collection.insert_many(records)  # Insert records into MongoDB

    df.to_csv(file_path, index=False)
    logging.info("Processed and saved to MongoDB: %s", file_path)

def scrape_subcategory(session, category_slug, subcategory_slug):
    
    for page in range(1, 17):
        extracted_data = []
        url = base_url.format(slug=subcategory_slug, page=page)
        response = session.get(url)

        if response.status_code != 200:
            logging.error("Error %s for %s page %s", response.status_code, subcategory_slug, page)
            break

        try:
            raw_data = decode_response(response)
            data = json.loads(raw_data)
            tabs = data.get('tabs', [])
            if tabs:
                products = tabs[0].get('product_info', {}).get('products', [])
                for product in products:
                    parent_id = product.get('id')
                    parent_desc = product.get('desc')
                    parent_images = product.get('images', [])
                    parent_weight = product.get('w')
                    parent_mrp = product.get('pricing', {}).get('discount', {}).get('mrp')
                    parent_discount_text = product.get('pricing', {}).get('discount', {}).get('d_text')
                    parent_image_large = parent_images[0].get('l') if parent_images else None
                    category_slug = product.get('category', {}).get('tlc_slug')

                    extracted_data.append([
                        parent_id, None, parent_desc, parent_weight, parent_mrp,
                        parent_discount_text, parent_image_large, category_slug
                    ])

                    for child in product.get('children', []):
                        child_id = child.get('id')
                        weight = child.get('w')
                        mrp = child.get('pricing', {}).get('discount', {}).get('mrp')
                        discount_text = child.get('pricing', {}).get('discount', {}).get('d_text')

                        extracted_data.append([
                            parent_id, child_id, parent_desc, weight, mrp,
                            discount_text, parent_image_large, category_slug
                        ])

            filename = os.path.join("outputs", f"bb-{category_slug}-{subcategory_slug}-page-{page}.csv")

            if extracted_data:
                with open(filename, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow([
                        'parent_id', 'child_id', 'name', 'weight', 'mrp',
                        'discount_text', 'imageURL', 'category_slug'
                    ])
                    writer.writerows(extracted_data)

                logging.info("Scraped: %s", filename)
                process_csv(filename)
                time.sleep(2)

        except Exception as e:
            logging.error("Error processing %s page %s: %s", subcategory_slug, page, e)
            break

def clean_output():
    for file in os.listdir("outputs"):
        if file.endswith(".csv") or file.endswith(".zip"):
            os.remove(os.path.join("outputs", file))

def run_scraper_stream():
    setup_logger()
    clean_output()
    session = create_session()

    df = pd.read_csv(os.path.join("data", "bb-category-tree.csv"))
    subcategory_slugs = df['subcategory_slug'].tolist()
    category_slugs = df['category_slug'].tolist()

    log_history = ""
    scraped_files = []
    total = len(category_slugs)

    try:
        for i, (category_slug, subcategory_slug) in enumerate(zip(category_slugs, subcategory_slugs)):
            msg = f"Scraping: bb-{category_slug}-{subcategory_slug}-page-1.csv"
            logging.info(msg)
            log_history += msg + "\n"
            yield log_history, (i / total), scraped_files.copy()

            scrape_subcategory(session, category_slug, subcategory_slug)

            filename = f"bb-{category_slug}-{subcategory_slug}-page-1.csv"
            scraped_files.append(filename)

            msg_done = f"✅ Done: bb-{category_slug}-{subcategory_slug}-page-1.csv"
            logging.info(msg_done)
            log_history += msg_done + "\n"
            yield log_history, ((i + 1) / total), scraped_files.copy()

    except Exception as e:
        error_msg = f"Error: {e}"
        logging.error(error_msg)
        log_history += error_msg + "\n"
        yield log_history, 1.0, scraped_files.copy()
    finally:
        session.close()

