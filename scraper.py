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

import dropbox
from dropbox.exceptions import ApiError
import os

# Replace with your own credentials
APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Initialize Dropbox client
dbx = dropbox.Dropbox(ACCESS_TOKEN)

# Function to upload a file to Dropbox
def upload_file(file_path, dropbox_path):
    with open(file_path, 'rb') as f:
        try:
            dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
            print(f"File uploaded to {dropbox_path}")
        except dropbox.exceptions.ApiError as e:
            if e.error.is_path_conflict():
                print(f"Conflict occurred: {e}")
            else:
                print(f"Error occurred: {e}")
                
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

def upload_file_from_memory(file_buffer, dropbox_path, dbx):
    try:
        file_buffer.seek(0)  # Make sure to move the buffer pointer to the beginning
        dbx.files_upload(file_buffer.getvalue().encode('utf-8'), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
        logging.info(f"File uploaded to {dropbox_path}")
    except dropbox.exceptions.ApiError as e:
        logging.error(f"Error uploading file to Dropbox: {e}")
        

# Function to process the data
def process_and_upload_data(extracted_data, category_slug, subcategory_slug, page, dbx):
    # Create an in-memory buffer to hold the CSV data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'parent_id', 'child_id', 'name', 'weight', 'mrp', 
        'discount_text', 'imageURL', 'category_slug'
    ])
    writer.writerows(extracted_data)

    # Process the data using pandas
    df = pd.read_csv(io.StringIO(output.getvalue()))
    df['hsn'] = df.apply(lambda row: row['child_id'] if pd.notna(row['child_id']) else row['parent_id'], axis=1)
    df['hsn'] = df['hsn'].astype('Int64')

    # Extract discount logic
    def extract_discount(discount_text):
        if pd.isna(discount_text): return 0
        if '%' in discount_text: return int(discount_text.split('%')[0])
        if '₹' in discount_text: return 0
        return 0

    df['price'] = df['mrp']
    df['discount'] = df['discount_text'].apply(extract_discount)

    # Split weight
    def split_weight(weight):
        if pd.isna(weight): return pd.Series([None, None])
        match = re.match(r'^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$', str(weight).strip())
        if match: return pd.Series([float(match.group(1)), match.group(2)])
        if re.match(r'^[a-zA-Z]+$', str(weight)): return pd.Series([None, str(weight)])
        return pd.Series([None, None])

    df[['quantity', 'unit']] = df['weight'].apply(split_weight)
    df = df[df['quantity'].notnull()]
    df.drop(['parent_id', 'child_id', 'weight', 'mrp', 'discount_text'], axis=1, inplace=True)

    # Prepare the final output
    final_output = io.StringIO()
    df.to_csv(final_output, index=False)

    # Upload to Dropbox
    dropbox_path = f"/ScrapedData/bb-{category_slug}-{subcategory_slug}-page-{page}.csv"
    upload_file_from_memory(final_output, dropbox_path, dbx)
    
# Scrape data and save to CSV
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


            if extracted_data:
                process_and_upload_data(extracted_data, category_slug, subcategory_slug, page, dbx)
                logging.info(f"Scraped and processed data for page {page}")
                time.sleep(2)  # Respectful delay to avoid overwhelming the server

        except Exception as e:
            logging.error(f"Error processing {subcategory_slug} page {page}: {e}")
            break   

# Clean output files
def clean_output():
    pass

# Run the scraper stream
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
            msg = f"Scraping: bb-{category_slug}-{subcategory_slug}-page-{i}.csv"
            logging.info(msg)
            log_history += msg + "\n"
            yield log_history, (i / total), scraped_files.copy()

            scrape_subcategory(session, category_slug, subcategory_slug)

            filename = f"bb-{category_slug}-{subcategory_slug}-page-{i}.csv"
            scraped_files.append(filename)

            msg_done = f"✅ Done: bb-{category_slug}-{subcategory_slug}-page-{i}.csv"
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