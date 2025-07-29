import os
import requests
import gzip
import shutil
import pandas as pd
import logging
from sqlalchemy import create_engine

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# === Configuration ===

DOWNLOAD_URL = "https://gz.blockchair.com/bitcoin/blocks/blockchair_bitcoin_blocks_20250727.tsv.gz"
FILENAME_GZ = "blockchair_bitcoin_blocks_20250727.tsv.gz"
FILENAME_TSV = "blockchair_bitcoin_blocks_20250727.tsv"

# Snowflake connection parameters from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")  # e.g. "xy12345.us-east-1"
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = "bitcoin_blocks"

# === Step 1: Download the file ===
def download_file(url, filename):
    logging.info(f"Downloading {filename} from {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise HTTPError for bad responses
    with open(filename, 'wb') as f:
        shutil.copyfileobj(response.raw, f)
    logging.info("Download complete.")

# === Step 2: Extract the file ===
def extract_gz(source_path, dest_path):
    logging.info(f"Extracting {source_path} to {dest_path}...")
    with gzip.open(source_path, 'rb') as f_in:
        with open(dest_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    logging.info("Extraction complete.")

# === Step 3: Load TSV to Snowflake ===
def load_to_snowflake(tsv_file):
    logging.info(f"Reading TSV file {tsv_file}...")
    df = pd.read_csv(tsv_file, sep='\t')
    logging.info(f"DataFrame loaded with {len(df)} records and {len(df.columns)} columns.")

    connection_string = (
        f'snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/'
        f'{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}'
    )
    logging.info("Connecting to Snowflake...")
    engine = create_engine(connection_string)

    logging.info(f"Uploading data to Snowflake table '{SNOWFLAKE_TABLE}'...")
    df.to_sql(SNOWFLAKE_TABLE, engine, if_exists='replace', index=False)
    logging.info("Upload to Snowflake complete.")

# === Main Execution ===
if __name__ == "__main__":
    try:
        download_file(DOWNLOAD_URL, FILENAME_GZ)
        extract_gz(FILENAME_GZ, FILENAME_TSV)
        load_to_snowflake(FILENAME_TSV)
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        # Clean up files
        for file in [FILENAME_GZ, FILENAME_TSV]:
            if os.path.exists(file):
                os.remove(file)
                logging.info(f"Removed temporary file: {file}")
        logging.info("Script completed.")
