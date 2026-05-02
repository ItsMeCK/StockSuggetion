import pandas as pd
import requests
import io
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_nse_csv(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    }
    
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=10)
    
    response = session.get(url, headers=headers, timeout=10)
    if response.status_code == 200:
        return pd.read_csv(io.StringIO(response.text))
    else:
        logging.error(f"Failed to fetch {url}. Status: {response.status_code}")
        return None

def generate_master_list():
    # Official Archival URL for Nifty 500
    nifty_500_url = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"

    logging.info("Fetching Nifty 500 constituents from NSE Archives...")
    df500 = fetch_nse_csv(nifty_500_url)
    
    if df500 is not None:
        # Clean symbols
        df500['Symbol'] = df500['Symbol'].str.strip().str.upper()
        unique_list = df500[['Symbol', 'Company Name', 'Series', 'ISIN Code']].drop_duplicates()
        
        output_path = "pipeline/master_universe.csv"
        os.makedirs("pipeline", exist_ok=True)
        unique_list.to_csv(output_path, index=False)
        
        logging.info(f"MASTER NIFTY 500 LIST GENERATED: {len(unique_list)} symbols saved to {output_path}")
        return unique_list
    else:
        logging.error("Failed to build master list due to download errors.")
        return None

if __name__ == "__main__":
    generate_master_list()
