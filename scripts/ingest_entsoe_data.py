# === Step 1: imports ===

"""
ENTSO-E Data Ingestion Pipeline
Fetches electricity generation data from ENTSO-E API and loads to BigQuery bronze layer.
"""

import os
import argparse
import yaml
from datetime import datetime, timedelta
from dotenv import load_dotenv

import pandas as pd
from entsoe import EntsoePandasClient
from google.cloud import bigquery

# === Step 2: parse_arguments() function ===

def parse_arguments():
    """
    Parse command line arguments for date range.
    If no arguments provided, defaults to last 30 days.
    """
    parser = argparse.ArgumentParser(
        description='Ingest ENTSO-E data to BigQuery bronze layer'
    )
    
    parser.add_argument(
        '--start',
        type=str,
        help='Start date (format: YYYY-MM-DD). Defaults to 30 days ago.'
    )
    parser.add_argument(
        '--end',
        type=str,
        help='End date (format: YYYY-MM-DD). Defaults to today.'
    )
    
    args = parser.parse_args()
    
    # Calculate dates
    if not args.start or not args.end:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        print(f"ℹ️  No dates provided. Using default: last 30 days")
    else:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
    
    print(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    return start_date, end_date

# === Step 3: load_configuration() function ===

def load_configuration():
    """Load configuration from .env and config.yaml"""
    
    # 1.1 load .env file
    load_dotenv()
    
    # 1.2 Get environment variables
    api_key = os.getenv("ENTSOE_API_KEY")
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BIGQUERY_DATASET")
    table = os.getenv("BIGQUERY_TABLE")
    
    # 1.3 Load config.yaml
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
        countries = config['countries']
        documents = config['documents']
    
    return {
        "api_key": api_key,
        "project_id": project_id,
        "dataset": dataset,
        "table": table,
        "countries": countries,
        "documents": documents
    }

# === Step 4: initialize_entsoe_client() function ===

def initialize_entsoe_client(api_key):
    """Initialize ENTSO-E client with API key"""

    client = EntsoePandasClient(api_key=api_key)


    return client

# === Step 5: fetch_data() function ===

def fetch_data(client, country_code, document_type, start_date, end_date):
    """
    Fetch data from ENTSO-E API for a specific country and document type.
    
    Args:
        client: EntsoePandasClient instance
        country_code: ISO country code (e.g., 'FR', 'DE')
        document_type: ENTSO-E document type (e.g., 'A75')
        start_date: Start datetime
        end_date: End datetime
    
    Returns:
        pandas.DataFrame or None if error
    """
    
    # Mapping document types to entsoe-py methods
    DOCUMENT_TYPE_MAPPING = {
        'A75': 'query_generation',
        'A73': 'query_generation_per_plant',
        'A68': 'query_installed_generation_capacity'
    }
    
    # 1. Convert dates to pandas Timestamp with timezone
    start = pd.Timestamp(start_date).tz_localize('Europe/Brussels')
    end = pd.Timestamp(end_date).tz_localize('Europe/Brussels')
    
    # 2. Check if document type is supported
    if document_type not in DOCUMENT_TYPE_MAPPING:
        print(f"⚠️  Document type {document_type} not supported")
        return None
    
    # 3. Get method name
    method_name = DOCUMENT_TYPE_MAPPING[document_type]
    
    # 4. Get the method from the client
    method = getattr(client, method_name)
    
    # 5. Call the method with parameters
    print(f"🔄 Fetching {document_type} data for {country_code}...")
    
    try:
        df = method(country_code, start=start, end=end)
        print(f"✅ Retrieved {len(df)} rows")
        return df
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None
    
# === Step 6: load_to_bigquery() function ===

def load_to_bigquery(df, project_id, dataset, table):
    """
    Load DataFrame to BigQuery bronze layer.
    
    Args:
        df: pandas.DataFrame to load
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table: BigQuery table name
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    if df is None or df.empty:
        print("⚠️  No data to load")
        return False
    
    # ====== NOUVELLE SECTION : Préparer le DataFrame ======
    # Reset index to convert it to columns
    df_prepared = df.reset_index()
    
    # Flatten column names if they are tuples
    if isinstance(df_prepared.columns, pd.MultiIndex):
        df_prepared.columns = ['_'.join(map(str, col)).strip() for col in df_prepared.columns]
    
    # Convert all column names to strings
    df_prepared.columns = df_prepared.columns.astype(str)
    # ======================================================
    
    # 1. Construct full table ID
    table_id = f"{project_id}.{dataset}.{table}"
    
    print(f"📤 Loading {len(df_prepared)} rows to {table_id}...")
    
    # 2. Create BigQuery client with credentials
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        print(f"🔑 Using credentials: {credentials_path}")
    else:
        print("⚠️  No credentials found, using default authentication")

    client = bigquery.Client(project=project_id)
    
    # 3. Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED"
    )
    
    # 4. Load the DataFrame
    try:
        job = client.load_table_from_dataframe(
            df_prepared, table_id, job_config=job_config  # ← Utilise df_prepared
        )
        job.result()
        
        print(f"✅ Successfully loaded {len(df_prepared)} rows to BigQuery")
        return True
        
    except Exception as e:
        print(f"❌ Error loading to BigQuery: {e}")
        return False
    
# === Step 7: main() function ===

def main():
    """Main pipeline orchestration"""
    
    print("=" * 60)
    print("🚀 ENTSO-E Data Ingestion Pipeline")
    print("=" * 60)
    
    # 1. Parse arguments (dates)
    start_date, end_date = parse_arguments()
    
    # 2. Load configuration
    config = load_configuration()
    
    # 3. Initialize ENTSO-E client
    client = initialize_entsoe_client(config['api_key'])
    
    # 4. Loop through countries and documents
    for country_code in config['countries'].keys():
        
        # Loop through each document type
        for document in config['documents']:
            
            # Get document type
            document_type = document['type']
            
            # Fetch data from API
            df = fetch_data(client, country_code, document_type, start_date, end_date)
            
            # Load to BigQuery if data retrieved successfully
            if df is not None:
                load_to_bigquery(df, config['project_id'], config['dataset'], config['table'])
    
    print("=" * 60)
    print("✅ Pipeline completed!")
    print("=" * 60)

# === Step 8: entrance point ===

if __name__ == "__main__":
    main()