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

import re

# === Step 2: Helper Functions ===

def sanitize_column_name(column_name: str) -> str:
    """
    Cleans up a column name to make it compatible with BigQuery.
    
    Args:
        column_name: Raw column name (ex: "Actual Generation/MW")
    
    Returns:
        Sanitized column name (ex: "actual_generation_mw")
    
    Examples:
        >>> sanitize_column_name("Actual Generation/MW")
        'actual_generation_mw'
        >>> sanitize_column_name("Production.Total")
        'production_total'
    """
    # 1. Convert to lowercase
    cleaned = column_name.lower()
    
    # 2. Replace special characters with underscores
    #    Keep only: letters (a-z), numbers (0-9), underscores
    cleaned = re.sub(r'[^a-z0-9_]', '_', cleaned)
    
    # 3. Replace multiple underscores with a single one
    cleaned = re.sub(r'_+', '_', cleaned)
    
    # 4. Remove the underscores at the beginning and end
    cleaned = cleaned.strip('_')
    
    return cleaned

def get_table_name(document_type):
    """
    Generate table name based on document type.
    
    Args:
        document_type: ENTSO-E document type (e.g., 'A75', 'A73', 'A68')
    
    Returns:
        Table name (e.g., 'bronze_entsoe_a75_actual_generation')
    """
    TABLE_NAME_MAPPING = {
        'A75': 'bronze_entsoe_a75_actual_generation',
        'A73': 'bronze_entsoe_a73_generation_forecast',
        'A68': 'bronze_entsoe_a68_installed_capacity'
    }
    
    return TABLE_NAME_MAPPING.get(document_type, f'bronze_entsoe_{document_type.lower()}')

# === Step 3: parse_arguments() function ===

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

# === Step 4: load_configuration() function ===

def load_configuration():
    """Load configuration from .env and config.yaml"""
    
    # 1.1 load .env file
    load_dotenv()
    
    # 1.2 Get environment variables
    api_key = os.getenv("ENTSOE_API_KEY")
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BIGQUERY_DATASET")
    
    # 1.3 Load config.yaml
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
        countries = config['countries']
        documents = config['documents']
    
    return {
        "api_key": api_key,
        "project_id": project_id,
        "dataset": dataset,
        "countries": countries,
        "documents": documents
    }

# === Step 5: initialize_entsoe_client() function ===

def initialize_entsoe_client(api_key):
    """Initialize ENTSO-E client with API key"""

    client = EntsoePandasClient(api_key=api_key)


    return client

# === Step 6: fetch_data() function ===

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
    
# === Step 7: load_to_bigquery() function ===

def load_to_bigquery(df, project_id, dataset, table, country_code, document_type):
    """
    Load DataFrame to BigQuery bronze layer.
    
    Args:
        df: pandas.DataFrame to load
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table: BigQuery table name
        country_code: ISO country code (e.g., 'FR')
        document_type: ENTSO-E document type (e.g., 'A75')
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    # 0. Check if DataFrame is valid
    if df is None or df.empty:
        print("⚠️  No data to load")
        return False
    
    # 1. Prepare DataFrame for BigQuery compatibility
    # 1.1 Reset index to convert it to columns
    df_prepared = df.reset_index()
    
    # 1.2 Flatten column names if they are MultiIndex (tuples)
    if isinstance(df_prepared.columns, pd.MultiIndex):
        df_prepared.columns = ['_'.join(map(str, col)).strip() for col in df_prepared.columns]
    
    # 1.3 Convert all column names to strings
    df_prepared.columns = df_prepared.columns.astype(str)

    # 1.4 Sanitize column names for BigQuery compatibility
    df_prepared.columns = [sanitize_column_name(col) for col in df_prepared.columns]
    
    # 1.5 Add metadata columns
    df_prepared['country_code'] = country_code
    df_prepared['document_type'] = document_type
    df_prepared['ingestion_timestamp'] = datetime.now()
    
    # 2. Construct full table ID
    table_id = f"{project_id}.{dataset}.{table}"
    
    print(f"📤 Loading {len(df_prepared)} rows to {table_id}...")
    
    # 3. Create BigQuery client with credentials
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        print(f"🔑 Using credentials: {credentials_path}")
    else:
        print("⚠️  No credentials found, using default authentication")

    client = bigquery.Client(project=project_id)
    
    # 4. Configure the load job
    job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    autodetect=True,
    create_disposition="CREATE_IF_NEEDED",
    schema_update_options=["ALLOW_FIELD_ADDITION"]  # ← Permet l'ajout de nouvelles colonnes
)
    
    # 5. Load the DataFrame
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
    
# === Step 8: main() function ===

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
                # Generate table name dynamically
                table_name = get_table_name(document_type)
                
                # Load with metadata
                load_to_bigquery(
                    df, 
                    config['project_id'], 
                    config['dataset'], 
                    table_name,          # ← Table dynamique
                    country_code,        # ← Nouveau paramètre
                    document_type        # ← Nouveau paramètre
                )
    
    print("=" * 60)
    print("✅ Pipeline completed!")
    print("=" * 60)

# === Step 9: entrance point ===

if __name__ == "__main__":
    main()