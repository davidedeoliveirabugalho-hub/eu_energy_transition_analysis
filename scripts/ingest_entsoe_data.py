# === Step 1: imports ===

"""
ENTSO-E Data Ingestion Pipeline
Fetches electricity generation data from ENTSO-E API and loads to BigQuery bronze layer.

Modes:
- Incremental (default): Auto-detects last ingested date per table, ingests only new data
- Explicit: Use --start and --end to specify exact date range (for backfill)

Usage:
    # Incremental mode (recommended for daily runs)
    python scripts/ingest_entsoe_data.py

    # Explicit mode (for initial load or backfill)
    python scripts/ingest_entsoe_data.py --start 2025-06-01 --end 2026-03-13

    # With caffeinate to prevent Mac sleep
    caffeinate -i python scripts/ingest_entsoe_data.py
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
    cleaned = re.sub(r"[^a-z0-9_]", "_", cleaned)

    # 3. Replace multiple underscores with a single one
    cleaned = re.sub(r"_+", "_", cleaned)

    # 4. Remove the underscores at the beginning and end
    cleaned = cleaned.strip("_")

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
        "A75": "bronze_entsoe_a75_actual_generation",
        "A73": "bronze_entsoe_a73_generation_forecast",
        "A68": "bronze_entsoe_a68_installed_capacity",
    }

    return TABLE_NAME_MAPPING.get(
        document_type, f"bronze_entsoe_{document_type.lower()}"
    )


# === Step 2.5: Get Last Ingested Date (NEW - Incremental Logic) ===


def get_last_ingested_date(project_id, dataset, table_name, country_code=None):
    """
    Query BigQuery to find the last ingested date for a table.

    Args:
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table_name: Table name (e.g., 'bronze_entsoe_a75_actual_generation')
        country_code: Optional - filter by country (e.g., 'FR', 'DE')

    Returns:
        datetime.date or None if table is empty/doesn't exist
    """
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table_name}"

    # Filter by country if provided
    if country_code:
        query = f"""
        SELECT MAX(DATE(index)) as last_date 
        FROM `{table_id}`
        WHERE country_code = '{country_code}'
        """
    else:
        query = f"""
        SELECT MAX(DATE(index)) as last_date 
        FROM `{table_id}`
        """

    try:
        result = client.query(query).result()
        row = list(result)[0]
        if row.last_date is None:
            return None
        return row.last_date
    except Exception as e:
        print(f"ℹ️  No existing data found in {table_name}")
        return None


# === Step 3: parse_arguments() function (MODIFIED for incremental) ===


def parse_arguments():
    """
    Parse command line arguments for date range.

    Modes:
    - If --start provided: Use explicit date range (full/backfill mode)
    - If no --start: Incremental mode (auto-detect from last ingested date)

    Returns:
        dict with keys: start_date, end_date, default_start, mode
    """
    parser = argparse.ArgumentParser(
        description="Ingest ENTSO-E data to BigQuery bronze layer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Incremental mode (daily runs)
  python scripts/ingest_entsoe_data.py
  
  # Full backfill from specific date
  python scripts/ingest_entsoe_data.py --start 2025-06-01
  
  # Specific date range
  python scripts/ingest_entsoe_data.py --start 2025-06-01 --end 2025-12-31
  
  # Change default start for first-time ingestion
  python scripts/ingest_entsoe_data.py --default-start 2024-01-01
        """,
    )

    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD). If omitted, uses incremental mode (auto-detect).",
    )
    parser.add_argument(
        "--end", type=str, help="End date (YYYY-MM-DD). Defaults to today."
    )
    parser.add_argument(
        "--default-start",
        type=str,
        default="2025-06-01",
        help="Default start date for first-time ingestion when tables are empty (YYYY-MM-DD). Default: 2025-06-01",
    )

    args = parser.parse_args()

    # End date: always today if not provided
    if args.end:
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    else:
        end_date = datetime.now()

    # Start date: explicit or None (incremental mode)
    if args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        mode = "explicit"
        print(f"📋 Mode: EXPLICIT (backfill)")
        print(
            f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )
    else:
        start_date = None  # Will be determined per-table
        mode = "incremental"
        print(f"🔄 Mode: INCREMENTAL (auto-detect last ingested date)")
        print(f"📅 End date: {end_date.strftime('%Y-%m-%d')}")

    default_start = datetime.strptime(args.default_start, "%Y-%m-%d")

    return {
        "start_date": start_date,
        "end_date": end_date,
        "default_start": default_start,
        "mode": mode,
    }


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
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
        countries = config["countries"]
        documents = config["documents"]

    return {
        "api_key": api_key,
        "project_id": project_id,
        "dataset": dataset,
        "countries": countries,
        "documents": documents,
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
        "A75": "query_generation",
        "A73": "query_generation_per_plant",
        "A68": "query_installed_generation_capacity",
    }

    # 1. Convert dates to pandas Timestamp with timezone
    start = pd.Timestamp(start_date).tz_localize("Europe/Brussels")
    end = pd.Timestamp(end_date).tz_localize("Europe/Brussels")

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
        df_prepared.columns = [
            "_".join(map(str, col)).strip() for col in df_prepared.columns
        ]

    # 1.3 Convert all column names to strings
    df_prepared.columns = df_prepared.columns.astype(str)

    # 1.4 Sanitize column names for BigQuery compatibility
    df_prepared.columns = [sanitize_column_name(col) for col in df_prepared.columns]

    # 1.5 Add metadata columns
    df_prepared["country_code"] = country_code
    df_prepared["document_type"] = document_type
    df_prepared["ingestion_timestamp"] = datetime.now()

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
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )

    # 5. Load the DataFrame
    try:
        job = client.load_table_from_dataframe(
            df_prepared, table_id, job_config=job_config
        )
        job.result()

        print(f"✅ Successfully loaded {len(df_prepared)} rows to BigQuery")
        return True

    except Exception as e:
        print(f"❌ Error loading to BigQuery: {e}")
        return False


# === Step 8: main() function (MODIFIED for incremental) ===


def main():
    """Main pipeline orchestration with incremental support"""

    print("=" * 60)
    print("🚀 ENTSO-E Data Ingestion Pipeline")
    print("=" * 60)

    # 1. Parse arguments
    date_config = parse_arguments()

    # 2. Load configuration
    config = load_configuration()

    # 3. Initialize ENTSO-E client
    client = initialize_entsoe_client(config["api_key"])

    # Stats tracking
    stats = {
        "total_tables": 0,
        "tables_updated": 0,
        "tables_skipped": 0,
        "tables_failed": 0,
        "total_rows": 0,
    }

    # 4. Loop through countries and documents
    for country_code in config["countries"].keys():

        for document in config["documents"]:
            document_type = document["type"]
            table_name = get_table_name(document_type)
            stats["total_tables"] += 1

            print("-" * 40)
            print(f"📊 Processing: {country_code} / {document_type}")

            # === INCREMENTAL LOGIC ===
            if date_config["mode"] == "incremental":
                # Find last ingested date for this specific table
                last_date = get_last_ingested_date(
                    config["project_id"], config["dataset"], table_name, country_code
                )

                if last_date is None:
                    # First ingestion - use default start
                    start_date = date_config["default_start"]
                    print(
                        f"📦 No existing data - Starting from {start_date.strftime('%Y-%m-%d')}"
                    )
                else:
                    # Incremental - start from last_date + 1 day
                    start_date = datetime.combine(
                        last_date, datetime.min.time()
                    ) + timedelta(days=1)
                    print(
                        f"🔄 Data exists until {last_date} - Ingesting from {start_date.strftime('%Y-%m-%d')}"
                    )
            else:
                start_date = date_config["start_date"]

            end_date = date_config["end_date"]

            # Skip if already up to date
            if start_date.date() > end_date.date():
                print(f"✅ Already up to date!")
                stats["tables_skipped"] += 1
                continue

            print(
                f"📅 Date range: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}"
            )

            # Fetch data from API
            df = fetch_data(client, country_code, document_type, start_date, end_date)

            # Load to BigQuery
            if df is not None and not df.empty:
                success = load_to_bigquery(
                    df,
                    config["project_id"],
                    config["dataset"],
                    table_name,
                    country_code,
                    document_type,
                )
                if success:
                    stats["tables_updated"] += 1
                    stats["total_rows"] += len(df)
                else:
                    stats["tables_failed"] += 1
            else:
                stats["tables_failed"] += 1

    # Print summary
    print("=" * 60)
    print("📈 PIPELINE SUMMARY")
    print("=" * 60)
    print(f"   Total tables processed: {stats['total_tables']}")
    print(f"   ✅ Updated: {stats['tables_updated']}")
    print(f"   ⏭️  Skipped (up to date): {stats['tables_skipped']}")
    print(f"   ❌ Failed: {stats['tables_failed']}")
    print(f"   📊 Total rows ingested: {stats['total_rows']:,}")
    print("=" * 60)
    print("✅ Pipeline completed!")
    print("=" * 60)


# === Step 9: entrance point ===

if __name__ == "__main__":
    main()
