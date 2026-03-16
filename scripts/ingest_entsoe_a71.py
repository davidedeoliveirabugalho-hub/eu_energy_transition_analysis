"""
ENTSO-E A71 Data Ingestion Pipeline
Fetches Installed Capacity Per Production Unit from ENTSO-E API and loads to BigQuery bronze layer.

Document Type A71 provides detailed capacity data per production unit (power plant level).
Uses REST API directly (not entsoe-py) due to domain code issues in the library.

Countries with A71 data: FR, UK, IT, NL, BE, AT, ES (7 countries)
Countries without A71 data: PT, DE (use A68 for Portugal)
"""

import os
import argparse
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv

import pandas as pd
from google.cloud import bigquery


# =============================================================================
# CONFIGURATION
# =============================================================================

# Correct ENTSO-E domain codes (critical for API to work!)
DOMAIN_CODES = {
    "FR": "10YFR-RTE------C",
    "UK": "10YGB----------A",
    "IT": "10YIT-GRTN-----B",
    "NL": "10YNL----------L",
    "BE": "10YBE----------2",
    "AT": "10YAT-APG------L",
    "ES": "10YES-REE------0",
    "PT": "10YPT-REN------W",
    "DE": "10Y1001A1001A83F",
    # Note: PT and DE currently return "Acknowledgement" (no data)
    # but we include them so future data will be captured automatically
}

# ENTSO-E API endpoint
ENTSOE_API_URL = "https://web-api.tp.entsoe.eu/api"

# XML namespace for parsing
NS = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def parse_arguments():
    """
    Parse command line arguments for date range.
    If no arguments provided, defaults to current year.
    """
    parser = argparse.ArgumentParser(
        description="Ingest ENTSO-E A71 (Installed Capacity Per Unit) data to BigQuery"
    )

    parser.add_argument(
        "--start",
        type=str,
        help="Start date (format: YYYY-MM-DD). Defaults to start of current year.",
    )
    parser.add_argument(
        "--end", type=str, help="End date (format: YYYY-MM-DD). Defaults to today."
    )
    parser.add_argument(
        "--countries",
        type=str,
        nargs="+",
        default=list(DOMAIN_CODES.keys()),
        help="Countries to ingest (default: all available)",
    )

    args = parser.parse_args()

    # Calculate dates
    if not args.start or not args.end:
        end_date = datetime.now()
        start_date = datetime(end_date.year, 1, 1)  # Start of current year
        print(f"ℹ️  No dates provided. Using default: current year")
    else:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")

    print(
        f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )
    print(f"🌍 Countries: {', '.join(args.countries)}")

    return start_date, end_date, args.countries


def load_configuration():
    """Load configuration from .env file"""
    load_dotenv()

    api_key = os.getenv("ENTSOE_API_KEY")
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BIGQUERY_DATASET", "analytics_bronze")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not api_key:
        raise ValueError("ENTSOE_API_KEY not found in .env file")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID not found in .env file")

    return {
        "api_key": api_key,
        "project_id": project_id,
        "dataset": dataset,
        "credentials_path": credentials_path,
    }


def format_date_for_api(dt):
    """Format datetime for ENTSO-E API (YYYYMMDDHHMM format)"""
    return dt.strftime("%Y%m%d%H%M")


def fetch_a71_data(api_key, country_code, start_date, end_date):
    """
    Fetch A71 (Installed Capacity Per Production Unit) data from ENTSO-E API.

    Args:
        api_key: ENTSO-E API security token
        country_code: ISO country code (e.g., 'FR', 'DE')
        start_date: Start datetime
        end_date: End datetime

    Returns:
        str: Raw XML response or None if error
    """

    if country_code not in DOMAIN_CODES:
        print(f"⚠️  {country_code}: Not available for A71 (no domain code)")
        return None

    domain = DOMAIN_CODES[country_code]

    params = {
        "documentType": "A71",
        "processType": "A33",  # Year ahead
        "in_Domain": domain,
        "periodStart": format_date_for_api(start_date),
        "periodEnd": format_date_for_api(end_date),
        "securityToken": api_key,
    }

    print(f"🔄 Fetching A71 data for {country_code}...")

    try:
        response = requests.get(ENTSOE_API_URL, params=params, timeout=60)

        if response.status_code == 200:
            # Check if it's real data or acknowledgement (error)
            if "GL_MarketDocument" in response.text:
                print(f"✅ {country_code}: {len(response.text):,} chars received")
                return response.text
            elif "Acknowledgement" in response.text:
                print(f"⚠️  {country_code}: No data available (Acknowledgement)")
                return None
            else:
                print(f"❓ {country_code}: Unknown response format")
                return None
        else:
            print(f"❌ {country_code}: HTTP {response.status_code}")
            return None

    except Exception as e:
        print(f"❌ {country_code}: Error - {str(e)[:100]}")
        return None


def parse_a71_xml(xml_content, country_code):
    """
    Parse A71 XML response into a pandas DataFrame.

    A71 structure:
    - TimeSeries (one per production unit)
      - registeredResource.mRID (unit ID)
      - registeredResource.name (unit name)
      - MktPSRType.psrType (energy type code)
      - Period
        - timeInterval (start/end)
        - Point (quantity = installed capacity MW)

    Args:
        xml_content: Raw XML string from API
        country_code: ISO country code

    Returns:
        pandas.DataFrame with parsed data
    """

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"❌ XML parse error for {country_code}: {e}")
        return None

    records = []

    # Find all TimeSeries elements
    for ts in root.findall(".//ns:TimeSeries", NS):

        # Extract unit information
        unit_id_elem = ts.find(".//ns:registeredResource.mRID", NS)
        unit_name_elem = ts.find(".//ns:registeredResource.name", NS)
        psr_type_elem = ts.find(".//ns:MktPSRType/ns:psrType", NS)

        # Location information (if available)
        lat_elem = ts.find(".//ns:location/ns:latitude", NS)
        lon_elem = ts.find(".//ns:location/ns:longitude", NS)

        unit_id = unit_id_elem.text if unit_id_elem is not None else None
        unit_name = unit_name_elem.text if unit_name_elem is not None else None
        psr_type = psr_type_elem.text if psr_type_elem is not None else None
        latitude = float(lat_elem.text) if lat_elem is not None else None
        longitude = float(lon_elem.text) if lon_elem is not None else None

        # Find all periods for this unit
        for period in ts.findall(".//ns:Period", NS):

            # Get time interval (XML uses timeInterval.start, not timeInterval/start)
            start_elem = period.find("ns:timeInterval.start", NS)
            end_elem = period.find("ns:timeInterval.end", NS)

            period_start = start_elem.text if start_elem is not None else None
            period_end = end_elem.text if end_elem is not None else None

            # Get capacity value from Point
            for point in period.findall(".//ns:Point", NS):
                quantity_elem = point.find("ns:quantity", NS)

                if quantity_elem is not None:
                    try:
                        capacity_mw = float(quantity_elem.text)
                    except (ValueError, TypeError):
                        capacity_mw = None

                    records.append(
                        {
                            "country_code": country_code,
                            "unit_id": unit_id,
                            "unit_name": unit_name,
                            "psr_type": psr_type,
                            "period_start": period_start,
                            "period_end": period_end,
                            "installed_capacity_mw": capacity_mw,
                            "latitude": latitude,
                            "longitude": longitude,
                        }
                    )

    if not records:
        print(f"⚠️  {country_code}: No records parsed from XML")
        return None

    df = pd.DataFrame(records)
    print(f"📊 {country_code}: Parsed {len(df)} records")

    return df


def map_psr_type_to_energy(psr_type):
    """
    Map ENTSO-E PSR type codes to human-readable energy types.

    Reference: ENTSO-E code list
    """
    PSR_TYPE_MAPPING = {
        "B01": "Biomass",
        "B02": "Fossil Brown coal/Lignite",
        "B03": "Fossil Coal-derived gas",
        "B04": "Fossil Gas",
        "B05": "Fossil Hard coal",
        "B06": "Fossil Oil",
        "B07": "Fossil Oil shale",
        "B08": "Fossil Peat",
        "B09": "Geothermal",
        "B10": "Hydro Pumped Storage",
        "B11": "Hydro Run-of-river and poundage",
        "B12": "Hydro Water Reservoir",
        "B13": "Marine",
        "B14": "Nuclear",
        "B15": "Other renewable",
        "B16": "Solar",
        "B17": "Waste",
        "B18": "Wind Offshore",
        "B19": "Wind Onshore",
        "B20": "Other",
        "B21": "AC Link",
        "B22": "DC Link",
        "B23": "Substation",
        "B24": "Transformer",
    }

    return PSR_TYPE_MAPPING.get(psr_type, f"Unknown ({psr_type})")


def enrich_dataframe(df):
    """
    Enrich the DataFrame with additional columns.
    """
    if df is None or df.empty:
        return df

    # Add energy type name
    df["energy_type"] = df["psr_type"].apply(map_psr_type_to_energy)

    # Add renewable flag
    RENEWABLE_TYPES = [
        "B01",
        "B09",
        "B10",
        "B11",
        "B12",
        "B13",
        "B15",
        "B16",
        "B18",
        "B19",
    ]
    df["is_renewable"] = df["psr_type"].isin(RENEWABLE_TYPES).astype(bool)

    # Add document type and ingestion timestamp
    df["document_type"] = "A71"
    df["ingestion_timestamp"] = datetime.now()

    # Keep period dates as strings (ISO format) - BigQuery will parse them
    #df["period_start"] = pd.to_datetime(df["period_start"])
    #df["period_end"] = pd.to_datetime(df["period_end"])

    return df


def load_to_bigquery(df, project_id, dataset, table_name, credentials_path=None):
    """
    Load DataFrame to BigQuery bronze layer.

    Args:
        df: pandas.DataFrame to load
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table_name: Table name
        credentials_path: Path to GCP service account key

    Returns:
        bool: True if successful, False otherwise
    """

    if df is None or df.empty:
        print("⚠️  No data to load")
        return False

    # Set credentials if provided
    if credentials_path and os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        print(f"🔑 Using credentials: {credentials_path}")

    # Create BigQuery client
    client = bigquery.Client(project=project_id)

    # Construct full table ID
    table_id = f"{project_id}.{dataset}.{table_name}"

    print(f"📤 Loading {len(df)} rows to {table_id}...")

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )

    # Ensure boolean type for is_renewable (fixes pyarrow string conversion)
    if "is_renewable" in df.columns:
        df["is_renewable"] = df["is_renewable"].astype(bool)

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion

        print(f"✅ Successfully loaded {len(df)} rows to BigQuery")
        return True

    except Exception as e:
        print(f"❌ Error loading to BigQuery: {e}")
        return False


def get_last_ingested_date(
    project_id, dataset, table_name, country_code, credentials_path=None
):
    """
    Query BigQuery to find the last ingested date for a country.

    Args:
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table_name: Table name
        country_code: ISO country code
        credentials_path: Path to GCP credentials

    Returns:
        datetime.date or None if table is empty/doesn't exist
    """
    if credentials_path and os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table_name}"

    query = f"""
    SELECT MAX(DATE(period_start)) as last_date 
    FROM `{table_id}`
    WHERE country_code = '{country_code}'
    """

    try:
        result = client.query(query).result()
        row = list(result)[0]
        if row.last_date is None:
            return None
        return row.last_date
    except Exception as e:
        print(f"ℹ️  No existing A71 data found for {country_code}")
        return None


# =============================================================================
# MAIN PIPELINE
# =============================================================================


def main():
    """Main pipeline orchestration"""

    print("=" * 60)
    print("🚀 ENTSO-E A71 Data Ingestion Pipeline")
    print("   Installed Capacity Per Production Unit")
    print("=" * 60)

    # 1. Parse arguments
    start_date, end_date, countries = parse_arguments()

    # 2. Load configuration
    config = load_configuration()

    # 3. Statistics
    stats = {"processed": 0, "success": 0, "failed": 0, "skipped": 0, "total_rows": 0}

    # 4. Process each country
    table_name = "bronze_entsoe_a71_installed_capacity_per_unit"

    for country_code in countries:
        stats["processed"] += 1

        # Check if country has A71 data
        if country_code not in DOMAIN_CODES:
            print(f"⏭️  {country_code}: Skipped (no A71 data available)")
            stats["skipped"] += 1
            continue

        # Fetch data
        xml_content = fetch_a71_data(
            config["api_key"], country_code, start_date, end_date
        )

        if xml_content is None:
            stats["failed"] += 1
            continue

        # Parse XML
        df = parse_a71_xml(xml_content, country_code)

        if df is None or df.empty:
            stats["failed"] += 1
            continue

        # Enrich data
        df = enrich_dataframe(df)

        # Load to BigQuery
        success = load_to_bigquery(
            df,
            config["project_id"],
            config["dataset"],
            table_name,
            config["credentials_path"],
        )

        if success:
            stats["success"] += 1
            stats["total_rows"] += len(df)
        else:
            stats["failed"] += 1

    # 5. Print summary
    print("\n" + "=" * 60)
    print("📈 PIPELINE SUMMARY")
    print("=" * 60)
    print(f"   Countries processed: {stats['processed']}")
    print(f"   ✅ Success: {stats['success']}")
    print(f"   ⏭️  Skipped: {stats['skipped']}")
    print(f"   ❌ Failed: {stats['failed']}")
    print(f"   📊 Total rows ingested: {stats['total_rows']:,}")
    print("=" * 60)
    print("✅ Pipeline completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
