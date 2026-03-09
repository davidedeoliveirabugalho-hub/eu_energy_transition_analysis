# European Energy Transition Analysis

## 📖 Description

This project analyzes the European energy transition from 2015 to 2024, exploring how countries have evolved their electricity mix toward renewable sources and the factors influencing this transformation.

## 🎯 Objectives

This project aims to answer key questions about Europe's energy transition:

- **Energy Mix Evolution**: How has the share of renewable vs. fossil fuels changed across European countries?
- **Consumption Trends**: What are the patterns in electricity consumption by country and over time?
- **Transition Leaders**: Which countries are leading the renewable energy adoption?
- **Price Impact**: How do energy prices correlate with the energy mix composition?

## 🛠️ Tech Stack

- **Data Ingestion**: Airbyte
- **Data Warehouse**: Google Cloud Platform (BigQuery)
- **Data Transformation**: dbt (data build tool)
- **Data Visualization**: Metabase
- **Version Control**: Git & GitHub
- **CI/CD**: GitHub Actions
- **Containerization**: Docker

## 🏗️ Architecture

Coming soon...

## 🏗️ Architecture & Decisions

For detailed technical decisions and alternatives explored, see [docs/technical_decisions.md](docs/technical_decisions.md)

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- Google Cloud Platform account with BigQuery enabled
- ENTSO-E API key ([register here](https://transparency.entsoe.eu/))

### 1. Clone the repository
```bash
git clone https://github.com/davidedeoliveirabugalho-hub/eu_energy_transition_analysis.git
cd eu_energy_transition_analysis
```

### 2. Set up environment
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure credentials
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials:
# - ENTSOE_API_KEY: Your ENTSO-E API key
# - GCP_PROJECT_ID: Your Google Cloud project ID
# - BIGQUERY_DATASET: Keep as 'analytics_bronze'
# - GOOGLE_APPLICATION_CREDENTIALS: Path to your GCP service account key
```

### 4. Create BigQuery datasets
The project uses a medallion architecture with three layers:
```bash
# Create the datasets (requires Google Cloud SDK installed)
bq mk --dataset --location=EU your-project-id:analytics_bronze
bq mk --dataset --location=EU your-project-id:analytics_silver
bq mk --dataset --location=EU your-project-id:analytics_gold
```

Or create them manually in [BigQuery Console](https://console.cloud.google.com/bigquery).

### 5. Run data ingestion
```bash
# Ingests last 30 days of ENTSO-E data by default
python scripts/ingest_entsoe_data.py

# Or specify custom date range
python scripts/ingest_entsoe_data.py --start 2025-01-01 --end 2025-01-31
```

### 6. Run dbt transformations
```bash
cd dbt
dbt run
```

## 📊 Key Insights

Coming soon...

## 📚 Data Sources

- **[ENTSO-E Transparency Platform](https://transparency.entsoe.eu/)** - European electricity generation and consumption data by source
- **[Open Power System Data](https://open-power-system-data.org/)** - Historical energy production and consumption datasets
- **[EEX/EPEX Spot](https://www.epexspot.com/)** - European energy market prices
- **[ERA5 (Copernicus)](https://cds.climate.copernicus.eu/)** - Climate and weather data for renewable energy correlation analysis

## Data Ingestion Strategy

**Hybrid Approach:**
- **Python Custom Scripts** (ENTSO-E API)
  - When: No native connector available
  - Why: Maximum flexibility and control
  
- **Airbyte** (Open Power System Data)
  - When: Native connectors available
  - Why: Faster setup, built-in monitoring



## 📝 Project Status

🟡 **In Progress** - Currently setting up the data pipeline infrastructure