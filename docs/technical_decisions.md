# Technical Decisions & Learning Process

## Project Overview

This document tracks the technical decisions, phases, and learning process for the EU Energy Transition Analysis project. It serves as both documentation and a reference for explaining architectural choices.

---

## Project Phases

### Phase 1-3: Project Setup & Configuration
**Status**: ✅ Completed

**Key Achievements:**
- Project structure created
- Environment configuration (.env, config.yaml)
- GCP project and BigQuery dataset setup
- GitHub repository initialized

---

### Phase 4: Initial Python Ingestion Script
**Status**: ✅ Completed  
**Date**: January 2026

**Objective:** Create a Python script to ingest ENTSO-E data into BigQuery bronze layer.

**Implementation:**
- Created `scripts/ingest_entsoe_data.py`
- Implemented command-line argument parsing for date ranges
- Integrated entsoe-py library for API calls
- Basic BigQuery loading functionality

**First Success:**
- ✅ 1,581 rows from France (A75) loaded into BigQuery
- ✅ Configuration working (8 countries, 3 document types)

**Issues Identified:**
- ❌ Invalid column names for BigQuery (special characters)
- ❌ Schema conflicts between document types
- ❌ Schema conflicts between countries
- ❌ Missing metadata (country, document type, ingestion timestamp)

---

### Phase 4bis: ENTSO-E Ingestion Pipeline Finalized
**Status**: ✅ Completed  
**Date**: January 19, 2026

**Context:**
Following Phase 4 which created the base script, Phase 4bis fixed 3 critical issues identified during multi-country testing.

#### Issues Resolved

**1. Invalid BigQuery Column Names**

**Problem**: ENTSO-E API returns columns with special characters (`/`, `.`, `-`, spaces) incompatible with BigQuery.

**Solution**: `sanitize_column_name()` function that:
- Converts to lowercase
- Replaces special characters with underscores
- Removes multiple/leading/trailing underscores
- Applied after all DataFrame transformations

**Example**: `Fossil Gas/Actual Aggregated` → `fossil_gas_actual_aggregated`

**2. Schema Conflicts Between Document Types**

**Problem**: The 3 document types (A75, A73, A68) have different schemas. Impossible to load into a single table.

**Solution**: 3 dynamically generated separate tables:
- `bronze_entsoe_a75_actual_generation` (actual generation)
- `bronze_entsoe_a73_generation_forecast` (generation forecast per plant)
- `bronze_entsoe_a68_installed_capacity` (installed capacity)

`get_table_name()` function for dynamic mapping.

**3. Schema Conflicts Between Countries**

**Problem**: Each country has different columns (e.g., Germany has lignite, France doesn't).

**Solution**: BigQuery configuration with `schema_update_options=["ALLOW_FIELD_ADDITION"]`
- Allows automatic addition of new columns
- Countries without certain energy types have NULL values

**4. Missing Metadata**

**Problem**: Impossible to trace data origin (country, type, ingestion date).

**Solution**: Added 3 metadata columns:
```python
df_prepared['country_code'] = country_code
df_prepared['document_type'] = document_type
df_prepared['ingestion_timestamp'] = datetime.now()
```

#### Results

**Phase C Test (8 countries, 3 documents, 7 days)**:
- ✅ 8 countries successfully ingested (FR, DE, ES, IT, NL, PT, BE, UK)
- ✅ 4,200+ rows in bronze_entsoe_a75_actual_generation
- ✅ 3,000+ rows in bronze_entsoe_a73_generation_forecast
- ✅ Some countries missing certain documents (normal API behavior)

**API Error Handling**: Pipeline continues even if certain data is unavailable (e.g., IT has no A68).

#### Final Architecture
```
scripts/ingest_entsoe_data.py
├── sanitize_column_name()      # Cleans column names
├── get_table_name()             # Generates dynamic table name
├── fetch_data()                 # ENTSO-E API call
├── load_to_bigquery()           # Loading with ALLOW_FIELD_ADDITION
└── main()                       # Orchestration 8 countries × 3 documents

BigQuery bronze layer
├── bronze_entsoe_a75_actual_generation     (variable schema per country)
├── bronze_entsoe_a73_generation_forecast   (variable schema per country)
└── bronze_entsoe_a68_installed_capacity    (variable schema per country)
```

#### Recommended Next Steps
1. Run full ingestion (30 days) for production dataset
2. Create dbt silver layer models for standardization
3. Add data quality tests (null columns, temporal consistency)

---

## Technical Decisions

### Decision 1: Data Ingestion Approach (ENTSO-E API)

#### Initial Exploration: Manual API Integration

Before implementing the final solution, I conducted a thorough analysis of the ENTSO-E Transparency Platform API to understand its structure and requirements.

**API Analysis**

**Base URL:** `https://web-api.tp.entsoe.eu/api`

**Required Parameters:**
- `documentType` (e.g., A75 for actual generation per type)
- `processType` (e.g., A16 for realized data)
- `in_Domain` (EIC code for the country)
- `periodStart` / `periodEnd` (format: YYYYMMDDHHmm)
- `securityToken` (API key)

**Response Format:** XML

**Manual Implementation Functions**

I developed three core functions to understand the ingestion workflow:

**1. Configuration Loading**
```python
def load_configuration():
    """Load configuration from .env and config.yaml"""
    load_dotenv()
    
    # Load secrets from .env
    api_key = os.getenv("ENTSOE_API_KEY")
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BIGQUERY_DATASET")
    table = os.getenv("BIGQUERY_TABLE")
    
    # Load business parameters from config.yaml
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
```

**2. API URL Construction**
```python
def build_api_url(document_type, process_type, in_domain, period_start, period_end, api_key):
    """Build ENTSO-E API URL with parameters"""
    base_url = "https://web-api.tp.entsoe.eu/api"
    
    url = f"{base_url}?documentType={document_type}&processType={process_type}&in_Domain={in_domain}&periodStart={period_start}&periodEnd={period_end}&securityToken={api_key}"
    
    return url
```

**3. HTTP Request Handling**
```python
def fetch_data_from_api(url):
    """Make HTTP request to ENTSO-E API and return data"""
    print(f"🔄 Calling API...")
    
    response = requests.get(url)
    
    if response.status_code == 200:
        print("✅ Data retrieved successfully!")
        return response.text  # XML format
    else:
        print(f"❌ Error {response.status_code}: {response.text}")
        return None
```

#### Final Decision: Using entsoe-py Library

After this exploration phase, I made the strategic decision to use the `entsoe-py` library instead of maintaining custom API integration code.

**Rationale**

**✅ Advantages of entsoe-py:**
1. **Encapsulation**: Handles API calls, XML parsing, and DataFrame transformation
2. **Robustness**: Manages ENTSO-E-specific edge cases and errors
3. **Maintenance**: Community-maintained and regularly updated
4. **Industry Standard**: Widely used in energy data projects
5. **Time Efficiency**: Faster development without sacrificing quality

**🎯 Engineering Principle Applied:**
> "Don't Reinvent the Wheel" - Use established libraries for common tasks while maintaining deep understanding of underlying mechanisms.

**Why the Exploration Was Valuable**

The manual implementation phase was essential because:
- **Understanding**: Deep knowledge of API structure and authentication
- **Debugging**: Ability to troubleshoot issues if entsoe-py fails
- **Interviews**: Can explain both approaches and justify architectural decisions
- **Flexibility**: Could switch to custom implementation if needed

**Implementation Comparison**

*Manual Approach (Explored):*
```python
# Multiple steps required
config = load_configuration()
url = build_api_url(doc_type, process, country, start, end, key)
xml_data = fetch_data_from_api(url)
df = parse_xml_to_dataframe(xml_data)  # Complex XML parsing needed
```

*Final Approach (entsoe-py):*
```python
# Simplified with library
from entsoe import EntsoePandasClient

client = EntsoePandasClient(api_key=api_key)
df = client.query_generation(country_code='FR', start=start, end=end)
```

---

## Lessons Learned

1. **Research First**: Always check for existing libraries before building custom solutions
2. **Understand Before Using**: Exploring the manual approach provided crucial API knowledge
3. **Pragmatic Decisions**: Choose tools that balance development speed and code maintainability
4. **Document Decisions**: Keep track of alternatives explored and rationale for final choices
5. **Test Incrementally**: Phase A → B → C testing strategy prevented major issues in production
6. **Schema Flexibility**: Multi-country data requires flexible schema management (ALLOW_FIELD_ADDITION)
7. **Resilience**: Pipelines must handle missing data gracefully (not all countries publish all document types)

---

*This document reflects professional data engineering practices: thoroughly understand the problem space, leverage appropriate tools, and maintain clear documentation of technical decisions.*