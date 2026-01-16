# Technical Decisions & Learning Process

## Data Ingestion from ENTSO-E API

### Initial Exploration: Manual API Integration

Before implementing the final solution, I conducted a thorough analysis of the ENTSO-E Transparency Platform API to understand its structure and requirements.

#### API Analysis
**Base URL:** `https://web-api.tp.entsoe.eu/api`

**Required Parameters:**
- `documentType` (e.g., A75 for actual generation per type)
- `processType` (e.g., A16 for realized data)
- `in_Domain` (EIC code for the country)
- `periodStart` / `periodEnd` (format: YYYYMMDDHHmm)
- `securityToken` (API key)

**Response Format:** XML

#### Manual Implementation Functions

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

### Final Decision: Using entsoe-py Library

After this exploration phase, I made the strategic decision to use the `entsoe-py` library instead of maintaining custom API integration code.

#### Rationale

**✅ Advantages of entsoe-py:**
1. **Encapsulation**: Handles API calls, XML parsing, and DataFrame transformation
2. **Robustness**: Manages ENTSO-E-specific edge cases and errors
3. **Maintenance**: Community-maintained and regularly updated
4. **Industry Standard**: Widely used in energy data projects
5. **Time Efficiency**: Faster development without sacrificing quality

**🎯 Engineering Principle Applied:**
> "Don't Reinvent the Wheel" - Use established libraries for common tasks while maintaining deep understanding of underlying mechanisms.

#### Why the Exploration Was Valuable

The manual implementation phase was essential because:
- **Understanding**: Deep knowledge of API structure and authentication
- **Debugging**: Ability to troubleshoot issues if entsoe-py fails
- **Interviews**: Can explain both approaches and justify architectural decisions
- **Flexibility**: Could switch to custom implementation if needed

### Implementation Comparison

**Manual Approach (Explored):**
```python
# Multiple steps required
config = load_configuration()
url = build_api_url(doc_type, process, country, start, end, key)
xml_data = fetch_data_from_api(url)
df = parse_xml_to_dataframe(xml_data)  # Complex XML parsing needed
```

**Final Approach (entsoe-py):**
```python
# Simplified with library
from entsoe import EntsoePandasClient

client = EntsoePandasClient(api_key=api_key)
df = client.query_generation(country_code='FR', start=start, end=end)
```

### Lessons Learned

1. **Research First**: Always check for existing libraries before building custom solutions
2. **Understand Before Using**: Exploring the manual approach provided crucial API knowledge
3. **Pragmatic Decisions**: Choose tools that balance development speed and code maintainability
4. **Document Decisions**: Keep track of alternatives explored and rationale for final choices

---

*This decision reflects professional data engineering practices: thoroughly understand the problem space, then leverage appropriate tools for efficient implementation.*