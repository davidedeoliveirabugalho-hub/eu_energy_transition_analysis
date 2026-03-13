"""
Quick test to check if A71 (Installed Capacity Per Unit)
provides more detailed data than A68
"""

from entsoe import EntsoePandasClient
import pandas as pd
import os
from dotenv import load_dotenv

# Load API key
load_dotenv()
api_key = os.getenv("ENTSOE_API_KEY")

if not api_key:
    print("❌ ENTSOE_API_KEY not found in .env")
    exit(1)

client = EntsoePandasClient(api_key=api_key)

# Test period
start = pd.Timestamp("20250101", tz="Europe/Brussels")
end = pd.Timestamp("20250131", tz="Europe/Brussels")

# Countries to test
countries = ["PT", "FR", "DE", "ES", "IT", "NL", "BE"]

print("=" * 60)
print("TEST A71 - Installed Capacity Per Production Unit")
print("=" * 60)

for country in countries:
    try:
        data = client.query_installed_generation_capacity_per_unit(
            country_code=country, start=start, end=end
        )

        print(f"\n✅ {country}: {len(data)} rows received")
        print(f"   Columns: {list(data.columns)[:5]}...")  # First 5 columns

        # Display sample for first country with data
        if len(data) > 0 and country == countries[0]:
            print(f"\n📊 Sample data for {country}:")
            print(data.head())

    except Exception as e:
        print(f"\n❌ {country}: Error - {str(e)[:100]}")

print("\n" + "=" * 60)
print("CONCLUSION:")
print("If multiple countries have data → A71 is better than A68")
print("If only Portugal has data → Modifying dbt is sufficient")
print("=" * 60)
