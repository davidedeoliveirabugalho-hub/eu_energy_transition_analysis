"""
Test A71 for all countries with correct domain codes
"""

import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('ENTSOE_API_KEY')

# Correct domain codes from ENTSO-E documentation
DOMAIN_CODES = {
    'PT': '10YPT-RTE------W',
    'FR': '10YFR-RTE------C',
    'DE': '10Y1001A1001A83F',
    'ES': '10YES-REE------0',
    'IT': '10YIT-GRTN-----B',
    'NL': '10YNL----------L',
    'BE': '10YBE----------2',
    'AT': '10YAT-APG------L',
    'UK': '10YGB----------A'
}

url = "https://web-api.tp.entsoe.eu/api"

print("=" * 60)
print("TEST A71 - All Countries with Correct Domain Codes")
print("=" * 60)

for country, domain in DOMAIN_CODES.items():
    params = {
        'documentType': 'A71',
        'processType': 'A33',
        'in_Domain': domain,
        'periodStart': '202601010000',
        'periodEnd': '202601020000',
        'securityToken': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            # Check if it's real data or acknowledgement (error)
            if 'GL_MarketDocument' in response.text:
                print(f"✅ {country}: {len(response.text):,} chars - REAL DATA!")
            elif 'Acknowledgement' in response.text:
                print(f"⚠️  {country}: Acknowledgement (no data)")
            else:
                print(f"❓ {country}: {len(response.text):,} chars - Unknown format")
        else:
            print(f"❌ {country}: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"❌ {country}: {str(e)[:50]}")

print("\n" + "=" * 60)
print("RECOMMENDATION:")
print("If 3+ countries have REAL DATA → A71 ingestion is worth it!")
print("=" * 60)