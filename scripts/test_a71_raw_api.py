"""
Test A71 with raw REST API call (bypassing entsoe-py library)
"""

import requests
import os
from dotenv import load_dotenv
from datetime import datetime

# Load API key
load_dotenv()
api_key = os.getenv('ENTSOE_API_KEY')

if not api_key:
    print("❌ ENTSOE_API_KEY not found in .env")
    exit(1)

# Test parameters
countries = ['PT', 'FR', 'DE']
period_start = '202501010000'  # YYYYMMDDHHMM format
period_end = '202501310000'

print("=" * 60)
print("TEST A71 - Raw REST API Call")
print("=" * 60)

for country in countries:
    # A71 endpoint
    url = "https://web-api.tp.entsoe.eu/api"
    
    params = {
        'securityToken': api_key,
        'documentType': 'A71',  # Installed capacity per production unit
        'processType': 'A33',   # Year ahead
        'in_Domain': f'10Y{country}-----------',  # Simplified, may need adjustment
        'periodStart': period_start,
        'periodEnd': period_end
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            print(f"\n✅ {country}: {len(response.text)} characters received")
            print(f"   Preview: {response.text[:200]}...")
        else:
            print(f"\n❌ {country}: HTTP {response.status_code}")
            print(f"   Error: {response.text[:200]}")
            
    except Exception as e:
        print(f"\n❌ {country}: Exception - {str(e)}")

print("\n" + "=" * 60)
