"""
Test A71 using the exact URL format from ENTSO-E Transparency Platform
"""

import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('ENTSOE_API_KEY')

if not api_key:
    print("❌ ENTSOE_API_KEY not found")
    exit(1)

# Test with Belgium (from your screenshot)
url = "https://web-api.tp.entsoe.eu/api"
params = {
    'documentType': 'A71',
    'processType': 'A33',
    'in_Domain': '10YBE----------2',  # Belgium
    'periodStart': '202601010000',
    'periodEnd': '202601020000',
    'securityToken': api_key
}

print("Testing A71 for Belgium (BE)...")
print(f"URL: {url}")
print(f"Params: {params}")

response = requests.get(url, params=params, timeout=30)

if response.status_code == 200:
    print(f"\n✅ SUCCESS: {len(response.text)} characters")
    print(f"\nPreview:\n{response.text[:500]}")
else:
    print(f"\n❌ FAILED: HTTP {response.status_code}")
    print(f"Error: {response.text}")