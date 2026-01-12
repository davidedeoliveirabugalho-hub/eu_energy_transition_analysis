#!/bin/bash

# ============================================================================
# BigQuery Datasets Setup Script
# Creates the 3 datasets for medallion architecture
# ============================================================================

set -e  # Exit on error

# Configuration
PROJECT_ID="eu-energy-transition"
LOCATION="EU"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Creating BigQuery datasets for project: ${PROJECT_ID}${NC}"
echo ""

# Create bronze dataset (raw data from Airbyte)
echo "Creating bronze dataset..."
bq mk \
  --dataset \
  --location=${LOCATION} \
  --description="Raw data layer - Data ingested from Airbyte" \
  ${PROJECT_ID}:bronze

echo -e "${GREEN}✓ bronze dataset created${NC}"

# Create silver dataset (cleaned data from dbt)
echo "Creating silver dataset..."
bq mk \
  --dataset \
  --location=${LOCATION} \
  --description="Silver data layer - Cleaned and transformed data (dbt staging + intermediate)" \
  ${PROJECT_ID}:silver

echo -e "${GREEN}✓ silver dataset created${NC}"

# Create gold dataset (analytics-ready marts)
echo "Creating gold dataset..."
bq mk \
  --dataset \
  --location=${LOCATION} \
  --description="Gold data layer - Analytics-ready data marts (dbt marts)" \
  ${PROJECT_ID}:gold

echo -e "${GREEN}✓ gold dataset created${NC}"

echo ""
echo -e "${GREEN}✅ All datasets created successfully!${NC}"
echo ""
echo "You can verify in BigQuery console:"
echo "https://console.cloud.google.com/bigquery?project=${PROJECT_ID}"