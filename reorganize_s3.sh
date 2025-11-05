#!/bin/bash

echo "=================================="
echo "📂 REORGANIZING S3 BUCKET"
echo "=================================="
echo ""

# Current structure: s3://formulary2026/medicare_parquet/*
# New structure: s3://formulary2026/2025/* and s3://formulary2026/2026/*

echo "1️⃣ Moving 2025 data from medicare_parquet/ to 2025/..."
aws s3 mv s3://formulary2026/medicare_parquet/ s3://formulary2026/2025/ --recursive

echo ""
echo "2️⃣ Verifying 2025 files..."
aws s3 ls s3://formulary2026/2025/ --human-readable

echo ""
echo "3️⃣ Verifying 2026 files..."
aws s3 ls s3://formulary2026/2026/ --human-readable

echo ""
echo "=================================="
echo "✅ S3 REORGANIZATION COMPLETE!"
echo "=================================="
echo ""
echo "📊 New structure:"
echo "   s3://formulary2026/2025/"
echo "      ├─ plan_information.parquet"
echo "      ├─ formulary_drugs.parquet"
echo "      ├─ beneficiary_costs.parquet"
echo "      ├─ drug_pricing.parquet"
echo "      ├─ geographic_locator.parquet"
echo "      └─ contract_organizations.parquet"
echo ""
echo "   s3://formulary2026/2026/"
echo "      ├─ plan_information.parquet"
echo "      ├─ formulary_drugs.parquet"
echo "      ├─ beneficiary_costs.parquet"
echo "      └─ geographic_locator.parquet"
echo ""

