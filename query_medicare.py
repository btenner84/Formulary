#!/usr/bin/env python3
"""
Query Medicare Part D Parquet files using DuckDB (SQL without PostgreSQL!)
"""

import duckdb
from pathlib import Path

# Point to parquet files
DATA_DIR = Path("/Users/bentenner/Dictionary/2025-Q2/medicare_parquet")

# Create DuckDB connection (in-memory)
conn = duckdb.connect()

# Register all parquet files as tables
print("Loading data tables...")
conn.execute(f"CREATE VIEW plan_information AS SELECT * FROM '{DATA_DIR}/plan_information.parquet'")
conn.execute(f"CREATE VIEW geographic_locator AS SELECT * FROM '{DATA_DIR}/geographic_locator.parquet'")
conn.execute(f"CREATE VIEW formulary_drugs AS SELECT * FROM '{DATA_DIR}/formulary_drugs.parquet'")
conn.execute(f"CREATE VIEW beneficiary_costs AS SELECT * FROM '{DATA_DIR}/beneficiary_costs.parquet'")
conn.execute(f"CREATE VIEW insulin_costs AS SELECT * FROM '{DATA_DIR}/insulin_costs.parquet'")
conn.execute(f"CREATE VIEW excluded_drugs AS SELECT * FROM '{DATA_DIR}/excluded_drugs.parquet'")
conn.execute(f"CREATE VIEW indication_based AS SELECT * FROM '{DATA_DIR}/indication_based.parquet'")

print("✓ All tables loaded\n")

# ==================================================
# EXAMPLE QUERIES
# ==================================================

print("="*60)
print("EXAMPLE 1: Formulary Summary")
print("="*60)

query = """
SELECT 
    formulary_id,
    COUNT(DISTINCT ndc) as total_drugs,
    COUNT(DISTINCT CASE WHEN tier_level_value = '5' THEN ndc END) as specialty_drugs,
    ROUND(COUNT(DISTINCT CASE WHEN tier_level_value = '5' THEN ndc END)::FLOAT / 
          COUNT(DISTINCT ndc) * 100, 1) as pct_specialty
FROM formulary_drugs
GROUP BY formulary_id
ORDER BY total_drugs DESC
LIMIT 10
"""

print(conn.execute(query).df().to_string(index=False))

# ==================================================

print("\n" + "="*60)
print("EXAMPLE 2: Top Contracts by Plan Count")
print("="*60)

query = """
SELECT 
    contract_id,
    contract_name,
    COUNT(DISTINCT plan_key) as plans,
    COUNT(DISTINCT formulary_id) as formularies,
    ROUND(AVG(CAST(premium AS FLOAT)), 2) as avg_premium,
    COUNT(DISTINCT state) as states
FROM plan_information
GROUP BY contract_id, contract_name
ORDER BY plans DESC
LIMIT 15
"""

print(conn.execute(query).df().to_string(index=False))

# ==================================================

print("\n" + "="*60)
print("EXAMPLE 3: Formulary 00025456 Details")
print("="*60)

query = """
SELECT 
    COUNT(DISTINCT plan_key) as plans_using_this_formulary
FROM plan_information
WHERE formulary_id = '00025456'
"""
print(conn.execute(query).df().to_string(index=False))

query = """
SELECT 
    tier_level_value as tier,
    COUNT(*) as drugs,
    SUM(CASE WHEN prior_authorization_yn = 'Y' THEN 1 ELSE 0 END) as with_pa,
    SUM(CASE WHEN quantity_limit_yn = 'Y' THEN 1 ELSE 0 END) as with_qty_limit
FROM formulary_drugs
WHERE formulary_id = '00025456'
GROUP BY tier_level_value
ORDER BY tier_level_value
"""
print("\nDrugs by tier:")
print(conn.execute(query).df().to_string(index=False))

# ==================================================

print("\n" + "="*60)
print("EXAMPLE 4: Plans in Missouri")
print("="*60)

query = """
SELECT 
    pi.contract_id,
    pi.plan_id,
    pi.plan_name,
    CAST(pi.premium AS FLOAT) as premium,
    CAST(pi.deductible AS FLOAT) as deductible,
    pi.formulary_id
FROM plan_information pi
WHERE pi.state = 'MO'
  AND pi.county_code = '29189'  -- St. Louis County
ORDER BY premium, deductible
LIMIT 10
"""

print("\nPlans in St. Louis County, MO:")
print(conn.execute(query).df().to_string(index=False))

# ==================================================

print("\n" + "="*60)
print("EXAMPLE 5: Specialty Drugs (Most Covered)")
print("="*60)

query = """
SELECT 
    ndc,
    rxcui,
    COUNT(DISTINCT formulary_id) as formulary_count,
    SUM(CASE WHEN prior_authorization_yn = 'Y' THEN 1 ELSE 0 END) as pa_required_count
FROM formulary_drugs
WHERE tier_level_value = '5'
GROUP BY ndc, rxcui
ORDER BY formulary_count DESC
LIMIT 15
"""

print(conn.execute(query).df().to_string(index=False))

# ==================================================

print("\n" + "="*60)
print("INTERACTIVE MODE")
print("="*60)
print("\nYou can now run custom queries!")
print("\nAvailable tables:")
print("  - plan_information")
print("  - formulary_drugs")
print("  - beneficiary_costs")
print("  - geographic_locator")
print("  - insulin_costs")
print("  - excluded_drugs")
print("  - indication_based")
print("\nExample:")
print("  query = 'SELECT * FROM formulary_drugs WHERE ndc = \"00002533754\"'")
print("  result = conn.execute(query).df()")
print("  print(result)")
print("\nOr use:")
print("  conn.sql('SELECT * FROM plan_information LIMIT 5').show()")
print("\n")

# Keep connection open for interactive use
import code
code.interact(local=locals(), banner="")

