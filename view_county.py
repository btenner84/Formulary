#!/usr/bin/env python3
"""
View 2: County Competitive Analysis
Input: county_code or state abbreviation
Shows: All plans available + competitive comparison
"""

import duckdb
import sys

if len(sys.argv) < 2:
    print("Usage: python3 view_county.py <county_code or state>")
    print("Example: python3 view_county.py 29189  (St. Louis, MO)")
    print("Example: python3 view_county.py MO     (all Missouri)")
    sys.exit(1)

search_term = sys.argv[1]

# Setup DuckDB
conn = duckdb.connect()
conn.execute("CREATE VIEW plans AS SELECT * FROM 'medicare_parquet/plan_information.parquet'")
conn.execute("CREATE VIEW geo AS SELECT * FROM 'medicare_parquet/geographic_locator.parquet'")
conn.execute("CREATE VIEW drugs AS SELECT * FROM 'medicare_parquet/formulary_drugs.parquet'")

print("="*80)
print(f"COUNTY/STATE ANALYSIS: {search_term}")
print("="*80)

# Determine if it's a county code or state
if len(search_term) == 5 and search_term.isdigit():
    # County code
    where_clause = f"p.county_code = '{search_term}'"
    
    # Get county name
    county_info = conn.execute(f"""
        SELECT state_name, county_name 
        FROM geo 
        WHERE county_code = '{search_term}'
    """).fetchone()
    
    if county_info:
        print(f"\n📍 Location: {county_info[1]}, {county_info[0]}")
    
else:
    # State abbreviation
    where_clause = f"p.state = '{search_term.upper()}'"
    print(f"\n📍 State: {search_term.upper()}")

# Plan count
plan_count = conn.execute(f"""
    SELECT COUNT(DISTINCT plan_key) as plans
    FROM plans p
    WHERE {where_clause}
""").fetchone()[0]

print(f"Plans available: {plan_count}")

# Plans overview
print("\n" + "="*80)
print("PLANS AVAILABLE (sorted by premium):")
print("="*80)

plans_df = conn.execute(f"""
    SELECT 
        p.contract_id,
        p.plan_id,
        p.plan_name,
        CAST(p.premium AS FLOAT) as premium,
        CAST(p.deductible AS FLOAT) as deductible,
        p.formulary_id,
        p.snp as snp_type
    FROM plans p
    WHERE {where_clause}
    GROUP BY p.contract_id, p.plan_id, p.plan_name, premium, deductible, p.formulary_id, p.snp
    ORDER BY premium, deductible
    LIMIT 30
""").df()

print(plans_df.to_string(index=False))

if len(plans_df) > 30:
    print(f"\n(Showing first 30 of {len(plans_df)} plans)")

# Premium stats
print("\n" + "="*80)
print("PREMIUM ANALYSIS:")
print("="*80)

premium_stats = conn.execute(f"""
    SELECT 
        COUNT(DISTINCT plan_key) as total_plans,
        ROUND(AVG(CAST(premium AS FLOAT)), 2) as avg_premium,
        MIN(CAST(premium AS FLOAT)) as min_premium,
        MAX(CAST(premium AS FLOAT)) as max_premium,
        ROUND(AVG(CAST(deductible AS FLOAT)), 2) as avg_deductible
    FROM (
        SELECT DISTINCT plan_key, premium, deductible
        FROM plans p
        WHERE {where_clause}
    ) sub
""").df()

print(premium_stats.to_string(index=False))

# Contract breakdown
print("\n" + "="*80)
print("TOP CONTRACTS IN THIS AREA:")
print("="*80)

contracts = conn.execute(f"""
    SELECT 
        p.contract_id,
        p.contract_name,
        COUNT(DISTINCT p.plan_key) as plans,
        COUNT(DISTINCT p.formulary_id) as formularies,
        ROUND(AVG(CAST(p.premium AS FLOAT)), 2) as avg_premium
    FROM plans p
    WHERE {where_clause}
    GROUP BY p.contract_id, p.contract_name
    ORDER BY plans DESC
    LIMIT 15
""").df()

print(contracts.to_string(index=False))

# Formulary coverage
print("\n" + "="*80)
print("FORMULARY ANALYSIS:")
print("="*80)

formulary_stats = conn.execute(f"""
    SELECT 
        p.formulary_id,
        COUNT(DISTINCT p.plan_key) as plans_in_area,
        COUNT(DISTINCT d.ndc) as total_drugs,
        COUNT(DISTINCT CASE WHEN d.tier_level_value = '5' THEN d.ndc END) as specialty_drugs
    FROM plans p
    LEFT JOIN drugs d ON d.formulary_id = p.formulary_id
    WHERE {where_clause}
    GROUP BY p.formulary_id
    ORDER BY plans_in_area DESC
    LIMIT 10
""").df()

print(formulary_stats.to_string(index=False))

# SNP breakdown
print("\n" + "="*80)
print("PLAN TYPES:")
print("="*80)

snp_breakdown = conn.execute(f"""
    SELECT 
        CASE 
            WHEN snp = '0' THEN 'Standard MA-PD'
            WHEN snp = '1' THEN 'SNP Type 1'
            WHEN snp = '2' THEN 'Dual Eligible SNP'
            WHEN snp = '3' THEN 'SNP Type 3'
            ELSE 'Other'
        END as plan_type,
        COUNT(DISTINCT plan_key) as plan_count
    FROM plans p
    WHERE {where_clause}
    GROUP BY snp
    ORDER BY plan_count DESC
""").df()

print(snp_breakdown.to_string(index=False))

print("\n" + "="*80)
print("NEXT STEPS:")
print("="*80)
print("1. View a specific formulary:")
print("   python3 view_formulary.py <formulary_id>")
print("\n2. Compare specific plans:")
print(f"   Use the plan_key values from above")
print("\n3. Export to CSV for Excel analysis:")
print(f"   python3 -c \"import pandas as pd; ")
print(f"   df = pd.read_parquet('medicare_parquet/plan_information.parquet'); ")
print(f"   df[df['{where_clause.split()[0]}'] == '{search_term}'].to_csv('plans_{search_term}.csv')\"")
print("="*80)

