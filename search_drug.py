#!/usr/bin/env python3
"""
Search for a drug across all formularies
Shows: Coverage, tiers, restrictions, which plans cover it
"""

import duckdb
import sys

if len(sys.argv) < 2:
    print("Usage: python3 search_drug.py <NDC>")
    print("Example: python3 search_drug.py 00002533754")
    sys.exit(1)

ndc = sys.argv[1]

# Setup DuckDB
conn = duckdb.connect()
conn.execute("CREATE VIEW drugs AS SELECT * FROM 'medicare_parquet/formulary_drugs.parquet'")
conn.execute("CREATE VIEW plans AS SELECT * FROM 'medicare_parquet/plan_information.parquet'")
conn.execute("CREATE VIEW costs AS SELECT * FROM 'medicare_parquet/beneficiary_costs.parquet'")

print("="*80)
print(f"DRUG ANALYSIS: NDC {ndc}")
print("="*80)

# Basic drug info
drug_info = conn.execute(f"""
    SELECT 
        ndc,
        rxcui,
        COUNT(DISTINCT formulary_id) as formularies_covering,
        COUNT(*) as total_entries
    FROM drugs
    WHERE ndc = '{ndc}'
    GROUP BY ndc, rxcui
""").df()

if len(drug_info) == 0:
    print(f"\n❌ Drug NDC {ndc} not found in any formulary")
    sys.exit(1)

print("\n📊 DRUG OVERVIEW:")
print(drug_info.to_string(index=False))

# Coverage by tier
print("\n" + "="*80)
print("TIER PLACEMENT ACROSS FORMULARIES:")
print("="*80)

tier_dist = conn.execute(f"""
    SELECT 
        tier_level_value as tier,
        CASE 
            WHEN tier_level_value = '1' THEN 'Generic'
            WHEN tier_level_value = '2' THEN 'Preferred Generic'
            WHEN tier_level_value = '3' THEN 'Preferred Brand'
            WHEN tier_level_value = '4' THEN 'Non-Preferred'
            WHEN tier_level_value = '5' THEN 'Specialty'
            ELSE 'Other'
        END as tier_name,
        COUNT(DISTINCT formulary_id) as formularies,
        COUNT(*) as entries
    FROM drugs
    WHERE ndc = '{ndc}'
    GROUP BY tier_level_value
    ORDER BY formularies DESC
""").df()

print(tier_dist.to_string(index=False))

# Restrictions
print("\n" + "="*80)
print("UTILIZATION MANAGEMENT:")
print("="*80)

restrictions = conn.execute(f"""
    SELECT 
        SUM(CASE WHEN prior_authorization_yn = 'Y' THEN 1 ELSE 0 END) as with_prior_auth,
        SUM(CASE WHEN quantity_limit_yn = 'Y' THEN 1 ELSE 0 END) as with_qty_limits,
        SUM(CASE WHEN step_therapy_yn = 'Y' THEN 1 ELSE 0 END) as with_step_therapy,
        COUNT(*) as total_entries
    FROM drugs
    WHERE ndc = '{ndc}'
""").df()

print(restrictions.to_string(index=False))

# Plans covering this drug
print("\n" + "="*80)
print("PLANS COVERING THIS DRUG (Sample):")
print("="*80)

plans_covering = conn.execute(f"""
    SELECT 
        p.contract_id,
        p.plan_id,
        p.plan_name,
        d.tier_level_value as tier,
        d.formulary_id,
        CAST(p.premium AS FLOAT) as premium,
        d.prior_authorization_yn as pa,
        d.quantity_limit_yn as qty_limit
    FROM drugs d
    JOIN plans p ON p.formulary_id = d.formulary_id
    WHERE d.ndc = '{ndc}'
    GROUP BY p.contract_id, p.plan_id, p.plan_name, d.tier_level_value, d.formulary_id, premium, pa, qty_limit
    ORDER BY premium, p.contract_id
    LIMIT 20
""").df()

print(plans_covering.to_string(index=False))
print(f"\n(Showing first 20 plans)")

# Get cost structure samples
print("\n" + "="*80)
print("MEMBER COST STRUCTURE (Sample Plans):")
print("="*80)

# Get a few sample plans
sample_plans = conn.execute(f"""
    SELECT DISTINCT p.plan_key, p.contract_id, p.plan_id, d.tier_level_value as tier
    FROM drugs d
    JOIN plans p ON p.formulary_id = d.formulary_id
    WHERE d.ndc = '{ndc}'
    LIMIT 5
""").df()

if len(sample_plans) > 0:
    for _, row in sample_plans.iterrows():
        plan_key = row['plan_key']
        tier = row['tier']
        
        cost_info = conn.execute(f"""
            SELECT 
                cost_type_pref,
                CAST(cost_amt_pref AS FLOAT) as amount,
                CAST(cost_max_amt_pref AS FLOAT) as max_cap
            FROM costs
            WHERE plan_key = '{plan_key}'
              AND CAST(tier AS TEXT) = '{tier}'
              AND coverage_level = 1
              AND days_supply = 1
            LIMIT 1
        """).fetchone()
        
        if cost_info:
            cost_type = cost_info[0]
            amount = cost_info[1]
            max_cap = cost_info[2]
            
            cost_str = "No charge"
            if cost_type == 0:
                cost_str = f"${amount:.2f} copay"
            elif cost_type == 1:
                cost_str = f"{amount*100:.0f}% coinsurance (max ${max_cap:.2f})"
            
            print(f"  {row['contract_id']}-{row['plan_id']} (Tier {tier}): {cost_str}")

print("\n" + "="*80)
print("ANALYSIS OPTIONS:")
print("="*80)
print("1. See which formularies cover this drug:")
print(f"   python3 -c \"import duckdb; conn = duckdb.connect(); ")
print(f"   conn.execute('SELECT DISTINCT formulary_id FROM \\'medicare_parquet/formulary_drugs.parquet\\' WHERE ndc = \\'{ndc}\\' ').show()\"")
print("\n2. Export to CSV:")
print(f"   python3 -c \"import pandas as pd; ")
print(f"   df = pd.read_parquet('medicare_parquet/formulary_drugs.parquet'); ")
print(f"   df[df['ndc'] == '{ndc}'].to_csv('drug_{ndc}.csv')\"")
print("="*80)

