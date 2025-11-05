#!/usr/bin/env python3
"""
View 1: Formulary Detail Page
Input: formulary_id
Shows: All plans using it + all drugs in it + cost structures
"""

import duckdb
import sys

if len(sys.argv) < 2:
    print("Usage: python3 view_formulary.py <formulary_id>")
    print("Example: python3 view_formulary.py 00025456")
    sys.exit(1)

formulary_id = sys.argv[1]

# Setup DuckDB
conn = duckdb.connect()
conn.execute("CREATE VIEW plans AS SELECT * FROM 'medicare_parquet/plan_information.parquet'")
conn.execute("CREATE VIEW drugs AS SELECT * FROM 'medicare_parquet/formulary_drugs.parquet'")
conn.execute("CREATE VIEW costs AS SELECT * FROM 'medicare_parquet/beneficiary_costs.parquet'")

print("="*80)
print(f"FORMULARY {formulary_id} - DETAILED VIEW")
print("="*80)

# Overview
overview = conn.execute(f"""
    SELECT 
        '{formulary_id}' as formulary_id,
        COUNT(DISTINCT plan_key) as plans_using,
        COUNT(DISTINCT ndc) as total_drugs,
        COUNT(DISTINCT CASE WHEN tier_level_value = '5' THEN ndc END) as specialty_drugs,
        COUNT(DISTINCT state) as states_covered
    FROM plans p
    LEFT JOIN drugs d ON d.formulary_id = p.formulary_id
    WHERE p.formulary_id = '{formulary_id}'
""").df()

print("\n📊 OVERVIEW:")
print(overview.to_string(index=False))

# Plans using this formulary
print("\n" + "="*80)
print("PLANS USING THIS FORMULARY:")
print("="*80)

plans = conn.execute(f"""
    SELECT 
        contract_id,
        plan_id,
        plan_name,
        CAST(premium AS FLOAT) as premium,
        CAST(deductible AS FLOAT) as deductible,
        COUNT(DISTINCT county_code) as counties
    FROM plans
    WHERE formulary_id = '{formulary_id}'
    GROUP BY contract_id, plan_id, plan_name, premium, deductible
    ORDER BY premium, deductible
    LIMIT 20
""").df()

print(plans.to_string(index=False))
print(f"\n(Showing first 20 of {len(plans)} plans)")

# Drugs by tier
print("\n" + "="*80)
print("DRUGS BY TIER:")
print("="*80)

tier_summary = conn.execute(f"""
    SELECT 
        tier_level_value as tier,
        CASE 
            WHEN tier_level_value = '1' THEN 'Generic'
            WHEN tier_level_value = '2' THEN 'Preferred Generic'
            WHEN tier_level_value = '3' THEN 'Preferred Brand'
            WHEN tier_level_value = '4' THEN 'Non-Preferred'
            WHEN tier_level_value = '5' THEN 'Specialty'
            WHEN tier_level_value = '6' THEN 'Tier 6'
            ELSE 'Other'
        END as tier_name,
        COUNT(*) as drug_count,
        SUM(CASE WHEN prior_authorization_yn = 'Y' THEN 1 ELSE 0 END) as with_prior_auth,
        SUM(CASE WHEN quantity_limit_yn = 'Y' THEN 1 ELSE 0 END) as with_qty_limits,
        SUM(CASE WHEN step_therapy_yn = 'Y' THEN 1 ELSE 0 END) as with_step_therapy
    FROM drugs
    WHERE formulary_id = '{formulary_id}'
    GROUP BY tier_level_value
    ORDER BY tier_level_value
""").df()

print(tier_summary.to_string(index=False))

# Specialty drugs detail
print("\n" + "="*80)
print("SPECIALTY DRUGS (TIER 5) - Sample:")
print("="*80)

specialty = conn.execute(f"""
    SELECT 
        ndc,
        rxcui,
        quantity_limit_yn as qty_limit,
        quantity_limit_amount as qty_amt,
        prior_authorization_yn as prior_auth,
        step_therapy_yn as step_therapy
    FROM drugs
    WHERE formulary_id = '{formulary_id}'
      AND tier_level_value = '5'
    ORDER BY ndc
    LIMIT 25
""").df()

print(specialty.to_string(index=False))
print(f"\n(Showing first 25 specialty drugs)")

# Get sample plan for cost structure
print("\n" + "="*80)
print("MEMBER COST STRUCTURE (Sample Plan):")
print("="*80)

sample_plan = conn.execute(f"""
    SELECT plan_key FROM plans WHERE formulary_id = '{formulary_id}' LIMIT 1
""").fetchone()

if sample_plan:
    plan_key = sample_plan[0]
    
    cost_structure = conn.execute(f"""
        SELECT 
            tier,
            CASE WHEN cost_type_pref = 0 THEN 'Copay'
                 WHEN cost_type_pref = 1 THEN 'Coinsurance'
                 ELSE 'No Charge' END as cost_type,
            CAST(cost_amt_pref AS FLOAT) as amount,
            CAST(cost_max_amt_pref AS FLOAT) as max_cap,
            tier_specialty_yn as specialty_flag
        FROM costs
        WHERE plan_key = '{plan_key}'
          AND coverage_level = 1
          AND days_supply = 1
        ORDER BY tier
    """).df()
    
    print(f"\nCost structure for plan {plan_key}:")
    print(cost_structure.to_string(index=False))
    print("\nNote: Cost type 0=flat copay, 1=coinsurance (%), 2=no charge")

print("\n" + "="*80)
print("To see drugs in this formulary:")
print(f"  python3 -c \"import duckdb; conn = duckdb.connect(); ")
print(f"  conn.execute('SELECT * FROM \\'medicare_parquet/formulary_drugs.parquet\\' WHERE formulary_id = \\'{formulary_id}\\' ').show()\"")
print("="*80)

