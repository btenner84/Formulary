"""
Quick exploration of Medicare Part D data
Run this BEFORE loading into database to understand the data structure
"""

import pandas as pd
import zipfile
from pathlib import Path

DATA_DIR = Path("../SPUF_2025_20250703")

def explore_plan_information():
    """Explore plan information file"""
    print("="*60)
    print("PLAN INFORMATION FILE")
    print("="*60)
    
    zip_path = DATA_DIR / "plan information  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("plan information  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', nrows=100)
    
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nTotal records (sample): {len(df)}")
    print(f"\nUnique contracts: {df['CONTRACT_ID'].nunique()}")
    print(f"\nUnique plans: {df[['CONTRACT_ID', 'PLAN_ID']].drop_duplicates().shape[0]}")
    print(f"\nUnique formularies: {df['FORMULARY_ID'].nunique()}")
    
    print("\nSample data:")
    print(df[['CONTRACT_ID', 'PLAN_ID', 'CONTRACT_NAME', 'PREMIUM', 'DEDUCTIBLE', 'FORMULARY_ID']].head())
    
    print("\nPremium stats:")
    print(df['PREMIUM'].astype(float).describe())

def explore_formulary():
    """Explore formulary file"""
    print("\n" + "="*60)
    print("FORMULARY FILE")
    print("="*60)
    
    zip_path = DATA_DIR / "basic drugs formulary file  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("basic drugs formulary file  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', nrows=1000)
    
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSample records: {len(df)}")
    
    print("\nTier distribution (sample):")
    print(df['TIER_LEVEL_VALUE'].value_counts().sort_index())
    
    print("\nPrior Authorization:")
    print(df['PRIOR_AUTHORIZATION_YN'].value_counts())
    
    print("\nQuantity Limits:")
    print(df['QUANTITY_LIMIT_YN'].value_counts())
    
    print("\nSample drugs:")
    print(df[['FORMULARY_ID', 'NDC', 'TIER_LEVEL_VALUE', 'QUANTITY_LIMIT_YN', 'PRIOR_AUTHORIZATION_YN']].head(10))

def explore_beneficiary_costs():
    """Explore beneficiary cost file"""
    print("\n" + "="*60)
    print("BENEFICIARY COST FILE")
    print("="*60)
    
    zip_path = DATA_DIR / "beneficiary cost file  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("beneficiary cost file  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', nrows=100)
    
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSample records: {len(df)}")
    
    print("\nCost Type Distribution (Preferred):")
    print("0=Copay, 1=Coinsurance, 2=No Charge")
    print(df['COST_TYPE_PREF'].value_counts())
    
    print("\nSpecialty Tier Flag:")
    print(df['TIER_SPECIALTY_YN'].value_counts())
    
    print("\nSample cost structures:")
    print(df[['CONTRACT_ID', 'PLAN_ID', 'TIER', 'COST_TYPE_PREF', 
               'COST_AMT_PREF', 'COST_MAX_AMT_PREF', 'TIER_SPECIALTY_YN']].head(10))

def explore_pricing():
    """Explore pricing file (WARNING: Large!)"""
    print("\n" + "="*60)
    print("PRICING FILE (first 1000 records only)")
    print("="*60)
    
    zip_path = DATA_DIR / "pricing file PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("pricing file PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', nrows=1000)
    
    print(f"\nColumns: {list(df.columns)}")
    
    print("\nPrice range (sample):")
    df['UNIT_COST'] = pd.to_numeric(df['UNIT_COST'], errors='coerce')
    print(df['UNIT_COST'].describe())
    
    print("\nSample pricing records:")
    print(df[['CONTRACT_ID', 'PLAN_ID', 'NDC', 'DAYS_SUPPLY', 'UNIT_COST']].head(10))
    
    print("\nDays supply distribution:")
    print(df['DAYS_SUPPLY'].value_counts())

def find_specialty_drug_example():
    """Find a specialty drug and show complete data"""
    print("\n" + "="*60)
    print("SPECIALTY DRUG EXAMPLE")
    print("="*60)
    
    # Load formulary to find specialty drug
    zip_path = DATA_DIR / "basic drugs formulary file  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("basic drugs formulary file  PPUF_2025Q2.txt") as f:
            df_form = pd.read_csv(f, sep='|', nrows=10000)
    
    # Find Tier 5 drugs
    specialty = df_form[df_form['TIER_LEVEL_VALUE'] == 5].head(1)
    
    if len(specialty) > 0:
        ndc = specialty.iloc[0]['NDC']
        formulary_id = specialty.iloc[0]['FORMULARY_ID']
        
        print(f"\nSpecialty Drug: NDC {ndc}")
        print(f"Formulary: {formulary_id}")
        print(f"Tier: {specialty.iloc[0]['TIER_LEVEL_VALUE']}")
        print(f"Prior Auth: {specialty.iloc[0]['PRIOR_AUTHORIZATION_YN']}")
        print(f"Quantity Limit: {specialty.iloc[0]['QUANTITY_LIMIT_YN']}")
        
        # Find pricing for this drug
        zip_path = DATA_DIR / "pricing file PPUF_2025Q2.zip"
        with zipfile.ZipFile(zip_path) as z:
            with z.open("pricing file PPUF_2025Q2.txt") as f:
                df_price = pd.read_csv(f, sep='|', nrows=100000)
        
        drug_prices = df_price[df_price['NDC'] == ndc]
        
        if len(drug_prices) > 0:
            print(f"\nPricing found for {len(drug_prices)} plan-day combinations:")
            print(drug_prices[['CONTRACT_ID', 'PLAN_ID', 'DAYS_SUPPLY', 'UNIT_COST']].head(10))
            
            print(f"\nPrice range:")
            print(f"  Min: ${drug_prices['UNIT_COST'].astype(float).min():.2f}")
            print(f"  Max: ${drug_prices['UNIT_COST'].astype(float).max():.2f}")
            print(f"  Avg: ${drug_prices['UNIT_COST'].astype(float).mean():.2f}")

def compare_contracts():
    """Compare major contracts"""
    print("\n" + "="*60)
    print("CONTRACT COMPARISON")
    print("="*60)
    
    zip_path = DATA_DIR / "plan information  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("plan information  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|')
    
    # Group by contract
    contract_summary = df.groupby('CONTRACT_ID').agg({
        'CONTRACT_NAME': 'first',
        'PLAN_ID': 'nunique',
        'FORMULARY_ID': 'nunique',
        'PREMIUM': lambda x: pd.to_numeric(x, errors='coerce').mean(),
        'STATE': 'nunique'
    }).round(2)
    
    contract_summary.columns = ['Contract Name', 'Plans', 'Formularies', 'Avg Premium', 'States']
    
    # Show top 20 by plan count
    print("\nTop 20 Contracts by Plan Count:")
    print(contract_summary.sort_values('Plans', ascending=False).head(20))
    
    # Contract type breakdown
    print("\nContract Type Breakdown:")
    print(f"H contracts (MA-PD): {len([c for c in df['CONTRACT_ID'].unique() if c.startswith('H')])}")
    print(f"S contracts (PDP): {len([c for c in df['CONTRACT_ID'].unique() if c.startswith('S')])}")
    print(f"R contracts (PACE): {len([c for c in df['CONTRACT_ID'].unique() if c.startswith('R')])}")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("MEDICARE PART D DATA EXPLORATION")
    print("Q2 2025 Dataset")
    print("="*60)
    
    try:
        explore_plan_information()
        explore_formulary()
        explore_beneficiary_costs()
        explore_pricing()
        find_specialty_drug_example()
        compare_contracts()
        
        print("\n" + "="*60)
        print("EXPLORATION COMPLETE!")
        print("="*60)
        print("\nNext steps:")
        print("1. Review BUILD_PLAN.md for architecture")
        print("2. Set up PostgreSQL database")
        print("3. Run etl/load_data.py to load data")
        print("4. Use sql/example_queries.sql for analysis")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        print("\nMake sure DATA_DIR points to the correct location:")
        print(f"  Current: {DATA_DIR.absolute()}")
        import traceback
        traceback.print_exc()

