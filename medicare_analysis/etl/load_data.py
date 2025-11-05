"""
Medicare Part D Data ETL Pipeline
Loads Q2 2025 data from ZIP files into PostgreSQL database
"""

import pandas as pd
import zipfile
from sqlalchemy import create_engine
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://localhost:5432/medicare_partd')
DATA_DIR = Path(os.getenv('DATA_DIR', '../SPUF_2025_20250703'))

# Create database connection
engine = create_engine(DATABASE_URL)
print(f"Connected to database: {DATABASE_URL}")

def load_plan_information():
    """Load plan information file"""
    print("\nLoading plan information...")
    
    zip_path = DATA_DIR / "plan information  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("plan information  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', dtype=str)
    
    print(f"  Loaded {len(df)} plan-county records")
    
    # Extract contracts
    df_contracts = df[['CONTRACT_ID', 'CONTRACT_NAME']].drop_duplicates()
    df_contracts.columns = ['contract_id', 'contract_name']
    df_contracts['contract_type'] = df_contracts['contract_id'].str[0]  # H, S, or R
    df_contracts.to_sql('contracts', engine, if_exists='append', index=False)
    print(f"  Loaded {len(df_contracts)} unique contracts")
    
    # Extract formularies
    df_formularies = df[['FORMULARY_ID']].drop_duplicates()
    df_formularies.columns = ['formulary_id']
    df_formularies['formulary_version'] = None
    df_formularies['contract_year'] = 2025
    df_formularies.to_sql('formularies', engine, if_exists='append', index=False)
    print(f"  Loaded {len(df_formularies)} unique formularies")
    
    # Extract plans
    df_plans = df[['CONTRACT_ID', 'PLAN_ID', 'SEGMENT_ID', 'PLAN_NAME', 
                    'FORMULARY_ID', 'PREMIUM', 'DEDUCTIBLE', 'SNP', 'PLAN_SUPPRESSED_YN']].drop_duplicates()
    df_plans['plan_key'] = df_plans['CONTRACT_ID'] + '|' + df_plans['PLAN_ID'] + '|' + df_plans['SEGMENT_ID']
    df_plans.columns = ['contract_id', 'plan_id', 'segment_id', 'plan_name', 
                        'formulary_id', 'premium', 'deductible', 'snp_type', 'plan_suppressed_yn', 'plan_key']
    
    # Reorder columns
    df_plans = df_plans[['plan_key', 'contract_id', 'plan_id', 'segment_id', 'plan_name',
                          'formulary_id', 'premium', 'deductible', 'snp_type', 'plan_suppressed_yn']]
    
    df_plans.to_sql('plans', engine, if_exists='append', index=False)
    print(f"  Loaded {len(df_plans)} unique plans")
    
    # Extract geography
    df_geography = df[['CONTRACT_ID', 'PLAN_ID', 'SEGMENT_ID', 'STATE', 'COUNTY_CODE', 
                        'MA_REGION_CODE', 'PDP_REGION_CODE']].copy()
    df_geography['plan_key'] = df_geography['CONTRACT_ID'] + '|' + df_geography['PLAN_ID'] + '|' + df_geography['SEGMENT_ID']
    df_geography = df_geography[['plan_key', 'STATE', 'COUNTY_CODE', 'MA_REGION_CODE', 'PDP_REGION_CODE']]
    df_geography.columns = ['plan_key', 'state_code', 'county_code', 'ma_region_code', 'pdp_region_code']
    
    df_geography.to_sql('plan_geography', engine, if_exists='append', index=False)
    print(f"  Loaded {len(df_geography)} plan-county relationships")

def load_geographic_locator():
    """Load geographic locator file"""
    print("\nLoading geographic locator...")
    
    zip_path = DATA_DIR / "geographic locator file PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("geographic locator file PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', dtype=str)
    
    df.columns = ['county_code', 'state_name', 'county_name', 'ma_region_code', 
                   'ma_region_name', 'pdp_region_code', 'pdp_region_name']
    
    df.to_sql('geographic_locator', engine, if_exists='append', index=False)
    print(f"  Loaded {len(df)} counties")

def load_formulary_drugs():
    """Load formulary drugs file"""
    print("\nLoading formulary drugs...")
    
    zip_path = DATA_DIR / "basic drugs formulary file  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("basic drugs formulary file  PPUF_2025Q2.txt") as f:
            # Load in chunks due to size
            chunk_size = 100000
            total_rows = 0
            
            for chunk in pd.read_csv(f, sep='|', dtype=str, chunksize=chunk_size):
                chunk.columns = ['formulary_id', 'formulary_version', 'contract_year', 'rxcui', 'ndc',
                                  'tier', 'quantity_limit_yn', 'quantity_limit_amount', 'quantity_limit_days',
                                  'prior_authorization_yn', 'step_therapy_yn']
                
                chunk.to_sql('formulary_drugs', engine, if_exists='append', index=False)
                total_rows += len(chunk)
                print(f"  Loaded {total_rows} drugs...")
    
    print(f"  Total: {total_rows} formulary drug entries")

def load_beneficiary_costs():
    """Load beneficiary cost file"""
    print("\nLoading beneficiary costs...")
    
    # Column names for beneficiary cost file
    columns = ['contract_id', 'plan_id', 'segment_id', 'coverage_level', 'tier', 'days_supply',
               'cost_type_pref', 'cost_amt_pref', 'cost_min_amt_pref', 'cost_max_amt_pref',
               'cost_type_nonpref', 'cost_amt_nonpref', 'cost_min_amt_nonpref', 'cost_max_amt_nonpref',
               'cost_type_mail_pref', 'cost_amt_mail_pref', 'cost_min_amt_mail_pref', 'cost_max_amt_mail_pref',
               'cost_type_mail_nonpref', 'cost_amt_mail_nonpref', 'cost_min_amt_mail_nonpref', 'cost_max_amt_mail_nonpref',
               'tier_specialty_yn', 'deductible_applies_yn']
    
    zip_path = DATA_DIR / "beneficiary cost file  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("beneficiary cost file  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', dtype=str)
            df.columns = columns
            
            # Create plan_key
            df['plan_key'] = df['contract_id'] + '|' + df['plan_id'] + '|' + df['segment_id']
            
            # Drop individual ID columns
            df = df.drop(['contract_id', 'plan_id', 'segment_id'], axis=1)
            
            # Reorder columns
            cols = ['plan_key'] + [col for col in df.columns if col != 'plan_key']
            df = df[cols]
            
            df.to_sql('beneficiary_costs', engine, if_exists='append', index=False)
    
    print(f"  Loaded {len(df)} beneficiary cost records")

def load_pricing_data():
    """Load pricing file (LARGE - 55M records)"""
    print("\nLoading pricing data...")
    print("  WARNING: This file is 2GB and may take 30+ minutes")
    
    zip_path = DATA_DIR / "pricing file PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("pricing file PPUF_2025Q2.txt") as f:
            # Load in chunks
            chunk_size = 500000
            total_rows = 0
            
            for chunk in pd.read_csv(f, sep='|', dtype=str, chunksize=chunk_size):
                chunk.columns = ['contract_id', 'plan_id', 'segment_id', 'ndc', 'days_supply', 'unit_cost']
                
                # Create plan_key
                chunk['plan_key'] = chunk['contract_id'] + '|' + chunk['plan_id'] + '|' + chunk['segment_id']
                
                # Keep only needed columns
                chunk = chunk[['plan_key', 'ndc', 'days_supply', 'unit_cost']]
                
                chunk.to_sql('drug_pricing', engine, if_exists='append', index=False)
                total_rows += len(chunk)
                
                if total_rows % 1000000 == 0:
                    print(f"  Loaded {total_rows:,} pricing records...")
    
    print(f"  Total: {total_rows:,} pricing records")

def refresh_materialized_views():
    """Refresh materialized views after data load"""
    print("\nRefreshing materialized views...")
    
    with engine.connect() as conn:
        conn.execute("REFRESH MATERIALIZED VIEW formulary_summary")
        conn.execute("REFRESH MATERIALIZED VIEW county_plan_summary")
    
    print("  Views refreshed")

if __name__ == "__main__":
    print("="*60)
    print("Medicare Part D Data ETL Pipeline")
    print("="*60)
    
    # Load data in order (respecting foreign keys)
    try:
        load_geographic_locator()
        load_plan_information()
        load_formulary_drugs()
        load_beneficiary_costs()
        
        # Ask before loading pricing (takes a long time)
        response = input("\nLoad pricing data? (55M records, ~30min) [y/N]: ")
        if response.lower() == 'y':
            load_pricing_data()
        
        refresh_materialized_views()
        
        print("\n" + "="*60)
        print("DATA LOAD COMPLETE!")
        print("="*60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

