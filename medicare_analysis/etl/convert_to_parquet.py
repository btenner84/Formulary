"""
Convert Medicare Part D data from ZIP files to Parquet format
Parquet is columnar, compressed, and can be queried with DuckDB
"""

import pandas as pd
import zipfile
from pathlib import Path
import sys

DATA_DIR = Path("/Users/bentenner/Dictionary/2025-Q2/SPUF_2025_20250703")
OUTPUT_DIR = Path("/Users/bentenner/Dictionary/2025-Q2/medicare_parquet")

# Create output directory
OUTPUT_DIR.mkdir(exist_ok=True)

print("="*60)
print("MEDICARE PART D → PARQUET CONVERSION")
print("="*60)

def convert_plan_information():
    """Convert plan information to Parquet"""
    print("\n1. Converting Plan Information...")
    
    zip_path = DATA_DIR / "plan information  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("plan information  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', dtype=str, low_memory=False)
    
    # Clean column names
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Convert numeric columns
    df['premium'] = pd.to_numeric(df['premium'], errors='coerce')
    df['deductible'] = pd.to_numeric(df['deductible'], errors='coerce')
    df['snp'] = pd.to_numeric(df['snp'], errors='coerce')
    
    # Create plan_key
    df['plan_key'] = df['contract_id'] + '|' + df['plan_id'] + '|' + df['segment_id']
    
    # Save
    output_path = OUTPUT_DIR / "plan_information.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')
    
    print(f"   ✓ Saved {len(df):,} records → {output_path.name}")
    print(f"   Size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return len(df)

def convert_geographic_locator():
    """Convert geographic locator to Parquet"""
    print("\n2. Converting Geographic Locator...")
    
    zip_path = DATA_DIR / "geographic locator file PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("geographic locator file PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', dtype=str, low_memory=False)
    
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    output_path = OUTPUT_DIR / "geographic_locator.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')
    
    print(f"   ✓ Saved {len(df):,} records → {output_path.name}")
    print(f"   Size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return len(df)

def convert_formulary_drugs():
    """Convert formulary drugs to Parquet"""
    print("\n3. Converting Formulary Drugs...")
    print("   (This is large - processing in chunks...)")
    
    zip_path = DATA_DIR / "basic drugs formulary file  PPUF_2025Q2.zip"
    output_path = OUTPUT_DIR / "formulary_drugs.parquet"
    
    chunks = []
    total_rows = 0
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("basic drugs formulary file  PPUF_2025Q2.txt") as f:
            for chunk in pd.read_csv(f, sep='|', dtype=str, chunksize=200000, low_memory=False):
                chunks.append(chunk)
                total_rows += len(chunk)
                if total_rows % 200000 == 0:
                    print(f"   ... {total_rows:,} rows processed")
    
    # Combine all chunks
    print("   Combining chunks...")
    df = pd.concat(chunks, ignore_index=True)
    
    # Clean column names
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Convert numeric columns
    df['tier_level_value'] = pd.to_numeric(df['tier_level_value'], errors='coerce')
    df['quantity_limit_amount'] = pd.to_numeric(df['quantity_limit_amount'], errors='coerce')
    df['quantity_limit_days'] = pd.to_numeric(df['quantity_limit_days'], errors='coerce')
    
    # Save
    df.to_parquet(output_path, index=False, compression='snappy')
    
    print(f"   ✓ Saved {len(df):,} records → {output_path.name}")
    print(f"   Size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return len(df)

def convert_beneficiary_costs():
    """Convert beneficiary costs to Parquet"""
    print("\n4. Converting Beneficiary Costs...")
    
    zip_path = DATA_DIR / "beneficiary cost file  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("beneficiary cost file  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', dtype=str, low_memory=False)
    
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Create plan_key
    df['plan_key'] = df['contract_id'] + '|' + df['plan_id'] + '|' + df['segment_id']
    
    # Convert numeric columns
    numeric_cols = ['coverage_level', 'tier', 'days_supply', 'cost_type_pref', 'cost_amt_pref',
                    'cost_min_amt_pref', 'cost_max_amt_pref', 'cost_type_nonpref', 'cost_amt_nonpref',
                    'cost_min_amt_nonpref', 'cost_max_amt_nonpref', 'cost_type_mail_pref',
                    'cost_amt_mail_pref', 'cost_min_amt_mail_pref', 'cost_max_amt_mail_pref',
                    'cost_type_mail_nonpref', 'cost_amt_mail_nonpref', 'cost_min_amt_mail_nonpref',
                    'cost_max_amt_mail_nonpref']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    output_path = OUTPUT_DIR / "beneficiary_costs.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')
    
    print(f"   ✓ Saved {len(df):,} records → {output_path.name}")
    print(f"   Size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return len(df)

def convert_insulin_costs():
    """Convert insulin beneficiary costs to Parquet"""
    print("\n5. Converting Insulin Costs...")
    
    zip_path = DATA_DIR / "insulin beneficiary cost file  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("insulin beneficiary cost file  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', dtype=str, low_memory=False)
    
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Create plan_key
    df['plan_key'] = df['contract_id'] + '|' + df['plan_id'] + '|' + df['segment_id']
    
    output_path = OUTPUT_DIR / "insulin_costs.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')
    
    print(f"   ✓ Saved {len(df):,} records → {output_path.name}")
    print(f"   Size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return len(df)

def convert_pricing(do_full=False):
    """Convert pricing data to Parquet"""
    print("\n6. Converting Pricing Data...")
    
    if not do_full:
        print("   SKIPPING full pricing data (55M records)")
        print("   Run with --full-pricing flag to convert")
        return 0
    
    print("   WARNING: This will take 15-30 minutes...")
    
    zip_path = DATA_DIR / "pricing file PPUF_2025Q2.zip"
    output_path = OUTPUT_DIR / "drug_pricing.parquet"
    
    chunks = []
    total_rows = 0
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("pricing file PPUF_2025Q2.txt") as f:
            for chunk in pd.read_csv(f, sep='|', dtype=str, chunksize=1000000, low_memory=False):
                chunk.columns = [col.lower().replace(' ', '_') for col in chunk.columns]
                
                # Create plan_key
                chunk['plan_key'] = chunk['contract_id'] + '|' + chunk['plan_id'] + '|' + chunk['segment_id']
                
                # Convert numeric
                chunk['days_supply'] = pd.to_numeric(chunk['days_supply'], errors='coerce')
                chunk['unit_cost'] = pd.to_numeric(chunk['unit_cost'], errors='coerce')
                
                chunks.append(chunk[['plan_key', 'ndc', 'days_supply', 'unit_cost']])
                total_rows += len(chunk)
                
                print(f"   ... {total_rows:,} rows processed")
    
    print("   Combining chunks...")
    df = pd.concat(chunks, ignore_index=True)
    
    df.to_parquet(output_path, index=False, compression='snappy')
    
    print(f"   ✓ Saved {len(df):,} records → {output_path.name}")
    print(f"   Size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return len(df)

def convert_excluded_drugs():
    """Convert excluded drugs to Parquet"""
    print("\n7. Converting Excluded Drugs...")
    
    zip_path = DATA_DIR / "excluded drugs formulary file  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("excluded drugs formulary file  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', dtype=str, low_memory=False)
    
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    output_path = OUTPUT_DIR / "excluded_drugs.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')
    
    print(f"   ✓ Saved {len(df):,} records → {output_path.name}")
    print(f"   Size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return len(df)

def convert_indication_based():
    """Convert indication-based coverage to Parquet"""
    print("\n8. Converting Indication-Based Coverage...")
    
    zip_path = DATA_DIR / "indication based coverage formulary file  PPUF_2025Q2.zip"
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("Indication Based Coverage Formulary File  PPUF_2025Q2.txt") as f:
            df = pd.read_csv(f, sep='|', dtype=str, low_memory=False)
    
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    output_path = OUTPUT_DIR / "indication_based_coverage.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')
    
    print(f"   ✓ Saved {len(df):,} records → {output_path.name}")
    print(f"   Size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return len(df)

if __name__ == "__main__":
    try:
        # Check if --full-pricing flag is provided
        do_full_pricing = '--full-pricing' in sys.argv
        
        total_records = 0
        
        total_records += convert_geographic_locator()
        total_records += convert_plan_information()
        total_records += convert_formulary_drugs()
        total_records += convert_beneficiary_costs()
        total_records += convert_insulin_costs()
        total_records += convert_excluded_drugs()
        total_records += convert_indication_based()
        total_records += convert_pricing(do_full_pricing)
        
        # Summary
        print("\n" + "="*60)
        print("CONVERSION COMPLETE!")
        print("="*60)
        print(f"\nTotal records processed: {total_records:,}")
        print(f"Output directory: {OUTPUT_DIR}")
        
        # List files
        print("\nParquet files created:")
        for f in sorted(OUTPUT_DIR.glob("*.parquet")):
            size_mb = f.stat().st_size / 1024 / 1024
            print(f"  {f.name:40s} {size_mb:8.1f} MB")
        
        total_size = sum(f.stat().st_size for f in OUTPUT_DIR.glob("*.parquet"))
        print(f"\nTotal size: {total_size / 1024 / 1024:.1f} MB")
        
        print("\n" + "="*60)
        print("NEXT STEPS:")
        print("="*60)
        print("1. Query with DuckDB:")
        print("   python query_parquet.py")
        print("\n2. Load into pandas:")
        print("   import pandas as pd")
        print("   df = pd.read_parquet('medicare_parquet/plan_information.parquet')")
        print("\n3. (Optional) Convert pricing data:")
        print("   python convert_to_parquet.py --full-pricing")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

