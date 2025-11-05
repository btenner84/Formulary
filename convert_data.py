#!/usr/bin/env python3
"""
Simple ETL - Convert Medicare Part D data to Parquet
"""

import pandas as pd
import zipfile
from pathlib import Path

DATA_DIR = Path("/Users/bentenner/Dictionary/2025-Q2/SPUF_2025_20250703")
OUTPUT_DIR = Path("/Users/bentenner/Dictionary/2025-Q2/medicare_parquet")
OUTPUT_DIR.mkdir(exist_ok=True)

print("="*60)
print("MEDICARE PART D → PARQUET CONVERSION")
print("="*60)

# File mapping: (zip_name, txt_name_inside, output_name)
FILES = [
    ("geographic locator file  PPUF_2025Q2.zip", "geographic locator file PPUF_2025Q2.txt", "geographic_locator"),
    ("plan information  PPUF_2025Q2.zip", "plan information  PPUF_2025Q2.txt", "plan_information"),
    ("beneficiary cost file  PPUF_2025Q2.zip", "beneficiary cost file  PPUF_2025Q2.txt", "beneficiary_costs"),
    ("insulin beneficiary cost file  PPUF_2025Q2.zip", "insulin beneficiary cost file  PPUF_2025Q2.txt", "insulin_costs"),
    ("excluded drugs formulary file  PPUF_2025Q2.zip", "excluded drugs formulary file  PPUF_2025Q2.txt", "excluded_drugs"),
    ("indication based coverage formulary file  PPUF_2025Q2.zip", "Indication Based Coverage Formulary File  PPUF_2025Q2.txt", "indication_based"),
]

# Convert each file
total_records = 0
for zip_name, txt_name, output_name in FILES:
    try:
        print(f"\n{output_name}...")
        zip_path = DATA_DIR / zip_name
        
        with zipfile.ZipFile(zip_path) as z:
            with z.open(txt_name) as f:
                df = pd.read_csv(f, sep='|', dtype=str, low_memory=False)
        
        # Clean column names
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Create plan_key if needed
        if all(c in df.columns for c in ['contract_id', 'plan_id', 'segment_id']):
            df['plan_key'] = df['contract_id'] + '|' + df['plan_id'] + '|' + df['segment_id']
        
        # Save
        output_path = OUTPUT_DIR / f"{output_name}.parquet"
        df.to_parquet(output_path, index=False, compression='snappy')
        
        size_mb = output_path.stat().st_size / 1024 / 1024
        print(f"  ✓ {len(df):,} records → {size_mb:.1f} MB")
        total_records += len(df)
        
    except Exception as e:
        print(f"  ✗ Error: {e}")

# Formulary drugs (large file, needs chunking)
print(f"\nformulary_drugs (large file)...")
try:
    zip_path = DATA_DIR / "basic drugs formulary file  PPUF_2025Q2.zip"
    chunks = []
    
    with zipfile.ZipFile(zip_path) as z:
        with z.open("basic drugs formulary file  PPUF_2025Q2.txt") as f:
            for i, chunk in enumerate(pd.read_csv(f, sep='|', dtype=str, chunksize=200000)):
                chunks.append(chunk)
                if (i+1) % 5 == 0:
                    print(f"  ... chunk {i+1}")
    
    df = pd.concat(chunks, ignore_index=True)
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    output_path = OUTPUT_DIR / "formulary_drugs.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')
    
    size_mb = output_path.stat().st_size / 1024 / 1024
    print(f"  ✓ {len(df):,} records → {size_mb:.1f} MB")
    total_records += len(df)
    
except Exception as e:
    print(f"  ✗ Error: {e}")

print("\n" + "="*60)
print("CONVERSION COMPLETE!")
print("="*60)
print(f"\nTotal records: {total_records:,}")
print(f"Output: {OUTPUT_DIR}")

# List files
print("\nFiles created:")
for f in sorted(OUTPUT_DIR.glob("*.parquet")):
    size_mb = f.stat().st_size / 1024 / 1024
    print(f"  {f.name:40s} {size_mb:8.1f} MB")

total_size = sum(f.stat().st_size for f in OUTPUT_DIR.glob("*.parquet"))
print(f"\nTotal size: {total_size / 1024 / 1024 / 1024:.2f} GB")

print("\nNote: Pricing file (55M records) skipped - run separately if needed")

