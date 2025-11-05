import pandas as pd
import zipfile
from pathlib import Path
import time

DATA_DIR = Path("SPUF_2025_20250703")
OUTPUT_DIR = Path("medicare_parquet")

print("="*80)
print("CONVERTING PRICING DATA")
print("="*80)

zip_path = DATA_DIR / "pricing file PPUF_2025Q2.zip"

chunks = []
total_rows = 0
start_time = time.time()

print("\nExtracting and processing pricing data...")
print("This will take 20-30 minutes for ~55M records\n")

with zipfile.ZipFile(zip_path) as z:
    with z.open("pricing file PPUF_2025Q2.txt") as f:
        for i, chunk in enumerate(pd.read_csv(f, sep='|', dtype=str, chunksize=1000000, low_memory=False, encoding='latin-1')):
            # Clean column names
            chunk.columns = [col.lower().replace(' ', '_') for col in chunk.columns]
            
            # Create plan_key for joining
            chunk['plan_key'] = chunk['contract_id'] + '|' + chunk['plan_id'] + '|' + chunk['segment_id']
            
            # Convert numeric columns
            chunk['days_supply'] = pd.to_numeric(chunk['days_supply'], errors='coerce')
            chunk['unit_cost'] = pd.to_numeric(chunk['unit_cost'], errors='coerce')
            
            # Keep only needed columns
            chunk = chunk[['plan_key', 'ndc', 'days_supply', 'unit_cost', 'contract_id', 'plan_id']]
            
            chunks.append(chunk)
            total_rows += len(chunk)
            
            elapsed = time.time() - start_time
            rate = total_rows / elapsed if elapsed > 0 else 0
            remaining = (55500000 - total_rows) / rate if rate > 0 else 0
            
            print(f"Chunk {i+1:3d}: {total_rows:10,} rows | {elapsed/60:5.1f} min elapsed | ~{remaining/60:4.0f} min remaining | Rate: {rate:,.0f} rows/sec")
            
            # Save in batches to avoid memory issues
            if len(chunks) >= 5:
                print("  💾 Saving batch...")
                batch_df = pd.concat(chunks, ignore_index=True)
                
                output_path = OUTPUT_DIR / "drug_pricing.parquet"
                if output_path.exists():
                    # Append to existing
                    existing = pd.read_parquet(output_path)
                    batch_df = pd.concat([existing, batch_df], ignore_index=True)
                
                batch_df.to_parquet(output_path, index=False, compression='snappy')
                chunks = []
                size_mb = output_path.stat().st_size / 1024 / 1024
                print(f"  ✅ Saved {total_rows:,} total rows ({size_mb:.1f} MB)\n")

# Save final batch
if chunks:
    print("\n💾 Saving final batch...")
    batch_df = pd.concat(chunks, ignore_index=True)
    
    output_path = OUTPUT_DIR / "drug_pricing.parquet"
    if output_path.exists():
        existing = pd.read_parquet(output_path)
        batch_df = pd.concat([existing, batch_df], ignore_index=True)
    
    batch_df.to_parquet(output_path, index=False, compression='snappy')

output_path = OUTPUT_DIR / "drug_pricing.parquet"
size_mb = output_path.stat().st_size / 1024 / 1024
elapsed = time.time() - start_time

print("\n" + "="*80)
print("✅ PRICING DATA CONVERSION COMPLETE!")
print("="*80)
print(f"Total records: {total_rows:,}")
print(f"Output size: {size_mb:.1f} MB")
print(f"Time taken: {elapsed/60:.1f} minutes")
print(f"Average rate: {total_rows/elapsed:,.0f} rows/second")
print(f"File: {output_path}")
print("="*80)

