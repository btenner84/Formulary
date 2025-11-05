# Medicare Part D Data - READY TO USE! ✅

## What You Have

I've converted all Medicare Part D Q2 2025 data to **Parquet files** (fast, compressed, queryable):

```
medicare_parquet/
├── plan_information.parquet      115K records  (all plans + geography)
├── formulary_drugs.parquet       1.3M records  (all drugs in formularies)
├── beneficiary_costs.parquet     165K records  (member copay/coinsurance)
├── geographic_locator.parquet    3.3K records  (county mapping)
├── insulin_costs.parquet         40K records   (insulin-specific costs)
├── excluded_drugs.parquet        15K records   (excluded drugs)
└── indication_based.parquet      650 records   (diagnosis-required drugs)

Total: 5.5 MB (compressed!)
```

## Quick Start (3 options)

### Option 1: Run Example Queries (Easiest!)
```bash
cd /Users/bentenner/Dictionary/2025-Q2
python3 query_medicare.py
```

**This shows you:**
- Formulary summaries
- Top contracts
- Plans by geography
- Specialty drug analysis

### Option 2: Custom SQL Queries
```python
import duckdb

conn = duckdb.connect()
conn.execute("CREATE VIEW plans AS SELECT * FROM 'medicare_parquet/plan_information.parquet'")

# Query like a database!
result = conn.execute("""
    SELECT contract_id, COUNT(*) as plan_count
    FROM plans
    GROUP BY contract_id
    ORDER BY plan_count DESC
    LIMIT 10
""").df()

print(result)
```

### Option 3: Use Pandas
```python
import pandas as pd

# Load any file
df_plans = pd.read_parquet('medicare_parquet/plan_information.parquet')
df_formulary = pd.read_parquet('medicare_parquet/formulary_drugs.parquet')

# Filter/analyze
humana_plans = df_plans[df_plans['contract_id'].str.startswith('H0028')]
specialty_drugs = df_formulary[df_formulary['tier_level_value'] == '5']

print(f"Humana has {len(humana_plans)} plan-county offerings")
print(f"Found {len(specialty_drugs)} specialty drug entries")
```

## Key Views You Wanted

### View 1: Formulary Detail
```python
import duckdb
conn = duckdb.connect()
conn.execute("CREATE VIEW formulary AS SELECT * FROM 'medicare_parquet/formulary_drugs.parquet'")
conn.execute("CREATE VIEW plans AS SELECT * FROM 'medicare_parquet/plan_information.parquet'")

# Get all plans using a formulary
formulary_id = '00025456'
result = conn.execute(f"""
    SELECT DISTINCT 
        contract_id, plan_id, plan_name, premium, deductible
    FROM plans
    WHERE formulary_id = '{formulary_id}'
    ORDER BY premium
""").df()

print(result)

# Get all drugs in that formulary
drugs = conn.execute(f"""
    SELECT 
        ndc, tier_level_value as tier,
        quantity_limit_yn, prior_authorization_yn
    FROM formulary
    WHERE formulary_id = '{formulary_id}'
    ORDER BY tier, ndc
""").df()

print(f"\n{len(drugs)} drugs in this formulary")
```

### View 2: County Competition
```python
# All plans in a county
county_code = '29189'  # St. Louis, MO

plans = conn.execute(f"""
    SELECT 
        contract_id, plan_id, plan_name,
        CAST(premium AS FLOAT) as premium,
        CAST(deductible AS FLOAT) as deductible,
        formulary_id
    FROM plans
    WHERE county_code = '{county_code}'
    ORDER BY premium, deductible
""").df()

print(f"{len(plans)} plans available in this county")
print(plans.head(20))
```

## What's Missing (Optional)

**Pricing data (55M records)** - Skipped due to size, but can add if needed:
- Shows what each plan pays for each drug
- Needed for competitive pricing analysis
- Takes 30-60 minutes to convert

**To add pricing:**
```bash
cd /Users/bentenner/Dictionary/2025-Q2
python3 -c "
import pandas as pd
import zipfile
from pathlib import Path

print('Converting pricing data (this takes 30+ minutes)...')
DATA_DIR = Path('SPUF_2025_20250703')
OUTPUT_DIR = Path('medicare_parquet')

zip_path = DATA_DIR / 'pricing file PPUF_2025Q2.zip'
chunks = []
total = 0

with zipfile.ZipFile(zip_path) as z:
    with z.open('pricing file PPUF_2025Q2.txt') as f:
        for chunk in pd.read_csv(f, sep='|', dtype=str, chunksize=1000000):
            chunk.columns = [c.lower().replace(' ', '_') for c in chunk.columns]
            chunk['plan_key'] = chunk['contract_id'] + '|' + chunk['plan_id'] + '|' + chunk['segment_id']
            chunks.append(chunk[['plan_key', 'ndc', 'days_supply', 'unit_cost']])
            total += len(chunk)
            print(f'... {total:,} rows')
            
df = pd.concat(chunks)
df.to_parquet(OUTPUT_DIR / 'drug_pricing.parquet', compression='snappy')
print('Done!')
"
```

## Next Steps

1. ✅ **Data is ready** - All Parquet files created
2. ✅ **Query examples** - Run `python3 query_medicare.py`
3. ✅ **Custom analysis** - Use DuckDB or pandas

### Build Your Views:

**Formulary Browser:**
- Input: formulary_id
- Show: All plans using it + all drugs in it

**County Comparison:**
- Input: county_code
- Show: All plans available + pricing comparison

**Specialty Drug Analysis:**
- Filter: tier_level_value = '5'
- Show: Coverage across formularies
- Compare: Copay/coinsurance structures

## Files Explained

| File | Records | What It Has |
|------|---------|-------------|
| `plan_information` | 115K | Every plan in every county: contract, premium, deductible, formulary |
| `formulary_drugs` | 1.3M | Every drug in every formulary: NDC, tier, restrictions |
| `beneficiary_costs` | 165K | Member cost-sharing by plan+tier: copay or coinsurance % |
| `geographic_locator` | 3.3K | County→State→Region mapping |

## Key Columns

**plan_information:**
- `contract_id` - Company (H=MA-PD, S=PDP)
- `plan_key` - Unique plan identifier (contract|plan|segment)
- `formulary_id` - Which drug list
- `premium`, `deductible` - Monthly/annual costs
- `county_code` - Where available

**formulary_drugs:**
- `formulary_id` - Which formulary
- `ndc` - Drug identifier
- `tier_level_value` - 1=Generic, 5=Specialty, etc.
- `prior_authorization_yn` - Need approval?
- `quantity_limit_yn` - Usage limits?

**beneficiary_costs:**
- `plan_key` - Which plan
- `tier` - Drug tier
- `cost_type_pref` - 0=Copay, 1=Coinsurance
- `cost_amt_pref` - Dollar amount or %
- `cost_max_amt_pref` - Cap on coinsurance

## Your Analysis Platform is Ready!

No database setup needed - just run Python and query with SQL or pandas!

**Questions you can answer NOW:**
- ✅ "Show me all Humana plans in Missouri"
- ✅ "Which formularies have the most specialty drugs?"
- ✅ "What drugs require prior authorization?"
- ✅ "Compare plans in St. Louis County"
- ✅ "Find all plans using formulary 00025456"

**With pricing data (optional):**
- "Which company pays least for specialty drugs?"
- "Compare drug costs across all Humana plans"
- "Calculate plan margins on high-cost drugs"

---

## Support

- **Examples:** `python3 query_medicare.py`
- **Documentation:** See `BUILD_PLAN.md` for architecture
- **Raw data:** `/Users/bentenner/Dictionary/2025-Q2/SPUF_2025_20250703/`
- **Parquet files:** `/Users/bentenner/Dictionary/2025-Q2/medicare_parquet/`

**Your competitive intelligence platform is ready to use!** 🎉

