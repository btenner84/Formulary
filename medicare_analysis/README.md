# Medicare Part D Analysis System

Complete data pipeline and analysis platform for Medicare Part D prescription drug plan data (Q2 2025).

## What This Does

Transforms 9 pipe-delimited ZIP files containing 55M+ records into a queryable PostgreSQL database with:
- **688 MA-PD contracts** (UnitedHealth, Humana, CVS/Aetna, etc.)
- **5,242 plans** across all US counties
- **384 formularies** with 6,024 drugs (1,896 specialty)
- **Complete pricing transparency** - negotiated costs per plan
- **Member cost-sharing** - copay/coinsurance structures

## Quick Start

### 1. Prerequisites
```bash
# Install PostgreSQL
brew install postgresql  # Mac
# or download from postgresql.org

# Start PostgreSQL
brew services start postgresql

# Create database
createdb medicare_partd
```

### 2. Setup Python Environment
```bash
cd medicare_analysis
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 3. Configure Database Connection
```bash
# Copy and edit .env file
cp .env.example .env

# Edit .env with your database credentials:
DATABASE_URL=postgresql://your_username:your_password@localhost:5432/medicare_partd
DATA_DIR=/Users/bentenner/Dictionary/2025-Q2/SPUF_2025_20250703
```

### 4. Create Database Schema
```bash
psql medicare_partd < sql/01_create_schema.sql
```

### 5. Load Data
```bash
cd etl
python load_data.py
```

**Expected time:**
- Plan information: 2-3 minutes
- Formulary drugs: 5-10 minutes  
- Beneficiary costs: 1-2 minutes
- Pricing data: **30-60 minutes** (55M records!)

## Key Views

### View 1: Formulary Analysis
**"Show me everything about formulary 00025456"**

```sql
-- See: sql/example_queries.sql

-- Get formulary overview
SELECT * FROM formulary_summary WHERE formulary_id = '00025456';

-- Get all plans using it
SELECT * FROM plans WHERE formulary_id = '00025456';

-- Get all drugs (with restrictions)
SELECT * FROM formulary_drugs WHERE formulary_id = '00025456';

-- Get specialty drugs only
SELECT * FROM formulary_drugs WHERE formulary_id = '00025456' AND tier = 5;
```

**Output:**
- 417 plans use this formulary
- 3,394 total drugs
- 675 specialty drugs
- Each drug shows: tier, quantity limits, prior auth, step therapy

### View 2: County Competition
**"Show me all plans in St. Louis County, MO"**

```sql
-- Get all plans in county
SELECT p.*, c.contract_name
FROM plan_geography pg
JOIN plans p ON p.plan_key = pg.plan_key
JOIN contracts c ON c.contract_id = p.contract_id
WHERE pg.county_code = '29189'
ORDER BY p.premium;
```

**Output:**
- All plans available in that county
- Premium + deductible for each
- Formulary assignments
- SNP type (dual eligible, etc.)

### View 3: Competitive Pricing
**"Compare specialty drug costs across plans"**

```sql
-- See full query in sql/example_queries.sql

-- For a specific drug in a specific county:
SELECT 
    p.contract_id,
    p.plan_name,
    dp.unit_cost as negotiated_cost,
    bc.cost_amt_pref as member_copay,
    (dp.unit_cost - bc.cost_amt_pref) as plan_net_cost
FROM drug_pricing dp
JOIN plans p ON p.plan_key = dp.plan_key
JOIN plan_geography pg ON pg.plan_key = p.plan_key
JOIN beneficiary_costs bc ON bc.plan_key = p.plan_key
WHERE pg.county_code = '29189'
  AND dp.ndc = '00002533754'
  AND dp.days_supply = 30;
```

**Output:**
- Humana pays $274.11, charges $0 → absorbs $274.11
- UnitedHealth pays $285.50, charges $71.38 → absorbs $214.12
- Anthem pays $268.00, charges $0 → absorbs $268.00

## Key Analysis Questions You Can Answer

### 1. Negotiating Power
**"Which company gets best specialty drug prices?"**
```sql
SELECT contract_id, AVG(unit_cost) as avg_cost
FROM drug_pricing dp
JOIN plans p ON p.plan_key = dp.plan_key
JOIN formulary_drugs fd ON fd.ndc = dp.ndc AND fd.formulary_id = p.formulary_id
WHERE fd.tier = 5 AND dp.days_supply = 30
GROUP BY contract_id
ORDER BY avg_cost;
```

### 2. Member Cost Competitiveness
**"Lowest cost plan for a specialty drug patient?"**
```sql
-- Use member cost calculator query in example_queries.sql
-- Input: List of patient's drugs
-- Output: Total annual cost by plan (premium + deductible + drug costs)
```

### 3. Formulary Restrictiveness
**"Which formularies have most prior authorizations?"**
```sql
SELECT 
    formulary_id,
    COUNT(*) as total_drugs,
    SUM(CASE WHEN prior_authorization_yn != 'N' THEN 1 ELSE 0 END) as drugs_with_pa,
    ROUND(SUM(CASE WHEN prior_authorization_yn != 'N' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1) as pct_with_pa
FROM formulary_drugs
GROUP BY formulary_id
ORDER BY pct_with_pa DESC;
```

### 4. Geographic Competition
**"Which counties have most plan options?"**
```sql
SELECT * FROM county_plan_summary
ORDER BY plan_count DESC
LIMIT 20;
```

## Database Schema

### Core Tables
- `contracts` - 688 MA-PD, PDP, PACE contracts
- `plans` - 5,242 unique plans
- `formularies` - 384 formularies
- `formulary_drugs` - 1.3M formulary entries
- `drug_pricing` - 55.5M pricing records
- `beneficiary_costs` - 165K cost-sharing records
- `plan_geography` - 115K plan-county relationships
- `geographic_locator` - 3,280 counties

### Materialized Views
- `formulary_summary` - Quick stats per formulary
- `county_plan_summary` - Quick stats per county

## Next Steps

### Option A: Direct SQL Analysis
```bash
# Connect to database
psql medicare_partd

# Run queries from example_queries.sql
\i sql/example_queries.sql
```

### Option B: Build API (FastAPI)
```python
# See: api/main.py (create this file)
# Endpoints:
# - GET /formulary/{id}
# - GET /formulary/{id}/drugs
# - GET /county/{code}/plans
# - GET /drug/{ndc}/pricing
```

### Option C: Build Dashboard (Streamlit)
```python
# See: app/streamlit_app.py (create this file)
# Views:
# - Formulary browser
# - County competition viewer
# - Drug pricing comparison
# - Member cost calculator
```

## Data Dictionary

### Key Fields

**Plans:**
- `contract_id` - Company identifier (H=MA-PD, S=PDP)
- `plan_id` - Plan number within contract
- `formulary_id` - Which drug list this plan uses
- `premium` - Monthly cost
- `deductible` - Annual deductible
- `snp_type` - 0=Standard, 2=Dual Eligible, etc.

**Formulary Drugs:**
- `ndc` - National Drug Code (unique drug identifier)
- `tier` - 1=Generic, 5=Specialty, etc.
- `prior_authorization_yn` - Requires approval?
- `quantity_limit_yn` - Has usage limits?
- `step_therapy_yn` - Must try cheaper drug first?

**Pricing:**
- `unit_cost` - What plan pays pharmacy (wholesale)

**Beneficiary Costs:**
- `cost_type_pref` - 0=Copay, 1=Coinsurance, 2=No charge
- `cost_amt_pref` - Dollar amount or percentage
- `cost_max_amt_pref` - Cap on coinsurance

## Performance Tips

### Indexes
Already created on:
- `plans.formulary_id`
- `formulary_drugs.formulary_id`
- `formulary_drugs.ndc`
- `drug_pricing.plan_key, ndc`
- `plan_geography.county_code`

### Query Optimization
- Use materialized views for aggregates
- Filter specialty drugs with `tier = 5`
- Always specify `days_supply = 30` for pricing
- Join on `plan_key` not individual IDs

### Large Queries
```sql
-- Pricing queries can be slow (55M records)
-- Always filter by:
WHERE dp.ndc = 'specific_drug'  -- or IN list
  AND dp.days_supply = 30
  AND p.contract_id IN ('H0028', 'H0034', ...)  -- limit contracts
```

## File Sizes

- **Total uncompressed:** ~25 GB
- **Database size:** ~15 GB (with indexes)
- **Largest table:** drug_pricing (55.5M rows)

## Support

See `BUILD_PLAN.md` for detailed architecture and implementation guide.

## Example Outputs

### Formulary View
```
Formulary: 00025456
Plans using: 417
Total drugs: 3,394
Specialty drugs: 675

Top 10 Specialty Drugs:
NDC          | Tier | PA | QL | Sample Cost
00002533754  | 5    | Y  | Y  | $274.11
00002481554  | 5    | Y  | Y  | $268.42
...
```

### County View
```
County: St. Louis, MO (29189)
Plans available: 47
Avg premium: $23.50

Plan Comparison:
Company    | Plan      | Premium | Deductible | Specialty Copay
Humana     | H0028-014 | $0      | $250       | $0
UnitedHlth | Hxxxx-yyy | $25     | $480       | 25% ($300 max)
```

### Competitive Analysis
```
Specialty Drug: NDC 00002533754

Contract   | Avg Cost | Best Plan Cost | Worst Plan Cost | Variation
H0022      | $262.86  | $262.86        | $262.86         | 0%
H0028      | $278.15  | $274.11        | $292.64         | 6.8%
```

This is your competitive intelligence platform!

