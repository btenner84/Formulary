# Medicare Part D Data Analysis System - Build Plan

## OBJECTIVE
Build a queryable database and API to analyze Medicare Part D formulary, pricing, and competitive data.

---

## SYSTEM ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────┐
│                    RAW DATA FILES                           │
│  (9 pipe-delimited text files in ZIP archives)             │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                ETL PIPELINE (Python)                        │
│  - Extract from ZIPs                                        │
│  - Parse pipe-delimited format                              │
│  - Clean & validate data                                    │
│  - Transform to relational schema                           │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│              POSTGRESQL DATABASE                            │
│  Tables:                                                    │
│  - contracts                                                │
│  - plans                                                    │
│  - formularies                                              │
│  - formulary_drugs                                          │
│  - drug_pricing                                             │
│  - beneficiary_costs                                        │
│  - plan_geography                                           │
│  - pharmacy_networks                                        │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                  API LAYER (FastAPI)                        │
│  Endpoints:                                                 │
│  - GET /formulary/{id}                                      │
│  - GET /formulary/{id}/plans                                │
│  - GET /formulary/{id}/drugs                                │
│  - GET /county/{fips}/plans                                 │
│  - GET /drug/{ndc}/pricing                                  │
│  - GET /analysis/specialty-drugs                            │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│            FRONTEND/ANALYSIS TOOLS                          │
│  Option A: Streamlit Dashboard                              │
│  Option B: React Web App                                    │
│  Option C: Jupyter Notebooks + SQL                          │
└─────────────────────────────────────────────────────────────┘
```

---

## DATABASE SCHEMA

### Table: `contracts`
```sql
CREATE TABLE contracts (
    contract_id VARCHAR(10) PRIMARY KEY,
    contract_name VARCHAR(500),
    contract_type VARCHAR(10) -- H=MA-PD, S=PDP, R=PACE
);
```

### Table: `plans`
```sql
CREATE TABLE plans (
    plan_key VARCHAR(20) PRIMARY KEY, -- contract_id|plan_id|segment_id
    contract_id VARCHAR(10) REFERENCES contracts(contract_id),
    plan_id VARCHAR(10),
    segment_id VARCHAR(10),
    plan_name VARCHAR(500),
    formulary_id VARCHAR(20),
    premium DECIMAL(10,2),
    deductible DECIMAL(10,2),
    snp_type INTEGER,
    UNIQUE(contract_id, plan_id, segment_id)
);
```

### Table: `formularies`
```sql
CREATE TABLE formularies (
    formulary_id VARCHAR(20) PRIMARY KEY,
    formulary_version VARCHAR(10),
    contract_year INTEGER,
    drug_count INTEGER,
    specialty_drug_count INTEGER
);
```

### Table: `formulary_drugs`
```sql
CREATE TABLE formulary_drugs (
    id SERIAL PRIMARY KEY,
    formulary_id VARCHAR(20) REFERENCES formularies(formulary_id),
    rxcui VARCHAR(20),
    ndc VARCHAR(20),
    tier INTEGER,
    quantity_limit_yn CHAR(1),
    quantity_limit_amount INTEGER,
    quantity_limit_days INTEGER,
    prior_authorization_yn CHAR(1),
    prior_authorization_days INTEGER,
    step_therapy_yn CHAR(1),
    is_specialty BOOLEAN, -- Derived from tier 5 or specialty flag
    INDEX(formulary_id),
    INDEX(ndc),
    INDEX(tier)
);
```

### Table: `drug_pricing`
```sql
CREATE TABLE drug_pricing (
    id SERIAL PRIMARY KEY,
    plan_key VARCHAR(20) REFERENCES plans(plan_key),
    ndc VARCHAR(20),
    days_supply INTEGER,
    unit_cost DECIMAL(10,4),
    INDEX(plan_key),
    INDEX(ndc),
    UNIQUE(plan_key, ndc, days_supply)
);
```

### Table: `beneficiary_costs`
```sql
CREATE TABLE beneficiary_costs (
    id SERIAL PRIMARY KEY,
    plan_key VARCHAR(20) REFERENCES plans(plan_key),
    coverage_level INTEGER,
    tier INTEGER,
    days_supply INTEGER,
    tier_specialty_yn CHAR(1),
    deductible_applies_yn CHAR(1),
    -- Preferred retail
    cost_type_pref INTEGER, -- 0=copay, 1=coinsurance, 2=no charge
    cost_amt_pref DECIMAL(10,4),
    cost_min_amt_pref DECIMAL(10,2),
    cost_max_amt_pref DECIMAL(10,2),
    -- Non-preferred retail
    cost_type_nonpref INTEGER,
    cost_amt_nonpref DECIMAL(10,4),
    cost_min_amt_nonpref DECIMAL(10,2),
    cost_max_amt_nonpref DECIMAL(10,2),
    -- Mail order costs omitted for brevity
    INDEX(plan_key),
    INDEX(tier),
    UNIQUE(plan_key, coverage_level, tier, days_supply)
);
```

### Table: `plan_geography`
```sql
CREATE TABLE plan_geography (
    id SERIAL PRIMARY KEY,
    plan_key VARCHAR(20) REFERENCES plans(plan_key),
    state_code CHAR(2),
    county_code VARCHAR(10),
    county_name VARCHAR(100),
    ma_region_code VARCHAR(10),
    pdp_region_code VARCHAR(10),
    INDEX(plan_key),
    INDEX(state_code),
    INDEX(county_code)
);
```

### Table: `geographic_locator`
```sql
CREATE TABLE geographic_locator (
    county_code VARCHAR(10) PRIMARY KEY,
    state_name VARCHAR(50),
    county_name VARCHAR(100),
    ma_region_code VARCHAR(10),
    ma_region_name VARCHAR(100),
    pdp_region_code VARCHAR(10),
    pdp_region_name VARCHAR(100)
);
```

---

## VIEW 1: FORMULARY DETAILED VIEW

### Query Structure
```sql
-- Get formulary overview
SELECT 
    f.formulary_id,
    f.drug_count,
    f.specialty_drug_count,
    COUNT(DISTINCT p.plan_key) as plans_using_formulary
FROM formularies f
LEFT JOIN plans p ON p.formulary_id = f.formulary_id
WHERE f.formulary_id = '00025456'
GROUP BY f.formulary_id;

-- Get all plans using this formulary
SELECT 
    p.contract_id,
    p.plan_id,
    p.plan_name,
    p.premium,
    p.deductible,
    COUNT(DISTINCT pg.county_code) as counties_covered
FROM plans p
LEFT JOIN plan_geography pg ON pg.plan_key = p.plan_key
WHERE p.formulary_id = '00025456'
GROUP BY p.plan_key
ORDER BY p.premium;

-- Get all drugs in formulary with cost structure
SELECT 
    fd.ndc,
    fd.rxcui,
    fd.tier,
    fd.quantity_limit_yn,
    fd.prior_authorization_yn,
    fd.step_therapy_yn,
    fd.is_specialty,
    -- Sample pricing from one plan
    dp.unit_cost,
    -- Sample beneficiary cost
    bc.cost_type_pref,
    bc.cost_amt_pref,
    bc.cost_max_amt_pref
FROM formulary_drugs fd
LEFT JOIN plans p ON p.formulary_id = fd.formulary_id
LEFT JOIN drug_pricing dp ON dp.plan_key = p.plan_key AND dp.ndc = fd.ndc AND dp.days_supply = 30
LEFT JOIN beneficiary_costs bc ON bc.plan_key = p.plan_key AND bc.tier = fd.tier AND bc.coverage_level = 1 AND bc.days_supply = 1
WHERE fd.formulary_id = '00025456'
  AND p.plan_key = (SELECT plan_key FROM plans WHERE formulary_id = '00025456' LIMIT 1)
ORDER BY fd.tier, fd.ndc;
```

---

## VIEW 2: COUNTY COMPETITIVE VIEW

### Query Structure
```sql
-- Get all plans in a county
SELECT 
    p.contract_id,
    p.plan_id,
    c.contract_name,
    p.plan_name,
    p.premium,
    p.deductible,
    p.formulary_id,
    f.specialty_drug_count,
    p.snp_type
FROM plan_geography pg
JOIN plans p ON p.plan_key = pg.plan_key
JOIN contracts c ON c.contract_id = p.contract_id
JOIN formularies f ON f.formulary_id = p.formulary_id
WHERE pg.county_code = '29189' -- St. Louis County, MO
ORDER BY p.premium, p.deductible;

-- Get specialty drug pricing comparison in county
SELECT 
    dp.ndc,
    p.contract_id,
    p.plan_id,
    p.premium,
    p.deductible,
    dp.unit_cost as negotiated_cost,
    bc.cost_amt_pref as member_cost,
    (dp.unit_cost - COALESCE(bc.cost_amt_pref * dp.unit_cost, bc.cost_amt_pref, 0)) as plan_net_cost
FROM plan_geography pg
JOIN plans p ON p.plan_key = pg.plan_key
JOIN formulary_drugs fd ON fd.formulary_id = p.formulary_id
JOIN drug_pricing dp ON dp.plan_key = p.plan_key AND dp.ndc = fd.ndc
JOIN beneficiary_costs bc ON bc.plan_key = p.plan_key AND bc.tier = fd.tier
WHERE pg.county_code = '29189'
  AND fd.is_specialty = TRUE
  AND dp.days_supply = 30
  AND bc.coverage_level = 1
  AND bc.days_supply = 1
ORDER BY dp.ndc, p.plan_key;
```

---

## IMPLEMENTATION STEPS

### Step 1: Setup Environment
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install pandas psycopg2-binary sqlalchemy fastapi uvicorn streamlit
```

### Step 2: ETL Pipeline (Python)
```python
# File: etl/load_data.py

import pandas as pd
import zipfile
from sqlalchemy import create_engine
from pathlib import Path

# Database connection
DATABASE_URL = "postgresql://user:password@localhost:5432/medicare_partd"
engine = create_engine(DATABASE_URL)

# Extract and load each file
data_dir = Path("/Users/bentenner/Dictionary/2025-Q2/SPUF_2025_20250703")

# 1. Load plan information
with zipfile.ZipFile(data_dir / "plan information  PPUF_2025Q2.zip") as z:
    with z.open("plan information  PPUF_2025Q2.txt") as f:
        df_plans = pd.read_csv(f, sep='|')
        # Transform and load to database
        # ... processing logic

# 2. Load formulary
with zipfile.ZipFile(data_dir / "basic drugs formulary file  PPUF_2025Q2.zip") as z:
    with z.open("basic drugs formulary file  PPUF_2025Q2.txt") as f:
        df_formulary = pd.read_csv(f, sep='|', chunksize=100000)
        for chunk in df_formulary:
            # Process in chunks due to size
            chunk.to_sql('formulary_drugs', engine, if_exists='append', index=False)

# ... continue for all files
```

### Step 3: API Layer (FastAPI)
```python
# File: api/main.py

from fastapi import FastAPI
from sqlalchemy import create_engine
import pandas as pd

app = FastAPI()
engine = create_engine(DATABASE_URL)

@app.get("/formulary/{formulary_id}")
def get_formulary(formulary_id: str):
    query = f"""
    SELECT f.*, COUNT(DISTINCT p.plan_key) as plan_count
    FROM formularies f
    LEFT JOIN plans p ON p.formulary_id = f.formulary_id
    WHERE f.formulary_id = '{formulary_id}'
    GROUP BY f.formulary_id
    """
    return pd.read_sql(query, engine).to_dict('records')

@app.get("/formulary/{formulary_id}/drugs")
def get_formulary_drugs(formulary_id: str, specialty_only: bool = False):
    query = f"""
    SELECT * FROM formulary_drugs
    WHERE formulary_id = '{formulary_id}'
    """
    if specialty_only:
        query += " AND is_specialty = TRUE"
    query += " ORDER BY tier, ndc"
    return pd.read_sql(query, engine).to_dict('records')

@app.get("/county/{county_code}/plans")
def get_county_plans(county_code: str):
    query = f"""
    SELECT p.*, c.contract_name
    FROM plan_geography pg
    JOIN plans p ON p.plan_key = pg.plan_key
    JOIN contracts c ON c.contract_id = p.contract_id
    WHERE pg.county_code = '{county_code}'
    ORDER BY p.premium
    """
    return pd.read_sql(query, engine).to_dict('records')
```

### Step 4: Frontend (Streamlit)
```python
# File: app/streamlit_app.py

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

st.title("Medicare Part D Analysis Platform")

# Sidebar navigation
view = st.sidebar.selectbox("Select View", ["Formulary View", "County View"])

if view == "Formulary View":
    formulary_id = st.text_input("Formulary ID", "00025456")
    
    if st.button("Load Formulary"):
        # Query database
        df_drugs = pd.read_sql(f"SELECT * FROM formulary_drugs WHERE formulary_id = '{formulary_id}'", engine)
        
        st.subheader(f"Formulary {formulary_id}")
        st.metric("Total Drugs", len(df_drugs))
        st.metric("Specialty Drugs", len(df_drugs[df_drugs['is_specialty'] == True]))
        
        # Display drugs table
        st.dataframe(df_drugs)

elif view == "County View":
    county_code = st.text_input("County Code (FIPS)", "29189")
    
    if st.button("Load County Plans"):
        df_plans = pd.read_sql(f"SELECT * FROM plan_geography pg JOIN plans p ON p.plan_key = pg.plan_key WHERE pg.county_code = '{county_code}'", engine)
        
        st.subheader(f"Plans in County {county_code}")
        st.dataframe(df_plans)
```

---

## TECHNOLOGY STACK RECOMMENDATION

### Option A: Full Stack (Recommended for Production)
- **Database:** PostgreSQL (handles 55M+ records efficiently)
- **ETL:** Python + Pandas + SQLAlchemy
- **API:** FastAPI (fast, async, auto-documentation)
- **Frontend:** React or Streamlit
- **Hosting:** AWS RDS (database) + EC2 (API/app)

### Option B: Rapid Prototype
- **Database:** SQLite (simpler, file-based)
- **ETL:** Python + Pandas
- **Analysis:** Jupyter Notebooks
- **Visualization:** Matplotlib, Plotly
- **No API layer needed initially**

### Option C: Analytics-Focused
- **Database:** PostgreSQL
- **Analysis:** DBT (data transformations)
- **Visualization:** Tableau or Looker
- **Direct SQL queries**

---

## ESTIMATED BUILD TIME

| Phase | Time | Complexity |
|-------|------|-----------|
| Database setup | 2-4 hours | Low |
| ETL pipeline | 8-16 hours | Medium (data cleaning) |
| API development | 4-8 hours | Low-Medium |
| Frontend (basic) | 8-16 hours | Medium |
| Testing & optimization | 8+ hours | Medium |
| **TOTAL** | **30-50 hours** | |

---

## NEXT STEPS

1. **Choose tech stack** (recommend Option A for scalability)
2. **Setup PostgreSQL database**
3. **Build ETL pipeline** (start with small files, then scale)
4. **Create core queries** for formulary and county views
5. **Build API endpoints**
6. **Create frontend interface**
7. **Optimize queries** (indexes, materialized views)
8. **Add analytics features** (comparison tools, dashboards)

---

## SAMPLE VIEWS TO BUILD

### View 1: Formulary Detail Page
```
┌─────────────────────────────────────────────────────────────┐
│ FORMULARY 00025456                                          │
│                                                             │
│ Used by 417 plans | 3,394 drugs | 675 specialty drugs     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ PLANS USING THIS FORMULARY:                                │
│ ┌───────────────────────────────────────────────────────┐ │
│ │ Contract  Plan   Premium  Deductible  Counties       │ │
│ │ H0028     007    $50.60   $590        45 counties    │ │
│ │ H0028     015    $0.00    $590        38 counties    │ │
│ │ ...                                                    │ │
│ └───────────────────────────────────────────────────────┘ │
│                                                             │
│ DRUGS IN FORMULARY:                                         │
│ [Filter: ▼ All Tiers  ▼ All Drugs  ☑ Specialty Only]      │
│ ┌───────────────────────────────────────────────────────┐ │
│ │ NDC          Tier  Restrictions  Sample Cost  Copay  │ │
│ │ 00002533754  5     PA,QL         $274.11     $0      │ │
│ │ 00002481554  5     PA,QL         $268.42     $0      │ │
│ │ ...                                                    │ │
│ └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### View 2: County Comparison
```
┌─────────────────────────────────────────────────────────────┐
│ ST. LOUIS COUNTY, MO (29189)                                │
│                                                             │
│ 47 plans available | Avg premium: $23.50                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ PLAN COMPARISON:                                            │
│ ┌───────────────────────────────────────────────────────┐ │
│ │ Company    Plan     Premium  Deduct  Specialty Copay │ │
│ │ Humana     H0028-014 $0      $250    $0              │ │
│ │ UnitedHlth Hxxxx-yyy $25     $480    25% ($300 max)  │ │
│ │ Anthem     Hxxxx-zzz $15     $400    $0              │ │
│ │ ...                                                    │ │
│ └───────────────────────────────────────────────────────┘ │
│                                                             │
│ SPECIALTY DRUG PRICING (Sample: 10 most utilized):         │
│ ┌───────────────────────────────────────────────────────┐ │
│ │ Drug          Humana    UnitedH   Anthem              │ │
│ │ NDC 00002...  $274.11   $285.50   $268.00            │ │
│ │ Member pays:  $0        $71.38    $0                  │ │
│ │ Plan absorbs: $274.11   $214.12   $268.00            │ │
│ └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## QUERIES YOU'LL BE ABLE TO RUN

1. "Show me all drugs in Humana's formulary 00025456"
2. "Compare specialty drug costs across all Humana plans"
3. "Which plans in Missouri have the lowest member cost for Drug X?"
4. "Rank all MA-PD contracts by average specialty drug negotiated cost"
5. "Show geographic coverage for Contract H0028"
6. "Compare formulary restrictiveness (PA, step therapy) across contracts"
7. "Calculate total annual member cost for a specific drug list by plan"

This will be your competitive intelligence platform!

