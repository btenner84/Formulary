from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
import duckdb
import pandas as pd
import os
from pathlib import Path
from typing import Optional

app = FastAPI(title="Medicare Part D Intelligence Platform")

# Mount static files and templates
app.mount("/static", StaticFiles(directory="webapp/static"), name="static")
templates = Jinja2Templates(directory="webapp/templates")

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "formulary2026")
S3_PREFIX = os.getenv("S3_PREFIX", "medicare_parquet")
USE_S3 = os.getenv("USE_S3", "true").lower() == "true"

# Local fallback
BASE_DIR = Path(__file__).parent.parent
LOCAL_DATA_DIR = BASE_DIR / "medicare_parquet"

def get_db(year: str = "2025"):
    """
    Get database connection with data loaded for specified year.
    Args:
        year: Year to load data for ("2025" or "2026")
    """
    conn = duckdb.connect(':memory:')
    
    # Install and load httpfs extension for S3 access
    if USE_S3:
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # Configure S3 credentials
        conn.execute(f"SET s3_region='us-east-1';")
        
        # Set credentials from environment variables (required for production)
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        if not aws_key or not aws_secret:
            raise ValueError("AWS credentials not found in environment variables!")
        
        conn.execute(f"SET s3_access_key_id='{aws_key}';")
        conn.execute(f"SET s3_secret_access_key='{aws_secret}';")
        
        # Use S3 paths with year
        data_path = f"s3://{S3_BUCKET}/{year}"
    else:
        # Use local paths
        data_path = str(LOCAL_DATA_DIR)
    
    # Load all parquet files
    conn.execute(f"""
        CREATE VIEW plans AS 
        SELECT * FROM read_parquet('{data_path}/plan_information.parquet')
    """)
    
    conn.execute(f"""
        CREATE VIEW formulary_drugs AS 
        SELECT * FROM read_parquet('{data_path}/formulary_drugs.parquet')
    """)
    
    conn.execute(f"""
        CREATE VIEW beneficiary_costs AS 
        SELECT * FROM read_parquet('{data_path}/beneficiary_costs.parquet')
    """)
    
    conn.execute(f"""
        CREATE VIEW geographic AS 
        SELECT * FROM read_parquet('{data_path}/geographic_locator.parquet')
    """)
    
    # Load pricing data (only available for 2025, not yet for 2026)
    try:
        conn.execute(f"""
            CREATE VIEW drug_pricing AS 
            SELECT * FROM read_parquet('{data_path}/drug_pricing.parquet')
        """)
        print(f"✅ Loaded drug_pricing for {year}")
    except Exception as e:
        print(f"⚠️  Note: drug_pricing not available for {year} (pricing comes in quarterly files)")
    
    # Load contract organizations mapping
    try:
        conn.execute(f"""
            CREATE VIEW contract_organizations AS 
            SELECT * FROM read_parquet('{data_path}/contract_organizations.parquet')
        """)
    except Exception as e:
        print(f"Note: contract_organizations.parquet not loaded: {e}")
    
    # Load plan enrollment data from CSV (use 2025 enrollment for both years until 2026 data available)
    try:
        if USE_S3:
            conn.execute(f"""
                CREATE VIEW plan_enrollment AS 
                SELECT 
                    "Contract Number" as contract_number,
                    "Plan ID" as plan_id,
                    "Parent Organization" as parent_organization,
                    "Organization Marketing Name" as organization_marketing_name,
                    "Plan Name" as plan_name,
                    TRY_CAST("Enrollment" AS INTEGER) as enrollment
                FROM read_csv_auto('s3://{S3_BUCKET}/2025/plan_enrollment.csv', ignore_errors=true)
            """)
            print(f"✅ Loaded 2025 enrollment data (using for {year} year)")
    except Exception as e:
        print(f"⚠️ Note: plan_enrollment not loaded - {e}")
    
    return conn

# No global connection - endpoints will create their own with year parameter

# ============================================================================
# WEB PAGES
# ============================================================================

@app.get("/")
async def home(request: Request):
    """Main page - contract selector"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/organization/{org_name:path}")
async def organization_detail(request: Request, org_name: str):
    """Organization detail page showing all plans"""
    return templates.TemplateResponse("organization.html", {
        "request": request,
        "org_name": org_name
    })

@app.get("/contract/{contract_name:path}")
async def contract_detail(request: Request, contract_name: str):
    """Contract detail page showing formularies"""
    return templates.TemplateResponse("contract.html", {
        "request": request,
        "contract_name": contract_name
    })

@app.get("/formulary/{formulary_id}")
async def formulary_detail(request: Request, formulary_id: str):
    """Formulary detail page"""
    return templates.TemplateResponse("formulary.html", {
        "request": request,
        "formulary_id": formulary_id
    })

@app.get("/formulary/{formulary_id}/tier/{tier}")
async def tier_detail(request: Request, formulary_id: str, tier: str):
    """Tier detail page with drug pricing"""
    return templates.TemplateResponse("tier_detail.html", {
        "request": request,
        "formulary_id": formulary_id,
        "tier": tier
    })

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/api/formularies")
async def get_formularies(org: Optional[str] = None, year: str = "2025"):
    """Get all formularies with parent organization info"""
    
    conn = get_db(year)
    query = """
        SELECT 
            formulary_id,
            COUNT(DISTINCT contract_name) as org_count,
            MAX(contract_name) as parent_org,
            COUNT(*) as plan_count,
            COUNT(DISTINCT state) as state_count
        FROM plans
        WHERE formulary_id IS NOT NULL
    """
    
    if org:
        query += f" AND contract_name LIKE '%{org}%'"
    
    query += """
        GROUP BY formulary_id
        ORDER BY plan_count DESC
    """
    
    result = conn.execute(query).fetchdf()
    return result.to_dict(orient='records')

@app.get("/api/organizations")
async def get_organizations(year: str = "2025"):
    """Get all parent organizations"""
    
    conn = get_db(year)
    result = conn.execute("""
        SELECT DISTINCT 
            contract_name as organization,
            COUNT(DISTINCT formulary_id) as formulary_count,
            COUNT(*) as plan_count
        FROM plans
        WHERE contract_name IS NOT NULL
        GROUP BY contract_name
        ORDER BY plan_count DESC
    """).fetchdf()
    
    return result.to_dict(orient='records')

@app.get("/api/contracts")
async def get_contracts(year: str = "2025"):
    """Get all contracts grouped by contract_id with parent org info"""
    
    conn = get_db(year)
    result = conn.execute("""
        SELECT 
            p.contract_id,
            COALESCE(co.parent_organization, MAX(p.contract_name)) as parent_organization,
            COALESCE(co.organization_marketing_name, MAX(p.contract_name)) as marketing_name,
            co.enrollment,
            COUNT(DISTINCT p.formulary_id) as formulary_count,
            COUNT(DISTINCT p.plan_id) as plan_count,
            COUNT(DISTINCT p.state) as state_count
        FROM plans p
        LEFT JOIN contract_organizations co ON p.contract_id = co.contract_number
        WHERE p.contract_id IS NOT NULL
        GROUP BY p.contract_id, co.parent_organization, co.organization_marketing_name, co.enrollment
        ORDER BY plan_count DESC
    """).fetchdf()
    
    return result.to_dict(orient='records')

@app.get("/api/plans")
async def get_all_plans(year: str = "2025"):
    """Get all plans grouped by parent organization, sorted by total enrollment - H contracts only with formulary data"""
    
    conn = get_db(year)
    result = conn.execute("""
        WITH h_contracts_with_formularies AS (
            SELECT DISTINCT
                pe.parent_organization,
                pe.organization_marketing_name,
                pe.contract_number,
                pe.plan_id,
                pe.enrollment,
                p.formulary_id
            FROM plan_enrollment pe
            INNER JOIN plans p ON pe.contract_number = p.contract_id AND pe.plan_id = p.plan_id
            WHERE pe.contract_number LIKE 'H%'
              AND p.formulary_id IS NOT NULL
              AND pe.enrollment IS NOT NULL
              AND pe.parent_organization IS NOT NULL
        ),
        org_stats AS (
            SELECT 
                parent_organization,
                MAX(organization_marketing_name) as organization_marketing_name,
                SUM(enrollment) as total_enrollment,
                COUNT(DISTINCT contract_number || '-' || plan_id) as plan_count,
                COUNT(DISTINCT contract_number) as contract_count,
                COUNT(DISTINCT formulary_id) as formulary_count
            FROM h_contracts_with_formularies
            GROUP BY parent_organization
        )
        SELECT 
            parent_organization,
            organization_marketing_name,
            total_enrollment,
            plan_count,
            contract_count,
            formulary_count
        FROM org_stats
        ORDER BY total_enrollment DESC
        LIMIT 100
    """).fetchdf()
    
    return result.to_dict(orient='records')

@app.get("/api/organization/{org_name:path}/plans")
async def get_organization_plans(org_name: str, year: str = "2025"):
    """Get all plans for a specific parent organization, sorted by enrollment"""
    
    conn = get_db(year)
    result = conn.execute(f"""
        SELECT 
            pe.contract_number,
            pe.plan_id,
            pe.contract_number || '-' || pe.plan_id as plan_full_id,
            pe.parent_organization,
            pe.organization_marketing_name,
            pe.plan_name,
            pe.enrollment,
            p.formulary_id,
            COUNT(DISTINCT p.state) as state_count
        FROM plan_enrollment pe
        LEFT JOIN plans p ON pe.contract_number = p.contract_id AND pe.plan_id = p.plan_id
        WHERE pe.parent_organization = '{org_name}'
          AND pe.contract_number LIKE 'H%'
          AND p.formulary_id IS NOT NULL
        GROUP BY 
            pe.contract_number,
            pe.plan_id,
            pe.parent_organization,
            pe.organization_marketing_name,
            pe.plan_name,
            pe.enrollment,
            p.formulary_id
        ORDER BY pe.enrollment DESC
    """).fetchdf()
    
    return result.to_dict(orient='records')

@app.get("/api/contract/{contract_id}/formularies")
async def get_contract_formularies(contract_id: str, year: str = "2025"):
    """Get all formularies for a specific contract"""
    
    conn = get_db(year)
    result = conn.execute(f"""
        SELECT 
            formulary_id,
            MAX(contract_name) as contract_name,
            COUNT(DISTINCT plan_id) as plan_count,
            COUNT(DISTINCT state) as state_count,
            COUNT(DISTINCT county_code) as county_count
        FROM plans
        WHERE contract_id = '{contract_id}'
        GROUP BY formulary_id
        ORDER BY plan_count DESC
    """).fetchdf()
    
    return result.to_dict(orient='records')

@app.get("/api/formulary/{formulary_id}/summary")
async def get_formulary_summary(formulary_id: str, year: str = "2025"):
    """Get comprehensive summary for a formulary"""
    
    conn = get_db(year)
    
    # Parent organization - get clean name from plan_enrollment
    parent = conn.execute(f"""
        SELECT 
            p.contract_id,
            COALESCE(pe.parent_organization, p.contract_name) as parent_org,
            COUNT(DISTINCT p.contract_name) as entity_count
        FROM plans p
        LEFT JOIN plan_enrollment pe ON p.contract_id = pe.contract_number AND p.plan_id = pe.plan_id
        WHERE p.formulary_id = '{formulary_id}'
        GROUP BY p.contract_id, pe.parent_organization, p.contract_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """).fetchdf()
    
    # Drug coverage counts by tier
    drug_counts = conn.execute(f"""
        SELECT 
            tier_level_value as tier,
            COUNT(DISTINCT rxcui) as drug_count
        FROM formulary_drugs
        WHERE formulary_id = '{formulary_id}'
        GROUP BY tier_level_value
        ORDER BY tier_level_value
    """).fetchdf()
    
    # Restrictions
    restrictions = conn.execute(f"""
        SELECT 
            SUM(CASE WHEN prior_authorization_yn = 'Y' THEN 1 ELSE 0 END) as prior_auth_count,
            SUM(CASE WHEN step_therapy_yn = 'Y' THEN 1 ELSE 0 END) as step_therapy_count,
            SUM(CASE WHEN quantity_limit_yn = 'Y' THEN 1 ELSE 0 END) as quantity_limit_count,
            COUNT(DISTINCT rxcui) as total_drugs
        FROM formulary_drugs
        WHERE formulary_id = '{formulary_id}'
    """).fetchdf()
    
    # Geographic footprint
    geography = conn.execute(f"""
        SELECT 
            COUNT(DISTINCT plan_id) as plan_count,
            COUNT(DISTINCT state) as state_count,
            COUNT(DISTINCT county_code) as county_count
        FROM plans
        WHERE formulary_id = '{formulary_id}'
    """).fetchdf()
    
    # Cost structure (sample from first plan)
    cost_structure = conn.execute(f"""
        SELECT 
            bc.tier,
            bc.cost_type_pref as retail_cost_type,
            bc.cost_amt_pref as retail_cost_amt,
            bc.cost_type_mail_pref as mail_cost_type,
            bc.cost_amt_mail_pref as mail_cost_amt,
            bc.tier_specialty_yn
        FROM beneficiary_costs bc
        JOIN plans p ON bc.plan_key = p.plan_key
        WHERE p.formulary_id = '{formulary_id}'
          AND bc.coverage_level = 'I'
        LIMIT 10
    """).fetchdf()
    
    # Specialty drug summary
    specialty = conn.execute(f"""
        SELECT 
            COUNT(DISTINCT rxcui) as specialty_drug_count,
            SUM(CASE WHEN prior_authorization_yn = 'Y' THEN 1 ELSE 0 END) as specialty_pa_count
        FROM formulary_drugs
        WHERE formulary_id = '{formulary_id}'
          AND (tier_level_value = '5' OR tier_level_value = '6')
    """).fetchdf()
    
    return {
        "formulary_id": formulary_id,
        "parent_org": parent.to_dict(orient='records')[0] if len(parent) > 0 else {},
        "drug_counts": drug_counts.to_dict(orient='records'),
        "restrictions": restrictions.to_dict(orient='records')[0],
        "geography": geography.to_dict(orient='records')[0],
        "cost_structure": cost_structure.to_dict(orient='records'),
        "specialty": specialty.to_dict(orient='records')[0]
    }

@app.get("/api/formulary/{formulary_id}/drugs")
async def get_formulary_drugs(
    formulary_id: str, 
    tier: Optional[str] = None,
    specialty_only: bool = False,
    year: str = "2025"
):
    """Get drugs in a formulary"""
    
    conn = get_db(year)
    query = f"""
        SELECT 
            rxcui,
            ndc,
            tier_level_value as tier,
            prior_authorization_yn,
            step_therapy_yn,
            quantity_limit_yn
        FROM formulary_drugs
        WHERE formulary_id = '{formulary_id}'
    """
    
    if tier:
        query += f" AND tier_level_value = '{tier}'"
    
    if specialty_only:
        query += " AND (tier_level_value = '5' OR tier_level_value = '6')"
    
    query += " ORDER BY tier_level_value, rxcui LIMIT 1000"
    
    result = conn.execute(query).fetchdf()
    return result.to_dict(orient='records')

@app.get("/api/formulary/{formulary_id}/states")
async def get_formulary_states(formulary_id: str, year: str = "2025"):
    """Get state breakdown for a formulary"""
    
    conn = get_db(year)
    result = conn.execute(f"""
        SELECT 
            state,
            COUNT(DISTINCT plan_id) as plan_count,
            AVG(CAST(premium AS FLOAT)) as avg_premium,
            AVG(CAST(deductible AS FLOAT)) as avg_deductible
        FROM plans
        WHERE formulary_id = '{formulary_id}'
        GROUP BY state
        ORDER BY plan_count DESC
    """).fetchdf()
    
    return result.to_dict(orient='records')

@app.get("/api/formulary/{formulary_id}/tier/{tier}/drugs")
async def get_tier_drugs_with_pricing(formulary_id: str, tier: str, year: str = "2025"):
    """Get all drugs in a specific tier with full pricing details"""
    
    conn = get_db(year)
    result = conn.execute(f"""
        WITH tier_drugs AS (
            SELECT DISTINCT
                fd.rxcui,
                fd.ndc,
                fd.tier_level_value,
                fd.prior_authorization_yn,
                fd.step_therapy_yn,
                fd.quantity_limit_yn
            FROM formulary_drugs fd
            WHERE fd.formulary_id = '{formulary_id}'
              AND fd.tier_level_value = '{tier}'
        ),
        pricing AS (
            SELECT 
                dp.ndc,
                AVG(dp.unit_cost) as avg_negotiated_cost
            FROM drug_pricing dp
            JOIN plans p ON dp.plan_key = p.plan_key
            WHERE p.formulary_id = '{formulary_id}'
            GROUP BY dp.ndc
        ),
        costs AS (
            SELECT 
                bc.tier,
                bc.cost_type_pref as cost_type,
                bc.cost_amt_pref as cost_amt,
                bc.tier_specialty_yn
            FROM beneficiary_costs bc
            JOIN plans p ON bc.plan_key = p.plan_key
            WHERE p.formulary_id = '{formulary_id}'
              AND bc.coverage_level = '1'
              AND bc.tier = '{tier}'
            ORDER BY CAST(bc.cost_amt_pref AS DOUBLE) DESC
            LIMIT 1
        )
        SELECT 
            td.rxcui,
            td.ndc,
            td.tier_level_value as tier,
            td.prior_authorization_yn as prior_auth,
            td.step_therapy_yn as step_therapy,
            td.quantity_limit_yn as quantity_limit,
            COALESCE(pr.avg_negotiated_cost, 0.0) as negotiated_cost,
            CASE 
                WHEN c.cost_type = '0' AND CAST(c.cost_amt AS DOUBLE) > 0 THEN 'COPAY'
                WHEN c.cost_type = '0' AND CAST(c.cost_amt AS DOUBLE) = 0 THEN 'COPAY'
                WHEN c.cost_type IN ('1', '2') AND CAST(c.cost_amt AS DOUBLE) > 0 THEN 'COINSURANCE' 
                ELSE 'COPAY' 
            END as cost_type,
            CASE
                WHEN c.cost_type IN ('1', '2') AND CAST(c.cost_amt AS DOUBLE) < 1 THEN CAST(c.cost_amt AS DOUBLE) * 100
                ELSE CAST(c.cost_amt AS DOUBLE)
            END as cost_amt,
            c.tier_specialty_yn as is_specialty,
            CASE 
                WHEN c.cost_type = '0' THEN CAST(c.cost_amt AS DOUBLE)
                WHEN c.cost_type IN ('1', '2') AND CAST(c.cost_amt AS DOUBLE) >= 1 THEN pr.avg_negotiated_cost * (CAST(c.cost_amt AS DOUBLE) / 100.0)
                WHEN c.cost_type IN ('1', '2') AND CAST(c.cost_amt AS DOUBLE) < 1 THEN pr.avg_negotiated_cost * CAST(c.cost_amt AS DOUBLE)
                ELSE 0.0
            END as member_pays,
            COALESCE(pr.avg_negotiated_cost, 0.0) - CASE 
                WHEN c.cost_type = '0' THEN CAST(c.cost_amt AS DOUBLE)
                WHEN c.cost_type IN ('1', '2') AND CAST(c.cost_amt AS DOUBLE) >= 1 THEN pr.avg_negotiated_cost * (CAST(c.cost_amt AS DOUBLE) / 100.0)
                WHEN c.cost_type IN ('1', '2') AND CAST(c.cost_amt AS DOUBLE) < 1 THEN pr.avg_negotiated_cost * CAST(c.cost_amt AS DOUBLE)
                ELSE 0.0
            END as plan_net_cost
        FROM tier_drugs td
        LEFT JOIN pricing pr ON td.ndc = pr.ndc
        CROSS JOIN costs c
        ORDER BY negotiated_cost DESC
        LIMIT 500
    """).fetchdf()
    
    return result.to_dict(orient='records')

@app.get("/api/stats")
async def get_global_stats(year: str = "2025"):
    """Get global statistics"""
    
    conn = get_db(year)
    stats = {
        "total_formularies": conn.execute("SELECT COUNT(DISTINCT formulary_id) FROM plans").fetchone()[0],
        "total_plans": conn.execute("SELECT COUNT(DISTINCT plan_id) FROM plans").fetchone()[0],
        "total_drugs": conn.execute("SELECT COUNT(DISTINCT rxcui) FROM formulary_drugs").fetchone()[0],
        "total_organizations": conn.execute("SELECT COUNT(DISTINCT contract_name) FROM plans").fetchone()[0],
        "year": year
    }
    
    return stats

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "medicare-part-d-intelligence"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

