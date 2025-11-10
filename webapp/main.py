from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from pathlib import Path
from typing import Optional
import duckdb
import pandas as pd
import os

app = FastAPI(title="Medicare Part D Intelligence Platform")

# Get the base directory (works both locally and on Railway)
BASE_DIR = Path(__file__).parent.parent
WEBAPP_DIR = Path(__file__).parent

# Mount static files and templates with dynamic paths
static_dir = WEBAPP_DIR / "static" if (WEBAPP_DIR / "static").exists() else BASE_DIR / "webapp" / "static"
templates_dir = WEBAPP_DIR / "templates" if (WEBAPP_DIR / "templates").exists() else BASE_DIR / "webapp" / "templates"

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
templates = Jinja2Templates(directory=str(templates_dir))

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "formulary2026")
S3_PREFIX = os.getenv("S3_PREFIX", "medicare_parquet")

# Auto-detect: Use S3 if AWS credentials are available, otherwise use local
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
USE_S3_ENV = os.getenv("USE_S3", "").lower()

# Determine if we should use S3
# If explicitly set to "true", require credentials (production mode)
# If explicitly set to "false", use local
# If not set, auto-detect based on credentials
if USE_S3_ENV == "false":
    USE_S3 = False
    USE_S3_EXPLICIT = False
elif USE_S3_ENV == "true":
    USE_S3 = True
    USE_S3_EXPLICIT = True  # Explicitly requested - require credentials
else:
    # Auto-detect: Use S3 if credentials available
    USE_S3 = bool(AWS_KEY and AWS_SECRET)
    USE_S3_EXPLICIT = False

# Local fallback (BASE_DIR already defined above)
LOCAL_DATA_DIR = BASE_DIR / "medicare_parquet"

def get_db(year: str = "2025"):
    """
    Get database connection with data loaded for specified year.
    Args:
        year: Year to load data for ("2025" or "2026")
    """
    conn = duckdb.connect(':memory:')
    
    # Determine data source (check AWS credentials at runtime)
    use_s3 = USE_S3
    
    # If S3 was explicitly requested (production mode), require credentials
    if USE_S3_EXPLICIT and (not AWS_KEY or not AWS_SECRET):
        raise ValueError(
            "USE_S3=true but AWS credentials not found!\n"
            "Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.\n"
            "Or set USE_S3=false to use local files."
        )
    
    # If auto-detected S3 but no credentials, fall back to local
    if use_s3 and (not AWS_KEY or not AWS_SECRET):
        print("⚠️  S3 auto-detected but AWS credentials not found, falling back to local")
        use_s3 = False
    
    # Install and load httpfs extension for S3 access
    if use_s3:
        try:
            conn.execute("INSTALL httpfs;")
            conn.execute("LOAD httpfs;")
            
            # Configure S3 credentials
            conn.execute(f"SET s3_region='us-east-1';")
            conn.execute(f"SET s3_access_key_id='{AWS_KEY}';")
            conn.execute(f"SET s3_secret_access_key='{AWS_SECRET}';")
            
            # Use S3 paths with year
            data_path = f"s3://{S3_BUCKET}/{year}"
            print(f"📦 Loading data from S3: {data_path}")
        except Exception as e:
            print(f"⚠️  S3 connection failed: {e}")
            print(f"⚠️  Falling back to local files...")
            use_s3 = False
            data_path = str(LOCAL_DATA_DIR)
    else:
        # Use local paths
        data_path = str(LOCAL_DATA_DIR)
        print(f"📁 Loading data from local: {data_path}")
    
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
    
    # Load plan enrollment data from Parquet (use 2025 enrollment for both years until 2026 data available)
    try:
        if use_s3:
            enrollment_path = f"s3://{S3_BUCKET}/2025/plan_enrollment.parquet"
        else:
            enrollment_path = f"{LOCAL_DATA_DIR}/plan_enrollment.parquet"
        
        conn.execute(f"""
            CREATE VIEW plan_enrollment AS 
            SELECT * FROM read_parquet('{enrollment_path}')
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

@app.get("/glp1")
async def glp1_analysis(request: Request):
    """GLP-1 Analysis page"""
    return templates.TemplateResponse("glp1.html", {
        "request": request
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
    
    # Replace NaN with None for JSON serialization
    result = result.fillna({'total_enrollment': 0, 'plan_count': 0, 'contract_count': 0, 'formulary_count': 0})
    result = result.where(result.notna(), None)
    
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
    
    # Replace NaN with None for JSON serialization
    result = result.fillna({'enrollment': 0, 'state_count': 0})
    result = result.where(result.notna(), None)
    
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
                bc.tier_specialty_yn,
                bc.pharmacy_type_pref as pharmacy_type
            FROM beneficiary_costs bc
            JOIN plans p ON bc.plan_key = p.plan_key
            WHERE p.formulary_id = '{formulary_id}'
              AND bc.coverage_level = '1'
              AND bc.tier = '{tier}'
              AND bc.pharmacy_type_pref = '1'
            ORDER BY CAST(bc.cost_amt_pref AS DOUBLE) ASC
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
# GLP-1 ANALYSIS ENDPOINTS
# ============================================================================

# GLP-1 Drug RXCUI mappings
# GLP-1 drugs with RXCUI codes verified from 2025 Q2 SPUF data using NDC patterns
GLP1_DRUGS = {
    'Ozempic': '2398842',      # Semaglutide injection - Found via NDC 00169-413x (370 formularies)
    'Wegovy': '2553603',       # Semaglutide injection weight loss - Found via NDC 00169-450x (26 formularies)
    'Rybelsus': '2619154',     # Semaglutide oral tablet - Found via NDC 00169-418x (370 formularies)
    'Mounjaro': '2601776',     # Tirzepatide injection - Found via NDC 00002-146x (373 formularies)
    'Trulicity': '1551306',    # Dulaglutide injection - Found via NDC 00002-143x (376 formularies)
    'Victoza': '897126'        # Liraglutide injection - Found via NDC 00169-406x (26 formularies)
}

# Target companies (case-insensitive matching)
TARGET_COMPANIES = [
    'Elevance',
    'UnitedHealth',
    'Humana',
    'CVS',
    'Molina',
    'Centene',
    'Alignment'
]

@app.get("/api/glp1/master-table")
async def get_glp1_master_table(year: str = "2025"):
    """Get comprehensive GLP-1 coverage analysis for target companies - pivoted with companies as columns"""
    
    try:
        conn = get_db(year)
        
        # Build parent organization filter (case-insensitive)
        org_filter = " OR ".join([
            f"UPPER(COALESCE(pe.parent_organization, p.contract_name)) LIKE '%{co.upper()}%'" 
            for co in TARGET_COMPANIES
        ])
        
        # Get parent organization mapping and normalize to 7 target companies
        org_mapping_df = conn.execute(f"""
            SELECT DISTINCT
                COALESCE(pe.parent_organization, p.contract_name) as parent_org,
                p.contract_id,
                p.contract_name
            FROM plans p
            LEFT JOIN plan_enrollment pe ON p.contract_id = pe.contract_number AND p.plan_id = pe.plan_id
            WHERE ({org_filter})
        """).fetchdf()
        
        # Map each contract to one of the 7 target companies
        def normalize_org(org_name):
            if not org_name:
                return None
            org_upper = org_name.upper()
            for target in TARGET_COMPANIES:
                if target.upper() in org_upper:
                    return target
            return None
        
        org_mapping_df['normalized_org'] = org_mapping_df['parent_org'].apply(normalize_org)
        org_mapping = dict(zip(org_mapping_df['contract_id'], org_mapping_df['normalized_org']))
        
        # Build contract_id filter for normalized companies
        valid_contracts = org_mapping_df[org_mapping_df['normalized_org'].notna()]['contract_id'].unique().tolist()
        if valid_contracts:
            contract_list = ','.join([f"'{c}'" for c in valid_contracts])
            contract_filter = f"p.contract_id IN ({contract_list})"
        else:
            contract_filter = "1=0"
        
        results = []
        
        for drug_name, rxcui in GLP1_DRUGS.items():
            # Get all NDCs for this drug (each NDC = different dosage/strength)
            ndcs_query = f"""
                SELECT DISTINCT ndc
                FROM formulary_drugs
                WHERE CAST(rxcui AS VARCHAR) = '{rxcui}'
                  AND ndc IS NOT NULL
                ORDER BY ndc
            """
            ndcs_df = conn.execute(ndcs_query).fetchdf()
            
            print(f"🔍 {drug_name} (RXCUI {rxcui}): Found {len(ndcs_df)} NDCs")
            if not ndcs_df.empty:
                print(f"   NDCs: {', '.join(ndcs_df['ndc'].tolist()[:10])}")  # Show first 10
            
            if ndcs_df.empty:
                continue
            
            # Process each NDC separately
            for _, ndc_row in ndcs_df.iterrows():
                ndc = ndc_row['ndc']
                
                # Get stats grouped by normalized parent organization for this specific NDC
                query = f"""
                WITH company_plans AS (
                    SELECT DISTINCT
                        p.contract_id,
                        p.formulary_id,
                        p.plan_id,
                        COALESCE(pe.parent_organization, p.contract_name) as parent_org
                    FROM plans p
                    LEFT JOIN plan_enrollment pe ON p.contract_id = pe.contract_number AND p.plan_id = pe.plan_id
                    WHERE {contract_filter}
                ),
                normalized_plans AS (
                    SELECT 
                        cp.*,
                        CASE 
                            WHEN UPPER(cp.parent_org) LIKE '%ELEVANCE%' THEN 'Elevance'
                            WHEN UPPER(cp.parent_org) LIKE '%UNITEDHEALTH%' OR UPPER(cp.parent_org) LIKE '%UNITED HEALTH%' THEN 'UnitedHealth'
                            WHEN UPPER(cp.parent_org) LIKE '%HUMANA%' THEN 'Humana'
                            WHEN UPPER(cp.parent_org) LIKE '%CVS%' OR UPPER(cp.parent_org) LIKE '%AETNA%' THEN 'CVS'
                            WHEN UPPER(cp.parent_org) LIKE '%MOLINA%' THEN 'Molina'
                            WHEN UPPER(cp.parent_org) LIKE '%CENTENE%' THEN 'Centene'
                            WHEN UPPER(cp.parent_org) LIKE '%ALIGNMENT%' THEN 'Alignment'
                            ELSE NULL
                        END as normalized_org
                    FROM company_plans cp
                    WHERE cp.parent_org IS NOT NULL
                ),
                company_totals AS (
                    SELECT 
                        normalized_org,
                        COUNT(DISTINCT formulary_id) as total_formularies,
                        COUNT(DISTINCT plan_id) as total_plans
                    FROM normalized_plans
                    WHERE normalized_org IS NOT NULL
                    GROUP BY normalized_org
                ),
                drug_coverage AS (
                    SELECT DISTINCT
                        np.normalized_org,
                        np.formulary_id,
                        np.plan_id,
                        CAST(fd.tier_level_value AS VARCHAR) as tier_level_value,
                        fd.prior_authorization_yn,
                        fd.step_therapy_yn,
                        fd.quantity_limit_yn
                    FROM normalized_plans np
                    JOIN formulary_drugs fd ON np.formulary_id = fd.formulary_id
                    WHERE CAST(fd.rxcui AS VARCHAR) = '{rxcui}'
                      AND fd.ndc = '{ndc}'
                      AND np.normalized_org IS NOT NULL
                ),
            drug_stats AS (
                SELECT 
                    dc.normalized_org,
                    COUNT(DISTINCT dc.formulary_id) as formularies_with_drug,
                    COUNT(DISTINCT dc.plan_id) as plans_with_drug,
                    COUNT(DISTINCT CASE WHEN CAST(dc.tier_level_value AS INTEGER) = 3 THEN dc.plan_id END) as plans_tier3,
                    COUNT(DISTINCT CASE WHEN CAST(dc.tier_level_value AS INTEGER) = 4 THEN dc.plan_id END) as plans_tier4,
                    COUNT(DISTINCT CASE WHEN CAST(dc.tier_level_value AS INTEGER) = 5 THEN dc.plan_id END) as plans_tier5,
                    COUNT(DISTINCT CASE WHEN CAST(dc.tier_level_value AS INTEGER) = 6 THEN dc.plan_id END) as plans_tier6,
                    COUNT(DISTINCT CASE WHEN UPPER(CAST(dc.prior_authorization_yn AS VARCHAR)) = 'Y' THEN dc.plan_id END) as plans_with_pa,
                    COUNT(DISTINCT CASE WHEN UPPER(CAST(dc.step_therapy_yn AS VARCHAR)) = 'Y' THEN dc.plan_id END) as plans_with_st,
                    COUNT(DISTINCT CASE WHEN UPPER(CAST(dc.quantity_limit_yn AS VARCHAR)) = 'Y' THEN dc.plan_id END) as plans_with_ql
                FROM drug_coverage dc
                GROUP BY dc.normalized_org
            )
            SELECT 
                ct.normalized_org as company,
                ct.total_formularies,
                ct.total_plans,
                COALESCE(ds.formularies_with_drug, 0) as formularies_with_drug,
                COALESCE(ds.plans_with_drug, 0) as plans_with_drug,
                ROUND(COALESCE(ds.formularies_with_drug, 0) * 100.0 / NULLIF(ct.total_formularies, 0), 1) as formulary_pct,
                ROUND(COALESCE(ds.plans_with_drug, 0) * 100.0 / NULLIF(ct.total_plans, 0), 1) as plan_pct,
                COALESCE(ds.plans_tier3, 0) as tier3_count,
                COALESCE(ds.plans_tier4, 0) as tier4_count,
                COALESCE(ds.plans_tier5, 0) as tier5_count,
                COALESCE(ds.plans_tier6, 0) as tier6_count,
                ROUND(COALESCE(ds.plans_tier3, 0) * 100.0 / NULLIF(ds.plans_with_drug, 0), 1) as tier3_pct,
                ROUND(COALESCE(ds.plans_tier4, 0) * 100.0 / NULLIF(ds.plans_with_drug, 0), 1) as tier4_pct,
                ROUND(COALESCE(ds.plans_tier5, 0) * 100.0 / NULLIF(ds.plans_with_drug, 0), 1) as tier5_pct,
                ROUND(COALESCE(ds.plans_tier6, 0) * 100.0 / NULLIF(ds.plans_with_drug, 0), 1) as tier6_pct,
                COALESCE(ds.plans_with_pa, 0) as pa_count,
                COALESCE(ds.plans_with_st, 0) as st_count,
                COALESCE(ds.plans_with_ql, 0) as ql_count,
                ROUND(COALESCE(ds.plans_with_pa, 0) * 100.0 / NULLIF(ds.plans_with_drug, 0), 1) as pa_pct,
                ROUND(COALESCE(ds.plans_with_st, 0) * 100.0 / NULLIF(ds.plans_with_drug, 0), 1) as st_pct,
                ROUND(COALESCE(ds.plans_with_ql, 0) * 100.0 / NULLIF(ds.plans_with_drug, 0), 1) as ql_pct
            FROM company_totals ct
            LEFT JOIN drug_stats ds ON ct.normalized_org = ds.normalized_org
            WHERE ct.normalized_org IS NOT NULL
            ORDER BY ct.normalized_org
            """
            
            try:
                result = conn.execute(query).fetchdf()
                
                # Check if result is empty - create empty rows for all 7 companies
                if result.empty:
                    print(f"⚠️  No data found for {drug_name} (RXCUI {rxcui})")
                    # Create empty result for all target companies
                    empty_data = []
                    for company in TARGET_COMPANIES:
                        empty_data.append({
                            'company': company,
                            'total_formularies': 0,
                            'total_plans': 0,
                            'formularies_with_drug': 0,
                            'plans_with_drug': 0,
                            'formulary_pct': 0.0,
                            'plan_pct': 0.0,
                            'tier3_count': 0,
                            'tier4_count': 0,
                            'tier5_count': 0,
                            'tier6_count': 0,
                            'tier3_pct': 0.0,
                            'tier4_pct': 0.0,
                            'tier5_pct': 0.0,
                            'tier6_pct': 0.0,
                            'pa_count': 0,
                            'st_count': 0,
                            'ql_count': 0,
                            'pa_pct': 0.0,
                            'st_pct': 0.0,
                            'ql_pct': 0.0
                        })
                    result = pd.DataFrame(empty_data)
                
                # Add drug name and NDC to each row
                result['drug_name'] = drug_name
                result['ndc'] = ndc
                result['rxcui'] = rxcui
                
                results.append(result)
            except Exception as query_error:
                print(f"❌ Query error for {drug_name} (RXCUI {rxcui}): {query_error}")
                import traceback
                traceback.print_exc()
                # Continue with next drug instead of failing completely
                continue
        
        # Combine all drugs
        if not results:
            # No results at all, return empty list
            return []
        
        combined = pd.concat(results, ignore_index=True)
    
        # Replace NaN with 0 for counts, None for strings
        combined = combined.fillna({
            'formularies_with_drug': 0,
            'plans_with_drug': 0,
            'formulary_pct': 0,
            'plan_pct': 0,
            'tier3_count': 0,
            'tier4_count': 0,
            'tier5_count': 0,
            'tier6_count': 0,
            'tier3_pct': 0,
            'tier4_pct': 0,
            'tier5_pct': 0,
            'tier6_pct': 0,
            'pa_count': 0,
            'st_count': 0,
            'ql_count': 0,
            'pa_pct': 0,
            'st_pct': 0,
            'ql_pct': 0
        })
        
        # Pivot: Transform to have one row per drug+NDC combination, with companies as nested objects
        pivoted_data = []
        # Group by drug_name and ndc
        for (drug_name, ndc), drug_data in combined.groupby(['drug_name', 'ndc']):
            drug_data = drug_data.copy()
            
            if drug_data.empty:
                # Create empty entry for this drug+NDC
                drug_row = {
                    'drug_name': drug_name,
                    'ndc': ndc,
                    'rxcui': GLP1_DRUGS.get(drug_name, ''),
                    'companies': {}
                }
                for company in TARGET_COMPANIES:
                    drug_row['companies'][company] = {
                        'total_formularies': 0,
                        'total_plans': 0,
                        'formularies_with_drug': 0,
                        'plans_with_drug': 0,
                        'formulary_pct': 0.0,
                        'plan_pct': 0.0,
                        'tier3_count': 0,
                        'tier4_count': 0,
                        'tier5_count': 0,
                        'tier6_count': 0,
                        'tier3_pct': 0.0,
                        'tier4_pct': 0.0,
                        'tier5_pct': 0.0,
                        'tier6_pct': 0.0,
                        'pa_count': 0,
                        'st_count': 0,
                        'ql_count': 0,
                        'pa_pct': 0.0,
                        'st_pct': 0.0,
                        'ql_pct': 0.0
                    }
            else:
                # Get first row for drug info
                first_row = drug_data.iloc[0]
                drug_row = {
                    'drug_name': drug_name,
                    'ndc': ndc,
                    'rxcui': first_row['rxcui'],
                    'companies': {}
                }
                
                # Add data for each company
                for _, row in drug_data.iterrows():
                    company = row['company']
                    drug_row['companies'][company] = {
                        'total_formularies': int(row['total_formularies']),
                        'total_plans': int(row['total_plans']),
                        'formularies_with_drug': int(row['formularies_with_drug']),
                        'plans_with_drug': int(row['plans_with_drug']),
                        'formulary_pct': float(row['formulary_pct']),
                        'plan_pct': float(row['plan_pct']),
                        'tier3_count': int(row['tier3_count']),
                        'tier4_count': int(row['tier4_count']),
                        'tier5_count': int(row['tier5_count']),
                        'tier6_count': int(row['tier6_count']),
                        'tier3_pct': float(row['tier3_pct']),
                        'tier4_pct': float(row['tier4_pct']),
                        'tier5_pct': float(row['tier5_pct']),
                        'tier6_pct': float(row['tier6_pct']),
                        'pa_count': int(row['pa_count']),
                        'st_count': int(row['st_count']),
                        'ql_count': int(row['ql_count']),
                        'pa_pct': float(row['pa_pct']),
                        'st_pct': float(row['st_pct']),
                        'ql_pct': float(row['ql_pct'])
                    }
                
                # Ensure all 7 companies are present (fill missing with zeros)
                for company in TARGET_COMPANIES:
                    if company not in drug_row['companies']:
                        drug_row['companies'][company] = {
                            'total_formularies': 0,
                            'total_plans': 0,
                            'formularies_with_drug': 0,
                            'plans_with_drug': 0,
                            'formulary_pct': 0.0,
                            'plan_pct': 0.0,
                            'tier3_count': 0,
                            'tier4_count': 0,
                            'tier5_count': 0,
                            'tier6_count': 0,
                            'tier3_pct': 0.0,
                            'tier4_pct': 0.0,
                            'tier5_pct': 0.0,
                            'tier6_pct': 0.0,
                            'pa_count': 0,
                            'st_count': 0,
                            'ql_count': 0,
                            'pa_pct': 0.0,
                            'st_pct': 0.0,
                            'ql_pct': 0.0
                        }
            
            pivoted_data.append(drug_row)
        
        return pivoted_data
    except Exception as e:
        import traceback
        error_msg = f"Error in get_glp1_master_table: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        # Return empty list instead of error response so frontend doesn't break
        # Log error to console for debugging
        import logging
        logging.error(error_msg)
        return []

@app.get("/api/glp1/pricing")
async def get_glp1_pricing(year: str = "2025"):
    """Get negotiated rates (plan's wholesale cost) for GLP-1 drugs by company"""
    
    try:
        conn = get_db(year)
        
        # Get parent organization mapping
        org_filter = " OR ".join([
            f"UPPER(COALESCE(pe.parent_organization, p.contract_name)) LIKE '%{co.upper()}%'" 
            for co in TARGET_COMPANIES
        ])
        
        org_mapping_df = conn.execute(f"""
            SELECT DISTINCT
                COALESCE(pe.parent_organization, p.contract_name) as parent_org,
                p.contract_id
            FROM plans p
            LEFT JOIN plan_enrollment pe ON p.contract_id = pe.contract_number AND p.plan_id = pe.plan_id
            WHERE ({org_filter})
        """).fetchdf()
        
        def normalize_org(org_name):
            if not org_name:
                return None
            org_upper = org_name.upper()
            for target in TARGET_COMPANIES:
                if target.upper() in org_upper:
                    return target
            return None
        
        org_mapping_df['normalized_org'] = org_mapping_df['parent_org'].apply(normalize_org)
        valid_contracts = org_mapping_df[org_mapping_df['normalized_org'].notna()]['contract_id'].unique().tolist()
        
        if not valid_contracts:
            return []
        
        contract_list = ','.join([f"'{c}'" for c in valid_contracts])
        
        results = []
        
        for drug_name, rxcui in GLP1_DRUGS.items():
            # Get all NDCs for this drug (each NDC = different dosage/strength)
            ndcs_query = f"""
                SELECT DISTINCT ndc
                FROM formulary_drugs
                WHERE CAST(rxcui AS VARCHAR) = '{rxcui}'
                  AND ndc IS NOT NULL
                ORDER BY ndc
            """
            ndcs_df = conn.execute(ndcs_query).fetchdf()
            
            if ndcs_df.empty:
                continue
            
            # Process each NDC separately
            for _, ndc_row in ndcs_df.iterrows():
                ndc = ndc_row['ndc']
                
                # Get pricing by normalized company for this specific NDC
                query = f"""
            WITH company_plans AS (
                SELECT DISTINCT
                    p.contract_id,
                    p.plan_key,
                    COALESCE(pe.parent_organization, p.contract_name) as parent_org
                FROM plans p
                LEFT JOIN plan_enrollment pe ON p.contract_id = pe.contract_number AND p.plan_id = pe.plan_id
                WHERE p.contract_id IN ({contract_list})
            ),
            normalized_plans AS (
                SELECT 
                    cp.*,
                    CASE 
                        WHEN UPPER(cp.parent_org) LIKE '%ELEVANCE%' THEN 'Elevance'
                        WHEN UPPER(cp.parent_org) LIKE '%UNITEDHEALTH%' OR UPPER(cp.parent_org) LIKE '%UNITED HEALTH%' THEN 'UnitedHealth'
                        WHEN UPPER(cp.parent_org) LIKE '%HUMANA%' THEN 'Humana'
                        WHEN UPPER(cp.parent_org) LIKE '%CVS%' OR UPPER(cp.parent_org) LIKE '%AETNA%' THEN 'CVS'
                        WHEN UPPER(cp.parent_org) LIKE '%MOLINA%' THEN 'Molina'
                        WHEN UPPER(cp.parent_org) LIKE '%CENTENE%' THEN 'Centene'
                        WHEN UPPER(cp.parent_org) LIKE '%ALIGNMENT%' THEN 'Alignment'
                        ELSE NULL
                    END as normalized_org
                FROM company_plans cp
                WHERE cp.parent_org IS NOT NULL
            ),
            pricing_data AS (
                SELECT 
                    np.normalized_org,
                    dp.unit_cost,
                    dp.days_supply
                FROM normalized_plans np
                JOIN drug_pricing dp ON np.plan_key = dp.plan_key
                WHERE dp.ndc = '{ndc}'
                  AND np.normalized_org IS NOT NULL
            )
            SELECT 
                normalized_org as company,
                days_supply,
                COUNT(*) as plan_count,
                AVG(CAST(unit_cost AS DOUBLE)) as avg_unit_cost,
                MIN(CAST(unit_cost AS DOUBLE)) as min_unit_cost,
                MAX(CAST(unit_cost AS DOUBLE)) as max_unit_cost
            FROM pricing_data
            WHERE normalized_org IS NOT NULL
            GROUP BY normalized_org, days_supply
            ORDER BY normalized_org, days_supply
            """
            
            try:
                result = conn.execute(query).fetchdf()
                
                # Pivot by company (create row even if empty)
                drug_row = {
                    'drug_name': drug_name,
                    'ndc': ndc,
                    'rxcui': rxcui,
                    'companies': {}
                }
                
                if result.empty:
                    # Create empty entries for all companies
                    for company in TARGET_COMPANIES:
                        drug_row['companies'][company] = {
                            'avg_unit_cost': 0.0,
                            'min_unit_cost': 0.0,
                            'max_unit_cost': 0.0,
                            'plan_count': 0
                        }
                    results.append(drug_row)
                    continue
                
                for company in TARGET_COMPANIES:
                    company_data = result[result['company'] == company]
                    if not company_data.empty:
                        # Get 30-day supply pricing (most common)
                        day30 = company_data[company_data['days_supply'] == 30]
                        if not day30.empty:
                            drug_row['companies'][company] = {
                                'avg_unit_cost': float(day30.iloc[0]['avg_unit_cost']),
                                'min_unit_cost': float(day30.iloc[0]['min_unit_cost']),
                                'max_unit_cost': float(day30.iloc[0]['max_unit_cost']),
                                'plan_count': int(day30.iloc[0]['plan_count'])
                            }
                        else:
                            drug_row['companies'][company] = {
                                'avg_unit_cost': 0.0,
                                'min_unit_cost': 0.0,
                                'max_unit_cost': 0.0,
                                'plan_count': 0
                            }
                    else:
                        drug_row['companies'][company] = {
                            'avg_unit_cost': 0.0,
                            'min_unit_cost': 0.0,
                            'max_unit_cost': 0.0,
                            'plan_count': 0
                        }
                
                results.append(drug_row)
            except Exception as e:
                print(f"Error getting pricing for {drug_name}: {e}")
                continue
        
        return results
    except Exception as e:
        import traceback
        error_msg = f"Error in get_glp1_pricing: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return []

@app.get("/api/glp1/member-costs")
async def get_glp1_member_costs(year: str = "2025"):
    """Get member cost-sharing (copay/coinsurance) for GLP-1 drugs by company"""
    
    try:
        conn = get_db(year)
        
        # Get parent organization mapping (same as pricing)
        org_filter = " OR ".join([
            f"UPPER(COALESCE(pe.parent_organization, p.contract_name)) LIKE '%{co.upper()}%'" 
            for co in TARGET_COMPANIES
        ])
        
        org_mapping_df = conn.execute(f"""
            SELECT DISTINCT
                COALESCE(pe.parent_organization, p.contract_name) as parent_org,
                p.contract_id
            FROM plans p
            LEFT JOIN plan_enrollment pe ON p.contract_id = pe.contract_number AND p.plan_id = pe.plan_id
            WHERE ({org_filter})
        """).fetchdf()
        
        def normalize_org(org_name):
            if not org_name:
                return None
            org_upper = org_name.upper()
            for target in TARGET_COMPANIES:
                if target.upper() in org_upper:
                    return target
            return None
        
        org_mapping_df['normalized_org'] = org_mapping_df['parent_org'].apply(normalize_org)
        valid_contracts = org_mapping_df[org_mapping_df['normalized_org'].notna()]['contract_id'].unique().tolist()
        
        if not valid_contracts:
            return []
        
        contract_list = ','.join([f"'{c}'" for c in valid_contracts])
        
        results = []
        
        for drug_name, rxcui in GLP1_DRUGS.items():
            # Get all NDCs for this drug (each NDC = different dosage/strength)
            ndcs_query = f"""
                SELECT DISTINCT ndc
                FROM formulary_drugs
                WHERE CAST(rxcui AS VARCHAR) = '{rxcui}'
                  AND ndc IS NOT NULL
                ORDER BY ndc
            """
            ndcs_df = conn.execute(ndcs_query).fetchdf()
            
            if ndcs_df.empty:
                continue
            
            # Process each NDC separately
            for _, ndc_row in ndcs_df.iterrows():
                ndc = ndc_row['ndc']
                
                query = f"""
            WITH company_plans AS (
                SELECT DISTINCT
                    p.contract_id,
                    p.plan_key,
                    p.formulary_id,
                    COALESCE(pe.parent_organization, p.contract_name) as parent_org
                FROM plans p
                LEFT JOIN plan_enrollment pe ON p.contract_id = pe.contract_number AND p.plan_id = pe.plan_id
                WHERE p.contract_id IN ({contract_list})
            ),
            normalized_plans AS (
                SELECT 
                    cp.*,
                    CASE 
                        WHEN UPPER(cp.parent_org) LIKE '%ELEVANCE%' THEN 'Elevance'
                        WHEN UPPER(cp.parent_org) LIKE '%UNITEDHEALTH%' OR UPPER(cp.parent_org) LIKE '%UNITED HEALTH%' THEN 'UnitedHealth'
                        WHEN UPPER(cp.parent_org) LIKE '%HUMANA%' THEN 'Humana'
                        WHEN UPPER(cp.parent_org) LIKE '%CVS%' OR UPPER(cp.parent_org) LIKE '%AETNA%' THEN 'CVS'
                        WHEN UPPER(cp.parent_org) LIKE '%MOLINA%' THEN 'Molina'
                        WHEN UPPER(cp.parent_org) LIKE '%CENTENE%' THEN 'Centene'
                        WHEN UPPER(cp.parent_org) LIKE '%ALIGNMENT%' THEN 'Alignment'
                        ELSE NULL
                    END as normalized_org
                FROM company_plans cp
                WHERE cp.parent_org IS NOT NULL
            ),
            drug_tiers AS (
                SELECT DISTINCT
                    np.normalized_org,
                    np.plan_key,
                    CAST(fd.tier_level_value AS INTEGER) as tier
                FROM normalized_plans np
                JOIN formulary_drugs fd ON np.formulary_id = fd.formulary_id
                WHERE CAST(fd.rxcui AS VARCHAR) = '{rxcui}'
                  AND fd.ndc = '{ndc}'
                  AND np.normalized_org IS NOT NULL
                  AND fd.tier_level_value IS NOT NULL
            ),
            member_costs AS (
                SELECT 
                    dt.normalized_org,
                    dt.plan_key,
                    dt.tier,
                    bc.cost_type_pref,
                    bc.cost_amt_pref,
                    bc.days_supply
                FROM drug_tiers dt
                JOIN beneficiary_costs bc ON dt.plan_key = bc.plan_key 
                    AND CAST(bc.tier AS INTEGER) = dt.tier
                    AND bc.coverage_level = 1
                    AND bc.days_supply = 30
            )
            SELECT 
                normalized_org as company,
                cost_type_pref,
                cost_amt_pref,
                COUNT(DISTINCT plan_key) as plan_count
            FROM member_costs
            WHERE normalized_org IS NOT NULL
            GROUP BY normalized_org, cost_type_pref, cost_amt_pref
            ORDER BY normalized_org, cost_type_pref, CAST(cost_amt_pref AS DOUBLE)
            """
            
            try:
                result = conn.execute(query).fetchdf()
                
                if result.empty:
                    # Create empty entry
                    drug_row = {
                        'drug_name': drug_name,
                        'ndc': ndc,
                        'rxcui': rxcui,
                        'companies': {}
                    }
                    for company in TARGET_COMPANIES:
                        drug_row['companies'][company] = {
                            'copay_plans': 0,
                            'copay_pct': 0.0,
                            'avg_copay': 0.0,
                            'coinsurance_plans': 0,
                            'coinsurance_pct': 0.0,
                            'avg_coinsurance': 0.0,
                            'no_charge_plans': 0,
                            'no_charge_pct': 0.0
                        }
                    results.append(drug_row)
                    continue
                
                # Get total plans per company for this drug
                total_plans_query = f"""
                    WITH company_plans AS (
                        SELECT DISTINCT
                            p.contract_id,
                            p.plan_key,
                            p.formulary_id,
                            COALESCE(pe.parent_organization, p.contract_name) as parent_org
                        FROM plans p
                        LEFT JOIN plan_enrollment pe ON p.contract_id = pe.contract_number AND p.plan_id = pe.plan_id
                        WHERE p.contract_id IN ({contract_list})
                    ),
                    normalized_plans AS (
                        SELECT 
                            cp.*,
                            CASE 
                                WHEN UPPER(cp.parent_org) LIKE '%ELEVANCE%' THEN 'Elevance'
                                WHEN UPPER(cp.parent_org) LIKE '%UNITEDHEALTH%' OR UPPER(cp.parent_org) LIKE '%UNITED HEALTH%' THEN 'UnitedHealth'
                                WHEN UPPER(cp.parent_org) LIKE '%HUMANA%' THEN 'Humana'
                                WHEN UPPER(cp.parent_org) LIKE '%CVS%' OR UPPER(cp.parent_org) LIKE '%AETNA%' THEN 'CVS'
                                WHEN UPPER(cp.parent_org) LIKE '%MOLINA%' THEN 'Molina'
                                WHEN UPPER(cp.parent_org) LIKE '%CENTENE%' THEN 'Centene'
                                WHEN UPPER(cp.parent_org) LIKE '%ALIGNMENT%' THEN 'Alignment'
                                ELSE NULL
                            END as normalized_org
                        FROM company_plans cp
                        WHERE cp.parent_org IS NOT NULL
                    ),
                    drug_plans AS (
                        SELECT DISTINCT
                            np.normalized_org,
                            np.plan_key
                        FROM normalized_plans np
                        JOIN formulary_drugs fd ON np.formulary_id = fd.formulary_id
                        WHERE CAST(fd.rxcui AS VARCHAR) = '{rxcui}'
                          AND np.normalized_org IS NOT NULL
                    )
                    SELECT 
                        normalized_org as company,
                        COUNT(DISTINCT plan_key) as total_plans
                    FROM drug_plans
                    GROUP BY normalized_org
                """
                
                total_plans_df = conn.execute(total_plans_query).fetchdf()
                total_plans_dict = dict(zip(total_plans_df['company'], total_plans_df['total_plans']))
                
                # Process results
                drug_row = {
                    'drug_name': drug_name,
                    'ndc': ndc,
                    'rxcui': rxcui,
                    'companies': {}
                }
                
                for company in TARGET_COMPANIES:
                    company_data = result[result['company'] == company]
                    total = total_plans_dict.get(company, 0)
                    
                    if company_data.empty or total == 0:
                        drug_row['companies'][company] = {
                            'copay_plans': 0,
                            'copay_pct': 0.0,
                            'avg_copay': 0.0,
                            'coinsurance_plans': 0,
                            'coinsurance_pct': 0.0,
                            'avg_coinsurance': 0.0,
                            'no_charge_plans': 0,
                            'no_charge_pct': 0.0
                        }
                    else:
                        # Separate by cost type (cost_type_pref: 0=copay, 1=coinsurance, 2=no charge)
                        copay_data = company_data[company_data['cost_type_pref'].astype(str) == '0']
                        coinsurance_data = company_data[company_data['cost_type_pref'].astype(str) == '1']
                        no_charge_data = company_data[company_data['cost_type_pref'].astype(str) == '2']
                        
                        copay_count = copay_data['plan_count'].sum() if not copay_data.empty else 0
                        coinsurance_count = coinsurance_data['plan_count'].sum() if not coinsurance_data.empty else 0
                        no_charge_count = no_charge_data['plan_count'].sum() if not no_charge_data.empty else 0
                        
                        avg_copay = 0.0
                        if not copay_data.empty:
                            copay_amounts = copay_data['cost_amt_pref'].astype(float)
                            avg_copay = float(copay_amounts.mean())
                        
                        avg_coinsurance = 0.0
                        if not coinsurance_data.empty:
                            coinsurance_amounts = coinsurance_data['cost_amt_pref'].astype(float)
                            avg_coinsurance = float(coinsurance_amounts.mean())
                        
                        drug_row['companies'][company] = {
                            'copay_plans': int(copay_count),
                            'copay_pct': round(copay_count * 100.0 / total, 1) if total > 0 else 0.0,
                            'avg_copay': round(avg_copay, 2),
                            'coinsurance_plans': int(coinsurance_count),
                            'coinsurance_pct': round(coinsurance_count * 100.0 / total, 1) if total > 0 else 0.0,
                            'avg_coinsurance': round(avg_coinsurance, 1),
                            'no_charge_plans': int(no_charge_count),
                            'no_charge_pct': round(no_charge_count * 100.0 / total, 1) if total > 0 else 0.0
                        }
                
                results.append(drug_row)
            except Exception as e:
                print(f"Error getting member costs for {drug_name}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        return results
    except Exception as e:
        import traceback
        error_msg = f"Error in get_glp1_member_costs: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return []

@app.get("/api/glp1/companies")
async def get_glp1_companies(year: str = "2025"):
    """Get list of target companies with their totals"""
    
    try:
        conn = get_db(year)
        
        company_filter = " OR ".join([
            f"UPPER(contract_name) LIKE '%{co.upper()}%'" 
            for co in TARGET_COMPANIES
        ])
        
        result = conn.execute(f"""
            SELECT 
                contract_name,
                COUNT(DISTINCT formulary_id) as total_formularies,
                COUNT(DISTINCT plan_id) as total_plans
            FROM plans
            WHERE ({company_filter})
            GROUP BY contract_name
            ORDER BY contract_name
        """).fetchdf()
        
        return result.to_dict(orient='records')
    except Exception as e:
        import traceback
        error_msg = f"Error in get_glp1_companies: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "details": error_msg}
        )

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "medicare-part-d-intelligence"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

