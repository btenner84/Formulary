import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path

# Page config
st.set_page_config(
    page_title="Medicare Part D Intelligence",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS - Clean, Professional Blue/White Theme
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Global theme */
    .stApp {
        background: #f8fafc;
        font-family: 'Inter', sans-serif;
    }
    
    /* Main title */
    h1 {
        color: #1e40af !important;
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        text-align: center;
        padding: 20px 0;
        border-bottom: 3px solid #3b82f6;
        margin-bottom: 30px;
    }
    
    /* Section headers */
    h2, h3 {
        color: #1e3a8a !important;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        border-left: 4px solid #3b82f6;
        padding-left: 15px;
        margin-top: 30px;
    }
    
    /* Metric boxes */
    [data-testid="stMetricValue"] {
        color: #1e40af !important;
        font-size: 2rem !important;
        font-family: 'Inter', sans-serif;
        font-weight: 700;
    }
    
    [data-testid="stMetricLabel"] {
        color: #475569 !important;
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.5px;
    }
    
    /* Dataframes */
    [data-testid="stDataFrame"] {
        border: 1px solid #e2e8f0 !important;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    
    /* Tables */
    .dataframe {
        background-color: white !important;
        color: #1e293b !important;
        font-family: 'Inter', sans-serif;
        font-size: 0.875rem;
    }
    
    .dataframe thead tr th {
        background-color: #3b82f6 !important;
        color: white !important;
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.5px;
        padding: 12px !important;
    }
    
    .dataframe tbody tr:hover {
        background-color: #f1f5f9 !important;
    }
    
    /* Selectbox */
    .stSelectbox label {
        color: #1e40af !important;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
    }
    
    /* Buttons */
    .stButton > button {
        background: #3b82f6;
        color: white;
        border: none;
        border-radius: 6px;
        padding: 10px 24px;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        transition: all 0.2s;
    }
    
    .stButton > button:hover {
        background: #2563eb;
        box-shadow: 0 4px 6px rgba(59, 130, 246, 0.3);
        transform: translateY(-1px);
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: white;
        border-right: 1px solid #e2e8f0;
    }
    
    [data-testid="stSidebar"] h2 {
        color: #1e40af !important;
        text-align: center;
        border: none;
    }
    
    /* Info boxes */
    .stAlert {
        background-color: #eff6ff;
        border: 1px solid #93c5fd;
        border-radius: 8px;
        color: #1e3a8a;
    }
    
    /* Divider */
    hr {
        border-color: #e2e8f0;
    }
    
    /* Cards */
    .metric-card {
        background: white;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
</style>
""", unsafe_allow_html=True)

# Initialize DuckDB connection
@st.cache_resource
def get_db_connection():
    conn = duckdb.connect(':memory:')
    data_dir = Path("medicare_parquet")
    
    # Load all parquet files
    conn.execute(f"""
        CREATE VIEW plans AS 
        SELECT * FROM read_parquet('{data_dir}/plan_information.parquet')
    """)
    
    conn.execute(f"""
        CREATE VIEW formulary_drugs AS 
        SELECT * FROM read_parquet('{data_dir}/formulary_drugs.parquet')
    """)
    
    conn.execute(f"""
        CREATE VIEW beneficiary_costs AS 
        SELECT * FROM read_parquet('{data_dir}/beneficiary_costs.parquet')
    """)
    
    conn.execute(f"""
        CREATE VIEW geographic AS 
        SELECT * FROM read_parquet('{data_dir}/geographic_locator.parquet')
    """)
    
    # Check if pricing data exists
    pricing_file = data_dir / "drug_pricing.parquet"
    if pricing_file.exists():
        conn.execute(f"""
            CREATE VIEW drug_pricing AS 
            SELECT * FROM read_parquet('{data_dir}/drug_pricing.parquet')
        """)
    
    return conn

conn = get_db_connection()

# Title
st.markdown("# 💊 Medicare Part D Intelligence Platform")

# Sidebar - Formulary Selector
with st.sidebar:
    st.markdown("## Select Formulary")
    
    # Get all unique formularies with parent organization
    formularies_df = conn.execute("""
        SELECT 
            formulary_id,
            COUNT(DISTINCT contract_name) as org_count,
            STRING_AGG(DISTINCT LEFT(contract_name, 30), ', ') as organizations,
            COUNT(*) as plan_count
        FROM plans
        WHERE formulary_id IS NOT NULL
        GROUP BY formulary_id
        ORDER BY plan_count DESC
    """).fetchdf()
    
    st.metric("Total Formularies", f"{len(formularies_df):,}")
    
    # Add organization filter
    all_orgs = conn.execute("""
        SELECT DISTINCT LEFT(contract_name, 30) as org
        FROM plans
        WHERE contract_name IS NOT NULL
        ORDER BY org
    """).fetchdf()['org'].tolist()
    
    org_filter = st.selectbox(
        "Filter by Organization:",
        options=["All"] + all_orgs,
        index=0
    )
    
    # Filter formularies by organization
    if org_filter != "All":
        filtered_formularies = conn.execute(f"""
            SELECT DISTINCT formulary_id
            FROM plans
            WHERE LEFT(contract_name, 30) = '{org_filter}'
        """).fetchdf()['formulary_id'].tolist()
        formularies_df = formularies_df[formularies_df['formulary_id'].isin(filtered_formularies)]
    
    selected_formulary = st.selectbox(
        "Choose Formulary:",
        options=formularies_df['formulary_id'].tolist(),
        format_func=lambda x: f"Formulary {x} ({formularies_df[formularies_df['formulary_id']==x]['plan_count'].iloc[0]} plans)"
    )
    
    # Show organizations using this formulary
    if selected_formulary:
        orgs = formularies_df[formularies_df['formulary_id']==selected_formulary]['organizations'].iloc[0]
        st.markdown(f"**Used by:** {orgs}...")
        org_count = formularies_df[formularies_df['formulary_id']==selected_formulary]['org_count'].iloc[0]
        st.caption(f"({org_count} organization{'s' if org_count > 1 else ''})")
    
    st.markdown("---")
    st.markdown("### Quick Stats")
    
    total_plans = conn.execute("SELECT COUNT(DISTINCT plan_id) FROM plans").fetchone()[0]
    total_drugs = conn.execute("SELECT COUNT(DISTINCT rxcui) FROM formulary_drugs").fetchone()[0]
    
    st.metric("Total Plans", f"{total_plans:,}")
    st.metric("Total Drugs", f"{total_drugs:,}")

# Main content
if selected_formulary:
    # Get parent organizations for this formulary
    parent_orgs_df = conn.execute(f"""
        SELECT 
            DISTINCT contract_name,
            COUNT(*) as plan_count
        FROM plans
        WHERE formulary_id = '{selected_formulary}'
        GROUP BY contract_name
        ORDER BY plan_count DESC
    """).fetchdf()
    
    st.markdown(f"## Formulary {selected_formulary}")
    
    # Show parent organizations
    if len(parent_orgs_df) > 0:
        st.markdown("### Parent Organizations")
        col1, col2 = st.columns([3, 1])
        with col1:
            org_list = ", ".join([f"**{row['contract_name']}**" for _, row in parent_orgs_df.head(5).iterrows()])
            if len(parent_orgs_df) > 5:
                org_list += f" + {len(parent_orgs_df) - 5} more"
            st.markdown(org_list)
        with col2:
            st.metric("Total Orgs", len(parent_orgs_df))
    
    # Get plans using this formulary
    plans_df = conn.execute(f"""
        SELECT 
            contract_name as parent_organization,
            contract_id,
            plan_id,
            plan_name,
            premium,
            deductible,
            state,
            snp as snp_type
        FROM plans
        WHERE formulary_id = '{selected_formulary}'
        ORDER BY contract_name, plan_name
    """).fetchdf()
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Plans Using This Formulary", f"{len(plans_df):,}")
    
    with col2:
        avg_premium = plans_df['premium'].astype(float).mean()
        st.metric("Avg Premium", f"${avg_premium:.2f}")
    
    with col3:
        avg_deductible = plans_df['deductible'].astype(float).mean()
        st.metric("Avg Deductible", f"${avg_deductible:.0f}")
    
    with col4:
        states = plans_df['state'].nunique()
        st.metric("States Covered", f"{states}")
    
    # Plans table
    st.markdown("---")
    st.markdown("## All Plans")
    st.dataframe(
        plans_df,
        use_container_width=True,
        height=400
    )
    
    # Get specialty drugs (Tier 5 or marked as specialty)
    specialty_drugs_df = conn.execute(f"""
        SELECT 
            fd.rxcui,
            fd.ndc,
            fd.tier,
            fd.quantity_limit_yn,
            fd.prior_authorization_yn,
            fd.step_therapy_yn,
            COUNT(DISTINCT fd.ndc) as ndc_count
        FROM formulary_drugs fd
        WHERE fd.formulary_id = '{selected_formulary}'
          AND (fd.tier = '5' OR fd.tier = '6')
        GROUP BY fd.rxcui, fd.ndc, fd.tier, fd.quantity_limit_yn, fd.prior_authorization_yn, fd.step_therapy_yn
        ORDER BY fd.rxcui
    """).fetchdf()
    
    st.markdown("## Specialty Drugs in This Formulary")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Specialty Drugs", f"{len(specialty_drugs_df):,}")
    with col2:
        pa_required = specialty_drugs_df['prior_authorization_yn'].value_counts().get('Y', 0)
        st.metric("Requiring Prior Auth", f"{pa_required:,}")
    
    # Get cost-sharing for specialty drugs
    # Get a sample plan from this formulary
    sample_plan = plans_df.iloc[0] if len(plans_df) > 0 else None
    
    if sample_plan is not None:
        cost_df = conn.execute(f"""
            SELECT 
                tier,
                cost_type,
                cost_amt,
                preferred_cost_share_yn,
                pharmacy_type
            FROM beneficiary_costs
            WHERE contract_id = '{sample_plan['contract_id']}'
              AND plan_id = '{sample_plan['plan_id']}'
              AND tier IN ('5', '6')
              AND coverage_level = 'I'
            ORDER BY tier, pharmacy_type
        """).fetchdf()
        
        if len(cost_df) > 0:
            st.markdown("### Cost-Sharing Structure")
            st.markdown(f"*Sample from: {sample_plan['parent_organization']} - {sample_plan['plan_name']}*")
            
            # Format cost display
            cost_display = cost_df.copy()
            cost_display['cost_type_label'] = cost_display['cost_type'].map({
                '0': 'Copay',
                '1': 'Coinsurance',
                '2': 'No Charge'
            })
            cost_display['cost_display'] = cost_display.apply(
                lambda row: f"${row['cost_amt']}" if row['cost_type'] == '0' else f"{row['cost_amt']}%" if row['cost_type'] == '1' else "No Charge",
                axis=1
            )
            
            st.dataframe(
                cost_display[['tier', 'cost_type_label', 'cost_display', 'preferred_cost_share_yn', 'pharmacy_type']],
                use_container_width=True,
                height=200
            )
    
    # Specialty drugs details
    st.markdown("### Specialty Drugs Details")
    
    # Add restrictions summary
    specialty_drugs_df['restrictions'] = specialty_drugs_df.apply(
        lambda row: ', '.join([
            x for x in [
                'Qty Limit' if row['quantity_limit_yn'] == 'Y' else None,
                'Prior Auth' if row['prior_authorization_yn'] == 'Y' else None,
                'Step Therapy' if row['step_therapy_yn'] == 'Y' else None
            ] if x
        ]) or 'None',
        axis=1
    )
    
    # Check if pricing data is available
    pricing_file = Path("medicare_parquet/drug_pricing.parquet")
    if pricing_file.exists() and len(plans_df) > 0:
        st.markdown("#### With Negotiated Pricing")
        
        # Get pricing for specialty drugs in these plans
        plan_keys = "','".join([f"{row['contract_id']}|{row['plan_id']}" for _, row in plans_df.head(5).iterrows()])
        
        pricing_query = f"""
            SELECT 
                sd.rxcui,
                sd.ndc,
                sd.tier,
                sd.restrictions,
                AVG(dp.unit_cost) as avg_unit_cost,
                MIN(dp.unit_cost) as min_unit_cost,
                MAX(dp.unit_cost) as max_unit_cost,
                COUNT(DISTINCT dp.plan_key) as plan_count
            FROM specialty_drugs_df sd
            LEFT JOIN drug_pricing dp ON sd.ndc = dp.ndc
            WHERE dp.plan_key IN ('{plan_keys}')
            GROUP BY sd.rxcui, sd.ndc, sd.tier, sd.restrictions
            ORDER BY avg_unit_cost DESC
            LIMIT 100
        """
        
        try:
            pricing_display = conn.execute(pricing_query).fetchdf()
            pricing_display['avg_unit_cost'] = pricing_display['avg_unit_cost'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
            pricing_display['min_unit_cost'] = pricing_display['min_unit_cost'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
            pricing_display['max_unit_cost'] = pricing_display['max_unit_cost'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
            
            st.dataframe(
                pricing_display,
                use_container_width=True,
                height=500
            )
        except:
            st.dataframe(
                specialty_drugs_df[['rxcui', 'ndc', 'tier', 'restrictions']],
                use_container_width=True,
                height=500
            )
    else:
        st.dataframe(
            specialty_drugs_df[['rxcui', 'ndc', 'tier', 'restrictions']],
            use_container_width=True,
            height=500
        )
        
        if not pricing_file.exists():
            st.info("💡 Pricing data is being converted. Refresh the page in a few minutes to see negotiated costs.")
    
    # Download button
    csv = specialty_drugs_df.to_csv(index=False)
    st.download_button(
        label="Download Specialty Drugs Data",
        data=csv,
        file_name=f"specialty_drugs_formulary_{selected_formulary}.csv",
        mime="text/csv"
    )

else:
    st.info("👈 Select a formulary from the sidebar to begin")
    
    # Show overview
    st.markdown("## System Overview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### Available Data
        - **115,379 Plans** across all contracts
        - **1.3M+ Formulary Records** with tier and restriction data
        - **165K+ Cost-Sharing Records** for patient calculations
        - **Geographic Coverage** across all US counties
        """)
    
    with col2:
        st.markdown("""
        ### How to Use
        1. Select a **Formulary** from the sidebar
        2. View all **Plans** using that formulary
        3. Analyze **Specialty Drugs** with pricing and restrictions
        4. Download data for further analysis
        """)

# Footer
st.markdown("---")
st.markdown(
    "<p style='text-align: center; color: #64748b; font-family: Inter; font-size: 0.875rem;'>Medicare Part D Intelligence Platform • CMS Q2 2025 Data</p>",
    unsafe_allow_html=True
)
