-- ============================================
-- MEDICARE PART D ANALYSIS QUERIES
-- ============================================

-- ============================================
-- VIEW 1: FORMULARY DETAILED VIEW
-- ============================================

-- Get formulary overview
SELECT 
    f.formulary_id,
    fs.total_drugs,
    fs.specialty_drugs,
    fs.plans_using
FROM formularies f
JOIN formulary_summary fs ON fs.formulary_id = f.formulary_id
WHERE f.formulary_id = '00025456';

-- Get all plans using a formulary
SELECT 
    p.contract_id,
    p.plan_id,
    c.contract_name,
    p.plan_name,
    p.premium,
    p.deductible,
    p.snp_type,
    COUNT(DISTINCT pg.county_code) as counties_covered
FROM plans p
JOIN contracts c ON c.contract_id = p.contract_id
LEFT JOIN plan_geography pg ON pg.plan_key = p.plan_key
WHERE p.formulary_id = '00025456'
GROUP BY p.plan_key, p.contract_id, p.plan_id, c.contract_name, p.plan_name, p.premium, p.deductible, p.snp_type
ORDER BY p.premium, p.deductible;

-- Get all drugs in a formulary with details
SELECT 
    fd.ndc,
    fd.rxcui,
    fd.tier,
    CASE 
        WHEN fd.tier = 1 THEN 'Generic'
        WHEN fd.tier = 2 THEN 'Preferred Generic'
        WHEN fd.tier = 3 THEN 'Preferred Brand'
        WHEN fd.tier = 4 THEN 'Non-Preferred'
        WHEN fd.tier = 5 THEN 'Specialty'
        ELSE 'Other'
    END as tier_description,
    fd.quantity_limit_yn,
    fd.quantity_limit_amount,
    fd.quantity_limit_days,
    fd.prior_authorization_yn,
    fd.step_therapy_yn,
    COUNT(*) as formulary_count
FROM formulary_drugs fd
WHERE fd.formulary_id = '00025456'
GROUP BY fd.ndc, fd.rxcui, fd.tier, fd.quantity_limit_yn, fd.quantity_limit_amount,
         fd.quantity_limit_days, fd.prior_authorization_yn, fd.step_therapy_yn
ORDER BY fd.tier, fd.ndc;

-- Get specialty drugs only in a formulary
SELECT 
    fd.ndc,
    fd.rxcui,
    fd.tier,
    fd.quantity_limit_yn,
    fd.prior_authorization_yn,
    fd.step_therapy_yn
FROM formulary_drugs fd
WHERE fd.formulary_id = '00025456'
  AND fd.tier = 5
ORDER BY fd.ndc;

-- ============================================
-- VIEW 2: COUNTY COMPETITIVE VIEW
-- ============================================

-- Get all plans available in a county
SELECT 
    p.contract_id,
    c.contract_name,
    p.plan_id,
    p.plan_name,
    p.premium,
    p.deductible,
    p.snp_type,
    p.formulary_id,
    fs.specialty_drugs
FROM plan_geography pg
JOIN plans p ON p.plan_key = pg.plan_key
JOIN contracts c ON c.contract_id = p.contract_id
LEFT JOIN formulary_summary fs ON fs.formulary_id = p.formulary_id
WHERE pg.county_code = '29189'  -- St. Louis County, MO
ORDER BY p.premium, p.deductible;

-- Get county summary
SELECT 
    county_code,
    state_name,
    county_name,
    plan_count,
    ROUND(avg_premium::numeric, 2) as avg_premium,
    ROUND(min_premium::numeric, 2) as min_premium,
    ROUND(max_premium::numeric, 2) as max_premium
FROM county_plan_summary
WHERE county_code = '29189';

-- Compare specialty drug pricing across plans in a county
SELECT 
    dp.ndc,
    p.contract_id,
    p.plan_id,
    c.contract_name,
    p.premium,
    p.deductible,
    ROUND(dp.unit_cost::numeric, 2) as negotiated_cost,
    bc.cost_type_pref,
    ROUND(bc.cost_amt_pref::numeric, 2) as member_cost_amt,
    ROUND(bc.cost_max_amt_pref::numeric, 2) as member_cost_max,
    CASE 
        WHEN bc.cost_type_pref = 0 THEN dp.unit_cost - COALESCE(bc.cost_amt_pref, 0)
        WHEN bc.cost_type_pref = 1 THEN dp.unit_cost - (dp.unit_cost * bc.cost_amt_pref)
        ELSE dp.unit_cost
    END as plan_net_cost
FROM plan_geography pg
JOIN plans p ON p.plan_key = pg.plan_key
JOIN contracts c ON c.contract_id = p.contract_id
JOIN formulary_drugs fd ON fd.formulary_id = p.formulary_id
JOIN drug_pricing dp ON dp.plan_key = p.plan_key AND dp.ndc = fd.ndc
LEFT JOIN beneficiary_costs bc ON bc.plan_key = p.plan_key AND bc.tier = fd.tier AND bc.coverage_level = 1 AND bc.days_supply = 1
WHERE pg.county_code = '29189'
  AND fd.tier = 5  -- Specialty drugs only
  AND dp.days_supply = 30
  AND dp.ndc = '00002533754'  -- Specific drug
ORDER BY dp.unit_cost;

-- ============================================
-- COMPETITIVE ANALYSIS QUERIES
-- ============================================

-- Top 25 most covered specialty drugs
SELECT 
    fd.ndc,
    COUNT(DISTINCT fd.formulary_id) as formulary_count,
    COUNT(DISTINCT CASE WHEN fd.prior_authorization_yn = 'Y' THEN fd.formulary_id END) as pa_required_count,
    COUNT(DISTINCT CASE WHEN fd.quantity_limit_yn = 'Y' THEN fd.formulary_id END) as qty_limit_count
FROM formulary_drugs fd
WHERE fd.tier = 5
GROUP BY fd.ndc
ORDER BY formulary_count DESC
LIMIT 25;

-- Compare negotiated costs for a specialty drug across all contracts
SELECT 
    c.contract_name,
    p.contract_id,
    COUNT(DISTINCT p.plan_key) as plan_count,
    ROUND(MIN(dp.unit_cost::numeric), 2) as min_cost,
    ROUND(AVG(dp.unit_cost::numeric), 2) as avg_cost,
    ROUND(MAX(dp.unit_cost::numeric), 2) as max_cost,
    ROUND((MAX(dp.unit_cost::numeric) - MIN(dp.unit_cost::numeric)) / MIN(dp.unit_cost::numeric) * 100, 1) as cost_variation_pct
FROM drug_pricing dp
JOIN plans p ON p.plan_key = dp.plan_key
JOIN contracts c ON c.contract_id = p.contract_id
WHERE dp.ndc = '00002533754'
  AND dp.days_supply = 30
GROUP BY c.contract_name, p.contract_id
HAVING COUNT(DISTINCT p.plan_key) > 0
ORDER BY avg_cost;

-- Formulary restrictiveness analysis
SELECT 
    f.formulary_id,
    COUNT(DISTINCT fd.ndc) as total_drugs,
    COUNT(DISTINCT CASE WHEN fd.tier = 5 THEN fd.ndc END) as specialty_drugs,
    ROUND(COUNT(DISTINCT CASE WHEN fd.prior_authorization_yn != 'N' THEN fd.ndc END)::numeric / 
          COUNT(DISTINCT fd.ndc) * 100, 1) as pct_with_pa,
    ROUND(COUNT(DISTINCT CASE WHEN fd.step_therapy_yn = 'Y' THEN fd.ndc END)::numeric / 
          COUNT(DISTINCT fd.ndc) * 100, 1) as pct_with_step_therapy,
    ROUND(COUNT(DISTINCT CASE WHEN fd.quantity_limit_yn = 'Y' THEN fd.ndc END)::numeric / 
          COUNT(DISTINCT fd.ndc) * 100, 1) as pct_with_qty_limits
FROM formularies f
JOIN formulary_drugs fd ON fd.formulary_id = f.formulary_id
GROUP BY f.formulary_id
ORDER BY pct_with_pa DESC;

-- Geographic competition intensity
SELECT 
    gl.state_name,
    gl.county_name,
    cps.county_code,
    cps.plan_count,
    ROUND(cps.avg_premium::numeric, 2) as avg_premium
FROM county_plan_summary cps
JOIN geographic_locator gl ON gl.county_code = cps.county_code
WHERE gl.state_name = 'Missouri'
ORDER BY cps.plan_count DESC
LIMIT 20;

-- ============================================
-- SPECIALTY DRUG MARGIN ANALYSIS
-- ============================================

-- Calculate plan net cost for specialty drugs
WITH specialty_pricing AS (
    SELECT 
        p.contract_id,
        p.plan_key,
        dp.ndc,
        dp.unit_cost as negotiated_cost,
        bc.cost_type_pref,
        bc.cost_amt_pref,
        CASE 
            WHEN bc.cost_type_pref = 0 THEN bc.cost_amt_pref  -- Flat copay
            WHEN bc.cost_type_pref = 1 THEN LEAST(dp.unit_cost * bc.cost_amt_pref, bc.cost_max_amt_pref)  -- Coinsurance with cap
            WHEN bc.cost_type_pref = 2 THEN 0  -- No charge
            ELSE 0
        END as member_pays,
        CASE 
            WHEN bc.cost_type_pref = 0 THEN dp.unit_cost - bc.cost_amt_pref
            WHEN bc.cost_type_pref = 1 THEN dp.unit_cost - LEAST(dp.unit_cost * bc.cost_amt_pref, bc.cost_max_amt_pref)
            WHEN bc.cost_type_pref = 2 THEN dp.unit_cost
            ELSE dp.unit_cost
        END as plan_net_cost
    FROM drug_pricing dp
    JOIN plans p ON p.plan_key = dp.plan_key
    JOIN formulary_drugs fd ON fd.formulary_id = p.formulary_id AND fd.ndc = dp.ndc
    JOIN beneficiary_costs bc ON bc.plan_key = p.plan_key AND bc.tier = fd.tier AND bc.coverage_level = 1 AND bc.days_supply = 1
    WHERE fd.tier = 5
      AND dp.days_supply = 30
)
SELECT 
    contract_id,
    COUNT(DISTINCT ndc) as specialty_drugs_priced,
    ROUND(AVG(negotiated_cost::numeric), 2) as avg_negotiated_cost,
    ROUND(AVG(member_pays::numeric), 2) as avg_member_pays,
    ROUND(AVG(plan_net_cost::numeric), 2) as avg_plan_net_cost
FROM specialty_pricing
GROUP BY contract_id
ORDER BY avg_plan_net_cost;

-- ============================================
-- MEMBER COST CALCULATOR
-- ============================================

-- Calculate total annual cost for a member with specific drugs
-- Replace NDCs in the IN clause with actual patient drug list
WITH patient_drugs AS (
    SELECT ndc, 12 as fills_per_year
    FROM (VALUES 
        ('00002533754'),
        ('00002448354'),
        ('00002481554')
    ) AS t(ndc)
),
plan_costs AS (
    SELECT 
        p.plan_key,
        p.contract_id,
        p.plan_name,
        p.premium,
        p.deductible,
        pd.ndc,
        pd.fills_per_year,
        dp.unit_cost as drug_cost,
        bc.cost_type_pref,
        bc.cost_amt_pref,
        bc.cost_max_amt_pref,
        CASE 
            WHEN bc.cost_type_pref = 0 THEN bc.cost_amt_pref * pd.fills_per_year
            WHEN bc.cost_type_pref = 1 THEN LEAST(dp.unit_cost * bc.cost_amt_pref, bc.cost_max_amt_pref) * pd.fills_per_year
            ELSE 0
        END as annual_drug_cost
    FROM patient_drugs pd
    JOIN formulary_drugs fd ON fd.ndc = pd.ndc
    JOIN plans p ON p.formulary_id = fd.formulary_id
    JOIN drug_pricing dp ON dp.plan_key = p.plan_key AND dp.ndc = pd.ndc AND dp.days_supply = 30
    JOIN beneficiary_costs bc ON bc.plan_key = p.plan_key AND bc.tier = fd.tier AND bc.coverage_level = 1 AND bc.days_supply = 1
    WHERE p.contract_id = 'H0028'  -- Humana only for this example
)
SELECT 
    plan_key,
    plan_name,
    ROUND(premium::numeric, 2) as monthly_premium,
    ROUND(deductible::numeric, 2) as annual_deductible,
    ROUND(SUM(annual_drug_cost)::numeric, 2) as annual_drug_costs,
    ROUND((premium * 12 + deductible + SUM(annual_drug_cost))::numeric, 2) as total_annual_cost
FROM plan_costs
GROUP BY plan_key, plan_name, premium, deductible
ORDER BY total_annual_cost;

