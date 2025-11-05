-- Medicare Part D Database Schema
-- Q2 2025 Data

-- ============================================
-- CORE TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS contracts (
    contract_id VARCHAR(10) PRIMARY KEY,
    contract_name VARCHAR(500),
    contract_type VARCHAR(10) -- H=MA-PD, S=PDP, R=PACE
);

CREATE TABLE IF NOT EXISTS formularies (
    formulary_id VARCHAR(20) PRIMARY KEY,
    formulary_version VARCHAR(10),
    contract_year INTEGER
);

CREATE TABLE IF NOT EXISTS plans (
    plan_key VARCHAR(30) PRIMARY KEY, -- contract_id|plan_id|segment_id
    contract_id VARCHAR(10) REFERENCES contracts(contract_id),
    plan_id VARCHAR(10),
    segment_id VARCHAR(10),
    plan_name VARCHAR(500),
    formulary_id VARCHAR(20) REFERENCES formularies(formulary_id),
    premium DECIMAL(10,2),
    deductible DECIMAL(10,2),
    snp_type INTEGER,
    plan_suppressed_yn CHAR(1),
    UNIQUE(contract_id, plan_id, segment_id)
);

CREATE INDEX idx_plans_formulary ON plans(formulary_id);
CREATE INDEX idx_plans_contract ON plans(contract_id);

-- ============================================
-- GEOGRAPHIC DATA
-- ============================================

CREATE TABLE IF NOT EXISTS geographic_locator (
    county_code VARCHAR(10) PRIMARY KEY,
    state_name VARCHAR(50),
    county_name VARCHAR(100),
    ma_region_code VARCHAR(10),
    ma_region_name VARCHAR(100),
    pdp_region_code VARCHAR(10),
    pdp_region_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS plan_geography (
    id SERIAL PRIMARY KEY,
    plan_key VARCHAR(30) REFERENCES plans(plan_key),
    state_code CHAR(2),
    county_code VARCHAR(10) REFERENCES geographic_locator(county_code),
    ma_region_code VARCHAR(10),
    pdp_region_code VARCHAR(10)
);

CREATE INDEX idx_plan_geo_plan ON plan_geography(plan_key);
CREATE INDEX idx_plan_geo_county ON plan_geography(county_code);
CREATE INDEX idx_plan_geo_state ON plan_geography(state_code);

-- ============================================
-- FORMULARY & DRUG DATA
-- ============================================

CREATE TABLE IF NOT EXISTS formulary_drugs (
    id SERIAL PRIMARY KEY,
    formulary_id VARCHAR(20) REFERENCES formularies(formulary_id),
    formulary_version VARCHAR(10),
    contract_year INTEGER,
    rxcui VARCHAR(20),
    ndc VARCHAR(20),
    tier INTEGER,
    quantity_limit_yn CHAR(1),
    quantity_limit_amount INTEGER,
    quantity_limit_days INTEGER,
    prior_authorization_yn VARCHAR(10), -- Can be Y/N or numeric days
    step_therapy_yn CHAR(1)
);

CREATE INDEX idx_formulary_drugs_formulary ON formulary_drugs(formulary_id);
CREATE INDEX idx_formulary_drugs_ndc ON formulary_drugs(ndc);
CREATE INDEX idx_formulary_drugs_tier ON formulary_drugs(tier);
CREATE INDEX idx_formulary_drugs_rxcui ON formulary_drugs(rxcui);

-- ============================================
-- PRICING DATA
-- ============================================

CREATE TABLE IF NOT EXISTS drug_pricing (
    id SERIAL PRIMARY KEY,
    plan_key VARCHAR(30) REFERENCES plans(plan_key),
    ndc VARCHAR(20),
    days_supply INTEGER,
    unit_cost DECIMAL(10,4)
);

CREATE INDEX idx_pricing_plan ON drug_pricing(plan_key);
CREATE INDEX idx_pricing_ndc ON drug_pricing(ndc);
CREATE INDEX idx_pricing_plan_ndc ON drug_pricing(plan_key, ndc);

-- ============================================
-- BENEFICIARY COST SHARING
-- ============================================

CREATE TABLE IF NOT EXISTS beneficiary_costs (
    id SERIAL PRIMARY KEY,
    plan_key VARCHAR(30) REFERENCES plans(plan_key),
    coverage_level INTEGER,
    tier INTEGER,
    days_supply INTEGER,
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
    -- Mail order preferred
    cost_type_mail_pref INTEGER,
    cost_amt_mail_pref DECIMAL(10,4),
    cost_min_amt_mail_pref DECIMAL(10,2),
    cost_max_amt_mail_pref DECIMAL(10,2),
    -- Mail order non-preferred
    cost_type_mail_nonpref INTEGER,
    cost_amt_mail_nonpref DECIMAL(10,4),
    cost_min_amt_mail_nonpref DECIMAL(10,2),
    cost_max_amt_mail_nonpref DECIMAL(10,2),
    -- Flags
    tier_specialty_yn CHAR(1),
    deductible_applies_yn CHAR(1)
);

CREATE INDEX idx_bene_costs_plan ON beneficiary_costs(plan_key);
CREATE INDEX idx_bene_costs_tier ON beneficiary_costs(tier);
CREATE INDEX idx_bene_costs_specialty ON beneficiary_costs(tier_specialty_yn);

-- ============================================
-- MATERIALIZED VIEWS FOR PERFORMANCE
-- ============================================

-- Formulary summary view
CREATE MATERIALIZED VIEW IF NOT EXISTS formulary_summary AS
SELECT 
    f.formulary_id,
    f.formulary_version,
    COUNT(DISTINCT fd.ndc) as total_drugs,
    COUNT(DISTINCT CASE WHEN fd.tier = 5 THEN fd.ndc END) as specialty_drugs,
    COUNT(DISTINCT p.plan_key) as plans_using
FROM formularies f
LEFT JOIN formulary_drugs fd ON fd.formulary_id = f.formulary_id
LEFT JOIN plans p ON p.formulary_id = f.formulary_id
GROUP BY f.formulary_id, f.formulary_version;

CREATE UNIQUE INDEX idx_formulary_summary_id ON formulary_summary(formulary_id);

-- County plan summary view
CREATE MATERIALIZED VIEW IF NOT EXISTS county_plan_summary AS
SELECT 
    pg.county_code,
    gl.state_name,
    gl.county_name,
    COUNT(DISTINCT p.plan_key) as plan_count,
    AVG(p.premium) as avg_premium,
    MIN(p.premium) as min_premium,
    MAX(p.premium) as max_premium
FROM plan_geography pg
JOIN plans p ON p.plan_key = pg.plan_key
JOIN geographic_locator gl ON gl.county_code = pg.county_code
GROUP BY pg.county_code, gl.state_name, gl.county_name;

CREATE UNIQUE INDEX idx_county_summary_county ON county_plan_summary(county_code);

-- Refresh views (run after data load)
-- REFRESH MATERIALIZED VIEW formulary_summary;
-- REFRESH MATERIALIZED VIEW county_plan_summary;

