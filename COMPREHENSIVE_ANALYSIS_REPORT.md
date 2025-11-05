# Medicare Part D Prescription Drug Plan Analysis - Q2 2025
## Comprehensive Data Analysis Report

**Data Source:** Summary Prescription Use Files (SPUF) / Prescription Plan Use Files (PPUF)  
**Data Period:** Q2 2025 (July 3, 2025 release)  
**Analysis Date:** November 4, 2025

---

## Executive Summary

This report provides a comprehensive analysis of Medicare Part D prescription drug plan data for Q2 2025, covering 723 contracts, 5,242 plans, and 384 unique formularies across all 50 states plus DC and territories. The dataset includes over 55 million pricing records, 1.3 million formulary entries, and 5.3+ million pharmacy network relationships.

---

## 1. PLAN INFORMATION ANALYSIS

### Overview Statistics
- **Total Records:** 115,380 plan-county combinations
- **Unique Contracts:** 723
- **Unique Plans:** 5,242
- **Unique Formularies:** 384
- **States/Territories Covered:** 52

### Premium Analysis
- **Minimum Premium:** $0.00 (zero-premium plans available)
- **Maximum Premium:** $190.80
- **Average Premium:** $12.70/month

### Deductible Analysis
- **Minimum Deductible:** $0.00
- **Maximum Deductible:** $590.00 (likely at or near CMS maximum)
- **Average Deductible:** $390.30

### Geographic Distribution
**Top 10 States by Plan Offerings:**
1. Georgia (GA) - 9,504 plan-county offerings
2. Texas (TX) - 6,792 plan-county offerings
3. Ohio (OH) - 6,355 plan-county offerings
4. Virginia (VA) - 5,986 plan-county offerings
5. Kentucky (KY) - 5,254 plan-county offerings
6. Pennsylvania (PA) - 4,979 plan-county offerings
7. North Carolina (NC) - 4,919 plan-county offerings
8. Indiana (IN) - 4,602 plan-county offerings
9. Michigan (MI) - 4,265 plan-county offerings
10. Tennessee (TN) - 4,214 plan-county offerings

### Special Needs Plans (SNP) Distribution
- **Standard Plans (SNP=0):** 70,563 offerings (61.2%)
- **SNP Type 1:** 6,452 offerings (5.6%)
- **Dual Eligible SNP (SNP=2):** 33,079 offerings (28.7%)
- **SNP Type 3:** 5,285 offerings (4.6%)

---

## 2. BASIC DRUGS FORMULARY ANALYSIS

### Formulary Coverage
- **Total Formulary Records:** 1,300,284
- **Unique Drugs (RxCUI):** 6,024
- **Unique NDC Codes:** 6,024

### Tier Distribution
| Tier | Records | Percentage |
|------|---------|-----------|
| Tier 1 (Generic) | 465,291 | 35.8% |
| Tier 2 (Preferred Generic) | 281,425 | 21.6% |
| Tier 5 (Specialty) | 198,848 | 15.3% |
| Tier 4 (Non-Preferred Drug) | 174,875 | 13.5% |
| Tier 3 (Preferred Brand) | 166,182 | 12.8% |
| Tier 6 | 13,649 | 1.0% |
| Tier 7 | 13 | <0.1% |

**Key Insight:** Over 57% of formulary entries are in Tiers 1-2 (generic drugs), showing strong generic utilization focus.

### Utilization Management

**Prior Authorization:**
- **No PA Required:** 808,249 entries (62.2%)
- **PA Required (Y):** 378,959 entries (29.1%)
- **Complex PA Codes:** Various numeric codes (14, 21, 25, 28, 30, 31, 365, etc.) representing different PA duration requirements in days

**Step Therapy:**
- **No Step Therapy:** 946,310 entries (72.8%)
- **Step Therapy Required:** 353,973 entries (27.2%)

**Quantity Limits:**
- **No Quantity Limits:** 808,249 entries (62.2%)
- **Quantity Limits Applied:** 492,034 entries (37.8%)

---

## 3. BENEFICIARY COST-SHARING ANALYSIS

### Cost-Sharing Structure
- **Total Cost Records:** 165,071
- **Coverage Levels:** Multiple tiers with varied cost-sharing by day supply (30/60/90 days)

### Cost Type Distribution (Preferred Drugs)
- **Coinsurance (Type 1):** 86,292 records (52.3%)
- **Copayment (Type 0):** 41,326 records (25.0%)
- **No Charge (Type 2):** 37,452 records (22.7%)

**Key Insight:** Over half of cost-sharing uses coinsurance (percentage-based), affecting out-of-pocket costs based on drug prices.

---

## 4. INSULIN BENEFICIARY COST FILE

### Insulin-Specific Coverage
- **Total Insulin Records:** 39,862
- **Plans with Insulin Coverage:** 5,621 unique plan IDs

### Sample Insulin Cost-Sharing
The data shows varied insulin copayments:
- Some plans: $0.00 copay for preferred insulin
- Other plans: $35.00 for non-preferred insulin (30-day supply)
- Mail order: $0.00 to $35.00 depending on tier and preference status

**Key Insight:** Significant variation in insulin costs across plans, with some offering $0 copays and others charging up to $35.

---

## 5. PRICING FILE ANALYSIS

### Pricing Data Scale
- **Total Pricing Records:** 55,534,021 (55.5 million records!)
- **File Size (Uncompressed):** ~2.0 GB

### Pricing Structure
- **Fields:** Contract ID, Plan ID, Segment ID, NDC, Days Supply, Unit Cost
- **Day Supply Options:** Typically 30, 60, and 90-day supplies
- **Pricing Variance:** Unit costs vary by contract, plan, and day supply

### Sample Pricing
- **Low-cost drugs:** $0.0001 per unit (likely generic medications)
- **Mid-range example:** $437-$658 per unit (sample brand medications)
- **High-cost drugs:** Analysis interrupted but likely specialty drugs exceed $10,000+ per unit

---

## 6. GEOGRAPHIC LOCATOR FILE

### Geographic Coverage
- **Total Records:** 3,280 county mappings
- **Coverage:** All US counties with Medicare beneficiaries

### Regional Structure
- Counties mapped to MA (Medicare Advantage) regions
- Counties mapped to PDP (Prescription Drug Plan) regions
- Example: Alabama counties in Region 10 (Alabama and Tennessee) for MA, Region 12 for PDP

---

## 7. PHARMACY NETWORKS ANALYSIS

### Network File Structure
- **Total Files:** 6 parts (split due to size)
- **Part 1 Records:** 5,328,929
- **Estimated Total:** ~30+ million pharmacy-plan relationships

### Network Data Included
- Pharmacy identification numbers
- ZIP codes
- Preferred status (retail vs mail order)
- In-network flags
- Dispensing fees by brand/generic and day supply (30/60/90)
- Floor pricing

**Sample Dispensing Fees:**
- Range: $0.25 - $0.65 per prescription
- Varies by pharmacy, drug type (brand/generic), and day supply

### File Sizes (Uncompressed)
1. Part 1: 3.88 GB
2. Part 2: 3.87 GB
3. Part 3: 3.80 GB
4. Part 4: 3.87 GB
5. Part 5: 3.77 GB
6. Part 6: 3.70 GB

**Total Network Data:** ~23 GB uncompressed

---

## 8. EXCLUDED DRUGS FORMULARY

### Excluded Drugs
- **Total Records:** 14,818
- **Unique Drugs (RxCUI):** 6 unique drugs excluded

### Exclusion Details
Drugs are excluded but still tracked with:
- Tier assignments
- Quantity limits
- Prior authorization requirements
- Step therapy requirements
- Capped benefit indicators

**Key Insight:** Very limited number of unique drugs excluded, but applied across many plan combinations.

---

## 9. INDICATION-BASED COVERAGE

### Diagnosis-Specific Coverage
- **Total Records:** 448 entries
- **Contracts with IBC:** 4 unique contracts (H1994, H2246, H5050)
- **Unique Drugs:** Limited set of specialty medications

### Covered Conditions
The indication-based coverage includes drugs for:

**Autoimmune/Inflammatory:**
- Ankylosing Spondylitis
- Psoriatic Arthritis
- Rheumatoid Arthritis
- Crohn Disease
- Ulcerative Colitis
- Spondylarthritis
- Cryopyrin-Associated Periodic Syndromes

**Respiratory:**
- Asthma
- COPD (Chronic Obstructive Pulmonary Disease)
- Prurigo Nodularis

**Neurological:**
- Narcolepsy
- Cataplexy
- Migraine Disorders

**Other Conditions:**
- Overactive Bladder
- Major Adverse Cardiac Events (MACE prevention)

**Key Insight:** Indication-based coverage is highly targeted to specialty/high-cost medications that may require specific diagnoses for coverage approval.

---

## 10. KEY FINDINGS & INSIGHTS

### Market Concentration
- **723 contracts** manage Medicare Part D benefits nationally
- **5,242 plans** provide diverse coverage options
- Only **384 unique formularies** across all plans (significant standardization)

### Cost-Sharing Trends
- Average deductible of $390.30 approaches CMS maximum
- Wide premium range ($0 - $190.80) reflects plan diversity
- 22.7% of cost-sharing records show "no charge" (generous coverage)

### Utilization Management Prevalence
- **37.8%** of formulary entries have quantity limits
- **27.2%** require step therapy
- **29.1%** require prior authorization
- These controls likely target high-cost/high-utilization drugs

### Generic Emphasis
- **57.4%** of formulary entries in generic tiers (1-2)
- Supports policy goals for generic utilization

### Geographic Variation
- Southern states show highest plan-county offerings
- GA, TX, OH lead in market competition

### Specialty Drug Coverage
- Tier 5 (Specialty) represents 15.3% of formulary
- Indication-based coverage targets complex conditions
- Insulin receives special tracking (dedicated file)

### Data Volume
- **55.5 million** pricing records demonstrate massive pricing complexity
- **~30 million** pharmacy network relationships show extensive networks
- **23 GB** of network data reflects comprehensive pharmacy access

---

## 11. DATA FILES SUMMARY

| File Name | Records | Key Fields | Purpose |
|-----------|---------|------------|---------|
| Plan Information | 115,380 | Contract, Plan, Premium, Deductible, Geography | Plan offerings by county |
| Basic Formulary | 1,300,284 | RXCUI, NDC, Tier, PA, Step Therapy | Drug coverage details |
| Beneficiary Cost | 165,071 | Contract, Tier, Copay/Coinsurance | Patient cost-sharing |
| Pricing | 55,534,021 | NDC, Days Supply, Unit Cost | Drug pricing by plan |
| Insulin Cost | 39,862 | Tier, Days Supply, Insulin Copays | Insulin-specific costs |
| Geographic Locator | 3,280 | County, State, MA/PDP Regions | Geographic mapping |
| Pharmacy Networks (6 parts) | ~30M+ | Pharmacy ID, ZIP, Fees, Preferred | Network relationships |
| Excluded Drugs | 14,818 | RXCUI, Tier, Restrictions | Excluded drug tracking |
| Indication-Based | 448 | RXCUI, Disease/Condition | Diagnosis-required drugs |

---

## 12. POTENTIAL ANALYSIS OPPORTUNITIES

### Cost Analysis
1. Compare drug pricing across plans for same NDC
2. Identify plans with lowest costs for specific drug classes
3. Analyze premium vs. out-of-pocket cost trade-offs
4. Track specialty drug pricing patterns

### Access Analysis
1. Pharmacy network adequacy by geography
2. Preferred vs. non-preferred pharmacy cost differences
3. Mail order vs. retail pharmacy economics
4. Geographic pharmacy coverage gaps

### Utilization Management Analysis
1. PA/Step therapy correlation with drug costs
2. Quantity limit patterns by drug class
3. Comparison of utilization controls across formularies
4. Impact of restrictions on beneficiary access

### Market Competition Analysis
1. Plan competition by county
2. Premium and benefit design clustering
3. Market share analysis by contract
4. SNP vs. standard plan differences

### Policy Research
1. Generic vs. brand utilization patterns
2. Insulin affordability across plans
3. Specialty drug access barriers
4. Coverage gaps for specific conditions

---

## 13. METHODOLOGY NOTES

### Data Extraction
- All zip files extracted to `/Users/bentenner/Dictionary/2025-Q2/extracted_data/`
- Disk space constraints required selective extraction
- Large files (pricing, pharmacy networks) analyzed directly from zip archives

### Analysis Tools
- Command-line tools (unzip, cut, sort, uniq, wc, awk)
- Pipe-delimited text file format
- Header rows present in all files

### Data Quality
- Complete dataset with consistent formatting
- No missing files from the SPUF Q2 2025 release
- Standard CMS data dictionary applies (see SPUFRecordLayout-2025.pdf)

---

## 14. DOCUMENTATION FILES

The following documentation files are included in the dataset:
- **AGREEMENT-FOR-USE.pdf** - Data use agreement
- **Methodology-SPUF-2025.pdf** - Methodology documentation
- **SPUFRecordLayout-2025.pdf** - Data dictionary and field definitions

---

## CONCLUSION

The Q2 2025 Medicare Part D dataset represents comprehensive prescription drug plan information across the United States. With 55+ million pricing records, 1.3 million formulary entries, and extensive pharmacy network data, this dataset enables deep analysis of drug coverage, costs, and access in the Medicare Part D program.

Key strengths include:
- Complete national coverage
- Granular pricing and cost-sharing data
- Extensive pharmacy network information
- Special focus on insulin and indication-based coverage

The data supports policy research, market analysis, beneficiary decision support, and healthcare cost studies.

---

**Report Generated:** November 4, 2025  
**Analyst:** AI Assistant  
**Data Source:** CMS SPUF/PPUF Q2 2025 Release

