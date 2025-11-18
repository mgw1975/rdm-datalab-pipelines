# RDM Datalab  
**Regional Data Metrics (RDM): A curated economic, labor, and environmental benchmarking portfolio**

---

## 🧭 Purpose of This Project

**RDM Datalab** is a data-engineering & analytics project that builds a unified, well-documented dataset for benchmarking U.S. counties and industries.  
The goal is to create high-quality, CFO-ready metrics and dashboards by integrating multiple public economic datasets into a standardized BigQuery environment.

This repository exists so that:

1. **Codex / AI coding assistants** have a clear description of the project’s structure, naming conventions, and data operations.  
2. Any contributor can understand the **domain**, **architecture**, and **pipeline logic** immediately.  
3. The project can scale as more datasets, models, or metrics are added.

---

## 📊 What RDM Datalab Ultimately Produces

RDM Datalab builds **benchmark-ready indicators** at:

- **county × NAICS2 × year** (core grain)  
- optionally: county-only, state-level, or NAICS-level aggregates

Examples of metrics:

- receipts per firm  
- wages per employee  
- payroll-to-receipts ratios  
- environmental intensity (emissions per employee/firm)  
- sector productivity comparisons  
- Smart Data Index (future)  

Dashboards are built in **Looker Studio, Tableau, and Python**.

---

## 🏗️ Current Technical Architecture

### **1. Programming Environment**
- **Python** for ingestion, cleaning, transformation  
- **Pandas / NumPy** for shaping data  
- **BigQuery** as the central warehouse  
- **Looker Studio** as primary visualization layer  
- **dbt (planned)** for model standardization  

### **2. Directory Structure (Recommended)**

```
RDM_Datalab/
│
├── data_raw/            # raw CSVs (ABS, QCEW, BEA, EPA TRI, GHGRP)
├── data_clean/          # cleaned datasets before BigQuery load
├── bigquery/
│   ├── ddl/             # table Create statements
│   ├── dml/             # load or merge SQL
│   └── views/           # analytical views
├── notebooks/
│   ├── ingestion/       # Jupyter/Colab ingestion notebooks
│   └── profiling/       # data profiling + QC
├── scripts/
│   ├── abs/             # ABS pipeline
│   ├── qcew/
│   ├── bea/
│   └── epa/
└── README.md            # this file
```

---

## 🧱 Data Sources Integrated (or in progress)

### **1. ABS — Annual Business Survey (Primary source completed)**
- Grain: **county × NAICS2 × year**  
- Firm-level measures  
- Units in **$1,000s → converted to dollars**

Standard fields now cleaned:
- abs_firms  
- abs_emp  
- abs_payroll_usd  
- abs_receipts_usd  
- derived: abs_wage_per_emp_usd, abs_receipts_per_firm_usd  

### **2. QCEW — Quarterly Census of Employment and Wages (Next focus)**
- Grain: **county × NAICS2 × year**  
- Establishment-level wages & employment  
- Raw units already in **dollars**  

Fields standardized:
- qcew_emp  
- qcew_wages_usd  
- qcew_avg_weekly_wage_usd  

### **3. BEA GDP (In progress)**
- County GDP by industry  
- Mapping issues identified between BEA LineCode → BEA sector  
- New crosswalk table created:  
  `xref_line_sector_bea`

### **4. EPA Environmental Signals (Planned)**
- TRI (Toxics Release Inventory)  
- GHGRP (Greenhouse Gas Reporting Program)  

Aggregation target:
- county × NAICS2 × year  
- total releases per county/sector  
- intensity metrics (lbs per employee or firm)

---

## 🧩 Standard Merge Model

All economic sources merge on:

```
state_fips  
county_fips  
naics2  
year
```

A unified merge scaffolding (Python) has been created and is already working for ABS + QCEW.

---

## 🚧 Immediate Next Steps

1. Pull QCEW 2022 CA county × NAICS2  
2. Run ABS + QCEW merge scaffolding  
3. Validate FIPS, NAICS mapping, units  
4. Outlier charts  
5. Start TRI aggregation  

---

## 📬 Contact

**Michael Walker**, Aptos CA  

---

## 🔧 **Bootstrap Codex Prompt**

Below is the canonical prompt for Codex or any AI assistant working inside this project.  
This ensures consistent behavior across all files and sessions.

---

# **Codex Bootstrap Prompt**

You are assisting in the **RDM Datalab Project**, a data engineering & analytics portfolio that integrates public economic datasets (ABS, QCEW, BEA GDP, EPA TRI/GHGRP) into a unified BigQuery warehouse.  
Your responsibilities:

1. Always follow **project naming conventions**:  
   - `ref_*` for lookup tables  
   - `xref_*` for mapping tables  
   - `portfolio_data.*` for cleaned, source-specific tables  
   - `econ_bnchmrk_*` for integrated benchmarking fact tables  
   - Always place the **data source last** in a table name  

2. All transformations must standardize data into the grain:  
   **county × NAICS2 × year**

3. When merging sources, NEVER sum metrics across sources (ABS vs QCEW vs BEA).  
   Always compute **ratios or benchmark metrics**, not additive totals.

4. Python code must use:  
   - pandas  
   - numpy  
   - consistent FIPS normalization (`zfill_series`)  
   - snake_case for all column names  

5. BigQuery SQL must:  
   - Use explicit data types  
   - Avoid mixed-type columns  
   - Use standard SQL, not legacy  
   - Follow naming patterns above  

6. Any generated files should be placed into the correct part of the repo (scripts, notebooks, ddl, dml, etc.).

7. Always optimize for clarity, reproducibility, and auditability.

Begin every task by referencing:  
- the grain (county × NAICS2 × year)  
- the data source  
- naming conventions  
- whether additional normalization or unit conversion is needed.

---

# End of Codex Bootstrap Prompt
