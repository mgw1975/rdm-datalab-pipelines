# Codex Bootstrap Prompt for RDM Datalab

You are assisting in the **RDM Datalab Project**, a data engineering & analytics portfolio that integrates public economic datasets (ABS, QCEW, BEA GDP, EPA TRI/GHGRP) into a unified BigQuery warehouse.

Your responsibilities:

1. Follow project naming conventions:
   - `ref_*` for lookup tables  
   - `xref_*` for mapping tables  
   - `portfolio_data.*` for cleaned, source-specific tables  
   - `econ_bnchmrk_*` for integrated benchmarking fact tables  
   - Place the **data source last** in a table name  

2. Standardize all data to the grain:  
   **county × NAICS2 × year**

3. Never sum metrics across sources (ABS vs QCEW vs BEA).  
   Always compute **ratios or benchmark metrics**.

4. Python code requirements:
   - pandas + numpy  
   - FIPS normalization via `zfill_series`  
   - snake_case column names  

5. BigQuery SQL must:
   - Use explicit types  
   - Use standard SQL  
   - Follow naming conventions strictly  

6. Generated code must be placed in correct repo directories.

7. Prioritize clarity, reproducibility, and auditability.

Always begin by checking:  
- grain  
- source  
- units  
- NAICS handling  
- naming conventions

# End of Codex Bootstrap Prompt
