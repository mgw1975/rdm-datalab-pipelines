# data_raw/

Raw source files live here but are **not versioned**. Each subfolder mirrors an external
system so it is obvious where to drop downloads before running the pipelines.

| Subfolder                | Contents / Notes |
|--------------------------|------------------|
| `abs/`                   | API pulls or FTP extracts from Census ABS (e.g., `ABS_2022_CA_allcounties_NAICS2_from_API.csv`). |
| `bea/`                   | BEA crosswalks and GDP source tables (`bea_cagdp2_*`, line code lookups). |
| `qcew/`                  | BLS QCEW single-file extracts plus any sampled subsets. |
| `naics/`                 | Official NAICS reference workbooks/CSVs. |
| `reference/`             | Shared crosswalks (CBSA, BEA↔NAICS, etc.). |
| `us_series/`             | USCODE/County Business Patterns text dumps; often hundreds of MB each. |
| `external/simplemaps/`   | Third-party geography lookup packages. |

For each dataset, capture provenance (URL, vintage, checksum) in the nearest README
or in `docs/` so anyone can re-download. If you need a tiny sample for unit tests,
create a `*_sample.csv` and commit it under `data_raw/samples/`.
