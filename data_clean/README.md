# data_clean/

Curated outputs (county × NAICS tables, merged benchmarking facts, QA reports) are
written here by the pipelines but **not committed**. Organize by source:

- `abs/` – ABS county/sector tables ready for loading to BigQuery.
- `qcew/` – QCEW county-sector summaries.
- `tri/` – TRI aggregation results and intensity metrics.
- `integration/` – merged ABS×QCEW×TRI scaffolding, benchmarking facts.

For reproducibility, store metadata (run date, command, parameters) alongside the
generated file or in `metadata/`. If you need to share a canonical CSV, publish it
via cloud storage and reference the location here instead of keeping the binary in git.
