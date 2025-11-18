# Data Dictionary Builder v1.1 (Auto-Definition)

Now with **automatic definition discovery** based on dataset context.

## What's new
- `dataset_context` (title, source, domain, year, geography, keywords, notes)
- `options.autofill_definitions` (default: true)
- Auto-fills definitions via:
  1) Source-specific presets (Census ABS, BLS QCEW)
  2) Heuristic regex guesses (NAICS, FIPS, GEO_ID, wages, employment, etc.)
  3) (placeholder) web_mode for future online enrichment

## Run
```bash
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
# or Docker:
# docker build -t data-dict-svc .
# docker run -p 8000:80 data-dict-svc
```

## Request shape
**POST /v1/dictionary:build** (multipart/form-data)

- `payload`: JSON string with `definitions` (optional), `options`, `dataset_context`
- `file`: CSV file

### Example payload
```json
{
  "definitions": [],
  "options": {
    "top_k": 10,
    "enum_threshold": 60,
    "autofill_definitions": true
  },
  "dataset_context": {
    "title": "ABS 2022 - CA Counties by NAICS2",
    "source": "Census ABS",
    "domain": "US business",
    "year": 2022,
    "geography": "County",
    "keywords": ["NAICS", "FIPS", "payroll"]
  }
}
```

This will auto-fill definitions for common ABS field names (e.g., NAME, GEO_ID, NAICS2022, PAYANN, EMP, etc.), and use heuristics for unknowns.

### Notes
- If you provide `definitions` for some columns, those win; other columns get auto-filled if possible.
- The response includes `autofill_confidence` and `autofill_source` per field so you can review/override.
- For internet lookups, you can extend `guess_definition` to call official APIs.
```python
# Example stub in guess_definition():
# if opts.web_mode == "allow": fetch metadata from the dataset's official API
```

