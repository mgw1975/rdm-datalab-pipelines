from fastapi import FastAPI, File, UploadFile, Body, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Tuple
import pandas as pd
import numpy as np
import io, json, re

app = FastAPI(
    title="Data Dictionary Builder",
    version="1.1.0",
    description="Build a profiled data dictionary, now with automatic definition discovery based on dataset context."
)

# ---------------- Models ----------------
class Constraint(BaseModel):
    pattern: Optional[str] = None
    min: Optional[float] = None
    max: Optional[float] = None
    allowed_values: Optional[List[Any]] = None

class FieldDef(BaseModel):
    name: str
    description: Optional[str] = None
    declared_type: Optional[str] = Field(None, description="integer | float | text | date | boolean")
    max_length: Optional[int] = None
    constraints: Optional[Constraint] = None
    allowed_values: Optional[List[Any]] = None

class DatasetContext(BaseModel):
    # Free-form hints to guide definition selection
    title: Optional[str] = None
    source: Optional[str] = None          # e.g., "BLS QCEW", "Census ABS"
    domain: Optional[str] = None          # e.g., "US labor", "US business"
    year: Optional[int] = None
    geography: Optional[str] = None       # e.g., "US", "US-CA", "County"
    keywords: Optional[List[str]] = None  # e.g., ["NAICS","FIPS","wages"]
    notes: Optional[str] = None

class Options(BaseModel):
    sample_rows: Optional[int] = 250000
    enum_threshold: Optional[int] = 60
    top_k: Optional[int] = 10
    quantiles: Optional[List[float]] = [0.05, 0.5, 0.95]
    outlier_method: Optional[str] = "iqr"
    pii_detection: Optional[bool] = False
    autofill_definitions: Optional[bool] = True     # NEW: try to find definitions
    web_mode: Optional[str] = Field("off", description="off | allow | require")  # placeholder

class BuildRequest(BaseModel):
    definitions: List[FieldDef] = Field(default_factory=list)
    data_url: Optional[str] = None
    options: Optional[Options] = Options()
    dataset_context: Optional[DatasetContext] = None

# ---------------- Preset vocabularies & heuristics ----------------
ABS_PRESET = {
    "NAME": "Geographic area name (e.g., county or state).",
    "GEO_ID": "Census geographic identifier (e.g., 0500000USssccc for counties).",
    "STATE": "State postal or FIPS code (context-dependent).",
    "state": "State FIPS code (2-digit).",
    "county": "County FIPS code (3-digit).",
    "NAICS2022": "2022 NAICS industry code (2–6 digits).",
    "NAICS2022_LABEL": "Industry title for NAICS2022 code.",
    "INDLEVEL": "Industry aggregation level indicator (e.g., 2-digit sector).",
    "FIRMPDEMP": "Count of firms with paid employees.",
    "EMP": "Number of paid employees.",
    "PAYANN": "Annual payroll (USD).",
    "RCPPDEMP": "Receipts or revenue per paid employees (context-specific ABS metric)."
}

QCEW_PRESET = {
    "area_fips": "Five-digit FIPS code identifying the geographic area (state/county).",
    "own_code": "Ownership code: 0=Total, 1=Private, 2=Federal, 3=State, 5=Local, 8=All Government.",
    "industry_code": "NAICS-based industry classification code (2–6 digits).",
    "agglvl_code": "Aggregation level code (geography/industry grouping).",
    "size_code": "Establishment size class code (0=All sizes).",
    "year": "Calendar year of data.",
    "qtr": "Quarter; 'A' indicates annual summary.",
    "disclosure_code": "Confidentiality flag: blank=published; 'N'=suppressed.",
    "annual_avg_estabs": "Annual average number of establishments (quarterly average).",
    "annual_avg_emplvl": "Annual average monthly employment level (12-month average).",
    "total_annual_wages": "Total wages paid (sum over four quarters), USD.",
    "taxable_annual_wages": "Wages subject to state unemployment insurance taxes, USD.",
    "annual_contributions": "Employer contributions to unemployment insurance, USD.",
    "annual_avg_wkly_wage": "Average weekly wage = total wages / avg employment / 52, USD.",
    "avg_annual_pay": "Average annual pay per employee, USD."
}

GENERIC_GUESSES = [
    (re.compile(r"^.*naics.*$", re.I), "NAICS industry classification code."),
    (re.compile(r"^.*fips.*$", re.I), "FIPS geographic code."),
    (re.compile(r"^geo[_\-]?id$", re.I), "Geographic identifier code (agency-specific format)."),
    (re.compile(r"^name$", re.I), "Human-readable name for the record (e.g., geography)."),
    (re.compile(r"^year$", re.I), "Calendar year."),
    (re.compile(r"^qtr$|^quarter$", re.I), "Quarter of year; 'A' indicates annual summary in some series."),
    (re.compile(r".*wage.*", re.I), "Wage or payroll amount (units likely USD)."),
    (re.compile(r".*empl.*", re.I), "Employment count (number of employees)."),
    (re.compile(r".*estab.*", re.I), "Establishment count (number of establishments).")
]

def guess_definition(col: str, ctx: Optional[DatasetContext]) -> Tuple[Optional[str], float, str]:
    """Return (definition, confidence, source) based on presets and heuristics."""
    # Prioritize context-specific presets
    if ctx and ctx.source:
        src = ctx.source.lower()
        if "qcew" in src and col in QCEW_PRESET:
            return QCEW_PRESET[col], 0.98, "preset:qcew"
        if ("census" in src or "abs" in src) and col in ABS_PRESET:
            return ABS_PRESET[col], 0.95, "preset:abs"

    # Try both presets if no explicit source
    if col in QCEW_PRESET:
        return QCEW_PRESET[col], 0.9, "preset:qcew"
    if col in ABS_PRESET:
        return ABS_PRESET[col], 0.9, "preset:abs"

    # Generic regex guesses
    for pat, desc in GENERIC_GUESSES:
        if pat.match(col):
            return desc, 0.6, "heuristic:regex"

    return None, 0.0, "none"

# ---------------- Core profiling ----------------
def is_numeric(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s)

def topk(s: pd.Series, k: int):
    vc = s.value_counts(dropna=True).head(k)
    denom = len(s.dropna())
    return [
        {"value": (None if pd.isna(idx) else idx), "freq": int(cnt), "pct": float(cnt/denom*100) if denom>0 else 0.0}
        for idx, cnt in vc.items()
    ]

def to_py(obj):
    if isinstance(obj, (np.integer,)): return int(obj)
    if isinstance(obj, (np.floating,)): return float(obj)
    if isinstance(obj, (np.bool_,)): return bool(obj)
    return obj

def profile_dataframe(df: pd.DataFrame, defs: Dict[str, dict], opts: dict, ctx: Optional[DatasetContext]):
    result = {
        "dataset_summary": {
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "memory_mb": round(float(df.memory_usage(deep=True).sum())/(1024**2), 2)
        },
        "fields": [],
        "warnings": [],
        "artifacts": {}
    }

    # Optional sampling
    if opts.get("sample_rows") and len(df) > opts["sample_rows"]:
        df = df.sample(opts["sample_rows"], random_state=42)

    # Auto-fill definitions if requested
    if opts.get("autofill_definitions", True):
        for col in df.columns:
            if col not in defs or not defs[col].get("description"):
                desc, conf, source = guess_definition(col, ctx)
                if desc:
                    defs[col] = {**defs.get(col, {"name": col}), "description": desc, "autofill_confidence": conf, "autofill_source": source}

    for col in df.columns:
        s = df[col]
        s_non_null = s.dropna()
        fdef = defs.get(col, {})
        observed_type = str(s.dtype)

        stats = {
            "count": int(s.shape[0]),
            "missing_count": int(s.isna().sum()),
            "missing_pct": round(float(s.isna().mean()*100), 3),
            "distinct_count": int(s.nunique(dropna=True)),
        }

        field_out = {
            "name": col,
            "definition": fdef.get("description",""),
            "declared_type": fdef.get("declared_type"),
            "observed_type": observed_type,
            "type_match": None if not fdef.get("declared_type") else (fdef["declared_type"].lower() in observed_type.lower()),
            "stats": stats,
            "examples": [to_py(e) for e in s_non_null.unique()[:10]],
            "top_values": topk(s, opts.get("top_k", 10)),
            "lengths": {},
            "validation": [],
            "notes": "",
            "autofill_confidence": fdef.get("autofill_confidence"),
            "autofill_source": fdef.get("autofill_source")
        }

        if is_numeric(s):
            try:
                q = s_non_null.quantile([0.05,0.5,0.95]).to_dict() if len(s_non_null) else {}
            except Exception:
                q = {}
            field_out["stats"].update({
                "min": to_py(s_non_null.min()) if len(s_non_null) else None,
                "p5": to_py(q.get(0.05)) if 0.05 in q else None,
                "p50": to_py(q.get(0.5)) if 0.5 in q else None,
                "p95": to_py(q.get(0.95)) if 0.95 in q else None,
                "max": to_py(s_non_null.max()) if len(s_non_null) else None,
                "mean": to_py(s_non_null.mean()) if len(s_non_null) else None,
                "std": to_py(s_non_null.std(ddof=1)) if len(s_non_null)>1 else None,
            })
        else:
            s_str = s_non_null.astype(str)
            field_out["lengths"] = {
                "max_len": int(s_str.map(len).max()) if not s_str.empty else 0,
                "avg_len": float(s_str.map(len).mean()) if not s_str.empty else 0.0
            }

        # Minimal validation support
        cons = fdef.get("constraints", {}) or {}
        if cons.get("min") is not None and is_numeric(s):
            field_out["validation"].append({"rule":"min","pass": bool((s_non_null >= cons["min"]).all()), "expected_min": cons["min"]})
        if cons.get("max") is not None and is_numeric(s):
            field_out["validation"].append({"rule":"max","pass": bool((s_non_null <= cons["max"]).all()), "expected_max": cons["max"]})
        allowed = fdef.get("allowed_values") or cons.get("allowed_values")
        if allowed is not None:
            allowed_set = set(allowed)
            unexpected = set(s_non_null.unique()) - allowed_set
            field_out["validation"].append({
                "rule":"allowed_values",
                "pass": len(unexpected)==0,
                "unexpected_values": [to_py(x) for x in list(unexpected)[:50]]
            })

        # Enum suggestion
        if stats["distinct_count"] <= opts.get("enum_threshold", 60):
            field_out["notes"] = "Enum candidate (low cardinality)"

        result["fields"].append(field_out)

    return result

# ---------------- API ----------------
@app.post("/v1/dictionary:build")
async def build_dictionary(payload: str = Body(None), file: UploadFile = File(None)):
    try:
        req = json.loads(payload) if payload else {}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in 'payload'")

    if not file:
        raise HTTPException(status_code=400, detail="No file provided")

    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to read CSV: {e}")

    defs = {d["name"]: d for d in req.get("definitions", [])}
    opts = req.get("options", {}) or {}
    ctx = req.get("dataset_context")

    # Convert ctx to DatasetContext for downstream convenience
    ctx_obj = None
    if ctx:
        try:
            ctx_obj = DatasetContext(**ctx)
        except Exception:
            ctx_obj = None

    result = profile_dataframe(df, defs, opts, ctx_obj)
    return result
