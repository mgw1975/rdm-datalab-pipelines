from fastapi import FastAPI, File, UploadFile, Body, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
import io, json

app = FastAPI(
    title="Data Dictionary Builder",
    version="1.0.0",
    description="Build a profiled data dictionary by merging baseline field definitions with observed dataset profiling."
)

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

class Options(BaseModel):
    sample_rows: Optional[int] = 250000
    enum_threshold: Optional[int] = 60
    top_k: Optional[int] = 10
    quantiles: Optional[List[float]] = [0.05, 0.5, 0.95]
    outlier_method: Optional[str] = "iqr"
    pii_detection: Optional[bool] = False

class BuildRequest(BaseModel):
    definitions: List[FieldDef] = Field(default_factory=list)
    data_url: Optional[str] = None
    options: Optional[Options] = Options()

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

def profile_dataframe(df: pd.DataFrame, defs: Dict[str, dict], opts: dict):
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

    if opts.get("sample_rows") and len(df) > opts["sample_rows"]:
        df = df.sample(opts["sample_rows"], random_state=42)

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
            "notes": ""
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

        if stats["distinct_count"] <= opts.get("enum_threshold", 60):
            field_out["notes"] = "Enum candidate (low cardinality)"

        result["fields"].append(field_out)

    return result

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
    opts = req.get("options", {})

    result = profile_dataframe(df, defs, opts)
    return result
