# save as: make_summary.py
import json, pandas as pd

d = json.load(open("dictionary_output.json"))
rows = []
for f in d["fields"]:
    s = f.get("stats", {})
    rows.append({
        "name": f["name"],
        "definition": f.get("definition",""),
        "declared_type": f.get("declared_type",""),
        "observed_type": f.get("observed_type",""),
        "distinct": s.get("distinct_count"),
        "missing_pct": s.get("missing_pct"),
        "min": s.get("min"), "p50": s.get("p50"), "max": s.get("max"),
        "examples": ", ".join(map(str, f.get("examples", [])[:3])),
        "notes": f.get("notes","")
    })
pd.DataFrame(rows).to_csv("dictionary_summary.csv", index=False)
print("Wrote dictionary_summary.csv")

