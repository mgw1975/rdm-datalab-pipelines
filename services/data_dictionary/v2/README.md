# Data Dictionary Builder (FastAPI + Docker + OpenAPI)

A lightweight microservice that turns **baseline field definitions** + a **sample dataset** into a **profiled data dictionary**.

## Run locally
```bash
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
# Swagger UI: http://localhost:8000/docs
# ReDoc:      http://localhost:8000/redoc
```

## Docker
```bash
docker build -t data-dict-svc .
docker run -p 8000:80 data-dict-svc
# Swagger UI: http://localhost:8000/docs
```

## API
### POST /v1/dictionary:build
- Content-Type: multipart/form-data
- Fields:
  - `payload`: JSON string
  - `file`: CSV file

**Example payload (as a JSON string)**
```json
{
  "definitions": [
    {"name":"industry_code","description":"NAICS-based code (2–6 digits)","declared_type":"text","max_length":6},
    {"name":"own_code","description":"Ownership code (0,1,2,3,5,8)","declared_type":"integer","allowed_values":[0,1,2,3,5,8]}
  ],
  "options": {"top_k": 10, "enum_threshold": 60}
}
```

**cURL example**
```bash
curl -X POST "http://localhost:8000/v1/dictionary:build"   -F 'payload={
        "definitions":[
          {"name":"industry_code","description":"NAICS-based code (2–6 digits)","declared_type":"text","max_length":6},
          {"name":"own_code","description":"Ownership code (0,1,2,3,5,8)","declared_type":"integer","allowed_values":[0,1,2,3,5,8]}
        ],
        "options":{"top_k":10,"enum_threshold":60}
      }'   -F "file=@/path/to/your.csv"
```
