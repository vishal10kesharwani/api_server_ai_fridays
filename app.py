from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import uuid
from typing import Dict, List, Union

app = FastAPI()

# ✅ CORS ENABLED
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ DATA DIRECTORY
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ✅ ALL DATASETS USED IN PROJECT
DATASETS = [
    "energy",
    "hvac",
    "environment",
    "weather",
    "tariff",
    "schedule",
    "maintenance",
    "zone",
    "opex",
    "audit"
]

# ✅ CREATE EMPTY JSON FILES SAFELY
for name in DATASETS:
    file_path = os.path.join(DATA_DIR, f"{name}.json")
    if not os.path.exists(file_path):
        with open(file_path, "w") as f:
            json.dump([], f)


# ✅ SAFE READ FUNCTION (FIXES YOUR ERROR)
def read_data(dataset: str) -> List[Dict]:
    file_path = os.path.join(DATA_DIR, f"{dataset}.json")

    if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
        return []

    with open(file_path, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []  # ✅ PREVENTS YOUR CRASH


# ✅ SAFE WRITE FUNCTION
def write_data(dataset: str, data: List[Dict]):
    file_path = os.path.join(DATA_DIR, f"{dataset}.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


# ✅ HEALTH CHECK
@app.get("/")
def home():
    return {"status": "AI Energy API Server Running with Local JSON Storage"}


# ✅ GENERIC CREATE (ADMIN, IOT)
@app.post("/api/{dataset}")
def create_record(dataset: str, payload: Union[Dict, List[Dict]]):
    if dataset not in DATASETS:
        raise HTTPException(status_code=404, detail="Invalid dataset")

    data = read_data(dataset)

    # ✅ BULK INSERT SUPPORT
    if isinstance(payload, list):
        new_items = []
        for item in payload:
            item["id"] = str(uuid.uuid4())
            new_items.append(item)
        data.extend(new_items)
        write_data(dataset, data)
        return {"inserted": len(new_items), "records": new_items}

    # ✅ SINGLE INSERT
    payload["id"] = str(uuid.uuid4())
    data.append(payload)
    write_data(dataset, data)
    return payload


# ✅ READ ALL (CLIENT, ADMIN, AI MODELS)
@app.get("/api/{dataset}")
def get_all_records(dataset: str):
    if dataset not in DATASETS:
        raise HTTPException(status_code=404, detail="Invalid dataset")
    return read_data(dataset)


# ✅ READ BY ID
@app.get("/api/{dataset}/{record_id}")
def get_record_by_id(dataset: str, record_id: str):
    if dataset not in DATASETS:
        raise HTTPException(status_code=404, detail="Invalid dataset")

    data = read_data(dataset)
    for item in data:
        if item.get("id") == record_id:
            return item

    raise HTTPException(status_code=404, detail="Record not found")


# ✅ DELETE (ADMIN ONLY IN REAL SYSTEM)
@app.delete("/api/{dataset}/{record_id}")
def delete_record(dataset: str, record_id: str):
    if dataset not in DATASETS:
        raise HTTPException(status_code=404, detail="Invalid dataset")

    data = read_data(dataset)
    new_data = [item for item in data if item.get("id") != record_id]

    if len(new_data) == len(data):
        raise HTTPException(status_code=404, detail="Record not found")

    write_data(dataset, new_data)
    return {"status": "deleted", "id": record_id}

