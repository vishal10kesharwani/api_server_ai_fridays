import os
import requests
import json
import time
import urllib3

# ============================
# ✅ DISABLE SSL WARNINGS
# ============================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================
# ✅ CONFIG
# ============================

LOCAL_API_SERVER = "http://localhost:8000"

LLM_BASE_URL = "https://genailab.tcs.in/v1/chat/completions"
LLM_MODEL = "azure_ai/genailab-maas-DeepSeek-V3-0324"
LLM_API_KEY = "sk-95rCU7cwmfkcd-vXv-GpUQ"
if not LLM_API_KEY:
    raise Exception("❌ SET GENAI_API_KEY IN ENVIRONMENT")

HEADERS = {
    "Authorization": f"Bearer {LLM_API_KEY}",
    "Content-Type": "application/json"
}

ROWS_PER_DATASET = 10
INTERVAL_SECONDS = 300     # ✅ AUTO RUN EVERY 5 SECONDS


# ============================
# ✅ DATASET PROMPTS
# ============================

DATASET_PROMPTS = {
    "energy": "Generate {n} energy records (timestamp, meter_id, instantaneous_power_kw, cumulative_energy, power_factor)",
    "hvac": "Generate {n} hvac telemetry records (timestamp, hvac_id, supply_air_temp, return_air_temp, fan_speed, compressor_status, vibration_level, filter_pressure_diff, EER)",
    "environment": "Generate {n} indoor environment records (timestamp, sensor_id, zone_id, indoor_temp, indoor_humidity, occupancy_count, occupancy_binary)",
    "weather": "Generate {n} weather records (timestamp, outdoor_temp, wet_bulb_temp, outdoor_humidity, solar_radiation, weather_condition)",
    "tariff": "Generate {n} tariff records (tariff_id, start_time, end_time, tariff_rate, demand_charge, tariff_type)",
    "schedule": "Generate {n} operational schedule records (schedule_id, asset_id, zone_id, start_time, stop_time, setpoint_temp, is_holiday)",
    "maintenance": "Generate {n} maintenance records (maintenance_id, asset_id, fault_code, downtime_minutes, replaced_component, technician_notes, timestamp)",
    "zone": "Generate {n} zone records (zone_id, zone_square_footage, zone_function, zone_orientation, thermal_mass_index)",
    "opex": "Generate {n} opex records (expense_id, expense_type, related_asset_id, cost_amount, cost_timestamp)",
    "audit": "Generate {n} audit log records (audit_log_timestamp, user_id, user_role, action_type, is_sensitive_data_flag)"
}


# ============================
# ✅ LLM CALL (SSL BYPASS)
# ============================

def generate_synthetic_data(dataset, n):
    prompt = DATASET_PROMPTS[dataset].format(n=n)

    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "Return ONLY valid JSON array."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.5
    }

    res = requests.post(
        LLM_BASE_URL,
        headers=HEADERS,
        json=payload,
        verify=False   # ✅ SSL BYPASS
    )

    if res.status_code != 200:
        raise Exception(f"LLM Error: {res.text}")

    raw = res.json()["choices"][0]["message"]["content"]

    try:
        return json.loads(raw)
    except:
        start = raw.find("[")
        end = raw.rfind("]") + 1
        return json.loads(raw[start:end])


# ============================
# ✅ UPLOAD TO API (SSL BYPASS)
# ============================

def upload_to_api(dataset, data):
    url = f"{LOCAL_API_SERVER}/api/{dataset}"

    res = requests.post(
        url,
        json=data,
        verify=False   # ✅ SSL BYPASS
    )

    if res.status_code != 200:
        print(f"❌ FAILED → {dataset}: {res.text}")
    else:
        print(f"✅ {dataset.upper()} → {len(data)} records uploaded")


# ============================
# ✅ AUTO LOOP ENGINE
# ============================

def auto_stream():
    print("\n🚀 REAL-TIME LLM → API DATA STREAM STARTED\n")

    while True:
        try:
            for dataset in DATASET_PROMPTS:
                print(f"\n🔁 Generating {ROWS_PER_DATASET} rows for {dataset}")
                data = generate_synthetic_data(dataset, ROWS_PER_DATASET)
                upload_to_api(dataset, data)
                time.sleep(1)

            print(f"\n⏳ Sleeping {INTERVAL_SECONDS} seconds...\n")
            time.sleep(INTERVAL_SECONDS)

        except Exception as e:
            print(f"\n⚠️ ERROR: {e}")
            time.sleep(10)


# ============================
# ✅ ENTRY POINT
# ============================

if __name__ == "__main__":
    auto_stream()
