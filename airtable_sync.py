import os
import requests
from concurrent.futures import ThreadPoolExecutor

API_KEY = os.getenv("COMPOSIO_API_KEY")
headers = {"Authorization": f"Bearer {API_KEY}"}

BASE_ID = "appeeELOgYbxo6gya"
DESTINATION_TABLE = "Smart AI Prompt"

FIELD_MAP = {
    "Prompt Name": "Prompt Title",
    "Prompt": "Prompt",
    "Categories": "Prompt Category"
}

source_tables = [
    "Recraft", "AI Prompt Heaven", "Artistly", "Creative AI Prompt Library",
    "Creative Fabrica", "DALLE", "FreePik", "Google Gemini", "Ideogram",
    "Kittl", "MidJourney", "Leonardo", "piclumen", "Prompt Ai"
]

def sync_table(table_name):
    list_url = f"https://app.rube.app/api/v1/airtable/list_records"
    create_url = f"https://app.rube.app/api/v1/airtable/create_multiple_records"
    update_url = f"https://app.rube.app/api/v1/airtable/update_multiple_records"

    response = requests.post(list_url, json={
        "baseId": BASE_ID,
        "tableIdOrName": table_name,
        "filterByFormula": "NOT({Synced})",
        "fields": list(FIELD_MAP.keys())
    }, headers=headers)

    if not response.ok:
        return f"❌ {table_name}: Fetch failed"
    records = response.json().get("response_data", {}).get("records", [])
    if not records:
        return f"✅ {table_name}: No new records"

    to_create = []
    for r in records:
        source_fields = r.get('fields', {})
        mapped_fields = {
            dest: source_fields.get(src, "")
            for src, dest in FIELD_MAP.items()
            if src in source_fields
        }
        if mapped_fields:
            to_create.append({"fields": mapped_fields})
    record_ids = [r['id'] for r in records]

    create_resp = requests.post(create_url, json={
        "baseId": BASE_ID,
        "tableIdOrName": DESTINATION_TABLE,
        "records": to_create
    }, headers=headers)
    if not create_resp.ok:
        return f"❌ {table_name}: Creation failed"

    update_resp = requests.post(update_url, json={
        "baseId": BASE_ID,
        "tableIdOrName": table_name,
        "records": [{"id": rid, "fields": {"Synced": True}} for rid in record_ids]
    }, headers=headers)

    return f"✅ {table_name}: Synced {len(to_create)} records"

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=10) as ex:
        results = list(ex.map(sync_table, source_tables))
    print("\n".join(results))
