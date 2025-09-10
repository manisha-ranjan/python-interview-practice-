import sqlite3
import requests

# Connect to existing DB (no table creation here)
conn = sqlite3.connect("records.db")
cursor = conn.cursor()

# Function to check duplicate
def is_duplicate(batch_id, field):
    cursor.execute("SELECT 1 FROM records WHERE batch_id=? AND field=?", (batch_id, field))
    return cursor.fetchone() is not None

# Function to insert record if not duplicate
def insert_record(batch_id, field):
    if is_duplicate(batch_id, field):
        print(f"Duplicate found → {batch_id}, {field} (skipped)")
        return None
    cursor.execute("INSERT INTO records (batch_id, field) VALUES (?, ?)", (batch_id, field))
    conn.commit()
    print(f"Inserted → {batch_id}, {field}")
    return cursor.lastrowid

# Function to call API and update response_status
def call_api_and_update(record_id, batch_id, field):
    url = "https://httpbin.org/post"   # test API
    payload = {"batch_id": batch_id, "field": field}
    try:
        response = requests.post(url, json=payload)
        status = str(response.status_code)

        cursor.execute("UPDATE records SET response_status=? WHERE rowid=?", (status, record_id))
        conn.commit()
        print(f"Updated record {record_id} → Status {status}")
    except Exception as e:
        print("API call failed:", e)

# --------------------
# Example usage
# --------------------
data = [
    ("B001", "field1"),
    ("B002", "field3"),
    ("B001", "field1"),  # duplicate, will skip
    ("B001", "field1"),
]

for batch_id, field in data:
    record_id = insert_record(batch_id, field)
    if record_id:
        call_api_and_update(record_id, batch_id, field)

conn.close()