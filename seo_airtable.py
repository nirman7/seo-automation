import csv
import os
from datetime import datetime
from pyairtable import Table

# === HARDCODED Airtable credentials ===
AIRTABLE_PAT = "pat5u8TBUU0vyYUC1.062a0449934355a185a7faddc86dcc27a11937701d946c30309d4df5522ac830"
AIRTABLE_BASE_ID = "app8Ixrz9x6GdANaT"  # Extracted from the Airtable URL
AIRTABLE_TABLE_NAME = "SEO Metrics"

# === Load CSV file ===
CSV_FILE = "seo_metrics_batch.csv"

# === Setup Airtable table ===
table = Table(AIRTABLE_PAT, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

# === Create a unique field to group data ===
batch_run_date = datetime.now().strftime("%Y-%m-%d")

# === Upload each row to Airtable ===
with open(CSV_FILE, newline="") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        record = {
            "Domain": row.get("Domain"),
            "Traffic Δ 1m": row.get("Traffic Δ 1m"),
            "Traffic Δ 3m": row.get("Traffic Δ 3m"),
            "Organic Keywords": row.get("Organic Keywords"),
            "Keyword Δ 1m": row.get("Keyword Δ 1m"),
            "Keyword Δ 3m": row.get("Keyword Δ 3m"),
            "Page 1 Keywords": row.get("Page 1 Keywords"),
            "Page 1 Δ 1m": row.get("Page 1 Δ 1m"),
            "Page 1 Δ 3m": row.get("Page 1 Δ 3m"),
            "Domain Rating": row.get("Domain Rating"),
            "Domain Power": row.get("Domain Power"),
            "Trust Flow": row.get("Trust Flow"),
            "Citation Flow": row.get("Citation Flow"),
            "Paid Keywords": row.get("Paid Keywords"),
            "Date": batch_run_date
        }

        try:
            table.create(record)
            print(f"✅ Uploaded: {row.get('Domain')}")
        except Exception as e:
            print(f"❌ Error uploading {row.get('Domain')}: {e}")
