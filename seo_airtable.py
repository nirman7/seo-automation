# seo_automation.py

import csv
import time
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyairtable import Table

# Airtable credentials
AIRTABLE_PAT = "pat5u8TBUU0vyYUC1"
AIRTABLE_BASE_ID = "app8Ixrz9x6GdANaT"
AIRTABLE_TABLE_NAME = "SEO_Metrics"

API_URL = "https://keyword.searchatlas.com/api/v2/competitor-research/"
DETAIL_URL_TEMPLATE = "https://keyword.searchatlas.com/api/v2/competitor-research/{id}/"
JWT_TOKEN = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoic2xpZGluZyIsImV4cCI6MTc2MjYyNDMzMSwianRpIjoiZjgyODk2Yzg1MDU4NDQ1Njk0NGU4N2M5ZmUxNDYxZmYiLCJyZWZyZXNoX2V4cCI6MTc2MjYyNDMzMSwidXNlcl9pZCI6MjQ3MjU1LCJjdXN0b21lciI6eyJpZCI6NDM5LCJlbWFpbCI6Im5pcm1hbi5yYXNhZGl5YUBzZWFyY2hhdGxhcy5jb20iLCJ0ZWFtX2lkcyI6W10sImlzX3N1YnNjcmliZXIiOnRydWUsInF1b3RhIjp7fSwicGxhbiI6IlBSTyIsInNlYXRzIjo2MCwidGltZXpvbmUiOm51bGwsImlzX3doaXRlbGFiZWwiOmZhbHNlLCJ3aGl0ZWxhYmVsX2RvbWFpbiI6IiIsIndoaXRlbGFiZWxfb3R0byI6Ik9UVE8iLCJpc192ZW5kYXN0YV9jbGllbnQiOmZhbHNlLCJwaG9uZV9udW1iZXIiOiIrMTY0NjgyNDkwMjMiLCJjb21wYW55X25hbWUiOiJMaW5rZ3JhcGgiLCJsb2dvIjoiaHR0cHM6Ly9zdG9yYWdlLmdvb2dsZWFwaXMuY29tL2xpbmtncmFwaC1jdXN0b21lci1sb2dvLzEucG5nP1gtR29vZy1BbGdvcml0aG09R09PRzQtUlNBLVNIQTI1NiZYLUdvb2ctQ3JlZGVudGlhbD1nY3MtZnVsbC1hY2Nlc3MlNDBvcmdhbmljLXJ1bGVyLTIwNzEyMy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSUyRjIwMjUwNTEyJTJGYXV0byUyRnN0b3JhZ2UlMkZnb29nNF9yZXF1ZXN0JlgtR29vZy1EYXRlPTIwMjUwNTEyVDE3NTIxMVomWC1Hb29nLUV4cGlyZXM9ODY0MDAmWC1Hb29nLVNpZ25lZEhlYWRlcnM9aG9zdCZYLUdvb2ctU2lnbmF0dXJlPTNkNGNlYTA1YjQ0NTY4NzEzZTk3ZjVlYTg1MzQ0ZTVjZGUzZDJlYjgzNjE4NjI4MTE2ODNkMzU0MGRiYmJhMDIyMzY5MDE5ZGQ5MjE1ZWM2NTFlNDQxMzY4ZjUwZTMyM2IzNTMyNWExNzRkZTBhODc5NDM0NWI1YjM3OGI3MmRmYTI4Yzk4YmUyMDUxMTkwZGVkMWQwMmEzNmU0YWFjMDZlMWQyOWM0YTJjZjYzMjM5MjQ0NGRjNGIzNGIxMmM2ZGYyMTAwZGM1MTVkZThhYzU0ZGFkZDg4MDI3M2Y2ODlhMWM4MTkyZjc5MTQ5YmIzYmM0YmNlMDJlZGIzYmMyMTJmYWFlYTY0Njg1NmY2NTlmODNkMGVkNzM3N2YzYWE2NTM4M2IyZWFmOGZlMDg5ZTdjNzVmMTU5YzAxYjU3MTQwZDE1N2M2ZDExMmUyODg5OTdiMjhmNTc0NjRkNGYzYzUzMWQ0N2M5NDg0YTQzYmNkMmQ0MWJkNzYxMWQ5OGRlZjhkODk2MTZkNzU2OTM0ZDIzMzgzY2I2NjNkZmY3ZDUzMzNiYjBmZGQxODUxNjQ3OWYyMTNiNWJjNWY1ZjBkOWE2MjU4NThiN2EzOTIxOTAzZDNmODNmZTBiMDdhNTg5NzM5MDk4NjM0ZDFhMmY3ZjJhNmZiNjBmMWRmYWZjOWZjIiwic2hvdWxkX2NoaWxkcmVuX3JlY2VpdmVfZW1haWxzIjp0cnVlLCJyZWdpc3RyYXRpb25fc291cmNlIjpudWxsLCJhZGRvbl9pc19hY3RpdmUiOnRydWUsInNlYXJjaGF0bGFzX2FwaV9rZXkiOiJiYjBhYjY2YTdiMzZjNzhlM2Y3ZWMyZWM3NzdjNzQyOCJ9fQ.rqKygJ6RDG3ZJS2U67AdX_hFJtw6Cy1zUZW-Ge4l7Hk"  # Update with a valid token

CSV_INPUT = "domains.csv"
CSV_OUTPUT = "seo_metrics_batch.csv"

HEADERS = {
    "Authorization": JWT_TOKEN,
    "Content-Type": "application/json"
}

def get_month_diff_data(trend_data, months_back):
    if not trend_data:
        return None
    try:
        latest_date = max(datetime.strptime(x["date"], "%Y-%m-%d") for x in trend_data)
        target_date = latest_date - relativedelta(months=months_back)
        closest = min(trend_data, key=lambda x: abs(datetime.strptime(x["date"], "%Y-%m-%d") - target_date))
        return closest
    except Exception:
        return None

def compute_delta(current, past, key):
    if current and past:
        return current.get(key, 0) - past.get(key, 0)
    return "Insufficient Data"

# === Main automation ===
results = []
with open(CSV_INPUT, newline="") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        domain = row.get("domain", "").strip()
        if not domain or domain.lower() == "n/a":
            print(f"❌ Skipping invalid domain: {domain}")
            continue

        print(f"\n🚀 Submitting: {domain}")
        try:
            payload = {"url": domain, "mode": "domain", "country_code": "US"}
            response = requests.post(API_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            research_id = response.json().get("id")
            time.sleep(6)

            detail_url = DETAIL_URL_TEMPLATE.format(id=research_id)
            detail_response = requests.get(detail_url, headers=HEADERS)
            detail_response.raise_for_status()
            data = detail_response.json().get("data", {})
            competitor = data.get("competitor_research", {})

            trend = sorted(competitor.get("organic_trend", []), key=lambda x: x["date"])
            if not trend:
                raise ValueError("No organic trend data")

            latest = trend[-1]
            one_month = get_month_diff_data(trend, 1)
            three_months = get_month_diff_data(trend, 3)

            result = {
                "Domain": domain,
                "Traffic Δ 1m": compute_delta(latest, one_month, "organic_traffic"),
                "Traffic Δ 3m": compute_delta(latest, three_months, "organic_traffic"),
                "Organic Keywords": latest.get("organic_keywords", "N/A"),
                "Keyword Δ 1m": compute_delta(latest, one_month, "organic_keywords"),
                "Keyword Δ 3m": compute_delta(latest, three_months, "organic_keywords"),
                "Page 1 Keywords": latest.get("organic_keywords_top_3", "N/A"),
                "Page 1 Δ 1m": compute_delta(latest, one_month, "organic_keywords_top_3"),
                "Page 1 Δ 3m": compute_delta(latest, three_months, "organic_keywords_top_3"),
                "Domain Rating": data.get("domain_rating", "N/A"),
                "Domain Power": data.get("domain_power", "N/A"),
                "Trust Flow": competitor.get("trust_flow", "N/A"),
                "Citation Flow": competitor.get("citation_flow", "N/A"),
                "Paid Keywords": competitor.get("paid_keywords", "N/A"),
                "Date": datetime.now().strftime("%Y-%m-%d")
            }

            results.append(result)
            print(f"✅ Completed: {domain}")
        except Exception as e:
            print(f"❌ Error for {domain}: {e}")

# === Save to CSV ===
with open(CSV_OUTPUT, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

# === Upload to Airtable ===
table = Table(AIRTABLE_PAT, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
for record in results:
    try:
        table.create(record)
        print(f"☁️ Uploaded to Airtable: {record['Domain']}")
    except Exception as e:
        print(f"❌ Airtable upload failed for {record['Domain']}: {e}")
