import csv
import os
import time
from datetime import datetime
import requests
from dateutil.relativedelta import relativedelta
from pyairtable import Api

# === Airtable Config ===
AIRTABLE_PAT = os.getenv("pat5u8TBUU0vyYUC1.062a0449934355a185a7faddc86dcc27a11937701d946c30309d4df5522ac830")
AIRTABLE_BASE_ID = os.getenv("app8Ixrz9x6GdANaT")
AIRTABLE_TABLE_NAME = os.getenv("SEO Metrics")

api = Api(AIRTABLE_PAT)
table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

# === SearchAtlas API Config ===
API_URL = "https://keyword.searchatlas.com/api/v2/competitor-research/"
DETAIL_URL_TEMPLATE = "https://keyword.searchatlas.com/api/v2/competitor-research/{id}/"
JWT_TOKEN = os.getenv("Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoic2xpZGluZyIsImV4cCI6MTc2MjYyNDMzMSwianRpIjoiZjgyODk2Yzg1MDU4NDQ1Njk0NGU4N2M5ZmUxNDYxZmYiLCJyZWZyZXNoX2V4cCI6MTc2MjYyNDMzMSwidXNlcl9pZCI6MjQ3MjU1LCJjdXN0b21lciI6eyJpZCI6NDM5LCJlbWFpbCI6Im5pcm1hbi5yYXNhZGl5YUBzZWFyY2hhdGxhcy5jb20iLCJ0ZWFtX2lkcyI6W10sImlzX3N1YnNjcmliZXIiOnRydWUsInF1b3RhIjp7fSwicGxhbiI6IlBSTyIsInNlYXRzIjo2MCwidGltZXpvbmUiOm51bGwsImlzX3doaXRlbGFiZWwiOmZhbHNlLCJ3aGl0ZWxhYmVsX2RvbWFpbiI6IiIsIndoaXRlbGFiZWxfb3R0byI6Ik9UVE8iLCJpc192ZW5kYXN0YV9jbGllbnQiOmZhbHNlLCJwaG9uZV9udW1iZXIiOiIrMTY0NjgyNDkwMjMiLCJjb21wYW55X25hbWUiOiJMaW5rZ3JhcGgiLCJsb2dvIjoiaHR0cHM6Ly9zdG9yYWdlLmdvb2dsZWFwaXMuY29tL2xpbmtncmFwaC1jdXN0b21lci1sb2dvLzEucG5nP1gtR29vZy1BbGdvcml0aG09R09PRzQtUlNBLVNIQTI1NiZYLUdvb2ctQ3JlZGVudGlhbD1nY3MtZnVsbC1hY2Nlc3MlNDBvcmdhbmljLXJ1bGVyLTIwNzEyMy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSUyRjIwMjUwNTEyJTJGYXV0byUyRnN0b3JhZ2UlMkZnb29nNF9yZXF1ZXN0JlgtR29vZy1EYXRlPTIwMjUwNTEyVDE3NTIxMVomWC1Hb29nLUV4cGlyZXM9ODY0MDAmWC1Hb29nLVNpZ25lZEhlYWRlcnM9aG9zdCZYLUdvb2ctU2lnbmF0dXJlPTNkNGNlYTA1YjQ0NTY4NzEzZTk3ZjVlYTg1MzQ0ZTVjZGUzZDJlYjgzNjE4NjI4MTE2ODNkMzU0MGRiYmJhMDIyMzY5MDE5ZGQ5MjE1ZWM2NTFlNDQxMzY4ZjUwZTMyM2IzNTMyNWExNzRkZTBhODc5NDM0NWI1YjM3OGI3MmRmYTI4Yzk4YmUyMDUxMTkwZGVkMWQwMmEzNmU0YWFjMDZlMWQyOWM0YTJjZjYzMjM5MjQ0NGRjNGIzNGIxMmM2ZGYyMTAwZGM1MTVkZThhYzU0ZGFkZDg4MDI3M2Y2ODlhMWM4MTkyZjc5MTQ5YmIzYmM0YmNlMDJlZGIzYmMyMTJmYWFlYTY0Njg1NmY2NTlmODNkMGVkNzM3N2YzYWE2NTM4M2IyZWFmOGZlMDg5ZTdjNzVmMTU5YzAxYjU3MTQwZDE1N2M2ZDExMmUyODg5OTdiMjhmNTc0NjRkNGYzYzUzMWQ0N2M5NDg0YTQzYmNkMmQ0MWJkNzYxMWQ5OGRlZjhkODk2MTZkNzU2OTM0ZDIzMzgzY2I2NjNkZmY3ZDUzMzNiYjBmZGQxODUxNjQ3OWYyMTNiNWJjNWY1ZjBkOWE2MjU4NThiN2EzOTIxOTAzZDNmODNmZTBiMDdhNTg5NzM5MDk4NjM0ZDFhMmY3ZjJhNmZiNjBmMWRmYWZjOWZjIiwic2hvdWxkX2NoaWxkcmVuX3JlY2VpdmVfZW1haWxzIjp0cnVlLCJyZWdpc3RyYXRpb25fc291cmNlIjpudWxsLCJhZGRvbl9pc19hY3RpdmUiOnRydWUsInNlYXJjaGF0bGFzX2FwaV9rZXkiOiJiYjBhYjY2YTdiMzZjNzhlM2Y3ZWMyZWM3NzdjNzQyOCJ9fQ.rqKygJ6RDG3ZJS2U67AdX_hFJtw6Cy1zUZW-Ge4l7Hk")  # Keep this secure in GitHub secrets

headers = {
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

def process_domain(domain):
    if not domain or domain.strip().lower() == "n/a":
        return None

    print(f"🚀 Submitting: {domain}")
    payload = {
        "url": domain,
        "mode": "domain",
        "country_code": "US"
    }

    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        response.raise_for_status()
        research_id = response.json().get("id")
        if not research_id:
            raise ValueError("No research ID returned.")

        print("⏳ Waiting for processing...")
        time.sleep(6)

        detail_url = DETAIL_URL_TEMPLATE.format(id=research_id)
        detail_response = requests.get(detail_url, headers=headers)
        detail_response.raise_for_status()
        detail_data = detail_response.json()

        data = detail_data.get("data", {})
        competitor = data.get("competitor_research", {})

        trend = sorted(competitor.get("organic_trend", []), key=lambda x: x["date"])
        if not trend:
            print(f"❌ No trend data for {domain}")
            return None

        latest = trend[-1]
        one_month = get_month_diff_data(trend, 1)
        three_months = get_month_diff_data(trend, 3)

        return {
            "Domain": domain,
            "Traffic Δ 1m": compute_delta(latest, one_month, "organic_traffic"),
            "Traffic Δ 3m": compute_delta(latest, three_months, "organic_traffic"),
            "Organic Keywords (now)": latest.get("organic_keywords", "N/A"),
            "Keyword Δ 1m": compute_delta(latest, one_month, "organic_keywords"),
            "Keyword Δ 3m": compute_delta(latest, three_months, "organic_keywords"),
            "Page 1 Keywords (now)": latest.get("organic_keywords_top_3", "N/A"),
            "Page 1 Δ 1m": compute_delta(latest, one_month, "organic_keywords_top_3"),
            "Page 1 Δ 3m": compute_delta(latest, three_months, "organic_keywords_top_3"),
            "Domain Rating": data.get("domain_rating", "N/A"),
            "Domain Power": data.get("domain_power", "N/A"),
            "Trust Flow": competitor.get("trust_flow", data.get("trust_flow", "N/A")),
            "Citation Flow": competitor.get("citation_flow", data.get("citation_flow", "N/A")),
            "Paid Keywords": competitor.get("paid_keywords", "N/A"),
            "Date": datetime.now().strftime("%Y-%m-%d")
        }

    except Exception as e:
        print(f"❌ Error for {domain}: {e}")
        return None

# === MAIN ===
if __name__ == "__main__":
    with open("domains.csv", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        domains = [row["domain"] for row in reader if "domain" in row and row["domain"].strip()]

    for domain in domains:
        result = process_domain(domain)
        if result:
            table.create(result)
            print(f"✅ Uploaded: {domain}")
