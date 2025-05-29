# seo_airtable.py  – v2  (keep file-name the same as in the GitHub Action)

import csv
import time
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyairtable import Table

# ---------------------------------------------------------------------------
# 1) CONSTANTS ───────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------
AIRTABLE_PAT        = "pat5u8TBUU0vyYUC1.062a0449934355a185a7faddc86dcc27a11937701d946c30309d4df5522ac830"
AIRTABLE_BASE_ID    = "app8Ixrz9x6GdANaT"
AIRTABLE_TABLE_NAME = "SEO_Metrics"

CSV_INPUT  = "domains.csv"          # first column must be called  domain
CSV_OUTPUT = "seo_metrics_batch.csv"

API_URL            = "https://keyword.searchatlas.com/api/v2/competitor-research/"
DETAIL_URL_TPL     = "https://keyword.searchatlas.com/api/v2/competitor-research/{id}/"
JWT_TOKEN          = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoic2xpZGluZyIsImV4cCI6MTc2MjYyNDMzMSwianRpIjoiZjgyODk2Yzg1MDU4NDQ1Njk0NGU4N2M5ZmUxNDYxZmYiLCJyZWZyZXNoX2V4cCI6MTc2MjYyNDMzMSwidXNlcl9pZCI6MjQ3MjU1LCJjdXN0b21lciI6eyJpZCI6NDM5LCJlbWFpbCI6Im5pcm1hbi5yYXNhZGl5YUBzZWFyY2hhdGxhcy5jb20iLCJ0ZWFtX2lkcyI6W10sImlzX3N1YnNjcmliZXIiOnRydWUsInF1b3RhIjp7fSwicGxhbiI6IlBSTyIsInNlYXRzIjo2MCwidGltZXpvbmUiOm51bGwsImlzX3doaXRlbGFiZWwiOmZhbHNlLCJ3aGl0ZWxhYmVsX2RvbWFpbiI6IiIsIndoaXRlbGFiZWxfb3R0byI6Ik9UVE8iLCJpc192ZW5kYXN0YV9jbGllbnQiOmZhbHNlLCJwaG9uZV9udW1iZXIiOiIrMTY0NjgyNDkwMjMiLCJjb21wYW55X25hbWUiOiJMaW5rZ3JhcGgiLCJsb2dvIjoiaHR0cHM6Ly9zdG9yYWdlLmdvb2dsZWFwaXMuY29tL2xpbmtncmFwaC1jdXN0b21lci1sb2dvLzEucG5nP1gtR29vZy1BbGdvcml0aG09R09PRzQtUlNBLVNIQTI1NiZYLUdvb2ctQ3JlZGVudGlhbD1nY3MtZnVsbC1hY2Nlc3MlNDBvcmdhbmljLXJ1bGVyLTIwNzEyMy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSUyRjIwMjUwNTEyJTJGYXV0byUyRnN0b3JhZ2UlMkZnb29nNF9yZXF1ZXN0JlgtR29vZy1EYXRlPTIwMjUwNTEyVDE3NTIxMVomWC1Hb29nLUV4cGlyZXM9ODY0MDAmWC1Hb29nLVNpZ25lZEhlYWRlcnM9aG9zdCZYLUdvb2ctU2lnbmF0dXJlPTNkNGNlYTA1YjQ0NTY4NzEzZTk3ZjVlYTg1MzQ0ZTVjZGUzZDJlYjgzNjE4NjI4MTE2ODNkMzU0MGRiYmJhMDIyMzY5MDE5ZGQ5MjE1ZWM2NTFlNDQxMzY4ZjUwZTMyM2IzNTMyNWExNzRkZTBhODc5NDM0NWI1YjM3OGI3MmRmYTI4Yzk4YmUyMDUxMTkwZGVkMWQwMmEzNmU0YWFjMDZlMWQyOWM0YTJjZjYzMjM5MjQ0NGRjNGIzNGIxMmM2ZGYyMTAwZGM1MTVkZThhYzU0ZGFkZDg4MDI3M2Y2ODlhMWM4MTkyZjc5MTQ5YmIzYmM0YmNlMDJlZGIzYmMyMTJmYWFlYTY0Njg1NmY2NTlmODNkMGVkNzM3N2YzYWE2NTM4M2IyZWFmOGZlMDg5ZTdjNzVmMTU5YzAxYjU3MTQwZDE1N2M2ZDExMmUyODg5OTdiMjhmNTc0NjRkNGYzYzUzMWQ0N2M5NDg0YTQzYmNkMmQ0MWJkNzYxMWQ5OGRlZjhkODk2MTZkNzU2OTM0ZDIzMzgzY2I2NjNkZmY3ZDUzMzNiYjBmZGQxODUxNjQ3OWYyMTNiNWJjNWY1ZjBkOWE2MjU4NThiN2EzOTIxOTAzZDNmODNmZTBiMDdhNTg5NzM5MDk4NjM0ZDFhMmY3ZjJhNmZiNjBmMWRmYWZjOWZjIiwic2hvdWxkX2NoaWxkcmVuX3JlY2VpdmVfZW1haWxzIjp0cnVlLCJyZWdpc3RyYXRpb25fc291cmNlIjpudWxsLCJhZGRvbl9pc19hY3RpdmUiOnRydWUsInNlYXJjaGF0bGFzX2FwaV9rZXkiOiJiYjBhYjY2YTdiMzZjNzhlM2Y3ZWMyZWM3NzdjNzQyOCJ9fQ.rqKygJ6RDG3ZJS2U67AdX_hFJtw6Cy1zUZW-Ge4l7Hk"
API_HEADERS        = {"Authorization": JWT_TOKEN, "Content-Type": "application/json"}

METRIC_COLUMNS = [
    "Domain", "Traffic Δ 1m", "Traffic Δ 3m", "Organic Keywords",
    "Keyword Δ 1m", "Keyword Δ 3m", "Page 1 Keywords",
    "Page 1 Δ 1m", "Page 1 Δ 3m", "% Change in Page 1 Keywords",
    "Traffic Efficiency", "Domain Rating", "Domain Power",
    "Trust Flow", "Citation Flow", "Paid Keywords", "Date"
]

# ---------------------------------------------------------------------------
# 2) HELPERS
# ---------------------------------------------------------------------------
def blank_record(domain: str, reason: str = "N/A") -> dict:
    rec = {col: reason for col in METRIC_COLUMNS}
    rec["Domain"] = domain
    rec["Date"]   = datetime.now().strftime("%Y-%m-%d")
    return rec

def month_delta(trend, months_back):
    if not trend:
        return None
    latest = max(datetime.strptime(pt["date"], "%Y-%m-%d") for pt in trend)
    target = latest - relativedelta(months=months_back)
    return min(trend, key=lambda pt: abs(datetime.strptime(pt["date"], "%Y-%m-%d") - target))

def delta(cur, past, key):
    return (cur.get(key, 0) - past.get(key, 0)) if (cur and past) else "Insufficient"

def safe_percent_change(delta_val, base_val):
    try:
        if isinstance(delta_val, (int, float)) and isinstance(base_val, (int, float)) and base_val != 0:
            return round((delta_val / base_val) * 100, 2)
    except:
        pass
    return "N/A"

def safe_ratio(numerator, denominator):
    try:
        if isinstance(numerator, (int, float)) and isinstance(denominator, (int, float)) and denominator != 0:
            return round(numerator / denominator, 2)
    except:
        pass
    return "N/A"

def fetch_metrics(domain: str) -> dict:
    payload = {"url": domain, "mode": "domain", "country_code": "US"}
    r = requests.post(API_URL, headers=API_HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    research_id = r.json()["id"]

    time.sleep(6)

    detail = requests.get(DETAIL_URL_TPL.format(id=research_id),
                          headers=API_HEADERS, timeout=60).json()
    data       = detail["data"]
    competitor = data.get("competitor_research", {})
    trend      = sorted(competitor.get("organic_trend", []), key=lambda x: x["date"])
    if not trend:
        raise ValueError("No organic_trend")

    latest   = trend[-1]
    one_mo   = month_delta(trend, 1)
    three_mo = month_delta(trend, 3)

    page1_now   = latest.get("organic_keywords_top_3")
    page1_delta = delta(latest, three_mo, "organic_keywords_top_3")
    page1_pct   = safe_percent_change(page1_delta, page1_now)

    traffic_delta = delta(latest, three_mo, "organic_traffic")
    keyword_delta = delta(latest, three_mo, "organic_keywords")
    traffic_eff   = safe_ratio(traffic_delta, keyword_delta)

    return {
        "Domain"                    : domain,
        "Traffic Δ 1m"              : delta(latest, one_mo,  "organic_traffic"),
        "Traffic Δ 3m"              : traffic_delta,
        "Organic Keywords"          : latest.get("organic_keywords", "N/A"),
        "Keyword Δ 1m"              : delta(latest, one_mo,  "organic_keywords"),
        "Keyword Δ 3m"              : keyword_delta,
        "Page 1 Keywords"           : page1_now,
        "Page 1 Δ 1m"               : delta(latest, one_mo,  "organic_keywords_top_3"),
        "Page 1 Δ 3m"               : page1_delta,
        "% Change in Page 1 Keywords": page1_pct,
        "Traffic Efficiency"        : traffic_eff,
        "Domain Rating"             : data.get("domain_rating",  "N/A"),
        "Domain Power"              : data.get("domain_power",   "N/A"),
        "Trust Flow"                : competitor.get("trust_flow",    "N/A"),
        "Citation Flow"             : competitor.get("citation_flow", "N/A"),
        "Paid Keywords"             : competitor.get("paid_keywords", "N/A"),
        "Date"                      : datetime.now().strftime("%Y-%m-%d")
    }

# ---------------------------------------------------------------------------
# 3) PROCESS INPUT
# ---------------------------------------------------------------------------
records = []

with open(CSV_INPUT, newline="") as src:
    reader = csv.DictReader(src)
    for row in reader:
        raw = (row.get("domain") or "").strip()
        lowered = raw.lower()

        if lowered in {"", "na", "n/a"}:
            print(f"⚠️  placeholder row for  '{raw}'")
            records.append(blank_record(raw))
            continue

        print(f"🚀 Submitting: {raw}")
        try:
            rec = fetch_metrics(lowered)
            rec["Domain"] = raw
            print(f"✅ Completed:  {raw}")
        except Exception as e:
            print(f"❌ Error for {raw}: {e}")
            rec = blank_record(raw, reason="Error")

        records.append(rec)

# ---------------------------------------------------------------------------
# 4) SAVE CSV OUTPUT
# ---------------------------------------------------------------------------
with open(CSV_OUTPUT, "w", newline="") as out:
    writer = csv.DictWriter(out, fieldnames=METRIC_COLUMNS)
    writer.writeheader()
    writer.writerows(records)

print(f"📄 Saved  {CSV_OUTPUT}")

# ---------------------------------------------------------------------------
# 5) UPLOAD TO AIRTABLE
# ---------------------------------------------------------------------------
table = Table(AIRTABLE_PAT, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

for rec in records:
    try:
        table.create(rec)
        print(f"☁️  Airtable row created for {rec['Domain']}")
    except Exception as e:
        print(f"❌ Airtable upload failed for {rec['Domain']}: {e}")
