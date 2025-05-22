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
JWT_TOKEN          = "Bearer <YOUR-VALID-SA-JWT>"
API_HEADERS        = {"Authorization": JWT_TOKEN, "Content-Type": "application/json"}

METRIC_COLUMNS = [
    "Domain", "Traffic Δ 1m", "Traffic Δ 3m", "Organic Keywords",
    "Keyword Δ 1m", "Keyword Δ 3m", "Page 1 Keywords",
    "Page 1 Δ 1m", "Page 1 Δ 3m", "Domain Rating", "Domain Power",
    "Trust Flow", "Citation Flow", "Paid Keywords", "Date"
]

# ---------------------------------------------------------------------------
# 2) SMALL HELPERS ───────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------
def blank_record(domain: str, reason: str = "N/A") -> dict:
    """Return a stub dict with every metric filled with `reason`."""
    rec = {col: reason for col in METRIC_COLUMNS}
    rec["Domain"] = domain
    rec["Date"]   = datetime.now().strftime("%Y-%m-%d")
    return rec


def month_delta(trend, months_back):
    """Pick the trend-point closest to now-minus-N-months."""
    if not trend:
        return None
    latest = max(datetime.strptime(pt["date"], "%Y-%m-%d") for pt in trend)
    target = latest - relativedelta(months=months_back)
    return min(trend, key=lambda pt: abs(datetime.strptime(pt["date"], "%Y-%m-%d") - target))


def delta(cur, past, key):
    return (cur.get(key, 0) - past.get(key, 0)) if (cur and past) else "Insufficient"


def fetch_metrics(domain: str) -> dict:
    """Full round-trip to SearchAtlas – may raise exceptions."""
    payload = {"url": domain, "mode": "domain", "country_code": "US"}
    r = requests.post(API_URL, headers=API_HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    research_id = r.json()["id"]

    time.sleep(6)   # SearchAtlas processing delay

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

    return {
        "Domain"           : domain,
        "Traffic Δ 1m"     : delta(latest, one_mo,  "organic_traffic"),
        "Traffic Δ 3m"     : delta(latest, three_mo,"organic_traffic"),
        "Organic Keywords" : latest.get("organic_keywords", "N/A"),
        "Keyword Δ 1m"     : delta(latest, one_mo,  "organic_keywords"),
        "Keyword Δ 3m"     : delta(latest, three_mo,"organic_keywords"),
        "Page 1 Keywords"  : latest.get("organic_keywords_top_3", "N/A"),
        "Page 1 Δ 1m"      : delta(latest, one_mo,  "organic_keywords_top_3"),
        "Page 1 Δ 3m"      : delta(latest, three_mo,"organic_keywords_top_3"),
        "Domain Rating"    : data.get("domain_rating",  "N/A"),
        "Domain Power"     : data.get("domain_power",   "N/A"),
        "Trust Flow"       : competitor.get("trust_flow",    "N/A"),
        "Citation Flow"    : competitor.get("citation_flow", "N/A"),
        "Paid Keywords"    : competitor.get("paid_keywords", "N/A"),
        "Date"             : datetime.now().strftime("%Y-%m-%d")
    }

# ---------------------------------------------------------------------------
# 3) BUILD THE CSV – always one row per input line ───────────────────────────
# ---------------------------------------------------------------------------
records = []

with open(CSV_INPUT, newline="") as src:
    reader = csv.DictReader(src)
    for row in reader:
        raw = (row.get("domain") or "").strip()
        lowered = raw.lower()

        # empty / NA rows → placeholder
        if lowered in {"", "na", "n/a"}:
            print(f"⚠️  placeholder row for  '{raw}'")
            records.append(blank_record(raw))
            continue

        print(f"🚀 Submitting: {raw}")
        try:
            rec = fetch_metrics(lowered)
            # keep the original casing/spaces from the CSV
            rec["Domain"] = raw
            print(f"✅ Completed:  {raw}")
        except Exception as e:
            print(f"❌ Error for {raw}: {e}")
            rec = blank_record(raw, reason="Error")

        records.append(rec)

# write out the batch CSV
with open(CSV_OUTPUT, "w", newline="") as out:
    writer = csv.DictWriter(out, fieldnames=METRIC_COLUMNS)
    writer.writeheader()
    writer.writerows(records)

print(f"📄 Saved  {CSV_OUTPUT}")

# ---------------------------------------------------------------------------
# 4) PUSH EVERY ROW TO AIRTABLE ──────────────────────────────────────────────
# ---------------------------------------------------------------------------
table = Table(AIRTABLE_PAT, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

for rec in records:
    try:
        table.create(rec)
        print(f"☁️  Airtable row created for {rec['Domain']}")
    except Exception as e:
        print(f"❌ Airtable upload failed for {rec['Domain']}: {e}")
