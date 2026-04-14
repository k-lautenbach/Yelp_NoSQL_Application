"""
turnover_analysis.py
--------------------
Analyzes business turnover rate across Indianapolis neighborhoods using
the reviews and businesses collections in MongoDB.

Database : yelp_indy  (mongodb://localhost:27017/)
Output   : results/turnover_by_neighborhood.csv
           results/turnover_chart.png
"""

import os
import csv
from datetime import datetime, timezone
import matplotlib.pyplot as plt
from pymongo import MongoClient
from dateutil.relativedelta import relativedelta

MONGO_URI       = "mongodb://localhost:27017/"
DB_NAME         = "yelp_indy"
RESULTS_DIR     = "results"
DATASET_CUTOFF  = datetime(2022, 1, 1, tzinfo=timezone.utc)
CLOSED_MONTHS   = 18
NEW_AFTER_YEAR  = 2015
MIN_BUSINESSES  = 20
TOP_N           = 15

os.makedirs(RESULTS_DIR, exist_ok=True)

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
client.admin.command("ping")
print(f"Connected to MongoDB at {MONGO_URI}\n")

db = client[DB_NAME]
FOCUS_NEIGHBORHOODS = [
    "Butler-Tarkington/Rocky Ripple",
    "Fairgrounds",
    "Fountain Square",
    "Meridian Hills/Williams Creek",
    "Augusta / New Augusta",
    "Downtown",
    "Near Eastside",
    "Broad Ripple",
    "Near NW - Riverside"
]
# ── Step 1: Aggregate first/last review date per business ──

print("Aggregating first/last review dates per business...")

review_dates = db.reviews.aggregate([
    {
        "$group": {
            "_id": "$business_id",
            "first_review_date": {"$min": "$date"},
            "last_review_date":  {"$max": "$date"},
        }
    }
], allowDiskUse=True)

review_map = {}
for doc in review_dates:
    review_map[doc["_id"]] = {
        "first_review_date": doc["first_review_date"],
        "last_review_date":  doc["last_review_date"],
    }

print(f"  Found review history for {len(review_map):,} businesses.\n")

# ── Step 2: Pull business metadata ────────────────────────

print("Fetching business metadata...")

businesses = db.businesses.find(
    {
        "neighborhood": {"$exists": True, "$in": FOCUS_NEIGHBORHOODS}
    },
    {"business_id": 1, "neighborhood": 1, "categories": 1, "is_open": 1}
)
# ── Step 3: Classify each business ────────────────────────

closed_threshold = DATASET_CUTOFF - relativedelta(months=CLOSED_MONTHS)

def parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None

neighborhood_stats = {}

for biz in businesses:
    neighborhood = biz.get("neighborhood")
    if not neighborhood or not isinstance(neighborhood, str):
        continue

    biz_id  = biz["business_id"]
    is_open = biz.get("is_open", 1)
    dates   = review_map.get(biz_id, {})

    first_date = parse_date(dates.get("first_review_date"))
    last_date  = parse_date(dates.get("last_review_date"))

    is_closed = (is_open == 0) or (last_date is not None and last_date < closed_threshold)
    is_new    = first_date is not None and first_date.year > NEW_AFTER_YEAR

    if neighborhood not in neighborhood_stats:
        neighborhood_stats[neighborhood] = {"total": 0, "closed": 0, "new": 0}

    neighborhood_stats[neighborhood]["total"]  += 1
    neighborhood_stats[neighborhood]["closed"] += int(is_closed)
    neighborhood_stats[neighborhood]["new"]    += int(is_new)

print(f"  Processed businesses across {len(neighborhood_stats):,} neighborhoods.\n")

# ── Step 4: Calculate rates ────────────────────────────────

results = []
for neighborhood, stats in neighborhood_stats.items():
    total = stats["total"]
    if total < MIN_BUSINESSES:
        continue

    closed = stats["closed"]
    new    = stats["new"]

    results.append({
        "neighborhood":       neighborhood,
        "total_businesses":   total,
        "closed":             closed,
        "new":                new,
        "turnover_rate":      round(closed / total, 4),
        "new_business_rate":  round(new / total, 4),
    })

results.sort(key=lambda x: x["turnover_rate"], reverse=True)

# ── Global average turnover rate across all Indianapolis ──
total_all   = sum(r["total_businesses"] for r in results)
closed_all  = sum(r["closed"] for r in results)
new_all     = sum(r["new"] for r in results)
global_turnover     = round(closed_all / total_all, 4) if total_all else 0
global_new_rate     = round(new_all / total_all, 4) if total_all else 0

print(f"\nGlobal Indianapolis Turnover Rate: {global_turnover:.1%}")
print(f"Global Indianapolis New Business Rate: {global_new_rate:.1%}")

# ── Step 5: Print top 10 neighborhoods ────────────────────

print("Top 10 neighborhoods by turnover rate:")
print(f"{'Neighborhood':30}  {'Total':>6}  {'Closed':>7}  {'New':>5}  {'Turnover':>9}  {'New Rate':>9}")
print("-" * 72)
for row in results[:10]:
    print(
        f"{row['neighborhood']:30}  "
        f"{row['total_businesses']:>6,}  "
        f"{row['closed']:>7,}  "
        f"{row['new']:>5,}  "
        f"{row['turnover_rate']:>8.1%}  "
        f"{row['new_business_rate']:>8.1%}"
    )

# ── Step 6: Export CSV ─────────────────────────────────────

csv_path = os.path.join(RESULTS_DIR, "turnover_by_neighborhood.csv")
fieldnames = ["neighborhood", "total_businesses", "closed", "new",
              "turnover_rate", "new_business_rate"]

with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print(f"\nFull results exported to {csv_path}")

# ── Step 7: Bar chart — ordered by count, top N ───────────

top_by_count = sorted(results, key=lambda x: x["total_businesses"], reverse=True)[:TOP_N]

names              = [r["neighborhood"] for r in top_by_count]
turnover_rates     = [r["turnover_rate"] * 100 for r in top_by_count]
new_business_rates = [r["new_business_rate"] * 100 for r in top_by_count]
counts             = [r["total_businesses"] for r in top_by_count]

x     = range(len(names))
width = 0.4

fig, ax = plt.subplots(figsize=(16, 7))

bars1 = ax.bar([i - width/2 for i in x], turnover_rates,     width, label="Turnover Rate",     color="steelblue")
bars2 = ax.bar([i + width/2 for i in x], new_business_rates, width, label="New Business Rate", color="darkorange")

for bar in bars1:
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
            f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=7, color="steelblue")
for bar in bars2:
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
            f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=7, color="darkorange")

for i, count in enumerate(counts):
    ax.text(i, max(turnover_rates[i], new_business_rates[i]) + 4,
            f"n={count}", ha="center", va="bottom", fontsize=8, color="gray")

# Add global average lines
ax.axhline(y=global_turnover * 100, color="steelblue", linestyle="--",
           linewidth=1.5, alpha=0.7)
ax.text(len(names) - 0.5, global_turnover * 100 + 0.5,
        f"Indy avg: {global_turnover:.1%}",
        ha="right", va="bottom", fontsize=9, color="black",
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="none", alpha=0.8))
ax.axhline(y=global_new_rate * 100, color="darkorange", linestyle="--",
           linewidth=1.5, alpha=0.7)
ax.text(len(names) - 0.5, global_new_rate * 100 + 0.5,
        f"Indy avg: {global_new_rate:.1%}",
        ha="right", va="bottom", fontsize=9, color="black",
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="none", alpha=0.8))
ax.set_xticks(list(x))
ax.set_xticklabels(names, rotation=45, ha="right", fontsize=9)
ax.set_ylabel("Rate (%)", fontsize=12)
ax.set_ylim(0, max(turnover_rates + new_business_rates) * 1.25)
ax.set_title(f"Indianapolis Business Turnover vs. New Business Rate by Neighborhood (Top {TOP_N} by Count)", fontsize=13)
ax.legend()
plt.tight_layout()

chart_path = os.path.join(RESULTS_DIR, "turnover_chart.png")
plt.savefig(chart_path, dpi=150)
plt.close()
print(f"Bar chart saved to {chart_path}")

client.close()