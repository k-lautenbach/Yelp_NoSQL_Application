"""
price_tier_analysis.py
----------------------
Tracks price tier composition per neighborhood over time using
first review year as a business "entry" timestamp.

Database : yelp_indy  (mongodb://localhost:27017/)
Output   : results/price_tier_by_neighborhood.csv
           results/price_tier_over_time.png
           results/price_tier_heatmap.png
"""

import os
import csv
from collections import defaultdict
import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient

MONGO_URI       = "mongodb://localhost:27017/"
DB_NAME         = "yelp_indy"
RESULTS_DIR     = "results"
MIN_BUSINESSES  = 20
MIN_PER_YEAR    = 5
FOCUS_NEIGHBORHOODS = [
    "Butler-Tarkington/Rocky Ripple",
    "Fairgrounds",
    "Fountain Square",
    "Augusta / New Augusta",
    "Downtown",
    "Near Eastside",
    "Broad Ripple",
    "Near NW - Riverside"
]

os.makedirs(RESULTS_DIR, exist_ok=True)

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
client.admin.command("ping")
print(f"Connected to MongoDB at {MONGO_URI}\n")

db = client[DB_NAME]

# ── Step 1: Get first review year per business ─────────────

print("Aggregating first review year per business...")

first_reviews = db.reviews.aggregate([
    {"$group": {
        "_id": "$business_id",
        "first_review_date": {"$min": "$date"}
    }}
], allowDiskUse=True)

def parse_year(s):
    if not s:
        return None
    try:
        return int(s[:4])
    except:
        return None

first_year_map = {}
for doc in first_reviews:
    year = parse_year(doc["first_review_date"])
    if year:
        first_year_map[doc["_id"]] = year

print(f"  Found first review year for {len(first_year_map):,} businesses.\n")

# ── Step 2: Pull businesses with price + neighborhood ──────

print("Fetching businesses...")

businesses = db.businesses.find(
    {
        "neighborhood": {"$exists": True},
        "attributes.RestaurantsPriceRange2": {"$exists": True}
    },
    {"business_id": 1, "neighborhood": 1, "attributes.RestaurantsPriceRange2": 1}
)

# ── Step 3: Build neighborhood → year → price tier counts ──

stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

skipped = 0
for biz in businesses:
    neighborhood = biz.get("neighborhood")
    if not neighborhood or not isinstance(neighborhood, str):
        continue

    biz_id = biz["business_id"]
    year   = first_year_map.get(biz_id)
    if not year or year < 2005 or year > 2021:
        continue

    raw_price = biz.get("attributes", {}).get("RestaurantsPriceRange2")
    try:
        price = int(float(str(raw_price)))
        if price not in [1, 2, 3, 4]:
            skipped += 1
            continue
    except:
        skipped += 1
        continue

    stats[neighborhood][year][price] += 1

print(f"  Built price tier data for {len(stats):,} neighborhoods. ({skipped} records skipped)\n")

# ── Step 4: Calculate upscale ratio per neighborhood/year ──

results = []
for neighborhood, year_data in stats.items():
    total_all_years = sum(sum(tc.values()) for tc in year_data.values())
    if total_all_years < MIN_BUSINESSES:
        continue

    for year, tier_counts in sorted(year_data.items()):
        total   = sum(tier_counts.values())
        upscale = tier_counts[2] + tier_counts[3] + tier_counts[4]
        results.append({
            "neighborhood":  neighborhood,
            "year":          year,
            "total":         total,
            "tier_1":        tier_counts[1],
            "tier_2":        tier_counts[2],
            "tier_3":        tier_counts[3],
            "tier_4":        tier_counts[4],
            "upscale_ratio": round(upscale / total, 4) if total else 0,
        })

# ── Global average upscale ratio across all Indianapolis ───

print("Computing global Indianapolis price tier average...")

global_by_year = defaultdict(lambda: {"total": 0, "upscale": 0})

for r in results:
    if r["total"] >= MIN_PER_YEAR:
        global_by_year[r["year"]]["total"]   += r["total"]
        global_by_year[r["year"]]["upscale"] += r["tier_2"] + r["tier_3"] + r["tier_4"]

global_years  = sorted(global_by_year.keys())
global_ratios = [
    global_by_year[y]["upscale"] / global_by_year[y]["total"]
    if global_by_year[y]["total"] > 0 else 0
    for y in global_years
]

df_global = pd.DataFrame({"year": global_years, "ratio": global_ratios}).set_index("year")
df_global["smoothed"] = df_global["ratio"].rolling(3, min_periods=1).mean()

# ── Step 5: Export CSV ─────────────────────────────────────

csv_path = os.path.join(RESULTS_DIR, "price_tier_by_neighborhood.csv")
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["neighborhood", "year", "total",
                                            "tier_1", "tier_2", "tier_3",
                                            "tier_4", "upscale_ratio"])
    writer.writeheader()
    writer.writerows(results)

print(f"CSV exported to {csv_path}")

# ── Step 6: Smoothed line chart ────────────────────────────

print("Generating smoothed line chart...")

fig, ax = plt.subplots(figsize=(14, 7))
colors = plt.cm.tab10.colors

for i, neighborhood in enumerate(FOCUS_NEIGHBORHOODS):
    neighborhood_data = sorted(
        [r for r in results if r["neighborhood"] == neighborhood and r["total"] >= MIN_PER_YEAR],
        key=lambda x: x["year"]
    )
    if not neighborhood_data:
        print(f"  Warning: no data for {neighborhood}")
        continue

    df = pd.DataFrame(neighborhood_data)
    df["smoothed"] = df["upscale_ratio"].rolling(3, min_periods=1).mean()

    ax.plot(df["year"], df["smoothed"] * 100,
            marker="o", linewidth=2,
            label=neighborhood,
            color=colors[i % len(colors)])

# Global Indianapolis average line
ax.plot(df_global.index, df_global["smoothed"] * 100,
        color="black", linewidth=2.5, linestyle="--",
        label="Indianapolis avg", zorder=5)

ax.axvline(x=2015, color="gray", linestyle="--", linewidth=1, alpha=0.7)
ax.text(2015.1, 95, "2015", color="gray", fontsize=9)

ax.set_xlabel("Year (first review = business entry)", fontsize=12)
ax.set_ylabel("Upscale Business Ratio (price tier 2+ as % of new businesses)", fontsize=11)
ax.set_title("Price Tier Creep Over Time by Indianapolis Neighborhood (3-Year Rolling Avg)", fontsize=13)
ax.legend(loc="upper left")
ax.set_xlim(2005, 2021)
ax.set_ylim(0, 100)
ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
chart_path = os.path.join(RESULTS_DIR, "price_tier_over_time.png")
plt.savefig(chart_path, dpi=150)
plt.close()
print(f"Line chart saved to {chart_path}")

# ── Step 7: Heatmap — overall upscale ratio per neighborhood

print("Generating heatmap...")

summary = defaultdict(lambda: {"total": 0, "upscale": 0})
for r in results:
    summary[r["neighborhood"]]["total"]   += r["total"]
    summary[r["neighborhood"]]["upscale"] += r["tier_2"] + r["tier_3"] + r["tier_4"]

summary_list = [
    {"neighborhood": n, "upscale_ratio": v["upscale"] / v["total"]}
    for n, v in summary.items() if v["total"] >= MIN_BUSINESSES
]
summary_list.sort(key=lambda x: x["upscale_ratio"], reverse=True)
top20 = summary_list[:20]

names  = [r["neighborhood"] for r in top20]
ratios = [r["upscale_ratio"] * 100 for r in top20]

fig, ax = plt.subplots(figsize=(10, 8))
bars = ax.barh(names[::-1], ratios[::-1],
               color=plt.cm.RdYlGn([r / 100 for r in ratios[::-1]]))

for bar, ratio in zip(bars, ratios[::-1]):
    ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
            f"{ratio:.1f}%", va="center", fontsize=9)

ax.set_xlabel("Upscale Business Ratio (% of price tier 2 or higher)", fontsize=12)
ax.set_title("Indianapolis Neighborhoods Ranked by Upscale Business Ratio", fontsize=13)
ax.set_xlim(0, 105)
ax.grid(axis="x", alpha=0.3)
plt.tight_layout()

heatmap_path = os.path.join(RESULTS_DIR, "price_tier_heatmap.png")
plt.savefig(heatmap_path, dpi=150)
plt.close()
print(f"Heatmap saved to {heatmap_path}")

client.close()
print("\nDone.")