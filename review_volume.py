"""
review_volume.py
----------------
Analyzes review volume over time for one or more Indianapolis neighborhoods
or zip codes, producing a line graph with one series per area.

Database : yelp_indy  (mongodb://localhost:27017/)
Output   : results/review_volume.png
           results/review_volume.csv

Usage examples
--------------
# By neighborhood (unlimited args, or omit for "all"):
  python review_volume.py --by neighborhood "Broad Ripple" "Downtown" "Fountain Square"
  python review_volume.py --by neighborhood

# By zip code:
  python review_volume.py --by zipcode 46202 46205
  python review_volume.py --by zipcode

# With dataset-wide average overlay:
  python review_volume.py --by neighborhood "Broad Ripple" "Downtown" --show-avg
"""

import argparse
import os
import csv
from collections import defaultdict

import matplotlib.pyplot as plt
from pymongo import MongoClient

from price_tier_analysis import neighborhood

# ── Config ─────────────────────────────────────────────────────────────────────

MONGO_URI   = "mongodb://localhost:27017/"
DB_NAME     = "yelp_indy"
RESULTS_DIR = "results"

os.makedirs(RESULTS_DIR, exist_ok=True)

# Type alias

VolumeData = dict[str, dict[int, int]]

#  CLI

def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot review volume over time by neighborhood or zip code."
    )
    parser.add_argument(
        "--by",
        choices=["neighborhood", "zipcode"],
        default="neighborhood",
        help="Group reviews by 'neighborhood' or 'zipcode'  (default: neighborhood)",
    )
    parser.add_argument(
        "areas",
        nargs="*",
        default=["all"],
        help=(
            'One or more area names/zip codes, or omit (defaults to "all") '
            "to include every area found in the database."
        ),
    )
    parser.add_argument(
        "--show-avg",
        action="store_true",
        help="Overlay the average review volume across all areas in the dataset.",
    )
    return parser.parse_args()

# Helpers

def mongo_field(by: str) -> str:
    """Map CLI 'by' value to the MongoDB businesses field name."""
    return "neighborhood" if by == "neighborhood" else "postal_code"


def year_count():
    return defaultdict(int)

#Core analysis

def fetch_biz_to_area(db, by: str) -> dict[str, str]:
    """Returns a full business_id → area mapping from the businesses collection."""
    field = mongo_field(by)
    biz_cursor = db.businesses.find(
        {field: {"$exists": True, "$ne": ""}},
        {"business_id": 1, field: 1, "_id": 0},
    )
    result = {}
    for biz in biz_cursor:
        area = str(biz.get(field, "")).strip()
        if area:
            result[biz["business_id"]] = area
    return result


def aggregate_reviews(db, biz_to_area: dict[str, str]) -> VolumeData:
    """Aggregate review counts by area and year for the given business→area map."""
    pipeline = [
        {"$match": {"business_id": {"$in": list(biz_to_area.keys())}}},
        {
            "$group": {
                "_id": {
                    "business_id": "$business_id",
                    "year": {"$substr": ["$date", 0, 4]},
                },
                "count": {"$sum": 1},
            }
        },
    ]

    volume: VolumeData = defaultdict(year_count)

    for doc in db.reviews.aggregate(pipeline, allowDiskUse=True):
        biz_id = doc["_id"]["business_id"]
        try:
            year = int(doc["_id"]["year"])
        except (ValueError, TypeError):
            continue
        area = biz_to_area.get(biz_id)
        if area:
            volume[area][year] += doc["count"]

    return dict(volume)


def build_volume_data(db, by: str, areas: list[str]) -> VolumeData:
    """
    Returns  { area_label: { year: review_count, ... }, ... }

    Steps
    -----
    1. Pull full business_id → area mapping from the businesses collection.
    2. If specific areas were requested, filter to those only.
    3. Aggregate reviews by year for each matching business.
    """
    use_all = (len(areas) == 1 and areas[0].lower() == "all")

    print(f"Building business → {by} map…")
    biz_to_area = fetch_biz_to_area(db, by)
    print(f"  Found {len(biz_to_area):,} businesses with a {by} value.")

    # Filter to requested areas
    if not use_all:
        if by == "neighborhood":
            requested = {a.lower(): a for a in areas}
            biz_to_area = {
                bid: requested[area.lower()]
                for bid, area in biz_to_area.items()
                if area.lower() in requested
            }
        else:
            requested_set = {str(a) for a in areas}
            biz_to_area = {
                bid: area
                for bid, area in biz_to_area.items()
                if area in requested_set
            }

    if not biz_to_area:
        print("  No matching businesses found. Check your area names/zip codes.")
        return {}

    print(f"  Using {len(biz_to_area):,} businesses across {len(set(biz_to_area.values()))} area(s).")
    print("Aggregating review counts by year…")

    volume = aggregate_reviews(db, biz_to_area)
    print(f"  Aggregated review data for {len(volume)} area(s).\n")
    return volume


def build_global_average(db, by: str) -> dict[int, float]:
    """
    Returns { year: avg_review_count_across_ALL_areas_in_dataset }
    The average is computed per-area per-year, then averaged across areas,
    so large areas don't dominate small ones.
    """
    print("Computing dataset-wide average…")

    biz_to_area = fetch_biz_to_area(db, by)
    full_volume = aggregate_reviews(db, biz_to_area)

    all_years = sorted({yr for counts in full_volume.values() for yr in counts})

    global_avg = {}
    for yr in all_years:
        counts_this_year = [counts.get(yr, 0) for counts in full_volume.values()]
        global_avg[yr] = sum(counts_this_year) / len(counts_this_year)

    print(f"  Average computed across {len(full_volume)} area(s).\n")
    return global_avg

# Saving outputs

def save_csv(volume: VolumeData, by: str) -> None:
    all_years = sorted({yr for counts in volume.values() for yr in counts})
    areas     = sorted(volume.keys())

    csv_path = os.path.join(RESULTS_DIR, "review_volume.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([by.capitalize()] + all_years)
        for area in areas:
            row = [area] + [volume[area].get(yr, 0) for yr in all_years]
            writer.writerow(row)

    print(f"  CSV saved  → {csv_path}")


def plot_volume(
    volume: VolumeData,
    by: str,
    global_avg: dict[int, float] | None = None,
) -> None:
    if not volume:
        print("Nothing to plot.")
        return

    all_years = sorted({yr for counts in volume.values() for yr in counts})
    areas     = sorted(volume.keys(), key=lambda a: -sum(volume[a].values()))

    fig, ax = plt.subplots(figsize=(14, 7))

    for area in areas:
        counts = [volume[area].get(yr, 0) for yr in all_years]
        ax.plot(all_years, counts, marker="o", linewidth=2, label=area)

    # ── Optional dataset average overlay ─────────────────────────────────────
    if global_avg:
        avg_years  = sorted(global_avg.keys())
        avg_counts = [global_avg[yr] for yr in avg_years]
        ax.plot(
            avg_years, avg_counts,
            color="black", linewidth=2.5,
            linestyle="--", label="Dataset Average",
        )

    ax.set_title(
        f"Review Volume Over Time by {by.capitalize()}",
        fontsize=15,
        fontweight="bold",
    )
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Number of Reviews", fontsize=12)
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(1.01, 1),
        borderaxespad=0,
        fontsize=9,
        title=by.capitalize(),
    )
    ax.grid(axis="y", alpha=0.3)
    ax.set_xticks(all_years)
    plt.xticks(rotation=45)
    plt.tight_layout()

    chart_path = os.path.join(RESULTS_DIR, "review_volume.png")
    plt.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Chart saved → {chart_path}")

#Main

def main():
    neighborhoods = ['Near Westside', "I-69/Fall Creek", 'Near Eastside',
                     'Martindale - Brightwood', 'Meridian Kessler']

    args = parse_args()
    areas = args.areas

    print(f"Connecting to MongoDB at {MONGO_URI}…")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("Connected.\n")

    db = client[DB_NAME]

    volume = build_volume_data(db, args.by, areas)

    if volume:
        global_avg = build_global_average(db,
                                          args.by) if args.show_avg else None
        save_csv(volume, args.by)
        plot_volume(volume, args.by, global_avg)
        print("\nDone.")
    else:
        print("No data to output. Exiting.")
    global_avg = build_global_average(db, args.by) if args.show_avg else None
    print("global_avg:", global_avg)

    client.close()




if __name__ == "__main__":
    main()