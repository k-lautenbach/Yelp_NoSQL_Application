"""
fountain_square_deep_dive.py
----------------------------
Deep dive into Fountain Square business composition and price tier
to understand why turnover is high but price tier is stable.
"""

from pymongo import MongoClient
from collections import defaultdict
import matplotlib.pyplot as plt
import pandas as pd

client = MongoClient("mongodb://localhost:27017/")
db = client["yelp_indy"]

NEIGHBORHOOD = "Fountain Square"

# ── 1. Category breakdown of ALL businesses ───────────────
print(f"\n=== Top 20 Categories in {NEIGHBORHOOD} ===")

pipeline = [
    {"$match": {"neighborhood": NEIGHBORHOOD}},
    {"$project": {"cats": {"$split": ["$categories", ", "]}}},
    {"$unwind": "$cats"},
    {"$group": {"_id": "$cats", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 20}
]

for doc in db.businesses.aggregate(pipeline):
    print(f"  {doc['_id']:40s} {doc['count']}")

# ── 2. Category breakdown split by price tier ─────────────
print(f"\n=== Category breakdown by price tier ===")

pipeline2 = [
    {"$match": {
        "neighborhood": NEIGHBORHOOD,
        "attributes.RestaurantsPriceRange2": {"$exists": True}
    }},
    {"$project": {
        "cats": {"$split": ["$categories", ", "]},
        "price": "$attributes.RestaurantsPriceRange2"
    }},
    {"$unwind": "$cats"},
    {"$group": {
        "_id": {"cat": "$cats", "price": "$price"},
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}},
    {"$limit": 30}
]

for doc in db.businesses.aggregate(pipeline2):
    print(f"  {doc['_id']['cat']:35s} price={doc['_id']['price']}  n={doc['count']}")

# ── 3. Price tier breakdown of open vs closed businesses ──
print(f"\n=== Price tier: open vs closed businesses ===")

pipeline3 = [
    {"$match": {
        "neighborhood": NEIGHBORHOOD,
        "attributes.RestaurantsPriceRange2": {"$exists": True}
    }},
    {"$group": {
        "_id": {
            "price": "$attributes.RestaurantsPriceRange2",
            "is_open": "$is_open"
        },
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id.price": 1, "_id.is_open": -1}}
]

for doc in db.businesses.aggregate(pipeline3):
    status = "open" if doc["_id"]["is_open"] == 1 else "closed"
    print(f"  Price {doc['_id']['price']}  {status:6s}  n={doc['count']}")

# ── 4. New businesses (post-2015) price tier breakdown ────
print(f"\n=== Price tier of NEW businesses (first review after 2015) ===")

# Get business_ids with first review after 2015
new_biz_ids = set()
for doc in db.reviews.aggregate([
    {"$group": {"_id": "$business_id", "first": {"$min": "$date"}}},
    {"$match": {"first": {"$gte": "2015-01-01"}}}
]):
    new_biz_ids.add(doc["_id"])

pipeline4 = [
    {"$match": {
        "neighborhood": NEIGHBORHOOD,
        "business_id": {"$in": list(new_biz_ids)},
        "attributes.RestaurantsPriceRange2": {"$exists": True}
    }},
    {"$group": {
        "_id": "$attributes.RestaurantsPriceRange2",
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id": 1}}
]

total_new = 0
tier_counts = {}
for doc in db.businesses.aggregate(pipeline4):
    tier_counts[doc["_id"]] = doc["count"]
    total_new += doc["count"]

for tier, count in sorted(tier_counts.items()):
    print(f"  Price {tier}: {count} ({count/total_new*100:.1f}%)")

client.close()