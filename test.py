from pymongo import MongoClient
from collections import Counter

client = MongoClient("mongodb://localhost:27017/")
db = client["yelp_indy"]

# Get every unique category across all Indianapolis businesses
all_cats = Counter()

for biz in db.businesses.find({}, {"categories": 1}):
    cats_raw = biz.get("categories", "")
    if not cats_raw or not isinstance(cats_raw, str):
        continue
    for cat in [c.strip() for c in cats_raw.split(",")]:
        all_cats[cat] += 1

print(f"Total unique categories: {len(all_cats)}\n")
print(f"{'Category':45s} {'Count':>6}")
print("-" * 55)
for cat, count in all_cats.most_common():
    print(f"{cat:45s} {count:>6}")