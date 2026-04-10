import json
import os

TARGET_CITY = "Indianapolis"  # change to "Indianapolis" if pivoting

DATA = os.path.join("Yelp-JSON", "Yelp JSON")
OUT  = os.path.join("Yelp-JSON", f"filtered_{TARGET_CITY.lower()}")
os.makedirs(OUT, exist_ok=True)

# ── Step 1: Filter businesses, collect valid business_ids ─
business_ids = set()
kept = 0

with open(os.path.join(DATA, "yelp_academic_dataset_business.json"), "r", encoding="utf-8") as fin, \
     open(os.path.join(OUT,  "yelp_academic_dataset_business.json"), "w", encoding="utf-8") as fout:
    for line in fin:
        biz = json.loads(line)
        if biz.get("city", "").lower() == TARGET_CITY.lower():
            fout.write(line)
            business_ids.add(biz["business_id"])
            kept += 1

print(f"Businesses kept: {kept}")

# ── Step 2: Filter the other files by business_id ─────────
for filename in ["yelp_academic_dataset_review.json",
                 "yelp_academic_dataset_checkin.json",
                 "yelp_academic_dataset_tip.json"]:
    kept = 0
    with open(os.path.join(DATA, filename), "r", encoding="utf-8") as fin, \
         open(os.path.join(OUT,  filename), "w", encoding="utf-8") as fout:
        for line in fin:
            record = json.loads(line)
            if record.get("business_id") in business_ids:
                fout.write(line)
                kept += 1
    print(f"{filename}: {kept} records kept")

print(f"\nFiltered files saved to: {OUT}")