"""
load_mongo.py
-------------
Ingests the filtered Indianapolis Yelp dataset into MongoDB.

Database : yelp_indy  (mongodb://localhost:27017/)
Collections created:
  businesses  – with 2dsphere index on GeoJSON location field
  reviews
  checkins
  tips
"""

import json
import os
from pymongo import MongoClient, GEOSPHERE, ASCENDING
from pymongo.errors import BulkWriteError

MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "yelp_indy"
DATA_DIR   = os.path.join("Yelp-JSON", "filtered_indianapolis")
BATCH_SIZE = 1000

FILES = {
    "businesses": "yelp_academic_dataset_business.json",
    "reviews":    "yelp_academic_dataset_review.json",
    "checkins":   "yelp_academic_dataset_checkin.json",
    "tips":       "yelp_academic_dataset_tip.json",
}

# ── Document transformations ───────────────────────────────────────────────────

def transform_business(doc):
    """Reshape lat/lon into a GeoJSON Point for 2dsphere indexing."""
    lat = doc.pop("latitude", None)
    lon = doc.pop("longitude", None)
    if lat is not None and lon is not None:
        doc["location"] = {
            "type": "Point",
            "coordinates": [lon, lat],   # GeoJSON order: [longitude, latitude]
        }
    return doc

def transform_checkin(doc):
    """Split the comma-separated date string into a list of timestamps."""
    raw = doc.get("date", "")
    if isinstance(raw, str):
        doc["date"] = [d.strip() for d in raw.split(",") if d.strip()]
    return doc

TRANSFORMS = {
    "businesses": transform_business,
    "checkins":   transform_checkin,
}

# ── Index definitions ──────────────────────────────────────────────────────────

def create_indexes(db):
    db.businesses.create_index([("business_id", ASCENDING)], unique=True)
    db.businesses.create_index([("location", GEOSPHERE)])
    db.businesses.create_index([("postal_code", ASCENDING)])
    db.businesses.create_index([("categories", ASCENDING)])

    db.reviews.create_index([("review_id", ASCENDING)], unique=True)
    db.reviews.create_index([("business_id", ASCENDING)])
    db.reviews.create_index([("date", ASCENDING)])
    db.reviews.create_index([("stars", ASCENDING)])

    db.checkins.create_index([("business_id", ASCENDING)])

    db.tips.create_index([("business_id", ASCENDING)])
    db.tips.create_index([("date", ASCENDING)])

    print("Indexes created.")

# ── Bulk loader ────────────────────────────────────────────────────────────────

def load_collection(collection, filepath, transform=None):
    total_inserted = 0
    batch = []

    def flush(batch):
        nonlocal total_inserted
        try:
            result = collection.insert_many(batch, ordered=False)
            total_inserted += len(result.inserted_ids)
        except BulkWriteError as e:
            # Count successful inserts even when some duplicates are skipped
            total_inserted += e.details.get("nInserted", 0)

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            if transform:
                doc = transform(doc)
            batch.append(doc)
            if len(batch) >= BATCH_SIZE:
                flush(batch)
                batch.clear()

    if batch:
        flush(batch)

    return total_inserted

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Fail fast if mongod is not running
    client.admin.command("ping")
    print(f"Connected to MongoDB at {MONGO_URI}\n")

    db = client[DB_NAME]
    create_indexes(db)
    print()

    for col_name, filename in FILES.items():
        filepath = os.path.join(DATA_DIR, filename)
        transform = TRANSFORMS.get(col_name)
        collection = db[col_name]

        print(f"Loading {col_name} from {filename} ...")
        inserted = load_collection(collection, filepath, transform)
        total = collection.count_documents({})
        print(f"  inserted this run : {inserted:>8,}")
        print(f"  total in collection: {total:>8,}")
        print()

    client.close()
    print("Done.")

if __name__ == "__main__":
    main()
