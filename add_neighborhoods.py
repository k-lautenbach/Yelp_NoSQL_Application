from pymongo import MongoClient
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
from bson import ObjectId

# ── Connection ─────────────────────────────────────────────
client = MongoClient("mongodb://localhost:27017/")
db = client["yelp_indy"]

# ── Load businesses from MongoDB ───────────────────────────
print("Loading businesses...")
businesses = list(db.businesses.find({}, {"_id": 1, "location": 1}))

# ── Build GeoDataFrame ─────────────────────────────────────
print("Building GeoDataFrame...")
gdf = gpd.GeoDataFrame(
    businesses,
    geometry=[Point(b["location"]["coordinates"][0],
                    b["location"]["coordinates"][1]) for b in businesses],
    crs="EPSG:4326"
)

# ── Load neighborhood boundaries ───────────────────────────
print("Loading neighborhoods...")
neighborhoods = gpd.read_file("indy_neighborhoods.geojson")

# ── Spatial join ───────────────────────────────────────────
print("Joining...")
joined = gpd.sjoin(gdf, neighborhoods[["NAME", "geometry"]], how="left", predicate="within")

# ── Write back to MongoDB ──────────────────────────────────
print("Updating MongoDB...")
updated = 0
for _, row in joined.iterrows():
    if pd.notna(row["NAME"]):
        result = db.businesses.update_one(
            {"_id": ObjectId(str(row["_id"]))},
            {"$set": {"neighborhood": row["NAME"]}}
        )
        updated += result.modified_count

print(f"Done. Updated {updated} businesses with neighborhood names.")