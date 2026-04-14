'''
Authors: Kate Lautenbach, Andrew Nee, Abigail Valladolid

this file tracks how business category composition changes over time
in key Indianapolis neighborhoods, using first review year
as business entry timestamp.

'''

MONGO_URI   = "mongodb://localhost:27017/"
DB_NAME     = "yelp_indy"
RESULTS_DIR = "results"

CATEGORY_GROUPS = {
    "Food & Drink":      ["Restaurants", "Food", "Specialty Food", "Sandwiches",
                          "Coffee & Tea", "Bakeries", "Desserts", "Delis",
                          "Ice Cream & Frozen Yogurt", "Juice Bars & Smoothies"],
    "Bars & Nightlife":  ["Nightlife", "Bars", "Cocktail Bars", "Beer Bar",
                          "Wine Bars", "Breweries", "Pubs", "Sports Bars",
                          "Lounges", "Brewpubs", "Beer Gardens", "Gastropubs"],
    "Retail & Shopping": ["Shopping", "Fashion", "Vintage & Consignment",
                          "Used", "Books", "Home Decor", "Arts & Crafts",
                          "Antiques", "Vinyl Records", "Bookstores"],
    "Arts & Wellness":   ["Arts & Entertainment", "Beauty & Spas", "Fitness & Instruction",
                          "Yoga", "Gyms", "Music Venues", "Art Galleries",
                          "Pilates", "Barre Classes", "Day Spas", "Performing Arts"],
    "Services":          ["Local Services", "Home Services", "Automotive",
                          "Health & Medical", "Real Estate", "Auto Repair",
                          "Nail Salons", "Hair Salons", "Barbers"]
}

# Categories associated with gentrification (incoming)
GENTRIFY_CATS = [
    "Coffee & Tea", "Cocktail Bars", "Wine Bars", "Wine & Spirits",
    "Breweries", "Brewpubs", "Beer Bar", "Beer Gardens", "Gastropubs",
    "American (New)", "Tapas Bars", "Tapas/Small Plates",
    "Juice Bars & Smoothies", "Vegetarian", "Vegan", "Gluten-Free",
    "Specialty Food", "Organic Stores", "Kombucha", "Acai Bowls",
    "Distilleries", "Cideries", "Speakeasies", "Whiskey Bars",
    "Arts & Entertainment", "Art Galleries", "Music Venues",
    "Performing Arts", "Festivals", "Art Classes", "Art Schools",
    "Yoga", "Gyms", "Fitness & Instruction", "Pilates", "Barre Classes",
    "Trainers", "Boot Camps", "Cycling Classes", "Interval Training Gyms",
    "Float Spa", "Meditation Centers", "Vintage & Consignment",
    "Books", "Bookstores", "Vinyl Records", "Shared Office Spaces",
    "Coffee Roasteries", "Day Spas", "Medical Spas", "Skin Care",
]

# Categories associated with pre-gentrification (displaced)
LEGACY_CATS = [
    "Fast Food", "Burgers", "Chicken Wings", "Chicken Shop", "Hot Dogs",
    "Diners", "Buffets", "Food Stands", "Comfort Food", "Sandwiches",
    "Laundry Services", "Laundromat", "Dry Cleaning & Laundry", "Dry Cleaning",
    "Convenience Stores", "Discount Store", "Thrift Stores", "Pawn Shops",
    "Drugstores", "Nail Salons", "Barbers", "Antiques",
    "Auto Repair", "Tires", "Oil Change Stations", "Auto Parts & Supplies",
    "Body Shops", "Transmission Repair",
    "Soul Food", "Southern", "Barbeque", "Mexican", "Latin American",
    "Caribbean", "Cajun/Creole", "Chinese", "Vietnamese",
    "Tobacco Shops", "Gas Stations",
]

FOCUS_NEIGHBORHOOD      = "Fountain Square"
COMPARISON_NEIGHBORHOODS = ["Fairgrounds", "Near Eastside", "Near NW - Riverside"]

os.makedirs(RESULTS_DIR, exist_ok=True)

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
client.admin.command("ping")
print(f"Connected to MongoDB\n")

db = client[DB_NAME]

# First review year per business

print("Getting first review years...")
first_year_map = {}
for doc in db.reviews.aggregate([
    {"$group": {"_id": "$business_id", "first": {"$min": "$date"}}}
], allowDiskUse=True):
    try:
        year = int(doc["first"][:4])
        if 2005 <= year <= 2021:
            first_year_map[doc["_id"]] = year
    except:
        pass

print(f"  {len(first_year_map):,} businesses with review history\n")

# build year to category counts per neighborhood

def build_category_timeline(neighborhood):
    timeline = defaultdict(lambda: defaultdict(int))
    businesses = db.businesses.find(
        {"neighborhood": neighborhood},
        {"business_id": 1, "categories": 1}
    )
    for biz in businesses:
        year = first_year_map.get(biz["business_id"])
        if not year:
            continue
        cats_raw = biz.get("categories", "")
        if not cats_raw:
            continue
        for cat in [c.strip() for c in cats_raw.split(",")]:
            timeline[year][cat] += 1
    return timeline

# aggregate into gentrify vs legacy scores

def get_gentrify_scores(timeline):
    years    = sorted(timeline.keys())
    gentrify = []
    legacy   = []
    for year in years:
        g = sum(timeline[year].get(cat, 0) for cat in GENTRIFY_CATS)
        l = sum(timeline[year].get(cat, 0) for cat in LEGACY_CATS)
        gentrify.append(g)
        legacy.append(l)
    return years, gentrify, legacy

# grouped category chart for Fountain Square

print(f"Analyzing {FOCUS_NEIGHBORHOOD}...")
fs_timeline = build_category_timeline(FOCUS_NEIGHBORHOOD)

years = sorted(fs_timeline.keys())
df_data = {"year": years}

for group_name, cats in CATEGORY_GROUPS.items():
    df_data[group_name] = [
        sum(fs_timeline[y].get(cat, 0) for cat in cats)
        for y in years
    ]

df = pd.DataFrame(df_data).set_index("year")
df_smooth = df.rolling(3, min_periods=1).mean()

fig, ax = plt.subplots(figsize=(14, 7))
colors = plt.cm.tab10.colors

for i, group in enumerate(CATEGORY_GROUPS.keys()):
    ax.plot(df_smooth.index, df_smooth[group],
            marker="o", linewidth=2.5,
            label=group,
            color=colors[i])

ax.axvline(x=2015, color="gray", linestyle="--", linewidth=1, alpha=0.7)
ax.text(2015.1, ax.get_ylim()[1] * 0.95 if ax.get_ylim()[1] > 0 else 10,
        "2015", color="gray", fontsize=9)

ax.set_xlabel("Year (first review = business entry)", fontsize=12)
ax.set_ylabel("New Businesses per Year (3-yr rolling avg)", fontsize=11)
ax.set_title(f"Business Category Group Composition Over Time — {FOCUS_NEIGHBORHOOD}", fontsize=13)
ax.legend(loc="upper left", fontsize=10)
ax.grid(axis="y", alpha=0.3)
ax.set_xlim(2005, 2021)
plt.tight_layout()

out = os.path.join(RESULTS_DIR, "category_shift_fountain_square.png")
plt.savefig(out, dpi=150)
plt.close()
print(f"  Saved {out}")

# gentrify vs Legacy comparison (2x2 grid)

print("Building gentrification score comparison...")

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
axes = axes.flatten()

all_neighborhoods = [FOCUS_NEIGHBORHOOD] + COMPARISON_NEIGHBORHOODS

for i, neighborhood in enumerate(all_neighborhoods):
    timeline = build_category_timeline(neighborhood)
    years, gentrify, legacy = get_gentrify_scores(timeline)

    if not years:
        continue

    df_g = pd.DataFrame({"year": years, "gentrify": gentrify, "legacy": legacy})
    df_g = df_g.set_index("year")
    df_g_smooth = df_g.rolling(3, min_periods=1).mean()

    ax = axes[i]
    ax.plot(df_g_smooth.index, df_g_smooth["gentrify"],
            color="steelblue", linewidth=2, marker="o",
            label="Gentrification categories")
    ax.plot(df_g_smooth.index, df_g_smooth["legacy"],
            color="darkorange", linewidth=2, marker="o",
            label="Legacy categories")

    ax.axvline(x=2015, color="gray", linestyle="--", linewidth=1, alpha=0.5)
    ax.set_title(neighborhood, fontsize=12)
    ax.set_xlabel("Year", fontsize=9)
    ax.set_ylabel("New businesses/yr", fontsize=9)
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.set_xlim(2005, 2021)

plt.suptitle("Gentrification vs. Legacy Category Arrivals by Neighborhood (3-yr rolling avg)",
             fontsize=13, y=1.01)
plt.tight_layout()

out2 = os.path.join(RESULTS_DIR, "category_shift_comparison.png")
plt.savefig(out2, dpi=150, bbox_inches="tight")
plt.close()
print(f"  Saved {out2}")

# export csv

rows = []
for neighborhood in all_neighborhoods:
    timeline = build_category_timeline(neighborhood)
    for year, cat_counts in sorted(timeline.items()):
        for cat, count in cat_counts.items():
            rows.append({
                "neighborhood": neighborhood,
                "year":         year,
                "category":     cat,
                "count":        count
            })

csv_path = os.path.join(RESULTS_DIR, "category_counts_by_year.csv")
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["neighborhood", "year", "category", "count"])
    writer.writeheader()
    writer.writerows(rows)

print(f"  CSV saved to {csv_path}")

client.close()
print("\nDone.")