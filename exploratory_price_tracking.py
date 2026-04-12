from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["yelp_indy"]

# 1. Distribution of price ranges across all restaurants
price_distribution = db.businesses.aggregate([
    {
        "$match": {
            "attributes.RestaurantsPriceRange2": {"$exists": True},
            "categories": {"$regex": "Restaurants", "$options": "i"}
        }
    },
    {
        "$group": {
            "_id": "$attributes.RestaurantsPriceRange2",
            "count": {"$sum": 1},
            "avg_stars": {"$avg": "$stars"},
            "avg_review_count": {"$avg": "$review_count"}
        }
    },
    {"$sort": {"_id": 1}}
])

print("=== Price Range Distribution ===")
for doc in price_distribution:
    print(f"Price Range {doc['_id']}: {doc['count']} restaurants | "
          f"Avg Stars: {doc['avg_stars']:.2f} | Avg Reviews: {doc['avg_review_count']:.0f}")


# 2. Price range vs. average star rating (quality vs. cost)
price_vs_stars = db.businesses.aggregate([
    {
        "$match": {
            "attributes.RestaurantsPriceRange2": {"$in": ["1", "2", "3", "4"]},
            "review_count": {"$gte": 10}   # filter noise: at least 10 reviews
        }
    },
    {
        "$group": {
            "_id": "$attributes.RestaurantsPriceRange2",
            "total_restaurants": {"$sum": 1},
            "avg_stars": {"$avg": "$stars"},
            "avg_reviews": {"$avg": "$review_count"},
            "total_reviews": {"$sum": "$review_count"}
        }
    },
    {"$sort": {"_id": 1}}
])

print("\n=== Price Range vs. Stars (min 10 reviews) ===")
for doc in price_vs_stars:
    label = {"1": "$", "2": "$$", "3": "$$$", "4": "$$$$"}.get(doc["_id"], "?")
    print(f"{label}: {doc['total_restaurants']} restaurants | "
          f"Avg Stars: {doc['avg_stars']:.2f} | Total Reviews: {doc['total_reviews']:,}")


# ── 3. Price range breakdown by city ──────────────────────────────────────────
price_by_city = db.businesses.aggregate([
    {
        "$match": {
            "attributes.RestaurantsPriceRange2": {"$exists": True},
            "city": {"$exists": True}
        }
    },
    {
        "$group": {
            "_id": {
                "city": "$city",
                "price_range": "$attributes.RestaurantsPriceRange2"
            },
            "count": {"$sum": 1},
            "avg_stars": {"$avg": "$stars"}
        }
    },
    {
        "$group": {
            "_id": "$_id.city",
            "total": {"$sum": "$count"},
            "price_breakdown": {
                "$push": {
                    "price": "$_id.price_range",
                    "count": "$count",
                    "avg_stars": "$avg_stars"
                }
            }
        }
    },
    {"$match": {"total": {"$gte": 50}}},   # cities with enough data
    {"$sort": {"total": -1}},
    {"$limit": 10}
])

print("\n=== Price Range by City (Top 10 cities) ===")
for doc in price_by_city:
    breakdown = sorted(doc["price_breakdown"], key=lambda x: x["price"])
    summary = " | ".join(
        f"Range {p['price']}: {p['count']} ({p['avg_stars']:.1f}★)"
        for p in breakdown
    )
    print(f"{doc['_id']} ({doc['total']} total)  →  {summary}")


# 4. Price range vs. review sentiment (join with reviews)
price_vs_sentiment = db.businesses.aggregate([
    {
        "$match": {
            "attributes.RestaurantsPriceRange2": {"$in": ["1", "2", "3", "4"]}
        }
    },
    {
        "$lookup": {
            "from": "reviews",
            "localField": "business_id",
            "foreignField": "business_id",
            "as": "reviews"
        }
    },
    {"$unwind": "$reviews"},
    {
        "$group": {
            "_id": "$attributes.RestaurantsPriceRange2",
            "avg_review_stars": {"$avg": "$reviews.stars"},
            "count_5_star": {
                "$sum": {"$cond": [{"$eq": ["$reviews.stars", 5]}, 1, 0]}
            },
            "count_1_star": {
                "$sum": {"$cond": [{"$eq": ["$reviews.stars", 1]}, 1, 0]}
            },
            "total_reviews": {"$sum": 1}
        }
    },
    {"$sort": {"_id": 1}}
])

print("\n=== Price Range vs. Review Sentiment ===")
for doc in price_vs_sentiment:
    pct_5 = (doc["count_5_star"] / doc["total_reviews"]) * 100
    pct_1 = (doc["count_1_star"] / doc["total_reviews"]) * 100
    print(f"Range {doc['_id']}: Avg Review Stars: {doc['avg_review_stars']:.2f} | "
          f"5★: {pct_5:.1f}% | 1★: {pct_1:.1f}% | Total: {doc['total_reviews']:,}")


# 5. Top categories per price tier
top_categories_per_tier = db.businesses.aggregate([
    {
        "$match": {
            "attributes.RestaurantsPriceRange2": {"$exists": True},
            "categories": {"$exists": True}
        }
    },
    {
        "$project": {
            "price": "$attributes.RestaurantsPriceRange2",
            "categories": {
                "$split": ["$categories", ", "]
            }
        }
    },
    {"$unwind": "$categories"},
    {
        "$group": {
            "_id": {
                "price": "$price",
                "category": "$categories"
            },
            "count": {"$sum": 1}
        }
    },
    {"$sort": {"_id.price": 1, "count": -1}},
    {
        "$group": {
            "_id": "$_id.price",
            "top_categories": {
                "$push": {"category": "$_id.category", "count": "$count"}
            }
        }
    },
    {
        "$project": {
            "top_categories": {"$slice": ["$top_categories", 5]}
        }
    },
    {"$sort": {"_id": 1}}
])

print("\n=== Top 5 Categories per Price Tier ===")
for doc in top_categories_per_tier:
    cats = ", ".join(f"{c['category']} ({c['count']})" for c in doc["top_categories"])
    print(f"Range {doc['_id']}: {cats}")