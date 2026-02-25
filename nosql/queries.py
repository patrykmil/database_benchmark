CRUD_OPERATIONS = {
    "insert_single": lambda: {
        "name": "test_user",
        "email": "test@example.com",
        "preferences": {"theme": "dark"},
    },
    "select_single": lambda id: {"_id": id},
    "select_where": lambda: {"email": {"$regex": "test"}},
    "update_single": lambda id: {"$set": {"name": "updated_name"}},
    "update_many": lambda: {"$set": {"verified": True}},
    "delete_single": lambda id: {"_id": id},
    "delete_many": lambda: {"created_at": {"$lt": "2020-01-01"}},
}

AGGREGATE_OPERATIONS = {
    "aggregate_count": [{"$count": "total"}],
    "aggregate_sum": [{"$group": {"_id": None, "total": {"$sum": "$total"}}}],
    "aggregate_avg": [{"$group": {"_id": None, "avg": {"$avg": "$price"}}}],
}

INDEXED_OPERATIONS = {
    "insert_indexed": lambda: ({"name": "new_product", "price": 99.99, "category_id": 1, "attributes": {"color": "red"}}, "insert"),
    "select_indexed": lambda: ({"category_id": 1}, None),
    "select_range": lambda: ({"price": {"$gte": 100, "$lte": 500}}, None),
    "select_like": lambda: ({"name": {"$regex": "widget"}}, None),
    "select_order_by": lambda: ({}, None),
    "update_indexed": lambda: ({"user_id": 1}, {"$set": {"status": "shipped"}}),
    "delete_indexed": lambda: ({"product_id": 1}, None),
    "select_between": lambda: ({"created_at": {"$gte": "2024-01-01", "$lte": "2024-12-31"}}, None),
    "select_in": lambda: ({"category_id": {"$in": [1, 2, 3]}}, None),
    "select_exists": lambda: ({"orders": {"$exists": True}}, None),
    "select_group_by": lambda: (None, [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]),
    "select_having": lambda: (None, [{"$match": {"total": {"$gt": 1000}}}]),
}

JOIN_OPERATIONS = {
    "select_join": [
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user",
            }
        },
        {"$limit": 100},
    ]
}

JSON_OPERATIONS = {
    "json_extract": lambda: {"preferences.language": "en"},
    "json_contains": lambda: {"attributes.color": "red"},
    "json_search": lambda: {"preferences.notifications": True},
}
