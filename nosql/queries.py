NONINDEXED_OPERATIONS = {
    # CREATE - 6 queries
    "insert_single": lambda: {
        "name": "test_user",
        "email": "test@example.com",
        "preferences": {"theme": "dark"},
    },
    "insert_bulk": lambda: [
        {"name": f"user{i}", "email": f"user{i}@example.com", "preferences": {}}
        for i in range(1000)
    ],
    "insert_ignore": lambda: {
        "name": "test_user",
        "email": "test@example.com",
        "preferences": {"theme": "dark"},
    },
    "insert_upsert": lambda: {
        "$setOnInsert": {"name": "test_user", "created_at": "2024-01-01"},
        "$set": {"preferences": {"theme": "light"}},
    },
    "insert_many": lambda: [{"name": f"cat{i}"} for i in range(100)],
    "insert_returning": lambda: {
        "name": "test_user",
        "email": "test@example.com",
        "preferences": {"theme": "dark"},
    },
    # READ - 6 queries
    "select_single": lambda id: {"_id": id},
    "select_where": lambda: {"email": {"$regex": "test"}},
    "select_join": lambda: [
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user",
            }
        },
        {"$limit": 100},
    ],
    "select_aggregate": lambda: [
        {"$match": {"user_id": 1}},
        {
            "$group": {
                "_id": None,
                "count": {"$sum": 1},
                "total": {"$sum": "$total"},
                "avg": {"$avg": "$total"},
            }
        },
    ],
    "select_pagination": lambda: [
        {"$sort": {"created_at": -1}},
        {"$skip": 0},
        {"$limit": 10},
    ],
    "select_distinct": lambda: [{"$group": {"_id": "$status"}}],
    # UPDATE - 6 queries
    "update_single": lambda: [{"$set": {"name": "updated_name"}}],
    "update_many": lambda: [{"$set": {"verified": True}}],
    "update_in": lambda: {"$set": {"status": "active"}},
    "update_case": lambda: [{"$set": {"status": "processed"}}],
    "update_join": lambda: [{"$set": {"status": "processed"}}],
    "update_upsert": lambda: {"$set": {"preferences": {"theme": "light"}}},
    # DELETE - 6 queries
    "delete_single": lambda id: {"_id": id},
    "delete_many": lambda: {"created_at": {"$lt": "2020-01-01"}},
    "delete_in": lambda: {"_id": {"$in": [100, 101, 102]}},
    "delete_cascade": lambda: {"user_id": 1},
    "delete_join": lambda: [
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user",
            }
        },
        {"$match": {"user.created_at": {"$lt": "2023-01-01"}}},
    ],
    "delete_truncate": lambda: {},
}

INDEXED_OPERATIONS = {
    # CREATE - 6 queries (using indexed fields)
    "index_insert_single": lambda: {
        "name": "indexed_user",
        "email": "indexed@example.com",
        "created_at": "2024-01-01",
        "preferences": {"theme": "dark"},
    },
    "index_insert_bulk": lambda: [
        {
            "name": f"bulkuser{i}",
            "email": f"bulkuser{i}@example.com",
            "created_at": "2024-01-01",
            "preferences": {},
        }
        for i in range(1000)
    ],
    "index_insert_ignore": lambda: {
        "name": "indexed_product",
        "price": 99.99,
        "category_id": 1,
        "attributes": {"color": "red"},
    },
    "index_insert_upsert": lambda: {
        "name": "upsert_product",
        "price": 49.99,
        "category_id": 1,
        "attributes": {"color": "blue"},
    },
    "index_insert_many": lambda: [
        {"name": f"product{i}", "price": 10.0, "category_id": 1, "attributes": {}}
        for i in range(100)
    ],
    "index_insert_returning": lambda: {
        "name": "returning_product",
        "price": 19.99,
        "category_id": 1,
        "attributes": {"color": "green"},
    },
    # READ - 6 queries (using indexed fields)
    "index_select_single": lambda: {"email": "user1000@example.com"},
    "index_select_where": lambda: {"created_at": {"$gte": "2024-01-01"}},
    "index_select_join": lambda: [
        {"$match": {"users.created_at": {"$gte": "2024-01-01"}}},
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user",
            }
        },
        {"$limit": 100},
    ],
    "index_select_aggregate": lambda: [
        {"$match": {"user_id": 1}},
        {
            "$group": {
                "_id": None,
                "count": {"$sum": 1},
                "total": {"$sum": "$total"},
                "avg": {"$avg": "$total"},
            }
        },
    ],
    "index_select_pagination": lambda: [
        {"$sort": {"created_at": -1}},
        {"$skip": 0},
        {"$limit": 10},
    ],
    "index_select_distinct": lambda: [{"$group": {"_id": "$status"}}],
    # UPDATE - 6 queries (using indexed fields)
    "index_update_single": lambda: [{"$set": {"name": "updated_email_user"}}],
    "index_update_many": lambda: {"$mul": {"price": 1.1}},
    "index_update_in": lambda: {"$set": {"price": 9.99}},
    "index_update_case": lambda: [
        {
            "$facet": {
                "shipped": [{"$match": {"_id": 1}}, {"$set": {"status": "shipped"}}],
                "delivered": [
                    {"$match": {"_id": 2}},
                    {"$set": {"status": "delivered"}},
                ],
            }
        }
    ],
    "index_update_join": lambda: [
        {"$set": {"status": "processed"}},
        {"$match": {"user.email": "user1@example.com"}},
    ],
    "index_update_upsert": lambda: {
        "$set": {"price": 29.99, "attributes": {"color": "blue"}},
    },
    # DELETE - 6 queries (using indexed fields)
    "index_delete_single": lambda: {"email": "delete@example.com"},
    "index_delete_many": lambda: {"created_at": {"$lt": "2020-01-01"}},
    "index_delete_in": lambda: {"category_id": {"$in": [100, 101]}},
    "index_delete_cascade": lambda: {"user_id": 1},
    "index_delete_join": lambda: [
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user",
            }
        },
        {"$match": {"user.created_at": {"$lt": "2023-01-01"}}},
    ],
    "index_delete_truncate": lambda: {},
}

AGGREGATE_OPERATIONS = {
    "aggregate_count": [{"$count": "total"}],
    "aggregate_sum": [{"$group": {"_id": None, "total": {"$sum": "$total"}}}],
    "aggregate_avg": [{"$group": {"_id": None, "avg": {"$avg": "$price"}}}],
}

EXPLAIN_OPERATIONS = {
    "explain_insert": lambda: {
        "name": "explain_user",
        "email": "explain@example.com",
        "preferences": {"theme": "dark"},
    },
    "explain_select": lambda: {"email": "user1000@example.com"},
    "explain_select_where": lambda: {"created_at": {"$gte": "2024-01-01"}},
    "explain_select_join": lambda: [
        {"$match": {"users.created_at": {"$gte": "2024-01-01"}}},
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user",
            }
        },
        {"$limit": 100},
    ],
    "explain_select_aggregate": lambda: [
        {"$match": {"user_id": 1}},
        {"$group": {"_id": None, "count": {"$sum": 1}, "total": {"$sum": "$total"}}},
    ],
    "explain_select_pagination": lambda: [
        {"$sort": {"created_at": -1}},
        {"$skip": 0},
        {"$limit": 10},
    ],
    "explain_select_distinct": lambda: [{"$group": {"_id": "$status"}}],
    "explain_update": lambda: [{"$set": {"name": "updated_email_user"}}],
    "explain_update_many": lambda: {"$mul": {"price": 1.1}},
    "explain_update_in": lambda: {"$set": {"price": 9.99}},
    "explain_delete": lambda: {"email": "delete@example.com"},
    "explain_delete_many": lambda: {"created_at": {"$lt": "2020-01-01"}},
    "explain_delete_in": lambda: {"category_id": {"$in": [100, 101]}},
    "explain_delete_cascade": lambda: {"user_id": 1},
    "explain_indexed_select": lambda: {"category_id": 1},
    "explain_indexed_range": lambda: {"price": {"$gte": 100, "$lte": 500}},
    "explain_complex_join": lambda: [
        {
            "$lookup": {
                "from": "orders",
                "localField": "_id",
                "foreignField": "user_id",
                "as": "orders",
            }
        },
        {"$match": {"created_at": {"$gt": "2023-01-01"}}},
        {"$unwind": {"path": "$orders", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$_id",
                "order_count": {"$sum": 1},
                "total_spent": {"$sum": "$orders.total"},
            }
        },
        {"$match": {"order_count": {"$gt": 5}}},
        {"$sort": {"total_spent": -1}},
        {"$limit": 100},
    ],
}

JSON_OPERATIONS = {
    "json_extract": lambda: {"preferences.language": "en"},
    "json_contains": lambda: {"attributes.color": "red"},
    "json_search": lambda: {"preferences.notifications": True},
}
