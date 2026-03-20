from src.utils.benchmark_helpers import (
    DELETE_TARGET_CATEGORIES,
    DELETE_TARGET_IDS,
)

NONINDEXED_OPERATIONS = {
    # CREATE - 6 queries
    "insert_single": lambda: {
        "name": "test_user_single",
        "email": "single@example.com",
        "created_at": "2024-01-01",
        "preferences": {"theme": "dark"},
    },
    "insert_bulk": lambda: None,
    "insert_ignore": lambda: {
        "name": "test_user_ignore",
        "email": "ignore@example.com",
        "created_at": "2024-01-01",
        "preferences": {"theme": "dark"},
    },
    "insert_upsert": lambda: {
        "filter": {"email": "upsert@example.com"},
        "update": {
            "$setOnInsert": {"created_at": "2024-01-01"},
            "$set": {
                "name": "test_user_upsert",
                "preferences": {"theme": "light"},
            },
        },
    },
    "insert_many": lambda: [{"name": f"cat{i}"} for i in range(100)],
    "insert_returning": lambda: {
        "name": "test_user",
        "email": "returning@example.com",
        "created_at": "2024-01-01",
        "preferences": {"theme": "dark"},
    },
    # READ - 6 queries
    "select_single": lambda: {"_id": 1},
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
        {"$unwind": "$user"},
        {
            "$project": {
                "_id": 1,
                "user_name": "$user.name",
                "total": 1,
                "status": 1,
            }
        },
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
    "update_single": lambda: {
        "filter": {"_id": 1},
        "update": {"$set": {"name": "updated_name"}},
    },
    "update_many": lambda: {
        "filter": {"_id": {"$gte": 1, "$lte": 1000}},
        "update": {"$set": {"preferences": {"verified": True}}},
    },
    "update_in": lambda: {
        "filter": {"_id": {"$in": [1, 2, 3]}},
        "update": {"$set": {"name": "updated_user"}},
    },
    "update_case": lambda: {
        "operations": [
            {"filter": {"_id": 1}, "update": {"$set": {"name": "user_active"}}},
            {"filter": {"_id": 2}, "update": {"$set": {"name": "user_inactive"}}},
        ]
    },
    "update_join": lambda: {
        "filter": {"user_id": 1},
        "update": {"$set": {"status": "processed"}},
    },
    "update_upsert": lambda: {
        "filter": {"name": "existing_product"},
        "update": {
            "$set": {
                "price": 29.99,
                "category_id": 1,
                "attributes": {"color": "blue"},
            }
        },
    },
    # DELETE - 6 queries
    "delete_single": lambda: {"_id": -1},
    "delete_many": lambda: {"created_at": {"$lt": "2020-01-01"}},
    "delete_in": lambda: {"_id": {"$in": DELETE_TARGET_IDS}},
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
        {"$unwind": "$user"},
        {"$match": {"user.created_at": {"$lt": "2023-01-01"}}},
        {"$project": {"_id": 1}},
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
    "index_insert_bulk": lambda: None,
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
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user",
            }
        },
        {"$unwind": "$user"},
        {"$match": {"user.created_at": {"$gte": "2024-01-01"}}},
        {
            "$project": {
                "_id": 1,
                "user_name": "$user.name",
                "total": 1,
                "status": 1,
            }
        },
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
    "index_update_single": lambda: {
        "filter": {"email": "user1@example.com"},
        "update": {"$set": {"name": "updated_email_user"}},
    },
    "index_update_many": lambda: {
        "filter": {"category_id": 1},
        "update": {"$mul": {"price": 1.1}},
    },
    "index_update_in": lambda: {
        "filter": {"category_id": {"$in": [1, 2, 3]}},
        "update": {"$set": {"price": 9.99}},
    },
    "index_update_case": lambda: {
        "operations": [
            {"filter": {"_id": 1}, "update": {"$set": {"status": "shipped"}}},
            {"filter": {"_id": 2}, "update": {"$set": {"status": "delivered"}}},
        ]
    },
    "index_update_join": lambda: {
        "filter": {"user_id": 1},
        "update": {"$set": {"status": "processed"}},
    },
    "index_update_upsert": lambda: {
        "filter": {"name": "existing_product"},
        "update": {
            "$set": {
                "price": 29.99,
                "category_id": 1,
                "attributes": {"color": "blue"},
            }
        },
    },
    # DELETE - 6 queries (using indexed fields)
    "index_delete_single": lambda: {"email": "delete@example.com"},
    "index_delete_many": lambda: {"created_at": {"$lt": "2020-01-01"}},
    "index_delete_in": lambda: {"category_id": {"$in": DELETE_TARGET_CATEGORIES}},
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
        {"$unwind": "$user"},
        {"$match": {"user.created_at": {"$lt": "2023-01-01"}}},
        {"$project": {"_id": 1}},
    ],
    "index_delete_truncate": lambda: {},
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
    "explain_delete_in": lambda: {"category_id": {"$in": DELETE_TARGET_CATEGORIES}},
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
