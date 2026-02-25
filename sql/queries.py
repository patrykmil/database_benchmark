CRUD_QUERIES = {
    "insert_single": {
        "query": "INSERT INTO users (name, email, created_at, preferences) VALUES (%s, %s, %s, %s)",
        "params": lambda: (
            "test_user",
            "test@example.com",
            "2024-01-01",
            '{"theme": "dark"}',
        ),
    },
    "insert_bulk": {
        "query": "INSERT INTO users (name, email, created_at, preferences) VALUES ",
        "bulk": True,
        "params": lambda: (),
    },
    "select_single": {
        "query": "SELECT * FROM users WHERE id = %s",
        "params": lambda: (1,),
    },
    "select_where": {
        "query": "SELECT * FROM users WHERE email LIKE %s",
        "params": lambda: ("%test%",),
    },
    "select_join": {
        "query": """SELECT o.id, u.name, o.total, o.status 
                    FROM orders o 
                    JOIN users u ON o.user_id = u.id 
                    LIMIT 100""",
        "params": lambda: (),
    },
    "update_single": {
        "query": "UPDATE users SET name = %s WHERE id = %s",
        "params": lambda: ("updated_name", 1),
    },
    "update_many": {
        "query": "UPDATE users SET preferences = %s WHERE id BETWEEN %s AND %s",
        "params": lambda: ('{"verified": true}', 1, 1000),
    },
    "delete_single": {
        "query": "DELETE FROM users WHERE id = %s",
        "params": lambda: (999999,),
    },
    "delete_many": {
        "query": "DELETE FROM users WHERE created_at < %s",
        "params": lambda: ("2020-01-01",),
    },
    "aggregate_count": {"query": "SELECT COUNT(*) FROM orders", "params": lambda: ()},
    "aggregate_sum": {"query": "SELECT SUM(total) FROM orders", "params": lambda: ()},
    "aggregate_avg": {"query": "SELECT AVG(price) FROM products", "params": lambda: ()},
}

INDEXED_QUERIES = {
    "insert_indexed": {
        "query": "INSERT INTO products (name, price, category_id, attributes) VALUES (%s, %s, %s, %s)",
        "params": lambda: ("new_product", 99.99, 1, '{"color": "red"}'),
    },
    "select_indexed": {
        "query": "SELECT * FROM products WHERE category_id = %s",
        "params": lambda: (1,),
    },
    "select_range": {
        "query": "SELECT * FROM products WHERE price BETWEEN %s AND %s",
        "params": lambda: (100, 500),
    },
    "select_like": {
        "query": "SELECT * FROM products WHERE name LIKE %s",
        "params": lambda: ("%widget%",),
    },
    "select_order_by": {
        "query": "SELECT * FROM orders ORDER BY created_at DESC LIMIT 100",
        "params": lambda: (),
    },
    "update_indexed": {
        "query": "UPDATE orders SET status = %s WHERE user_id = %s",
        "params": lambda: ("shipped", 1),
    },
    "delete_indexed": {
        "query": "DELETE FROM inventory WHERE product_id = %s",
        "params": lambda: (1,),
    },
    "select_between": {
        "query": "SELECT * FROM orders WHERE created_at BETWEEN %s AND %s",
        "params": lambda: ("2024-01-01", "2024-12-31"),
    },
    "select_in": {
        "query": "SELECT * FROM products WHERE category_id IN %s",
        "params": lambda: ((1, 2, 3),),
    },
    "select_exists": {
        "query": """SELECT * FROM users u 
                    WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)""",
        "params": lambda: (),
    },
    "select_group_by": {
        "query": "SELECT status, COUNT(*) FROM orders GROUP BY status",
        "params": lambda: (),
    },
    "select_having": {
        "query": "SELECT user_id, SUM(total) as total FROM orders GROUP BY user_id HAVING SUM(total) > %s",
        "params": lambda: (1000,),
    },
}

EXPLAIN_QUERIES = {
    "explain_select": {
        "query": "EXPLAIN ANALYZE SELECT * FROM users WHERE email = %s",
        "params": lambda: ("user1000@example.com",),
    },
    "explain_join": {
        "query": """EXPLAIN ANALYZE 
                    SELECT o.id, u.name, p.name 
                    FROM orders o 
                    JOIN users u ON o.user_id = u.id 
                    JOIN order_items oi ON o.id = oi.order_id 
                    JOIN products p ON oi.product_id = p.id 
                    WHERE o.id <= 1000""",
        "params": lambda: (),
    },
    "explain_aggregate": {
        "query": "EXPLAIN ANALYZE SELECT category_id, AVG(price) FROM products GROUP BY category_id",
        "params": lambda: (),
    },
    "explain_subquery": {
        "query": """EXPLAIN ANALYZE 
                    SELECT * FROM users 
                    WHERE id IN (SELECT user_id FROM orders WHERE total > 1000)""",
        "params": lambda: (),
    },
    "explain_indexed": {
        "query": "EXPLAIN ANALYZE SELECT * FROM products WHERE category_id = 5",
        "params": lambda: (),
    },
    "explain_complex_join": {
        "query": """EXPLAIN ANALYZE 
                    SELECT u.name, COUNT(o.id) as order_count, SUM(o.total) as total_spent
                    FROM users u
                    LEFT JOIN orders o ON u.id = o.user_id
                    WHERE u.created_at > '2023-01-01'
                    GROUP BY u.id
                    HAVING COUNT(o.id) > 5
                    ORDER BY total_spent DESC
                    LIMIT 100""",
        "params": lambda: (),
    },
}

JSON_QUERIES = {
    "json_extract": {
        "query": "SELECT id, name, preferences->>'theme' as theme FROM users WHERE preferences->>'language' = %s",
        "params": lambda: ("en",),
    },
    "json_contains": {
        "query": "SELECT * FROM products WHERE attributes @> %s",
        "params": lambda: ('{"color": "red"}',),
    },
    "json_search": {
        "query": "SELECT * FROM users WHERE preferences @> %s",
        "params": lambda: ('{"notifications": true}',),
    },
}
