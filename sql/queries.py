CRUD_QUERIES = {
    # CREATE - 6 queries
    "insert_single": {
        "query": "INSERT INTO users (name, email, created_at, preferences) VALUES (%s, %s, %s, %s)",
        "params": lambda: (
            "test_user_single",
            "single@example.com",
            "2024-01-01",
            '{"theme": "dark"}',
        ),
    },
    "insert_bulk": {
        "query": "INSERT INTO users (name, email, created_at, preferences) VALUES ",
        "bulk": True,
        "params": lambda: (),
    },
    "insert_ignore": {
        "query": "INSERT INTO users (name, email, created_at, preferences) VALUES (%s, %s, %s, %s) ON CONFLICT (email) DO NOTHING",
        "params": lambda: (
            "test_user_ignore",
            "ignore@example.com",
            "2024-01-01",
            '{"theme": "dark"}',
        ),
    },
    "insert_upsert": {
        "query": """INSERT INTO users (name, email, created_at, preferences) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name, preferences = EXCLUDED.preferences""",
        "params": lambda: (
            "test_user_upsert",
            "upsert@example.com",
            "2024-01-01",
            '{"theme": "light"}',
        ),
    },
    "insert_many": {
        "query": "INSERT INTO categories (name) VALUES ",
        "bulk": True,
        "params": lambda: (),
    },
    "insert_returning": {
        "query": "INSERT INTO users (name, email, created_at, preferences) VALUES (%s, %s, %s, %s) RETURNING id",
        "params": lambda: (
            "test_user",
            "returning@example.com",
            "2024-01-01",
            '{"theme": "dark"}',
        ),
    },
    # READ - 6 queries
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
                    JOIN users u ON o.user_id = u.id""",
        "params": lambda: (),
    },
    "select_aggregate": {
        "query": "SELECT COUNT(*), SUM(total), AVG(total) FROM orders WHERE user_id = %s",
        "params": lambda: (1,),
    },
    "select_pagination": {
        "query": "SELECT * FROM users ORDER BY created_at DESC LIMIT %s OFFSET %s",
        "params": lambda: (10, 0),
    },
    "select_distinct": {
        "query": "SELECT DISTINCT status FROM orders",
        "params": lambda: (),
    },
    # UPDATE - 6 queries
    "update_single": {
        "query": "UPDATE users SET name = %s WHERE id = %s",
        "params": lambda: ("updated_name", 1),
    },
    "update_many": {
        "query": "UPDATE users SET preferences = %s WHERE id BETWEEN %s AND %s",
        "params": lambda: ('{"verified": true}', 1, 1000),
    },
    "update_in": {
        "query": "UPDATE users SET name = %s WHERE id IN (%s, %s, %s)",
        "params": lambda: ("updated_user", 1, 2, 3),
    },
    "update_case": {
        "query": "UPDATE users SET name = CASE WHEN id = %s THEN %s WHEN id = %s THEN %s ELSE name END",
        "params": lambda: (1, "user_active", 2, "user_inactive"),
    },
    "update_join": {
        "query": """UPDATE orders SET status = %s WHERE user_id = 1""",
        "params": lambda: ("processed",),
    },
    "update_upsert": {
        "query": """INSERT INTO products (name, price, category_id, attributes) VALUES (%s, %s, %s, %s)""",
        "params": lambda: ("existing_product", 29.99, 1, '{"color": "blue"}'),
    },
    # DELETE - 6 queries
    "delete_single": {
        "query": "DELETE FROM users WHERE id = %s",
        "params": lambda: (999999,),
    },
    "delete_many": {
        "query": "DELETE FROM users WHERE created_at < %s",
        "params": lambda: ("2020-01-01",),
    },
    "delete_in": {
        "query": "DELETE FROM users WHERE id IN (%s, %s, %s)",
        "params": lambda: (100, 101, 102),
    },
    "delete_cascade": {
        "query": "DELETE FROM orders WHERE user_id = %s",
        "params": lambda: (1,),
    },
    "delete_join": {
        "query": """DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE created_at < %s)""",
        "params": lambda: ("2023-01-01",),
    },
    "delete_truncate": {
        "query": "DELETE FROM addresses",
        "params": lambda: (),
    },
}

INDEXED_QUERIES = {
    # CREATE - 6 queries (using indexed fields)
    "index_insert_single": {
        "query": "INSERT INTO users (name, email, created_at, preferences) VALUES (%s, %s, %s, %s)",
        "params": lambda: (
            "indexed_user",
            "indexed@example.com",
            "2024-01-01",
            '{"theme": "dark"}',
        ),
    },
    "index_insert_bulk": {
        "query": "INSERT INTO users (name, email, created_at, preferences) VALUES ",
        "bulk": True,
        "params": lambda: (),
    },
    "index_insert_ignore": {
        "query": "INSERT INTO products (name, price, category_id, attributes) VALUES (%s, %s, %s, %s)",
        "params": lambda: (
            "indexed_product",
            99.99,
            1,
            '{"color": "red"}',
        ),
    },
    "index_insert_upsert": {
        "query": """INSERT INTO products (name, price, category_id, attributes) VALUES (%s, %s, %s, %s)""",
        "params": lambda: (
            "upsert_product",
            49.99,
            1,
            '{"color": "blue"}',
        ),
    },
    "index_insert_many": {
        "query": "INSERT INTO products (name, price, category_id, attributes) VALUES ",
        "bulk": True,
        "params": lambda: (),
    },
    "index_insert_returning": {
        "query": "INSERT INTO products (name, price, category_id, attributes) VALUES (%s, %s, %s, %s) RETURNING id",
        "params": lambda: (
            "returning_product",
            19.99,
            1,
            '{"color": "green"}',
        ),
    },
    # READ - 6 queries (using indexed fields: email, category_id, price, user_id, status, created_at)
    "index_select_single": {
        "query": "SELECT * FROM users WHERE email = %s",
        "params": lambda: ("user1000@example.com",),
    },
    "index_select_where": {
        "query": "SELECT * FROM users WHERE created_at >= %s",
        "params": lambda: ("2024-01-01",),
    },
    "index_select_join": {
        "query": """SELECT o.id, u.name, o.total, o.status 
                    FROM orders o 
                    JOIN users u ON o.user_id = u.id 
                    WHERE u.created_at >= %s""",
        "params": lambda: ("2024-01-01",),
    },
    "index_select_aggregate": {
        "query": "SELECT COUNT(*), SUM(total), AVG(total) FROM orders WHERE user_id = %s",
        "params": lambda: (1,),
    },
    "index_select_pagination": {
        "query": "SELECT * FROM orders ORDER BY created_at DESC LIMIT %s OFFSET %s",
        "params": lambda: (10, 0),
    },
    "index_select_distinct": {
        "query": "SELECT DISTINCT status FROM orders",
        "params": lambda: (),
    },
    # UPDATE - 6 queries (using indexed fields)
    "index_update_single": {
        "query": "UPDATE users SET name = %s WHERE email = %s",
        "params": lambda: ("updated_email_user", "user1@example.com"),
    },
    "index_update_many": {
        "query": "UPDATE products SET price = price * %s WHERE category_id = %s",
        "params": lambda: (1.1, 1),
    },
    "index_update_in": {
        "query": "UPDATE products SET price = %s WHERE category_id IN (%s, %s, %s)",
        "params": lambda: (9.99, 1, 2, 3),
    },
    "index_update_case": {
        "query": "UPDATE orders SET status = CASE WHEN id = %s THEN %s WHEN id = %s THEN %s ELSE status END",
        "params": lambda: (1, "shipped", 2, "delivered"),
    },
    "index_update_join": {
        "query": """UPDATE orders SET status = %s WHERE user_id = 1""",
        "params": lambda: ("processed",),
    },
    "index_update_upsert": {
        "query": """INSERT INTO products (name, price, category_id, attributes) VALUES (%s, %s, %s, %s)""",
        "params": lambda: ("existing_product", 29.99, 1, '{"color": "blue"}'),
    },
    # DELETE - 6 queries (using indexed fields)
    "index_delete_single": {
        "query": "DELETE FROM users WHERE email = %s",
        "params": lambda: ("delete@example.com",),
    },
    "index_delete_many": {
        "query": "DELETE FROM users WHERE created_at < %s",
        "params": lambda: ("2020-01-01",),
    },
    "index_delete_in": {
        "query": "DELETE FROM products WHERE category_id IN (%s, %s)",
        "params": lambda: (100, 101),
    },
    "index_delete_cascade": {
        "query": "DELETE FROM orders WHERE user_id = %s",
        "params": lambda: (1,),
    },
    "index_delete_join": {
        "query": """DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE created_at < %s)""",
        "params": lambda: ("2023-01-01",),
    },
    "index_delete_truncate": {
        "query": "DELETE FROM addresses",
        "params": lambda: (),
    },
}

EXPLAIN_QUERIES = {
    "explain_insert": {
        "query": "EXPLAIN INSERT INTO users (name, email, created_at, preferences) VALUES (%s, %s, %s, %s)",
        "params": lambda: (
            "explain_user",
            "explain@example.com",
            "2024-01-01",
            '{"theme": "dark"}',
        ),
    },
    "explain_select": {
        "query": "EXPLAIN ANALYZE SELECT * FROM users WHERE email = %s",
        "params": lambda: ("user1000@example.com",),
    },
    "explain_select_where": {
        "query": "EXPLAIN ANALYZE SELECT * FROM users WHERE created_at >= %s",
        "params": lambda: ("2024-01-01",),
    },
    "explain_select_join": {
        "query": """EXPLAIN ANALYZE 
                    SELECT o.id, u.name, o.total, o.status 
                    FROM orders o 
                    JOIN users u ON o.user_id = u.id 
                    WHERE u.created_at >= %s""",
        "params": lambda: ("2024-01-01",),
    },
    "explain_select_aggregate": {
        "query": "EXPLAIN ANALYZE SELECT COUNT(*), SUM(total), AVG(total) FROM orders WHERE user_id = %s",
        "params": lambda: (1,),
    },
    "explain_select_pagination": {
        "query": "EXPLAIN ANALYZE SELECT * FROM orders ORDER BY created_at DESC LIMIT %s OFFSET %s",
        "params": lambda: (10, 0),
    },
    "explain_select_distinct": {
        "query": "EXPLAIN ANALYZE SELECT DISTINCT status FROM orders",
        "params": lambda: (),
    },
    "explain_update": {
        "query": "EXPLAIN ANALYZE UPDATE users SET name = %s WHERE email = %s",
        "params": lambda: ("updated_email_user", "user1@example.com"),
    },
    "explain_update_many": {
        "query": "EXPLAIN ANALYZE UPDATE products SET price = price * %s WHERE category_id = %s",
        "params": lambda: (1.1, 1),
    },
    "explain_update_in": {
        "query": "EXPLAIN ANALYZE UPDATE products SET price = %s WHERE category_id IN (%s, %s, %s)",
        "params": lambda: (9.99, 1, 2, 3),
    },
    "explain_delete": {
        "query": "EXPLAIN ANALYZE DELETE FROM users WHERE email = %s",
        "params": lambda: ("delete@example.com",),
    },
    "explain_delete_many": {
        "query": "EXPLAIN ANALYZE DELETE FROM users WHERE created_at < %s",
        "params": lambda: ("2020-01-01",),
    },
    "explain_delete_in": {
        "query": "EXPLAIN ANALYZE DELETE FROM products WHERE category_id IN (%s, %s)",
        "params": lambda: (100, 101),
    },
    "explain_delete_cascade": {
        "query": "EXPLAIN ANALYZE DELETE FROM orders WHERE user_id = %s",
        "params": lambda: (1,),
    },
    "explain_indexed_select": {
        "query": "EXPLAIN ANALYZE SELECT * FROM products WHERE category_id = %s",
        "params": lambda: (1,),
    },
    "explain_indexed_range": {
        "query": "EXPLAIN ANALYZE SELECT * FROM products WHERE price BETWEEN %s AND %s",
        "params": lambda: (100, 500),
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
