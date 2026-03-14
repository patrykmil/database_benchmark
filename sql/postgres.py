import json
import random
import time

import psycopg2
from psycopg2.extras import execute_values

from config import DATABASES
from sql.queries import (
    EXPLAIN_QUERIES,
    INDEXED_QUERIES,
    JSON_QUERIES,
    NONINDEXED_QUERIES,
)
from sql.schema import INDEXES, SCHEMA
from utils.generator import (
    generate_address,
    generate_bulk_addresses,
    generate_bulk_categories,
    generate_bulk_inventory,
    generate_bulk_order_items,
    generate_bulk_orders,
    generate_bulk_payments,
    generate_bulk_products,
    generate_bulk_reviews,
    generate_bulk_users,
    generate_bulk_warehouses,
    generate_category,
    generate_inventory,
    generate_order,
    generate_order_item,
    generate_payment,
    generate_review,
    generate_warehouse,
    split_starting_data,
)
from utils.results import save_explain_result, save_result


class PostgresBenchmark:
    def __init__(self):
        self.config = DATABASES["postgres"]
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.config["host"],
            port=self.config["port"],
            database=self.config["database"],
            user=self.config["user"],
            password=self.config["password"],
        )
        self.conn.autocommit = True

    def close(self):
        if self.conn:
            self.conn.close()

    def setup_schema(self, create_indexes=True):
        with self.conn.cursor() as cur:
            cur.execute(SCHEMA)
            if create_indexes:
                cur.execute(INDEXES)

    def _starting_tables_exist(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    to_regclass('public.users') IS NOT NULL
                    AND to_regclass('public.categories') IS NOT NULL
                    AND to_regclass('public.products') IS NOT NULL
                    AND to_regclass('public.orders') IS NOT NULL
                    AND to_regclass('public.order_items') IS NOT NULL
                    AND to_regclass('public.reviews') IS NOT NULL
                    AND to_regclass('public.warehouses') IS NOT NULL
                    AND to_regclass('public.inventory') IS NOT NULL
                    AND to_regclass('public.addresses') IS NOT NULL
                    AND to_regclass('public.payments') IS NOT NULL
                """
            )
            return cur.fetchone()[0]

    def get_total_record_count(self):
        if not self._starting_tables_exist():
            return None

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM users)
                    + (SELECT COUNT(*) FROM categories)
                    + (SELECT COUNT(*) FROM products)
                    + (SELECT COUNT(*) FROM orders)
                    + (SELECT COUNT(*) FROM order_items)
                    + (SELECT COUNT(*) FROM reviews)
                    + (SELECT COUNT(*) FROM warehouses)
                    + (SELECT COUNT(*) FROM inventory)
                    + (SELECT COUNT(*) FROM addresses)
                    + (SELECT COUNT(*) FROM payments)
                """
            )
            return cur.fetchone()[0]

    def needs_starting_data_refresh(self, target_size):
        current_size = self.get_total_record_count()
        if current_size is None:
            print("Current total record count is unknown, refreshing data.")
            return True
        need = abs(current_size - target_size) > (target_size * 0.05)
        if need:
            print(
                f"Current total record count {current_size:_} differs from target {target_size:_} by more than 5%, refreshing data."
            )
        return need

    def ensure_indexes(self):
        with self.conn.cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_products_price ON products(price)"
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id)")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews(product_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(user_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory(product_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_inventory_warehouse ON inventory(warehouse_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_addresses_user ON addresses(user_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id)"
            )

    def drop_indexes(self):
        with self.conn.cursor() as cur:
            cur.execute("DROP INDEX IF EXISTS idx_users_email")
            cur.execute("DROP INDEX IF EXISTS idx_users_created_at")
            cur.execute("DROP INDEX IF EXISTS idx_products_category")
            cur.execute("DROP INDEX IF EXISTS idx_products_price")
            cur.execute("DROP INDEX IF EXISTS idx_orders_user")
            cur.execute("DROP INDEX IF EXISTS idx_orders_status")
            cur.execute("DROP INDEX IF EXISTS idx_orders_created")
            cur.execute("DROP INDEX IF EXISTS idx_order_items_order")
            cur.execute("DROP INDEX IF EXISTS idx_order_items_product")
            cur.execute("DROP INDEX IF EXISTS idx_reviews_product")
            cur.execute("DROP INDEX IF EXISTS idx_reviews_user")
            cur.execute("DROP INDEX IF EXISTS idx_inventory_product")
            cur.execute("DROP INDEX IF EXISTS idx_inventory_warehouse")
            cur.execute("DROP INDEX IF EXISTS idx_addresses_user")
            cur.execute("DROP INDEX IF EXISTS idx_payments_order")

    def ensure_reference_data(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM categories")
            categories_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM warehouses")
            warehouses_count = cur.fetchone()[0]

        if categories_count == 0 or warehouses_count == 0:
            self.setup_reference_data()

    def setup_reference_data(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO categories (name) VALUES ('cat1'), ('cat2'), ('cat3'), ('cat4'), ('cat5')"
            )
            cur.execute(
                "INSERT INTO warehouses (name, location) VALUES ('wh1', 'loc1'), ('wh2', 'loc2')"
            )

    def bulk_insert_users(self, count):
        users = generate_bulk_users(count)
        data = [
            (u["name"], u["email"], u["created_at"], json.dumps(u["preferences"]))
            for u in users
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO users (name, email, created_at, preferences) VALUES %s",
                data,
                template="(%s, %s, %s, %s)",
            )

    def get_table_count(self, table_name):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cur.fetchone()[0]

    def get_existing_ids(self, table_name):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {table_name} ORDER BY id")
            return [row[0] for row in cur.fetchall()]

    def get_max_id(self, table_name):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}")
            return cur.fetchone()[0]

    def get_ids_to_trim(self, table_name, target_count):
        current_count = self.get_table_count(table_name)
        excess = current_count - target_count
        if excess <= 0:
            return []

        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT id FROM {table_name} ORDER BY id DESC LIMIT %s",
                (excess,),
            )
            return [row[0] for row in cur.fetchall()]

    def delete_ids(self, table_name, ids):
        if not ids:
            return
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table_name} WHERE id = ANY(%s)", (ids,))

    def delete_by_foreign_key(self, table_name, column_name, ids):
        if not ids:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table_name} WHERE {column_name} = ANY(%s)",
                (ids,),
            )

    def insert_users(self, users):
        if not users:
            return
        data = [
            (u["name"], u["email"], u["created_at"], json.dumps(u["preferences"]))
            for u in users
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO users (name, email, created_at, preferences) VALUES %s",
                data,
            )

    def insert_categories(self, categories):
        if not categories:
            return
        data = [(c["name"], c["parent_id"]) for c in categories]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO categories (name, parent_id) VALUES %s",
                data,
            )

    def insert_warehouses(self, warehouses):
        if not warehouses:
            return
        data = [(w["name"], w["location"]) for w in warehouses]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO warehouses (name, location) VALUES %s",
                data,
            )

    def insert_products(self, products):
        if not products:
            return
        data = [
            (p["name"], p["price"], p["category_id"], json.dumps(p["attributes"]))
            for p in products
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO products (name, price, category_id, attributes) VALUES %s",
                data,
            )

    def insert_orders(self, orders):
        if not orders:
            return
        data = [
            (o["user_id"], o["status"], o["total"], o["created_at"]) for o in orders
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO orders (user_id, status, total, created_at) VALUES %s",
                data,
            )

    def insert_order_items(self, order_items):
        if not order_items:
            return
        data = [
            (oi["order_id"], oi["product_id"], oi["quantity"], oi["price"])
            for oi in order_items
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES %s",
                data,
            )

    def insert_reviews(self, reviews):
        if not reviews:
            return
        data = [
            (
                r["user_id"],
                r["product_id"],
                r["rating"],
                r["comment"],
                json.dumps(r["metadata"]),
            )
            for r in reviews
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO reviews (user_id, product_id, rating, comment, metadata) VALUES %s",
                data,
            )

    def insert_inventory(self, inventory):
        if not inventory:
            return
        data = [
            (record["product_id"], record["warehouse_id"], record["quantity"])
            for record in inventory
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO inventory (product_id, warehouse_id, quantity) VALUES %s",
                data,
            )

    def insert_addresses(self, addresses):
        if not addresses:
            return
        data = [
            (
                a["user_id"],
                a["city"],
                a["country"],
                json.dumps(a["details"]),
            )
            for a in addresses
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO addresses (user_id, city, country, details) VALUES %s",
                data,
            )

    def insert_payments(self, payments):
        if not payments:
            return
        data = [
            (p["order_id"], p["method"], p["amount"], json.dumps(p["data"]))
            for p in payments
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO payments (order_id, method, amount, data) VALUES %s",
                data,
            )

    def reconcile_starting_data(self, total_records):
        target_counts = split_starting_data(total_records)

        users_to_delete = self.get_ids_to_trim("users", target_counts["users"])
        categories_to_delete = self.get_ids_to_trim(
            "categories", target_counts["categories"]
        )
        warehouses_to_delete = self.get_ids_to_trim(
            "warehouses", target_counts["warehouses"]
        )
        products_to_delete = self.get_ids_to_trim("products", target_counts["products"])
        orders_to_delete = self.get_ids_to_trim("orders", target_counts["orders"])

        if users_to_delete:
            self.delete_by_foreign_key("payments", "order_id", orders_to_delete)
            with self.conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM payments WHERE order_id IN (SELECT id FROM orders WHERE user_id = ANY(%s))",
                    (users_to_delete,),
                )
                cur.execute(
                    "DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE user_id = ANY(%s))",
                    (users_to_delete,),
                )
                cur.execute(
                    "DELETE FROM orders WHERE user_id = ANY(%s)",
                    (users_to_delete,),
                )
            self.delete_by_foreign_key("reviews", "user_id", users_to_delete)
            self.delete_by_foreign_key("addresses", "user_id", users_to_delete)

        if categories_to_delete:
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE categories SET parent_id = NULL WHERE parent_id = ANY(%s)",
                    (categories_to_delete,),
                )
                cur.execute(
                    "DELETE FROM order_items WHERE product_id IN (SELECT id FROM products WHERE category_id = ANY(%s))",
                    (categories_to_delete,),
                )
                cur.execute(
                    "DELETE FROM reviews WHERE product_id IN (SELECT id FROM products WHERE category_id = ANY(%s))",
                    (categories_to_delete,),
                )
                cur.execute(
                    "DELETE FROM inventory WHERE product_id IN (SELECT id FROM products WHERE category_id = ANY(%s))",
                    (categories_to_delete,),
                )
                cur.execute(
                    "DELETE FROM products WHERE category_id = ANY(%s)",
                    (categories_to_delete,),
                )

        if warehouses_to_delete:
            self.delete_by_foreign_key(
                "inventory", "warehouse_id", warehouses_to_delete
            )

        if products_to_delete:
            self.delete_by_foreign_key("order_items", "product_id", products_to_delete)
            self.delete_by_foreign_key("reviews", "product_id", products_to_delete)
            self.delete_by_foreign_key("inventory", "product_id", products_to_delete)

        if orders_to_delete:
            self.delete_by_foreign_key("payments", "order_id", orders_to_delete)
            self.delete_by_foreign_key("order_items", "order_id", orders_to_delete)

        self.delete_ids("orders", orders_to_delete)
        self.delete_ids("products", products_to_delete)
        self.delete_ids("warehouses", warehouses_to_delete)
        self.delete_ids("categories", categories_to_delete)
        self.delete_ids("users", users_to_delete)

        self.delete_ids(
            "order_items",
            self.get_ids_to_trim("order_items", target_counts["order_items"]),
        )
        self.delete_ids(
            "reviews", self.get_ids_to_trim("reviews", target_counts["reviews"])
        )
        self.delete_ids(
            "inventory",
            self.get_ids_to_trim("inventory", target_counts["inventory"]),
        )
        self.delete_ids(
            "addresses",
            self.get_ids_to_trim("addresses", target_counts["addresses"]),
        )
        self.delete_ids(
            "payments",
            self.get_ids_to_trim("payments", target_counts["payments"]),
        )

        users_missing = target_counts["users"] - self.get_table_count("users")
        if users_missing > 0:
            self.insert_users(generate_bulk_users(users_missing))

        categories_missing = target_counts["categories"] - self.get_table_count(
            "categories"
        )
        if categories_missing > 0:
            existing_category_ids = self.get_existing_ids("categories")
            categories = []
            for _ in range(categories_missing):
                parent_id = (
                    random.choice(existing_category_ids)
                    if existing_category_ids and random.random() > 0.7
                    else None
                )
                category = generate_category(parent_id)
                categories.append(category)
            self.insert_categories(categories)

        warehouses_missing = target_counts["warehouses"] - self.get_table_count(
            "warehouses"
        )
        if warehouses_missing > 0:
            self.insert_warehouses(
                [generate_warehouse() for _ in range(warehouses_missing)]
            )

        user_ids = self.get_existing_ids("users")
        category_ids = self.get_existing_ids("categories")
        warehouse_ids = self.get_existing_ids("warehouses")
        product_ids = self.get_existing_ids("products")
        order_ids = self.get_existing_ids("orders")

        products_missing = target_counts["products"] - self.get_table_count("products")
        if products_missing > 0 and category_ids:
            self.insert_products(generate_bulk_products(products_missing, category_ids))

        user_ids = self.get_existing_ids("users")
        product_ids = self.get_existing_ids("products")

        orders_missing = target_counts["orders"] - self.get_table_count("orders")
        if orders_missing > 0 and user_ids:
            self.insert_orders(
                [generate_order(random.choice(user_ids)) for _ in range(orders_missing)]
            )

        order_ids = self.get_existing_ids("orders")

        order_items_missing = target_counts["order_items"] - self.get_table_count(
            "order_items"
        )
        if order_items_missing > 0 and order_ids and product_ids:
            self.insert_order_items(
                [
                    generate_order_item(
                        random.choice(order_ids),
                        random.choice(product_ids),
                    )
                    for _ in range(order_items_missing)
                ]
            )

        reviews_missing = target_counts["reviews"] - self.get_table_count("reviews")
        if reviews_missing > 0 and user_ids and product_ids:
            self.insert_reviews(
                [
                    generate_review(
                        random.choice(user_ids),
                        random.choice(product_ids),
                    )
                    for _ in range(reviews_missing)
                ]
            )

        inventory_missing = target_counts["inventory"] - self.get_table_count(
            "inventory"
        )
        if inventory_missing > 0 and product_ids and warehouse_ids:
            self.insert_inventory(
                [
                    generate_inventory(
                        random.choice(product_ids),
                        random.choice(warehouse_ids),
                    )
                    for _ in range(inventory_missing)
                ]
            )

        addresses_missing = target_counts["addresses"] - self.get_table_count(
            "addresses"
        )
        if addresses_missing > 0 and user_ids:
            self.insert_addresses(
                [
                    generate_address(random.choice(user_ids))
                    for _ in range(addresses_missing)
                ]
            )

        payments_missing = target_counts["payments"] - self.get_table_count("payments")
        if payments_missing > 0 and order_ids:
            self.insert_payments(
                [
                    generate_payment(random.choice(order_ids))
                    for _ in range(payments_missing)
                ]
            )

    def populate_starting_data(self, total_records):
        counts = split_starting_data(total_records)

        users = generate_bulk_users(counts["users"])
        users_data = [
            (u["name"], u["email"], u["created_at"], json.dumps(u["preferences"]))
            for u in users
        ]

        categories = generate_bulk_categories(counts["categories"])
        categories_data = [(c["name"], c["parent_id"]) for c in categories]

        warehouses = generate_bulk_warehouses(counts["warehouses"])
        warehouses_data = [(w["name"], w["location"]) for w in warehouses]

        category_ids = list(range(1, counts["categories"] + 1))
        products = generate_bulk_products(counts["products"], category_ids)
        products_data = [
            (p["name"], p["price"], p["category_id"], json.dumps(p["attributes"]))
            for p in products
        ]

        orders = generate_bulk_orders(counts["orders"], counts["users"])
        orders_data = [
            (o["user_id"], o["status"], o["total"], o["created_at"]) for o in orders
        ]

        order_items = generate_bulk_order_items(
            counts["order_items"], counts["orders"], counts["products"]
        )
        order_items_data = [
            (oi["order_id"], oi["product_id"], oi["quantity"], oi["price"])
            for oi in order_items
        ]

        reviews = generate_bulk_reviews(
            counts["reviews"], counts["users"], counts["products"]
        )
        reviews_data = [
            (
                r["user_id"],
                r["product_id"],
                r["rating"],
                r["comment"],
                json.dumps(r["metadata"]),
            )
            for r in reviews
        ]

        inventory = generate_bulk_inventory(
            counts["inventory"], counts["products"], counts["warehouses"]
        )
        inventory_data = [
            (i["product_id"], i["warehouse_id"], i["quantity"]) for i in inventory
        ]

        addresses = generate_bulk_addresses(counts["addresses"], counts["users"])
        addresses_data = [
            (
                a["user_id"],
                a["city"],
                a["country"],
                json.dumps(a["details"]),
            )
            for a in addresses
        ]

        payments = generate_bulk_payments(counts["payments"], counts["orders"])
        payments_data = [
            (p["order_id"], p["method"], p["amount"], json.dumps(p["data"]))
            for p in payments
        ]

        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO users (name, email, created_at, preferences) VALUES %s",
                users_data,
            )
            execute_values(
                cur,
                "INSERT INTO categories (name, parent_id) VALUES %s",
                categories_data,
            )
            execute_values(
                cur,
                "INSERT INTO warehouses (name, location) VALUES %s",
                warehouses_data,
            )
            execute_values(
                cur,
                "INSERT INTO products (name, price, category_id, attributes) VALUES %s",
                products_data,
            )
            execute_values(
                cur,
                "INSERT INTO orders (user_id, status, total, created_at) VALUES %s",
                orders_data,
            )
            execute_values(
                cur,
                "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES %s",
                order_items_data,
            )
            execute_values(
                cur,
                "INSERT INTO reviews (user_id, product_id, rating, comment, metadata) VALUES %s",
                reviews_data,
            )
            execute_values(
                cur,
                "INSERT INTO inventory (product_id, warehouse_id, quantity) VALUES %s",
                inventory_data,
            )
            execute_values(
                cur,
                "INSERT INTO addresses (user_id, city, country, details) VALUES %s",
                addresses_data,
            )
            execute_values(
                cur,
                "INSERT INTO payments (order_id, method, amount, data) VALUES %s",
                payments_data,
            )

    def cleanup_benchmark_rows(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM payments
                WHERE order_id IN (
                    SELECT o.id
                    FROM orders o
                    JOIN users u ON u.id = o.user_id
                    WHERE u.email IN (
                        'single@example.com',
                        'ignore@example.com',
                        'upsert@example.com',
                        'returning@example.com',
                        'indexed@example.com'
                    )
                )
                """
            )
            cur.execute(
                """
                DELETE FROM order_items
                WHERE order_id IN (
                    SELECT o.id
                    FROM orders o
                    JOIN users u ON u.id = o.user_id
                    WHERE u.email IN (
                        'single@example.com',
                        'ignore@example.com',
                        'upsert@example.com',
                        'returning@example.com',
                        'indexed@example.com'
                    )
                )
                """
            )
            cur.execute(
                """
                DELETE FROM orders
                WHERE user_id IN (
                    SELECT id FROM users
                    WHERE email IN (
                        'single@example.com',
                        'ignore@example.com',
                        'upsert@example.com',
                        'returning@example.com',
                        'indexed@example.com'
                    )
                )
                """
            )
            cur.execute(
                """
                DELETE FROM reviews
                WHERE user_id IN (
                    SELECT id FROM users
                    WHERE email IN (
                        'single@example.com',
                        'ignore@example.com',
                        'upsert@example.com',
                        'returning@example.com',
                        'indexed@example.com'
                    )
                )
                """
            )
            cur.execute(
                """
                DELETE FROM addresses
                WHERE user_id IN (
                    SELECT id FROM users
                    WHERE email IN (
                        'single@example.com',
                        'ignore@example.com',
                        'upsert@example.com',
                        'returning@example.com',
                        'indexed@example.com'
                    )
                )
                """
            )
            cur.execute(
                """
                DELETE FROM users
                WHERE email IN (
                    'single@example.com',
                    'ignore@example.com',
                    'upsert@example.com',
                    'returning@example.com',
                    'indexed@example.com'
                )
                """
            )

    def cleanup_delete_targets(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM order_items
                WHERE product_id IN (
                    SELECT id FROM products WHERE category_id IN (100, 101)
                )
                """
            )
            cur.execute(
                """
                DELETE FROM reviews
                WHERE product_id IN (
                    SELECT id FROM products WHERE category_id IN (100, 101)
                )
                """
            )
            cur.execute(
                """
                DELETE FROM inventory
                WHERE product_id IN (
                    SELECT id FROM products WHERE category_id IN (100, 101)
                )
                """
            )
            cur.execute(
                """
                DELETE FROM payments
                WHERE order_id IN (
                    SELECT id FROM orders WHERE user_id IN (1, 100, 101, 102)
                )
                """
            )
            cur.execute(
                """
                DELETE FROM order_items
                WHERE order_id IN (
                    SELECT id FROM orders WHERE user_id IN (1, 100, 101, 102)
                )
                """
            )
            cur.execute("DELETE FROM orders WHERE user_id IN (1, 100, 101, 102)")
            cur.execute("DELETE FROM reviews WHERE user_id IN (1, 100, 101, 102)")
            cur.execute("DELETE FROM addresses WHERE user_id IN (1, 100, 101, 102)")

    def ensure_addresses_volume(self, total_records):
        target_addresses = split_starting_data(total_records)["addresses"]
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM addresses")
            current_addresses = cur.fetchone()[0]
            if current_addresses >= target_addresses:
                return

            cur.execute("SELECT id FROM users")
            user_ids = [row[0] for row in cur.fetchall()]
            if not user_ids:
                return

            missing = target_addresses - current_addresses
            address_rows = []
            for _ in range(missing):
                address = generate_address(random.choice(user_ids))
                address_rows.append(
                    (
                        address["user_id"],
                        address["city"],
                        address["country"],
                        json.dumps(address["details"]),
                    )
                )

            execute_values(
                cur,
                "INSERT INTO addresses (user_id, city, country, details) VALUES %s",
                address_rows,
            )

    def run_nonindexed_queries(self, size, trial=1):
        self.cleanup_benchmark_rows()
        self.cleanup_delete_targets()
        results = {}
        for name, q in NONINDEXED_QUERIES.items():
            params = q["params"]()
            if name == "insert_bulk":
                users = generate_bulk_users(1000)
                data = [
                    (
                        u["name"],
                        u["email"],
                        u["created_at"],
                        json.dumps(u["preferences"]),
                    )
                    for u in users
                ]
                query = q["query"] + "(%s, %s, %s, %s)" + ",(%s, %s, %s, %s)" * 999
                flat_data = []
                for u in data:
                    flat_data.extend(u)
                start = time.time()
                with self.conn.cursor() as cur:
                    cur.execute(query, flat_data)
                elapsed = (time.time() - start) * 1000
            elif name == "insert_many":
                cats = [(f"cat{i}",) for i in range(100)]
                query = q["query"] + "(%s)" + ",(%s)" * 99
                flat_data = []
                for c in cats:
                    flat_data.extend(c)
                start = time.time()
                with self.conn.cursor() as cur:
                    cur.execute(query, flat_data)
                elapsed = (time.time() - start) * 1000
            else:
                start = time.time()
                with self.conn.cursor() as cur:
                    cur.execute(q["query"], params)
                    if name.startswith("select"):
                        cur.fetchall()
                elapsed = (time.time() - start) * 1000

            results[name] = elapsed
            save_result("postgres", name, size, elapsed, size, trial=trial)
        self.ensure_addresses_volume(size)
        return results

    def run_indexed_queries(self, size, trial=1):
        self.cleanup_benchmark_rows()
        self.cleanup_delete_targets()
        results = {}
        for name, q in INDEXED_QUERIES.items():
            params = q["params"]()
            if name == "index_insert_bulk":
                users = generate_bulk_users(1000)
                data = [
                    (
                        u["name"],
                        u["email"],
                        u["created_at"],
                        json.dumps(u["preferences"]),
                    )
                    for u in users
                ]
                query = q["query"] + "(%s, %s, %s, %s)" + ",(%s, %s, %s, %s)" * 999
                flat_data = []
                for u in data:
                    flat_data.extend(u)
                start = time.time()
                with self.conn.cursor() as cur:
                    cur.execute(query, flat_data)
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_many":
                prods = [
                    (f"product{i}", 10.0, 1, '{"color": "red"}') for i in range(100)
                ]
                query = q["query"] + "(%s, %s, %s, %s)" + ",(%s, %s, %s, %s)" * 99
                flat_data = []
                for p in prods:
                    flat_data.extend(p)
                start = time.time()
                with self.conn.cursor() as cur:
                    cur.execute(query, flat_data)
                elapsed = (time.time() - start) * 1000
            else:
                start = time.time()
                with self.conn.cursor() as cur:
                    cur.execute(q["query"], params)
                    if name.startswith("select"):
                        cur.fetchall()
                elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("postgres", name, size, elapsed, size, trial=trial)
        self.ensure_addresses_volume(size)
        return results

    def run_explain_queries(self, trial=1):
        for name, q in EXPLAIN_QUERIES.items():
            params = q["params"]()
            start = time.time()
            with self.conn.cursor() as cur:
                cur.execute(q["query"], params)
                plan = cur.fetchall()
            elapsed = (time.time() - start) * 1000
            plan_text = "\n".join([str(row) for row in plan])
            save_explain_result("postgres", name, plan_text, elapsed, trial=trial)
        return True

    def run_json_queries(self, size, trial=1):
        results = {}
        for name, q in JSON_QUERIES.items():
            params = q["params"]()
            start = time.time()
            with self.conn.cursor() as cur:
                cur.execute(q["query"], params)
                cur.fetchall()
            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("postgres", f"json_{name}", size, elapsed, size, trial=trial)
        return results


def run_postgres_benchmark(size, operation_type="all", trial=1):
    bench = PostgresBenchmark()
    bench.connect()

    try:
        if operation_type in ["all", "nonindexed"]:
            if bench.get_total_record_count() is None:
                bench.setup_schema(create_indexes=False)
                bench.populate_starting_data(size)
            elif bench.needs_starting_data_refresh(size):
                bench.drop_indexes()
                bench.reconcile_starting_data(size)
            else:
                bench.drop_indexes()
            bench.run_nonindexed_queries(size, trial=trial)

        if operation_type in ["all", "indexed"]:
            if bench.get_total_record_count() is None:
                bench.setup_schema(create_indexes=True)
                bench.populate_starting_data(size)
            elif bench.needs_starting_data_refresh(size):
                bench.ensure_indexes()
                bench.reconcile_starting_data(size)
            else:
                bench.ensure_indexes()
            bench.run_indexed_queries(size, trial=trial)

        if operation_type in ["explain"]:
            bench.run_explain_queries(trial=trial)

        if operation_type in ["all", "json"]:
            bench.run_json_queries(size, trial=trial)

    finally:
        bench.close()


if __name__ == "__main__":
    run_postgres_benchmark(1000, "all")
