import json
import random
import sqlite3
import time

from config import DATABASES
from sql.queries import (
    EXPLAIN_QUERIES,
    INDEXED_QUERIES,
    JSON_QUERIES,
    NONINDEXED_QUERIES,
)
from sql.schema import SQLITE_INDEXES, SQLITE_SCHEMA
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
    split_starting_data,
)
from utils.results import save_explain_result, save_result


def to_sqlite_query(query):
    return query.replace("%s", "?")


class SQLiteBenchmark:
    def __init__(self):
        self.config = DATABASES["sqlite"]
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.config["database"])
        self.conn.row_factory = sqlite3.Row

    def close(self):
        if self.conn:
            self.conn.close()

    def setup_schema(self, create_indexes=True):
        cur = self.conn.cursor()
        for stmt in SQLITE_SCHEMA.split(";"):
            if stmt.strip():
                cur.execute(stmt)
        if create_indexes:
            for stmt in SQLITE_INDEXES.split(";"):
                if stmt.strip():
                    cur.execute(stmt)
        self.conn.commit()

    def _starting_tables_exist(self):
        cur = self.conn.cursor()
        required = [
            "users",
            "categories",
            "products",
            "orders",
            "order_items",
            "reviews",
            "warehouses",
            "inventory",
            "addresses",
            "payments",
        ]
        for table in required:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            if cur.fetchone() is None:
                return False
        return True

    def get_total_record_count(self):
        if not self._starting_tables_exist():
            return None

        cur = self.conn.cursor()
        tables = [
            "users",
            "categories",
            "products",
            "orders",
            "order_items",
            "reviews",
            "warehouses",
            "inventory",
            "addresses",
            "payments",
        ]
        total = 0
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            total += cur.fetchone()[0]
        return total

    def needs_starting_data_refresh(self, target_size):
        current_size = self.get_total_record_count()
        if current_size is None:
            return True
        return abs(current_size - target_size) > (target_size * 0.05)

    def ensure_indexes(self):
        cur = self.conn.cursor()
        for stmt in SQLITE_INDEXES.split(";"):
            if stmt.strip():
                cur.execute(stmt)
        self.conn.commit()

    def drop_indexes(self):
        cur = self.conn.cursor()
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
        self.conn.commit()

    def ensure_reference_data(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM categories")
        categories_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM warehouses")
        warehouses_count = cur.fetchone()[0]

        if categories_count == 0 or warehouses_count == 0:
            self.setup_reference_data()

    def setup_reference_data(self):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO categories (name) VALUES ('cat1'), ('cat2'), ('cat3'), ('cat4'), ('cat5')"
        )
        cur.execute(
            "INSERT INTO warehouses (name, location) VALUES ('wh1', 'loc1'), ('wh2', 'loc2')"
        )
        self.conn.commit()

    def bulk_insert_users(self, count):
        users = generate_bulk_users(count)
        cur = self.conn.cursor()
        cur.executemany(
            "INSERT INTO users (name, email, created_at, preferences) VALUES (?, ?, ?, ?)",
            [
                (
                    u["name"],
                    u["email"],
                    str(u["created_at"]),
                    json.dumps(u["preferences"]),
                )
                for u in users
            ],
        )
        self.conn.commit()

    def populate_starting_data(self, total_records):
        counts = split_starting_data(total_records)

        users = generate_bulk_users(counts["users"])
        categories = generate_bulk_categories(counts["categories"])
        warehouses = generate_bulk_warehouses(counts["warehouses"])
        products = generate_bulk_products(
            counts["products"], list(range(1, counts["categories"] + 1))
        )
        orders = generate_bulk_orders(counts["orders"], counts["users"])
        order_items = generate_bulk_order_items(
            counts["order_items"], counts["orders"], counts["products"]
        )
        reviews = generate_bulk_reviews(
            counts["reviews"], counts["users"], counts["products"]
        )
        inventory = generate_bulk_inventory(
            counts["inventory"], counts["products"], counts["warehouses"]
        )
        addresses = generate_bulk_addresses(counts["addresses"], counts["users"])
        payments = generate_bulk_payments(counts["payments"], counts["orders"])

        cur = self.conn.cursor()
        cur.executemany(
            "INSERT INTO users (name, email, created_at, preferences) VALUES (?, ?, ?, ?)",
            [
                (
                    u["name"],
                    u["email"],
                    str(u["created_at"]),
                    json.dumps(u["preferences"]),
                )
                for u in users
            ],
        )
        cur.executemany(
            "INSERT INTO categories (name, parent_id) VALUES (?, ?)",
            [(c["name"], c["parent_id"]) for c in categories],
        )
        cur.executemany(
            "INSERT INTO warehouses (name, location) VALUES (?, ?)",
            [(w["name"], w["location"]) for w in warehouses],
        )
        cur.executemany(
            "INSERT INTO products (name, price, category_id, attributes) VALUES (?, ?, ?, ?)",
            [
                (
                    p["name"],
                    p["price"],
                    p["category_id"],
                    json.dumps(p["attributes"]),
                )
                for p in products
            ],
        )
        cur.executemany(
            "INSERT INTO orders (user_id, status, total, created_at) VALUES (?, ?, ?, ?)",
            [
                (o["user_id"], o["status"], o["total"], str(o["created_at"]))
                for o in orders
            ],
        )
        cur.executemany(
            "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)",
            [
                (oi["order_id"], oi["product_id"], oi["quantity"], oi["price"])
                for oi in order_items
            ],
        )
        cur.executemany(
            "INSERT INTO reviews (user_id, product_id, rating, comment, metadata) VALUES (?, ?, ?, ?, ?)",
            [
                (
                    r["user_id"],
                    r["product_id"],
                    r["rating"],
                    r["comment"],
                    json.dumps(r["metadata"]),
                )
                for r in reviews
            ],
        )
        cur.executemany(
            "INSERT INTO inventory (product_id, warehouse_id, quantity) VALUES (?, ?, ?)",
            [(i["product_id"], i["warehouse_id"], i["quantity"]) for i in inventory],
        )
        cur.executemany(
            "INSERT INTO addresses (user_id, city, country, details) VALUES (?, ?, ?, ?)",
            [
                (
                    a["user_id"],
                    a["city"],
                    a["country"],
                    json.dumps(a["details"]),
                )
                for a in addresses
            ],
        )
        cur.executemany(
            "INSERT INTO payments (order_id, method, amount, data) VALUES (?, ?, ?, ?)",
            [
                (p["order_id"], p["method"], p["amount"], json.dumps(p["data"]))
                for p in payments
            ],
        )
        self.conn.commit()

    def cleanup_benchmark_rows(self):
        cur = self.conn.cursor()
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
        self.conn.commit()

    def cleanup_delete_targets(self):
        cur = self.conn.cursor()
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
        self.conn.commit()

    def ensure_addresses_volume(self, total_records):
        target_addresses = split_starting_data(total_records)["addresses"]
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM addresses")
        current_addresses = cur.fetchone()[0]
        if current_addresses >= target_addresses:
            return

        cur.execute("SELECT id FROM users")
        user_ids = [row[0] for row in cur.fetchall()]
        if not user_ids:
            return

        missing = target_addresses - current_addresses
        data = []
        for _ in range(missing):
            address = generate_address(random.choice(user_ids))
            data.append(
                (
                    address["user_id"],
                    address["city"],
                    address["country"],
                    json.dumps(address["details"]),
                )
            )

        cur.executemany(
            "INSERT INTO addresses (user_id, city, country, details) VALUES (?, ?, ?, ?)",
            data,
        )
        self.conn.commit()

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
                        str(u["created_at"]),
                        json.dumps(u["preferences"]),
                    )
                    for u in users
                ]
                start = time.time()
                cur = self.conn.cursor()
                cur.executemany(
                    q["query"].replace("VALUES", "VALUES (?)").split("VALUES")[0]
                    + "VALUES (?, ?, ?, ?)",
                    data,
                )
                self.conn.commit()
                elapsed = (time.time() - start) * 1000
            elif name == "insert_many":
                cats = [(f"cat{i}",) for i in range(100)]
                start = time.time()
                cur = self.conn.cursor()
                cur.executemany(
                    q["query"].replace("VALUES", "VALUES (?)").split("VALUES")[0]
                    + "VALUES (?)",
                    cats,
                )
                self.conn.commit()
                elapsed = (time.time() - start) * 1000
            else:
                start = time.time()
                cur = self.conn.cursor()
                cur.execute(to_sqlite_query(q["query"]), params)
                if name.startswith("select"):
                    cur.fetchall()
                elapsed = (time.time() - start) * 1000

            results[name] = elapsed
            save_result("sqlite", name, size, elapsed, size, trial=trial)
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
                        str(u["created_at"]),
                        json.dumps(u["preferences"]),
                    )
                    for u in users
                ]
                start = time.time()
                cur = self.conn.cursor()
                cur.executemany(
                    q["query"].replace("VALUES", "VALUES (?)").split("VALUES")[0]
                    + "VALUES (?, ?, ?, ?)",
                    data,
                )
                self.conn.commit()
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_many":
                prods = [
                    (f"product{i}", 10.0, 1, '{"color": "red"}') for i in range(100)
                ]
                start = time.time()
                cur = self.conn.cursor()
                cur.executemany(
                    q["query"].replace("VALUES", "VALUES (?)").split("VALUES")[0]
                    + "VALUES (?, ?, ?, ?)",
                    prods,
                )
                self.conn.commit()
                elapsed = (time.time() - start) * 1000
            else:
                start = time.time()
                cur = self.conn.cursor()
                cur.execute(to_sqlite_query(q["query"]), params)
                if name.startswith("select"):
                    cur.fetchall()
                elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("sqlite", name, size, elapsed, size, trial=trial)
        self.ensure_addresses_volume(size)
        return results

    def run_explain_queries(self, trial=1):
        for name, q in EXPLAIN_QUERIES.items():
            params = q["params"]()
            start = time.time()
            cur = self.conn.cursor()
            query = q["query"].replace("EXPLAIN ANALYZE", "EXPLAIN QUERY PLAN")
            query = to_sqlite_query(query)
            cur.execute(query, params)
            plan = cur.fetchall()
            elapsed = (time.time() - start) * 1000
            plan_text = "\n".join([str(list(row)) for row in plan])
            save_explain_result("sqlite", name, plan_text, elapsed, trial=trial)
        return True

    def run_json_queries(self, size, trial=1):
        results = {}
        for name, q in JSON_QUERIES.items():
            params = q["params"]()
            query = q["query"].replace("->>", "json_extract").replace("@>", "json_each")
            query = to_sqlite_query(query)
            start = time.time()
            cur = self.conn.cursor()
            try:
                cur.execute(query, params)
                cur.fetchall()
            except:
                pass
            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("sqlite", f"json_{name}", size, elapsed, size, trial=trial)
        return results


def run_sqlite_benchmark(size, operation_type="all", trial=1):
    bench = SQLiteBenchmark()
    bench.connect()

    try:
        if operation_type in ["all", "nonindexed"]:
            if bench.needs_starting_data_refresh(size):
                bench.setup_schema(create_indexes=False)
                bench.populate_starting_data(size)
            else:
                bench.drop_indexes()
            bench.run_nonindexed_queries(size, trial=trial)

        if operation_type in ["all", "indexed"]:
            if bench.needs_starting_data_refresh(size):
                bench.setup_schema(create_indexes=True)
                bench.populate_starting_data(size)
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
    run_sqlite_benchmark(1000, "all")
