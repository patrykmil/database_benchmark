import json
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
from utils.generator import generate_bulk_users
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

    def _users_table_exists(self):
        cur = self.conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )
        return cur.fetchone() is not None

    def get_user_count(self):
        if not self._users_table_exists():
            return None

        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        return cur.fetchone()[0]

    def needs_starting_data_refresh(self, target_size):
        current_size = self.get_user_count()
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

    def run_nonindexed_queries(self, size, trial=1):
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
        return results

    def run_indexed_queries(self, size, trial=1):
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
                bench.bulk_insert_users(size)
            else:
                bench.drop_indexes()
            bench.run_nonindexed_queries(size, trial=trial)

        if operation_type in ["all", "indexed"]:
            if bench.needs_starting_data_refresh(size):
                bench.setup_schema(create_indexes=True)
                bench.setup_reference_data()
                bench.bulk_insert_users(size)
            else:
                bench.ensure_indexes()
                bench.ensure_reference_data()
            bench.run_indexed_queries(size, trial=trial)

        if operation_type in ["explain"]:
            bench.run_explain_queries(trial=trial)

        if operation_type in ["all", "json"]:
            bench.run_json_queries(size, trial=trial)

    finally:
        bench.close()


if __name__ == "__main__":
    run_sqlite_benchmark(1000, "all")
