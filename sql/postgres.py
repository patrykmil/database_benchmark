import time

import psycopg2
from psycopg2.extras import execute_values

from config import DATABASES
from sql.queries import CRUD_QUERIES, EXPLAIN_QUERIES, INDEXED_QUERIES, JSON_QUERIES
from sql.schema import INDEXES, SCHEMA
from utils.generator import (
    generate_bulk_categories,
    generate_bulk_products,
    generate_bulk_users,
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

    def setup_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(SCHEMA)
            cur.execute(INDEXES)

    def bulk_insert_users(self, count):
        users = generate_bulk_users(count)
        data = [
            (u["name"], u["email"], u["created_at"], str(u["preferences"]))
            for u in users
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO users (name, email, created_at, preferences) VALUES %s",
                data,
                template="(%s, %s, %s, %s)",
            )

    def run_crud_queries(self, size):
        results = {}
        for name, q in CRUD_QUERIES.items():
            params = q["params"]()
            if name == "insert_bulk":
                users = generate_bulk_users(1000)
                data = [
                    (u["name"], u["email"], u["created_at"], str(u["preferences"]))
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
            else:
                start = time.time()
                with self.conn.cursor() as cur:
                    cur.execute(q["query"], params)
                    if name.startswith("select"):
                        cur.fetchall()
                elapsed = (time.time() - start) * 1000

            results[name] = elapsed
            save_result("postgres", name, size, elapsed, size)
        return results

    def run_indexed_queries(self, size):
        results = {}
        for name, q in INDEXED_QUERIES.items():
            params = q["params"]()
            start = time.time()
            with self.conn.cursor() as cur:
                cur.execute(q["query"], params)
                if name.startswith("select"):
                    cur.fetchall()
            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("postgres", name, size, elapsed, size)
        return results

    def run_explain_queries(self):
        for name, q in EXPLAIN_QUERIES.items():
            params = q["params"]()
            start = time.time()
            with self.conn.cursor() as cur:
                cur.execute(q["query"], params)
                plan = cur.fetchall()
            elapsed = (time.time() - start) * 1000
            plan_text = "\n".join([str(row) for row in plan])
            save_explain_result("postgres", name, plan_text, elapsed)
        return True

    def run_json_queries(self, size):
        results = {}
        for name, q in JSON_QUERIES.items():
            params = q["params"]()
            start = time.time()
            with self.conn.cursor() as cur:
                cur.execute(q["query"], params)
                cur.fetchall()
            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("postgres", f"json_{name}", size, elapsed, size)
        return results


def run_postgres_benchmark(size, operation_type="all"):
    bench = PostgresBenchmark()
    bench.connect()

    try:
        bench.setup_schema()

        if operation_type in ["all", "crud"]:
            bench.bulk_insert_users(size)
            bench.run_crud_queries(size)

        if operation_type in ["all", "indexed"]:
            bench.run_indexed_queries(size)

        if operation_type == "explain":
            bench.run_explain_queries()

        if operation_type == "json":
            bench.run_json_queries(size)

    finally:
        bench.close()


if __name__ == "__main__":
    run_postgres_benchmark(1000, "all")
