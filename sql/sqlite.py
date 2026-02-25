import json
import sqlite3
import time

from config import DATABASES
from sql.queries import CRUD_QUERIES, EXPLAIN_QUERIES, INDEXED_QUERIES, JSON_QUERIES
from sql.schema import SQLITE_INDEXES, SQLITE_SCHEMA
from utils.generator import generate_bulk_users
from utils.results import save_explain_result, save_result


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

    def setup_schema(self):
        cur = self.conn.cursor()
        for stmt in SQLITE_SCHEMA.split(";"):
            if stmt.strip():
                cur.execute(stmt)
        for stmt in SQLITE_INDEXES.split(";"):
            if stmt.strip():
                cur.execute(stmt)
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

    def run_crud_queries(self, size):
        results = {}
        for name, q in CRUD_QUERIES.items():
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
            else:
                start = time.time()
                cur = self.conn.cursor()
                cur.execute(q["query"], params)
                if name.startswith("select"):
                    cur.fetchall()
                elapsed = (time.time() - start) * 1000

            results[name] = elapsed
            save_result("sqlite", name, size, elapsed, size)
        return results

    def run_indexed_queries(self, size):
        results = {}
        for name, q in INDEXED_QUERIES.items():
            params = q["params"]()
            start = time.time()
            cur = self.conn.cursor()
            cur.execute(q["query"], params)
            if name.startswith("select"):
                cur.fetchall()
            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("sqlite", name, size, elapsed, size)
        return results

    def run_explain_queries(self):
        for name, q in EXPLAIN_QUERIES.items():
            params = q["params"]()
            start = time.time()
            cur = self.conn.cursor()
            cur.execute(
                q["query"].replace("EXPLAIN ANALYZE", "EXPLAIN QUERY PLAN"), params
            )
            plan = cur.fetchall()
            elapsed = (time.time() - start) * 1000
            plan_text = "\n".join([str(row) for row in plan])
            save_explain_result("sqlite", name, plan_text, elapsed)
        return True

    def run_json_queries(self, size):
        results = {}
        for name, q in JSON_QUERIES.items():
            params = q["params"]()
            query = q["query"].replace("->>", "json_extract").replace("@>", "json_each")
            start = time.time()
            cur = self.conn.cursor()
            try:
                cur.execute(query, params)
                cur.fetchall()
            except:
                pass
            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("sqlite", f"json_{name}", size, elapsed, size)
        return results


def run_sqlite_benchmark(size, operation_type="all"):
    bench = SQLiteBenchmark()
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
    run_sqlite_benchmark(1000, "all")
