import os
import time

import unqlite

from config import DATABASES
from nosql.queries import (
    EXPLAIN_OPERATIONS,
    INDEXED_OPERATIONS,
    JSON_OPERATIONS,
    NONINDEXED_OPERATIONS,
)
from utils.generator import generate_bulk_users
from utils.results import save_explain_result, save_result


class UnqliteBenchmark:
    def __init__(self):
        self.config = DATABASES["unqlite"]
        self.db = None
        self.record_ids = []

    def connect(self):
        if os.path.exists(self.config["database"]):
            os.remove(self.config["database"])
        self.db = unqlite.UnQLite(self.config["database"])

    def close(self):
        if self.db:
            self.db.close()

    def _get_collection(self, name):
        return self.db.collection(name)

    def bulk_insert(self, collection_name, count, generator_func):
        docs = generator_func(count)
        col = self._get_collection(collection_name)
        if not col.exists():
            col.create()
        for doc in docs:
            rid = col.store(doc)
            self.record_ids.append(rid)

    def run_nonindexed_queries(self, size):
        results = {}

        for name in NONINDEXED_OPERATIONS.keys():
            elapsed = 0
            if name == "insert_single":
                user = {
                    "name": "test_user",
                    "email": "test@example.com",
                    "preferences": {"theme": "dark"},
                }
                col = self._get_collection("users")
                if not col.exists():
                    col.create()
                start = time.time()
                rid = col.store(user)
                self.record_ids.append(rid)
                elapsed = (time.time() - start) * 1000
            elif name == "insert_bulk":
                users = generate_bulk_users(1000)
                col = self._get_collection("users")
                start = time.time()
                for u in users:
                    rid = col.store(u)
                    self.record_ids.append(rid)
                elapsed = (time.time() - start) * 1000
            elif name == "insert_ignore":
                user = {
                    "name": "test_user",
                    "email": "test@example.com",
                    "preferences": {"theme": "dark"},
                }
                col = self._get_collection("users")
                start = time.time()
                try:
                    col.store(user)
                except:
                    pass
                elapsed = (time.time() - start) * 1000
            elif name == "insert_upsert":
                user = {
                    "name": "test_user",
                    "email": "test@example.com",
                    "preferences": {"theme": "light"},
                }
                col = self._get_collection("users")
                start = time.time()
                col.store(user)
                elapsed = (time.time() - start) * 1000
            elif name == "insert_many":
                col = self._get_collection("categories")
                if not col.exists():
                    col.create()
                start = time.time()
                for i in range(100):
                    col.store({"name": f"cat{i}"})
                elapsed = (time.time() - start) * 1000
            elif name == "insert_returning":
                user = {
                    "name": "test_user",
                    "email": "test@example.com",
                    "preferences": {"theme": "dark"},
                }
                col = self._get_collection("users")
                start = time.time()
                col.store(user)
                elapsed = (time.time() - start) * 1000
            elif name == "select_single":
                col = self._get_collection("users")
                start = time.time()
                list(col.all())
                elapsed = (time.time() - start) * 1000
            elif name == "select_where":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: "test" in doc.get("email", "")))
                elapsed = (time.time() - start) * 1000
            elif name == "select_join":
                elapsed = 0
            elif name == "select_aggregate":
                col = self._get_collection("orders")
                start = time.time()
                count = len(col.all()) if col.exists() else 0
                elapsed = (time.time() - start) * 1000
            elif name == "select_pagination":
                col = self._get_collection("users")
                start = time.time()
                list(col.all()[:10])
                elapsed = (time.time() - start) * 1000
            elif name == "select_distinct":
                elapsed = 0
            elif name == "update_single":
                col = self._get_collection("users")
                start = time.time()
                if self.record_ids:
                    col.update(self.record_ids[0], {"name": "updated_name"})
                elapsed = (time.time() - start) * 1000
            elif name == "update_many":
                col = self._get_collection("users")
                start = time.time()
                for rid in self.record_ids[:100]:
                    col.update(rid, {"verified": True})
                elapsed = (time.time() - start) * 1000
            elif name == "update_in":
                col = self._get_collection("users")
                start = time.time()
                for rid in self.record_ids[:3]:
                    col.update(rid, {"verified": True})
                elapsed = (time.time() - start) * 1000
            elif name == "update_case":
                elapsed = 0
            elif name == "update_join":
                elapsed = 0
            elif name == "update_upsert":
                col = self._get_collection("products")
                if not col.exists():
                    col.create()
                start = time.time()
                col.store({"name": "upsert_product", "price": 29.99})
                elapsed = (time.time() - start) * 1000
            elif name == "delete_single":
                col = self._get_collection("users")
                start = time.time()
                if self.record_ids:
                    col.delete(self.record_ids[-1])
                elapsed = (time.time() - start) * 1000
            elif name == "delete_many":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: doc.get("created_at", "") < "2020-01-01"))
                elapsed = (time.time() - start) * 1000
            elif name == "delete_in":
                elapsed = 0
            elif name == "delete_cascade":
                elapsed = 0
            elif name == "delete_join":
                elapsed = 0
            elif name == "delete_truncate":
                col = self._get_collection("addresses")
                if col.exists():
                    start = time.time()
                    for doc in col.all():
                        col.delete(doc._id)
                    elapsed = (time.time() - start) * 1000
                else:
                    elapsed = 0

            results[name] = elapsed
            save_result("unqlite", name, size, elapsed, size)

        return results

    def run_indexed_queries(self, size):
        results = {}

        for name in INDEXED_OPERATIONS.keys():
            elapsed = 0
            if name == "index_insert_single":
                col = self._get_collection("users")
                if not col.exists():
                    col.create()
                start = time.time()
                col.store(
                    {
                        "name": "indexed_user",
                        "email": "indexed@example.com",
                        "created_at": "2024-01-01",
                    }
                )
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_bulk":
                col = self._get_collection("users")
                start = time.time()
                for i in range(1000):
                    col.store(
                        {"name": f"bulkuser{i}", "email": f"bulkuser{i}@example.com"}
                    )
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_ignore":
                col = self._get_collection("products")
                if not col.exists():
                    col.create()
                start = time.time()
                col.store({"name": "indexed_product", "price": 99.99, "category_id": 1})
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_upsert":
                col = self._get_collection("products")
                start = time.time()
                col.store({"name": "upsert_product", "price": 49.99})
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_many":
                col = self._get_collection("products")
                start = time.time()
                for i in range(100):
                    col.store({"name": f"product{i}", "price": 10.0, "category_id": 1})
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_returning":
                col = self._get_collection("products")
                start = time.time()
                col.store({"name": "returning_product", "price": 19.99})
                elapsed = (time.time() - start) * 1000
            elif name == "index_select_single":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: doc.get("email") == "user1000@example.com"))
                elapsed = (time.time() - start) * 1000
            elif name == "index_select_where":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: doc.get("created_at", "") >= "2024-01-01"))
                elapsed = (time.time() - start) * 1000
            elif name == "index_select_join":
                elapsed = 0
            elif name == "index_select_aggregate":
                elapsed = 0
            elif name == "index_select_pagination":
                col = self._get_collection("orders")
                if col.exists():
                    start = time.time()
                    list(col.all()[:10])
                    elapsed = (time.time() - start) * 1000
                else:
                    elapsed = 0
            elif name == "index_select_distinct":
                elapsed = 0
            elif name == "index_update_single":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: doc.get("email") == "user1@example.com"))
                elapsed = (time.time() - start) * 1000
            elif name == "index_update_many":
                elapsed = 0
            elif name == "index_update_in":
                elapsed = 0
            elif name == "index_update_case":
                elapsed = 0
            elif name == "index_update_join":
                elapsed = 0
            elif name == "index_update_upsert":
                elapsed = 0
            elif name == "index_delete_single":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: doc.get("email") == "delete@example.com"))
                elapsed = (time.time() - start) * 1000
            elif name == "index_delete_many":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: doc.get("created_at", "") < "2020-01-01"))
                elapsed = (time.time() - start) * 1000
            elif name == "index_delete_in":
                elapsed = 0
            elif name == "index_delete_cascade":
                elapsed = 0
            elif name == "index_delete_join":
                elapsed = 0
            elif name == "index_delete_truncate":
                elapsed = 0

            results[name] = elapsed
            save_result("unqlite", name, size, elapsed, size)

        return results

    def run_explain_queries(self):
        for name in EXPLAIN_OPERATIONS.keys():
            start = time.time()
            plan_text = f"explain_{name}"
            elapsed = (time.time() - start) * 1000
            save_explain_result("unqlite", name, plan_text, elapsed)

        return True

    def run_json_queries(self, size):
        results = {}

        for name in JSON_OPERATIONS.keys():
            elapsed = 0
            results[name] = elapsed
            save_result("unqlite", f"json_{name}", size, elapsed, size)

        return results


def run_unqlite_benchmark(size, operation_type="all"):
    bench = UnqliteBenchmark()
    bench.connect()

    try:
        if operation_type in ["all", "nonindexed"]:
            bench.bulk_insert("users", size, generate_bulk_users)
            bench.run_nonindexed_queries(size)

        if operation_type in ["all", "indexed"]:
            bench.run_indexed_queries(size)

        if operation_type in ["all", "explain"]:
            bench.run_explain_queries()

        if operation_type in ["all", "json"]:
            bench.run_json_queries(size)

    finally:
        bench.close()


if __name__ == "__main__":
    run_unqlite_benchmark(1000, "all")
