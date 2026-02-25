import json
import os
import time

import unqlite

from config import DATABASES
from nosql.queries import CRUD_OPERATIONS, INDEXED_OPERATIONS, JSON_OPERATIONS
from utils.generator import generate_bulk_users
from utils.results import save_result


class UnqliteBenchmark:
    def __init__(self):
        self.config = DATABASES["unqlite"]
        self.db = None
        self.collection_prefix = "collection_"

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
        col.store(docs)

    def run_crud_queries(self, size):
        results = {}

        user = CRUD_OPERATIONS["insert_single"]()
        col = self._get_collection("users")
        if not col.exists():
            col.create()
        start = time.time()
        col.store(user)
        elapsed = (time.time() - start) * 1000
        results["insert_single"] = elapsed
        save_result("unqlite", "insert_single", size, elapsed, size)

        users = generate_bulk_users(1000)
        start = time.time()
        col.store(users)
        elapsed = (time.time() - start) * 1000
        results["insert_bulk"] = elapsed
        save_result("unqlite", "insert_bulk", size, elapsed, size)

        start = time.time()
        list(col.all())
        elapsed = (time.time() - start) * 1000
        results["select_single"] = elapsed
        save_result("unqlite", "select_single", size, elapsed, size)

        start = time.time()
        list(col.filter(lambda doc: "test" in doc.get("email", "")))
        elapsed = (time.time() - start) * 1000
        results["select_where"] = elapsed
        save_result("unqlite", "select_where", size, elapsed, size)

        results["select_join"] = 0
        save_result("unqlite", "select_join", size, 0, size)

        start = time.time()
        col.update(0, {"$set": {"name": "updated_name"}})
        elapsed = (time.time() - start) * 1000
        results["update_single"] = elapsed
        save_result("unqlite", "update_single", size, elapsed, size)

        start = time.time()
        for doc in col.all():
            col.update(doc["_id"], {"verified": True})
        elapsed = (time.time() - start) * 1000
        results["update_many"] = elapsed
        save_result("unqlite", "update_many", size, elapsed, size)

        start = time.time()
        col.delete(0)
        elapsed = (time.time() - start) * 1000
        results["delete_single"] = elapsed
        save_result("unqlite", "delete_single", size, elapsed, size)

        start = time.time()
        docs_to_delete = list(
            col.filter(lambda doc: doc.get("created_at", "") < "2020-01-01")
        )
        for doc in docs_to_delete:
            col.delete(doc["_id"])
        elapsed = (time.time() - start) * 1000
        results["delete_many"] = elapsed
        save_result("unqlite", "delete_many", size, elapsed, size)

        start = time.time()
        count = len(col.all())
        elapsed = (time.time() - start) * 1000
        results["aggregate_count"] = elapsed
        save_result("unqlite", "aggregate_count", size, elapsed, size)

        results["aggregate_sum"] = 0
        results["aggregate_avg"] = 0
        save_result("unqlite", "aggregate_sum", size, 0, size)
        save_result("unqlite", "aggregate_avg", size, 0, size)

        return results

    def run_indexed_queries(self, size):
        results = {}

        results["select_indexed"] = 0
        save_result("unqlite", "select_indexed", size, 0, size)

        results["select_range"] = 0
        save_result("unqlite", "select_range", size, 0, size)

        results["select_like"] = 0
        save_result("unqlite", "select_like", size, 0, size)

        results["select_order_by"] = 0
        save_result("unqlite", "select_order_by", size, 0, size)

        results["update_indexed"] = 0
        save_result("unqlite", "update_indexed", size, 0, size)

        results["delete_indexed"] = 0
        save_result("unqlite", "delete_indexed", size, 0, size)

        results["select_between"] = 0
        save_result("unqlite", "select_between", size, 0, size)

        results["select_in"] = 0
        save_result("unqlite", "select_in", size, 0, size)

        results["select_exists"] = 0
        save_result("unqlite", "select_exists", size, 0, size)

        results["select_group_by"] = 0
        save_result("unqlite", "select_group_by", size, 0, size)

        results["select_having"] = 0
        save_result("unqlite", "select_having", size, 0, size)

        return results


def run_unqlite_benchmark(size, operation_type="all"):
    bench = UnqliteBenchmark()
    bench.connect()

    try:
        if operation_type in ["all", "crud"]:
            bench.bulk_insert("users", size, generate_bulk_users)
            bench.run_crud_queries(size)

        if operation_type in ["all", "indexed"]:
            bench.run_indexed_queries(size)

    finally:
        bench.close()


if __name__ == "__main__":
    run_unqlite_benchmark(1000, "all")
