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
from utils.generator import (
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


class UnqliteBenchmark:
    def __init__(self):
        self.config = DATABASES["unqlite"]
        self.db = None
        self.record_ids = []

    def connect(self):
        self.db = unqlite.UnQLite(self.config["database"])

    def reset_database(self):
        if self.db:
            self.db.close()
            self.db = None
        if os.path.exists(self.config["database"]):
            os.remove(self.config["database"])
        self.connect()
        self.record_ids = []

    def close(self):
        if self.db:
            self.db.close()

    def _get_collection(self, name):
        if self.db is None:
            raise RuntimeError("UnQLite is not connected")
        return self.db.collection(name)

    def bulk_insert(self, collection_name, count, generator_func):
        docs = generator_func(count)
        col = self._get_collection(collection_name)
        if not col.exists():
            col.create()
        for doc in docs:
            rid = col.store(doc)
            self.record_ids.append(rid)

    def get_total_record_count(self):
        collections = [
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
        for name in collections:
            col = self._get_collection(name)
            if not col.exists():
                return None
            total += len(col.all())
        return total

    def needs_starting_data_refresh(self, target_size):
        current_size = self.get_total_record_count()
        need = False
        if current_size is None:
            need = True
        if abs(current_size - target_size) > (target_size * 0.05):
            need = True
        if need:
            print(
                f"Current total record count {current_size:_} differs from target {target_size:_} by more than 5%, refreshing data."
            )
        return need

    def _bulk_store(self, collection_name, docs):
        col = self._get_collection(collection_name)
        if not col.exists():
            col.create()
        for doc in docs:
            col.store(doc)

    def populate_starting_data(self, total_records):
        counts = split_starting_data(total_records)

        users = generate_bulk_users(counts["users"])
        for idx, user in enumerate(users, start=1):
            user["id"] = idx

        categories = generate_bulk_categories(counts["categories"])
        warehouses = generate_bulk_warehouses(counts["warehouses"])

        products = generate_bulk_products(
            counts["products"], list(range(1, counts["categories"] + 1))
        )
        for idx, product in enumerate(products, start=1):
            product["id"] = idx

        orders = generate_bulk_orders(counts["orders"], counts["users"])
        for idx, order in enumerate(orders, start=1):
            order["id"] = idx

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

        self._bulk_store("users", users)
        self._bulk_store("categories", categories)
        self._bulk_store("warehouses", warehouses)
        self._bulk_store("products", products)
        self._bulk_store("orders", orders)
        self._bulk_store("order_items", order_items)
        self._bulk_store("reviews", reviews)
        self._bulk_store("inventory", inventory)
        self._bulk_store("addresses", addresses)
        self._bulk_store("payments", payments)

    def run_nonindexed_queries(self, size, trial=1):
        results = {}
        unsupported_nonindexed = {
            "select_join",
            "select_distinct",
            "update_case",
            "update_join",
            "delete_in",
            "delete_cascade",
            "delete_join",
        }

        for name in NONINDEXED_OPERATIONS.keys():
            elapsed = None
            status = "ok"

            if name in unsupported_nonindexed:
                status = "unsupported"
                results[name] = elapsed
                save_result(
                    "unqlite", name, size, elapsed, size, trial=trial, status=status
                )
                continue

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
            elif name == "delete_truncate":
                col = self._get_collection("addresses")
                if col.exists():
                    start = time.time()
                    col.drop()
                    col.create()
                    elapsed = (time.time() - start) * 1000
                else:
                    status = "unsupported"
            else:
                status = "unsupported"

            results[name] = elapsed
            save_result(
                "unqlite", name, size, elapsed, size, trial=trial, status=status
            )

        return results

    def run_indexed_queries(self, size, trial=1):
        results = {}
        unsupported_indexed = {
            "index_select_join",
            "index_select_aggregate",
            "index_select_pagination",
            "index_select_distinct",
            "index_update_many",
            "index_update_in",
            "index_update_case",
            "index_update_join",
            "index_update_upsert",
            "index_delete_in",
            "index_delete_cascade",
            "index_delete_join",
            "index_delete_truncate",
        }

        for name in INDEXED_OPERATIONS.keys():
            elapsed = None
            status = "ok"

            if name in unsupported_indexed:
                status = "unsupported"
                results[name] = elapsed
                save_result(
                    "unqlite", name, size, elapsed, size, trial=trial, status=status
                )
                continue

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
            elif name == "index_update_single":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: doc.get("email") == "user1@example.com"))
                elapsed = (time.time() - start) * 1000
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
            else:
                status = "unsupported"

            results[name] = elapsed
            save_result(
                "unqlite", name, size, elapsed, size, trial=trial, status=status
            )

        return results

    def run_explain_queries(self, trial=1):
        for name in EXPLAIN_OPERATIONS.keys():
            plan_text = f"explain_{name}"
            save_explain_result(
                "unqlite",
                name,
                plan_text,
                None,
                trial=trial,
                status="unsupported",
            )

        return True

    def run_json_queries(self, size, trial=1):
        results = {}

        for name in JSON_OPERATIONS.keys():
            elapsed = None
            results[name] = elapsed
            save_result(
                "unqlite",
                f"json_{name}",
                size,
                elapsed,
                size,
                trial=trial,
                status="unsupported",
            )

        return results


def run_unqlite_benchmark(size, operation_type="all", trial=1):
    bench = UnqliteBenchmark()
    bench.connect()

    try:
        if operation_type in ["all", "nonindexed"]:
            if bench.needs_starting_data_refresh(size):
                bench.reset_database()
                bench.populate_starting_data(size)
            bench.run_nonindexed_queries(size, trial=trial)

        if operation_type in ["all", "indexed"]:
            if bench.needs_starting_data_refresh(size):
                bench.reset_database()
                bench.populate_starting_data(size)
            bench.run_indexed_queries(size, trial=trial)

        if operation_type in ["explain"]:
            if bench.needs_starting_data_refresh(size):
                bench.reset_database()
                bench.populate_starting_data(size)
            bench.run_explain_queries(trial=trial)

        if operation_type in ["all", "json"]:
            if bench.needs_starting_data_refresh(size):
                bench.reset_database()
                bench.populate_starting_data(size)
            bench.run_json_queries(size, trial=trial)

    finally:
        bench.close()


if __name__ == "__main__":
    run_unqlite_benchmark(1000, "all")
