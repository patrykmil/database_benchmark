import os
import random
import time
from datetime import date, datetime

import unqlite

from config import DATABASES
from nosql.queries import (
    EXPLAIN_OPERATIONS,
    INDEXED_OPERATIONS,
    JSON_OPERATIONS,
    NONINDEXED_OPERATIONS,
)
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

    def _prepare_doc(self, doc):
        if isinstance(doc, dict):
            return {
                key: self._prepare_doc(value)
                for key, value in doc.items()
                if key != "__id"
            }
        if isinstance(doc, list):
            return [self._prepare_doc(value) for value in doc]
        if isinstance(doc, (datetime, date)):
            return doc.isoformat()
        return doc

    def _store_doc(self, collection_name, doc):
        col = self._get_collection(collection_name)
        if not col.exists():
            col.create()
        return col.store(self._prepare_doc(doc))

    def _update_doc(self, col, record_id, updates):
        current = col.fetch(record_id) or {}
        current_prepared = self._prepare_doc(current)
        merged = dict(current_prepared) if isinstance(current_prepared, dict) else {}
        prepared_updates = self._prepare_doc(updates)
        if isinstance(prepared_updates, dict):
            merged.update(prepared_updates)
        col.update(record_id, merged)

    def bulk_insert(self, collection_name, count, generator_func):
        docs = generator_func(count)
        for doc in docs:
            rid = self._store_doc(collection_name, doc)
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
        if current_size is None:
            print("Current total record count is unknown, refreshing data.")
            return True
        need = abs(current_size - target_size) > (target_size * 0.05)
        if need:
            print(
                f"Current total record count {current_size:_} differs from target {target_size:_} by more than 5%, refreshing data."
            )
        return need

    def _bulk_store(self, collection_name, docs):
        for doc in docs:
            self._store_doc(collection_name, doc)

    def ensure_addresses_volume(self, total_records):
        target_addresses = split_starting_data(total_records)["addresses"]
        addresses = self._get_collection("addresses")
        if not addresses.exists():
            addresses.create()
            current_addresses = 0
        else:
            current_addresses = len(addresses.all())

        if current_addresses >= target_addresses:
            return

        users = self._get_collection("users")
        if not users.exists():
            return

        user_ids = [doc.get("id") for doc in users.all() if doc.get("id") is not None]
        if not user_ids:
            return

        missing = target_addresses - current_addresses
        addresses = [generate_address(random.choice(user_ids)) for _ in range(missing)]
        self._bulk_store("addresses", addresses)

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
                doc = NONINDEXED_OPERATIONS[name]()
                col = self._get_collection("users")
                if not col.exists():
                    col.create()
                start = time.time()
                self._store_doc("users", doc)
                elapsed = (time.time() - start) * 1000
            elif name == "insert_bulk":
                docs = generate_bulk_users(1000)
                col = self._get_collection("users")
                if not col.exists():
                    col.create()
                start = time.time()
                for doc in docs:
                    self._store_doc("users", doc)
                elapsed = (time.time() - start) * 1000
            elif name == "insert_ignore":
                doc = NONINDEXED_OPERATIONS[name]()
                col = self._get_collection("users")
                if not col.exists():
                    col.create()
                start = time.time()
                try:
                    self._store_doc("users", doc)
                except Exception:
                    pass
                elapsed = (time.time() - start) * 1000
            elif name == "insert_upsert":
                data = NONINDEXED_OPERATIONS[name]()
                col = self._get_collection("users")
                if not col.exists():
                    col.create()
                start = time.time()
                self._store_doc(
                    "users",
                    {
                        "name": data["update"]["$set"]["name"],
                        "email": data["filter"]["email"],
                        "created_at": data["update"]["$setOnInsert"]["created_at"],
                        "preferences": data["update"]["$set"]["preferences"],
                    },
                )
                elapsed = (time.time() - start) * 1000
            elif name == "insert_many":
                docs = NONINDEXED_OPERATIONS[name]()
                col = self._get_collection("categories")
                if not col.exists():
                    col.create()
                start = time.time()
                for doc in docs:
                    self._store_doc("categories", doc)
                elapsed = (time.time() - start) * 1000
            elif name == "insert_returning":
                doc = NONINDEXED_OPERATIONS[name]()
                col = self._get_collection("users")
                if not col.exists():
                    col.create()
                start = time.time()
                rid = self._store_doc("users", doc)
                _ = rid
                elapsed = (time.time() - start) * 1000
            elif name == "select_single":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: doc.get("id") == 1))
                elapsed = (time.time() - start) * 1000
            elif name == "select_where":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: "test" in doc.get("email", "")))
                elapsed = (time.time() - start) * 1000
            elif name == "select_aggregate":
                col = self._get_collection("orders")
                start = time.time()
                if col.exists():
                    matching = list(col.filter(lambda doc: doc.get("user_id") == 1))
                    count = len(matching)
                    total = sum(doc.get("total") or 0 for doc in matching)
                    avg = total / count if count > 0 else 0
                else:
                    count, total, avg = 0, 0, 0
                elapsed = (time.time() - start) * 1000
            elif name == "select_pagination":
                col = self._get_collection("users")
                start = time.time()
                all_docs = col.all()
                sorted_docs = sorted(
                    all_docs, key=lambda d: d.get("created_at") or "", reverse=True
                )
                _ = sorted_docs[0:10]
                elapsed = (time.time() - start) * 1000
            elif name == "update_single":
                col = self._get_collection("users")
                start = time.time()
                matching = list(col.filter(lambda doc: doc.get("id") == 1))
                for doc in matching:
                    self._update_doc(col, doc["__id"], {"name": "updated_name"})
                elapsed = (time.time() - start) * 1000
            elif name == "update_many":
                col = self._get_collection("users")
                start = time.time()
                matching = list(
                    col.filter(lambda doc: 1 <= (doc.get("id") or 0) <= 1000)
                )
                for doc in matching:
                    self._update_doc(
                        col, doc["__id"], {"preferences": {"verified": True}}
                    )
                elapsed = (time.time() - start) * 1000
            elif name == "update_in":
                col = self._get_collection("users")
                start = time.time()
                matching = list(col.filter(lambda doc: doc.get("id") in [1, 2, 3]))
                for doc in matching:
                    self._update_doc(col, doc["__id"], {"name": "updated_user"})
                elapsed = (time.time() - start) * 1000
            elif name == "update_case":
                col = self._get_collection("users")
                start = time.time()
                for doc in col.filter(lambda doc: doc.get("id") == 1):
                    self._update_doc(col, doc["__id"], {"name": "user_active"})
                for doc in col.filter(lambda doc: doc.get("id") == 2):
                    self._update_doc(col, doc["__id"], {"name": "user_inactive"})
                elapsed = (time.time() - start) * 1000
            elif name == "update_join":
                col = self._get_collection("orders")
                start = time.time()
                if col.exists():
                    matching = list(col.filter(lambda doc: doc.get("user_id") == 1))
                    for doc in matching:
                        self._update_doc(col, doc["__id"], {"status": "processed"})
                elapsed = (time.time() - start) * 1000
            elif name == "update_upsert":
                col = self._get_collection("products")
                if not col.exists():
                    col.create()
                start = time.time()
                self._store_doc(
                    "products",
                    {
                        "name": "existing_product",
                        "price": 29.99,
                        "category_id": 1,
                        "attributes": {"color": "blue"},
                    },
                )
                elapsed = (time.time() - start) * 1000
            elif name == "delete_single":
                col = self._get_collection("users")
                start = time.time()
                matching = list(col.filter(lambda doc: doc.get("id") == -1))
                for doc in matching:
                    col.delete(doc["__id"])
                elapsed = (time.time() - start) * 1000
            elif name == "delete_many":
                col = self._get_collection("users")
                start = time.time()
                matching = list(
                    col.filter(
                        lambda doc: (
                            doc.get("created_at")
                            and doc.get("created_at") < "2020-01-01"
                        )
                    )
                )
                for doc in matching:
                    col.delete(doc["__id"])
                elapsed = (time.time() - start) * 1000
            elif name == "delete_in":
                col = self._get_collection("users")
                start = time.time()
                matching = list(
                    col.filter(lambda doc: doc.get("id") in [100, 101, 102])
                )
                for doc in matching:
                    col.delete(doc["__id"])
                elapsed = (time.time() - start) * 1000
            elif name == "delete_cascade":
                col = self._get_collection("orders")
                start = time.time()
                if col.exists():
                    matching = list(col.filter(lambda doc: doc.get("user_id") == 1))
                    for doc in matching:
                        col.delete(doc["__id"])
                elapsed = (time.time() - start) * 1000
            elif name == "delete_truncate":
                col = self._get_collection("addresses")
                if col.exists():
                    start = time.time()
                    col.drop()
                    col.create()
                    elapsed = (time.time() - start) * 1000
                else:
                    start = time.time()
                    elapsed = (time.time() - start) * 1000
            else:
                status = "unsupported"

            results[name] = elapsed
            save_result(
                "unqlite", name, size, elapsed, size, trial=trial, status=status
            )

        self.ensure_addresses_volume(size)
        return results

    def run_indexed_queries(self, size, trial=1):
        results = {}
        unsupported_indexed = {
            "index_select_join",
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
                doc = INDEXED_OPERATIONS[name]()
                col = self._get_collection("users")
                if not col.exists():
                    col.create()
                start = time.time()
                self._store_doc("users", doc)
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_bulk":
                docs = generate_bulk_users(1000)
                col = self._get_collection("users")
                if not col.exists():
                    col.create()
                start = time.time()
                for doc in docs:
                    self._store_doc("users", doc)
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_ignore":
                doc = INDEXED_OPERATIONS[name]()
                col = self._get_collection("products")
                if not col.exists():
                    col.create()
                start = time.time()
                try:
                    self._store_doc("products", doc)
                except Exception:
                    pass
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_upsert":
                doc = INDEXED_OPERATIONS[name]()
                col = self._get_collection("products")
                if not col.exists():
                    col.create()
                start = time.time()
                self._store_doc("products", doc)
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_many":
                docs = INDEXED_OPERATIONS[name]()
                col = self._get_collection("products")
                if not col.exists():
                    col.create()
                start = time.time()
                for doc in docs:
                    self._store_doc("products", doc)
                elapsed = (time.time() - start) * 1000
            elif name == "index_insert_returning":
                doc = INDEXED_OPERATIONS[name]()
                col = self._get_collection("products")
                if not col.exists():
                    col.create()
                start = time.time()
                rid = self._store_doc("products", doc)
                _ = rid
                elapsed = (time.time() - start) * 1000
            elif name == "index_select_single":
                col = self._get_collection("users")
                start = time.time()
                list(col.filter(lambda doc: doc.get("email") == "user1000@example.com"))
                elapsed = (time.time() - start) * 1000
            elif name == "index_select_where":
                col = self._get_collection("users")
                start = time.time()
                list(
                    col.filter(
                        lambda doc: (doc.get("created_at") or "") >= "2024-01-01"
                    )
                )
                elapsed = (time.time() - start) * 1000
            elif name == "index_select_aggregate":
                col = self._get_collection("orders")
                start = time.time()
                if col.exists():
                    matching = list(col.filter(lambda doc: doc.get("user_id") == 1))
                    count = len(matching)
                    total = sum(doc.get("total") or 0 for doc in matching)
                    avg = total / count if count > 0 else 0
                else:
                    count, total, avg = 0, 0, 0
                elapsed = (time.time() - start) * 1000
            elif name == "index_select_pagination":
                col = self._get_collection("orders")
                start = time.time()
                if col.exists():
                    all_docs = col.all()
                    sorted_docs = sorted(
                        all_docs,
                        key=lambda d: d.get("created_at") or "",
                        reverse=True,
                    )
                    _ = sorted_docs[0:10]
                elapsed = (time.time() - start) * 1000
            elif name == "index_select_distinct":
                col = self._get_collection("orders")
                start = time.time()
                if col.exists():
                    statuses = set()
                    for doc in col.all():
                        statuses.add(doc.get("status"))
                elapsed = (time.time() - start) * 1000
            elif name == "index_update_single":
                col = self._get_collection("users")
                start = time.time()
                matching = list(
                    col.filter(lambda doc: doc.get("email") == "user1@example.com")
                )
                for doc in matching:
                    self._update_doc(col, doc["__id"], {"name": "updated_email_user"})
                elapsed = (time.time() - start) * 1000
            elif name == "index_update_many":
                col = self._get_collection("products")
                start = time.time()
                if col.exists():
                    matching = list(col.filter(lambda doc: doc.get("category_id") == 1))
                    for doc in matching:
                        new_price = (doc.get("price") or 0) * 1.1
                        self._update_doc(col, doc["__id"], {"price": new_price})
                elapsed = (time.time() - start) * 1000
            elif name == "index_update_in":
                col = self._get_collection("products")
                start = time.time()
                if col.exists():
                    matching = list(
                        col.filter(lambda doc: doc.get("category_id") in [1, 2, 3])
                    )
                    for doc in matching:
                        self._update_doc(col, doc["__id"], {"price": 9.99})
                elapsed = (time.time() - start) * 1000
            elif name == "index_update_case":
                col = self._get_collection("orders")
                start = time.time()
                if col.exists():
                    for doc in col.filter(lambda doc: doc.get("id") == 1):
                        self._update_doc(col, doc["__id"], {"status": "shipped"})
                    for doc in col.filter(lambda doc: doc.get("id") == 2):
                        self._update_doc(col, doc["__id"], {"status": "delivered"})
                elapsed = (time.time() - start) * 1000
            elif name == "index_update_join":
                col = self._get_collection("orders")
                start = time.time()
                if col.exists():
                    matching = list(col.filter(lambda doc: doc.get("user_id") == 1))
                    for doc in matching:
                        self._update_doc(col, doc["__id"], {"status": "processed"})
                elapsed = (time.time() - start) * 1000
            elif name == "index_update_upsert":
                col = self._get_collection("products")
                if not col.exists():
                    col.create()
                start = time.time()
                self._store_doc(
                    "products",
                    {
                        "name": "existing_product",
                        "price": 29.99,
                        "category_id": 1,
                        "attributes": {"color": "blue"},
                    },
                )
                elapsed = (time.time() - start) * 1000
            elif name == "index_delete_single":
                col = self._get_collection("users")
                start = time.time()
                matching = list(
                    col.filter(lambda doc: doc.get("email") == "delete@example.com")
                )
                for doc in matching:
                    col.delete(doc["__id"])
                elapsed = (time.time() - start) * 1000
            elif name == "index_delete_many":
                col = self._get_collection("users")
                start = time.time()
                matching = list(
                    col.filter(
                        lambda doc: (
                            doc.get("created_at")
                            and doc.get("created_at") < "2020-01-01"
                        )
                    )
                )
                for doc in matching:
                    col.delete(doc["__id"])
                elapsed = (time.time() - start) * 1000
            elif name == "index_delete_in":
                col = self._get_collection("products")
                start = time.time()
                if col.exists():
                    matching = list(
                        col.filter(lambda doc: doc.get("category_id") in [100, 101])
                    )
                    for doc in matching:
                        col.delete(doc["__id"])
                elapsed = (time.time() - start) * 1000
            elif name == "index_delete_cascade":
                col = self._get_collection("orders")
                start = time.time()
                if col.exists():
                    matching = list(col.filter(lambda doc: doc.get("user_id") == 1))
                    for doc in matching:
                        col.delete(doc["__id"])
                elapsed = (time.time() - start) * 1000
            elif name == "index_delete_truncate":
                # SQL: DELETE FROM addresses
                col = self._get_collection("addresses")
                if col.exists():
                    start = time.time()
                    col.drop()
                    col.create()
                    elapsed = (time.time() - start) * 1000
                else:
                    start = time.time()
                    elapsed = (time.time() - start) * 1000
            else:
                status = "unsupported"

            results[name] = elapsed
            save_result(
                "unqlite", name, size, elapsed, size, trial=trial, status=status
            )

        self.ensure_addresses_volume(size)
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
