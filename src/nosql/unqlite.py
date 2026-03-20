import os
import random
import time
from datetime import date, datetime

import unqlite

from src.config.connections import DATABASES
from src.nosql.queries import (
    EXPLAIN_OPERATIONS,
    INDEXED_OPERATIONS,
    JSON_OPERATIONS,
    NONINDEXED_OPERATIONS,
)
from src.utils.benchmark_helpers import needs_starting_data_refresh
from src.utils.generator import (
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
    generate_product,
    generate_review,
    generate_warehouse,
    split_starting_data,
)
from src.utils.results import save_explain_result, save_result


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
        return needs_starting_data_refresh(self, target_size)

    def _bulk_store(self, collection_name, docs):
        for doc in docs:
            self._store_doc(collection_name, doc)

    def _collection_docs(self, collection_name):
        col = self._get_collection(collection_name)
        if not col.exists():
            return []
        return list(col.all())

    def collection_count(self, collection_name):
        return len(self._collection_docs(collection_name))

    def get_max_field_id(self, collection_name, field_name="id"):
        max_id = 0
        for doc in self._collection_docs(collection_name):
            value = doc.get(field_name)
            if isinstance(value, int):
                max_id = max(max_id, value)
        return max_id

    def get_field_ids(self, collection_name, field_name="id"):
        ids = []
        for doc in self._collection_docs(collection_name):
            value = doc.get(field_name)
            if isinstance(value, int):
                ids.append(value)
        return sorted(ids)

    def get_docs_to_trim(self, collection_name, target_count, field_name="id"):
        docs = self._collection_docs(collection_name)
        excess = len(docs) - target_count
        if excess <= 0:
            return []

        return sorted(
            docs,
            key=lambda doc: (
                doc.get(field_name)
                if isinstance(doc.get(field_name), int)
                else doc.get("__id", 0)
            ),
            reverse=True,
        )[:excess]

    def delete_record_ids(self, collection_name, record_ids):
        if not record_ids:
            return
        col = self._get_collection(collection_name)
        for record_id in record_ids:
            col.delete(record_id)

    def delete_by_field(self, collection_name, field_name, values):
        if not values:
            return
        col = self._get_collection(collection_name)
        if not col.exists():
            return
        matching = list(col.filter(lambda doc: doc.get(field_name) in values))
        for doc in matching:
            col.delete(doc["__id"])

    def reconcile_starting_data(self, total_records):
        target_counts = split_starting_data(total_records)

        users_to_delete = self.get_docs_to_trim("users", target_counts["users"])
        categories_to_delete = self.get_docs_to_trim(
            "categories", target_counts["categories"]
        )
        warehouses_to_delete = self.get_docs_to_trim(
            "warehouses", target_counts["warehouses"]
        )
        products_to_delete = self.get_docs_to_trim(
            "products", target_counts["products"]
        )
        orders_to_delete = self.get_docs_to_trim("orders", target_counts["orders"])

        user_ids_to_delete = [
            doc.get("id") for doc in users_to_delete if doc.get("id") is not None
        ]
        category_ids_to_delete = [
            doc.get("id") for doc in categories_to_delete if doc.get("id") is not None
        ]
        warehouse_ids_to_delete = [
            doc.get("id") for doc in warehouses_to_delete if doc.get("id") is not None
        ]
        product_ids_to_delete = [
            doc.get("id") for doc in products_to_delete if doc.get("id") is not None
        ]
        order_ids_to_delete = [
            doc.get("id") for doc in orders_to_delete if doc.get("id") is not None
        ]

        if user_ids_to_delete:
            extra_order_ids = [
                doc.get("id")
                for doc in self._get_collection("orders").filter(
                    lambda doc: doc.get("user_id") in user_ids_to_delete
                )
                if doc.get("id") is not None
            ]
            if extra_order_ids:
                self.delete_by_field("payments", "order_id", extra_order_ids)
                self.delete_by_field("order_items", "order_id", extra_order_ids)
                self.delete_by_field("orders", "id", extra_order_ids)
            self.delete_by_field("reviews", "user_id", user_ids_to_delete)
            self.delete_by_field("addresses", "user_id", user_ids_to_delete)
            self.delete_record_ids(
                "users", [doc["__id"] for doc in users_to_delete if "__id" in doc]
            )

        if category_ids_to_delete:
            extra_product_ids = [
                doc.get("id")
                for doc in self._get_collection("products").filter(
                    lambda doc: doc.get("category_id") in category_ids_to_delete
                )
                if doc.get("id") is not None
            ]
            if extra_product_ids:
                self.delete_by_field("order_items", "product_id", extra_product_ids)
                self.delete_by_field("reviews", "product_id", extra_product_ids)
                self.delete_by_field("inventory", "product_id", extra_product_ids)
                self.delete_by_field("products", "id", extra_product_ids)

            categories_col = self._get_collection("categories")
            for doc in list(
                categories_col.filter(
                    lambda doc: doc.get("parent_id") in category_ids_to_delete
                )
            ):
                updated = dict(doc)
                updated.pop("__id", None)
                updated["parent_id"] = None
                categories_col.update(doc["__id"], self._prepare_doc(updated))

            self.delete_record_ids(
                "categories",
                [doc["__id"] for doc in categories_to_delete if "__id" in doc],
            )

        if warehouse_ids_to_delete:
            self.delete_by_field("inventory", "warehouse_id", warehouse_ids_to_delete)
            self.delete_record_ids(
                "warehouses",
                [doc["__id"] for doc in warehouses_to_delete if "__id" in doc],
            )

        if product_ids_to_delete:
            self.delete_by_field("order_items", "product_id", product_ids_to_delete)
            self.delete_by_field("reviews", "product_id", product_ids_to_delete)
            self.delete_by_field("inventory", "product_id", product_ids_to_delete)
            self.delete_record_ids(
                "products", [doc["__id"] for doc in products_to_delete if "__id" in doc]
            )

        if order_ids_to_delete:
            self.delete_by_field("payments", "order_id", order_ids_to_delete)
            self.delete_by_field("order_items", "order_id", order_ids_to_delete)
            self.delete_record_ids(
                "orders", [doc["__id"] for doc in orders_to_delete if "__id" in doc]
            )

        for collection_name in [
            "order_items",
            "reviews",
            "inventory",
            "addresses",
            "payments",
        ]:
            docs_to_delete = self.get_docs_to_trim(
                collection_name, target_counts[collection_name]
            )
            self.delete_record_ids(
                collection_name,
                [doc["__id"] for doc in docs_to_delete if "__id" in doc],
            )

        users_missing = target_counts["users"] - self.collection_count("users")
        if users_missing > 0:
            start_id = self.get_max_field_id("users") + 1
            users = generate_bulk_users(users_missing)
            for offset, user in enumerate(users):
                user["id"] = start_id + offset
            self._bulk_store("users", users)

        categories_missing = target_counts["categories"] - self.collection_count(
            "categories"
        )
        if categories_missing > 0:
            existing_category_ids = self.get_field_ids("categories")
            start_id = self.get_max_field_id("categories") + 1
            categories = []
            for offset in range(categories_missing):
                category_id = start_id + offset
                parent_id = (
                    random.choice(existing_category_ids)
                    if existing_category_ids and random.random() > 0.7
                    else None
                )
                category = generate_category(parent_id)
                category["id"] = category_id
                categories.append(category)
                existing_category_ids.append(category_id)
            self._bulk_store("categories", categories)

        warehouses_missing = target_counts["warehouses"] - self.collection_count(
            "warehouses"
        )
        if warehouses_missing > 0:
            start_id = self.get_max_field_id("warehouses") + 1
            warehouses = [generate_warehouse() for _ in range(warehouses_missing)]
            for offset, warehouse in enumerate(warehouses):
                warehouse["id"] = start_id + offset
            self._bulk_store("warehouses", warehouses)

        user_ids = self.get_field_ids("users")
        category_ids = self.get_field_ids("categories")
        warehouse_ids = self.get_field_ids("warehouses")

        products_missing = target_counts["products"] - self.collection_count("products")
        if products_missing > 0 and category_ids:
            start_id = self.get_max_field_id("products") + 1
            products = [
                generate_product(random.choice(category_ids))
                for _ in range(products_missing)
            ]
            for offset, product in enumerate(products):
                product["id"] = start_id + offset
            self._bulk_store("products", products)

        product_ids = self.get_field_ids("products")

        orders_missing = target_counts["orders"] - self.collection_count("orders")
        if orders_missing > 0 and user_ids:
            start_id = self.get_max_field_id("orders") + 1
            orders = [
                generate_order(random.choice(user_ids)) for _ in range(orders_missing)
            ]
            for offset, order in enumerate(orders):
                order["id"] = start_id + offset
            self._bulk_store("orders", orders)

        order_ids = self.get_field_ids("orders")

        order_items_missing = target_counts["order_items"] - self.collection_count(
            "order_items"
        )
        if order_items_missing > 0 and order_ids and product_ids:
            order_items = [
                generate_order_item(
                    random.choice(order_ids), random.choice(product_ids)
                )
                for _ in range(order_items_missing)
            ]
            self._bulk_store("order_items", order_items)

        reviews_missing = target_counts["reviews"] - self.collection_count("reviews")
        if reviews_missing > 0 and user_ids and product_ids:
            reviews = [
                generate_review(random.choice(user_ids), random.choice(product_ids))
                for _ in range(reviews_missing)
            ]
            self._bulk_store("reviews", reviews)

        inventory_missing = target_counts["inventory"] - self.collection_count(
            "inventory"
        )
        if inventory_missing > 0 and product_ids and warehouse_ids:
            inventory = [
                generate_inventory(
                    random.choice(product_ids), random.choice(warehouse_ids)
                )
                for _ in range(inventory_missing)
            ]
            self._bulk_store("inventory", inventory)

        addresses_missing = target_counts["addresses"] - self.collection_count(
            "addresses"
        )
        if addresses_missing > 0 and user_ids:
            addresses = [
                generate_address(random.choice(user_ids))
                for _ in range(addresses_missing)
            ]
            self._bulk_store("addresses", addresses)

        payments_missing = target_counts["payments"] - self.collection_count("payments")
        if payments_missing > 0 and order_ids:
            payments = [
                generate_payment(random.choice(order_ids))
                for _ in range(payments_missing)
            ]
            self._bulk_store("payments", payments)

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
        for idx, warehouse in enumerate(warehouses, start=1):
            warehouse["id"] = idx

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
            if bench.get_total_record_count() is None:
                bench.reset_database()
                bench.populate_starting_data(size)
            else:
                needs_refresh, use_populate = bench.needs_starting_data_refresh(size)
                if needs_refresh:
                    if use_populate:
                        bench.reset_database()
                        bench.populate_starting_data(size)
                    else:
                        bench.reconcile_starting_data(size)
            bench.run_nonindexed_queries(size, trial=trial)

        if operation_type in ["all", "indexed"]:
            if bench.get_total_record_count() is None:
                bench.reset_database()
                bench.populate_starting_data(size)
            else:
                needs_refresh, use_populate = bench.needs_starting_data_refresh(size)
                if needs_refresh:
                    if use_populate:
                        bench.reset_database()
                        bench.populate_starting_data(size)
                    else:
                        bench.reconcile_starting_data(size)
            bench.run_indexed_queries(size, trial=trial)

        if operation_type in ["explain"]:
            if bench.get_total_record_count() is None:
                bench.reset_database()
                bench.populate_starting_data(size)
            else:
                needs_refresh, use_populate = bench.needs_starting_data_refresh(size)
                if needs_refresh:
                    if use_populate:
                        bench.reset_database()
                        bench.populate_starting_data(size)
                    else:
                        bench.reconcile_starting_data(size)
            bench.run_explain_queries(trial=trial)

        if operation_type in ["all", "json"]:
            if bench.get_total_record_count() is None:
                bench.reset_database()
                bench.populate_starting_data(size)
            else:
                needs_refresh, use_populate = bench.needs_starting_data_refresh(size)
                if needs_refresh:
                    if use_populate:
                        bench.reset_database()
                        bench.populate_starting_data(size)
                    else:
                        bench.reconcile_starting_data(size)
            bench.run_json_queries(size, trial=trial)

    finally:
        bench.close()


if __name__ == "__main__":
    run_unqlite_benchmark(1000, "all")
