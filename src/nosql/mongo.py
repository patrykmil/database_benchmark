import random
import time

from pymongo import MongoClient

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


class MongoBenchmark:
    def __init__(self):
        self.config = DATABASES["mongo"]
        self.client = None
        self.db = None

    def connect(self):
        self.client = MongoClient(self.config["host"], self.config["port"])
        self.db = self.client[self.config["database"]]

    def close(self):
        if self.client:
            self.client.close()

    def setup_collections(self, create_indexes=True):
        self.db.users.drop()
        self.db.products.drop()
        self.db.orders.drop()
        self.db.categories.drop()
        self.db.order_items.drop()
        self.db.reviews.drop()
        self.db.warehouses.drop()
        self.db.inventory.drop()
        self.db.addresses.drop()
        self.db.payments.drop()

        if create_indexes:
            self.db.users.create_index("email")
            self.db.users.create_index("created_at")
            self.db.products.create_index("category_id")
            self.db.products.create_index("price")
            self.db.orders.create_index("user_id")
            self.db.orders.create_index("status")
            self.db.orders.create_index("created_at")

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
        existing = set(self.db.list_collection_names())
        if not all(name in existing for name in collections):
            return None

        total = 0
        for collection in collections:
            total += getattr(self.db, collection).count_documents({})
        return total

    def needs_starting_data_refresh(self, target_size):
        return needs_starting_data_refresh(self, target_size)

    def collection_count(self, collection_name):
        return getattr(self.db, collection_name).count_documents({})

    def get_max_numeric_id(self, collection_name):
        max_id = 0
        for doc in getattr(self.db, collection_name).find({}, {"_id": 1}):
            if isinstance(doc.get("_id"), int):
                max_id = max(max_id, doc["_id"])
        return max_id

    def get_numeric_ids(self, collection_name, field_name="_id"):
        values = []
        for doc in getattr(self.db, collection_name).find({}, {field_name: 1}):
            value = doc.get(field_name)
            if isinstance(value, int):
                values.append(value)
        return sorted(values)

    def get_ids_to_trim(self, collection_name, target_count):
        current_count = self.collection_count(collection_name)
        excess = current_count - target_count
        if excess <= 0:
            return []
        docs = (
            getattr(self.db, collection_name)
            .find({}, {"_id": 1})
            .sort("_id", -1)
            .limit(excess)
        )
        return [doc["_id"] for doc in docs]

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
            user_order_ids = [
                doc["_id"]
                for doc in self.db.orders.find(
                    {"user_id": {"$in": users_to_delete}}, {"_id": 1}
                )
            ]
            if user_order_ids:
                self.db.payments.delete_many({"order_id": {"$in": user_order_ids}})
                self.db.order_items.delete_many({"order_id": {"$in": user_order_ids}})
                self.db.orders.delete_many({"_id": {"$in": user_order_ids}})
            self.db.reviews.delete_many({"user_id": {"$in": users_to_delete}})
            self.db.addresses.delete_many({"user_id": {"$in": users_to_delete}})
            self.db.users.delete_many({"_id": {"$in": users_to_delete}})

        if categories_to_delete:
            category_product_ids = [
                doc["_id"]
                for doc in self.db.products.find(
                    {"category_id": {"$in": categories_to_delete}},
                    {"_id": 1},
                )
            ]
            if category_product_ids:
                self.db.order_items.delete_many(
                    {"product_id": {"$in": category_product_ids}}
                )
                self.db.reviews.delete_many(
                    {"product_id": {"$in": category_product_ids}}
                )
                self.db.inventory.delete_many(
                    {"product_id": {"$in": category_product_ids}}
                )
                self.db.products.delete_many({"_id": {"$in": category_product_ids}})
            self.db.categories.update_many(
                {"parent_id": {"$in": categories_to_delete}},
                {"$set": {"parent_id": None}},
            )
            self.db.categories.delete_many({"_id": {"$in": categories_to_delete}})

        if warehouses_to_delete:
            self.db.inventory.delete_many(
                {"warehouse_id": {"$in": warehouses_to_delete}}
            )
            self.db.warehouses.delete_many({"_id": {"$in": warehouses_to_delete}})

        if products_to_delete:
            self.db.order_items.delete_many({"product_id": {"$in": products_to_delete}})
            self.db.reviews.delete_many({"product_id": {"$in": products_to_delete}})
            self.db.inventory.delete_many({"product_id": {"$in": products_to_delete}})
            self.db.products.delete_many({"_id": {"$in": products_to_delete}})

        if orders_to_delete:
            self.db.payments.delete_many({"order_id": {"$in": orders_to_delete}})
            self.db.order_items.delete_many({"order_id": {"$in": orders_to_delete}})
            self.db.orders.delete_many({"_id": {"$in": orders_to_delete}})

        for collection_name in [
            "order_items",
            "reviews",
            "inventory",
            "addresses",
            "payments",
        ]:
            ids = self.get_ids_to_trim(collection_name, target_counts[collection_name])
            if ids:
                getattr(self.db, collection_name).delete_many({"_id": {"$in": ids}})

        users_missing = target_counts["users"] - self.collection_count("users")
        if users_missing > 0:
            start_id = self.get_max_numeric_id("users") + 1
            users = generate_bulk_users(users_missing)
            for idx, user in enumerate(users, start=start_id):
                user["_id"] = idx
            self.db.users.insert_many(users)

        categories_missing = target_counts["categories"] - self.collection_count(
            "categories"
        )
        if categories_missing > 0:
            existing_category_ids = self.get_numeric_ids("categories")
            start_id = self.get_max_numeric_id("categories") + 1
            categories = []
            for idx in range(categories_missing):
                category_id = start_id + idx
                parent_id = (
                    random.choice(existing_category_ids)
                    if existing_category_ids and random.random() > 0.7
                    else None
                )
                category = generate_category(parent_id)
                category["_id"] = category_id
                categories.append(category)
                existing_category_ids.append(category_id)
            self.db.categories.insert_many(categories)

        warehouses_missing = target_counts["warehouses"] - self.collection_count(
            "warehouses"
        )
        if warehouses_missing > 0:
            start_id = self.get_max_numeric_id("warehouses") + 1
            warehouses = [generate_warehouse() for _ in range(warehouses_missing)]
            for idx, warehouse in enumerate(warehouses, start=start_id):
                warehouse["_id"] = idx
            self.db.warehouses.insert_many(warehouses)

        user_ids = self.get_numeric_ids("users")
        category_ids = self.get_numeric_ids("categories")
        warehouse_ids = self.get_numeric_ids("warehouses")

        products_missing = target_counts["products"] - self.collection_count("products")
        if products_missing > 0 and category_ids:
            start_id = self.get_max_numeric_id("products") + 1
            products = [
                generate_product(random.choice(category_ids))
                for _ in range(products_missing)
            ]
            for idx, product in enumerate(products, start=start_id):
                product["_id"] = idx
            self.db.products.insert_many(products)

        product_ids = self.get_numeric_ids("products")

        orders_missing = target_counts["orders"] - self.collection_count("orders")
        if orders_missing > 0 and user_ids:
            start_id = self.get_max_numeric_id("orders") + 1
            orders = [
                generate_order(random.choice(user_ids)) for _ in range(orders_missing)
            ]
            for idx, order in enumerate(orders, start=start_id):
                order["_id"] = idx
            self.db.orders.insert_many(orders)

        order_ids = self.get_numeric_ids("orders")

        order_items_missing = target_counts["order_items"] - self.collection_count(
            "order_items"
        )
        if order_items_missing > 0 and order_ids and product_ids:
            start_id = self.get_max_numeric_id("order_items") + 1
            order_items = [
                generate_order_item(
                    random.choice(order_ids), random.choice(product_ids)
                )
                for _ in range(order_items_missing)
            ]
            for idx, item in enumerate(order_items, start=start_id):
                item["_id"] = idx
            self.db.order_items.insert_many(order_items)

        reviews_missing = target_counts["reviews"] - self.collection_count("reviews")
        if reviews_missing > 0 and user_ids and product_ids:
            start_id = self.get_max_numeric_id("reviews") + 1
            reviews = [
                generate_review(random.choice(user_ids), random.choice(product_ids))
                for _ in range(reviews_missing)
            ]
            for idx, review in enumerate(reviews, start=start_id):
                review["_id"] = idx
            self.db.reviews.insert_many(reviews)

        inventory_missing = target_counts["inventory"] - self.collection_count(
            "inventory"
        )
        if inventory_missing > 0 and product_ids and warehouse_ids:
            start_id = self.get_max_numeric_id("inventory") + 1
            inventory = [
                generate_inventory(
                    random.choice(product_ids), random.choice(warehouse_ids)
                )
                for _ in range(inventory_missing)
            ]
            for idx, record in enumerate(inventory, start=start_id):
                record["_id"] = idx
            self.db.inventory.insert_many(inventory)

        addresses_missing = target_counts["addresses"] - self.collection_count(
            "addresses"
        )
        if addresses_missing > 0 and user_ids:
            start_id = self.get_max_numeric_id("addresses") + 1
            addresses = [
                generate_address(random.choice(user_ids))
                for _ in range(addresses_missing)
            ]
            for idx, address in enumerate(addresses, start=start_id):
                address["_id"] = idx
            self.db.addresses.insert_many(addresses)

        payments_missing = target_counts["payments"] - self.collection_count("payments")
        if payments_missing > 0 and order_ids:
            start_id = self.get_max_numeric_id("payments") + 1
            payments = [
                generate_payment(random.choice(order_ids))
                for _ in range(payments_missing)
            ]
            for idx, payment in enumerate(payments, start=start_id):
                payment["_id"] = idx
            self.db.payments.insert_many(payments)

    def populate_starting_data(self, total_records):
        counts = split_starting_data(total_records)

        users = generate_bulk_users(counts["users"])
        for idx, user in enumerate(users, start=1):
            user["_id"] = idx

        categories = generate_bulk_categories(counts["categories"])
        for idx, category in enumerate(categories, start=1):
            category["_id"] = idx

        warehouses = generate_bulk_warehouses(counts["warehouses"])
        for idx, warehouse in enumerate(warehouses, start=1):
            warehouse["_id"] = idx

        products = generate_bulk_products(
            counts["products"], list(range(1, counts["categories"] + 1))
        )
        for idx, product in enumerate(products, start=1):
            product["_id"] = idx

        orders = generate_bulk_orders(counts["orders"], counts["users"])
        for idx, order in enumerate(orders, start=1):
            order["_id"] = idx

        order_items = generate_bulk_order_items(
            counts["order_items"], counts["orders"], counts["products"]
        )
        for idx, item in enumerate(order_items, start=1):
            item["_id"] = idx

        reviews = generate_bulk_reviews(
            counts["reviews"], counts["users"], counts["products"]
        )
        for idx, review in enumerate(reviews, start=1):
            review["_id"] = idx

        inventory = generate_bulk_inventory(
            counts["inventory"], counts["products"], counts["warehouses"]
        )
        for idx, record in enumerate(inventory, start=1):
            record["_id"] = idx

        addresses = generate_bulk_addresses(counts["addresses"], counts["users"])
        for idx, address in enumerate(addresses, start=1):
            address["_id"] = idx

        payments = generate_bulk_payments(counts["payments"], counts["orders"])
        for idx, payment in enumerate(payments, start=1):
            payment["_id"] = idx

        self.db.users.insert_many(users)
        self.db.categories.insert_many(categories)
        self.db.warehouses.insert_many(warehouses)
        self.db.products.insert_many(products)
        self.db.orders.insert_many(orders)
        self.db.order_items.insert_many(order_items)
        self.db.reviews.insert_many(reviews)
        self.db.inventory.insert_many(inventory)
        self.db.addresses.insert_many(addresses)
        self.db.payments.insert_many(payments)

    def ensure_indexes(self):
        self.db.users.create_index("email")
        self.db.users.create_index("created_at")
        self.db.products.create_index("category_id")
        self.db.products.create_index("price")
        self.db.orders.create_index("user_id")
        self.db.orders.create_index("status")
        self.db.orders.create_index("created_at")

    def drop_indexes(self):
        for collection_name in ["users", "products", "orders"]:
            if collection_name in self.db.list_collection_names():
                getattr(self.db, collection_name).drop_indexes()

    def bulk_insert(self, collection, count, generator_func):
        docs = generator_func(count)
        getattr(self.db, collection).insert_many(docs)

    def run_nonindexed_queries(self, size, trial=1):
        results = {}

        for name, query_func in NONINDEXED_OPERATIONS.items():
            start = time.time()
            status = "ok"
            elapsed = None

            if name == "insert_single":
                doc = query_func()
                self.db.users.insert_one(doc)
            elif name == "insert_bulk":
                docs = generate_bulk_users(1000)
                self.db.users.insert_many(docs)
            elif name == "insert_ignore":
                doc = query_func()
                try:
                    self.db.users.insert_one(doc)
                except:
                    pass
            elif name == "insert_upsert":
                data = query_func()
                self.db.users.update_one(data["filter"], data["update"], upsert=True)
            elif name == "insert_many":
                docs = query_func()
                self.db.categories.insert_many(docs)
            elif name == "insert_returning":
                doc = query_func()
                result = self.db.users.insert_one(doc)
                _ = result.inserted_id
            elif name == "select_single":
                doc_filter = query_func()
                list(self.db.users.find(doc_filter))
            elif name == "select_where":
                doc_filter = query_func()
                list(self.db.users.find(doc_filter))
            elif name == "select_join":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "select_aggregate":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "select_pagination":
                pipeline = query_func()
                list(self.db.users.aggregate(pipeline))
            elif name == "select_distinct":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "update_single":
                data = query_func()
                self.db.users.update_one(data["filter"], data["update"])
            elif name == "update_many":
                data = query_func()
                self.db.users.update_many(data["filter"], data["update"])
            elif name == "update_in":
                data = query_func()
                self.db.users.update_many(data["filter"], data["update"])
            elif name == "update_case":
                data = query_func()
                for op in data["operations"]:
                    self.db.users.update_one(op["filter"], op["update"])
            elif name == "update_join":
                data = query_func()
                self.db.orders.update_many(data["filter"], data["update"])
            elif name == "update_upsert":
                data = query_func()
                self.db.products.update_one(data["filter"], data["update"], upsert=True)
            elif name == "delete_single":
                doc_filter = query_func()
                self.db.users.delete_one(doc_filter)
            elif name == "delete_many":
                doc_filter = query_func()
                self.db.users.delete_many(doc_filter)
            elif name == "delete_in":
                doc_filter = query_func()
                self.db.users.delete_many(doc_filter)
            elif name == "delete_cascade":
                doc_filter = query_func()
                self.db.orders.delete_many(doc_filter)
            elif name == "delete_join":
                pipeline = query_func()
                matching = list(self.db.orders.aggregate(pipeline))
                if matching:
                    ids = [doc["_id"] for doc in matching]
                    self.db.orders.delete_many({"_id": {"$in": ids}})
            elif name == "delete_truncate":
                self.db.addresses.delete_many({})

            if status == "ok":
                elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("mongo", name, size, elapsed, size, trial=trial, status=status)

        return results

    def run_indexed_queries(self, size, trial=1):
        results = {}

        for name, query_func in INDEXED_OPERATIONS.items():
            start = time.time()
            status = "ok"
            elapsed = None

            if name == "index_insert_single":
                doc = query_func()
                self.db.users.insert_one(doc)
            elif name == "index_insert_bulk":
                docs = generate_bulk_users(1000)
                self.db.users.insert_many(docs)
            elif name == "index_insert_ignore":
                doc = query_func()
                try:
                    self.db.products.insert_one(doc)
                except:
                    pass
            elif name == "index_insert_upsert":
                doc = query_func()
                self.db.products.update_one(
                    {"name": doc["name"]}, {"$set": doc}, upsert=True
                )
            elif name == "index_insert_many":
                docs = query_func()
                self.db.products.insert_many(docs)
            elif name == "index_insert_returning":
                doc = query_func()
                result = self.db.products.insert_one(doc)
                _ = result.inserted_id
            elif name == "index_select_single":
                doc_filter = query_func()
                list(self.db.users.find(doc_filter))
            elif name == "index_select_where":
                doc_filter = query_func()
                list(self.db.users.find(doc_filter))
            elif name == "index_select_join":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "index_select_aggregate":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "index_select_pagination":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "index_select_distinct":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "index_update_single":
                data = query_func()
                self.db.users.update_one(data["filter"], data["update"])
            elif name == "index_update_many":
                data = query_func()
                self.db.products.update_many(data["filter"], data["update"])
            elif name == "index_update_in":
                data = query_func()
                self.db.products.update_many(data["filter"], data["update"])
            elif name == "index_update_case":
                data = query_func()
                for op in data["operations"]:
                    self.db.orders.update_one(op["filter"], op["update"])
            elif name == "index_update_join":
                data = query_func()
                self.db.orders.update_many(data["filter"], data["update"])
            elif name == "index_update_upsert":
                data = query_func()
                self.db.products.update_one(data["filter"], data["update"], upsert=True)
            elif name == "index_delete_single":
                doc_filter = query_func()
                self.db.users.delete_one(doc_filter)
            elif name == "index_delete_many":
                doc_filter = query_func()
                self.db.users.delete_many(doc_filter)
            elif name == "index_delete_in":
                doc_filter = query_func()
                self.db.products.delete_many(doc_filter)
            elif name == "index_delete_cascade":
                doc_filter = query_func()
                self.db.orders.delete_many(doc_filter)
            elif name == "index_delete_join":
                pipeline = query_func()
                matching = list(self.db.orders.aggregate(pipeline))
                if matching:
                    ids = [doc["_id"] for doc in matching]
                    self.db.orders.delete_many({"_id": {"$in": ids}})
            elif name == "index_delete_truncate":
                self.db.addresses.delete_many({})

            if status == "ok":
                elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("mongo", name, size, elapsed, size, trial=trial, status=status)

        return results

    def run_explain_queries(self, trial=1):
        for name, query_func in EXPLAIN_OPERATIONS.items():
            start = time.time()
            plan_text = ""

            if name == "explain_insert":
                doc = query_func()
                self.db.users.insert_one(doc)
                plan_text = "insert"
            elif name == "explain_select":
                doc_filter = query_func()
                list(self.db.users.find(doc_filter).explain())
            elif name == "explain_select_where":
                doc_filter = query_func()
                list(self.db.users.find(doc_filter).explain())
            elif name == "explain_select_join":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "explain_select_aggregate":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "explain_select_pagination":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "explain_select_distinct":
                pipeline = query_func()
                list(self.db.orders.aggregate(pipeline))
            elif name == "explain_update":
                update_doc = query_func()
                self.db.users.update_one({"email": "user1@example.com"}, update_doc)
            elif name == "explain_update_many":
                update_doc = query_func()
                self.db.products.update_many(update_doc.get("$match", {}), update_doc)
            elif name == "explain_update_in":
                update_doc = query_func()
                self.db.products.update_many(update_doc.get("$match", {}), update_doc)
            elif name == "explain_delete":
                doc_filter = query_func()
                self.db.users.delete_one(doc_filter)
            elif name == "explain_delete_many":
                doc_filter = query_func()
                self.db.users.delete_many(doc_filter)
            elif name == "explain_delete_in":
                doc_filter = query_func()
                self.db.products.delete_many(doc_filter)
            elif name == "explain_delete_cascade":
                doc_filter = query_func()
                self.db.orders.delete_many(doc_filter)
            elif name == "explain_indexed_select":
                doc_filter = query_func()
                list(self.db.products.find(doc_filter).explain())
            elif name == "explain_indexed_range":
                doc_filter = query_func()
                list(self.db.products.find(doc_filter).explain())
            elif name == "explain_complex_join":
                pipeline = query_func()
                list(self.db.users.aggregate(pipeline))

            elapsed = (time.time() - start) * 1000
            save_explain_result("mongo", name, plan_text, elapsed, trial=trial)

        return True

    def run_json_queries(self, size, trial=1):
        results = {}

        for name, query_func in JSON_OPERATIONS.items():
            start = time.time()
            doc_filter = query_func()
            list(self.db.users.find(doc_filter))
            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("mongo", f"json_{name}", size, elapsed, size, trial=trial)

        return results


def run_mongo_benchmark(size, operation_type="all", trial=1):
    bench = MongoBenchmark()
    bench.connect()

    try:
        if operation_type in ["all", "nonindexed"]:
            if bench.get_total_record_count() is None:
                bench.setup_collections(create_indexes=False)
                bench.populate_starting_data(size)
            else:
                needs_refresh, use_populate = bench.needs_starting_data_refresh(size)
                if needs_refresh:
                    bench.drop_indexes()
                    if use_populate:
                        bench.setup_collections(create_indexes=False)
                        bench.populate_starting_data(size)
                    else:
                        bench.reconcile_starting_data(size)
                else:
                    bench.drop_indexes()
            bench.run_nonindexed_queries(size, trial=trial)

        if operation_type in ["all", "indexed"]:
            if bench.get_total_record_count() is None:
                bench.setup_collections(create_indexes=True)
                bench.populate_starting_data(size)
            else:
                needs_refresh, use_populate = bench.needs_starting_data_refresh(size)
                if needs_refresh:
                    bench.ensure_indexes()
                    if use_populate:
                        bench.setup_collections(create_indexes=True)
                        bench.populate_starting_data(size)
                    else:
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
    run_mongo_benchmark(1000, "all")
