import time

from pymongo import MongoClient

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
                # SQL: INSERT ... ON CONFLICT (email) DO UPDATE SET name, preferences
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
            if bench.needs_starting_data_refresh(size):
                bench.setup_collections(create_indexes=False)
                bench.populate_starting_data(size)
            else:
                bench.drop_indexes()
            bench.run_nonindexed_queries(size, trial=trial)

        if operation_type in ["all", "indexed"]:
            if bench.needs_starting_data_refresh(size):
                bench.setup_collections(create_indexes=True)
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
    run_mongo_benchmark(1000, "all")
