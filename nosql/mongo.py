import time

from pymongo import MongoClient

from config import DATABASES
from nosql.queries import (
    EXPLAIN_OPERATIONS,
    INDEXED_OPERATIONS,
    JSON_OPERATIONS,
    NONINDEXED_OPERATIONS,
)
from utils.generator import generate_bulk_users
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

    def bulk_insert(self, collection, count, generator_func):
        docs = generator_func(count)
        getattr(self.db, collection).insert_many(docs)

    def run_nonindexed_queries(self, size):
        results = {}

        for name, query_func in NONINDEXED_OPERATIONS.items():
            start = time.time()

            if name == "insert_single":
                doc = query_func()
                self.db.users.insert_one(doc)
            elif name == "insert_bulk":
                docs = query_func()
                self.db.users.insert_many(docs)
            elif name == "insert_ignore":
                doc = query_func()
                try:
                    self.db.users.insert_one(doc)
                except:
                    pass
            elif name == "insert_upsert":
                doc = query_func()
                self.db.users.update_one(
                    {"email": "test@example.com"}, doc, upsert=True
                )
            elif name == "insert_many":
                docs = query_func()
                self.db.categories.insert_many(docs)
            elif name == "insert_returning":
                doc = query_func()
                self.db.users.insert_one(doc)
            elif name == "select_single":
                doc_filter = query_func(1)
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
                update_doc = query_func()
                self.db.users.update_one({"_id": 1}, update_doc)
            elif name == "update_many":
                update_doc = query_func()
                self.db.users.update_many({}, update_doc)
            elif name == "update_in":
                update_doc = query_func()
                self.db.users.update_many({"_id": {"$in": [1, 2, 3]}}, update_doc)
            elif name == "update_case":
                update_doc = query_func()
                pass
            elif name == "update_join":
                update_doc = query_func()
                pass
            elif name == "update_upsert":
                doc = query_func()
                self.db.products.update_one(
                    {"name": "upsert_product"}, {"$set": doc}, upsert=True
                )
            elif name == "delete_single":
                doc_filter = query_func(999999)
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
                pass
            elif name == "delete_truncate":
                self.db.addresses.delete_many({})

            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("mongo", name, size, elapsed, size)

        return results

    def run_indexed_queries(self, size):
        results = {}

        for name, query_func in INDEXED_OPERATIONS.items():
            start = time.time()

            if name == "index_insert_single":
                doc = query_func()
                self.db.users.insert_one(doc)
            elif name == "index_insert_bulk":
                docs = query_func()
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
                    {"name": "upsert_product"}, {"$set": doc}, upsert=True
                )
            elif name == "index_insert_many":
                docs = query_func()
                self.db.products.insert_many(docs)
            elif name == "index_insert_returning":
                doc = query_func()
                self.db.products.insert_one(doc)
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
                update_doc = query_func()
                self.db.users.update_one({"email": "user1@example.com"}, update_doc)
            elif name == "index_update_many":
                update_doc = query_func()
                self.db.products.update_many({"category_id": 1}, update_doc)
            elif name == "index_update_in":
                update_doc = query_func()
                self.db.products.update_many(
                    {"category_id": {"$in": [1, 2, 3]}}, update_doc
                )
            elif name == "index_update_case":
                pass
            elif name == "index_update_join":
                update_doc = query_func()
                pass
            elif name == "index_update_upsert":
                doc = query_func()
                self.db.products.update_one(
                    {"name": "existing_product"}, {"$set": doc}, upsert=True
                )
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
                pass
            elif name == "index_delete_truncate":
                self.db.addresses.delete_many({})

            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("mongo", name, size, elapsed, size)

        return results

    def run_explain_queries(self):
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
            save_explain_result("mongo", name, plan_text, elapsed)

        return True

    def run_json_queries(self, size):
        results = {}

        for name, query_func in JSON_OPERATIONS.items():
            start = time.time()
            doc_filter = query_func()
            list(self.db.users.find(doc_filter))
            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("mongo", f"json_{name}", size, elapsed, size)

        return results


def run_mongo_benchmark(size, operation_type="all"):
    bench = MongoBenchmark()
    bench.connect()

    try:
        if operation_type in ["all", "nonindexed"]:
            bench.setup_collections(create_indexes=False)
            bench.bulk_insert("users", size, generate_bulk_users)
            bench.run_nonindexed_queries(size)

        if operation_type in ["all", "indexed"]:
            bench.setup_collections(create_indexes=True)
            bench.run_indexed_queries(size)

        if operation_type in ["all", "explain"]:
            bench.run_explain_queries()

        if operation_type in ["all", "json"]:
            bench.run_json_queries(size)

    finally:
        bench.close()


if __name__ == "__main__":
    run_mongo_benchmark(1000, "all")
