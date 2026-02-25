import time

from pymongo import MongoClient

from config import DATABASES
from nosql.queries import (
    AGGREGATE_OPERATIONS,
    CRUD_OPERATIONS,
    INDEXED_OPERATIONS,
    JOIN_OPERATIONS,
    JSON_OPERATIONS,
)
from utils.generator import generate_bulk_users
from utils.results import save_result


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

    def setup_collections(self):
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

    def run_crud_queries(self, size):
        results = {}

        user = CRUD_OPERATIONS["insert_single"]()
        start = time.time()
        self.db.users.insert_one(user)
        elapsed = (time.time() - start) * 1000
        results["insert_single"] = elapsed
        save_result("mongo", "insert_single", size, elapsed, size)

        users = generate_bulk_users(1000)
        start = time.time()
        self.db.users.insert_many(users)
        elapsed = (time.time() - start) * 1000
        results["insert_bulk"] = elapsed
        save_result("mongo", "insert_bulk", size, elapsed, size)

        start = time.time()
        list(self.db.users.find({}))
        elapsed = (time.time() - start) * 1000
        results["select_single"] = elapsed
        save_result("mongo", "select_single", size, elapsed, size)

        start = time.time()
        list(self.db.users.find({"email": {"$regex": "test"}}))
        elapsed = (time.time() - start) * 1000
        results["select_where"] = elapsed
        save_result("mongo", "select_where", size, elapsed, size)

        start = time.time()
        list(
            self.db.orders.aggregate(
                [
                    {
                        "$lookup": {
                            "from": "users",
                            "localField": "user_id",
                            "foreignField": "_id",
                            "as": "user",
                        }
                    },
                    {"$limit": 100},
                ]
            )
        )
        elapsed = (time.time() - start) * 1000
        results["select_join"] = elapsed
        save_result("mongo", "select_join", size, elapsed, size)

        start = time.time()
        self.db.users.update_one({"_id": 1}, {"$set": {"name": "updated_name"}})
        elapsed = (time.time() - start) * 1000
        results["update_single"] = elapsed
        save_result("mongo", "update_single", size, elapsed, size)

        start = time.time()
        self.db.users.update_many({}, {"$set": {"verified": True}})
        elapsed = (time.time() - start) * 1000
        results["update_many"] = elapsed
        save_result("mongo", "update_many", size, elapsed, size)

        start = time.time()
        self.db.users.delete_one({})
        elapsed = (time.time() - start) * 1000
        results["delete_single"] = elapsed
        save_result("mongo", "delete_single", size, elapsed, size)

        start = time.time()
        self.db.users.delete_many({"created_at": {"$lt": "2020-01-01"}})
        elapsed = (time.time() - start) * 1000
        results["delete_many"] = elapsed
        save_result("mongo", "delete_many", size, elapsed, size)

        start = time.time()
        list(self.db.orders.aggregate([{"$count": "total"}]))
        elapsed = (time.time() - start) * 1000
        results["aggregate_count"] = elapsed
        save_result("mongo", "aggregate_count", size, elapsed, size)

        start = time.time()
        list(
            self.db.orders.aggregate(
                [{"$group": {"_id": None, "total": {"$sum": "$total"}}}]
            )
        )
        elapsed = (time.time() - start) * 1000
        results["aggregate_sum"] = elapsed
        save_result("mongo", "aggregate_sum", size, elapsed, size)

        start = time.time()
        list(
            self.db.products.aggregate(
                [{"$group": {"_id": None, "avg": {"$avg": "$price"}}}]
            )
        )
        elapsed = (time.time() - start) * 1000
        results["aggregate_avg"] = elapsed
        save_result("mongo", "aggregate_avg", size, elapsed, size)

        return results

    def run_indexed_queries(self, size):
        results = {}

        for name, query in INDEXED_OPERATIONS.items():
            start = time.time()
            if name == "select_order_by":
                list(self.db.orders.find({}).sort("created_at", -1).limit(100))
            else:
                list(self.db.products.find(query()))
            elapsed = (time.time() - start) * 1000
            results[name] = elapsed
            save_result("mongo", name, size, elapsed, size)

        return results


def run_mongo_benchmark(size, operation_type="all"):
    bench = MongoBenchmark()
    bench.connect()

    try:
        bench.setup_collections()

        if operation_type in ["all", "crud"]:
            bench.bulk_insert("users", size, generate_bulk_users)
            bench.run_crud_queries(size)

        if operation_type in ["all", "indexed"]:
            bench.run_indexed_queries(size)

    finally:
        bench.close()


if __name__ == "__main__":
    run_mongo_benchmark(1000, "all")
