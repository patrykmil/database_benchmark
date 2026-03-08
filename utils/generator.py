import random
import string
from datetime import datetime, timedelta

STARTING_DATA_WEIGHTS = {
    "users": 0.20,
    "categories": 0.01,
    "products": 0.14,
    "orders": 0.20,
    "order_items": 0.28,
    "reviews": 0.06,
    "warehouses": 0.005,
    "inventory": 0.08,
    "addresses": 0.04,
    "payments": 0.025,
}

STARTING_DATA_ORDER = [
    "order_items",
    "orders",
    "users",
    "products",
    "inventory",
    "reviews",
    "addresses",
    "payments",
    "categories",
    "warehouses",
]


def random_string(length=10):
    return "".join(random.choices(string.ascii_letters, k=length))


def random_email():
    return f"{random_string(8)}@{random_string(5)}.com"


def random_date():
    return datetime.now() - timedelta(days=random.randint(0, 365))


def generate_user():
    return {
        "id": None,
        "name": random_string(20),
        "email": random_email(),
        "created_at": random_date(),
        "preferences": {
            "theme": random.choice(["light", "dark"]),
            "notifications": random.choice([True, False]),
            "language": random.choice(["en", "pl", "de"]),
        },
    }


def generate_product(category_id):
    return {
        "id": None,
        "name": random_string(30),
        "price": round(random.uniform(10, 1000), 2),
        "category_id": category_id,
        "attributes": {
            "color": random.choice(["red", "blue", "green", "black", "white"]),
            "weight": random.randint(1, 100),
            "tags": [random_string(5) for _ in range(3)],
        },
    }


def generate_category(parent_id=None):
    return {"id": None, "name": random_string(15), "parent_id": parent_id}


def generate_order(user_id):
    return {
        "id": None,
        "user_id": user_id,
        "status": random.choice(["pending", "completed", "cancelled"]),
        "total": round(random.uniform(50, 5000), 2),
        "created_at": random_date(),
    }


def generate_order_item(order_id, product_id):
    return {
        "id": None,
        "order_id": order_id,
        "product_id": product_id,
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(10, 500), 2),
    }


def generate_review(user_id, product_id):
    return {
        "id": None,
        "user_id": user_id,
        "product_id": product_id,
        "rating": random.randint(1, 5),
        "comment": random_string(100),
        "metadata": {
            "helpful": random.randint(0, 50),
            "verified": random.choice([True, False]),
        },
    }


def generate_address(user_id):
    return {
        "id": None,
        "user_id": user_id,
        "city": random_string(15),
        "country": random_string(15),
        "details": {
            "street": random_string(20),
            "zip": random_string(6),
            "phone": random_string(10),
        },
    }


def generate_payment(order_id):
    return {
        "id": None,
        "order_id": order_id,
        "method": random.choice(["card", "cash", "transfer"]),
        "amount": round(random.uniform(50, 5000), 2),
        "data": {
            "transaction_id": random_string(20),
            "processed": random.choice([True, False]),
        },
    }


def generate_inventory(product_id, warehouse_id):
    return {
        "id": None,
        "product_id": product_id,
        "warehouse_id": warehouse_id,
        "quantity": random.randint(0, 1000),
    }


def generate_warehouse():
    return {"id": None, "name": random_string(15), "location": random_string(20)}


def generate_bulk_users(count):
    return [generate_user() for _ in range(count)]


def generate_bulk_products(count, category_ids):
    return [generate_product(random.choice(category_ids)) for _ in range(count)]


def generate_bulk_categories(count):
    cats = []
    for i in range(count):
        parent = random.choice(cats)["id"] if cats and random.random() > 0.7 else None
        cat = generate_category(parent)
        cat["id"] = i + 1
        cats.append(cat)
    return cats


def split_starting_data(total_records):
    total = int(total_records)
    if total < len(STARTING_DATA_WEIGHTS):
        raise ValueError("target size is too small to split across all tables")

    raw = {name: total * weight for name, weight in STARTING_DATA_WEIGHTS.items()}
    counts = {name: int(value) for name, value in raw.items()}

    for table in STARTING_DATA_WEIGHTS:
        if counts[table] == 0:
            counts[table] = 1

    allocated = sum(counts.values())
    remainder = total - allocated

    if remainder > 0:
        order = sorted(
            STARTING_DATA_WEIGHTS.keys(),
            key=lambda name: raw[name] - int(raw[name]),
            reverse=True,
        )
        idx = 0
        while remainder > 0:
            counts[order[idx % len(order)]] += 1
            remainder -= 1
            idx += 1
    elif remainder < 0:
        to_remove = -remainder
        idx = 0
        while to_remove > 0:
            table = STARTING_DATA_ORDER[idx % len(STARTING_DATA_ORDER)]
            if counts[table] > 1:
                counts[table] -= 1
                to_remove -= 1
            idx += 1

    return counts


def generate_bulk_warehouses(count):
    return [generate_warehouse() for _ in range(count)]


def generate_bulk_orders(count, user_count):
    if user_count < 1:
        return []
    return [generate_order(random.randint(1, user_count)) for _ in range(count)]


def generate_bulk_order_items(count, order_count, product_count):
    if order_count < 1 or product_count < 1:
        return []
    return [
        generate_order_item(
            random.randint(1, order_count),
            random.randint(1, product_count),
        )
        for _ in range(count)
    ]


def generate_bulk_reviews(count, user_count, product_count):
    if user_count < 1 or product_count < 1:
        return []
    return [
        generate_review(
            random.randint(1, user_count),
            random.randint(1, product_count),
        )
        for _ in range(count)
    ]


def generate_bulk_inventory(count, product_count, warehouse_count):
    if product_count < 1 or warehouse_count < 1:
        return []
    return [
        generate_inventory(
            random.randint(1, product_count),
            random.randint(1, warehouse_count),
        )
        for _ in range(count)
    ]


def generate_bulk_addresses(count, user_count):
    if user_count < 1:
        return []
    return [generate_address(random.randint(1, user_count)) for _ in range(count)]


def generate_bulk_payments(count, order_count):
    if order_count < 1:
        return []
    return [generate_payment(random.randint(1, order_count)) for _ in range(count)]
