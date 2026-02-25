import random
import string
from datetime import datetime, timedelta


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
