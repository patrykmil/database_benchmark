# Database Benchmark

## Databases

| Type  | Server     | Single-file |
| ----- | ---------- | ----------- |
| SQL   | PostgreSQL | SQLite      |
| NoSQL | MongoDB    | UnQLite     |

## Setup

1. Install dependencies with 
```sh
pip install -r requirements.txt
```

2. Ensure PostgreSQL and MongoDB servers are running
```sh
docker compose up -d
```

## Usage

#### Run all benchmarks for a specific database
```sh
python main.py --db postgres --operation all --size all
```

#### Run specific operation types

```sh
# CRUD operations only
python main.py --db postgres --operation crud --size 5000

# Indexed queries
python main.py --db sqlite --operation indexed --size 5000

# EXPLAIN ANALYZE (SQL only)
python main.py --db postgres --operation explain

# JSON queries (SQL only)
python main.py --db sqlite --operation json

# Run all databases with all operations and sizes
python main.py --db all --operation all --size all
```

### Available options

**--db**: `postgres`, `sqlite`, `mongo`, `unqlite`, `all`

**--operation**: `crud`, `indexed`, `explain`, `json`, `all`

**--size**: `5000`, `500000`, `1000000`, `10000000`, `all`

## Schema (10 tables)

1. users - id, name, email, created_at, preferences (JSON)
2. categories - id, name, parent_id
3. products - id, name, price, category_id, attributes (JSON)
4. orders - id, user_id, status, total, created_at
5. order_items - id, order_id, product_id, quantity, price
6. reviews - id, user_id, product_id, rating, comment, metadata (JSON)
7. warehouses - id, name, location
8. inventory - id, product_id, warehouse_id, quantity
9. addresses - id, user_id, city, country, details (JSON)
10. payments - id, order_id, method, amount, data (JSON)

## Results

Results are stored in CSV format at:

- `results/benchmark_results.csv` - timing results
- `results/{db}_explain_{timestamp}.csv` - EXPLAIN plans