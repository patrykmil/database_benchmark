# Database Benchmark Tool

A modular benchmark framework comparing SQL and NoSQL databases.

## Databases

| Type | Server | Single-file |
|------|--------|-------------|
| SQL | PostgreSQL | SQLite |
| NoSQL | MongoDB | UnQLite |

## Setup

1. Install dependenciespip install -r requirements.txt
```

2. Ensure:
```bash
 PostgreSQL and MongoDB servers are running

## Usage

### Run all benchmarks for a specific database
```bash
python main.py --db postgres --operation all --size all
```

### Run specific operation types
```bash
# CRUD operations only
python main.py --db postgres --operation crud --size 1000000

# Indexed queries
python main.py --db sqlite --operation indexed --size 1000000

# EXPLAIN ANALYZE (SQL only)
python main.py --db postgres --operation explain

# JSON queries (SQL only)
python main.py --db sqlite --operation json
```

### Available options

**--db**: `postgres`, `sqlite`, `mongo`, `unqlite`, `all`

**--operation**: `crud`, `indexed`, `explain`, `json`, `all`

**--size**: `500000`, `1000000`, `10000000`, `all`

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

## Query Types

- **CRUD**: 12 basic insert/select/update/delete/aggregate queries
- **Indexed**: 12 queries using indexed fields
- **EXPLAIN**: 6 queries with EXPLAIN ANALYZE plans (SQL only)
- **JSON**: 3 JSON extraction/containment queries (SQL only)

## Results

Results are stored in CSV format at:
- `results/benchmark_results.csv` - timing results
- `results/{db}_explain_{timestamp}.csv` - EXPLAIN plans

## Examples

```bash
# Run PostgreSQL CRUD benchmark with 1M records
python main.py --db postgres --operation crud --size 1000000

# Run all databases with all operations and sizes
python main.py --db all --operation all --size all

# Compare SQLite vs PostgreSQL with indexed queries
python main.py --db postgres --operation indexed --size 500000
python main.py --db sqlite --operation indexed --size 500000
```
