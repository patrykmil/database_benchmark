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
# nonindexed operations only
python main.py --db postgres --operation nonindexed --size 5000

# Indexed queries
python main.py --db sqlite --operation indexed --size 5000

# EXPLAIN ANALYZE (SQL only)
python main.py --db postgres --operation explain

# JSON queries (SQL only)
python main.py --db sqlite --operation json

# Run all databases with all operations and sizes and 3 trials
python main.py --db all --operation all --size all --trials 3

# Draw diagrams from benchmark summary
python main.py --draw

# Generate extended analysis from summary
python main.py --analyze
```

### Available options

**--db**: `postgres`, `sqlite`, `mongo`, `unqlite`, `all`

**--operation**: `nonindexed`, `indexed`, `explain`, `json`, `all`

**--size**: `5k/5000`, `500k/500000/small`, `1m/1000000/medium`, `10m/10000000/large`, `25m/25000000`, `50m/50000000`, `all`

**--trials**: `1`, `2`, `3`, ...

**--draw**

**--analyze**

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
- `results/benchmark_summary.csv` - aggregated summary (uses last 3 samples)
- `results/benchmark_analysis.md` - extended analysis from summary (uses last 3 samples)
- `results/explain/{db}_explain_trial{trial}_{timestamp}.csv` - EXPLAIN plans
- `results/diagrams/{operation}_{timestamp}.png` - operation line diagrams
- `results/diagrams/all_operations_average_{timestamp}.png` - average line chart across all operations
- `results/diagrams/operation_type_boxplot_{timestamp}.png` - seaborn boxplot grouped by insert/select/update/delete and database
