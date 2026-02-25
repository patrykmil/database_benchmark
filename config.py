SIZES = [500000, 1000000, 10000000]

DATABASES = {
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "database": "benchmark",
        "user": "postgres",
        "password": "postgres",
    },
    "sqlite": {"database": "benchmark.db"},
    "mongo": {"host": "localhost", "port": 27017, "database": "benchmark"},
    "unqlite": {"database": "benchmark.unqlite"},
}

CSV_FILE = "results/benchmark_results.csv"

CRUD_OPERATIONS = [
    "insert_single",
    "insert_bulk",
    "select_single",
    "select_where",
    "select_join",
    "update_single",
    "update_many",
    "delete_single",
    "delete_many",
    "aggregate_count",
    "aggregate_sum",
    "aggregate_avg",
]

INDEXED_OPERATIONS = [
    "insert_indexed",
    "select_indexed",
    "select_range",
    "select_like",
    "select_order_by",
    "update_indexed",
    "delete_indexed",
    "select_between",
    "select_in",
    "select_exists",
    "select_group_by",
    "select_having",
]

EXPLAIN_QUERIES = [
    "explain_select",
    "explain_join",
    "explain_aggregate",
    "explain_subquery",
    "explain_indexed",
    "explain_complex_join",
]
