SIZES = [10_000_000, 1_000_000, 500_000]

DATABASES = {
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "database": "benchmark",
        "user": "postgres",
        "password": "postgres",
    },
    "sqlite": {"database": "benchmark.sqlite"},
    "mongo": {"host": "localhost", "port": 27017, "database": "benchmark"},
    "unqlite": {"database": "benchmark.unqlite"},
}

CSV_FILE = "results/benchmark_results.csv"
