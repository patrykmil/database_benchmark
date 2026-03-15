STANDARD_SIZES = [500_000, 1_000_000, 10_000_000]
HUGE_SIZES = [25_000_000, 50_000_000]
ALL_SIZES = STANDARD_SIZES + HUGE_SIZES

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
