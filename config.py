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

RESULTS_CSV_FILE = "results/benchmark_results.csv"
SUMMARY_CSV_FILE = "results/benchmark_summary.csv"
ANALYSIS_FILE = "results/benchmark_analysis.md"
