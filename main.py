#!/usr/bin/env python3
import argparse
import sys

from config import SIZES
from nosql.mongo import run_mongo_benchmark
from nosql.unqlite import run_unqlite_benchmark
from sql.postgres import run_postgres_benchmark
from sql.sqlite import run_sqlite_benchmark
from utils.results import init_csv

DATABASES = {
    "postgres": run_postgres_benchmark,
    "sqlite": run_sqlite_benchmark,
    "mongo": run_mongo_benchmark,
    "unqlite": run_unqlite_benchmark,
    "all": "all",
}

OPERATIONS = {
    "crud": "crud",
    "indexed": "indexed",
    "explain": "explain",
    "json": "json",
    "all": "all",
}

SIZES_MAP = {"500000": 500000, "1000000": 1000000, "10000000": 10000000, "all": "all"}


def main():
    parser = argparse.ArgumentParser(description="Database Benchmark Tool")
    parser.add_argument(
        "--db",
        choices=list(DATABASES.keys()),
        required=True,
        help="Database to benchmark",
    )
    parser.add_argument(
        "--operation",
        choices=list(OPERATIONS.keys()),
        default="all",
        help="Operation type to run",
    )
    parser.add_argument(
        "--size",
        default="all",
        help="Size of test data (500000, 1000000, 10000000, or all)",
    )

    args = parser.parse_args()

    init_csv()

    sizes = SIZES if args.size == "all" else [SIZES_MAP[args.size]]

    if args.db == "all":
        dbs = ["postgres", "sqlite", "mongo", "unqlite"]
    else:
        dbs = [args.db]

    for db in dbs:
        print(f"\n{'=' * 50}")
        print(f"Running benchmark for: {db}")
        print(f"{'=' * 50}")

        for size in sizes:
            print(f"\n--- Size: {size:,} ---")
            try:
                DATABASES[db](size, OPERATIONS[args.operation])
                print(f"Completed: {db} - {size:,}")
            except Exception as e:
                print(f"Error running {db} with size {size}: {e}")
                import traceback

                traceback.print_exc()

    print(f"\n{'=' * 50}")
    print("All benchmarks completed!")
    print(f"Results saved to: results/benchmark_results.csv")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
