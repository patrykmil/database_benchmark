#!/usr/bin/env python3
import argparse

from config import SIZES
from nosql.mongo import run_mongo_benchmark
from nosql.unqlite import run_unqlite_benchmark
from sql.postgres import run_postgres_benchmark
from sql.sqlite import run_sqlite_benchmark
from utils.results import (
    build_extended_analysis,
    build_summary_csv,
    draw_summary_diagrams,
    init_csv,
)

DATABASES = {
    "postgres": run_postgres_benchmark,
    "sqlite": run_sqlite_benchmark,
    "mongo": run_mongo_benchmark,
    "unqlite": run_unqlite_benchmark,
    "all": "all",
}

OPERATIONS = {
    "nonindexed": "nonindexed",
    "indexed": "indexed",
    "explain": "explain",
    "json": "json",
    "all": "all",
}

SIZES_MAP = {
    "5000": 5_000,
    "500000": 500_000,
    "1000000": 1_000_000,
    "10000000": 10_000_000,
    "small": 500_000,
    "medium": 1_000_000,
    "large": 10_000_000,
    "all": "all",
}


def main():
    parser = argparse.ArgumentParser(description="Database Benchmark Tool")
    parser.add_argument(
        "--db",
        choices=list(DATABASES.keys()),
        default="all",
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
        choices=list(SIZES_MAP.keys()),
        default="all",
        help=(
            "Size profile: small/medium/large "
            "(500000/1000000/10000000), exact numeric value, or all"
        ),
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=1,
        help="Number of independent benchmark trials per database and size",
    )
    parser.add_argument(
        "--draw",
        action="store_true",
        help=(
            "Draw line diagrams from results/benchmark_summary.csv "
            "and save to results/diagrams"
        ),
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help=(
            "Create extended analysis from results/benchmark_summary.csv "
            "(up to last 3 samples) and save to results/benchmark_analysis.md"
        ),
    )

    args = parser.parse_args()

    init_csv()

    if args.trials < 1:
        parser.error("--trials must be at least 1")

    if args.draw:
        diagrams = draw_summary_diagrams()
        if not diagrams:
            print("No summary data available to draw diagrams.")
            return

        print(f"Generated {len(diagrams)} diagrams:")
        for path in diagrams:
            print(f"- {path}")
        return

    if args.analyze:
        analysis_path = build_extended_analysis()
        if not analysis_path:
            print("No summary data available to analyze.")
            return

        print(f"Extended analysis saved to: {analysis_path}")
        return

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
            for trial in range(1, args.trials + 1):
                print(f"Trial {trial}/{args.trials}")
                try:
                    DATABASES[db](size, OPERATIONS[args.operation], trial=trial)
                    print(f"Completed: {db} - {size:,} (trial {trial})")
                except Exception as e:
                    print(f"Error running {db} with size {size} on trial {trial}: {e}")
                    import traceback

                    traceback.print_exc()

    print(f"\n{'=' * 50}")
    print("All benchmarks completed!")
    print("Results saved to: results/benchmark_results.csv")
    build_summary_csv()
    print("Summary saved to: results/benchmark_summary.csv")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
