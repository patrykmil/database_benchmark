#!/usr/bin/env python3
from datetime import datetime

from arguments import DATABASES, OPERATIONS, parse_args
from nosql.mongo import run_mongo_benchmark
from nosql.unqlite import run_unqlite_benchmark
from sizes import SIZES_MAP, get_sizes
from sql.postgres import run_postgres_benchmark
from sql.sqlite import run_sqlite_benchmark
from utils.results import (
    build_extended_analysis,
    build_summary_csv,
    draw_summary_diagrams,
    init_results_csv,
)

DATABASE_FUNCTIONS = {
    "postgres": run_postgres_benchmark,
    "sqlite": run_sqlite_benchmark,
    "mongo": run_mongo_benchmark,
    "unqlite": run_unqlite_benchmark,
}


def main():
    args = parse_args()

    init_results_csv()

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

    sizes = get_sizes(args.size)
    if sizes is None:
        raise ValueError(
            f"Invalid size: {args.size}. Must be one of: {', '.join(SIZES_MAP.keys())}"
        )

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
                print(
                    f"Trial {trial}/{args.trials} -- Start-time: {datetime.now().strftime('%H:%M:%S')}"
                )
                try:
                    DATABASE_FUNCTIONS[db](
                        size, OPERATIONS[args.operation], trial=trial
                    )
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
