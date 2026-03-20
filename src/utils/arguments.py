import argparse

from src.config.sizes import SIZES_MAP

DATABASES = {
    "postgres": "postgres",
    "sqlite": "sqlite",
    "mongo": "mongo",
    "unqlite": "unqlite",
    "all": "all",
}

OPERATIONS = {
    "nonindexed": "nonindexed",
    "indexed": "indexed",
    "explain": "explain",
    "json": "json",
    "all": "all",
}


def parse_args():
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
            "Data size: exact number (5000, 500000, 1000000...), "
            "compact (5k, 500k, 1m, 5m, 10m, 25m, 50m), "
            "or profiles: standard=[500k,1m,10m], huge=[25m,50m], all=[standard+huge]"
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
            "Draw line diagrams from results/benchmark_summary.csv and save to results/diagrams"
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

    if args.trials < 1:
        parser.error("--trials must be at least 1")

    return args
