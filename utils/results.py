import csv
import os
from collections import defaultdict
from datetime import datetime

from config import CSV_FILE

SUMMARY_CSV_FILE = "results/benchmark_summary.csv"


def init_csv():
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "database",
                    "operation",
                    "size",
                    "time_ms",
                    "elements",
                    "trial",
                    "timestamp",
                ]
            )


def init_summary_csv():
    os.makedirs(os.path.dirname(SUMMARY_CSV_FILE), exist_ok=True)
    with open(SUMMARY_CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "database",
                "operation",
                "size",
                "avg_time_ms",
                "min_time_ms",
                "max_time_ms",
                "samples_count",
                "generated_at",
            ]
        )


def build_summary_csv():
    if not os.path.exists(CSV_FILE):
        return

    grouped = defaultdict(list)
    with open(CSV_FILE, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                key = (row["database"], row["operation"], int(row["size"]))
                grouped[key].append(float(row["time_ms"]))
            except (KeyError, ValueError, TypeError):
                continue

    init_summary_csv()

    generated_at = datetime.now().isoformat()
    with open(SUMMARY_CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        for database, operation, size in sorted(grouped.keys()):
            times = grouped[(database, operation, size)]
            if not times:
                continue
            avg_time = sum(times) / len(times)
            writer.writerow(
                [
                    database,
                    operation,
                    size,
                    round(avg_time, 2),
                    round(min(times), 2),
                    round(max(times), 2),
                    len(times),
                    generated_at,
                ]
            )


def save_result(database, operation, size, time_ms, elements, trial=1):
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                database,
                operation,
                size,
                round(time_ms, 2),
                elements,
                trial,
                datetime.now().isoformat(),
            ]
        )


def save_explain_result(database, query_name, plan_text, execution_time, trial=1):
    explain_file = (
        f"results/{database}_explain_trial{trial}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv"
    )
    os.makedirs("results", exist_ok=True)
    with open(explain_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["database", "query", "plan", "execution_time_ms", "trial", "timestamp"]
        )
        writer.writerow(
            [
                database,
                query_name,
                plan_text,
                round(execution_time, 2),
                trial,
                datetime.now().isoformat(),
            ]
        )
