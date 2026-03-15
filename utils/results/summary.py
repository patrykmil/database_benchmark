import csv
import os
from collections import defaultdict
from datetime import datetime

from config import RESULTS_CSV_FILE, SUMMARY_CSV_FILE

SUMMARY_LAST_SAMPLES = 3

SUMMARY_COLUMNS = [
    "database",
    "operation",
    "size",
    "avg_time_ms",
    "min_time_ms",
    "max_time_ms",
    "samples_count",
    "generated_at",
]


def init_summary_csv():
    os.makedirs(os.path.dirname(SUMMARY_CSV_FILE), exist_ok=True)
    with open(SUMMARY_CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(SUMMARY_COLUMNS)


def build_summary_csv():
    if not os.path.exists(RESULTS_CSV_FILE):
        return

    grouped = defaultdict(list)
    with open(RESULTS_CSV_FILE, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                status = row.get("status", "ok")
                if status != "ok":
                    continue
                key = (row["database"], row["operation"], int(row["size"]))
                grouped[key].append(float(row["time_ms"]))
            except (KeyError, ValueError, TypeError):
                continue

    init_summary_csv()

    generated_at = datetime.now().isoformat()
    with open(SUMMARY_CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        for database, operation, size in sorted(grouped.keys()):
            times = grouped[(database, operation, size)][-SUMMARY_LAST_SAMPLES:]
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
