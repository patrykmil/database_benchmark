import csv
import os
from datetime import datetime

from config import CSV_FILE


def init_csv():
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["database", "operation", "size", "time_ms", "elements", "timestamp"]
            )


def save_result(database, operation, size, time_ms, elements):
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                database,
                operation,
                size,
                round(time_ms, 2),
                elements,
                datetime.now().isoformat(),
            ]
        )


def save_explain_result(database, query_name, plan_text, execution_time):
    explain_file = (
        f"results/{database}_explain_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    os.makedirs("results", exist_ok=True)
    with open(explain_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["database", "query", "plan", "execution_time_ms"])
        writer.writerow([database, query_name, plan_text, round(execution_time, 2)])
