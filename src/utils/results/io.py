import csv
import os
from datetime import datetime

from src.config.files import RESULTS_CSV_FILE

CSV_COLUMNS = [
    "database",
    "operation",
    "size",
    "time_ms",
    "elements",
    "trial",
    "status",
    "timestamp",
]


def init_results_csv():
    os.makedirs(os.path.dirname(RESULTS_CSV_FILE), exist_ok=True)
    if not os.path.exists(RESULTS_CSV_FILE):
        with open(RESULTS_CSV_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_COLUMNS)


def save_result(database, operation, size, time_ms, elements, trial=1, status="ok"):
    serialized_time = ""
    if time_ms is not None:
        serialized_time = round(time_ms, 2)

    with open(RESULTS_CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                database,
                operation,
                size,
                serialized_time,
                elements,
                trial,
                status,
                datetime.now().isoformat(),
            ]
        )


def save_explain_result(
    database, query_name, plan_text, execution_time, trial=1, status="ok"
):
    serialized_time = ""
    if execution_time is not None:
        serialized_time = round(execution_time, 2)

    explain_file = f"results/explain/{database}_explain_trial{trial}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv"
    os.makedirs("results/explain", exist_ok=True)
    with open(explain_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "database",
                "query",
                "plan",
                "execution_time_ms",
                "trial",
                "status",
                "timestamp",
            ]
        )
        writer.writerow(
            [
                database,
                query_name,
                plan_text,
                serialized_time,
                trial,
                status,
                datetime.now().isoformat(),
            ]
        )
