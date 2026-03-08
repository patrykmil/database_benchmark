import csv
import os
from collections import defaultdict
from datetime import datetime

from config import CSV_FILE

SUMMARY_CSV_FILE = "results/benchmark_summary.csv"
SUMMARY_LAST_SAMPLES = 3
DB_COLORS = {
    "postgres": "#1f77b4",
    "sqlite": "#0ee3ff",
    "mongo": "#2ca02c",
    "unqlite": "#76D03D",
}


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
                    "status",
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


def draw_summary_diagrams():
    if not os.path.exists(SUMMARY_CSV_FILE):
        return []

    grouped = defaultdict(lambda: defaultdict(list))
    with open(SUMMARY_CSV_FILE, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                operation = row["operation"]
                database = row["database"]
                size = int(row["size"])
                avg_time = float(row["avg_time_ms"])
            except (KeyError, ValueError, TypeError):
                continue
            grouped[operation][database].append((size, avg_time))

    if not grouped:
        return []

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    output_dir = "results/diagrams"
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    generated_files = []
    for operation in sorted(grouped.keys()):
        plt.figure(figsize=(10, 6))

        for database in sorted(grouped[operation].keys()):
            points = sorted(grouped[operation][database], key=lambda x: x[0])
            sizes = [point[0] for point in points]
            times = [point[1] for point in points]
            color = DB_COLORS.get(database)

            plt.plot(sizes, times, marker="o", linewidth=2, label=database, color=color)

        plt.title(f"Operation: {operation}")
        plt.xlabel("Data Size")
        plt.ylabel("Avg Time (ms)")
        ax = plt.gca()
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.4)
        plt.tight_layout()

        safe_operation = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in operation)
        output_file = f"{safe_operation}_{timestamp}.png"
        output_path = os.path.join(output_dir, output_file)
        plt.savefig(output_path, dpi=150)
        plt.close()
        generated_files.append(output_path)

    return generated_files


def save_result(database, operation, size, time_ms, elements, trial=1, status="ok"):
    serialized_time = ""
    if time_ms is not None:
        serialized_time = round(time_ms, 2)

    with open(CSV_FILE, "a", newline="") as f:
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


def save_explain_result(database, query_name, plan_text, execution_time, trial=1, status="ok"):
    serialized_time = ""
    if execution_time is not None:
        serialized_time = round(execution_time, 2)

    explain_file = (
        f"results/explain/{database}_explain_trial{trial}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv"
    )
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
