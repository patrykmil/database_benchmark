import csv
import math
import os
from collections import defaultdict
from datetime import datetime

from config import ANALYSIS_FILE, RESULTS_CSV_FILE, SUMMARY_CSV_FILE

MAX_SAMPLES_DEFAULT = 3


def _operation_type(operation):
    if operation.startswith("index_"):
        operation = operation[len("index_") :]

    for op_type in ("insert", "select", "update", "delete"):
        if operation.startswith(f"{op_type}_") or operation == op_type:
            return op_type

    return None


def build_extended_analysis(max_samples=MAX_SAMPLES_DEFAULT):
    if max_samples < 1:
        max_samples = 1

    if not os.path.exists(SUMMARY_CSV_FILE):
        return None

    summary_rows = []
    with open(SUMMARY_CSV_FILE, "r", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return None

    header = [cell.strip() for cell in rows[0]]
    has_header = header == [
        "database",
        "operation",
        "size",
        "avg_time_ms",
        "min_time_ms",
        "max_time_ms",
        "samples_count",
        "generated_at",
    ]

    data_rows = rows[1:] if has_header else rows

    for row in data_rows:
        try:
            if len(row) < 7:
                continue
            samples_count = int(row[6])
            summary_rows.append(
                {
                    "database": row[0],
                    "operation": row[1],
                    "size": int(row[2]),
                    "avg_time_ms": float(row[3]),
                    "min_time_ms": float(row[4]),
                    "max_time_ms": float(row[5]),
                    "samples_count": min(samples_count, max_samples),
                }
            )
        except (ValueError, TypeError):
            continue

    if not summary_rows:
        return None

    raw_grouped = defaultdict(list)
    if os.path.exists(RESULTS_CSV_FILE):
        with open(RESULTS_CSV_FILE, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    if row.get("status", "ok") != "ok":
                        continue
                    time_ms = float(row["time_ms"])
                    key = (row["database"], row["operation"], int(row["size"]))
                    trial = int(row.get("trial", 0) or 0)
                    timestamp = row.get("timestamp", "")
                except (KeyError, ValueError, TypeError):
                    continue

                raw_grouped[key].append((timestamp, trial, time_ms))

    latest_trials = {}
    for key, values in raw_grouped.items():
        sorted_values = sorted(values, key=lambda item: (item[0], item[1]))
        latest_trials[key] = [item[2] for item in sorted_values[-max_samples:]]

    db_times = defaultdict(list)
    operation_times = defaultdict(lambda: defaultdict(list))
    size_trends = defaultdict(list)
    coverage = defaultdict(int)

    for row in summary_rows:
        db = row["database"]
        op = row["operation"]
        size = row["size"]
        avg_time = row["avg_time_ms"]

        db_times[db].append(avg_time)
        operation_times[op][db].append(avg_time)
        size_trends[(db, op)].append((size, avg_time))
        coverage[row["samples_count"]] += 1

    db_ranking = []
    for db, times in db_times.items():
        avg = sum(times) / len(times)
        db_ranking.append((db, avg))
    db_ranking.sort(key=lambda item: item[1])

    op_leaders = []
    for operation, db_map in operation_times.items():
        db_avg = []
        for db, times in db_map.items():
            db_avg.append((db, sum(times) / len(times)))
        if not db_avg:
            continue
        db_avg.sort(key=lambda item: item[1])
        best_db, best_time = db_avg[0]
        worst_db, worst_time = db_avg[-1]
        ratio = (worst_time / best_time) if best_time > 0 else 0.0
        op_leaders.append((operation, best_db, best_time, worst_db, worst_time, ratio))
    op_leaders.sort(key=lambda item: item[0])

    stability_by_db = defaultdict(list)
    unstable_cases = []
    for key, samples in latest_trials.items():
        if not samples:
            continue
        mean_value = sum(samples) / len(samples)
        if mean_value <= 0:
            continue

        if len(samples) > 1:
            variance = sum((value - mean_value) ** 2 for value in samples) / len(
                samples
            )
            std_dev = math.sqrt(variance)
        else:
            std_dev = 0.0

        cv = (std_dev / mean_value) * 100
        db, op, size = key
        stability_by_db[db].append(cv)
        unstable_cases.append((cv, db, op, size, len(samples), mean_value))

    stability_ranking = []
    for db, cvs in stability_by_db.items():
        stability_ranking.append((db, sum(cvs) / len(cvs)))
    stability_ranking.sort(key=lambda item: item[1])

    unstable_cases.sort(key=lambda item: item[0], reverse=True)

    growth_cases = []
    for (db, op), points in size_trends.items():
        if len(points) < 2:
            continue
        ordered_points = sorted(points, key=lambda item: item[0])
        min_size, min_time = ordered_points[0]
        max_size, max_time = ordered_points[-1]
        if min_time <= 0:
            continue
        growth = max_time / min_time
        growth_cases.append((growth, db, op, min_size, max_size, min_time, max_time))

    growth_cases.sort(key=lambda item: item[0], reverse=True)

    os.makedirs(os.path.dirname(ANALYSIS_FILE), exist_ok=True)
    generated_at = datetime.now().isoformat(timespec="seconds")

    with open(ANALYSIS_FILE, "w", newline="") as f:
        f.write(f"Generated at: {generated_at}\n")

        total_cases = len(summary_rows)
        f.write(f"- Cases in summary: {total_cases}\n")
        for sample_count in sorted(coverage.keys()):
            f.write(
                f"- Cases with {sample_count} sample(s): {coverage[sample_count]}\n"
            )
        f.write("\n")

        f.write("## Global database ranking (lower is better)\n")
        for index, (db, avg_time) in enumerate(db_ranking, start=1):
            f.write(
                f"{index}. {db}: {avg_time:.2f} ms average across operations/sizes\n"
            )
        f.write("\n")

        f.write("## Operation leaders\n")
        for operation, best_db, best_time, worst_db, worst_time, ratio in op_leaders:
            f.write(
                f"- {operation}: best={best_db} ({best_time:.2f} ms), "
                f"slowest={worst_db} ({worst_time:.2f} ms), gap={ratio:.2f}x\n"
            )
        f.write("\n")

        f.write("## Stability (coefficient of variation from last trials)\n")
        if stability_ranking:
            for db, avg_cv in stability_ranking:
                f.write(f"- {db}: average CV={avg_cv:.2f}%\n")
        else:
            f.write("- Not enough trial data for stability analysis\n")

        f.write("\nTop volatile cases:\n")
        if unstable_cases:
            for cv, db, op, size, sample_count, mean_value in unstable_cases[:10]:
                f.write(
                    f"- {db} / {op} / size={size}: CV={cv:.2f}% "
                    f"(samples={sample_count}, mean={mean_value:.2f} ms)\n"
                )
        else:
            f.write("- No volatile cases detected\n")
        f.write("\n")

        f.write("## Scaling sensitivity (largest vs smallest size)\n")
        if growth_cases:
            for growth, db, op, min_size, max_size, min_time, max_time in growth_cases[
                :10
            ]:
                f.write(
                    f"- {db} / {op}: {min_size}->{max_size}, "
                    f"{min_time:.2f}->{max_time:.2f} ms ({growth:.2f}x)\n"
                )
        else:
            f.write("- Not enough size points for scaling analysis\n")

    return ANALYSIS_FILE
