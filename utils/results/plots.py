import csv
import os
from collections import defaultdict

import pandas as pd
import seaborn as sns
from matplotlib.ticker import FuncFormatter

from config import SUMMARY_CSV_FILE

DB_COLORS = {
    "postgres": "#1f77b4",
    "sqlite": "#0ee3ff",
    "mongo": "#2ca02c",
    "unqlite": "#76D03D",
}


def _operation_type(operation):
    if operation.startswith("index_"):
        operation = operation[len("index_") :]

    for op_type in ("insert", "select", "update", "delete"):
        if operation.startswith(f"{op_type}_") or operation == op_type:
            return op_type

    return None


def draw_summary_diagrams():
    if not os.path.exists(SUMMARY_CSV_FILE):
        return []

    grouped = defaultdict(lambda: defaultdict(list))
    overall_by_db_size = defaultdict(list)
    index_overall_by_db_size = defaultdict(list)
    boxplot_rows = []
    with open(SUMMARY_CSV_FILE, "r", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return []

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
            if len(row) < 4:
                continue
            database = row[0]
            operation = row[1]
            size = int(row[2])
            avg_time = float(row[3])
        except (ValueError, TypeError):
            continue

        grouped[operation][database].append((size, avg_time))
        overall_by_db_size[(database, size)].append(avg_time)
        if operation.startswith("index_"):
            index_overall_by_db_size[(database, size)].append(avg_time)
        op_type = _operation_type(operation)
        if op_type:
            boxplot_rows.append((database, op_type, avg_time, size))

    if not grouped:
        return []

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    standard_sizes = {500000, 1000000, 10000000}
    huge_sizes = {10000000, 25000000, 50000000}
    huge_dbs = {"postgres", "sqlite"}

    generated_files = []
    for sizes_subset, subdir, prefix in [
        (standard_sizes, "standard", ""),
        (huge_sizes, "huge", "huge_"),
    ]:
        output_dir = f"results/diagrams/{subdir}"
        os.makedirs(output_dir, exist_ok=True)

        filtered_grouped = defaultdict(lambda: defaultdict(list))
        filtered_overall = defaultdict(list)
        filtered_index_overall = defaultdict(list)
        filtered_boxplot = []

        for operation, op_dbs in grouped.items():
            for database, points in op_dbs.items():
                if subdir == "huge" and database not in huge_dbs:
                    continue
                filtered_points = [(s, t) for s, t in points if s in sizes_subset]
                if filtered_points:
                    filtered_grouped[operation][database] = filtered_points

        for (database, size), values in overall_by_db_size.items():
            if subdir == "huge" and database not in huge_dbs:
                continue
            if size in sizes_subset and values:
                filtered_overall[database].append((size, sum(values) / len(values)))

        for (database, size), values in index_overall_by_db_size.items():
            if subdir == "huge" and database not in huge_dbs:
                continue
            if size in sizes_subset and values:
                filtered_index_overall[database].append(
                    (size, sum(values) / len(values))
                )

        for db, op_type, avg_time, size in boxplot_rows:
            if subdir == "huge" and db not in huge_dbs:
                continue
            if size in sizes_subset:
                filtered_boxplot.append((db, op_type, avg_time))

        for operation in sorted(filtered_grouped.keys()):
            plt.figure(figsize=(10, 6))

            for database in sorted(filtered_grouped[operation].keys()):
                points = sorted(
                    filtered_grouped[operation][database], key=lambda x: x[0]
                )
                sizes = [point[0] for point in points]
                times = [point[1] for point in points]
                color = DB_COLORS.get(database)

                plt.plot(
                    sizes, times, marker="o", linewidth=2, label=database, color=color
                )

            plt.title(f"Operation: {operation}")
            plt.xlabel("Data Size")
            plt.ylabel("Avg Time (ms)")
            ax = plt.gca()
            ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
            plt.legend()
            plt.grid(True, linestyle="--", alpha=0.4)
            plt.tight_layout()

            safe_operation = "".join(
                c if c.isalnum() or c in ("-", "_") else "_" for c in operation
            )
            output_file = f"{prefix}{safe_operation}.png"
            output_path = os.path.join(output_dir, output_file)
            plt.savefig(output_path, dpi=150)
            plt.close()
            generated_files.append(output_path)

        if filtered_overall:
            plt.figure(figsize=(10, 6))

            for database in sorted(filtered_overall.keys()):
                points = sorted(filtered_overall[database], key=lambda x: x[0])
                sizes = [point[0] for point in points]
                times = [point[1] for point in points]
                color = DB_COLORS.get(database)
                plt.plot(
                    sizes, times, marker="o", linewidth=2, label=database, color=color
                )

            plt.title("Average across all operations")
            plt.xlabel("Data Size")
            plt.ylabel("Avg Time (ms)")
            ax = plt.gca()
            ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
            plt.legend()
            plt.grid(True, linestyle="--", alpha=0.4)
            plt.tight_layout()

            output_file = f"{prefix}all_operations_average.png"
            output_path = os.path.join(output_dir, output_file)
            plt.savefig(output_path, dpi=150)
            plt.close()
            generated_files.append(output_path)

        if filtered_index_overall:
            plt.figure(figsize=(10, 6))

            for database in sorted(filtered_index_overall.keys()):
                points = sorted(filtered_index_overall[database], key=lambda x: x[0])
                sizes = [point[0] for point in points]
                times = [point[1] for point in points]
                color = DB_COLORS.get(database)
                plt.plot(
                    sizes, times, marker="o", linewidth=2, label=database, color=color
                )

            plt.title("Average across all index operations")
            plt.xlabel("Data Size")
            plt.ylabel("Avg Time (ms)")
            ax = plt.gca()
            ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
            plt.legend()
            plt.grid(True, linestyle="--", alpha=0.4)
            plt.tight_layout()

            output_file = f"{prefix}index_all_operations_average.png"
            output_path = os.path.join(output_dir, output_file)
            plt.savefig(output_path, dpi=150)
            plt.close()
            generated_files.append(output_path)

        if filtered_overall or filtered_index_overall:
            plt.figure(figsize=(10, 6))

            for database in sorted(filtered_overall.keys()):
                points = sorted(filtered_overall[database], key=lambda x: x[0])
                sizes = [point[0] for point in points]
                times = [point[1] for point in points]
                color = DB_COLORS.get(database)
                plt.plot(
                    sizes,
                    times,
                    marker="o",
                    linewidth=2,
                    label=f"{database}",
                    color=color,
                    linestyle="-",
                )

            for database in sorted(filtered_index_overall.keys()):
                points = sorted(filtered_index_overall[database], key=lambda x: x[0])
                sizes = [point[0] for point in points]
                times = [point[1] for point in points]
                color = DB_COLORS.get(database)
                plt.plot(
                    sizes,
                    times,
                    marker="s",
                    linewidth=2,
                    label=f"{database} (index)",
                    color=color,
                    linestyle="--",
                )

            plt.title("All operations average vs Index operations average")
            plt.xlabel("Data Size")
            plt.ylabel("Avg Time (ms)")
            ax = plt.gca()
            ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
            plt.legend()
            plt.grid(True, linestyle="--", alpha=0.4)
            plt.tight_layout()

            output_file = f"{prefix}combined_all_vs_index_operations_average.png"
            output_path = os.path.join(output_dir, output_file)
            plt.savefig(output_path, dpi=150)
            plt.close()
            generated_files.append(output_path)

        if filtered_boxplot:
            boxplot_data = pd.DataFrame(
                [
                    {"database": db, "operation_type": op_type, "avg_time_ms": avg_time}
                    for db, op_type, avg_time in filtered_boxplot
                ]
            )

            plt.figure(figsize=(11, 6.5))
            sns.boxplot(
                data=boxplot_data,
                x="operation_type",
                y="avg_time_ms",
                hue="database",
                order=["insert", "select", "update", "delete"],
                hue_order=sorted({row[0] for row in filtered_boxplot}),
                palette=DB_COLORS,
                showfliers=False,
            )
            plt.title("Operation Type Distribution by Database")
            plt.xlabel("Operation Type")
            plt.ylabel("Avg Time (ms)")
            plt.grid(True, axis="y", linestyle="--", alpha=0.3)
            plt.tight_layout()

            output_file = f"{prefix}operation_type_boxplot.png"
            output_path = os.path.join(output_dir, output_file)
            plt.savefig(output_path, dpi=150)
            plt.close()
            generated_files.append(output_path)

            boxplot_data_no_unqlite = boxplot_data[
                boxplot_data["database"] != "unqlite"
            ]
            if not boxplot_data_no_unqlite.empty:
                plt.figure(figsize=(11, 6.5))
                sns.boxplot(
                    data=boxplot_data_no_unqlite,
                    x="operation_type",
                    y="avg_time_ms",
                    hue="database",
                    order=["insert", "select", "update", "delete"],
                    hue_order=sorted(set(boxplot_data_no_unqlite["database"])),
                    palette=DB_COLORS,
                    showfliers=False,
                )
                plt.title("Operation Type Distribution by Database (without UnQLite)")
                plt.xlabel("Operation Type")
                plt.ylabel("Avg Time (ms)")
                plt.grid(True, axis="y", linestyle="--", alpha=0.3)
                plt.tight_layout()

                output_file = f"{prefix}operation_type_boxplot_no_unqlite.png"
                output_path = os.path.join(output_dir, output_file)
                plt.savefig(output_path, dpi=150)
                plt.close()
                generated_files.append(output_path)

    return generated_files
