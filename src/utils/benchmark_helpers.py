import time

from src.utils.results import save_explain_result, save_result


def execute_and_time_query(
    execute_func, query_name, db_name, size, trial, fetch_results=True
):
    start = time.time()
    result = execute_func()
    elapsed = (time.time() - start) * 1000

    if fetch_results and hasattr(result, "fetchall"):
        result.fetchall()

    save_result(db_name, query_name, size, elapsed, size, trial=trial)
    return elapsed


def execute_explain_query(execute_func, query_name, db_name, trial):
    start = time.time()
    result = execute_func()
    elapsed = (time.time() - start) * 1000

    plan = result.fetchall() if hasattr(result, "fetchall") else result
    plan_text = "\n".join([str(row) for row in plan])

    save_explain_result(db_name, query_name, plan_text, elapsed, trial=trial)
    return elapsed


def run_benchmark_operations(
    db_name,
    queries_dict,
    size,
    trial,
    execute_query_func,
    extra_setup_func=None,
):
    if extra_setup_func:
        extra_setup_func()

    results = {}
    for name, q in queries_dict.items():
        params = q["params"]()

        start = time.time()
        execute_query_func(q["query"], params, name.startswith("select"))
        elapsed = (time.time() - start) * 1000

        results[name] = elapsed
        save_result(db_name, name, size, elapsed, size, trial=trial)

    return results


BENCHMARK_EMAILS = [
    "single@example.com",
    "ignore@example.com",
    "upsert@example.com",
    "returning@example.com",
    "indexed@example.com",
]

DELETE_TARGET_IDS = [1, 100, 101, 102]
DELETE_TARGET_CATEGORIES = [100, 101]

DATA_REFRESH_THRESHOLD = 0.05
LARGE_SIZE_THRESHOLD = 5_000_000


def needs_starting_data_refresh(benchmark, target_size):
    current_size = benchmark.get_total_record_count()
    if current_size is None:
        print("Current total record count is unknown, refreshing data.")
        return True, True

    need = abs(current_size - target_size) > (target_size * DATA_REFRESH_THRESHOLD)
    if need:
        print(
            f"Current total record count {current_size:_} differs from target {target_size:_} by more than 5%, refreshing data."
        )
        use_populate = target_size < LARGE_SIZE_THRESHOLD
        return True, use_populate

    return False, False
