set -l main_py_cond "string match -q '*main.py*' -- (commandline -opc | string join ' ')"

# --db
complete -c python -n $main_py_cond -l db -r -f -d 'Database to benchmark' \
    -a "postgres\t'PostgreSQL server' sqlite\t'SQLite single-file' mongo\t'MongoDB server' unqlite\t'UnQLite single-file' all\t'All databases'"

# --operation
complete -c python -n $main_py_cond -l operation -r -f -d 'Operation type to run' \
    -a "nonindexed\t'Non-indexed queries' indexed\t'Indexed queries' explain\t'EXPLAIN ANALYZE (SQL only)' json\t'JSON queries (SQL only)' all\t'All operations'"

# --size
complete -c python -n $main_py_cond -l size -r -f -d 'Size of test data' \
    -a "5000\t'5 thousand rows' 500000\t'500 thousand rows' 1000000\t'1 million rows' 10000000\t'10 million rows' all\t'All sizes'"

