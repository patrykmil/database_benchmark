set -l main_py_cond "string match -q '*main.py*' -- (commandline -opc | string join ' ')"

# --db
complete -c python -n $main_py_cond -l db -r -f -d 'Database to benchmark' \
    -a "postgres\t'PostgreSQL server' sqlite\t'SQLite single-file' mongo\t'MongoDB server' unqlite\t'UnQLite single-file' all\t'All databases'"

# --operation
complete -c python -n $main_py_cond -l operation -r -f -d 'Operation type to run' \
    -a "nonindexed\t'Non-indexed queries' indexed\t'Indexed queries' explain\t'EXPLAIN ANALYZE (SQL only)' json\t'JSON queries (SQL only)' all\t'All operations'"

# --size
complete -c python -n $main_py_cond -l size -r -f -d 'Size of test data' \
    -a "5k\t'5 thousand rows' 500k\t'500 thousand rows' 1m\t'1 million rows' 10m\t'10 million rows' 25m\t'25 million rows' 50m\t'50 million rows' standard\t'Standard benchmark sizes (500k, 1m, 10m)' huge\t'Huge benchmark sizes (25m, 50m)' all\t'All sizes'"

# --trials
complete -c python -n $main_py_cond -l trials -r -f -d 'Number of trials to run' \
    -a "1\t'1 trial (default)' 2\t'2 trials' 3\t'3 trials'"

# --draw
complete -c python -n $main_py_cond -l draw -f -d 'Draw diagrams from benchmark summary'

# --analyze
complete -c python -n $main_py_cond -l analyze -f -d 'Generate analysis from benchmark summary'
