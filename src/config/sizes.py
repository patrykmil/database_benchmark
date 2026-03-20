STANDARD_SIZES = [500_000, 1_000_000, 10_000_000]
HUGE_SIZES = [25_000_000, 50_000_000]
ALL_SIZES = STANDARD_SIZES + HUGE_SIZES

SIZE_ALIASES = [
    (5_000, ["5000", "5k"]),
    (500_000, ["500000", "500k"]),
    (1_000_000, ["1000000", "1m"]),
    (5_000_000, ["5000000", "5m"]),
    (10_000_000, ["10000000", "10m"]),
    (25_000_000, ["25000000", "25m"]),
    (50_000_000, ["50000000", "50m"]),
    ("standard", ["standard"]),
    ("huge", ["huge"]),
    ("all", ["all"]),
]

SIZES_MAP = {alias: size for size, aliases in SIZE_ALIASES for alias in aliases}


def get_sizes(size_arg):
    if size_arg == "standard":
        return STANDARD_SIZES
    if size_arg == "huge":
        return HUGE_SIZES
    if size_arg == "all":
        return ALL_SIZES
    if size_arg in SIZES_MAP:
        return [SIZES_MAP[size_arg]]
    return None
