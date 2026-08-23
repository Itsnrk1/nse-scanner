# ============================================================
# 15-MINUTE EOD REVERSAL BASE + PREVIOUS-DAY OPENING FILTERS
# ============================================================
#
# BASE STRATEGY
#
# Previous trading day:
#
#   15:00 - 15:14  -> BULL
#   15:15 - 15:29  -> BEAR
#
# Then:
#
#   NEXT DAY 09:15 OPEN  -> SHORT
#   NEXT DAY 15:27 OPEN  -> EXIT
#
# IMPORTANT:
#   The signal uses ONLY the previous trading day.
#
# We then test simple PREVIOUS-DAY OPENING filters against
# this fixed base.
#
# ============================================================

import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import warnings

warnings.filterwarnings("ignore")


# ============================================================
# SETTINGS
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/nse-fno-1min-data/"
    "releases/download/v1.0.0/stocks_1m_csvs.zip"
)

ENTRY_TIME = "09:15"
EXIT_TIME = "15:27"

ROUND_TRIP_COST = 0.10

MIN_TRAIN_TRADES = 100
MIN_VALIDATION_TRADES = 30
MIN_TEST_TRADES = 30

TARGET_WIN_RATE = 85.0

# Number of filters allowed together.
MAX_FILTER_DEPTH = 3

# Candidates retained at each level.
MAX_SURVIVORS = 250


# ============================================================
# THRESHOLDS
# ============================================================

BODY_THRESHOLDS = [
    0.50,
    0.60,
    0.70,
    0.80,
    0.90
]

CLOSE_THRESHOLDS = [
    0.10,
    0.20,
    0.30,
    0.40,
    0.60,
    0.70,
    0.80,
    0.90
]

MOMENTUM_THRESHOLDS = [
    0.05,
    0.10,
    0.15,
    0.20,
    0.30,
    0.50,
    1.00
]


# ============================================================
# TIMEFRAME DEFINITIONS
# ============================================================

# These are previous-day opening structures.
#
# They are available before the next day's 09:15 entry.

OPENING_WINDOWS = {

    "1m": [
        (9 * 60 + 15, 9 * 60 + 15),
        (9 * 60 + 16, 9 * 60 + 16),
        (9 * 60 + 17, 9 * 60 + 17),
        (9 * 60 + 18, 9 * 60 + 18),
        (9 * 60 + 19, 9 * 60 + 19)
    ],

    "2m": [
        (9 * 60 + 15, 9 * 60 + 16),
        (9 * 60 + 17, 9 * 60 + 18),
        (9 * 60 + 19, 9 * 60 + 20),
        (9 * 60 + 21, 9 * 60 + 22)
    ],

    "3m": [
        (9 * 60 + 15, 9 * 60 + 17),
        (9 * 60 + 18, 9 * 60 + 20),
        (9 * 60 + 21, 9 * 60 + 23)
    ],

    "5m": [
        (9 * 60 + 15, 9 * 60 + 19),
        (9 * 60 + 20, 9 * 60 + 24)
    ],

    "10m": [
        (9 * 60 + 15, 9 * 60 + 24)
    ],

    "15m": [
        (9 * 60 + 15, 9 * 60 + 29)
    ]
}


# ============================================================
# HELPERS
# ============================================================

def minute_to_key(total_minutes):

    hour = total_minutes // 60
    minute = total_minutes % 60

    return f"{hour:02d}:{minute:02d}"


def candle_direction(candle):

    if candle["close"] > candle["open"]:
        return "BULL"

    if candle["close"] < candle["open"]:
        return "BEAR"

    return "DOJI"


def candle_metrics(candle):

    candle_range = (
        candle["high"] -
        candle["low"]
    )

    body = abs(
        candle["close"] -
        candle["open"]
    )

    if candle_range > 0:

        body_ratio = (
            body /
            candle_range
        )

        close_position = (
            candle["close"] -
            candle["low"]
        ) / candle_range

    else:

        body_ratio = 0.0
        close_position = 0.5

    if candle["open"] != 0:

        momentum = (
            (
                candle["close"] -
                candle["open"]
            )
            /
            candle["open"]
            *
            100
        )

    else:

        momentum = 0.0

    return {
        "range": candle_range,
        "body": body_ratio,
        "close_pos": close_position,
        "momentum": momentum
    }


def build_candle(
    day,
    start_minute,
    end_minute
):

    rows = []

    for minute in range(
        start_minute,
        end_minute + 1
    ):

        key = minute_to_key(
            minute
        )

        if key not in day:
            return None

        rows.append(
            day[key]
        )

    if len(rows) == 0:
        return None

    return {

        "open":
            rows[0]["open"],

        "high":
            max(
                row["high"]
                for row in rows
            ),

        "low":
            min(
                row["low"]
                for row in rows
            ),

        "close":
            rows[-1]["close"],

        "volume":
            sum(
                row["volume"]
                for row in rows
            )
    }


# ============================================================
# LOAD STOCK
# ============================================================

def load_stock(
    zip_file,
    filename
):

    try:

        raw = zip_file.read(
            filename
        )

        df = pd.read_csv(
            io.BytesIO(raw),
            compression="gzip"
        )

        if df.empty:
            return None

        if "time" not in df.columns:
            return None

        df["datetime"] = (
            pd.to_datetime(
                df["time"],
                unit="s",
                utc=True
            )
            .dt
            .tz_convert(
                "Asia/Kolkata"
            )
        )

        df["date"] = (
            df["datetime"]
            .dt
            .strftime("%Y-%m-%d")
        )

        df["hm"] = (
            df["datetime"]
            .dt
            .strftime("%H:%M")
        )

        for column in [
            "open",
            "high",
            "low",
            "close"
        ]:

            if column not in df.columns:
                return None

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

        if "Volume" in df.columns:

            df["volume"] = pd.to_numeric(
                df["Volume"],
                errors="coerce"
            )

        elif "volume" in df.columns:

            df["volume"] = pd.to_numeric(
                df["volume"],
                errors="coerce"
            )

        else:

            return None

        df = df.dropna(
            subset=[
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]
        )

        df = df[
            (df["hm"] >= "09:15")
            &
            (df["hm"] <= "15:29")
        ].copy()

        return df[
            [
                "date",
                "hm",
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]
        ]

    except Exception:

        return None


# ============================================================
# DAY LOOKUP
# ============================================================

def make_day_lookup(group):

    result = {}

    for _, row in group.iterrows():

        result[row["hm"]] = {

            "open":
                float(row["open"]),

            "high":
                float(row["high"]),

            "low":
                float(row["low"]),

            "close":
                float(row["close"]),

            "volume":
                float(row["volume"])
        }

    return result


# ============================================================
# BUILD PREVIOUS-DAY FEATURES
# ============================================================

def build_features(day):

    # ========================================================
    # 1. EXACT BASE: 15-MINUTE REVERSAL
    # ========================================================

    first_15 = build_candle(
        day,
        15 * 60,
        15 * 60 + 14
    )

    second_15 = build_candle(
        day,
        15 * 60 + 15,
        15 * 60 + 29
    )

    if (
        first_15 is None
        or
        second_15 is None
    ):

        return None

    # Base condition.
    if (
        candle_direction(first_15)
        !=
        "BULL"
    ):

        return None

    if (
        candle_direction(second_15)
        !=
        "BEAR"
    ):

        return None

    m_first = candle_metrics(
        first_15
    )

    m_second = candle_metrics(
        second_15
    )

    features = {}

    # ========================================================
    # BASE CANDLE METRICS
    # ========================================================

    features[
        "BASE_FIRST_BODY"
    ] = m_first["body"]

    features[
        "BASE_SECOND_BODY"
    ] = m_second["body"]

    features[
        "BASE_FIRST_CLOSE_POS"
    ] = m_first["close_pos"]

    features[
        "BASE_SECOND_CLOSE_POS"
    ] = m_second["close_pos"]

    features[
        "BASE_FIRST_RANGE"
    ] = m_first["range"]

    features[
        "BASE_SECOND_RANGE"
    ] = m_second["range"]

    features[
        "BASE_FIRST_MOMENTUM"
    ] = m_first["momentum"]

    features[
        "BASE_SECOND_MOMENTUM"
    ] = m_second["momentum"]

    features[
        "BASE_FIRST_VOLUME"
    ] = first_15["volume"]

    features[
        "BASE_SECOND_VOLUME"
    ] = second_15["volume"]

    # ========================================================
    # BASE RELATIONSHIPS
    # ========================================================

    features[
        "BASE_SECOND_VOLUME_MORE"
    ] = (
        second_15["volume"]
        >
        first_15["volume"]
    )

    features[
        "BASE_FIRST_VOLUME_MORE"
    ] = (
        first_15["volume"]
        >
        second_15["volume"]
    )

    features[
        "BASE_SECOND_BODY_MORE"
    ] = (
        m_second["body"]
        >
        m_first["body"]
    )

    features[
        "BASE_FIRST_BODY_MORE"
    ] = (
        m_first["body"]
        >
        m_second["body"]
    )

    features[
        "BASE_SECOND_RANGE_MORE"
    ] = (
        m_second["range"]
        >
        m_first["range"]
    )

    features[
        "BASE_FIRST_RANGE_MORE"
    ] = (
        m_first["range"]
        >
        m_second["range"]
    )

    # ========================================================
    # 2. PREVIOUS-DAY WHOLE-DAY INFORMATION
    # ========================================================

    if (
        "09:15" not in day
        or
        "15:29" not in day
    ):

        return None

    day_open = day[
        "09:15"
    ]["open"]

    day_close = day[
        "15:29"
    ]["close"]

    if day_open <= 0:
        return None

    day_high = max(
        x["high"]
        for x in day.values()
    )

    day_low = min(
        x["low"]
        for x in day.values()
    )

    day_range = (
        day_high -
        day_low
    )

    day_return = (
        (
            day_close -
            day_open
        )
        /
        day_open
        *
        100
    )

    if day_range > 0:

        day_close_position = (
            day_close -
            day_low
        ) /
        day_range

    else:

        day_close_position = 0.5

    features[
        "DAY_RETURN"
    ] = day_return

    features[
        "DAY_RANGE"
    ] = day_range

    features[
        "DAY_CLOSE_POSITION"
    ] = day_close_position

    # ========================================================
    # 3. PREVIOUS-DAY OPENING CANDLES
    # ========================================================

    for timeframe, windows in (
        OPENING_WINDOWS.items()
    ):

        for index, (
            start,
            end
        ) in enumerate(
            windows,
            1
        ):

            candle = build_candle(
                day,
                start,
                end
            )

            if candle is None:
                continue

            metrics = candle_metrics(
                candle
            )

            prefix = (
                f"{timeframe}_OPEN_{index}"
            )

            # ------------------------------------------------
            # Direction
            # ------------------------------------------------

            features[
                prefix + "_BULL"
            ] = (
                candle_direction(candle)
                ==
                "BULL"
            )

            features[
                prefix + "_BEAR"
            ] = (
                candle_direction(candle)
                ==
                "BEAR"
            )

            # ------------------------------------------------
            # Numerical properties
            # ------------------------------------------------

            features[
                prefix + "_BODY"
            ] = metrics["body"]

            features[
                prefix + "_CLOSE_POS"
            ] = metrics["close_pos"]

            features[
                prefix + "_RANGE"
            ] = metrics["range"]

            features[
                prefix + "_MOMENTUM"
            ] = metrics["momentum"]

            features[
                prefix + "_VOLUME"
            ] = candle["volume"]

    # ========================================================
    # 4. OPENING CANDLE RELATIONSHIPS
    # ========================================================

    for timeframe, windows in (
        OPENING_WINDOWS.items()
    ):

        candles = []

        for index, (
            start,
            end
        ) in enumerate(
            windows,
            1
        ):

            candle = build_candle(
                day,
                start,
                end
            )

            if candle is None:
                continue

            candles.append(
                (
                    index,
                    candle
                )
            )

        for i in range(
            len(candles) - 1
        ):

            index_a, candle_a = (
                candles[i]
            )

            index_b, candle_b = (
                candles[i + 1]
            )

            prefix = (
                f"{timeframe}_"
                f"OPEN_{index_a}_VS_"
                f"{index_b}"
            )

            direction_a = (
                candle_direction(
                    candle_a
                )
            )

            direction_b = (
                candle_direction(
                    candle_b
                )
            )

            # Same trend
            features[
                prefix + "_SAME"
            ] = (
                direction_a ==
                direction_b
                and
                direction_a !=
                "DOJI"
            )

            # Opposite trend
            features[
                prefix + "_OPPOSITE"
            ] = (
                direction_a !=
                direction_b
                and
                direction_a !=
                "DOJI"
                and
                direction_b !=
                "DOJI"
            )

            # Volume comparison
            features[
                prefix + "_SECOND_VOL_MORE"
            ] = (
                candle_b["volume"]
                >
                candle_a["volume"]
            )

            features[
                prefix + "_FIRST_VOL_MORE"
            ] = (
                candle_a["volume"]
                >
                candle_b["volume"]
            )

    return features


# ============================================================
# EXTRACT EVENTS
# ============================================================

def extract_events(
    zip_file,
    filename
):

    df = load_stock(
        zip_file,
        filename
    )

    if df is None:
        return []

    grouped = {
        date: group
        for date, group
        in df.groupby("date")
    }

    dates = sorted(
        grouped.keys()
    )

    if len(dates) < 2:
        return []

    symbol = os.path.basename(
        filename
    )

    symbol = symbol.replace(
        ".csv.gz",
        ""
    )

    symbol = symbol.replace(
        "_1m",
        ""
    )

    events = []

    for i in range(
        1,
        len(dates)
    ):

        previous_date = dates[
            i - 1
        ]

        next_date = dates[
            i
        ]

        previous_day = make_day_lookup(
            grouped[
                previous_date
            ]
        )

        next_day = make_day_lookup(
            grouped[
                next_date
            ]
        )

        # ----------------------------------------------------
        # NEXT DAY ENTRY / EXIT
        # ----------------------------------------------------

        if ENTRY_TIME not in next_day:
            continue

        if EXIT_TIME not in next_day:
            continue

        # ----------------------------------------------------
        # PREVIOUS DAY SIGNAL
        # ----------------------------------------------------

        features = build_features(
            previous_day
        )

        if features is None:
            continue

        entry_price = next_day[
            ENTRY_TIME
        ]["open"]

        exit_price = next_day[
            EXIT_TIME
        ]["open"]

        if entry_price <= 0:
            continue

        # ----------------------------------------------------
        # SHORT RETURN
        # ----------------------------------------------------

        short_return = (
            (
                entry_price -
                exit_price
            )
            /
            entry_price
            *
            100
        )

        events.append({

            "symbol":
                symbol,

            "signal_date":
                pd.Timestamp(
                    previous_date
                ),

            "event_date":
                pd.Timestamp(
                    next_date
                ),

            "RETURN":
                short_return,

            **features
        })

    return events


# ============================================================
# DOWNLOAD
# ============================================================

def download_data():

    print("=" * 100)
    print("DOWNLOADING DATA")
    print("=" * 100)

    response = requests.get(
        DATA_URL,
        timeout=900
    )

    response.raise_for_status()

    print(
        f"Downloaded "
        f"{len(response.content) / 1024 / 1024:.1f} MB"
    )

    return response.content


# ============================================================
# CREATE DATASET
# ============================================================

def create_dataset():

    raw = download_data()

    zip_file = zipfile.ZipFile(
        io.BytesIO(raw)
    )

    files = [
        f
        for f in zip_file.namelist()
        if f.endswith(".csv.gz")
    ]

    print(
        f"Stocks found: "
        f"{len(files):,}"
    )

    events = []

    for number, filename in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing "
            f"{number:,}/{len(files):,}",
            end=""
        )

        try:

            events.extend(
                extract_events(
                    zip_file,
                    filename
                )
            )

        except Exception:

            continue

    print()

    df = pd.DataFrame(
        events
    )

    if df.empty:

        raise RuntimeError(
            "No base events found."
        )

    df = df.sort_values(
        "event_date"
    ).reset_index(
        drop=True
    )

    return df


# ============================================================
# SPLIT
# ============================================================

def chronological_split(df):

    dates = sorted(
        df[
            "event_date"
        ]
        .dt
        .normalize()
        .unique()
    )

    n = len(dates)

    train_end = dates[
        int(n * 0.60)
    ]

    validation_end = dates[
        int(n * 0.80)
    ]

    train = df[
        df["event_date"].dt.normalize()
        <
        train_end
    ].copy()

    validation = df[
        (
            df["event_date"].dt.normalize()
            >=
            train_end
        )
        &
        (
            df["event_date"].dt.normalize()
            <
            validation_end
        )
    ].copy()

    test = df[
        df["event_date"].dt.normalize()
        >=
        validation_end
    ].copy()

    return (
        train,
        validation,
        test,
        train_end,
        validation_end
    )


# ============================================================
# STATISTICS
# ============================================================

def calculate_stats(returns):

    returns = pd.Series(
        returns
    ).dropna()

    if len(returns) == 0:
        return None

    net = (
        returns -
        ROUND_TRIP_COST
    )

    wins = (
        net > 0
    ).sum()

    win_rate = (
        wins /
        len(net)
        *
        100
    )

    gross_profit = (
        net[net > 0].sum()
    )

    gross_loss = abs(
        net[net < 0].sum()
    )

    if gross_loss > 0:

        profit_factor = (
            gross_profit /
            gross_loss
        )

    else:

        profit_factor = np.inf

    return {

        "trades":
            len(net),

        "win_rate":
            win_rate,

        "average":
            net.mean(),

        "profit_factor":
            profit_factor,

        "total":
            net.sum()
    }


# ============================================================
# CONDITION GENERATION
# ============================================================

def generate_conditions(df):

    conditions = {}

    # ========================================================
    # BOOLEAN FEATURES
    # ========================================================

    for column in df.columns:

        if column in [
            "symbol",
            "signal_date",
            "event_date",
            "RETURN"
        ]:
            continue

        # IMPORTANT:
        # Handle bool BEFORE numeric.
        #
        # This fixes the pandas quantile error.
        # ====================================================

        if pd.api.types.is_bool_dtype(
            df[column]
        ):

            conditions[
                column
            ] = (
                df[column]
                .fillna(False)
                .astype(bool)
            )

    # ========================================================
    # NUMERIC FEATURES
    # ========================================================

    for column in df.columns:

        if column in [
            "symbol",
            "signal_date",
            "event_date",
            "RETURN"
        ]:
            continue

        # Never process booleans as numeric.
        if pd.api.types.is_bool_dtype(
            df[column]
        ):
            continue

        if not pd.api.types.is_numeric_dtype(
            df[column]
        ):
            continue

        series = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        if series.notna().sum() < 100:
            continue

        # ----------------------------------------------------
        # Quantiles
        # ----------------------------------------------------

        try:

            quantiles = (
                series
                .dropna()
                .quantile([
                    0.10,
                    0.20,
                    0.30,
                    0.50,
                    0.70,
                    0.80,
                    0.90
                ])
                .dropna()
                .unique()
            )

        except Exception:

            continue

        for value in quantiles:

            if not np.isfinite(
                value
            ):
                continue

            lower_name = (
                f"{column}<=Q{value:.8g}"
            )

            upper_name = (
                f"{column}>=Q{value:.8g}"
            )

            conditions[
                lower_name
            ] = (
                series <= value
            )

            conditions[
                upper_name
            ] = (
                series >= value
            )

    # ========================================================
    # REMOVE DUPLICATE MASKS
    # ========================================================

    unique = {}

    for name, mask in conditions.items():

        mask = (
            pd.Series(
                mask,
                index=df.index
            )
            .fillna(False)
            .astype(bool)
        )

        signature = np.packbits(
            mask.to_numpy(
                dtype=np.uint8
            )
        ).tobytes()

        if signature not in unique:

            unique[
                signature
            ] = (
                name,
                mask
            )

    return {
        name: mask
        for name, mask
        in unique.values()
    }


# ============================================================
# BASE PERFORMANCE
# ============================================================

def print_base_results(
    train,
    validation,
    test
):

    print()
    print("=" * 100)
    print("UNFILTERED 15-MINUTE BASE PERFORMANCE")
    print("=" * 100)

    for name, data in [
        ("TRAIN", train),
        ("VALIDATION", validation),
        ("FINAL TEST", test)
    ]:

        result = calculate_stats(
            data["RETURN"]
        )

        print()
        print(name)

        print(
            f"Trades        : "
            f"{result['trades']:,}"
        )

        print(
            f"Win rate      : "
            f"{result['win_rate']:.2f}%"
        )

        print(
            f"Average       : "
            f"{result['average']:.4f}%"
        )

        print(
            f"Profit factor : "
            f"{result['profit_factor']:.3f}"
        )

        print(
            f"Total return  : "
            f"{result['total']:.4f}%"
        )


# ============================================================
# CANDIDATE SCORE
# ============================================================

def candidate_score(result):

    pf = result[
        "win_rate"
    ]

    profit_factor = result[
        "profit_factor"
    ]

    if not np.isfinite(
        profit_factor
    ):

        profit_factor = 10

    return (
        pf
        +
        8 *
        np.log1p(
            max(
                profit_factor,
                0
            )
        )
        +
        5 *
        result[
            "average"
        ]
    )


# ============================================================
# SEARCH DEPTH 1
# ============================================================

def search_depth_1(
    train,
    conditions
):

    results = []

    for name, mask in conditions.items():

        subset = train[
            mask
        ]

        if (
            len(subset)
            <
            MIN_TRAIN_TRADES
        ):
            continue

        result = calculate_stats(
            subset["RETURN"]
        )

        if result is None:
            continue

        results.append({

            "pattern":
                name,

            "_mask":
                mask,

            **result
        })

    if not results:
        return []

    results.sort(
        key=candidate_score,
        reverse=True
    )

    return results[
        :MAX_SURVIVORS
    ]


# ============================================================
# SEARCH COMBINATIONS
# ============================================================

def search_depth(
    train,
    survivors,
    conditions,
    depth
):

    results = []

    checked = 0

    condition_items = list(
        conditions.items()
    )

    for candidate in survivors:

        existing = set(
            candidate[
                "pattern"
            ].split(
                " + "
            )
        )

        for name, mask in condition_items:

            if name in existing:
                continue

            # Prevent duplicate orderings.
            if name <= max(existing):
                continue

            checked += 1

            combined_mask = (
                candidate[
                    "_mask"
                ]
                &
                mask
            )

            count = int(
                combined_mask.sum()
            )

            if count < MIN_TRAIN_TRADES:
                continue

            result = calculate_stats(
                train.loc[
                    combined_mask,
                    "RETURN"
                ]
            )

            if result is None:
                continue

            pattern = " + ".join(
                sorted(
                    existing |
                    {name}
                )
            )

            results.append({

                "pattern":
                    pattern,

                "_mask":
                    combined_mask,

                **result
            })

    print(
        f"Depth {depth}: "
        f"{checked:,} combinations checked"
    )

    if not results:
        return []

    # ========================================================
    # Remove duplicate masks
    # ========================================================

    unique = {}

    for result in results:

        signature = np.packbits(
            result[
                "_mask"
            ].to_numpy(
                dtype=np.uint8
            )
        ).tobytes()

        if signature not in unique:

            unique[
                signature
            ] = result

    results = list(
        unique.values()
    )

    results.sort(
        key=candidate_score,
        reverse=True
    )

    print(
        f"Depth {depth}: "
        f"{len(results):,} unique candidates"
    )

    return results[
        :MAX_SURVIVORS
    ]


# ============================================================
# APPLY PATTERN TO NEW DATA
# ============================================================

def apply_pattern(
    df,
    pattern
):

    conditions = generate_conditions(
        df
    )

    parts = [
        x.strip()
        for x
        in pattern.split(
            " + "
        )
    ]

    mask = pd.Series(
        True,
        index=df.index
    )

    for part in parts:

        if part not in conditions:

            return None

        mask &= (
            conditions[
                part
            ]
        )

    return mask


# ============================================================
# VALIDATION
# ============================================================

def validate_candidates(
    candidates,
    validation
):

    results = []

    for candidate in candidates:

        mask = apply_pattern(
            validation,
            candidate[
                "pattern"
            ]
        )

        if mask is None:
            continue

        subset = validation[
            mask
        ]

        if (
            len(subset)
            <
            MIN_VALIDATION_TRADES
        ):
            continue

        result = calculate_stats(
            subset["RETURN"]
        )

        if result is None:
            continue

        results.append({

            "pattern":
                candidate["pattern"],

            "train_trades":
                candidate["trades"],

            "train_win":
                candidate["win_rate"],

            "train_average":
                candidate["average"],

            "train_pf":
                candidate["profit_factor"],

            "validation_trades":
                result["trades"],

            "validation_win":
                result["win_rate"],

            "validation_average":
                result["average"],

            "validation_pf":
                result["profit_factor"]
        })

    if not results:
        return pd.DataFrame()

    result_df = pd.DataFrame(
        results
    )

    result_df = (
        result_df
        .sort_values(
            [
                "validation_win",
                "validation_pf",
                "validation_average"
            ],
            ascending=False
        )
        .head(
            MAX_SURVIVORS
        )
        .reset_index(
            drop=True
        )
    )

    return result_df


# ============================================================
# FINAL TEST
# ============================================================

def test_candidates(
    validation_results,
    test
):

    results = []

    for _, candidate in (
        validation_results.iterrows()
    ):

        mask = apply_pattern(
            test,
            candidate[
                "pattern"
            ]
        )

        if mask is None:
            continue

        subset = test[
            mask
        ]

        if (
            len(subset)
            <
            MIN_TEST_TRADES
        ):
            continue

        result = calculate_stats(
            subset["RETURN"]
        )

        if result is None:
            continue

        results.append({

            "pattern":
                candidate["pattern"],

            "train_trades":
                candidate["train_trades"],

            "train_win":
                candidate["train_win"],

            "train_pf":
                candidate["train_pf"],

            "validation_trades":
                candidate[
                    "validation_trades"
                ],

            "validation_win":
                candidate[
                    "validation_win"
                ],

            "validation_pf":
                candidate[
                    "validation_pf"
                ],

            "test_trades":
                result["trades"],

            "test_win":
                result["win_rate"],

            "test_average":
                result["average"],

            "test_pf":
                result["profit_factor"],

            "test_total":
                result["total"]
        })

    if not results:
        return pd.DataFrame()

    return (
        pd.DataFrame(
            results
        )
        .sort_values(
            [
                "test_win",
                "test_pf",
                "test_average"
            ],
            ascending=False
        )
        .reset_index(
            drop=True
        )
    )


# ============================================================
# YEARLY TEST
# ============================================================

def yearly_test(
    test,
    pattern
):

    mask = apply_pattern(
        test,
        pattern
    )

    if mask is None:
        return pd.DataFrame()

    subset = test[
        mask
    ].copy()

    if subset.empty:
        return pd.DataFrame()

    subset["YEAR"] = (
        subset[
            "event_date"
        ]
        .dt
        .year
    )

    rows = []

    for year, group in (
        subset.groupby(
            "YEAR"
        )
    ):

        result = calculate_stats(
            group["RETURN"]
        )

        if result is None:
            continue

        rows.append({

            "year":
                year,

            "trades":
                result["trades"],

            "win_rate":
                result["win_rate"],

            "average":
                result["average"],

            "profit_factor":
                result["profit_factor"],

            "total":
                result["total"]
        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 100)
    print(
        "15-MINUTE EOD REVERSAL + OPENING-CANDLE FILTER SEARCH"
    )
    print("=" * 100)

    print()
    print(
        "BASE:"
    )

    print(
        "Previous day 15:00-15:14 = BULL"
    )

    print(
        "Previous day 15:15-15:29 = BEAR"
    )

    print(
        "Direction = SHORT"
    )

    print(
        "Entry = NEXT DAY 09:15 OPEN"
    )

    print(
        "Exit = NEXT DAY 15:27 OPEN"
    )

    print()
    print(
        "No next-day opening information is used."
    )

    # ========================================================
    # DATA
    # ========================================================

    df = create_dataset()

    print()
    print(
        f"TOTAL BASE EVENTS: "
        f"{len(df):,}"
    )

    # ========================================================
    # SPLIT
    # ========================================================

    (
        train,
        validation,
        test,
        train_end,
        validation_end
    ) = chronological_split(
        df
    )

    print()
    print("=" * 100)
    print("DATA SPLIT")
    print("=" * 100)

    print(
        f"TRAIN       : "
        f"{len(train):,}"
    )

    print(
        f"VALIDATION  : "
        f"{len(validation):,}"
    )

    print(
        f"FINAL TEST  : "
        f"{len(test):,}"
    )

    print()
    print(
        f"TRAIN ENDS: "
        f"{train_end}"
    )

    print(
        f"VALIDATION ENDS: "
        f"{validation_end}"
    )

    # ========================================================
    # BASE PERFORMANCE
    # ========================================================

    print_base_results(
        train,
        validation,
        test
    )

    # ========================================================
    # CONDITIONS
    # ========================================================

    print()
    print("=" * 100)
    print("GENERATING FILTER CONDITIONS")
    print("=" * 100)

    conditions = generate_conditions(
        train
    )

    print(
        f"Unique conditions: "
        f"{len(conditions):,}"
    )

    # ========================================================
    # DEPTH 1
    # ========================================================

    survivors = search_depth_1(
        train,
        conditions
    )

    print(
        f"Depth 1 survivors: "
        f"{len(survivors):,}"
    )

    # ========================================================
    # DEPTH 2 / 3
    # ========================================================

    for depth in range(
        2,
        MAX_FILTER_DEPTH + 1
    ):

        if not survivors:
            break

        survivors = search_depth(
            train,
            survivors,
            conditions,
            depth
        )

        print(
            f"Depth {depth} survivors: "
            f"{len(survivors):,}"
        )

    # ========================================================
    # TRAINING OUTPUT
    # ========================================================

    if not survivors:

        print(
            "\nNo filter candidates survived."
        )

        return

    training_output = pd.DataFrame([
        {
            key: value
            for key, value
            in candidate.items()
            if key != "_mask"
        }
        for candidate
        in survivors
    ])

    training_output.to_csv(
        "BASE_OPEN_FILTER_TRAINING.csv",
        index=False
    )

    print()
    print("=" * 100)
    print("TOP TRAINING FILTERS")
    print("=" * 100)

    print(
        training_output
        .head(30)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # VALIDATION
    # ========================================================

    print()
    print("=" * 100)
    print("VALIDATION")
    print("=" * 100)

    validation_results = (
        validate_candidates(
            survivors,
            validation
        )
    )

    if validation_results.empty:

        print(
            "NO PATTERN SURVIVED VALIDATION."
        )

        return

    validation_results.to_csv(
        "BASE_OPEN_FILTER_VALIDATION.csv",
        index=False
    )

    print(
        validation_results
        .head(30)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # FINAL TEST
    # ========================================================

    print()
    print("=" * 100)
    print("FINAL UNSEEN TEST")
    print("=" * 100)

    final_results = test_candidates(
        validation_results,
        test
    )

    if final_results.empty:

        print(
            "NO PATTERN SURVIVED FINAL TEST."
        )

        return

    final_results.to_csv(
        "BASE_OPEN_FILTER_FINAL_TEST.csv",
        index=False
    )

    print(
        final_results
        .head(50)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # 85% TARGET
    # ========================================================

    print()
    print("=" * 100)
    print("FINAL TEST >= 85%")
    print("=" * 100)

    over_85 = final_results[
        final_results[
            "test_win"
        ] >= TARGET_WIN_RATE
    ]

    if over_85.empty:

        print("NONE")

    else:

        print(
            over_85.to_string(
                index=False
            )
        )

    # ========================================================
    # BEST RESULT
    # ========================================================

    best = final_results.iloc[0]

    print()
    print("=" * 100)
    print("BEST FINAL TEST RESULT")
    print("=" * 100)

    print(
        f"Pattern:\n"
        f"{best['pattern']}"
    )

    print(
        f"\nTrades: "
        f"{int(best['test_trades']):,}"
    )

    print(
        f"Win rate: "
        f"{best['test_win']:.2f}%"
    )

    print(
        f"Average: "
        f"{best['test_average']:.4f}%"
    )

    print(
        f"Profit factor: "
        f"{best['test_pf']:.3f}"
    )

    print(
        f"Total: "
        f"{best['test_total']:.4f}%"
    )

    # ========================================================
    # YEARLY
    # ========================================================

    print()
    print("=" * 100)
    print("YEAR-BY-YEAR PERFORMANCE")
    print("=" * 100)

    yearly = yearly_test(
        test,
        best["pattern"]
    )

    if yearly.empty:

        print(
            "No yearly results."
        )

    else:

        print(
            yearly.to_string(
                index=False
            )
        )

    # ========================================================
    # FILES
    # ========================================================

    print()
    print("=" * 100)
    print("FILES CREATED")
    print("=" * 100)

    print(
        "BASE_OPEN_FILTER_TRAINING.csv"
    )

    print(
        "BASE_OPEN_FILTER_VALIDATION.csv"
    )

    print(
        "BASE_OPEN_FILTER_FINAL_TEST.csv"
    )

    print()
    print("=" * 100)
    print("SEARCH COMPLETE")
    print("=" * 100)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
