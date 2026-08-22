import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import warnings

warnings.filterwarnings("ignore")


# ============================================================
# ROBUSTNESS TEST OF THE BEST DISCOVERED EOD PATTERN
# ============================================================
#
# Signal:
#   PREVIOUS TRADING DAY
#
# Entry:
#   NEXT DAY 09:15 OPEN
#
# Exit:
#   NEXT DAY 15:27 OPEN
#
# Main pattern family:
#
#   15m CLOSE POSITION
#   3m CLOSE POSITION
#   3m RANGE RATIO
#   5m PREVIOUS CANDLE TREND
#
# We deliberately test nearby thresholds rather than only
# the exact pattern discovered previously.
#
# ============================================================


# ============================================================
# SETTINGS
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/nse-fno-1min-data/"
    "releases/download/v1.0.0/stocks_1m_csvs.zip"
)

ENTRY_TIME = "09:15"
EXIT_TIME = "15:27"

# Estimated total round-trip cost.
#
# This is deliberately configurable.
#
# Example:
# 0.10 means 0.10 percentage points total.
#
TOTAL_COST_PCT = 0.10

# Minimum number of trades required for a pattern
MIN_TRAIN = 100
MIN_VALIDATION = 30
MIN_TEST = 30


# ============================================================
# TRAIN / VALIDATION / TEST
# ============================================================

TRAIN_PERCENT = 0.60
VALIDATION_PERCENT = 0.20
TEST_PERCENT = 0.20


# ============================================================
# THRESHOLDS TO TEST
# ============================================================
#
# Original discovered pattern:
#
# 15m close position <= 0.30
# 3m close position >= 0.70
# 3m range ratio <= 0.75
# 5m previous candle = BEAR
#
# We test nearby values to determine whether the edge is
# broad or exists only at one exact threshold.
# ============================================================


CLOSE_15M_LEVELS = [
    0.10,
    0.15,
    0.20,
    0.25,
    0.30,
    0.35,
    0.40,
    0.45
]

CLOSE_3M_LEVELS = [
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90
]

RANGE_3M_LEVELS = [
    0.50,
    0.60,
    0.70,
    0.75,
    0.80,
    0.90,
    1.00,
    1.10,
    1.25
]


# ============================================================
# OPTIONAL 5m CONTEXT
# ============================================================
#
# We test several forms of 5m context:
#
# PREVIOUS_BEAR
# PREVIOUS_BULL
# LAST_BEAR
# LAST_BULL
#
# The original pattern uses PREVIOUS_BEAR.
# ============================================================

FIVE_MIN_CONTEXTS = [
    "PREVIOUS_BEAR",
    "PREVIOUS_BULL",
    "LAST_BEAR",
    "LAST_BULL"
]


# ============================================================
# DOWNLOAD
# ============================================================

def download_dataset():

    print("=" * 90)
    print("DOWNLOADING NSE 1-MINUTE DATA")
    print("=" * 90)

    response = requests.get(
        DATA_URL,
        stream=True,
        timeout=600
    )

    response.raise_for_status()

    chunks = []

    total = int(
        response.headers.get(
            "content-length",
            0
        )
    )

    downloaded = 0

    for chunk in response.iter_content(
        chunk_size=1024 * 1024
    ):

        if not chunk:
            continue

        chunks.append(chunk)

        downloaded += len(chunk)

        if total:

            print(
                f"\rDownloaded "
                f"{downloaded / total * 100:.1f}%",
                end=""
            )

    print()

    return b"".join(chunks)


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

        required = [
            "open",
            "high",
            "low",
            "close"
        ]

        for column in required:

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

def make_day_lookup(
    group
):

    result = {}

    for _, row in group.iterrows():

        result[
            row["hm"]
        ] = {
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
# BUILD MULTI-MINUTE CANDLE
# ============================================================

def build_candle(
    day,
    start_minute,
    minutes
):

    rows = []

    for i in range(
        minutes
    ):

        minute = (
            start_minute +
            i
        )

        key = (
            f"15:{minute:02d}"
        )

        if key not in day:

            return None

        rows.append(
            day[key]
        )

    if not rows:

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
# CANDLE METRICS
# ============================================================

def candle_metrics(
    candle
):

    candle_range = (
        candle["high"]
        -
        candle["low"]
    )

    body = abs(
        candle["close"]
        -
        candle["open"]
    )

    if candle_range > 0:

        close_position = (
            candle["close"]
            -
            candle["low"]
        ) / candle_range

    else:

        close_position = 0.5

    return {

        "range":
            candle_range,

        "body":
            body,

        "close_position":
            close_position
    }


# ============================================================
# DIRECTION
# ============================================================

def direction(
    candle
):

    if candle["close"] > candle["open"]:

        return "BULL"

    if candle["close"] < candle["open"]:

        return "BEAR"

    return "DOJI"


# ============================================================
# PREVIOUS-DAY EOD FEATURES
# ============================================================

def build_features(
    day
):

    # --------------------------------------------------------
    # 15-minute
    #
    # 15:00 -> 15:15
    # --------------------------------------------------------

    candle_15_prev = build_candle(
        day,
        0,
        15
    )

    candle_15_last = build_candle(
        day,
        15,
        15
    )

    # --------------------------------------------------------
    # 3-minute
    #
    # 15:21 -> 15:24
    # 15:24 -> 15:27
    #
    # Last completed EOD candle = 15:24
    # Previous = 15:21
    # --------------------------------------------------------

    candle_3_previous = build_candle(
        day,
        21,
        3
    )

    candle_3_last = build_candle(
        day,
        24,
        3
    )

    # --------------------------------------------------------
    # 5-minute
    #
    # 15:20 -> 15:25
    # 15:25 -> 15:30
    #
    # For the discovered condition, we use the candle
    # immediately preceding the final 5m candle.
    # --------------------------------------------------------

    candle_5_previous = build_candle(
        day,
        20,
        5
    )

    candle_5_last = build_candle(
        day,
        25,
        5
    )

    required = [
        candle_15_last,
        candle_3_last,
        candle_5_previous,
        candle_5_last
    ]

    if any(
        candle is None
        for candle in required
    ):

        return None

    # --------------------------------------------------------
    # Metrics
    # --------------------------------------------------------

    m15 = candle_metrics(
        candle_15_last
    )

    m3 = candle_metrics(
        candle_3_last
    )

    m3_previous = candle_metrics(
        candle_3_previous
    )

    # --------------------------------------------------------
    # 3m average range
    #
    # Use previous completed 3m candles before 15:24.
    # --------------------------------------------------------

    historical_ranges = []

    for start in [
        0,
        3,
        6,
        9,
        12,
        15,
        18,
        21
    ]:

        candle = build_candle(
            day,
            start,
            3
        )

        if candle is not None:

            historical_ranges.append(
                candle_metrics(
                    candle
                )["range"]
            )

    if len(
        historical_ranges
    ) >= 2:

        # Exclude the last candle itself
        avg_range = np.mean(
            historical_ranges[:-1]
        )

    else:

        avg_range = 0

    if avg_range > 0:

        range_ratio = (
            m3["range"]
            /
            avg_range
        )

    else:

        range_ratio = 0

    # --------------------------------------------------------
    # Features
    # --------------------------------------------------------

    return {

        "15m_close_position":
            m15["close_position"],

        "3m_close_position":
            m3["close_position"],

        "3m_range_ratio":
            range_ratio,

        "5m_previous_direction":
            direction(
                candle_5_previous
            ),

        "5m_last_direction":
            direction(
                candle_5_last
            ),

        "3m_last_direction":
            direction(
                candle_3_last
            ),

        "3m_previous_direction":
            direction(
                candle_3_previous
            )
    }


# ============================================================
# BUILD EVENTS
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
        in df.groupby(
            "date"
        )
    }

    dates = sorted(
        grouped.keys()
    )

    symbol = os.path.basename(
        filename
    )

    symbol = symbol.replace(
        "_1m.csv.gz",
        ""
    )

    results = []

    for i in range(
        1,
        len(dates)
    ):

        signal_date = dates[
            i - 1
        ]

        event_date = dates[
            i
        ]

        previous_day = (
            make_day_lookup(
                grouped[
                    signal_date
                ]
            )
        )

        event_day = (
            make_day_lookup(
                grouped[
                    event_date
                ]
            )
        )

        if ENTRY_TIME not in event_day:

            continue

        if EXIT_TIME not in event_day:

            continue

        features = build_features(
            previous_day
        )

        if features is None:

            continue

        entry = event_day[
            ENTRY_TIME
        ]["open"]

        exit_price = event_day[
            EXIT_TIME
        ]["open"]

        if entry <= 0:

            continue

        raw_return = (
            (
                exit_price
                -
                entry
            )
            /
            entry
            *
            100
        )

        results.append({

            "symbol":
                symbol,

            "signal_date":
                pd.Timestamp(
                    signal_date
                ),

            "event_date":
                pd.Timestamp(
                    event_date
                ),

            "raw_return":
                raw_return,

            "long_return":
                raw_return,

            "short_return":
                -raw_return,

            **features
        })

    return results


# ============================================================
# LOAD ENTIRE DATASET
# ============================================================

def create_dataset():

    data = download_dataset()

    zip_file = zipfile.ZipFile(
        io.BytesIO(data)
    )

    files = [
        f
        for f
        in zip_file.namelist()
        if f.endswith(
            ".csv.gz"
        )
    ]

    print()
    print(
        f"Stocks found: "
        f"{len(files):,}"
    )

    all_events = []

    for number, filename in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing "
            f"{number}/{len(files)}",
            end=""
        )

        events = extract_events(
            zip_file,
            filename
        )

        all_events.extend(
            events
        )

    print()

    df = pd.DataFrame(
        all_events
    )

    if df.empty:

        raise RuntimeError(
            "No valid observations found."
        )

    df = df.sort_values(
        "event_date"
    ).reset_index(
        drop=True
    )

    return df


# ============================================================
# CHRONOLOGICAL SPLIT
# ============================================================

def chronological_split(
    df
):

    dates = sorted(
        df[
            "event_date"
        ].dt.normalize()
        .unique()
    )

    n = len(dates)

    train_end_index = int(
        n * TRAIN_PERCENT
    )

    validation_end_index = int(
        n *
        (
            TRAIN_PERCENT
            +
            VALIDATION_PERCENT
        )
    )

    train_end = dates[
        train_end_index
    ]

    validation_end = dates[
        validation_end_index
    ]

    train = df[
        df[
            "event_date"
        ].dt.normalize()
        < train_end
    ].copy()

    validation = df[
        (
            df[
                "event_date"
            ].dt.normalize()
            >= train_end
        )
        &
        (
            df[
                "event_date"
            ].dt.normalize()
            < validation_end
        )
    ].copy()

    test = df[
        df[
            "event_date"
        ].dt.normalize()
        >= validation_end
    ].copy()

    return (
        train,
        validation,
        test,
        train_end,
        validation_end
    )


# ============================================================
# PATTERN MASK
# ============================================================

def get_pattern_mask(
    df,
    close15,
    close3,
    range3,
    context
):

    mask = (
        df[
            "15m_close_position"
        ]
        <= close15
    )

    mask &= (
        df[
            "3m_close_position"
        ]
        >= close3
    )

    mask &= (
        df[
            "3m_range_ratio"
        ]
        <= range3
    )

    if context == "PREVIOUS_BEAR":

        mask &= (
            df[
                "5m_previous_direction"
            ]
            == "BEAR"
        )

    elif context == "PREVIOUS_BULL":

        mask &= (
            df[
                "5m_previous_direction"
            ]
            == "BULL"
        )

    elif context == "LAST_BEAR":

        mask &= (
            df[
                "5m_last_direction"
            ]
            == "BEAR"
        )

    elif context == "LAST_BULL":

        mask &= (
            df[
                "5m_last_direction"
            ]
            == "BULL"
        )

    return mask.astype(bool)


# ============================================================
# STATS
# ============================================================

def calculate_stats(
    returns
):

    returns = pd.Series(
        returns
    ).dropna()

    if len(returns) == 0:

        return None

    wins = (
        returns > 0
    ).sum()

    win_rate = (
        wins
        /
        len(returns)
        *
        100
    )

    gross_profit = (
        returns[
            returns > 0
        ].sum()
    )

    gross_loss = abs(
        returns[
            returns < 0
        ].sum()
    )

    if gross_loss > 0:

        pf = (
            gross_profit
            /
            gross_loss
        )

    else:

        pf = np.inf

    return {

        "trades":
            len(returns),

        "win_rate":
            win_rate,

        "average":
            returns.mean(),

        "profit_factor":
            pf,

        "total":
            returns.sum()
    }


# ============================================================
# APPLY COST
# ============================================================

def apply_cost(
    returns
):

    return (
        returns
        -
        TOTAL_COST_PCT
    )


# ============================================================
# MAX LOSING STREAK
# ============================================================

def max_losing_streak(
    returns
):

    losses = (
        pd.Series(
            returns
        )
        <= 0
    )

    maximum = 0
    current = 0

    for value in losses:

        if value:

            current += 1

            maximum = max(
                maximum,
                current
            )

        else:

            current = 0

    return maximum


# ============================================================
# EVALUATE ONE PATTERN
# ============================================================

def evaluate_pattern(
    df,
    mask,
    direction_name
):

    subset = df[
        mask
    ].copy()

    if subset.empty:

        return None

    if direction_name == "SHORT":

        raw_returns = (
            subset[
                "short_return"
            ]
        )

    else:

        raw_returns = (
            subset[
                "long_return"
            ]
        )

    net_returns = apply_cost(
        raw_returns
    )

    stats = calculate_stats(
        net_returns
    )

    if stats is None:

        return None

    stats[
        "max_losing_streak"
    ] = max_losing_streak(
        net_returns
    )

    return stats


# ============================================================
# TEST ALL PATTERNS
# ============================================================

def scan_patterns(
    train,
    validation,
    test
):

    results = []

    total = (
        len(CLOSE_15M_LEVELS)
        *
        len(CLOSE_3M_LEVELS)
        *
        len(RANGE_3M_LEVELS)
        *
        len(FIVE_MIN_CONTEXTS)
    )

    print()
    print(
        f"Patterns to test: "
        f"{total:,}"
    )

    counter = 0

    for close15 in (
        CLOSE_15M_LEVELS
    ):

        for close3 in (
            CLOSE_3M_LEVELS
        ):

            for range3 in (
                RANGE_3M_LEVELS
            ):

                for context in (
                    FIVE_MIN_CONTEXTS
                ):

                    counter += 1

                    if counter % 100 == 0:

                        print(
                            f"\rTesting "
                            f"{counter:,}/"
                            f"{total:,}",
                            end=""
                        )

                    train_mask = (
                        get_pattern_mask(
                            train,
                            close15,
                            close3,
                            range3,
                            context
                        )
                    )

                    validation_mask = (
                        get_pattern_mask(
                            validation,
                            close15,
                            close3,
                            range3,
                            context
                        )
                    )

                    test_mask = (
                        get_pattern_mask(
                            test,
                            close15,
                            close3,
                            range3,
                            context
                        )
                    )

                    # ----------------------------------------
                    # Determine direction using TRAIN ONLY
                    # ----------------------------------------

                    train_long = (
                        train[
                            train_mask
                        ][
                            "long_return"
                        ]
                    )

                    train_short = (
                        train[
                            train_mask
                        ][
                            "short_return"
                        ]
                    )

                    if len(
                        train_long
                    ) < MIN_TRAIN:

                        continue

                    long_net = apply_cost(
                        train_long
                    )

                    short_net = apply_cost(
                        train_short
                    )

                    long_stats = (
                        calculate_stats(
                            long_net
                        )
                    )

                    short_stats = (
                        calculate_stats(
                            short_net
                        )
                    )

                    if (
                        long_stats[
                            "win_rate"
                        ]
                        >=
                        short_stats[
                            "win_rate"
                        ]
                    ):

                        direction = "LONG"

                    else:

                        direction = "SHORT"

                    # ----------------------------------------
                    # Training
                    # ----------------------------------------

                    train_returns = (
                        train[
                            train_mask
                        ][
                            (
                                "long_return"
                                if
                                direction
                                == "LONG"
                                else
                                "short_return"
                            )
                        ]
                    )

                    train_stats = (
                        evaluate_pattern(
                            train,
                            train_mask,
                            direction
                        )
                    )

                    if (
                        train_stats is None
                        or
                        train_stats[
                            "trades"
                        ]
                        < MIN_TRAIN
                    ):

                        continue

                    # ----------------------------------------
                    # Validation
                    # ----------------------------------------

                    validation_subset = (
                        validation[
                            validation_mask
                        ]
                    )

                    if len(
                        validation_subset
                    ) < MIN_VALIDATION:

                        continue

                    validation_stats = (
                        evaluate_pattern(
                            validation,
                            validation_mask,
                            direction
                        )
                    )

                    if validation_stats is None:

                        continue

                    # ----------------------------------------
                    # Final TEST
                    # ----------------------------------------

                    test_subset = (
                        test[
                            test_mask
                        ]
                    )

                    if len(
                        test_subset
                    ) < MIN_TEST:

                        continue

                    test_stats = (
                        evaluate_pattern(
                            test,
                            test_mask,
                            direction
                        )
                    )

                    if test_stats is None:

                        continue

                    # ----------------------------------------
                    # Pattern name
                    # ----------------------------------------

                    pattern = (
                        f"15m_CLOSE<={close15:.2f}"
                        f" + "
                        f"3m_CLOSE>={close3:.2f}"
                        f" + "
                        f"3m_RANGE<={range3:.2f}x"
                        f" + "
                        f"5m_{context}"
                    )

                    results.append({

                        "pattern":
                            pattern,

                        "15m_close_threshold":
                            close15,

                        "3m_close_threshold":
                            close3,

                        "3m_range_threshold":
                            range3,

                        "5m_context":
                            context,

                        "direction":
                            direction,

                        # Training
                        "train_trades":
                            train_stats[
                                "trades"
                            ],

                        "train_win":
                            train_stats[
                                "win_rate"
                            ],

                        "train_avg":
                            train_stats[
                                "average"
                            ],

                        "train_pf":
                            train_stats[
                                "profit_factor"
                            ],

                        "train_streak":
                            train_stats[
                                "max_losing_streak"
                            ],

                        # Validation
                        "validation_trades":
                            validation_stats[
                                "trades"
                            ],

                        "validation_win":
                            validation_stats[
                                "win_rate"
                            ],

                        "validation_avg":
                            validation_stats[
                                "average"
                            ],

                        "validation_pf":
                            validation_stats[
                                "profit_factor"
                            ],

                        "validation_streak":
                            validation_stats[
                                "max_losing_streak"
                            ],

                        # Final test
                        "test_trades":
                            test_stats[
                                "trades"
                            ],

                        "test_win":
                            test_stats[
                                "win_rate"
                            ],

                        "test_avg":
                            test_stats[
                                "average"
                            ],

                        "test_pf":
                            test_stats[
                                "profit_factor"
                            ],

                        "test_streak":
                            test_stats[
                                "max_losing_streak"
                            ],

                        "test_total":
                            test_stats[
                                "total"
                            ]
                    })

    print()

    return pd.DataFrame(
        results
    )


# ============================================================
# YEAR-BY-YEAR TEST
# ============================================================

def year_analysis(
    test,
    row
):

    mask = get_pattern_mask(
        test,
        row[
            "15m_close_threshold"
        ],
        row[
            "3m_close_threshold"
        ],
        row[
            "3m_range_threshold"
        ],
        row[
            "5m_context"
        ]
    )

    subset = test[
        mask
    ].copy()

    if subset.empty:

        return pd.DataFrame()

    subset["year"] = (
        subset[
            "event_date"
        ].dt.year
    )

    rows = []

    for year, group in (
        subset.groupby(
            "year"
        )
    ):

        if row[
            "direction"
        ] == "SHORT":

            returns = (
                group[
                    "short_return"
                ]
            )

        else:

            returns = (
                group[
                    "long_return"
                ]
            )

        returns = apply_cost(
            returns
        )

        stats = calculate_stats(
            returns
        )

        rows.append({

            "year":
                year,

            "trades":
                stats[
                    "trades"
                ],

            "win_rate":
                stats[
                    "win_rate"
                ],

            "average":
                stats[
                    "average"
                ],

            "profit_factor":
                stats[
                    "profit_factor"
                ],

            "total":
                stats[
                    "total"
                ]
        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# ROBUSTNESS SUMMARY
# ============================================================

def robustness_score(
    row
):

    # We don't optimize only for win rate.
    #
    # A pattern must have:
    #   - good training
    #   - good validation
    #   - good test
    #   - adequate sample size
    #   - good PF
    #
    # The score is only for ranking.
    # It is NOT a probability.

    return (
        0.20 *
        row["train_win"]
        +
        0.30 *
        row["validation_win"]
        +
        0.50 *
        row["test_win"]
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 90)
    print(
        "ROBUSTNESS TEST — 15m / 3m / 5m EDGE"
    )
    print("=" * 90)

    print()
    print(
        "Entry : NEXT DAY 09:15 OPEN"
    )

    print(
        "Exit  : NEXT DAY 15:27 OPEN"
    )

    print(
        f"Cost  : {TOTAL_COST_PCT:.2f}% round trip"
    )

    print()
    print(
        "Original candidate:"
    )

    print(
        "15m CLOSE_POS <= 0.30"
    )

    print(
        "3m CLOSE_POS >= 0.70"
    )

    print(
        "3m RANGE_RATIO <= 0.75"
    )

    print(
        "5m PREVIOUS_BEAR"
    )

    # ========================================================
    # DATA
    # ========================================================

    df = create_dataset()

    print()
    print(
        f"Total observations: "
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
    print(
        "=" * 90
    )

    print(
        f"TRAIN: "
        f"{len(train):,}"
    )

    print(
        f"VALIDATION: "
        f"{len(validation):,}"
    )

    print(
        f"FINAL TEST: "
        f"{len(test):,}"
    )

    print()

    print(
        "Training ends:"
    )

    print(
        train_end
    )

    print()

    print(
        "Validation ends:"
    )

    print(
        validation_end
    )

    # ========================================================
    # SCAN
    # ========================================================

    results = scan_patterns(
        train,
        validation,
        test
    )

    if results.empty:

        print()
        print(
            "No qualifying patterns found."
        )

        return

    # ========================================================
    # ROBUSTNESS SCORE
    # ========================================================

    results[
        "robustness_score"
    ] = results.apply(
        robustness_score,
        axis=1
    )

    # ========================================================
    # SAVE EVERYTHING
    # ========================================================

    results = results.sort_values(
        [
            "robustness_score",
            "test_pf",
            "test_win"
        ],
        ascending=False
    ).reset_index(
        drop=True
    )

    results.to_csv(
        "ROBUSTNESS_ALL_PATTERNS.csv",
        index=False
    )

    # ========================================================
    # TOP PATTERNS
    # ========================================================

    print()
    print("=" * 90)
    print(
        "TOP ROBUSTNESS CANDIDATES"
    )
    print("=" * 90)

    columns = [
        "pattern",
        "direction",

        "train_trades",
        "train_win",
        "train_pf",

        "validation_trades",
        "validation_win",
        "validation_pf",

        "test_trades",
        "test_win",
        "test_pf",

        "test_avg",
        "test_streak"
    ]

    print(
        results[
            columns
        ]
        .head(30)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # ORIGINAL PATTERN
    # ========================================================

    original = results[
        (
            results[
                "15m_close_threshold"
            ]
            == 0.30
        )
        &
        (
            results[
                "3m_close_threshold"
            ]
            == 0.70
        )
        &
        (
            results[
                "3m_range_threshold"
            ]
            == 0.75
        )
        &
        (
            results[
                "5m_context"
            ]
            == "PREVIOUS_BEAR"
        )
    ]

    print()
    print("=" * 90)
    print(
        "ORIGINAL 73.44% CANDIDATE"
    )
    print("=" * 90)

    if original.empty:

        print(
            "Original candidate not found."
        )

    else:

        print(
            original[
                columns
            ].to_string(
                index=False
            )
        )

    # ========================================================
    # 70%+ TEST
    # ========================================================

    strong_70 = results[
        (
            results[
                "test_win"
            ]
            >= 70
        )
    ]

    print()
    print("=" * 90)
    print(
        "PATTERNS WITH >= 70% FINAL TEST WIN RATE"
    )
    print("=" * 90)

    if strong_70.empty:

        print(
            "NONE"
        )

    else:

        print(
            strong_70[
                columns
            ]
            .head(50)
            .to_string(
                index=False
            )
        )

    # ========================================================
    # 75%+ TEST
    # ========================================================

    strong_75 = results[
        (
            results[
                "test_win"
            ]
            >= 75
        )
    ]

    print()
    print("=" * 90)
    print(
        "PATTERNS WITH >= 75% FINAL TEST WIN RATE"
    )
    print("=" * 90)

    if strong_75.empty:

        print(
            "NONE"
        )

    else:

        print(
            strong_75[
                columns
            ]
            .head(50)
            .to_string(
                index=False
            )
        )

    # ========================================================
    # 80%+ TEST
    # ========================================================

    strong_80 = results[
        (
            results[
                "test_win"
            ]
            >= 80
        )
    ]

    print()
    print("=" * 90)
    print(
        "PATTERNS WITH >= 80% FINAL TEST WIN RATE"
    )
    print("=" * 90)

    if strong_80.empty:

        print(
            "NONE"
        )

    else:

        print(
            strong_80[
                columns
            ]
            .head(50)
            .to_string(
                index=False
            )
        )

    # ========================================================
    # 85%+ TEST
    # ========================================================

    strong_85 = results[
        (
            results[
                "test_win"
            ]
            >= 85
        )
    ]

    print()
    print("=" * 90)
    print(
        "PATTERNS WITH >= 85% FINAL TEST WIN RATE"
    )
    print("=" * 90)

    if strong_85.empty:

        print(
            "NONE"
        )

    else:

        print(
            strong_85[
                columns
            ]
            .to_string(
                index=False
            )
        )

    # ========================================================
    # YEAR ANALYSIS
    # ========================================================

    print()
    print("=" * 90)
    print(
        "YEAR-BY-YEAR TEST STABILITY"
    )
    print("=" * 90)

    top_patterns = results.head(
        10
    )

    for _, row in (
        top_patterns.iterrows()
    ):

        print()
        print(
            row["pattern"]
        )

        yearly = year_analysis(
            test,
            row
        )

        if yearly.empty:

            print(
                "No yearly data."
            )

        else:

            print(
                yearly.to_string(
                    index=False
                )
            )

    # ========================================================
    # THRESHOLD STABILITY
    # ========================================================

    print()
    print("=" * 90)
    print(
        "THRESHOLD STABILITY AROUND ORIGINAL PATTERN"
    )
    print("=" * 90)

    nearby = results[
        (
            results[
                "15m_close_threshold"
            ].between(
                0.20,
                0.40
            )
        )
        &
        (
            results[
                "3m_close_threshold"
            ].between(
                0.60,
                0.80
            )
        )
        &
        (
            results[
                "3m_range_threshold"
            ].between(
                0.60,
                1.00
            )
        )
    ]

    if nearby.empty:

        print(
            "No nearby candidates."
        )

    else:

        print(
            nearby[
                columns
            ]
            .head(50)
            .to_string(
                index=False
            )
        )

    # ========================================================
    # FINISH
    # ========================================================

    print()
    print("=" * 90)
    print(
        "ROBUSTNESS TEST COMPLETE"
    )
    print("=" * 90)

    print()
    print(
        "Results saved to:"
    )

    print(
        "ROBUSTNESS_ALL_PATTERNS.csv"
    )

    print()
    print(
        "IMPORTANT:"
    )

    print(
        "The FINAL TEST was not used to "
        "select the direction or thresholds."
    )

    print(
        "Costs were subtracted before "
        "calculating the reported net statistics."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
