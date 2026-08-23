# ============================================================
# EXHAUSTIVE PREVIOUS-DAY OPENING + EOD STRATEGY SEARCH
# ============================================================
#
# FIXED TRADING RULES
#
# SIGNAL:
#     Previous trading day ONLY
#
# ENTRY:
#     Next trading day 09:15 OPEN
#
# EXIT:
#     Next trading day 15:27 OPEN
#
# GOAL:
#     Find >= 85% FINAL TEST win rate
#     while maximizing return / profit factor.
#
# TIMEFRAMES:
#     1m, 2m, 3m, 5m, 10m, 15m
#
# SEARCH:
#     Previous-day opening structures
#     Previous-day EOD structures
#     Cross-timeframe combinations
#     LONG + SHORT
#
# NO NEXT-DAY GAP INFORMATION IS USED.
#
# ============================================================

import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import itertools
import warnings
import math
from collections import defaultdict

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

TARGET_WIN_RATE = 85.0

ROUND_TRIP_COST = 0.10

MIN_TRAIN_TRADES = 75
MIN_VALIDATION_TRADES = 25
MIN_TEST_TRADES = 25

# Number of strongest individual conditions carried
# into combination searches.
#
# This is deliberately large enough to search broadly,
# while preventing impossible billions-of-combinations
# searches.
MAX_ATOMIC_CANDIDATES = 500

# Maximum number of candidates carried to the next depth.
MAX_SURVIVORS = 500

# Search up to this depth.
MAX_DEPTH = 4

# Minimum training win rate to be considered for
# combination expansion.
MIN_TRAIN_WIN_FOR_EXPANSION = 52.0


# ============================================================
# TIMEFRAMES
# ============================================================

TIMEFRAMES = {
    "1m": 1,
    "2m": 2,
    "3m": 3,
    "5m": 5,
    "10m": 10,
    "15m": 15
}


# ============================================================
# OPENING WINDOWS
# ============================================================
#
# These are PREVIOUS-DAY opening structures.
#
# We deliberately use windows beginning at 09:15.
#
# Example:
#
# 1m:
#     09:15
#     09:16
#     09:17
#     ...
#
# 3m:
#     09:15-09:17
#     09:18-09:20
#     ...
#
# The exact minute alignment is kept consistent with
# standard NSE intraday candle construction.
# ============================================================

OPENING_WINDOW_ENDS = {
    "1m": [15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
    "2m": [17, 19, 21, 23, 25, 27, 29],
    "3m": [17, 20, 23, 26, 29],
    "5m": [19, 24, 29],
    "10m": [24, 29],
    "15m": [29]
}


# ============================================================
# EOD WINDOWS
# ============================================================
#
# We use completed candles ending before/around the close.
# For each timeframe the scanner examines multiple EOD
# structures instead of only the final candle.
# ============================================================

EOD_STARTS = {
    "1m": [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
    "2m": [19, 21, 23, 25, 27],
    "3m": [15, 18, 21, 24, 27],
    "5m": [10, 15, 20, 25],
    "10m": [0, 10, 20],
    "15m": [0, 15]
}


# ============================================================
# BASIC THRESHOLDS
# ============================================================

BODY_LEVELS = [
    0.50,
    0.60,
    0.70,
    0.80,
    0.90
]

CLOSE_LEVELS = [
    0.10,
    0.20,
    0.30,
    0.40,
    0.60,
    0.70,
    0.80,
    0.90
]

REL_VOL_LEVELS = [
    0.50,
    0.75,
    1.00,
    1.25,
    1.50,
    2.00,
    2.50,
    3.00
]

RANGE_RATIO_LEVELS = [
    0.50,
    0.75,
    1.00,
    1.25,
    1.50,
    2.00,
    2.50,
    3.00
]

MOMENTUM_LEVELS = [
    0.05,
    0.10,
    0.15,
    0.20,
    0.30,
    0.50,
    1.00
]


# ============================================================
# DOWNLOAD DATA
# ============================================================

def download_dataset():

    print("=" * 100)
    print("DOWNLOADING NSE DATA")
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
# LOAD ONE STOCK
# ============================================================

def load_stock(zip_file, filename):

    try:

        raw = zip_file.read(filename)

        df = pd.read_csv(
            io.BytesIO(raw),
            compression="gzip"
        )

        if df.empty:
            return None

        if "time" not in df.columns:
            return None

        # ----------------------------------------------------
        # Timestamp
        # ----------------------------------------------------

        df["datetime"] = (
            pd.to_datetime(
                df["time"],
                unit="s",
                utc=True
            )
            .dt
            .tz_convert("Asia/Kolkata")
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

        # ----------------------------------------------------
        # OHLC
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Volume
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Only NSE regular session
        # ----------------------------------------------------

        df = df[
            (
                df["hm"] >= "09:15"
            )
            &
            (
                df["hm"] <= "15:29"
            )
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
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"])
        }

    return result


# ============================================================
# MINUTE KEY
# ============================================================

def minute_key(total_minutes):

    hour = total_minutes // 60
    minute = total_minutes % 60

    return f"{hour:02d}:{minute:02d}"


# ============================================================
# BUILD CANDLE FROM START + END
# ============================================================

def build_candle(
    day,
    start_total,
    end_total
):

    rows = []

    for t in range(
        start_total,
        end_total + 1
    ):

        key = minute_key(t)

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
                x["high"]
                for x in rows
            ),

        "low":
            min(
                x["low"]
                for x in rows
            ),

        "close":
            rows[-1]["close"],

        "volume":
            sum(
                x["volume"]
                for x in rows
            )
    }


# ============================================================
# CANDLE METRICS
# ============================================================

def candle_metrics(candle):

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

        body_ratio = (
            body /
            candle_range
        )

        close_position = (
            candle["close"]
            -
            candle["low"]
        ) / candle_range

    else:

        body_ratio = 0.0
        close_position = 0.5

    if candle["open"] != 0:

        momentum = (
            (
                candle["close"]
                -
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


# ============================================================
# DIRECTION
# ============================================================

def candle_direction(candle):

    if candle["close"] > candle["open"]:
        return "BULL"

    if candle["close"] < candle["open"]:
        return "BEAR"

    return "DOJI"


# ============================================================
# BUILD TIMEFRAME STRUCTURES
# ============================================================

def build_timeframe_structures(
    day,
    timeframe
):

    minutes = TIMEFRAMES[timeframe]

    structures = {}

    # --------------------------------------------------------
    # OPENING STRUCTURES
    # --------------------------------------------------------

    for end_minute in (
        OPENING_WINDOW_ENDS[
            timeframe
        ]
    ):

        start_total = (
            9 * 60 + 15
        )

        end_total = (
            9 * 60 +
            end_minute
        )

        candle = build_candle(
            day,
            start_total,
            end_total
        )

        if candle is None:
            continue

        structures[
            f"OPEN_{end_minute}"
        ] = candle

    # --------------------------------------------------------
    # EOD STRUCTURES
    # --------------------------------------------------------

    for start_minute in (
        EOD_STARTS[
            timeframe
        ]
    ):

        # Special handling for 10m / 15m,
        # where the starts are expressed relative
        # to the 15:xx portion.
        #
        # For starts <= 29 we interpret them as
        # 15:start_minute.
        #
        # 10m starts [0,10,20] means:
        # 15:00-15:09, etc.
        #
        # 15m starts [0,15] means:
        # 15:00-15:14 and 15:15-15:29.

        start_total = (
            15 * 60 +
            start_minute
        )

        end_total = (
            start_total +
            minutes -
            1
        )

        # Do not go past 15:29.
        if end_total > (
            15 * 60 + 29
        ):
            continue

        candle = build_candle(
            day,
            start_total,
            end_total
        )

        if candle is None:
            continue

        structures[
            f"EOD_{start_minute}"
        ] = candle

    return structures


# ============================================================
# BUILD FEATURES FOR ONE TIMEFRAME
# ============================================================

def timeframe_features(
    day,
    timeframe
):

    structures = build_timeframe_structures(
        day,
        timeframe
    )

    if not structures:
        return {}

    features = {}

    # ========================================================
    # OPENING STRUCTURES
    # ========================================================

    opening_keys = sorted([
        k for k in structures
        if k.startswith("OPEN_")
    ])

    for key in opening_keys:

        candle = structures[key]

        metrics = candle_metrics(
            candle
        )

        prefix = (
            f"{timeframe}:{key}"
        )

        features[
            f"{prefix}:BULL"
        ] = (
            candle_direction(
                candle
            ) == "BULL"
        )

        features[
            f"{prefix}:BEAR"
        ] = (
            candle_direction(
                candle
            ) == "BEAR"
        )

        features[
            f"{prefix}:BODY"
        ] = metrics[
            "body"
        ]

        features[
            f"{prefix}:CLOSE_POS"
        ] = metrics[
            "close_pos"
        ]

        features[
            f"{prefix}:MOMENTUM"
        ] = metrics[
            "momentum"
        ]

        features[
            f"{prefix}:RANGE"
        ] = metrics[
            "range"
        ]

        features[
            f"{prefix}:VOLUME"
        ] = candle[
            "volume"
        ]

    # ========================================================
    # EOD STRUCTURES
    # ========================================================

    eod_keys = sorted([
        k for k in structures
        if k.startswith("EOD_")
    ])

    for key in eod_keys:

        candle = structures[key]

        metrics = candle_metrics(
            candle
        )

        prefix = (
            f"{timeframe}:{key}"
        )

        features[
            f"{prefix}:BULL"
        ] = (
            candle_direction(
                candle
            ) == "BULL"
        )

        features[
            f"{prefix}:BEAR"
        ] = (
            candle_direction(
                candle
            ) == "BEAR"
        )

        features[
            f"{prefix}:BODY"
        ] = metrics[
            "body"
        ]

        features[
            f"{prefix}:CLOSE_POS"
        ] = metrics[
            "close_pos"
        ]

        features[
            f"{prefix}:MOMENTUM"
        ] = metrics[
            "momentum"
        ]

        features[
            f"{prefix}:RANGE"
        ] = metrics[
            "range"
        ]

        features[
            f"{prefix}:VOLUME"
        ] = candle[
            "volume"
        ]

    # ========================================================
    # OPENING CANDLE RELATIONSHIPS
    # ========================================================

    for a, b in itertools.combinations(
        opening_keys,
        2
    ):

        candle_a = structures[a]
        candle_b = structures[b]

        dir_a = candle_direction(
            candle_a
        )

        dir_b = candle_direction(
            candle_b
        )

        prefix = (
            f"{timeframe}:{a}_VS_{b}"
        )

        if dir_a == dir_b and dir_a != "DOJI":

            features[
                f"{prefix}:SAME_DIRECTION"
            ] = True

        else:

            features[
                f"{prefix}:SAME_DIRECTION"
            ] = False

        if (
            dir_a != dir_b
            and
            dir_a != "DOJI"
            and
            dir_b != "DOJI"
        ):

            features[
                f"{prefix}:OPPOSITE_DIRECTION"
            ] = True

        else:

            features[
                f"{prefix}:OPPOSITE_DIRECTION"
            ] = False

        if candle_a["volume"] > candle_b["volume"]:

            features[
                f"{prefix}:A_VOLUME_MORE"
            ] = True

        else:

            features[
                f"{prefix}:A_VOLUME_MORE"
            ] = False

        if candle_b["volume"] > candle_a["volume"]:

            features[
                f"{prefix}:B_VOLUME_MORE"
            ] = True

        else:

            features[
                f"{prefix}:B_VOLUME_MORE"
            ] = False

    # ========================================================
    # EOD CANDLE RELATIONSHIPS
    # ========================================================

    for a, b in itertools.combinations(
        eod_keys,
        2
    ):

        candle_a = structures[a]
        candle_b = structures[b]

        dir_a = candle_direction(
            candle_a
        )

        dir_b = candle_direction(
            candle_b
        )

        prefix = (
            f"{timeframe}:{a}_VS_{b}"
        )

        features[
            f"{prefix}:SAME_DIRECTION"
        ] = (
            dir_a == dir_b
            and
            dir_a != "DOJI"
        )

        features[
            f"{prefix}:OPPOSITE_DIRECTION"
        ] = (
            dir_a != dir_b
            and
            dir_a != "DOJI"
            and
            dir_b != "DOJI"
        )

        features[
            f"{prefix}:A_VOLUME_MORE"
        ] = (
            candle_a["volume"]
            >
            candle_b["volume"]
        )

        features[
            f"{prefix}:B_VOLUME_MORE"
        ] = (
            candle_b["volume"]
            >
            candle_a["volume"]
        )

    # ========================================================
    # OPENING VS EOD
    # ========================================================

    for opening_key in opening_keys:

        for eod_key in eod_keys:

            opening = structures[
                opening_key
            ]

            eod = structures[
                eod_key
            ]

            opening_dir = candle_direction(
                opening
            )

            eod_dir = candle_direction(
                eod
            )

            prefix = (
                f"{timeframe}:"
                f"{opening_key}_VS_"
                f"{eod_key}"
            )

            features[
                f"{prefix}:SAME_DIRECTION"
            ] = (
                opening_dir ==
                eod_dir
                and
                opening_dir != "DOJI"
            )

            features[
                f"{prefix}:OPPOSITE_DIRECTION"
            ] = (
                opening_dir !=
                eod_dir
                and
                opening_dir != "DOJI"
                and
                eod_dir != "DOJI"
            )

            features[
                f"{prefix}:OPEN_VOLUME_MORE"
            ] = (
                opening["volume"]
                >
                eod["volume"]
            )

            features[
                f"{prefix}:EOD_VOLUME_MORE"
            ] = (
                eod["volume"]
                >
                opening["volume"]
            )

    return features


# ============================================================
# BUILD COMPLETE PREVIOUS-DAY FEATURE SET
# ============================================================

def build_features(day):

    features = {}

    for timeframe in TIMEFRAMES:

        tf_features = timeframe_features(
            day,
            timeframe
        )

        features.update(
            tf_features
        )

    # ========================================================
    # WHOLE-DAY FEATURES
    # ========================================================

    if (
        "09:15" in day
        and
        "15:29" in day
    ):

        day_open = day[
            "09:15"
        ]["open"]

        day_close = day[
            "15:29"
        ]["close"]

        if day_open > 0:

            day_return = (
                (
                    day_close
                    -
                    day_open
                )
                /
                day_open
                *
                100
            )

        else:

            day_return = 0

        features[
            "DAY:RETURN"
        ] = day_return

        # ----------------------------------------------------
        # Whole day high / low
        # ----------------------------------------------------

        highs = [
            x["high"]
            for x in day.values()
        ]

        lows = [
            x["low"]
            for x in day.values()
        ]

        day_high = max(highs)
        day_low = min(lows)

        day_range = (
            day_high -
            day_low
        )

        if day_range > 0:

            features[
                "DAY:CLOSE_POSITION"
            ] = (
                (
                    day_close
                    -
                    day_low
                )
                /
                day_range
            )

        else:

            features[
                "DAY:CLOSE_POSITION"
            ] = 0.5

        features[
            "DAY:RANGE"
        ] = day_range

    return features


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
        # FIXED NEXT-DAY ENTRY
        # ----------------------------------------------------

        if ENTRY_TIME not in next_day:
            continue

        # ----------------------------------------------------
        # FIXED NEXT-DAY EXIT
        # ----------------------------------------------------

        if EXIT_TIME not in next_day:
            continue

        # ----------------------------------------------------
        # BUILD SIGNAL FROM PREVIOUS DAY ONLY
        # ----------------------------------------------------

        features = build_features(
            previous_day
        )

        if not features:
            continue

        entry = next_day[
            ENTRY_TIME
        ]["open"]

        exit_price = next_day[
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

            "LONG_RETURN":
                raw_return,

            "SHORT_RETURN":
                -raw_return,

            **features
        })

    return events


# ============================================================
# CREATE DATASET
# ============================================================

def create_dataset():

    raw = download_dataset()

    zip_file = zipfile.ZipFile(
        io.BytesIO(raw)
    )

    files = [
        x
        for x in zip_file.namelist()
        if x.endswith(
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
            f"{number:,}/{len(files):,}",
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
            "No events were created."
        )

    df = df.sort_values(
        "event_date"
    ).reset_index(
        drop=True
    )

    return df


# ============================================================
# TRAIN / VALIDATION / TEST
# ============================================================

def chronological_split(df):

    unique_dates = sorted(
        df[
            "event_date"
        ]
        .dt
        .normalize()
        .unique()
    )

    n = len(unique_dates)

    train_end_index = int(
        n * 0.60
    )

    validation_end_index = int(
        n * 0.80
    )

    train_end = unique_dates[
        train_end_index
    ]

    validation_end = unique_dates[
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
# CONDITION GENERATION
# ============================================================

def generate_conditions(df):

    conditions = {}

    # ========================================================
    # BOOLEAN CONDITIONS
    # ========================================================

    for column in df.columns:

        if (
            column in [
                "symbol",
                "signal_date",
                "event_date",
                "LONG_RETURN",
                "SHORT_RETURN"
            ]
        ):
            continue

        if (
            df[column].dtype
            == bool
        ):

            conditions[
                column
            ] = (
                df[column]
                .fillna(False)
                .astype(bool)
            )

    # ========================================================
    # NUMERIC CONDITIONS
    # ========================================================

    numeric_columns = []

    for column in df.columns:

        if (
            column in [
                "LONG_RETURN",
                "SHORT_RETURN",
                "symbol",
                "signal_date",
                "event_date"
            ]
        ):
            continue

        if pd.api.types.is_numeric_dtype(
            df[column]
        ):

            numeric_columns.append(
                column
            )

    for column in numeric_columns:

        series = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        # ----------------------------------------------------
        # Only create threshold conditions for meaningful
        # numeric features.
        # ----------------------------------------------------

        if series.notna().sum() < 100:
            continue

        # Quantile thresholds prevent arbitrary overfitting.
        quantiles = [
            0.05,
            0.10,
            0.20,
            0.30,
            0.50,
            0.70,
            0.80,
            0.90,
            0.95
        ]

        values = (
            series
            .dropna()
            .quantile(
                quantiles
            )
            .unique()
        )

        for value in values:

            if not np.isfinite(value):
                continue

            conditions[
                f"{column}<=Q{value:.6g}"
            ] = (
                series <= value
            )

            conditions[
                f"{column}>=Q{value:.6g}"
            ] = (
                series >= value
            )

    # ========================================================
    # REMOVE DUPLICATE BOOLEAN MASKS
    # ========================================================

    unique_conditions = {}

    seen = set()

    for name, mask in conditions.items():

        mask = (
            pd.Series(
                mask,
                index=df.index
            )
            .fillna(False)
            .astype(bool)
        )

        # Pack boolean mask into bytes.
        signature = np.packbits(
            mask.to_numpy(
                dtype=np.uint8
            )
        ).tobytes()

        if signature in seen:
            continue

        seen.add(
            signature
        )

        unique_conditions[
            name
        ] = mask

    return unique_conditions


# ============================================================
# STATISTICS
# ============================================================

def stats(returns):

    returns = pd.Series(
        returns
    ).dropna()

    if len(returns) == 0:
        return None

    net = (
        returns
        -
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
# DIRECTION EVALUATION
# ============================================================

def evaluate_mask(
    df,
    mask
):

    subset = df[
        mask
    ]

    if len(subset) < MIN_TRAIN_TRADES:
        return None

    long_stats = stats(
        subset[
            "LONG_RETURN"
        ]
    )

    short_stats = stats(
        subset[
            "SHORT_RETURN"
        ]
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
        selected = long_stats

    else:

        direction = "SHORT"
        selected = short_stats

    return {

        "direction":
            direction,

        "trades":
            selected["trades"],

        "win_rate":
            selected["win_rate"],

        "average":
            selected["average"],

        "profit_factor":
            selected["profit_factor"],

        "total":
            selected["total"]
    }


# ============================================================
# CANDIDATE SCORING
# ============================================================

def candidate_score(row):

    pf = row["profit_factor"]

    if not np.isfinite(pf):
        pf = 10

    # Win rate dominates.
    # PF and average return reward profitable setups.
    # Trade count provides a mild stability preference.

    return (
        row["win_rate"]
        +
        10 *
        np.log1p(
            max(pf, 0)
        )
        +
        2 *
        np.log1p(
            max(
                row["trades"],
                1
            )
        )
        +
        5 *
        max(
            row["average"],
            -1
        )
    )


# ============================================================
# SEARCH DEPTH 1
# ============================================================

def search_depth_1(
    df,
    conditions
):

    print()
    print("=" * 100)
    print("SEARCHING DEPTH 1")
    print("=" * 100)

    rows = []

    for name, mask in conditions.items():

        result = evaluate_mask(
            df,
            mask
        )

        if result is None:
            continue

        if (
            result["win_rate"]
            <
            MIN_TRAIN_WIN_FOR_EXPANSION
        ):
            continue

        rows.append({

            "depth": 1,
            "pattern": name,
            "_mask": mask,
            **result
        })

    if not rows:
        return []

    rows.sort(
        key=candidate_score,
        reverse=True
    )

    return rows[
        :MAX_ATOMIC_CANDIDATES
    ]


# ============================================================
# COMBINATION SEARCH
# ============================================================

def search_next_depth(
    df,
    previous_survivors,
    all_conditions,
    depth
):

    print()
    print("=" * 100)
    print(
        f"SEARCHING DEPTH {depth}"
    )
    print("=" * 100)

    results = []

    checked = 0

    # --------------------------------------------------------
    # Candidate conditions are kept separate so the same
    # atomic condition cannot appear twice.
    # --------------------------------------------------------

    condition_items = list(
        all_conditions.items()
    )

    for survivor in previous_survivors:

        previous_parts = set(
            survivor[
                "pattern"
            ].split(
                " + "
            )
        )

        for condition_name, condition_mask in condition_items:

            if (
                condition_name
                in previous_parts
            ):
                continue

            # Avoid adding conditions that are
            # mathematically identical to an existing
            # condition.
            #
            # The signature is calculated directly.

            checked += 1

            combined_mask = (
                survivor[
                    "_mask"
                ]
                &
                condition_mask
            )

            result = evaluate_mask(
                df,
                combined_mask
            )

            if result is None:
                continue

            if (
                result["win_rate"]
                <
                MIN_TRAIN_WIN_FOR_EXPANSION
            ):
                continue

            pattern_parts = (
                previous_parts
                |
                {condition_name}
            )

            # Canonical ordering prevents:
            #
            # A + B
            # B + A
            #
            # from being counted twice.

            pattern = " + ".join(
                sorted(
                    pattern_parts
                )
            )

            results.append({

                "depth":
                    depth,

                "pattern":
                    pattern,

                "_mask":
                    combined_mask,

                **result
            })

    print(
        f"Combinations checked: "
        f"{checked:,}"
    )

    print(
        f"Qualifying combinations: "
        f"{len(results):,}"
    )

    if not results:
        return []

    # --------------------------------------------------------
    # Remove duplicate masks.
    # --------------------------------------------------------

    unique = {}

    for row in results:

        signature = np.packbits(
            row[
                "_mask"
            ]
            .to_numpy(
                dtype=np.uint8
            )
        ).tobytes()

        if signature not in unique:

            unique[
                signature
            ] = row

    results = list(
        unique.values()
    )

    print(
        f"Unique combinations: "
        f"{len(results):,}"
    )

    # --------------------------------------------------------
    # Rank.
    # --------------------------------------------------------

    results.sort(
        key=candidate_score,
        reverse=True
    )

    return results[
        :MAX_SURVIVORS
    ]


# ============================================================
# VALIDATION
# ============================================================

def evaluate_survivors_on_validation(
    train_survivors,
    validation
):

    survivors = []

    for row in train_survivors:

        pattern = row[
            "pattern"
        ]

        # ----------------------------------------------------
        # Pattern is represented by its conditions.
        # We reconstruct masks from the stored condition
        # masks by using the names.
        # ----------------------------------------------------

        parts = [
            x.strip()
            for x
            in pattern.split(
                " + "
            )
        ]

        mask = pd.Series(
            True,
            index=validation.index
        )

        valid = True

        conditions = generate_conditions(
            validation
        )

        for part in parts:

            if part not in conditions:

                valid = False
                break

            mask &= (
                conditions[
                    part
                ]
                .astype(bool)
            )

        if not valid:
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

        if row[
            "direction"
        ] == "LONG":

            returns = subset[
                "LONG_RETURN"
            ]

        else:

            returns = subset[
                "SHORT_RETURN"
            ]

        result = stats(
            returns
        )

        if result is None:
            continue

        survivors.append({

            "depth":
                row["depth"],

            "pattern":
                pattern,

            "direction":
                row["direction"],

            "train_trades":
                row["trades"],

            "train_win":
                row["win_rate"],

            "train_pf":
                row["profit_factor"],

            "validation_trades":
                result["trades"],

            "validation_win":
                result["win_rate"],

            "validation_average":
                result["average"],

            "validation_pf":
                result["profit_factor"],

            "validation_total":
                result["total"]
        })

    if not survivors:
        return pd.DataFrame()

    frame = pd.DataFrame(
        survivors
    )

    frame[
        "validation_score"
    ] = (
        frame[
            "validation_win"
        ]
        +
        10 *
        np.log1p(
            frame[
                "validation_pf"
            ].clip(
                lower=0
            )
        )
        +
        5 *
        frame[
            "validation_average"
        ]
    )

    frame = (
        frame
        .sort_values(
            "validation_score",
            ascending=False
        )
        .reset_index(
            drop=True
        )
    )

    return frame


# ============================================================
# FINAL TEST
# ============================================================

def final_test(
    validation_survivors,
    test
):

    results = []

    conditions = generate_conditions(
        test
    )

    for _, row in (
        validation_survivors
        .head(MAX_SURVIVORS)
        .iterrows()
    ):

        parts = [
            x.strip()
            for x
            in row[
                "pattern"
            ].split(
                " + "
            )
        ]

        mask = pd.Series(
            True,
            index=test.index
        )

        valid = True

        for part in parts:

            if part not in conditions:

                valid = False
                break

            mask &= (
                conditions[
                    part
                ]
                .astype(bool)
            )

        if not valid:
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

        if row[
            "direction"
        ] == "LONG":

            returns = subset[
                "LONG_RETURN"
            ]

        else:

            returns = subset[
                "SHORT_RETURN"
            ]

        result = stats(
            returns
        )

        if result is None:
            continue

        results.append({

            "depth":
                row["depth"],

            "pattern":
                row["pattern"],

            "direction":
                row["direction"],

            "train_trades":
                row["train_trades"],

            "train_win":
                row["train_win"],

            "train_pf":
                row["train_pf"],

            "validation_trades":
                row[
                    "validation_trades"
                ],

            "validation_win":
                row[
                    "validation_win"
                ],

            "validation_pf":
                row[
                    "validation_pf"
                ],

            "test_trades":
                result[
                    "trades"
                ],

            "test_win":
                result[
                    "win_rate"
                ],

            "test_average":
                result[
                    "average"
                ],

            "test_pf":
                result[
                    "profit_factor"
                ],

            "test_total":
                result[
                    "total"
                ]
        })

    if not results:
        return pd.DataFrame()

    result_df = pd.DataFrame(
        results
    )

    result_df[
        "final_score"
    ] = (
        result_df[
            "test_win"
        ]
        +
        10 *
        np.log1p(
            result_df[
                "test_pf"
            ].clip(
                lower=0
            )
        )
        +
        5 *
        result_df[
            "test_average"
        ]
    )

    return (
        result_df
        .sort_values(
            [
                "test_win",
                "test_pf",
                "test_average",
                "test_trades"
            ],
            ascending=False
        )
        .reset_index(
            drop=True
        )
    )


# ============================================================
# YEARLY STABILITY
# ============================================================

def yearly_results(
    test,
    pattern,
    direction
):

    conditions = generate_conditions(
        test
    )

    parts = [
        x.strip()
        for x in pattern.split(
            " + "
        )
    ]

    mask = pd.Series(
        True,
        index=test.index
    )

    for part in parts:

        if part not in conditions:
            return pd.DataFrame()

        mask &= (
            conditions[
                part
            ]
            .astype(bool)
        )

    subset = test[
        mask
    ].copy()

    if subset.empty:
        return pd.DataFrame()

    subset[
        "YEAR"
    ] = (
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

        if direction == "LONG":

            returns = group[
                "LONG_RETURN"
            ]

        else:

            returns = group[
                "SHORT_RETURN"
            ]

        s = stats(
            returns
        )

        if s is None:
            continue

        rows.append({

            "year":
                year,

            "trades":
                s["trades"],

            "win_rate":
                s["win_rate"],

            "average":
                s["average"],

            "profit_factor":
                s["profit_factor"],

            "total":
                s["total"]
        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# PRINT TOP RESULTS
# ============================================================

def print_top(
    df,
    title,
    n=30
):

    print()
    print("=" * 100)
    print(title)
    print("=" * 100)

    if df is None or df.empty:

        print("NONE")

        return

    print(
        df.head(n)
        .to_string(
            index=False
        )
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 100)
    print(
        "EXHAUSTIVE PREVIOUS-DAY OPENING + EOD SEARCH"
    )
    print("=" * 100)

    print()
    print(
        "ENTRY : NEXT DAY 09:15 OPEN"
    )

    print(
        "EXIT  : NEXT DAY 15:27 OPEN"
    )

    print()
    print(
        "SIGNAL INFORMATION:"
    )

    print(
        "PREVIOUS TRADING DAY ONLY"
    )

    print()
    print(
        "TIMEFRAMES:"
    )

    print(
        "1m / 2m / 3m / 5m / 10m / 15m"
    )

    print()
    print(
        f"TARGET FINAL TEST WIN RATE: "
        f"{TARGET_WIN_RATE:.0f}%"
    )

    # ========================================================
    # DATA
    # ========================================================

    df = create_dataset()

    print()
    print(
        f"TOTAL OBSERVATIONS: "
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
        f"TRAIN ENDS: "
        f"{train_end}"
    )

    print(
        f"VALIDATION ENDS: "
        f"{validation_end}"
    )

    # ========================================================
    # CONDITIONS
    # ========================================================

    print()
    print("=" * 100)
    print(
        "GENERATING PREVIOUS-DAY CONDITIONS"
    )
    print("=" * 100)

    conditions = generate_conditions(
        train
    )

    print(
        f"TOTAL UNIQUE CONDITIONS: "
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
        f"DEPTH 1 SURVIVORS: "
        f"{len(survivors):,}"
    )

    all_training_results = []

    for row in survivors:

        all_training_results.append({
            k: v
            for k, v in row.items()
            if k != "_mask"
        })

    # ========================================================
    # DEPTH 2-4
    # ========================================================

    for depth in range(
        2,
        MAX_DEPTH + 1
    ):

        if not survivors:

            print(
                f"No survivors for depth "
                f"{depth}."
            )

            break

        survivors = search_next_depth(
            train,
            survivors,
            conditions,
            depth
        )

        print(
            f"DEPTH {depth} SURVIVORS: "
            f"{len(survivors):,}"
        )

        for row in survivors:

            all_training_results.append({
                k: v
                for k, v in row.items()
                if k != "_mask"
            })

    # ========================================================
    # SAVE TRAINING
    # ========================================================

    training_df = pd.DataFrame(
        all_training_results
    )

    if not training_df.empty:

        training_df.to_csv(
            "EXHAUSTIVE_EOD_OPEN_TRAINING.csv",
            index=False
        )

    print_top(
        training_df,
        "TOP TRAINING PATTERNS",
        50
    )

    # ========================================================
    # VALIDATION
    # ========================================================

    print()
    print("=" * 100)
    print(
        "VALIDATING SURVIVING PATTERNS"
    )
    print("=" * 100)

    if not survivors:

        print(
            "No training survivors."
        )

        return

    # --------------------------------------------------------
    # Validate the final survivors from deepest search.
    # --------------------------------------------------------

    validation_df = (
        evaluate_survivors_on_validation(
            survivors,
            validation
        )
    )

    print_top(
        validation_df,
        "VALIDATION SURVIVORS",
        50
    )

    if validation_df.empty:

        print()
        print(
            "NO PATTERNS SURVIVED VALIDATION."
        )

        return

    validation_df.to_csv(
        "EXHAUSTIVE_EOD_OPEN_VALIDATION.csv",
        index=False
    )

    # ========================================================
    # FINAL TEST
    # ========================================================

    print()
    print("=" * 100)
    print(
        "FINAL UNSEEN TEST"
    )
    print("=" * 100)

    final_df = final_test(
        validation_df,
        test
    )

    if final_df.empty:

        print(
            "NO PATTERNS SURVIVED "
            "FINAL TEST REQUIREMENTS."
        )

        return

    final_df.to_csv(
        "EXHAUSTIVE_EOD_OPEN_FINAL_TEST.csv",
        index=False
    )

    print_top(
        final_df,
        "TOP FINAL TEST RESULTS",
        50
    )

    # ========================================================
    # 85% RESULTS
    # ========================================================

    print()
    print("=" * 100)
    print(
        "FINAL TEST >= 85%"
    )
    print("=" * 100)

    over_85 = final_df[
        final_df[
            "test_win"
        ] >= TARGET_WIN_RATE
    ].copy()

    if over_85.empty:

        print(
            "NONE"
        )

    else:

        print(
            over_85
            .sort_values(
                [
                    "test_win",
                    "test_pf",
                    "test_average"
                ],
                ascending=False
            )
            .to_string(
                index=False
            )
        )

    # ========================================================
    # 80% RESULTS
    # ========================================================

    print()
    print("=" * 100)
    print(
        "FINAL TEST >= 80%"
    )
    print("=" * 100)

    over_80 = final_df[
        final_df[
            "test_win"
        ] >= 80
    ]

    if over_80.empty:

        print("NONE")

    else:

        print(
            over_80
            .head(50)
            .to_string(
                index=False
            )
        )

    # ========================================================
    # BEST RESULT
    # ========================================================

    best = final_df.iloc[0]

    print()
    print("=" * 100)
    print(
        "BEST FINAL TEST RESULT"
    )
    print("=" * 100)

    print()
    print(
        f"Pattern       : "
        f"{best['pattern']}"
    )

    print(
        f"Direction     : "
        f"{best['direction']}"
    )

    print(
        f"Train trades  : "
        f"{int(best['train_trades']):,}"
    )

    print(
        f"Train win     : "
        f"{best['train_win']:.2f}%"
    )

    print(
        f"Validation    : "
        f"{best['validation_win']:.2f}%"
    )

    print(
        f"Test trades   : "
        f"{int(best['test_trades']):,}"
    )

    print(
        f"TEST WIN RATE : "
        f"{best['test_win']:.2f}%"
    )

    print(
        f"TEST AVG      : "
        f"{best['test_average']:.4f}%"
    )

    print(
        f"TEST PF       : "
        f"{best['test_pf']:.3f}"
    )

    print(
        f"TEST TOTAL    : "
        f"{best['test_total']:.4f}%"
    )

    # ========================================================
    # YEARLY STABILITY
    # ========================================================

    print()
    print("=" * 100)
    print(
        "YEAR-BY-YEAR RESULTS OF TOP 10"
    )
    print("=" * 100)

    for index, row in (
        final_df
        .head(10)
        .iterrows()
    ):

        print()
        print(
            "-" * 100
        )

        print(
            f"{index + 1}. "
            f"{row['pattern']}"
        )

        print(
            f"Direction: "
            f"{row['direction']}"
        )

        yearly = yearly_results(
            test,
            row["pattern"],
            row["direction"]
        )

        if yearly.empty:

            print(
                "No yearly data."
            )

        else:

            print(
                yearly
                .to_string(
                    index=False
                )
            )

    # ========================================================
    # FINAL MESSAGE
    # ========================================================

    print()
    print("=" * 100)
    print(
        "SEARCH COMPLETE"
    )
    print("=" * 100)

    print()
    print(
        "Files created:"
    )

    print(
        "EXHAUSTIVE_EOD_OPEN_TRAINING.csv"
    )

    print(
        "EXHAUSTIVE_EOD_OPEN_VALIDATION.csv"
    )

    print(
        "EXHAUSTIVE_EOD_OPEN_FINAL_TEST.csv"
    )

    print()

    if over_85.empty:

        print(
            "RESULT: NO 85%+ FINAL-TEST "
            "PATTERN WAS FOUND."
        )

    else:

        print(
            "RESULT: 85%+ FINAL-TEST "
            "PATTERN(S) FOUND."
        )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
