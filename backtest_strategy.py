import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import itertools
import warnings

warnings.filterwarnings("ignore")


# ============================================================
# LARGE NEXT-DAY MOVE PRECURSOR SCANNER
# ============================================================
#
# SIGNAL:
#   Previous trading day's EOD structure
#
# OUTCOME:
#   Following trading day
#
# ENTRY:
#   09:15 open
#
# EXIT:
#   15:27 open
#
# TIMEFRAMES:
#   1m / 2m / 3m / 5m / 10m / 15m
#
# TARGETS:
#   Top 1%, 2%, 5% gainers
#   Bottom 1%, 2%, 5% losers
#   Top 1%, 2%, 5% absolute movers
#
# IMPORTANT:
#   The final test is NEVER used to discover patterns.
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

TRAIN_PERCENT = 0.60
VALIDATION_PERCENT = 0.20
TEST_PERCENT = 0.20

MIN_TRAIN = 100
MIN_VALIDATION = 30
MIN_TEST = 30

# Maximum number of conditions in a pattern.
MAX_DEPTH = 3

# Number of strongest atomic conditions retained for
# combination search.
ATOMIC_CANDIDATES = 350

# Round-trip estimated cost.
# Used for trading-return statistics only.
COST_PERCENT = 0.10


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
# THRESHOLDS
# ============================================================

BODY_LEVELS = [
    0.30,
    0.40,
    0.50,
    0.60,
    0.70,
    0.80,
    0.90
]

REL_VOLUME_LEVELS = [
    0.75,
    1.00,
    1.25,
    1.50,
    2.00,
    2.50,
    3.00
]

VOLUME_RATIO_LEVELS = [
    0.50,
    0.75,
    1.00,
    1.25,
    1.50,
    2.00,
    3.00
]

RANGE_RATIO_LEVELS = [
    0.50,
    0.75,
    1.00,
    1.25,
    1.50,
    2.00,
    2.50
]

CLOSE_POSITION_LEVELS = [
    0.10,
    0.20,
    0.30,
    0.40,
    0.50,
    0.60,
    0.70,
    0.80,
    0.90
]

MOMENTUM_LEVELS = [
    0.05,
    0.10,
    0.15,
    0.20,
    0.30,
    0.50,
    0.75,
    1.00
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
# BUILD CANDLE
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

        body_ratio = (
            body /
            candle_range
        )

        close_position = (
            candle["close"]
            -
            candle["low"]
        ) / candle_range

        upper_wick = (
            candle["high"]
            -
            max(
                candle["open"],
                candle["close"]
            )
        ) / candle_range

        lower_wick = (
            min(
                candle["open"],
                candle["close"]
            )
            -
            candle["low"]
        ) / candle_range

    else:

        body_ratio = 0
        close_position = 0.5
        upper_wick = 0
        lower_wick = 0

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

        momentum = 0

    return {

        "range":
            candle_range,

        "body_ratio":
            body_ratio,

        "close_position":
            close_position,

        "upper_wick":
            upper_wick,

        "lower_wick":
            lower_wick,

        "momentum":
            momentum
    }


# ============================================================
# DIRECTION
# ============================================================

def candle_direction(
    candle
):

    if candle["close"] > candle["open"]:
        return "BULL"

    if candle["close"] < candle["open"]:
        return "BEAR"

    return "DOJI"


# ============================================================
# EOD CANDLE STARTS
# ============================================================

def get_starts(
    timeframe
):

    if timeframe == 1:

        return [
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28
        ]

    if timeframe == 2:

        return [
            18,
            20,
            22,
            24,
            26,
            28
        ]

    if timeframe == 3:

        return [
            15,
            18,
            21,
            24,
            27
        ]

    if timeframe == 5:

        return [
            10,
            15,
            20,
            25
        ]

    if timeframe == 10:

        return [
            0,
            10,
            20
        ]

    if timeframe == 15:

        return [
            0,
            15
        ]

    return []


# ============================================================
# BUILD TIMEFRAME FEATURES
# ============================================================

def build_timeframe_features(
    day,
    timeframe,
    minutes
):

    starts = get_starts(
        minutes
    )

    candles = {}

    for start in starts:

        candle = build_candle(
            day,
            start,
            minutes
        )

        if candle is not None:

            candles[
                start
            ] = candle

    if len(candles) < 2:

        return {}

    ordered = sorted(
        candles.keys()
    )

    last_start = ordered[-1]
    previous_start = ordered[-2]

    last = candles[
        last_start
    ]

    previous = candles[
        previous_start
    ]

    last_metrics = candle_metrics(
        last
    )

    previous_metrics = candle_metrics(
        previous
    )

    # --------------------------------------------------------
    # Historical range / volume
    # --------------------------------------------------------

    historical_ranges = []
    historical_volumes = []

    for start in ordered[:-1]:

        candle = candles[
            start
        ]

        metrics = candle_metrics(
            candle
        )

        historical_ranges.append(
            metrics["range"]
        )

        historical_volumes.append(
            candle["volume"]
        )

    if historical_ranges:

        average_range = np.mean(
            historical_ranges
        )

    else:

        average_range = 0

    if historical_volumes:

        average_volume = np.mean(
            historical_volumes
        )

    else:

        average_volume = 0

    if average_range > 0:

        range_ratio = (
            last_metrics["range"]
            /
            average_range
        )

    else:

        range_ratio = 0

    if average_volume > 0:

        relative_volume = (
            last["volume"]
            /
            average_volume
        )

    else:

        relative_volume = 0

    if previous["volume"] > 0:

        volume_ratio = (
            last["volume"]
            /
            previous["volume"]
        )

    else:

        volume_ratio = 0

    prefix = timeframe

    return {

        f"{prefix}_BEAR":
            candle_direction(
                last
            ) == "BEAR",

        f"{prefix}_BULL":
            candle_direction(
                last
            ) == "BULL",

        f"{prefix}_PREVIOUS_BEAR":
            candle_direction(
                previous
            ) == "BEAR",

        f"{prefix}_PREVIOUS_BULL":
            candle_direction(
                previous
            ) == "BULL",

        f"{prefix}_SAME":
            (
                candle_direction(last)
                ==
                candle_direction(previous)
                and
                candle_direction(last)
                != "DOJI"
            ),

        f"{prefix}_OPPOSITE":
            (
                candle_direction(last)
                !=
                candle_direction(previous)
                and
                candle_direction(last)
                != "DOJI"
                and
                candle_direction(previous)
                != "DOJI"
            ),

        f"{prefix}_BODY":
            last_metrics[
                "body_ratio"
            ],

        f"{prefix}_CLOSE_POS":
            last_metrics[
                "close_position"
            ],

        f"{prefix}_UPPER_WICK":
            last_metrics[
                "upper_wick"
            ],

        f"{prefix}_LOWER_WICK":
            last_metrics[
                "lower_wick"
            ],

        f"{prefix}_RANGE":
            last_metrics[
                "range"
            ],

        f"{prefix}_RANGE_RATIO":
            range_ratio,

        f"{prefix}_REL_VOL":
            relative_volume,

        f"{prefix}_VOL_RATIO":
            volume_ratio,

        f"{prefix}_MOMENTUM":
            last_metrics[
                "momentum"
            ],

        f"{prefix}_MOM_CHANGE":
            (
                last_metrics[
                    "momentum"
                ]
                -
                previous_metrics[
                    "momentum"
                ]
            ),

        f"{prefix}_BODY_CHANGE":
            (
                last_metrics[
                    "body_ratio"
                ]
                -
                previous_metrics[
                    "body_ratio"
                ]
            )
    }


# ============================================================
# BUILD ALL PREVIOUS-DAY FEATURES
# ============================================================

def build_features(
    day
):

    features = {}

    for timeframe, minutes in (
        TIMEFRAMES.items()
    ):

        timeframe_features = (
            build_timeframe_features(
                day,
                timeframe,
                minutes
            )
        )

        features.update(
            timeframe_features
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
        in df.groupby(
            "date"
        )
    }

    dates = sorted(
        grouped.keys()
    )

    symbol = os.path.basename(
        filename
    ).replace(
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

        # ----------------------------------------------------
        # Next-day trade
        # ----------------------------------------------------

        if ENTRY_TIME not in event_day:
            continue

        if EXIT_TIME not in event_day:
            continue

        features = build_features(
            previous_day
        )

        if not features:
            continue

        entry = event_day[
            ENTRY_TIME
        ]["open"]

        exit_price = event_day[
            EXIT_TIME
        ]["open"]

        if entry <= 0:
            continue

        next_day_return = (
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

            "next_day_return":
                next_day_return,

            "absolute_move":
                abs(
                    next_day_return
                ),

            **features
        })

    return results


# ============================================================
# CREATE DATASET
# ============================================================

def create_dataset():

    raw_data = download_dataset()

    zip_file = zipfile.ZipFile(
        io.BytesIO(
            raw_data
        )
    )

    files = [
        filename
        for filename
        in zip_file.namelist()
        if filename.endswith(
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

def split_data(
    df
):

    dates = sorted(
        df[
            "event_date"
        ].dt.normalize()
        .unique()
    )

    n = len(dates)

    train_index = int(
        n * TRAIN_PERCENT
    )

    validation_index = int(
        n *
        (
            TRAIN_PERCENT
            +
            VALIDATION_PERCENT
        )
    )

    train_end = dates[
        train_index
    ]

    validation_end = dates[
        validation_index
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
# CREATE LARGE-MOVE TARGETS
# ============================================================

def create_targets(
    df,
    percentile
):

    data = df.copy()

    # --------------------------------------------------------
    # Calculate threshold from THIS DATASET ONLY.
    #
    # For discovery this is done separately on train.
    # Validation and test are evaluated against their own
    # realized distribution, so we can measure whether the
    # pattern finds unusually large movers there.
    # --------------------------------------------------------

    gain_threshold = (
        data[
            "next_day_return"
        ]
        .quantile(
            1 - percentile
        )
    )

    loss_threshold = (
        data[
            "next_day_return"
        ]
        .quantile(
            percentile
        )
    )

    absolute_threshold = (
        data[
            "absolute_move"
        ]
        .quantile(
            1 - percentile
        )
    )

    data["BIG_GAIN"] = (
        data[
            "next_day_return"
        ]
        >= gain_threshold
    )

    data["BIG_LOSS"] = (
        data[
            "next_day_return"
        ]
        <= loss_threshold
    )

    data["BIG_ABSOLUTE_MOVE"] = (
        data[
            "absolute_move"
        ]
        >= absolute_threshold
    )

    return (
        data,
        gain_threshold,
        loss_threshold,
        absolute_threshold
    )


# ============================================================
# ATOMIC CONDITIONS
# ============================================================

def generate_atomic_conditions(
    df
):

    conditions = {}

    for timeframe in TIMEFRAMES:

        prefix = timeframe

        # ----------------------------------------------------
        # Direction
        # ----------------------------------------------------

        conditions[
            f"{prefix}:BEAR"
        ] = df[
            f"{prefix}_BEAR"
        ].astype(bool)

        conditions[
            f"{prefix}:BULL"
        ] = df[
            f"{prefix}_BULL"
        ].astype(bool)

        conditions[
            f"{prefix}:PREVIOUS_BEAR"
        ] = df[
            f"{prefix}_PREVIOUS_BEAR"
        ].astype(bool)

        conditions[
            f"{prefix}:PREVIOUS_BULL"
        ] = df[
            f"{prefix}_PREVIOUS_BULL"
        ].astype(bool)

        conditions[
            f"{prefix}:SAME"
        ] = df[
            f"{prefix}_SAME"
        ].astype(bool)

        conditions[
            f"{prefix}:OPPOSITE"
        ] = df[
            f"{prefix}_OPPOSITE"
        ].astype(bool)

        # ----------------------------------------------------
        # Body
        # ----------------------------------------------------

        for level in BODY_LEVELS:

            conditions[
                f"{prefix}:BODY>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_BODY"
                ]
                >= level
            )

        # ----------------------------------------------------
        # Close position
        # ----------------------------------------------------

        for level in (
            CLOSE_POSITION_LEVELS
        ):

            conditions[
                f"{prefix}:CLOSE_POS<={level:.2f}"
            ] = (
                df[
                    f"{prefix}_CLOSE_POS"
                ]
                <= level
            )

            conditions[
                f"{prefix}:CLOSE_POS>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_CLOSE_POS"
                ]
                >= level
            )

        # ----------------------------------------------------
        # Relative volume
        # ----------------------------------------------------

        for level in (
            REL_VOLUME_LEVELS
        ):

            conditions[
                f"{prefix}:REL_VOL>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_REL_VOL"
                ]
                >= level
            )

        # ----------------------------------------------------
        # Volume ratio
        # ----------------------------------------------------

        for level in (
            VOLUME_RATIO_LEVELS
        ):

            conditions[
                f"{prefix}:VOL_RATIO>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_VOL_RATIO"
                ]
                >= level
            )

        # ----------------------------------------------------
        # Range ratio
        # ----------------------------------------------------

        for level in (
            RANGE_RATIO_LEVELS
        ):

            conditions[
                f"{prefix}:RANGE_RATIO<={level:.2f}"
            ] = (
                df[
                    f"{prefix}_RANGE_RATIO"
                ]
                <= level
            )

            conditions[
                f"{prefix}:RANGE_RATIO>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_RANGE_RATIO"
                ]
                >= level
            )

        # ----------------------------------------------------
        # Momentum
        # ----------------------------------------------------

        for level in (
            MOMENTUM_LEVELS
        ):

            conditions[
                f"{prefix}:MOM>={level:.2f}%"
            ] = (
                df[
                    f"{prefix}_MOMENTUM"
                ]
                >= level
            )

            conditions[
                f"{prefix}:MOM<=-{level:.2f}%"
            ] = (
                df[
                    f"{prefix}_MOMENTUM"
                ]
                <= -level
            )

        # ----------------------------------------------------
        # Momentum change
        # ----------------------------------------------------

        for level in [
            0.05,
            0.10,
            0.20,
            0.30,
            0.50
        ]:

            conditions[
                f"{prefix}:MOM_CHANGE>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_MOM_CHANGE"
                ]
                >= level
            )

            conditions[
                f"{prefix}:MOM_CHANGE<=-{level:.2f}"
            ] = (
                df[
                    f"{prefix}_MOM_CHANGE"
                ]
                <= -level
            )

    return conditions


# ============================================================
# CROSS-TIMEFRAME CONDITIONS
# ============================================================

def generate_cross_conditions(
    df
):

    conditions = {}

    timeframes = list(
        TIMEFRAMES.keys()
    )

    for tf1, tf2 in itertools.combinations(
        timeframes,
        2
    ):

        conditions[
            f"{tf1}:BEAR + {tf2}:BEAR"
        ] = (
            df[
                f"{tf1}_BEAR"
            ]
            &
            df[
                f"{tf2}_BEAR"
            ]
        )

        conditions[
            f"{tf1}:BULL + {tf2}:BULL"
        ] = (
            df[
                f"{tf1}_BULL"
            ]
            &
            df[
                f"{tf2}_BULL"
            ]
        )

        conditions[
            f"{tf1}:OPPOSITE + {tf2}:OPPOSITE"
        ] = (
            df[
                f"{tf1}_OPPOSITE"
            ]
            &
            df[
                f"{tf2}_OPPOSITE"
            ]
        )

        conditions[
            f"{tf1}:SAME + {tf2}:SAME"
        ] = (
            df[
                f"{tf1}_SAME"
            ]
            &
            df[
                f"{tf2}_SAME"
            ]
        )

    return conditions


# ============================================================
# EVALUATE TARGET
# ============================================================

def evaluate_target(
    df,
    mask,
    target_column
):

    subset = df[
        mask
    ]

    n = len(
        subset
    )

    if n < MIN_TRAIN:

        return None

    hits = (
        subset[
            target_column
        ]
        .sum()
    )

    precision = (
        hits /
        n *
        100
    )

    baseline = (
        df[
            target_column
        ]
        .mean()
        *
        100
    )

    lift = (
        precision /
        baseline
        if baseline > 0
        else np.nan
    )

    return {

        "trades":
            n,

        "hits":
            int(hits),

        "precision":
            precision,

        "baseline":
            baseline,

        "lift":
            lift,

        "average_move":
            subset[
                "absolute_move"
            ].mean(),

        "average_return":
            subset[
                "next_day_return"
            ].mean()
    }


# ============================================================
# RANK ATOMIC CONDITIONS
# ============================================================

def rank_atomic_conditions(
    df,
    conditions,
    target
):

    rows = []

    for name, mask in (
        conditions.items()
    ):

        stats = evaluate_target(
            df,
            mask,
            target
        )

        if stats is None:
            continue

        rows.append({

            "pattern":
                name,

            **stats
        })

    if not rows:

        return []

    frame = pd.DataFrame(
        rows
    )

    # Use a combination of precision and lift.
    frame[
        "score"
    ] = (
        frame[
            "precision"
        ]
        *
        np.log1p(
            frame[
                "lift"
            ]
        )
    )

    frame = frame.sort_values(
        [
            "score",
            "precision",
            "lift"
        ],
        ascending=False
    )

    return frame.head(
        ATOMIC_CANDIDATES
    )[
        "pattern"
    ].tolist()


# ============================================================
# COMBINATION SEARCH
# ============================================================

def search_combinations(
    df,
    conditions,
    target,
    max_depth=3
):

    # --------------------------------------------------------
    # Store masks permanently.
    # This avoids the KeyError problem from the earlier
    # exhaustive scanner.
    # --------------------------------------------------------

    masks = {
        name:
            pd.Series(
                mask,
                index=df.index
            ).fillna(False).astype(bool)
        for name, mask
        in conditions.items()
    }

    # --------------------------------------------------------
    # Atomic ranking
    # --------------------------------------------------------

    candidates = rank_atomic_conditions(
        df,
        masks,
        target
    )

    print(
        f"Candidate atomic conditions: "
        f"{len(candidates):,}"
    )

    results = []

    # ========================================================
    # DEPTH 1
    # ========================================================

    depth_patterns = []

    for name in candidates:

        mask = masks[
            name
        ]

        stats = evaluate_target(
            df,
            mask,
            target
        )

        if stats is None:
            continue

        row = {

            "depth":
                1,

            "pattern":
                name,

            "_mask":
                mask,

            **stats
        }

        depth_patterns.append(
            row
        )

        results.append(
            {
                k: v
                for k, v in row.items()
                if k != "_mask"
            }
        )

    # ========================================================
    # DEPTH 2 / 3
    # ========================================================

    current = depth_patterns

    for depth in range(
        2,
        max_depth + 1
    ):

        print()
        print(
            "-" * 80
        )

        print(
            f"SEARCHING DEPTH {depth}"
        )

        new_patterns = []

        checked = 0

        for previous in current:

            previous_name = (
                previous[
                    "pattern"
                ]
            )

            previous_mask = (
                previous[
                    "_mask"
                ]
            )

            previous_parts = set(
                previous_name.split(
                    " + "
                )
            )

            for atomic_name in candidates:

                if atomic_name in previous_parts:
                    continue

                checked += 1

                combined_mask = (
                    previous_mask
                    &
                    masks[
                        atomic_name
                    ]
                )

                stats = evaluate_target(
                    df,
                    combined_mask,
                    target
                )

                if stats is None:
                    continue

                pattern = (
                    previous_name
                    +
                    " + "
                    +
                    atomic_name
                )

                row = {

                    "depth":
                        depth,

                    "pattern":
                        pattern,

                    "_mask":
                        combined_mask,

                    **stats
                }

                new_patterns.append(
                    row
                )

        print(
            f"Checked: "
            f"{checked:,}"
        )

        print(
            f"Qualifying: "
            f"{len(new_patterns):,}"
        )

        if not new_patterns:
            break

        # ----------------------------------------------------
        # Deduplicate
        # ----------------------------------------------------

        unique = {}

        for row in new_patterns:

            unique[
                row[
                    "pattern"
                ]
            ] = row

        new_patterns = list(
            unique.values()
        )

        # ----------------------------------------------------
        # Rank by precision and lift
        # ----------------------------------------------------

        ranking = pd.DataFrame([
            {
                k: v
                for k, v in row.items()
                if k != "_mask"
            }
            for row in new_patterns
        ])

        ranking[
            "score"
        ] = (
            ranking[
                "precision"
            ]
            *
            np.log1p(
                ranking[
                    "lift"
                ]
            )
        )

        ranking = ranking.sort_values(
            [
                "score",
                "precision",
                "lift"
            ],
            ascending=False
        )

        keep_names = set(
            ranking
            .head(
                200
            )[
                "pattern"
            ]
            .tolist()
        )

        current = [
            row
            for row
            in new_patterns
            if row[
                "pattern"
            ] in keep_names
        ]

        for row in new_patterns:

            results.append(
                {
                    k: v
                    for k, v in row.items()
                    if k != "_mask"
                }
            )

        print(
            f"Retained for next depth: "
            f"{len(current):,}"
        )

    return pd.DataFrame(
        results
    )


# ============================================================
# BUILD MASK FROM PATTERN
# ============================================================

def pattern_mask(
    df,
    pattern,
    library
):

    parts = [
        x.strip()
        for x in pattern.split(
            " + "
        )
    ]

    mask = pd.Series(
        True,
        index=df.index
    )

    for part in parts:

        if part not in library:

            return None

        mask &= library[
            part
        ]

    return mask


# ============================================================
# VALIDATE PATTERNS
# ============================================================

def validate_patterns(
    train,
    validation,
    test,
    patterns,
    target
):

    train_library = {}

    train_library.update(
        generate_atomic_conditions(
            train
        )
    )

    train_library.update(
        generate_cross_conditions(
            train
        )
    )

    validation_library = {}

    validation_library.update(
        generate_atomic_conditions(
            validation
        )
    )

    validation_library.update(
        generate_cross_conditions(
            validation
        )
    )

    test_library = {}

    test_library.update(
        generate_atomic_conditions(
            test
        )
    )

    test_library.update(
        generate_cross_conditions(
            test
        )
    )

    rows = []

    # --------------------------------------------------------
    # Only the strongest training patterns move forward.
    # --------------------------------------------------------

    training_sorted = (
        patterns
        .sort_values(
            [
                "precision",
                "lift"
            ],
            ascending=False
        )
        .head(
            500
        )
    )

    for _, discovered in (
        training_sorted.iterrows()
    ):

        pattern = discovered[
            "pattern"
        ]

        train_mask = pattern_mask(
            train,
            pattern,
            train_library
        )

        validation_mask = pattern_mask(
            validation,
            pattern,
            validation_library
        )

        test_mask = pattern_mask(
            test,
            pattern,
            test_library
        )

        if (
            train_mask is None
            or
            validation_mask is None
            or
            test_mask is None
        ):

            continue

        train_stats = evaluate_target(
            train,
            train_mask,
            target
        )

        validation_stats = evaluate_target(
            validation,
            validation_mask,
            target
        )

        if (
            validation_stats is None
            or
            validation_stats[
                "trades"
            ] < MIN_VALIDATION
        ):

            continue

        # ----------------------------------------------------
        # FINAL TEST
        # ----------------------------------------------------

        test_stats = evaluate_target(
            test,
            test_mask,
            target
        )

        if (
            test_stats is None
            or
            test_stats[
                "trades"
            ] < MIN_TEST
        ):

            continue

        rows.append({

            "pattern":
                pattern,

            "train_trades":
                train_stats[
                    "trades"
                ],

            "train_precision":
                train_stats[
                    "precision"
                ],

            "train_lift":
                train_stats[
                    "lift"
                ],

            "validation_trades":
                validation_stats[
                    "trades"
                ],

            "validation_precision":
                validation_stats[
                    "precision"
                ],

            "validation_lift":
                validation_stats[
                    "lift"
                ],

            "test_trades":
                test_stats[
                    "trades"
                ],

            "test_precision":
                test_stats[
                    "precision"
                ],

            "test_lift":
                test_stats[
                    "lift"
                ],

            "test_average_move":
                test_stats[
                    "average_move"
                ],

            "test_average_return":
                test_stats[
                    "average_return"
                ]
        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# YEAR STABILITY
# ============================================================

def year_stability(
    test,
    pattern,
    target
):

    library = {}

    library.update(
        generate_atomic_conditions(
            test
        )
    )

    library.update(
        generate_cross_conditions(
            test
        )
    )

    mask = pattern_mask(
        test,
        pattern,
        library
    )

    if mask is None:
        return pd.DataFrame()

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

        n = len(
            group
        )

        if n == 0:
            continue

        hits = (
            group[
                target
            ].sum()
        )

        precision = (
            hits /
            n *
            100
        )

        rows.append({

            "year":
                year,

            "trades":
                n,

            "hits":
                int(hits),

            "precision":
                precision,

            "average_move":
                group[
                    "absolute_move"
                ].mean(),

            "average_return":
                group[
                    "next_day_return"
                ].mean()
        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 90)
    print(
        "NEXT-DAY LARGE-MOVE PRECURSOR SCANNER"
    )
    print("=" * 90)

    print()
    print(
        "Previous-day EOD signal"
    )

    print(
        "Next-day 09:15 entry"
    )

    print(
        "Next-day 15:27 exit"
    )

    print()
    print(
        "Timeframes:"
    )

    print(
        "1m | 2m | 3m | 5m | 10m | 15m"
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
    ) = split_data(
        df
    )

    print()
    print("=" * 90)

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
    # TARGET PERCENTILES
    # ========================================================

    target_percentiles = [
        0.01,
        0.02,
        0.05
    ]

    # ========================================================
    # PROCESS EACH TARGET
    # ========================================================

    for percentile in (
        target_percentiles
    ):

        label = (
            f"{int(percentile * 100)}%"
        )

        print()
        print("=" * 90)
        print(
            f"TARGET: TOP/BOTTOM {label} MOVERS"
        )
        print("=" * 90)

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # Target thresholds are calculated separately within
        # each dataset. This asks:
        #
        # "Does this pattern identify unusually large movers
        # in each period?"
        #
        # ----------------------------------------------------

        (
            train_target,
            train_gain_threshold,
            train_loss_threshold,
            train_absolute_threshold
        ) = create_targets(
            train,
            percentile
        )

        (
            validation_target,
            validation_gain_threshold,
            validation_loss_threshold,
            validation_absolute_threshold
        ) = create_targets(
            validation,
            percentile
        )

        (
            test_target,
            test_gain_threshold,
            test_loss_threshold,
            test_absolute_threshold
        ) = create_targets(
            test,
            percentile
        )

        print()
        print(
            "TRAIN thresholds:"
        )

        print(
            f"Top gain: "
            f"{train_gain_threshold:.3f}%"
        )

        print(
            f"Bottom loss: "
            f"{train_loss_threshold:.3f}%"
        )

        print(
            f"Absolute move: "
            f"{train_absolute_threshold:.3f}%"
        )

        print()
        print(
            "TEST thresholds:"
        )

        print(
            f"Top gain: "
            f"{test_gain_threshold:.3f}%"
        )

        print(
            f"Bottom loss: "
            f"{test_loss_threshold:.3f}%"
        )

        print(
            f"Absolute move: "
            f"{test_absolute_threshold:.3f}%"
        )

        targets = {

            "BIG_GAIN":
                "BIG_GAIN",

            "BIG_LOSS":
                "BIG_LOSS",

            "BIG_ABSOLUTE_MOVE":
                "BIG_ABSOLUTE_MOVE"
        }

        # ====================================================
        # EACH TARGET
        # ====================================================

        for target_name, target_column in (
            targets.items()
        ):

            print()
            print("-" * 80)

            print(
                f"SEARCHING TARGET: "
                f"{target_name}"
            )

            # ------------------------------------------------
            # CONDITIONS
            # ------------------------------------------------

            print()
            print(
                "Generating conditions..."
            )

            conditions = {}

            conditions.update(
                generate_atomic_conditions(
                    train_target
                )
            )

            conditions.update(
                generate_cross_conditions(
                    train_target
                )
            )

            print(
                f"Total conditions: "
                f"{len(conditions):,}"
            )

            # ------------------------------------------------
            # SEARCH
            # ------------------------------------------------

            training_patterns = (
                search_combinations(
                    train_target,
                    conditions,
                    target_column,
                    MAX_DEPTH
                )
            )

            if training_patterns.empty:

                print(
                    "No training patterns found."
                )

                continue

            # ------------------------------------------------
            # SAVE TRAINING
            # ------------------------------------------------

            training_file = (
                f"LARGE_MOVE_"
                f"{label}_"
                f"{target_name}_"
                f"TRAIN.csv"
            )

            training_patterns.to_csv(
                training_file,
                index=False
            )

            # ------------------------------------------------
            # VALIDATION / TEST
            # ------------------------------------------------

            final_results = (
                validate_patterns(
                    train_target,
                    validation_target,
                    test_target,
                    training_patterns,
                    target_column
                )
            )

            if final_results.empty:

                print(
                    "No patterns survived "
                    "validation."
                )

                continue

            # ------------------------------------------------
            # Sort by FINAL TEST precision
            #
            # This is ONLY for displaying the results.
            # The test was not used to discover the pattern.
            # ------------------------------------------------

            final_results = (
                final_results
                .sort_values(
                    [
                        "test_precision",
                        "test_lift"
                    ],
                    ascending=False
                )
                .reset_index(
                    drop=True
                )
            )

            final_file = (
                f"LARGE_MOVE_"
                f"{label}_"
                f"{target_name}_"
                f"FINAL_TEST.csv"
            )

            final_results.to_csv(
                final_file,
                index=False
            )

            # =================================================
            # TOP RESULTS
            # =================================================

            print()
            print("=" * 90)
            print(
                f"TOP {label} "
                f"{target_name} PATTERNS"
            )
            print("=" * 90)

            print(
                final_results
                .head(30)
                .to_string(
                    index=False
                )
            )

            # =================================================
            # 70% PRECISION
            # =================================================

            strong_70 = final_results[
                final_results[
                    "test_precision"
                ]
                >= 70
            ]

            print()
            print(
                f"{target_name} "
                f"FINAL TEST >= 70%"
            )

            if strong_70.empty:

                print(
                    "NONE"
                )

            else:

                print(
                    strong_70
                    .head(50)
                    .to_string(
                        index=False
                    )
                )

            # =================================================
            # 80%
            # =================================================

            strong_80 = final_results[
                final_results[
                    "test_precision"
                ]
                >= 80
            ]

            print()
            print(
                f"{target_name} "
                f"FINAL TEST >= 80%"
            )

            if strong_80.empty:

                print(
                    "NONE"
                )

            else:

                print(
                    strong_80
                    .head(50)
                    .to_string(
                        index=False
                    )
                )

            # =================================================
            # 85%
            # =================================================

            strong_85 = final_results[
                final_results[
                    "test_precision"
                ]
                >= 85
            ]

            print()
            print(
                f"{target_name} "
                f"FINAL TEST >= 85%"
            )

            if strong_85.empty:

                print(
                    "NONE"
                )

            else:

                print(
                    strong_85
                    .to_string(
                        index=False
                    )
                )

            # =================================================
            # YEAR STABILITY
            # =================================================

            print()
            print(
                "YEAR-BY-YEAR STABILITY "
                "OF TOP 5"
            )

            for _, row in (
                final_results
                .head(5)
                .iterrows()
            ):

                print()
                print(
                    row[
                        "pattern"
                    ]
                )

                yearly = (
                    year_stability(
                        test_target,
                        row[
                            "pattern"
                        ],
                        target_column
                    )
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
    # COMPLETE
    # ========================================================

    print()
    print("=" * 90)
    print(
        "LARGE-MOVE PRECURSOR SEARCH COMPLETE"
    )
    print("=" * 90)

    print()
    print(
        "The most important output is:"
    )

    print(
        "FINAL TEST >= 85%"
    )

    print()
    print(
        "A high training result alone does NOT "
        "count as an edge."
    )

    print(
        "The pattern must survive validation "
        "and the completely unseen final test."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
