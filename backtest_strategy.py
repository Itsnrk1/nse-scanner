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
# SIMPLE 85% STRATEGY SEARCH
# ============================================================
#
# FIXED EXECUTION:
#
#   Entry = NEXT DAY 09:15 OPEN
#   Exit  = NEXT DAY 15:27 OPEN
#
# DECISION INFORMATION:
#
#   ONLY previous trading day information.
#
# GOAL:
#
#   1) Find final-test win rate >= 85%
#   2) Among those, maximize return / profit factor
#   3) Keep logic simple
#
# IMPORTANT:
#
#   09:15 opening price is NEVER used to decide whether
#   the trade should be taken.
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

TARGET_WIN_RATE = 85.0

MIN_TRAIN_TRADES = 100
MIN_VALIDATION_TRADES = 30
MIN_TEST_TRADES = 30

# Estimated round-trip cost.
# Change this if needed.
ROUND_TRIP_COST = 0.10


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
# SIMPLE FACTOR THRESHOLDS
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

REL_VOLUME_LEVELS = [
    1.25,
    1.50,
    2.00,
    2.50,
    3.00
]

RANGE_LEVELS = [
    0.75,
    1.00,
    1.25,
    1.50,
    2.00
]

MOMENTUM_LEVELS = [
    0.05,
    0.10,
    0.20,
    0.30,
    0.50
]

RELATIVE_STRENGTH_LEVELS = [
    0.25,
    0.50,
    0.75,
    1.00
]


# ============================================================
# DOWNLOAD DATA
# ============================================================

def download_dataset():

    print("=" * 90)
    print("DOWNLOADING NSE DATA")
    print("=" * 90)

    response = requests.get(
        DATA_URL,
        stream=True,
        timeout=600
    )

    response.raise_for_status()

    total = int(
        response.headers.get(
            "content-length",
            0
        )
    )

    downloaded = 0
    chunks = []

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
            start_minute + i
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

        body_ratio = 0
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

        momentum = 0

    return {

        "range":
            candle_range,

        "body_ratio":
            body_ratio,

        "close_position":
            close_position,

        "momentum":
            momentum
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
# TIMEFRAME STARTS
# ============================================================

def timeframe_starts(
    minutes
):

    if minutes == 1:

        return [
            24,
            25,
            26,
            27,
            28,
            29
        ]

    if minutes == 2:

        return [
            20,
            22,
            24,
            26,
            28
        ]

    if minutes == 3:

        return [
            15,
            18,
            21,
            24,
            27
        ]

    if minutes == 5:

        return [
            10,
            15,
            20,
            25
        ]

    if minutes == 10:

        return [
            0,
            10,
            20
        ]

    if minutes == 15:

        return [
            0,
            15
        ]

    return []


# ============================================================
# BUILD FEATURES
# ============================================================

def build_features(
    day
):

    features = {}

    for timeframe, minutes in (
        TIMEFRAMES.items()
    ):

        starts = timeframe_starts(
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
            continue

        ordered = sorted(
            candles.keys()
        )

        last = candles[
            ordered[-1]
        ]

        previous = candles[
            ordered[-2]
        ]

        last_metrics = candle_metrics(
            last
        )

        previous_metrics = (
            candle_metrics(
                previous
            )
        )

        older = [
            candles[x]
            for x
            in ordered[:-1]
        ]

        if older:

            avg_range = np.mean([
                candle_metrics(
                    x
                )["range"]
                for x in older
            ])

            avg_volume = np.mean([
                x["volume"]
                for x in older
            ])

        else:

            avg_range = 0
            avg_volume = 0

        if avg_range > 0:

            range_ratio = (
                last_metrics["range"]
                /
                avg_range
            )

        else:

            range_ratio = 0

        if avg_volume > 0:

            rel_volume = (
                last["volume"]
                /
                avg_volume
            )

        else:

            rel_volume = 0

        if previous["volume"] > 0:

            volume_ratio = (
                last["volume"]
                /
                previous["volume"]
            )

        else:

            volume_ratio = 0

        last_dir = direction(
            last
        )

        previous_dir = direction(
            previous
        )

        features[
            f"{timeframe}_BULL"
        ] = (
            last_dir == "BULL"
        )

        features[
            f"{timeframe}_BEAR"
        ] = (
            last_dir == "BEAR"
        )

        features[
            f"{timeframe}_PREVIOUS_BULL"
        ] = (
            previous_dir == "BULL"
        )

        features[
            f"{timeframe}_PREVIOUS_BEAR"
        ] = (
            previous_dir == "BEAR"
        )

        features[
            f"{timeframe}_SAME"
        ] = (
            last_dir ==
            previous_dir
            and
            last_dir != "DOJI"
        )

        features[
            f"{timeframe}_OPPOSITE"
        ] = (
            last_dir !=
            previous_dir
            and
            last_dir != "DOJI"
            and
            previous_dir != "DOJI"
        )

        features[
            f"{timeframe}_BODY"
        ] = last_metrics[
            "body_ratio"
        ]

        features[
            f"{timeframe}_CLOSE"
        ] = last_metrics[
            "close_position"
        ]

        features[
            f"{timeframe}_RANGE_RATIO"
        ] = range_ratio

        features[
            f"{timeframe}_REL_VOLUME"
        ] = rel_volume

        features[
            f"{timeframe}_VOLUME_RATIO"
        ] = volume_ratio

        features[
            f"{timeframe}_MOMENTUM"
        ] = last_metrics[
            "momentum"
        ]

        features[
            f"{timeframe}_MOM_CHANGE"
        ] = (
            last_metrics[
                "momentum"
            ]
            -
            previous_metrics[
                "momentum"
            ]
        )

    # ========================================================
    # PREVIOUS-DAY OVERALL MOVE
    # ========================================================

    first_candle = day.get(
        "09:15"
    )

    last_candle = day.get(
        "15:29"
    )

    if (
        first_candle is not None
        and
        last_candle is not None
        and
        first_candle["open"] > 0
    ):

        features[
            "DAY_RETURN"
        ] = (
            (
                last_candle["close"]
                -
                first_candle["open"]
            )
            /
            first_candle["open"]
            *
            100
        )

    else:

        features[
            "DAY_RETURN"
        ] = 0

    # ========================================================
    # PREVIOUS-DAY CLOSE LOCATION
    # ========================================================

    highs = []
    lows = []

    for minute, row in day.items():

        try:

            h = int(
                minute.replace(
                    ":",
                    ""
                )
            )

            # Only regular market data
            if 915 <= h <= 1529:

                highs.append(
                    row["high"]
                )

                lows.append(
                    row["low"]
                )

        except Exception:

            continue

    if (
        highs
        and
        lows
        and
        last_candle is not None
    ):

        day_high = max(
            highs
        )

        day_low = min(
            lows
        )

        day_range = (
            day_high -
            day_low
        )

        if day_range > 0:

            features[
                "DAY_CLOSE_POSITION"
            ] = (
                (
                    last_candle["close"]
                    -
                    day_low
                )
                /
                day_range
            )

        else:

            features[
                "DAY_CLOSE_POSITION"
            ] = 0.5

    else:

        features[
            "DAY_CLOSE_POSITION"
        ] = 0.5

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
        "_1m_csv.gz",
        ""
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
        # FIXED ENTRY / EXIT
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

            "long_return":
                raw_return,

            "short_return":
                -raw_return,

            **features
        })

    return results


# ============================================================
# CREATE COMPLETE DATASET
# ============================================================

def create_dataset():

    raw = download_dataset()

    zip_file = zipfile.ZipFile(
        io.BytesIO(raw)
    )

    files = [
        f
        for f in zip_file.namelist()
        if f.endswith(
            ".csv.gz"
        )
    ]

    print()
    print(
        f"Stocks found: "
        f"{len(files):,}"
    )

    all_rows = []

    for number, filename in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing "
            f"{number}/{len(files)}",
            end=""
        )

        rows = extract_events(
            zip_file,
            filename
        )

        all_rows.extend(
            rows
        )

    print()

    df = pd.DataFrame(
        all_rows
    )

    if df.empty:

        raise RuntimeError(
            "No valid observations."
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

def split_dataset(
    df
):

    dates = sorted(
        df[
            "event_date"
        ]
        .dt
        .normalize()
        .unique()
    )

    n = len(
        dates
    )

    train_index = int(
        n *
        TRAIN_PERCENT
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
# CANDIDATE CONDITIONS
# ============================================================

def generate_conditions(
    df
):

    conditions = {}

    # ========================================================
    # SIMPLE DIRECTIONAL CONDITIONS
    # ========================================================

    for tf in TIMEFRAMES:

        conditions[
            f"{tf}:BULL"
        ] = df[
            f"{tf}_BULL"
        ].astype(bool)

        conditions[
            f"{tf}:BEAR"
        ] = df[
            f"{tf}_BEAR"
        ].astype(bool)

        conditions[
            f"{tf}:PREVIOUS_BULL"
        ] = df[
            f"{tf}_PREVIOUS_BULL"
        ].astype(bool)

        conditions[
            f"{tf}:PREVIOUS_BEAR"
        ] = df[
            f"{tf}_PREVIOUS_BEAR"
        ].astype(bool)

        conditions[
            f"{tf}:SAME"
        ] = df[
            f"{tf}_SAME"
        ].astype(bool)

        conditions[
            f"{tf}:OPPOSITE"
        ] = df[
            f"{tf}_OPPOSITE"
        ].astype(bool)

        # ----------------------------------------------------
        # BODY
        # ----------------------------------------------------

        for level in BODY_LEVELS:

            conditions[
                f"{tf}:BODY>={level:.2f}"
            ] = (
                df[
                    f"{tf}_BODY"
                ]
                >= level
            )

        # ----------------------------------------------------
        # CLOSE
        # ----------------------------------------------------

        for level in CLOSE_LEVELS:

            conditions[
                f"{tf}:CLOSE<={level:.2f}"
            ] = (
                df[
                    f"{tf}_CLOSE"
                ]
                <= level
            )

            conditions[
                f"{tf}:CLOSE>={level:.2f}"
            ] = (
                df[
                    f"{tf}_CLOSE"
                ]
                >= level
            )

        # ----------------------------------------------------
        # RELATIVE VOLUME
        # ----------------------------------------------------

        for level in REL_VOLUME_LEVELS:

            conditions[
                f"{tf}:REL_VOL>={level:.2f}"
            ] = (
                df[
                    f"{tf}_REL_VOLUME"
                ]
                >= level
            )

        # ----------------------------------------------------
        # RANGE
        # ----------------------------------------------------

        for level in RANGE_LEVELS:

            conditions[
                f"{tf}:RANGE<={level:.2f}x"
            ] = (
                df[
                    f"{tf}_RANGE_RATIO"
                ]
                <= level
            )

        # ----------------------------------------------------
        # MOMENTUM
        # ----------------------------------------------------

        for level in MOMENTUM_LEVELS:

            conditions[
                f"{tf}:MOM>={level:.2f}%"
            ] = (
                df[
                    f"{tf}_MOMENTUM"
                ]
                >= level
            )

            conditions[
                f"{tf}:MOM<=-{level:.2f}%"
            ] = (
                df[
                    f"{tf}_MOMENTUM"
                ]
                <= -level
            )

    # ========================================================
    # PREVIOUS-DAY TOTAL MOVE
    # ========================================================

    for level in RELATIVE_STRENGTH_LEVELS:

        conditions[
            f"DAY_RETURN>={level:.2f}%"
        ] = (
            df[
                "DAY_RETURN"
            ]
            >= level
        )

        conditions[
            f"DAY_RETURN<=-{level:.2f}%"
        ] = (
            df[
                "DAY_RETURN"
            ]
            <= -level
        )

    # ========================================================
    # DAY CLOSE LOCATION
    # ========================================================

    for level in CLOSE_LEVELS:

        conditions[
            f"DAY_CLOSE<={level:.2f}"
        ] = (
            df[
                "DAY_CLOSE_POSITION"
            ]
            <= level
        )

        conditions[
            f"DAY_CLOSE>={level:.2f}"
        ] = (
            df[
                "DAY_CLOSE_POSITION"
            ]
            >= level
        )

    # ========================================================
    # CROSS-TIMEFRAME ALIGNMENT
    # ========================================================

    timeframes = list(
        TIMEFRAMES.keys()
    )

    for tf1, tf2 in itertools.combinations(
        timeframes,
        2
    ):

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
            f"{tf1}:BULL + {tf2}:BEAR"
        ] = (
            df[
                f"{tf1}_BULL"
            ]
            &
            df[
                f"{tf2}_BEAR"
            ]
        )

        conditions[
            f"{tf1}:BEAR + {tf2}:BULL"
        ] = (
            df[
                f"{tf1}_BEAR"
            ]
            &
            df[
                f"{tf2}_BULL"
            ]
        )

    return conditions


# ============================================================
# RETURN STATISTICS
# ============================================================

def get_stats(
    returns
):

    returns = pd.Series(
        returns
    ).dropna()

    if len(returns) == 0:
        return None

    net_returns = (
        returns
        -
        ROUND_TRIP_COST
    )

    wins = (
        net_returns > 0
    ).sum()

    win_rate = (
        wins /
        len(net_returns)
        *
        100
    )

    gross_profit = (
        net_returns[
            net_returns > 0
        ].sum()
    )

    gross_loss = abs(
        net_returns[
            net_returns < 0
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
            len(net_returns),

        "win_rate":
            win_rate,

        "average":
            net_returns.mean(),

        "profit_factor":
            pf,

        "total":
            net_returns.sum()
    }


# ============================================================
# EVALUATE BOTH DIRECTIONS
# ============================================================

def evaluate_direction(
    df,
    mask
):

    subset = df[
        mask
    ]

    if len(subset) < MIN_TRAIN_TRADES:
        return None

    long_stats = get_stats(
        subset[
            "long_return"
        ]
    )

    short_stats = get_stats(
        subset[
            "short_return"
        ]
    )

    # --------------------------------------------------------
    # Direction is selected ONLY from training data.
    # --------------------------------------------------------

    if (
        long_stats["win_rate"]
        >=
        short_stats["win_rate"]
    ):

        direction_name = "LONG"

        selected = long_stats

    else:

        direction_name = "SHORT"

        selected = short_stats

    return {

        "direction":
            direction_name,

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
# SELECT SIMPLE TRAINING CANDIDATES
# ============================================================

def rank_candidates(
    df,
    conditions
):

    rows = []

    for name, mask in (
        conditions.items()
    ):

        result = (
            evaluate_direction(
                df,
                mask
            )
        )

        if result is None:
            continue

        rows.append({

            "pattern":
                name,

            **result
        })

    if not rows:
        return []

    frame = pd.DataFrame(
        rows
    )

    # Prefer win rate, but reward profitability too.
    frame[
        "score"
    ] = (
        frame[
            "win_rate"
        ]
        +
        8 *
        np.log1p(
            frame[
                "profit_factor"
            ]
        )
    )

    return (
        frame
        .sort_values(
            [
                "score",
                "win_rate",
                "profit_factor"
            ],
            ascending=False
        )
        .head(250)
        [
            "pattern"
        ]
        .tolist()
    )


# ============================================================
# COMBINE CONDITIONS
# ============================================================

def search_patterns(
    df,
    conditions
):

    masks = {
        name:
            pd.Series(
                mask,
                index=df.index
            ).fillna(False).astype(bool)
        for name, mask
        in conditions.items()
    }

    atomic_candidates = rank_candidates(
        df,
        masks
    )

    print(
        f"Atomic candidates retained: "
        f"{len(atomic_candidates):,}"
    )

    results = []

    # ========================================================
    # SINGLE CONDITION SEARCH
    # ========================================================

    depth1 = []

    for name in atomic_candidates:

        result = evaluate_direction(
            df,
            masks[
                name
            ]
        )

        if result is None:
            continue

        row = {

            "depth":
                1,

            "pattern":
                name,

            "_mask":
                masks[
                    name
                ],

            **result
        }

        depth1.append(
            row
        )

        results.append(
            {
                key: value
                for key, value
                in row.items()
                if key != "_mask"
            }
        )

    # ========================================================
    # DEPTH 2
    # ========================================================

    depth2 = []

    checked = 0

    for a, b in itertools.combinations(
        atomic_candidates,
        2
    ):

        checked += 1

        combined = (
            masks[a]
            &
            masks[b]
        )

        result = evaluate_direction(
            df,
            combined
        )

        if result is None:
            continue

        pattern = (
            a +
            " + " +
            b
        )

        row = {

            "depth":
                2,

            "pattern":
                pattern,

            "_mask":
                combined,

            **result
        }

        depth2.append(
            row
        )

    print(
        f"Depth 2 checked: "
        f"{checked:,}"
    )

    print(
        f"Depth 2 qualifying: "
        f"{len(depth2):,}"
    )

    # Keep only strongest combinations
    if depth2:

        temp = pd.DataFrame([
            {
                key: value
                for key, value
                in row.items()
                if key != "_mask"
            }
            for row in depth2
        ])

        temp[
            "score"
        ] = (
            temp[
                "win_rate"
            ]
            +
            8 *
            np.log1p(
                temp[
                    "profit_factor"
                ]
            )
        )

        keep_names = set(
            temp
            .sort_values(
                "score",
                ascending=False
            )
            .head(200)
            [
                "pattern"
            ]
            .tolist()
        )

        depth2 = [
            row
            for row in depth2
            if row[
                "pattern"
            ] in keep_names
        ]

    # Add clean results
    for row in depth2:

        results.append(
            {
                key: value
                for key, value
                in row.items()
                if key != "_mask"
            }
        )

    # ========================================================
    # DEPTH 3
    # ========================================================

    depth3 = []

    checked = 0

    for previous in depth2:

        previous_name = previous[
            "pattern"
        ]

        previous_mask = previous[
            "_mask"
        ]

        previous_parts = set(
            previous_name.split(
                " + "
            )
        )

        for atomic_name in (
            atomic_candidates
        ):

            if (
                atomic_name
                in previous_parts
            ):
                continue

            checked += 1

            combined = (
                previous_mask
                &
                masks[
                    atomic_name
                ]
            )

            result = evaluate_direction(
                df,
                combined
            )

            if result is None:
                continue

            pattern = (
                previous_name
                +
                " + "
                +
                atomic_name
            )

            depth3.append({

                "depth":
                    3,

                "pattern":
                    pattern,

                "_mask":
                    combined,

                **result
            })

    print(
        f"Depth 3 checked: "
        f"{checked:,}"
    )

    print(
        f"Depth 3 qualifying: "
        f"{len(depth3):,}"
    )

    # Keep strongest depth 3
    if depth3:

        temp = pd.DataFrame([
            {
                key: value
                for key, value
                in row.items()
                if key != "_mask"
            }
            for row in depth3
        ])

        temp[
            "score"
        ] = (
            temp[
                "win_rate"
            ]
            +
            8 *
            np.log1p(
                temp[
                    "profit_factor"
                ]
            )
        )

        keep_names = set(
            temp
            .sort_values(
                "score",
                ascending=False
            )
            .head(250)
            [
                "pattern"
            ]
            .tolist()
        )

        depth3 = [
            row
            for row in depth3
            if row[
                "pattern"
            ] in keep_names
        ]

    for row in depth3:

        results.append(
            {
                key: value
                for key, value
                in row.items()
                if key != "_mask"
            }
        )

    return pd.DataFrame(
        results
    )


# ============================================================
# BUILD MASK FROM PATTERN
# ============================================================

def build_mask_from_pattern(
    df,
    pattern
):

    conditions = generate_conditions(
        df
    )

    parts = [
        part.strip()
        for part
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
            .astype(bool)
        )

    return mask


# ============================================================
# VALIDATE TOP TRAINING PATTERNS
# ============================================================

def validate_patterns(
    train,
    validation,
    test,
    training_patterns
):

    # --------------------------------------------------------
    # Select candidates using TRAINING ONLY.
    # --------------------------------------------------------

    candidates = set()

    for metric in [
        "win_rate",
        "profit_factor",
        "average"
    ]:

        top = (
            training_patterns
            .sort_values(
                metric,
                ascending=False
            )
            .head(300)
        )

        candidates.update(
            top[
                "pattern"
            ].tolist()
        )

    rows = []

    for pattern in candidates:

        train_mask = (
            build_mask_from_pattern(
                train,
                pattern
            )
        )

        validation_mask = (
            build_mask_from_pattern(
                validation,
                pattern
            )
        )

        test_mask = (
            build_mask_from_pattern(
                test,
                pattern
            )
        )

        if (
            train_mask is None
            or
            validation_mask is None
            or
            test_mask is None
        ):
            continue

        train_result = (
            evaluate_direction(
                train,
                train_mask
            )
        )

        if train_result is None:
            continue

        direction_name = (
            train_result[
                "direction"
            ]
        )

        validation_subset = (
            validation[
                validation_mask
            ]
        )

        if (
            len(
                validation_subset
            )
            <
            MIN_VALIDATION_TRADES
        ):
            continue

        test_subset = (
            test[
                test_mask
            ]
        )

        if (
            len(
                test_subset
            )
            <
            MIN_TEST_TRADES
        ):
            continue

        # ----------------------------------------------------
        # Use training-selected direction.
        # ----------------------------------------------------

        if direction_name == "LONG":

            validation_returns = (
                validation_subset[
                    "long_return"
                ]
            )

            test_returns = (
                test_subset[
                    "long_return"
                ]
            )

        else:

            validation_returns = (
                validation_subset[
                    "short_return"
                ]
            )

            test_returns = (
                test_subset[
                    "short_return"
                ]
            )

        validation_stats = get_stats(
            validation_returns
        )

        test_stats = get_stats(
            test_returns
        )

        if (
            validation_stats is None
            or
            test_stats is None
        ):
            continue

        rows.append({

            "pattern":
                pattern,

            "direction":
                direction_name,

            "train_trades":
                train_result[
                    "trades"
                ],

            "train_win":
                train_result[
                    "win_rate"
                ],

            "train_pf":
                train_result[
                    "profit_factor"
                ],

            "validation_trades":
                validation_stats[
                    "trades"
                ],

            "validation_win":
                validation_stats[
                    "win_rate"
                ],

            "validation_pf":
                validation_stats[
                    "profit_factor"
                ],

            "validation_avg":
                validation_stats[
                    "average"
                ],

            "test_trades":
                test_stats[
                    "trades"
                ],

            "test_win":
                test_stats[
                    "win_rate"
                ],

            "test_pf":
                test_stats[
                    "profit_factor"
                ],

            "test_avg":
                test_stats[
                    "average"
                ],

            "test_total":
                test_stats[
                    "total"
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
    direction_name
):

    mask = (
        build_mask_from_pattern(
            test,
            pattern
        )
    )

    if mask is None:
        return pd.DataFrame()

    subset = test[
        mask
    ].copy()

    if subset.empty:
        return pd.DataFrame()

    subset[
        "year"
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
            "year"
        )
    ):

        if direction_name == "LONG":

            returns = group[
                "long_return"
            ]

        else:

            returns = group[
                "short_return"
            ]

        stats = get_stats(
            returns
        )

        if stats is None:
            continue

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
# MAIN
# ============================================================

def main():

    print()
    print("=" * 90)
    print(
        "HIGH-WIN-RATE SIMPLE STRATEGY SEARCH"
    )
    print("=" * 90)

    print()
    print(
        "FIXED ENTRY : NEXT DAY 09:15 OPEN"
    )

    print(
        "FIXED EXIT  : NEXT DAY 15:27 OPEN"
    )

    print()
    print(
        "NO INFORMATION AFTER 09:15 "
        "IS USED TO DECIDE ENTRY."
    )

    print()
    print(
        "Target win rate: "
        f"{TARGET_WIN_RATE:.0f}%"
    )

    print()
    print(
        "Logic is intentionally kept simple."
    )

    # ========================================================
    # LOAD DATA
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
    ) = split_dataset(
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
    # GENERATE CONDITIONS
    # ========================================================

    print()
    print(
        "Generating simple conditions..."
    )

    conditions = generate_conditions(
        train
    )

    print(
        f"Conditions generated: "
        f"{len(conditions):,}"
    )

    # ========================================================
    # TRAIN SEARCH
    # ========================================================

    print()
    print("=" * 90)
    print(
        "SEARCHING TRAINING DATA"
    )
    print("=" * 90)

    training_patterns = search_patterns(
        train,
        conditions
    )

    if training_patterns.empty:

        print(
            "No qualifying patterns."
        )

        return

    # ========================================================
    # SAVE TRAINING
    # ========================================================

    training_patterns.to_csv(
        "SIMPLE_85_TRAINING_PATTERNS.csv",
        index=False
    )

    # ========================================================
    # TOP TRAINING
    # ========================================================

    print()
    print("=" * 90)
    print(
        "TOP TRAINING PATTERNS"
    )
    print("=" * 90)

    print(
        training_patterns
        .sort_values(
            [
                "win_rate",
                "profit_factor"
            ],
            ascending=False
        )
        .head(50)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # VALIDATE
    # ========================================================

    print()
    print("=" * 90)
    print(
        "VALIDATING TOP PATTERNS"
    )
    print("=" * 90)

    final_results = validate_patterns(
        train,
        validation,
        test,
        training_patterns
    )

    if final_results.empty:

        print(
            "NO PATTERNS SURVIVED VALIDATION."
        )

        return

    final_results = (
        final_results
        .sort_values(
            [
                "test_win",
                "test_pf",
                "test_avg"
            ],
            ascending=False
        )
        .reset_index(
            drop=True
        )
    )

    final_results.to_csv(
        "SIMPLE_85_FINAL_TEST.csv",
        index=False
    )

    # ========================================================
    # FINAL TEST
    # ========================================================

    print()
    print("=" * 90)
    print(
        "FINAL UNSEEN TEST"
    )
    print("=" * 90)

    print(
        final_results
        .head(50)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # 70%
    # ========================================================

    for threshold in [
        70,
        75,
        80,
        85
    ]:

        strong = final_results[
            final_results[
                "test_win"
            ]
            >= threshold
        ]

        print()
        print("=" * 90)
        print(
            f"FINAL TEST >= {threshold}%"
        )
        print("=" * 90)

        if strong.empty:

            print(
                "NONE"
            )

        else:

            print(
                strong
                .head(50)
                .to_string(
                    index=False
                )
            )

    # ========================================================
    # YEAR STABILITY
    # ========================================================

    print()
    print("=" * 90)
    print(
        "YEAR-BY-YEAR STABILITY"
    )
    print("=" * 90)

    for _, row in (
        final_results
        .head(10)
        .iterrows()
    ):

        print()
        print(
            row[
                "pattern"
            ]
        )

        print(
            "Direction:",
            row[
                "direction"
            ]
        )

        yearly = (
            year_stability(
                test,
                row[
                    "pattern"
                ],
                row[
                    "direction"
                ]
            )
        )

        if yearly.empty:

            print(
                "No yearly results."
            )

        else:

            print(
                yearly
                .to_string(
                    index=False
                )
            )

    # ========================================================
    # 85% DECISION
    # ========================================================

    candidates_85 = final_results[
        final_results[
            "test_win"
        ]
        >= 85
    ]

    print()
    print("=" * 90)
    print(
        "85% FINAL TEST DECISION"
    )
    print("=" * 90)

    if candidates_85.empty:

        print(
            "NO 85%+ STRATEGY SURVIVED "
            "THE FINAL UNSEEN TEST."
        )

    else:

        print(
            "85%+ STRATEGIES FOUND:"
        )

        print(
            candidates_85
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
        "SEARCH COMPLETE"
    )
    print("=" * 90)

    print()
    print(
        "Files:"
    )

    print(
        "SIMPLE_85_TRAINING_PATTERNS.csv"
    )

    print(
        "SIMPLE_85_FINAL_TEST.csv"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
