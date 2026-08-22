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
# FULL EXHAUSTIVE EOD PATTERN SEARCH
# ============================================================
#
# Signal:
#   PREVIOUS TRADING DAY EOD
#
# Outcome:
#   FOLLOWING DAY
#
# Trade:
#   09:15 OPEN -> 15:27 OPEN
#
# Target:
#   TOP DAILY GAINERS / LOSERS
#
# Timeframes:
#   1m / 2m / 3m / 5m / 10m / 15m
#
# IMPORTANT:
#   The final test set is NEVER used while discovering patterns.
#
# ============================================================


# ============================================================
# SETTINGS
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/nse-fno-1min-data/"
    "releases/download/v1.0.0/stocks_1m_csvs.zip"
)

TRAIN_PERCENT = 0.60
VALIDATION_PERCENT = 0.20
TEST_PERCENT = 0.20

TOP_RANKS = [5, 10, 20, 50]

TIMEFRAMES = {
    "1m": 1,
    "2m": 2,
    "3m": 3,
    "5m": 5,
    "10m": 10,
    "15m": 15
}


# Minimum observations
MIN_TRAIN = 200
MIN_VALIDATION = 50
MIN_TEST = 50


# Search depth.
#
# 1 = single conditions
# 2 = pairs
# 3 = triples
# 4 = quadruples
#
# We go through 4, but the search is aggressively
# pruned so the computer does not attempt millions
# of useless combinations.
MAX_COMBINATION_DEPTH = 4


# Candidate thresholds
BODY_LEVELS = [
    0.30,
    0.40,
    0.50,
    0.60,
    0.70,
    0.80,
    0.90
]

VOLUME_LEVELS = [
    0.75,
    1.00,
    1.25,
    1.50,
    1.75,
    2.00,
    2.50,
    3.00,
    4.00
]

RANGE_LEVELS = [
    0.75,
    1.00,
    1.25,
    1.50,
    1.75,
    2.00,
    2.50,
    3.00
]

MOMENTUM_LEVELS = [
    0.03,
    0.05,
    0.10,
    0.15,
    0.20,
    0.30,
    0.50,
    0.75,
    1.00
]

CLOSE_LEVELS = [
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

WICK_LEVELS = [
    0.10,
    0.20,
    0.30,
    0.40,
    0.50,
    0.60
]


# ============================================================
# DOWNLOAD
# ============================================================

def download_dataset():

    print("=" * 90)
    print("DOWNLOADING NSE DATA")
    print("=" * 90)

    r = requests.get(
        DATA_URL,
        stream=True,
        timeout=600
    )

    r.raise_for_status()

    total = int(
        r.headers.get(
            "content-length",
            0
        )
    )

    downloaded = 0
    chunks = []

    for chunk in r.iter_content(
        chunk_size=1024 * 1024
    ):

        if chunk:

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

def load_stock(z, filename):

    try:

        raw = z.read(filename)

        df = pd.read_csv(
            io.BytesIO(raw),
            compression="gzip"
        )

        if df.empty:
            return None

        if "time" not in df.columns:
            return None

        df["datetime"] = pd.to_datetime(
            df["time"],
            unit="s",
            utc=True
        ).dt.tz_convert(
            "Asia/Kolkata"
        )

        df["date"] = (
            df["datetime"]
            .dt.strftime("%Y-%m-%d")
        )

        df["hm"] = (
            df["datetime"]
            .dt.strftime("%H:%M")
        )

        for col in [
            "open",
            "high",
            "low",
            "close"
        ]:

            if col not in df.columns:
                return None

            df[col] = pd.to_numeric(
                df[col],
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
# GENERIC CANDLE
# ============================================================

def build_candle(
    day,
    start_minute,
    minutes
):

    rows = []

    for i in range(minutes):

        minute = start_minute + i

        timestamp = f"15:{minute:02d}"

        if timestamp not in day:

            return None

        rows.append(
            day[timestamp]
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

def candle_metrics(c):

    if c is None:
        return None

    rng = c["high"] - c["low"]

    body = abs(
        c["close"] -
        c["open"]
    )

    upper_wick = (
        c["high"] -
        max(
            c["open"],
            c["close"]
        )
    )

    lower_wick = (
        min(
            c["open"],
            c["close"]
        ) -
        c["low"]
    )

    if rng > 0:

        body_ratio = body / rng

        upper_wick_ratio = (
            upper_wick / rng
        )

        lower_wick_ratio = (
            lower_wick / rng
        )

        close_position = (
            c["close"] -
            c["low"]
        ) / rng

    else:

        body_ratio = 0
        upper_wick_ratio = 0
        lower_wick_ratio = 0
        close_position = 0.5

    if c["open"] > 0:

        momentum = (
            (
                c["close"] -
                c["open"]
            )
            /
            c["open"]
            *
            100
        )

    else:

        momentum = 0

    return {

        "range":
            rng,

        "body":
            body,

        "body_ratio":
            body_ratio,

        "upper_wick_ratio":
            upper_wick_ratio,

        "lower_wick_ratio":
            lower_wick_ratio,

        "close_position":
            close_position,

        "momentum":
            momentum
    }


# ============================================================
# DIRECTION
# ============================================================

def candle_direction(c):

    if c["close"] > c["open"]:
        return "BULL"

    if c["close"] < c["open"]:
        return "BEAR"

    return "DOJI"


# ============================================================
# EOD CANDLE DEFINITIONS
# ============================================================

def get_eod_candles(
    day,
    minutes
):

    if minutes == 1:

        starts = [
            24,
            25,
            26,
            27,
            28,
            29
        ]

    elif minutes == 2:

        starts = [
            19,
            21,
            23,
            25,
            27
        ]

    elif minutes == 3:

        starts = [
            15,
            18,
            21,
            24,
            27
        ]

    elif minutes == 5:

        starts = [
            10,
            15,
            20,
            25
        ]

    elif minutes == 10:

        starts = [
            0,
            10,
            20
        ]

    elif minutes == 15:

        starts = [
            0,
            15
        ]

    else:

        return {}

    result = {}

    for start in starts:

        candle = build_candle(
            day,
            start,
            minutes
        )

        if candle is not None:

            result[start] = candle

    return result


# ============================================================
# BUILD COMPLETE EOD FEATURE SET
# ============================================================

def build_features(day):

    features = {}

    for tf, minutes in TIMEFRAMES.items():

        candles = get_eod_candles(
            day,
            minutes
        )

        if len(candles) < 2:

            continue

        ordered = sorted(
            candles.keys()
        )

        last_start = ordered[-1]
        second_start = ordered[-2]

        last = candles[
            last_start
        ]

        second = candles[
            second_start
        ]

        last_m = candle_metrics(
            last
        )

        second_m = candle_metrics(
            second
        )

        # Previous candles for averages
        older = [
            candles[x]
            for x in ordered[:-1]
        ]

        if older:

            older_ranges = [
                candle_metrics(x)["range"]
                for x in older
            ]

            older_volumes = [
                x["volume"]
                for x in older
            ]

            avg_range = np.mean(
                older_ranges
            )

            avg_volume = np.mean(
                older_volumes
            )

        else:

            avg_range = 0
            avg_volume = 0

        prefix = tf

        last_direction = (
            candle_direction(last)
        )

        second_direction = (
            candle_direction(second)
        )

        # ----------------------------------------------------
        # Direction
        # ----------------------------------------------------

        features[
            f"{prefix}_LAST_BEAR"
        ] = (
            last_direction == "BEAR"
        )

        features[
            f"{prefix}_LAST_BULL"
        ] = (
            last_direction == "BULL"
        )

        features[
            f"{prefix}_SECOND_BEAR"
        ] = (
            second_direction == "BEAR"
        )

        features[
            f"{prefix}_SECOND_BULL"
        ] = (
            second_direction == "BULL"
        )

        features[
            f"{prefix}_SAME"
        ] = (
            last_direction ==
            second_direction
            and
            last_direction != "DOJI"
        )

        features[
            f"{prefix}_OPPOSITE"
        ] = (
            last_direction !=
            second_direction
            and
            last_direction != "DOJI"
            and
            second_direction != "DOJI"
        )

        # ----------------------------------------------------
        # Candle metrics
        # ----------------------------------------------------

        features[
            f"{prefix}_BODY"
        ] = last_m[
            "body_ratio"
        ]

        features[
            f"{prefix}_UPPER_WICK"
        ] = last_m[
            "upper_wick_ratio"
        ]

        features[
            f"{prefix}_LOWER_WICK"
        ] = last_m[
            "lower_wick_ratio"
        ]

        features[
            f"{prefix}_CLOSE_POSITION"
        ] = last_m[
            "close_position"
        ]

        features[
            f"{prefix}_RANGE"
        ] = last_m[
            "range"
        ]

        features[
            f"{prefix}_RANGE_RATIO"
        ] = (
            last_m["range"] /
            avg_range
            if avg_range > 0
            else 0
        )

        features[
            f"{prefix}_MOMENTUM"
        ] = last_m[
            "momentum"
        ]

        # ----------------------------------------------------
        # Volume
        # ----------------------------------------------------

        features[
            f"{prefix}_REL_VOLUME"
        ] = (
            last["volume"] /
            avg_volume
            if avg_volume > 0
            else 0
        )

        features[
            f"{prefix}_VOLUME_RATIO"
        ] = (
            last["volume"] /
            second["volume"]
            if second["volume"] > 0
            else 0
        )

        # ----------------------------------------------------
        # Change from second-last candle
        # ----------------------------------------------------

        features[
            f"{prefix}_BODY_CHANGE"
        ] = (
            last_m["body_ratio"] -
            second_m["body_ratio"]
        )

        features[
            f"{prefix}_RANGE_CHANGE"
        ] = (
            (
                last_m["range"] /
                second_m["range"]
            )
            if second_m["range"] > 0
            else 0
        )

        features[
            f"{prefix}_MOMENTUM_CHANGE"
        ] = (
            last_m["momentum"] -
            second_m["momentum"]
        )

        # ----------------------------------------------------
        # Candle-to-candle price relationship
        # ----------------------------------------------------

        if second["close"] > 0:

            features[
                f"{prefix}_PRICE_CHANGE"
            ] = (
                (
                    last["close"] -
                    second["close"]
                )
                /
                second["close"]
                *
                100
            )

        else:

            features[
                f"{prefix}_PRICE_CHANGE"
            ] = 0

    return features


# ============================================================
# EXTRACT PREVIOUS-DAY SIGNAL / NEXT-DAY OUTCOME
# ============================================================

def extract_stock_events(
    z,
    filename
):

    df = load_stock(
        z,
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

    symbol = os.path.basename(
        filename
    ).replace(
        "_1m.csv.gz",
        ""
    )

    rows = []

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

        signal_day = make_day_lookup(
            grouped[
                signal_date
            ]
        )

        event_day = make_day_lookup(
            grouped[
                event_date
            ]
        )

        # ----------------------------------------------------
        # Need next-day entry and exit
        # ----------------------------------------------------

        if "09:15" not in event_day:
            continue

        if "15:27" not in event_day:
            continue

        entry = event_day[
            "09:15"
        ]["open"]

        exit_price = event_day[
            "15:27"
        ]["open"]

        if entry <= 0:
            continue

        # ----------------------------------------------------
        # Previous-day features
        # ----------------------------------------------------

        features = build_features(
            signal_day
        )

        if not features:
            continue

        event_return = (
            (
                exit_price -
                entry
            )
            /
            entry
            *
            100
        )

        rows.append({

            "symbol":
                symbol,

            "signal_date":
                signal_date,

            "event_date":
                event_date,

            "event_return":
                event_return,

            "long_return":
                event_return,

            "short_return":
                -event_return,

            **features
        })

    return rows


# ============================================================
# DAILY EXTREME RANKING
# ============================================================

def classify_events(
    df,
    top_n
):

    data = df.copy()

    data["gain_rank"] = (
        data.groupby(
            "event_date"
        )["event_return"]
        .rank(
            method="first",
            ascending=False
        )
    )

    data["loss_rank"] = (
        data.groupby(
            "event_date"
        )["event_return"]
        .rank(
            method="first",
            ascending=True
        )
    )

    gainers = data[
        data["gain_rank"] <= top_n
    ].copy()

    losers = data[
        data["loss_rank"] <= top_n
    ].copy()

    gainers["target"] = "GAINER"

    losers["target"] = "LOSER"

    return pd.concat(
        [
            gainers,
            losers
        ],
        ignore_index=True
    )


# ============================================================
# FEATURE CONDITIONS
# ============================================================

def generate_atomic_conditions(
    df
):

    conditions = {}

    for tf in TIMEFRAMES:

        # ----------------------------------------------------
        # Direction
        # ----------------------------------------------------

        conditions[
            f"{tf}:LAST_BEAR"
        ] = df[
            f"{tf}_LAST_BEAR"
        ]

        conditions[
            f"{tf}:LAST_BULL"
        ] = df[
            f"{tf}_LAST_BULL"
        ]

        conditions[
            f"{tf}:SECOND_BEAR"
        ] = df[
            f"{tf}_SECOND_BEAR"
        ]

        conditions[
            f"{tf}:SECOND_BULL"
        ] = df[
            f"{tf}_SECOND_BULL"
        ]

        conditions[
            f"{tf}:SAME"
        ] = df[
            f"{tf}_SAME"
        ]

        conditions[
            f"{tf}:OPPOSITE"
        ] = df[
            f"{tf}_OPPOSITE"
        ]

        # ----------------------------------------------------
        # Body
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

            conditions[
                f"{tf}:BODY<={1-level:.2f}"
            ] = (
                df[
                    f"{tf}_BODY"
                ]
                <= 1 - level
            )

        # ----------------------------------------------------
        # Upper wick
        # ----------------------------------------------------

        for level in WICK_LEVELS:

            conditions[
                f"{tf}:UPPER_WICK>={level:.2f}"
            ] = (
                df[
                    f"{tf}_UPPER_WICK"
                ]
                >= level
            )

            conditions[
                f"{tf}:UPPER_WICK<={level:.2f}"
            ] = (
                df[
                    f"{tf}_UPPER_WICK"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Lower wick
        # ----------------------------------------------------

        for level in WICK_LEVELS:

            conditions[
                f"{tf}:LOWER_WICK>={level:.2f}"
            ] = (
                df[
                    f"{tf}_LOWER_WICK"
                ]
                >= level
            )

            conditions[
                f"{tf}:LOWER_WICK<={level:.2f}"
            ] = (
                df[
                    f"{tf}_LOWER_WICK"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Close position
        # ----------------------------------------------------

        for level in CLOSE_LEVELS:

            conditions[
                f"{tf}:CLOSE_POS>={level:.2f}"
            ] = (
                df[
                    f"{tf}_CLOSE_POSITION"
                ]
                >= level
            )

            conditions[
                f"{tf}:CLOSE_POS<={level:.2f}"
            ] = (
                df[
                    f"{tf}_CLOSE_POSITION"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Relative volume
        # ----------------------------------------------------

        for level in VOLUME_LEVELS:

            conditions[
                f"{tf}:REL_VOL>={level:.2f}"
            ] = (
                df[
                    f"{tf}_REL_VOLUME"
                ]
                >= level
            )

            conditions[
                f"{tf}:REL_VOL<={level:.2f}"
            ] = (
                df[
                    f"{tf}_REL_VOLUME"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Volume vs previous candle
        # ----------------------------------------------------

        for level in VOLUME_LEVELS:

            conditions[
                f"{tf}:VOL_RATIO>={level:.2f}"
            ] = (
                df[
                    f"{tf}_VOLUME_RATIO"
                ]
                >= level
            )

            conditions[
                f"{tf}:VOL_RATIO<={level:.2f}"
            ] = (
                df[
                    f"{tf}_VOLUME_RATIO"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Range ratio
        # ----------------------------------------------------

        for level in RANGE_LEVELS:

            conditions[
                f"{tf}:RANGE_RATIO>={level:.2f}"
            ] = (
                df[
                    f"{tf}_RANGE_RATIO"
                ]
                >= level
            )

            conditions[
                f"{tf}:RANGE_RATIO<={level:.2f}"
            ] = (
                df[
                    f"{tf}_RANGE_RATIO"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Momentum
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
                f"{tf}:MOM_CHANGE>={level:.2f}"
            ] = (
                df[
                    f"{tf}_MOMENTUM_CHANGE"
                ]
                >= level
            )

            conditions[
                f"{tf}:MOM_CHANGE<=-{level:.2f}"
            ] = (
                df[
                    f"{tf}_MOMENTUM_CHANGE"
                ]
                <= -level
            )

        # ----------------------------------------------------
        # Range change
        # ----------------------------------------------------

        for level in [
            1.25,
            1.50,
            2.00,
            2.50
        ]:

            conditions[
                f"{tf}:RANGE_CHANGE>={level:.2f}"
            ] = (
                df[
                    f"{tf}_RANGE_CHANGE"
                ]
                >= level
            )

        # ----------------------------------------------------
        # Price change between final candles
        # ----------------------------------------------------

        for level in [
            0.05,
            0.10,
            0.20,
            0.30,
            0.50,
            1.00
        ]:

            conditions[
                f"{tf}:PRICE_CHANGE>={level:.2f}%"
            ] = (
                df[
                    f"{tf}_PRICE_CHANGE"
                ]
                >= level
            )

            conditions[
                f"{tf}:PRICE_CHANGE<=-{level:.2f}%"
            ] = (
                df[
                    f"{tf}_PRICE_CHANGE"
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

    tfs = list(
        TIMEFRAMES.keys()
    )

    # --------------------------------------------------------
    # Every pair
    # --------------------------------------------------------

    for tf1, tf2 in itertools.combinations(
        tfs,
        2
    ):

        # Same direction
        conditions[
            f"{tf1}:BEAR + {tf2}:BEAR"
        ] = (
            df[
                f"{tf1}_LAST_BEAR"
            ]
            &
            df[
                f"{tf2}_LAST_BEAR"
            ]
        )

        conditions[
            f"{tf1}:BULL + {tf2}:BULL"
        ] = (
            df[
                f"{tf1}_LAST_BULL"
            ]
            &
            df[
                f"{tf2}_LAST_BULL"
            ]
        )

        # Opposite
        conditions[
            f"{tf1}:BEAR + {tf2}:BULL"
        ] = (
            df[
                f"{tf1}_LAST_BEAR"
            ]
            &
            df[
                f"{tf2}_LAST_BULL"
            ]
        )

        conditions[
            f"{tf1}:BULL + {tf2}:BEAR"
        ] = (
            df[
                f"{tf1}_LAST_BULL"
            ]
            &
            df[
                f"{tf2}_LAST_BEAR"
            ]
        )

        # Both same-candle structures
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

    # --------------------------------------------------------
    # All timeframe alignment
    # --------------------------------------------------------

    bear_masks = [
        df[
            f"{tf}_LAST_BEAR"
        ]
        for tf in tfs
    ]

    bull_masks = [
        df[
            f"{tf}_LAST_BULL"
        ]
        for tf in tfs
    ]

    conditions[
        "ALL_6_TIMEFRAMES_BEAR"
    ] = np.logical_and.reduce(
        bear_masks
    )

    conditions[
        "ALL_6_TIMEFRAMES_BULL"
    ] = np.logical_and.reduce(
        bull_masks
    )

    # --------------------------------------------------------
    # Majority alignment
    # --------------------------------------------------------

    bear_count = sum(
        bear_masks
    )

    bull_count = sum(
        bull_masks
    )

    conditions[
        "5_OF_6_TIMEFRAMES_BEAR"
    ] = (
        bear_count >= 5
    )

    conditions[
        "4_OF_6_TIMEFRAMES_BEAR"
    ] = (
        bear_count >= 4
    )

    conditions[
        "5_OF_6_TIMEFRAMES_BULL"
    ] = (
        bull_count >= 5
    )

    conditions[
        "4_OF_6_TIMEFRAMES_BULL"
    ] = (
        bull_count >= 4
    )

    return conditions


# ============================================================
# TRIPLE TIMEFRAME ALIGNMENT
# ============================================================

def generate_triple_conditions(
    df
):

    conditions = {}

    tfs = list(
        TIMEFRAMES.keys()
    )

    for tf1, tf2, tf3 in itertools.combinations(
        tfs,
        3
    ):

        conditions[
            (
                f"{tf1}:BEAR + "
                f"{tf2}:BEAR + "
                f"{tf3}:BEAR"
            )
        ] = (
            df[
                f"{tf1}_LAST_BEAR"
            ]
            &
            df[
                f"{tf2}_LAST_BEAR"
            ]
            &
            df[
                f"{tf3}_LAST_BEAR"
            ]
        )

        conditions[
            (
                f"{tf1}:BULL + "
                f"{tf2}:BULL + "
                f"{tf3}:BULL"
            )
        ] = (
            df[
                f"{tf1}_LAST_BULL"
            ]
            &
            df[
                f"{tf2}_LAST_BULL"
            ]
            &
            df[
                f"{tf3}_LAST_BULL"
            ]
        )

    return conditions


# ============================================================
# TARGET-SPECIFIC CONDITIONS
# ============================================================

def generate_target_conditions(
    df
):

    conditions = {}

    # --------------------------------------------------------
    # Extreme magnitude of previous day's move
    # --------------------------------------------------------

    conditions[
        "PREV_DAY_RETURN<=-0.5%"
    ] = (
        df["event_return"]
        <= -0.5
    )

    conditions[
        "PREV_DAY_RETURN<=-1.0%"
    ] = (
        df["event_return"]
        <= -1.0
    )

    conditions[
        "PREV_DAY_RETURN>=0.5%"
    ] = (
        df["event_return"]
        >= 0.5
    )

    conditions[
        "PREV_DAY_RETURN>=1.0%"
    ] = (
        df["event_return"]
        >= 1.0
    )

    return conditions


# ============================================================
# STATISTICS
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
        wins /
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
            gross_profit /
            gross_loss
        )

    else:

        pf = np.inf

    return {

        "n":
            len(returns),

        "win_rate":
            win_rate,

        "average":
            returns.mean(),

        "pf":
            pf,

        "total":
            returns.sum()
    }


# ============================================================
# EVALUATE A CONDITION
# ============================================================

def evaluate_condition(
    df,
    mask
):

    subset = df[
        mask
    ]

    if len(subset) < MIN_TRAIN:

        return None

    long_stats = calculate_stats(
        subset[
            "long_return"
        ]
    )

    short_stats = calculate_stats(
        subset[
            "short_return"
        ]
    )

    if (
        long_stats["win_rate"]
        >=
        short_stats["win_rate"]
    ):

        direction = "LONG"

        stats = long_stats

    else:

        direction = "SHORT"

        stats = short_stats

    return {

        "direction":
            direction,

        "n":
            stats["n"],

        "win_rate":
            stats["win_rate"],

        "average":
            stats["average"],

        "pf":
            stats["pf"],

        "total":
            stats["total"]
    }


# ============================================================
# EXHAUSTIVE SEARCH
# ============================================================

def exhaustive_search(
    df,
    base_conditions,
    max_depth
):

    print(
        f"\nSearching "
        f"{len(base_conditions):,} "
        f"atomic conditions..."
    )

    names = list(
        base_conditions.keys()
    )

    results = []

    # --------------------------------------------------------
    # Single conditions
    # --------------------------------------------------------

    for name in names:

        mask = base_conditions[
            name
        ]

        result = evaluate_condition(
            df,
            mask
        )

        if result is None:

            continue

        results.append({

            "depth": 1,

            "pattern":
                name,

            **result
        })

    print(
        f"Depth 1 complete: "
        f"{len(results):,}"
    )

    if max_depth < 2:

        return pd.DataFrame(
            results
        )

    # --------------------------------------------------------
    # Rank useful atomic conditions
    # --------------------------------------------------------

    level1 = pd.DataFrame(
        results
    )

    if level1.empty:

        return level1

    # Keep a broad candidate pool.
    #
    # We don't only keep highest win-rate conditions.
    # PF and average return are also considered.
    #
    candidate_names = set()

    top_by_win = (
        level1
        .sort_values(
            "win_rate",
            ascending=False
        )
        .head(250)
    )

    top_by_pf = (
        level1
        .sort_values(
            "pf",
            ascending=False
        )
        .head(250)
    )

    top_by_avg = (
        level1
        .sort_values(
            "average",
            ascending=False
        )
        .head(250)
    )

    for table in [
        top_by_win,
        top_by_pf,
        top_by_avg
    ]:

        candidate_names.update(
            table["pattern"]
        )

    candidate_names = [
        x
        for x in names
        if x in candidate_names
    ]

    print(
        f"Candidate conditions "
        f"for combinations: "
        f"{len(candidate_names):,}"
    )

    # --------------------------------------------------------
    # Depth 2+
    # --------------------------------------------------------

    previous_level = candidate_names

    for depth in range(
        2,
        max_depth + 1
    ):

        print(
            "\n"
            + "-" * 70
        )

        print(
            f"SEARCHING COMBINATIONS "
            f"OF DEPTH {depth}"
        )

        current_results = []

        combinations_checked = 0

        # ----------------------------------------------------
        # Avoid impossible combinations
        # ----------------------------------------------------

        for combo in itertools.combinations(
            previous_level,
            2
        ):

            # For depth 3/4 we build combinations
            # progressively from useful lower-level
            # conditions.
            #
            # Duplicate semantic conditions are skipped.

            combinations_checked += 1

            if combinations_checked % 50000 == 0:

                print(
                    f"\rChecked "
                    f"{combinations_checked:,}",
                    end=""
                )

            masks = [
                base_conditions[x]
                for x in combo
            ]

            mask = masks[0]

            for m in masks[1:]:

                mask = (
                    mask &
                    m
                )

            # Quick sample check
            count = int(
                mask.sum()
            )

            if count < MIN_TRAIN:

                continue

            result = evaluate_condition(
                df,
                mask
            )

            if result is None:

                continue

            pattern = " + ".join(
                combo
            )

            current_results.append({

                "depth":
                    depth,

                "pattern":
                    pattern,

                **result
            })

        print(
            f"\nDepth {depth}: "
            f"{combinations_checked:,} "
            f"combinations checked"
        )

        if not current_results:

            print(
                "No qualifying combinations."
            )

            break

        results.extend(
            current_results
        )

        level_df = pd.DataFrame(
            current_results
        )

        # ----------------------------------------------------
        # Keep candidates for next depth
        # ----------------------------------------------------

        keep = set()

        for sort_col in [
            "win_rate",
            "pf",
            "average"
        ]:

            top = (
                level_df
                .sort_values(
                    sort_col,
                    ascending=False
                )
                .head(200)
            )

            for pattern in top[
                "pattern"
            ]:

                pieces = [
                    x.strip()
                    for x in pattern.split("+")
                ]

                keep.update(
                    pieces
                )

        # We need complete lower-level patterns,
        # not just atomic conditions.
        #
        # Therefore next level uses top complete
        # combinations.

        next_patterns = []

        for sort_col in [
            "win_rate",
            "pf",
            "average"
        ]:

            top = (
                level_df
                .sort_values(
                    sort_col,
                    ascending=False
                )
                .head(150)
            )

            next_patterns.extend(
                top[
                    "pattern"
                ].tolist()
            )

        # Remove duplicates
        previous_level = list(
            dict.fromkeys(
                next_patterns
            )
        )

        # ----------------------------------------------------
        # If no useful candidates remain
        # ----------------------------------------------------

        if not previous_level:

            break

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # The above progressive search uses the
        # training set only.
        #
        # Validation and final test are untouched.
        # ----------------------------------------------------

    return pd.DataFrame(
        results
    )


# ============================================================
# VALIDATE DISCOVERED PATTERNS
# ============================================================

def pattern_mask_from_expression(
    df,
    expression
):

    parts = [
        x.strip()
        for x in expression.split("+")
    ]

    conditions = (
        generate_atomic_conditions(
            df
        )
    )

    conditions.update(
        generate_cross_conditions(
            df
        )
    )

    conditions.update(
        generate_triple_conditions(
            df
        )
    )

    mask = None

    for part in parts:

        if part not in conditions:

            return None

        if mask is None:

            mask = conditions[
                part
            ]

        else:

            mask = (
                mask &
                conditions[
                    part
                ]
            )

    return mask


# ============================================================
# TEST PATTERN
# ============================================================

def test_discovered_patterns(
    train,
    validation,
    test,
    discovered
):

    if discovered.empty:

        return pd.DataFrame()

    # --------------------------------------------------------
    # Only top candidates from TRAINING
    # --------------------------------------------------------

    candidates = []

    for sort_col in [
        "win_rate",
        "pf",
        "average"
    ]:

        top = (
            discovered
            .sort_values(
                sort_col,
                ascending=False
            )
            .head(250)
        )

        candidates.extend(
            top["pattern"]
            .tolist()
        )

    candidates = list(
        dict.fromkeys(
            candidates
        )
    )

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    validation_results = []

    for pattern in candidates:

        train_mask = (
            pattern_mask_from_expression(
                train,
                pattern
            )
        )

        if train_mask is None:
            continue

        val_mask = (
            pattern_mask_from_expression(
                validation,
                pattern
            )
        )

        if val_mask is None:
            continue

        train_subset = train[
            train_mask
        ]

        val_subset = validation[
            val_mask
        ]

        if len(val_subset) < MIN_VALIDATION:

            continue

        # Use training-selected direction
        train_long = calculate_stats(
            train_subset[
                "long_return"
            ]
        )

        train_short = calculate_stats(
            train_subset[
                "short_return"
            ]
        )

        if (
            train_long["win_rate"]
            >=
            train_short["win_rate"]
        ):

            direction = "LONG"

            val_returns = (
                val_subset[
                    "long_return"
                ]
            )

        else:

            direction = "SHORT"

            val_returns = (
                val_subset[
                    "short_return"
                ]
            )

        val_stats = calculate_stats(
            val_returns
        )

        validation_results.append({

            "pattern":
                pattern,

            "direction":
                direction,

            "train_n":
                len(train_subset),

            "train_win":
                max(
                    train_long["win_rate"],
                    train_short["win_rate"]
                ),

            "validation_n":
                val_stats["n"],

            "validation_win":
                val_stats["win_rate"],

            "validation_avg":
                val_stats["average"],

            "validation_pf":
                val_stats["pf"]
        })

    validation_df = pd.DataFrame(
        validation_results
    )

    if validation_df.empty:

        return validation_df

    # --------------------------------------------------------
    # Only candidates that survive validation
    # --------------------------------------------------------

    validation_df = (
        validation_df
        .sort_values(
            [
                "validation_win",
                "validation_pf"
            ],
            ascending=False
        )
        .head(100)
    )

    # --------------------------------------------------------
    # Final TEST
    # --------------------------------------------------------

    final_results = []

    for _, row in validation_df.iterrows():

        pattern = row[
            "pattern"
        ]

        direction = row[
            "direction"
        ]

        test_mask = (
            pattern_mask_from_expression(
                test,
                pattern
            )
        )

        if test_mask is None:

            continue

        subset = test[
            test_mask
        ]

        if len(subset) < MIN_TEST:

            continue

        if direction == "LONG":

            returns = subset[
                "long_return"
            ]

        else:

            returns = subset[
                "short_return"
            ]

        stats = calculate_stats(
            returns
        )

        final_results.append({

            "pattern":
                pattern,

            "direction":
                direction,

            "train_n":
                row["train_n"],

            "train_win":
                row["train_win"],

            "validation_n":
                row["validation_n"],

            "validation_win":
                row["validation_win"],

            "validation_avg":
                row["validation_avg"],

            "validation_pf":
                row["validation_pf"],

            "TEST_n":
                stats["n"],

            "TEST_win":
                stats["win_rate"],

            "TEST_avg":
                stats["average"],

            "TEST_pf":
                stats["pf"],

            "TEST_total":
                stats["total"]
        })

    return pd.DataFrame(
        final_results
    )


# ============================================================
# YEAR STABILITY
# ============================================================

def year_stability(
    test,
    pattern,
    direction
):

    mask = (
        pattern_mask_from_expression(
            test,
            pattern
        )
    )

    if mask is None:

        return []

    subset = test[
        mask
    ].copy()

    if subset.empty:

        return []

    subset["year"] = (
        subset[
            "event_date"
        ]
        .dt.year
    )

    results = []

    for year, group in (
        subset.groupby("year")
    ):

        if direction == "LONG":

            returns = group[
                "long_return"
            ]

        else:

            returns = group[
                "short_return"
            ]

        stats = calculate_stats(
            returns
        )

        results.append({

            "year":
                year,

            "n":
                stats["n"],

            "win":
                stats["win_rate"],

            "avg":
                stats["average"],

            "pf":
                stats["pf"]
        })

    return results


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n")

    print("=" * 90)
    print(
        "FULL EXHAUSTIVE EOD EDGE SEARCH"
    )
    print("=" * 90)

    print(
        "\nThis is the FINAL exhaustive "
        "EOD-only experiment."
    )

    print(
        "\nNo same-day opening information "
        "is used."
    )

    print(
        "\nTimeframes:"
    )

    print(
        "1m | 2m | 3m | 5m | 10m | 15m"
    )

    print(
        "\nTrade:"
    )

    print(
        "Next day 09:15 -> 15:27"
    )

    print(
        "\nValidation:"
    )

    print(
        "60% TRAIN / 20% VALIDATION / 20% FINAL TEST"
    )

    # ========================================================
    # DOWNLOAD
    # ========================================================

    data = download_dataset()

    z = zipfile.ZipFile(
        io.BytesIO(data)
    )

    files = [
        f
        for f in z.namelist()
        if f.endswith(".csv.gz")
    ]

    print(
        f"\nFiles found: "
        f"{len(files):,}"
    )

    # ========================================================
    # BUILD OBSERVATIONS
    # ========================================================

    all_rows = []

    for i, filename in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing "
            f"{i}/{len(files)}",
            end=""
        )

        rows = extract_stock_events(
            z,
            filename
        )

        all_rows.extend(
            rows
        )

    print("\n")

    if not all_rows:

        print(
            "No valid observations."
        )

        return

    df = pd.DataFrame(
        all_rows
    )

    df["signal_date"] = pd.to_datetime(
        df["signal_date"]
    )

    df["event_date"] = pd.to_datetime(
        df["event_date"]
    )

    print(
        f"Total observations: "
        f"{len(df):,}"
    )

    # ========================================================
    # CHRONOLOGICAL SPLIT
    # ========================================================

    dates = sorted(
        df[
            "event_date"
        ].unique()
    )

    n_dates = len(
        dates
    )

    train_end = int(
        n_dates *
        TRAIN_PERCENT
    )

    validation_end = int(
        n_dates *
        (
            TRAIN_PERCENT +
            VALIDATION_PERCENT
        )
    )

    train_end_date = dates[
        train_end
    ]

    validation_end_date = dates[
        validation_end
    ]

    train_all = df[
        df["event_date"] <
        train_end_date
    ].copy()

    validation_all = df[
        (
            df["event_date"] >=
            train_end_date
        )
        &
        (
            df["event_date"] <
            validation_end_date
        )
    ].copy()

    test_all = df[
        df["event_date"] >=
        validation_end_date
    ].copy()

    print(
        "\nTRAIN:"
    )

    print(
        len(train_all)
    )

    print(
        "\nVALIDATION:"
    )

    print(
        len(validation_all)
    )

    print(
        "\nFINAL TEST:"
    )

    print(
        len(test_all)
    )

    print(
        "\nTrain ends:"
    )

    print(
        train_end_date
    )

    print(
        "\nValidation ends:"
    )

    print(
        validation_end_date
    )

    # ========================================================
    # EACH DAILY EXTREME CATEGORY
    # ========================================================

    for top_n in TOP_RANKS:

        print("\n")
        print("=" * 90)
        print(
            f"TOP {top_n} DAILY GAINERS / LOSERS"
        )
        print("=" * 90)

        train = classify_events(
            train_all,
            top_n
        )

        validation = classify_events(
            validation_all,
            top_n
        )

        test = classify_events(
            test_all,
            top_n
        )

        print(
            f"\nTRAIN EVENTS: "
            f"{len(train):,}"
        )

        print(
            f"VALIDATION EVENTS: "
            f"{len(validation):,}"
        )

        print(
            f"TEST EVENTS: "
            f"{len(test):,}"
        )

        # ====================================================
        # ATOMIC CONDITIONS
        # ====================================================

        print(
            "\nGenerating atomic conditions..."
        )

        atomic = (
            generate_atomic_conditions(
                train
            )
        )

        print(
            f"Atomic conditions: "
            f"{len(atomic):,}"
        )

        # ====================================================
        # CROSS CONDITIONS
        # ====================================================

        cross = (
            generate_cross_conditions(
                train
            )
        )

        print(
            f"Cross-timeframe conditions: "
            f"{len(cross):,}"
        )

        # ====================================================
        # TRIPLE CONDITIONS
        # ====================================================

        triples = (
            generate_triple_conditions(
                train
            )
        )

        print(
            f"Triple-timeframe conditions: "
            f"{len(triples):,}"
        )

        # ====================================================
        # COMBINE
        # ====================================================

        all_conditions = {}

        all_conditions.update(
            atomic
        )

        all_conditions.update(
            cross
        )

        all_conditions.update(
            triples
        )

        print(
            f"Total searchable conditions: "
            f"{len(all_conditions):,}"
        )

        # ====================================================
        # EXHAUSTIVE SEARCH
        # ====================================================

        discovered = exhaustive_search(
            train,
            all_conditions,
            MAX_COMBINATION_DEPTH
        )

        if discovered.empty:

            print(
                "\nNo qualifying patterns."
            )

            continue

        # ====================================================
        # SAVE TRAINING RESULTS
        # ====================================================

        discovered = (
            discovered
            .sort_values(
                [
                    "win_rate",
                    "pf",
                    "average"
                ],
                ascending=False
            )
        )

        discovered.to_csv(
            f"FULL_EOD_TRAINING_TOP{top_n}.csv",
            index=False
        )

        # ====================================================
        # TOP TRAINING
        # ====================================================

        print("\n")
        print(
            "=" * 90
        )

        print(
            "TOP TRAINING PATTERNS"
        )

        print(
            "=" * 90
        )

        print(
            discovered
            .head(50)
            .to_string(
                index=False
            )
        )

        # ====================================================
        # 85% TRAINING
        # ====================================================

        high_training = discovered[
            discovered[
                "win_rate"
            ] >= 85
        ]

        print("\n")
        print(
            "85%+ TRAINING PATTERNS"
        )

        if high_training.empty:

            print(
                "NONE"
            )

        else:

            print(
                high_training
                .head(100)
                .to_string(
                    index=False
                )
            )

        # ====================================================
        # VALIDATION + FINAL TEST
        # ====================================================

        print("\n")
        print(
            "VALIDATING TOP TRAINING PATTERNS..."
        )

        final_results = (
            test_discovered_patterns(
                train,
                validation,
                test,
                discovered
            )
        )

        if final_results.empty:

            print(
                "\nNo patterns survived validation."
            )

            continue

        final_results = (
            final_results
            .sort_values(
                [
                    "TEST_win",
                    "TEST_pf"
                ],
                ascending=False
            )
        )

        final_results.to_csv(
            f"FULL_EOD_FINAL_TEST_TOP{top_n}.csv",
            index=False
        )

        # ====================================================
        # FINAL TEST
        # ====================================================

        print("\n")
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

        # ====================================================
        # 85% FINAL TEST
        # ====================================================

        high_test = final_results[
            final_results[
                "TEST_win"
            ] >= 85
        ]

        print("\n")
        print(
            "85%+ FINAL UNSEEN PATTERNS"
        )

        if high_test.empty:

            print(
                "NONE"
            )

        else:

            print(
                high_test
                .to_string(
                    index=False
                )
            )

        # ====================================================
        # YEAR STABILITY
        # ====================================================

        print("\n")
        print(
            "YEAR-BY-YEAR STABILITY "
            "OF TOP FINAL PATTERNS"
        )

        for _, row in (
            final_results.head(10).iterrows()
        ):

            pattern = row[
                "pattern"
            ]

            direction = row[
                "direction"
            ]

            print("\n")
            print(
                pattern
            )

            print(
                "Direction:",
                direction
            )

            yearly = year_stability(
                test,
                pattern,
                direction
            )

            if yearly:

                print(
                    pd.DataFrame(
                        yearly
                    ).to_string(
                        index=False
                    )
                )

    # ========================================================
    # COMPLETE
    # ========================================================

    print("\n")
    print("=" * 90)
    print(
        "FULL EXHAUSTIVE EOD SEARCH COMPLETE"
    )
    print("=" * 90)

    print(
        "\nNo same-day opening candles were used."
    )

    print(
        "If no 85% pattern survives the FINAL TEST,"
    )

    print(
        "we should move to the same-day opening "
        "confirmation experiment."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
