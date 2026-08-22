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
# FULL EXHAUSTIVE EOD EDGE SEARCH
# ============================================================
#
# SIGNAL:
#   PREVIOUS TRADING DAY EOD
#
# OUTCOME:
#   FOLLOWING TRADING DAY
#
# TRADE:
#   09:15 OPEN -> 15:27 OPEN
#
# TARGET:
#   DAILY TOP GAINERS / LOSERS
#
# TIMEFRAMES:
#   1m / 2m / 3m / 5m / 10m / 15m
#
# SEARCH:
#   Single conditions
#   2-condition combinations
#   3-condition combinations
#   4-condition combinations
#
# VALIDATION:
#   60% TRAIN
#   20% VALIDATION
#   20% FINAL TEST
#
# IMPORTANT:
#   The FINAL TEST is never used to discover patterns.
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

TOP_RANKS = [
    5,
    10,
    20,
    50
]

TIMEFRAMES = {
    "1m": 1,
    "2m": 2,
    "3m": 3,
    "5m": 5,
    "10m": 10,
    "15m": 15
}


# ============================================================
# SAMPLE SIZE REQUIREMENTS
# ============================================================

MIN_TRAIN = 200
MIN_VALIDATION = 50
MIN_TEST = 50


# ============================================================
# MAXIMUM COMBINATION DEPTH
# ============================================================

MAX_COMBINATION_DEPTH = 4


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
# DOWNLOAD DATA
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

        if chunk:

            chunks.append(chunk)

            downloaded += len(chunk)

            if total:

                print(
                    f"\rDownloaded: "
                    f"{downloaded / total * 100:.1f}%",
                    end=""
                )

    print()

    return b"".join(chunks)


# ============================================================
# LOAD ONE STOCK
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

    for offset in range(
        minutes
    ):

        minute = (
            start_minute +
            offset
        )

        timestamp = (
            f"15:{minute:02d}"
        )

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

def candle_metrics(
    candle
):

    if candle is None:

        return None

    candle_range = (
        candle["high"] -
        candle["low"]
    )

    body = abs(
        candle["close"] -
        candle["open"]
    )

    upper_wick = (
        candle["high"] -
        max(
            candle["open"],
            candle["close"]
        )
    )

    lower_wick = (
        min(
            candle["open"],
            candle["close"]
        ) -
        candle["low"]
    )

    if candle_range > 0:

        body_ratio = (
            body /
            candle_range
        )

        upper_wick_ratio = (
            upper_wick /
            candle_range
        )

        lower_wick_ratio = (
            lower_wick /
            candle_range
        )

        close_position = (
            candle["close"] -
            candle["low"]
        ) / candle_range

    else:

        body_ratio = 0
        upper_wick_ratio = 0
        lower_wick_ratio = 0
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

        momentum = 0

    return {

        "range":
            candle_range,

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

def candle_direction(
    candle
):

    if candle["close"] > candle["open"]:

        return "BULL"

    elif candle["close"] < candle["open"]:

        return "BEAR"

    else:

        return "DOJI"


# ============================================================
# EOD CANDLE START TIMES
# ============================================================

def get_eod_candles(
    day,
    timeframe_minutes
):

    if timeframe_minutes == 1:

        starts = [
            24,
            25,
            26,
            27,
            28,
            29
        ]

    elif timeframe_minutes == 2:

        starts = [
            19,
            21,
            23,
            25,
            27
        ]

    elif timeframe_minutes == 3:

        starts = [
            15,
            18,
            21,
            24,
            27
        ]

    elif timeframe_minutes == 5:

        starts = [
            10,
            15,
            20,
            25
        ]

    elif timeframe_minutes == 10:

        starts = [
            0,
            10,
            20
        ]

    elif timeframe_minutes == 15:

        starts = [
            0,
            15
        ]

    else:

        return {}

    candles = {}

    for start in starts:

        candle = build_candle(
            day,
            start,
            timeframe_minutes
        )

        if candle is not None:

            candles[
                start
            ] = candle

    return candles


# ============================================================
# BUILD PREVIOUS-DAY FEATURES
# ============================================================

def build_features(
    day
):

    features = {}

    for timeframe, minutes in (
        TIMEFRAMES.items()
    ):

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
        previous_start = ordered[-2]

        last = candles[
            last_start
        ]

        previous = candles[
            previous_start
        ]

        last_metrics = (
            candle_metrics(last)
        )

        previous_metrics = (
            candle_metrics(previous)
        )

        older_candles = [
            candles[x]
            for x in ordered[:-1]
        ]

        if older_candles:

            average_range = np.mean([
                candle_metrics(x)["range"]
                for x in older_candles
            ])

            average_volume = np.mean([
                x["volume"]
                for x in older_candles
            ])

        else:

            average_range = 0
            average_volume = 0

        last_direction = (
            candle_direction(last)
        )

        previous_direction = (
            candle_direction(previous)
        )

        prefix = timeframe

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
            f"{prefix}_PREVIOUS_BEAR"
        ] = (
            previous_direction == "BEAR"
        )

        features[
            f"{prefix}_PREVIOUS_BULL"
        ] = (
            previous_direction == "BULL"
        )

        # ----------------------------------------------------
        # Same / opposite
        # ----------------------------------------------------

        features[
            f"{prefix}_SAME"
        ] = (
            last_direction ==
            previous_direction
            and
            last_direction != "DOJI"
        )

        features[
            f"{prefix}_OPPOSITE"
        ] = (
            last_direction !=
            previous_direction
            and
            last_direction != "DOJI"
            and
            previous_direction != "DOJI"
        )

        # ----------------------------------------------------
        # Candle structure
        # ----------------------------------------------------

        features[
            f"{prefix}_BODY"
        ] = last_metrics[
            "body_ratio"
        ]

        features[
            f"{prefix}_UPPER_WICK"
        ] = last_metrics[
            "upper_wick_ratio"
        ]

        features[
            f"{prefix}_LOWER_WICK"
        ] = last_metrics[
            "lower_wick_ratio"
        ]

        features[
            f"{prefix}_CLOSE_POSITION"
        ] = last_metrics[
            "close_position"
        ]

        features[
            f"{prefix}_RANGE"
        ] = last_metrics[
            "range"
        ]

        features[
            f"{prefix}_MOMENTUM"
        ] = last_metrics[
            "momentum"
        ]

        # ----------------------------------------------------
        # Relative range
        # ----------------------------------------------------

        if average_range > 0:

            features[
                f"{prefix}_RANGE_RATIO"
            ] = (
                last_metrics["range"]
                /
                average_range
            )

        else:

            features[
                f"{prefix}_RANGE_RATIO"
            ] = 0

        # ----------------------------------------------------
        # Relative volume
        # ----------------------------------------------------

        if average_volume > 0:

            features[
                f"{prefix}_REL_VOLUME"
            ] = (
                last["volume"]
                /
                average_volume
            )

        else:

            features[
                f"{prefix}_REL_VOLUME"
            ] = 0

        # ----------------------------------------------------
        # Last vs previous volume
        # ----------------------------------------------------

        if previous["volume"] > 0:

            features[
                f"{prefix}_VOLUME_RATIO"
            ] = (
                last["volume"]
                /
                previous["volume"]
            )

        else:

            features[
                f"{prefix}_VOLUME_RATIO"
            ] = 0

        # ----------------------------------------------------
        # Body change
        # ----------------------------------------------------

        features[
            f"{prefix}_BODY_CHANGE"
        ] = (
            last_metrics["body_ratio"]
            -
            previous_metrics["body_ratio"]
        )

        # ----------------------------------------------------
        # Range change
        # ----------------------------------------------------

        if previous_metrics["range"] > 0:

            features[
                f"{prefix}_RANGE_CHANGE"
            ] = (
                last_metrics["range"]
                /
                previous_metrics["range"]
            )

        else:

            features[
                f"{prefix}_RANGE_CHANGE"
            ] = 0

        # ----------------------------------------------------
        # Momentum change
        # ----------------------------------------------------

        features[
            f"{prefix}_MOMENTUM_CHANGE"
        ] = (
            last_metrics["momentum"]
            -
            previous_metrics["momentum"]
        )

        # ----------------------------------------------------
        # Price change
        # ----------------------------------------------------

        if previous["close"] > 0:

            features[
                f"{prefix}_PRICE_CHANGE"
            ] = (
                (
                    last["close"]
                    -
                    previous["close"]
                )
                /
                previous["close"]
                *
                100
            )

        else:

            features[
                f"{prefix}_PRICE_CHANGE"
            ] = 0

    return features


# ============================================================
# EXTRACT STOCK EVENTS
# ============================================================

def extract_stock_events(
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

    symbol = os.path.basename(
        filename
    ).replace(
        "_1m.csv.gz",
        ""
    )

    rows = []

    for index in range(
        1,
        len(dates)
    ):

        signal_date = dates[
            index - 1
        ]

        event_date = dates[
            index
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
        # Next-day entry / exit
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
        # Previous-day EOD features
        # ----------------------------------------------------

        features = build_features(
            previous_day
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
# DAILY TOP GAINERS / LOSERS
# ============================================================

def classify_events(
    dataframe,
    top_n
):

    data = dataframe.copy()

    data["gain_rank"] = (
        data.groupby(
            "event_date"
        )[
            "event_return"
        ]
        .rank(
            method="first",
            ascending=False
        )
    )

    data["loss_rank"] = (
        data.groupby(
            "event_date"
        )[
            "event_return"
        ]
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

    gainers["target"] = (
        "GAINER"
    )

    losers["target"] = (
        "LOSER"
    )

    return pd.concat(
        [
            gainers,
            losers
        ],
        ignore_index=True
    )


# ============================================================
# GENERATE ATOMIC CONDITIONS
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
            f"{prefix}:LAST_BEAR"
        ] = df[
            f"{prefix}_LAST_BEAR"
        ].astype(bool)

        conditions[
            f"{prefix}:LAST_BULL"
        ] = df[
            f"{prefix}_LAST_BULL"
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

        # ----------------------------------------------------
        # Same / opposite
        # ----------------------------------------------------

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

            conditions[
                f"{prefix}:BODY<={level:.2f}"
            ] = (
                df[
                    f"{prefix}_BODY"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Upper wick
        # ----------------------------------------------------

        for level in WICK_LEVELS:

            conditions[
                f"{prefix}:UPPER_WICK>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_UPPER_WICK"
                ]
                >= level
            )

            conditions[
                f"{prefix}:UPPER_WICK<={level:.2f}"
            ] = (
                df[
                    f"{prefix}_UPPER_WICK"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Lower wick
        # ----------------------------------------------------

        for level in WICK_LEVELS:

            conditions[
                f"{prefix}:LOWER_WICK>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_LOWER_WICK"
                ]
                >= level
            )

            conditions[
                f"{prefix}:LOWER_WICK<={level:.2f}"
            ] = (
                df[
                    f"{prefix}_LOWER_WICK"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Close position
        # ----------------------------------------------------

        for level in CLOSE_LEVELS:

            conditions[
                f"{prefix}:CLOSE_POS>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_CLOSE_POSITION"
                ]
                >= level
            )

            conditions[
                f"{prefix}:CLOSE_POS<={level:.2f}"
            ] = (
                df[
                    f"{prefix}_CLOSE_POSITION"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Relative volume
        # ----------------------------------------------------

        for level in VOLUME_LEVELS:

            conditions[
                f"{prefix}:REL_VOL>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_REL_VOLUME"
                ]
                >= level
            )

            conditions[
                f"{prefix}:REL_VOL<={level:.2f}"
            ] = (
                df[
                    f"{prefix}_REL_VOLUME"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Volume ratio
        # ----------------------------------------------------

        for level in VOLUME_LEVELS:

            conditions[
                f"{prefix}:VOL_RATIO>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_VOLUME_RATIO"
                ]
                >= level
            )

            conditions[
                f"{prefix}:VOL_RATIO<={level:.2f}"
            ] = (
                df[
                    f"{prefix}_VOLUME_RATIO"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Range ratio
        # ----------------------------------------------------

        for level in RANGE_LEVELS:

            conditions[
                f"{prefix}:RANGE_RATIO>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_RANGE_RATIO"
                ]
                >= level
            )

            conditions[
                f"{prefix}:RANGE_RATIO<={level:.2f}"
            ] = (
                df[
                    f"{prefix}_RANGE_RATIO"
                ]
                <= level
            )

        # ----------------------------------------------------
        # Momentum
        # ----------------------------------------------------

        for level in MOMENTUM_LEVELS:

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
                    f"{prefix}_MOMENTUM_CHANGE"
                ]
                >= level
            )

            conditions[
                f"{prefix}:MOM_CHANGE<=-{level:.2f}"
            ] = (
                df[
                    f"{prefix}_MOMENTUM_CHANGE"
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
            2.50,
            3.00
        ]:

            conditions[
                f"{prefix}:RANGE_CHANGE>={level:.2f}"
            ] = (
                df[
                    f"{prefix}_RANGE_CHANGE"
                ]
                >= level
            )

        # ----------------------------------------------------
        # Price change between last two candles
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
                f"{prefix}:PRICE_CHANGE>={level:.2f}%"
            ] = (
                df[
                    f"{prefix}_PRICE_CHANGE"
                ]
                >= level
            )

            conditions[
                f"{prefix}:PRICE_CHANGE<=-{level:.2f}%"
            ] = (
                df[
                    f"{prefix}_PRICE_CHANGE"
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

        # ----------------------------------------------------
        # Same bearish direction
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Same bullish direction
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Opposite directions
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Both timeframe reversals
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Both timeframe continuation
        # ----------------------------------------------------

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

    # ========================================================
    # ALL-TIMEFRAME ALIGNMENT
    # ========================================================

    bear_masks = [
        df[
            f"{tf}_LAST_BEAR"
        ].astype(bool)
        for tf in timeframes
    ]

    bull_masks = [
        df[
            f"{tf}_LAST_BULL"
        ].astype(bool)
        for tf in timeframes
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

    # ========================================================
    # MAJORITY ALIGNMENT
    # ========================================================

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
# TRIPLE-TIMEFRAME CONDITIONS
# ============================================================

def generate_triple_conditions(
    df
):

    conditions = {}

    timeframes = list(
        TIMEFRAMES.keys()
    )

    for tf1, tf2, tf3 in itertools.combinations(
        timeframes,
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

        profit_factor = (
            gross_profit /
            gross_loss
        )

    else:

        profit_factor = np.inf

    return {

        "n":
            len(returns),

        "win_rate":
            win_rate,

        "average":
            returns.mean(),

        "pf":
            profit_factor,

        "total":
            returns.sum()
    }


# ============================================================
# EVALUATE A MASK
# ============================================================

def evaluate_mask(
    df,
    mask
):

    mask = pd.Series(
        mask,
        index=df.index
    ).fillna(False).astype(bool)

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

        selected = long_stats

    else:

        direction = "SHORT"

        selected = short_stats

    return {

        "direction":
            direction,

        "n":
            selected["n"],

        "win_rate":
            selected["win_rate"],

        "average":
            selected["average"],

        "pf":
            selected["pf"],

        "total":
            selected["total"]
    }


# ============================================================
# SELECT CANDIDATES
# ============================================================

def select_candidates(
    result_rows,
    number=200
):

    if not result_rows:

        return []

    frame = pd.DataFrame(
        [
            {
                k: v
                for k, v in row.items()
                if k != "_mask"
            }
            for row in result_rows
        ]
    )

    selected = set()

    for metric in [
        "win_rate",
        "pf",
        "average"
    ]:

        top = (
            frame
            .sort_values(
                metric,
                ascending=False
            )
            .head(number)
        )

        selected.update(
            top["pattern"].tolist()
        )

    return list(
        selected
    )


# ============================================================
# EXHAUSTIVE COMBINATION SEARCH
# ============================================================

def exhaustive_search(
    df,
    base_conditions,
    max_depth
):

    print()
    print(
        f"Searching "
        f"{len(base_conditions):,} "
        f"base conditions..."
    )

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Every condition has its REAL BOOLEAN MASK.
    #
    # This prevents the KeyError from the previous version.
    # --------------------------------------------------------

    condition_masks = {}

    for name, mask in (
        base_conditions.items()
    ):

        condition_masks[
            name
        ] = pd.Series(
            mask,
            index=df.index
        ).fillna(False).astype(bool)

    all_results = []

    # ========================================================
    # DEPTH 1
    # ========================================================

    depth1 = []

    for name, mask in (
        condition_masks.items()
    ):

        count = int(
            mask.sum()
        )

        if count < MIN_TRAIN:

            continue

        result = evaluate_mask(
            df,
            mask
        )

        if result is None:

            continue

        row = {

            "depth":
                1,

            "pattern":
                name,

            **result,

            "_mask":
                mask
        }

        depth1.append(
            row
        )

        all_results.append(
            {
                k: v
                for k, v in row.items()
                if k != "_mask"
            }
        )

    print(
        f"Depth 1 complete: "
        f"{len(depth1):,}"
    )

    if max_depth < 2:

        return pd.DataFrame(
            all_results
        )

    # ========================================================
    # SELECT DEPTH-1 CANDIDATES
    # ========================================================

    current_patterns = (
        select_candidates(
            depth1,
            number=250
        )
    )

    current_lookup = {
        row["pattern"]:
            row
        for row in depth1
        if row["pattern"]
        in current_patterns
    }

    print(
        f"Depth-1 candidates retained: "
        f"{len(current_lookup):,}"
    )

    # ========================================================
    # DEPTH 2 -> 4
    # ========================================================

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

        new_results = []

        checked = 0

        # ----------------------------------------------------
        # Atomic conditions that can be appended
        # ----------------------------------------------------

        atomic_names = list(
            condition_masks.keys()
        )

        # ----------------------------------------------------
        # For deeper searches, don't repeatedly use every
        # single atomic condition. Keep the strongest atomic
        # conditions from training.
        # ----------------------------------------------------

        if depth >= 3:

            depth1_sorted = sorted(
                depth1,
                key=lambda x: (
                    x["win_rate"],
                    x["pf"],
                    x["average"]
                ),
                reverse=True
            )

            atomic_names = [
                row["pattern"]
                for row in depth1_sorted[
                    :300
                ]
            ]

        # ----------------------------------------------------
        # Combine current patterns with atomic conditions
        # ----------------------------------------------------

        for previous_name, previous_row in (
            current_lookup.items()
        ):

            previous_mask = (
                previous_row["_mask"]
            )

            previous_parts = set(
                previous_name.split(
                    " + "
                )
            )

            for atomic_name in atomic_names:

                checked += 1

                if checked % 50000 == 0:

                    print(
                        f"\rChecked "
                        f"{checked:,}",
                        end=""
                    )

                # Don't add same condition twice
                if (
                    atomic_name
                    in previous_parts
                ):

                    continue

                atomic_mask = (
                    condition_masks[
                        atomic_name
                    ]
                )

                combined_mask = (
                    previous_mask
                    &
                    atomic_mask
                )

                count = int(
                    combined_mask.sum()
                )

                if count < MIN_TRAIN:

                    continue

                result = evaluate_mask(
                    df,
                    combined_mask
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

                new_results.append({

                    "depth":
                        depth,

                    "pattern":
                        pattern,

                    **result,

                    "_mask":
                        combined_mask
                })

        print()

        print(
            f"Depth {depth}: "
            f"{checked:,} combinations checked"
        )

        print(
            f"Depth {depth}: "
            f"{len(new_results):,} "
            f"qualifying combinations"
        )

        if not new_results:

            print(
                "No qualifying combinations."
            )

            break

        # ----------------------------------------------------
        # Remove duplicate pattern strings
        # ----------------------------------------------------

        unique = {}

        for row in new_results:

            unique[
                row["pattern"]
            ] = row

        new_results = list(
            unique.values()
        )

        # ----------------------------------------------------
        # Save clean results
        # ----------------------------------------------------

        for row in new_results:

            all_results.append(
                {
                    k: v
                    for k, v in row.items()
                    if k != "_mask"
                }
            )

        # ----------------------------------------------------
        # Select patterns for next level
        # ----------------------------------------------------

        retained_names = (
            select_candidates(
                new_results,
                number=200
            )
        )

        current_lookup = {
            row["pattern"]:
                row
            for row in new_results
            if row["pattern"]
            in retained_names
        }

        print(
            f"Patterns retained for "
            f"next depth: "
            f"{len(current_lookup):,}"
        )

        if not current_lookup:

            break

    # ========================================================
    # RETURN
    # ========================================================

    final = pd.DataFrame(
        all_results
    )

    if not final.empty:

        final = (
            final
            .drop_duplicates(
                subset=[
                    "pattern"
                ]
            )
            .sort_values(
                [
                    "win_rate",
                    "pf",
                    "average"
                ],
                ascending=False
            )
            .reset_index(
                drop=True
            )
        )

    return final


# ============================================================
# REBUILD MASK FROM PATTERN
# ============================================================

def build_condition_library(
    df
):

    library = {}

    library.update(
        generate_atomic_conditions(
            df
        )
    )

    library.update(
        generate_cross_conditions(
            df
        )
    )

    library.update(
        generate_triple_conditions(
            df
        )
    )

    return {
        name:
            pd.Series(
                mask,
                index=df.index
            ).fillna(False).astype(bool)
        for name, mask
        in library.items()
    }


# ============================================================
# BUILD MASK FOR A COMBINATION
# ============================================================

def pattern_to_mask(
    df,
    pattern,
    condition_library=None
):

    if condition_library is None:

        condition_library = (
            build_condition_library(
                df
            )
        )

    pieces = [
        piece.strip()
        for piece
        in pattern.split(
            " + "
        )
    ]

    final_mask = (
        pd.Series(
            True,
            index=df.index
        )
    )

    for piece in pieces:

        if piece not in condition_library:

            return None

        final_mask = (
            final_mask
            &
            condition_library[
                piece
            ]
        )

    return final_mask


# ============================================================
# VALIDATE PATTERNS
# ============================================================

def validate_patterns(
    train,
    validation,
    test,
    discovered
):

    if discovered.empty:

        return pd.DataFrame()

    # --------------------------------------------------------
    # Select candidates ONLY from TRAINING
    # --------------------------------------------------------

    candidate_names = set()

    for metric in [
        "win_rate",
        "pf",
        "average"
    ]:

        top = (
            discovered
            .sort_values(
                metric,
                ascending=False
            )
            .head(300)
        )

        candidate_names.update(
            top[
                "pattern"
            ].tolist()
        )

    # --------------------------------------------------------
    # Condition libraries
    # --------------------------------------------------------

    train_library = (
        build_condition_library(
            train
        )
    )

    validation_library = (
        build_condition_library(
            validation
        )
    )

    test_library = (
        build_condition_library(
            test
        )
    )

    validation_results = []

    # ========================================================
    # VALIDATION
    # ========================================================

    for pattern in candidate_names:

        train_mask = pattern_to_mask(
            train,
            pattern,
            train_library
        )

        validation_mask = pattern_to_mask(
            validation,
            pattern,
            validation_library
        )

        if (
            train_mask is None
            or
            validation_mask is None
        ):

            continue

        train_subset = train[
            train_mask
        ]

        validation_subset = (
            validation[
                validation_mask
            ]
        )

        if len(train_subset) < MIN_TRAIN:

            continue

        if len(validation_subset) < MIN_VALIDATION:

            continue

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

            validation_returns = (
                validation_subset[
                    "long_return"
                ]
            )

        else:

            direction = "SHORT"

            validation_returns = (
                validation_subset[
                    "short_return"
                ]
            )

        validation_stats = (
            calculate_stats(
                validation_returns
            )
        )

        if validation_stats is None:

            continue

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

            "train_pf":
                max(
                    train_long["pf"],
                    train_short["pf"]
                ),

            "validation_n":
                validation_stats["n"],

            "validation_win":
                validation_stats["win_rate"],

            "validation_avg":
                validation_stats["average"],

            "validation_pf":
                validation_stats["pf"]
        })

    validation_df = pd.DataFrame(
        validation_results
    )

    if validation_df.empty:

        return validation_df

    validation_df = (
        validation_df
        .sort_values(
            [
                "validation_win",
                "validation_pf",
                "validation_avg"
            ],
            ascending=False
        )
        .head(100)
    )

    # ========================================================
    # FINAL TEST
    # ========================================================

    final_results = []

    for _, row in (
        validation_df.iterrows()
    ):

        pattern = row[
            "pattern"
        ]

        direction = row[
            "direction"
        ]

        test_mask = pattern_to_mask(
            test,
            pattern,
            test_library
        )

        if test_mask is None:

            continue

        test_subset = test[
            test_mask
        ]

        if len(test_subset) < MIN_TEST:

            continue

        if direction == "LONG":

            test_returns = (
                test_subset[
                    "long_return"
                ]
            )

        else:

            test_returns = (
                test_subset[
                    "short_return"
                ]
            )

        test_stats = calculate_stats(
            test_returns
        )

        if test_stats is None:

            continue

        final_results.append({

            "pattern":
                pattern,

            "direction":
                direction,

            "train_n":
                row["train_n"],

            "train_win":
                row["train_win"],

            "train_pf":
                row["train_pf"],

            "validation_n":
                row["validation_n"],

            "validation_win":
                row["validation_win"],

            "validation_avg":
                row["validation_avg"],

            "validation_pf":
                row["validation_pf"],

            "TEST_n":
                test_stats["n"],

            "TEST_win":
                test_stats["win_rate"],

            "TEST_avg":
                test_stats["average"],

            "TEST_pf":
                test_stats["pf"],

            "TEST_total":
                test_stats["total"]
        })

    final_df = pd.DataFrame(
        final_results
    )

    if not final_df.empty:

        final_df = (
            final_df
            .sort_values(
                [
                    "TEST_win",
                    "TEST_pf",
                    "TEST_avg"
                ],
                ascending=False
            )
            .reset_index(
                drop=True
            )
        )

    return final_df


# ============================================================
# YEAR-BY-YEAR TEST STABILITY
# ============================================================

def test_year_stability(
    test,
    pattern,
    direction
):

    library = (
        build_condition_library(
            test
        )
    )

    mask = pattern_to_mask(
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

        if stats is None:

            continue

        rows.append({

            "year":
                year,

            "trades":
                stats["n"],

            "win_rate":
                stats["win_rate"],

            "average":
                stats["average"],

            "profit_factor":
                stats["pf"]
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
        "FULL EXHAUSTIVE EOD PATTERN SEARCH"
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
        "15:27 exit"
    )

    print()
    print(
        "Timeframes:"
    )

    print(
        "1m | 2m | 3m | 5m | 10m | 15m"
    )

    print()
    print(
        "No same-day opening candles are used."
    )

    print()
    print(
        "Train / Validation / Final Test:"
    )

    print(
        "60% / 20% / 20%"
    )

    # ========================================================
    # DOWNLOAD
    # ========================================================

    data = download_dataset()

    zip_file = zipfile.ZipFile(
        io.BytesIO(data)
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

    # ========================================================
    # BUILD DATASET
    # ========================================================

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

        rows = extract_stock_events(
            zip_file,
            filename
        )

        all_rows.extend(
            rows
        )

    print()
    print()

    if not all_rows:

        print(
            "No valid observations found."
        )

        return

    dataframe = pd.DataFrame(
        all_rows
    )

    dataframe[
        "signal_date"
    ] = pd.to_datetime(
        dataframe[
            "signal_date"
        ]
    )

    dataframe[
        "event_date"
    ] = pd.to_datetime(
        dataframe[
            "event_date"
        ]
    )

    print(
        f"Total observations: "
        f"{len(dataframe):,}"
    )

    # ========================================================
    # CHRONOLOGICAL SPLIT
    # ========================================================

    dates = sorted(
        dataframe[
            "event_date"
        ].unique()
    )

    total_dates = len(
        dates
    )

    train_index = int(
        total_dates *
        TRAIN_PERCENT
    )

    validation_index = int(
        total_dates *
        (
            TRAIN_PERCENT +
            VALIDATION_PERCENT
        )
    )

    train_end = dates[
        train_index
    ]

    validation_end = dates[
        validation_index
    ]

    train_all = dataframe[
        dataframe[
            "event_date"
        ] < train_end
    ].copy()

    validation_all = dataframe[
        (
            dataframe[
                "event_date"
            ] >= train_end
        )
        &
        (
            dataframe[
                "event_date"
            ] < validation_end
        )
    ].copy()

    test_all = dataframe[
        dataframe[
            "event_date"
        ] >= validation_end
    ].copy()

    print()
    print(
        "TRAIN:"
    )

    print(
        len(train_all)
    )

    print()
    print(
        "VALIDATION:"
    )

    print(
        len(validation_all)
    )

    print()
    print(
        "FINAL TEST:"
    )

    print(
        len(test_all)
    )

    print()
    print(
        "Train ends:"
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
    # EACH TOP-N
    # ========================================================

    for top_n in TOP_RANKS:

        print()
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

        print()
        print(
            f"TRAIN EVENTS: "
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
        # GENERATE CONDITIONS
        # ====================================================

        print()
        print(
            "Generating atomic conditions..."
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

        print()
        print(
            "Generating cross-timeframe conditions..."
        )

        cross = (
            generate_cross_conditions(
                train
            )
        )

        print(
            f"Cross-timeframe conditions: "
            f"{len(cross):,}"
        )

        print()
        print(
            "Generating triple-timeframe conditions..."
        )

        triple = (
            generate_triple_conditions(
                train
            )
        )

        print(
            f"Triple-timeframe conditions: "
            f"{len(triple):,}"
        )

        all_conditions = {}

        all_conditions.update(
            atomic
        )

        all_conditions.update(
            cross
        )

        all_conditions.update(
            triple
        )

        print()
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

            print()
            print(
                "No patterns survived."
            )

            continue

        # ====================================================
        # SAVE TRAINING RESULTS
        # ====================================================

        training_file = (
            f"FULL_EOD_TRAINING_TOP"
            f"{top_n}.csv"
        )

        discovered.to_csv(
            training_file,
            index=False
        )

        # ====================================================
        # TOP TRAINING PATTERNS
        # ====================================================

        print()
        print("=" * 90)
        print(
            "TOP TRAINING PATTERNS"
        )
        print("=" * 90)

        print(
            discovered
            .head(50)
            .to_string(
                index=False
            )
        )

        # ====================================================
        # TRAINING 85%
        # ====================================================

        training_85 = discovered[
            discovered[
                "win_rate"
            ] >= 85
        ]

        print()
        print(
            "85%+ TRAINING PATTERNS"
        )

        if training_85.empty:

            print(
                "NONE"
            )

        else:

            print(
                training_85
                .head(100)
                .to_string(
                    index=False
                )
            )

        # ====================================================
        # VALIDATION + FINAL TEST
        # ====================================================

        print()
        print(
            "VALIDATING DISCOVERED PATTERNS..."
        )

        final_results = validate_patterns(
            train,
            validation,
            test,
            discovered
        )

        if final_results.empty:

            print()
            print(
                "No patterns survived validation."
            )

            continue

        # ====================================================
        # SAVE FINAL RESULTS
        # ====================================================

        final_file = (
            f"FULL_EOD_FINAL_TEST_TOP"
            f"{top_n}.csv"
        )

        final_results.to_csv(
            final_file,
            index=False
        )

        # ====================================================
        # FINAL TEST
        # ====================================================

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

        # ====================================================
        # FINAL 85%
        # ====================================================

        final_85 = final_results[
            final_results[
                "TEST_win"
            ] >= 85
        ]

        print()
        print(
            "85%+ FINAL UNSEEN PATTERNS"
        )

        if final_85.empty:

            print(
                "NONE"
            )

        else:

            print(
                final_85
                .to_string(
                    index=False
                )
            )

        # ====================================================
        # YEAR STABILITY
        # ====================================================

        print()
        print(
            "YEAR-BY-YEAR STABILITY"
        )

        for _, row in (
            final_results
            .head(10)
            .iterrows()
        ):

            pattern = row[
                "pattern"
            ]

            direction = row[
                "direction"
            ]

            print()
            print(
                pattern
            )

            print(
                "Direction:",
                direction
            )

            yearly = (
                test_year_stability(
                    test,
                    pattern,
                    direction
                )
            )

            if not yearly.empty:

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
        "FULL EXHAUSTIVE EOD SEARCH COMPLETE"
    )
    print("=" * 90)

    print()
    print(
        "No same-day opening information "
        "was used."
    )

    print()
    print(
        "The important result is "
        "'85%+ FINAL UNSEEN PATTERNS'."
    )

    print()
    print(
        "If that section is empty, "
        "the EOD-only route did not "
        "produce an 85% pattern that "
        "survived the final unseen test."
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
