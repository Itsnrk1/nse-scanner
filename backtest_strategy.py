# ============================================================
# 09:15 OPENING-VOLATILITY STRATEGY
# ============================================================
#
# GOAL
# ------------------------------------------------------------
# Find the best simple strategy that:
#
#   SIGNAL AVAILABLE BEFORE 09:15
#   ENTRY = 09:15 OPEN
#   EXIT  = 15:27 OPEN
#
# BOTH LONG AND SHORT ARE TESTED.
#
# ============================================================
#
# FEATURES USED
# ------------------------------------------------------------
#
# PREVIOUS DAY:
#   - return
#   - range
#   - volatility
#   - body
#   - close position
#   - EOD momentum
#   - EOD reversal
#   - volume
#
# PREVIOUS-DAY OPEN:
#   - first 1m candle
#   - first 3m
#   - first 5m
#   - first 10m
#   - first 15m
#   - opening direction
#   - opening body
#   - opening range
#   - opening volume
#
# HISTORICAL CONTEXT:
#   - previous 5/10/20 day returns
#   - previous 5/10/20 day volatility
#   - historical direction frequency
#
# NO NEXT-DAY 09:16+ INFORMATION IS USED.
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
# CONFIGURATION
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/nse-fno-1min-data/"
    "releases/download/v1.0.0/stocks_1m_csvs.zip"
)

ENTRY_TIME = "09:15"
EXIT_TIME = "15:27"

# Approximate total round-trip cost in percentage points.
# Set to 0 for raw backtest.
ROUND_TRIP_COST = 0.10

MIN_TRAIN_TRADES = 100
MIN_VALIDATION_TRADES = 30
MIN_TEST_TRADES = 30

MAX_DEPTH = 3
MAX_CANDIDATES = 300

TARGET_WIN_RATE = 85.0


# ============================================================
# BASIC UTILITIES
# ============================================================

def candle_direction(open_price, close_price):

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


def safe_pct(a, b):

    if b is None or b == 0:
        return np.nan

    return (
        (a - b) /
        b *
        100.0
    )


def candle_features(candle):

    if candle is None:
        return None

    o = candle["open"]
    h = candle["high"]
    l = candle["low"]
    c = candle["close"]

    if o == 0:
        return None

    rng = h - l
    body = abs(c - o)

    if rng > 0:

        body_ratio = body / rng

        close_position = (
            c - l
        ) / rng

    else:

        body_ratio = 0.0
        close_position = 0.5

    return {

        "direction":
            candle_direction(
                o,
                c
            ),

        "body_ratio":
            body_ratio,

        "range":
            rng,

        "close_position":
            close_position,

        "momentum":
            (c - o) / o * 100,

        "volume":
            candle["volume"]
    }


# ============================================================
# DOWNLOAD
# ============================================================

def download_data():

    print("=" * 100)
    print("DOWNLOADING NSE 1-MINUTE DATA")
    print("=" * 100)

    r = requests.get(
        DATA_URL,
        timeout=900
    )

    r.raise_for_status()

    print(
        f"Downloaded "
        f"{len(r.content) / 1024 / 1024:.1f} MB"
    )

    return r.content


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
# BUILD INTRADAY CANDLE
# ============================================================

def build_candle(
    day,
    start,
    end
):

    rows = []

    for minute in range(
        start,
        end + 1
    ):

        h = minute // 60
        m = minute % 60

        key = f"{h:02d}:{m:02d}"

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
# PREVIOUS DAY FEATURES
# ============================================================

def build_previous_day_features(
    day
):

    if (
        "09:15" not in day
        or
        "15:29" not in day
    ):

        return None

    features = {}

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

    day_volume = sum(
        x["volume"]
        for x in day.values()
    )

    day_range = (
        day_high -
        day_low
    )

    # --------------------------------------------------------
    # Whole-day features
    # --------------------------------------------------------

    features[
        "PREV_DAY_RETURN"
    ] = safe_pct(
        day_close,
        day_open
    )

    features[
        "PREV_DAY_RANGE_PCT"
    ] = (
        day_range /
        day_open *
        100
    )

    if day_range > 0:

        features[
            "PREV_DAY_CLOSE_POS"
        ] = (
            day_close -
            day_low
        ) / day_range

    else:

        features[
            "PREV_DAY_CLOSE_POS"
        ] = 0.5

    # --------------------------------------------------------
    # EOD candles
    # --------------------------------------------------------

    eod_1 = build_candle(
        day,
        15 * 60,
        15 * 60 + 14
    )

    eod_2 = build_candle(
        day,
        15 * 60 + 15,
        15 * 60 + 29
    )

    for label, candle in [
        ("EOD1", eod_1),
        ("EOD2", eod_2)
    ]:

        cf = candle_features(
            candle
        )

        if cf is None:
            continue

        features[
            f"{label}_DIR"
        ] = cf["direction"]

        features[
            f"{label}_BODY"
        ] = cf["body_ratio"]

        features[
            f"{label}_RANGE"
        ] = cf["range"]

        features[
            f"{label}_CLOSE_POS"
        ] = cf["close_position"]

        features[
            f"{label}_MOMENTUM"
        ] = cf["momentum"]

        features[
            f"{label}_VOLUME"
        ] = cf["volume"]

    # EOD reversal
    if (
        eod_1 is not None
        and
        eod_2 is not None
    ):

        d1 = candle_direction(
            eod_1["open"],
            eod_1["close"]
        )

        d2 = candle_direction(
            eod_2["open"],
            eod_2["close"]
        )

        features[
            "EOD_REVERSAL"
        ] = (
            d1 != 0
            and
            d2 != 0
            and
            d1 != d2
        )

        features[
            "EOD_SECOND_VOL_MORE"
        ] = (
            eod_2["volume"]
            >
            eod_1["volume"]
        )

    # --------------------------------------------------------
    # PREVIOUS-DAY OPENING STRUCTURES
    # --------------------------------------------------------

    windows = {

        "OPEN_1M":
            (9 * 60 + 15,
             9 * 60 + 15),

        "OPEN_3M":
            (9 * 60 + 15,
             9 * 60 + 17),

        "OPEN_5M":
            (9 * 60 + 15,
             9 * 60 + 19),

        "OPEN_10M":
            (9 * 60 + 15,
             9 * 60 + 24),

        "OPEN_15M":
            (9 * 60 + 15,
             9 * 60 + 29)
    }

    for label, (
        start,
        end
    ) in windows.items():

        candle = build_candle(
            day,
            start,
            end
        )

        cf = candle_features(
            candle
        )

        if cf is None:
            continue

        features[
            f"{label}_DIR"
        ] = cf["direction"]

        features[
            f"{label}_BODY"
        ] = cf["body_ratio"]

        features[
            f"{label}_RANGE"
        ] = cf["range"]

        features[
            f"{label}_CLOSE_POS"
        ] = cf["close_position"]

        features[
            f"{label}_MOMENTUM"
        ] = cf["momentum"]

        features[
            f"{label}_VOLUME"
        ] = cf["volume"]

    # --------------------------------------------------------
    # Opening progression
    # --------------------------------------------------------

    opening_1 = build_candle(
        day,
        9 * 60 + 15,
        9 * 60 + 15
    )

    opening_3 = build_candle(
        day,
        9 * 60 + 15,
        9 * 60 + 17
    )

    opening_5 = build_candle(
        day,
        9 * 60 + 15,
        9 * 60 + 19
    )

    if (
        opening_1 is not None
        and
        opening_5 is not None
    ):

        features[
            "OPEN_5M_VS_1M_DIRECTION"
        ] = (
            candle_direction(
                opening_5["open"],
                opening_5["close"]
            )
            ==
            candle_direction(
                opening_1["open"],
                opening_1["close"]
            )
        )

        features[
            "OPEN_5M_VOLUME_MORE_1M"
        ] = (
            opening_5["volume"]
            >
            opening_1["volume"]
        )

    if (
        opening_3 is not None
        and
        opening_5 is not None
    ):

        features[
            "OPEN_5M_VS_3M_DIRECTION"
        ] = (
            candle_direction(
                opening_5["open"],
                opening_5["close"]
            )
            ==
            candle_direction(
                opening_3["open"],
                opening_3["close"]
            )
        )

        features[
            "OPEN_5M_VOLUME_MORE_3M"
        ] = (
            opening_5["volume"]
            >
            opening_3["volume"]
        )

    return features


# ============================================================
# HISTORICAL CONTEXT
# ============================================================

def add_historical_context(
    feature_rows
):

    df = pd.DataFrame(
        feature_rows
    )

    if df.empty:
        return df

    df = df.sort_values(
        [
            "symbol",
            "signal_date"
        ]
    ).reset_index(
        drop=True
    )

    # --------------------------------------------------------
    # IMPORTANT:
    # Shift before calculating statistics.
    #
    # This prevents today's signal from seeing today's result.
    # --------------------------------------------------------

    grouped = df.groupby(
        "symbol",
        group_keys=False
    )

    df[
        "HIST_RETURN_5"
    ] = grouped[
        "PREV_DAY_RETURN"
    ].transform(
        lambda x:
        x.shift(1)
        .rolling(5)
        .mean()
    )

    df[
        "HIST_RETURN_10"
    ] = grouped[
        "PREV_DAY_RETURN"
    ].transform(
        lambda x:
        x.shift(1)
        .rolling(10)
        .mean()
    )

    df[
        "HIST_RETURN_20"
    ] = grouped[
        "PREV_DAY_RETURN"
    ].transform(
        lambda x:
        x.shift(1)
        .rolling(20)
        .mean()
    )

    df[
        "HIST_VOL_10"
    ] = grouped[
        "PREV_DAY_RANGE_PCT"
    ].transform(
        lambda x:
        x.shift(1)
        .rolling(10)
        .mean()
    )

    # --------------------------------------------------------
    # Historical bullish frequency
    # --------------------------------------------------------

    prev_dir = (
        df[
            "PREV_DAY_RETURN"
        ] > 0
    ).astype(float)

    df[
        "HIST_BULL_FREQ_20"
    ] = (
        prev_dir
        .groupby(
            df["symbol"]
        )
        .transform(
            lambda x:
            x.shift(1)
            .rolling(20)
            .mean()
        )
    )

    return df


# ============================================================
# PROCESS STOCK
# ============================================================

def process_stock(
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

    rows = []

    for i in range(
        1,
        len(dates)
    ):

        previous_date = dates[
            i - 1
        ]

        trade_date = dates[
            i
        ]

        previous_day = make_day_lookup(
            grouped[
                previous_date
            ]
        )

        current_day = make_day_lookup(
            grouped[
                trade_date
            ]
        )

        # ----------------------------------------------------
        # Next-day execution prices
        # ----------------------------------------------------

        if ENTRY_TIME not in current_day:
            continue

        if EXIT_TIME not in current_day:
            continue

        entry = current_day[
            ENTRY_TIME
        ]["open"]

        exit_price = current_day[
            EXIT_TIME
        ]["open"]

        if entry <= 0:
            continue

        features = (
            build_previous_day_features(
                previous_day
            )
        )

        if features is None:
            continue

        # ----------------------------------------------------
        # Both directions
        # ----------------------------------------------------

        long_return = (
            (
                exit_price -
                entry
            )
            /
            entry
            *
            100
        )

        short_return = (
            (
                entry -
                exit_price
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
                pd.Timestamp(
                    previous_date
                ),

            "trade_date":
                pd.Timestamp(
                    trade_date
                ),

            "ENTRY":
                entry,

            "EXIT":
                exit_price,

            "LONG_RETURN":
                long_return,

            "SHORT_RETURN":
                short_return,

            **features
        })

    return rows


# ============================================================
# CREATE DATASET
# ============================================================

def create_dataset():

    raw = download_data()

    z = zipfile.ZipFile(
        io.BytesIO(raw)
    )

    files = [
        x
        for x in z.namelist()
        if x.endswith(
            ".csv.gz"
        )
    ]

    print(
        f"Stocks found: "
        f"{len(files):,}"
    )

    rows = []

    for n, filename in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing "
            f"{n:,}/"
            f"{len(files):,}",
            end=""
        )

        try:

            rows.extend(
                process_stock(
                    z,
                    filename
                )
            )

        except Exception:

            continue

    print()

    df = pd.DataFrame(
        rows
    )

    if df.empty:

        raise RuntimeError(
            "Dataset is empty."
        )

    df = add_historical_context(
        df
    )

    return df.sort_values(
        "trade_date"
    ).reset_index(
        drop=True
    )


# ============================================================
# SPLIT
# ============================================================

def split_dataset(
    df
):

    dates = sorted(
        df[
            "trade_date"
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
        df[
            "trade_date"
        ].dt.normalize()
        <
        train_end
    ].copy()

    validation = df[
        (
            df[
                "trade_date"
            ].dt.normalize()
            >=
            train_end
        )
        &
        (
            df[
                "trade_date"
            ].dt.normalize()
            <
            validation_end
        )
    ].copy()

    test = df[
        df[
            "trade_date"
        ].dt.normalize()
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
# MAKE LONG/SHORT EVENT DATA
# ============================================================

def make_direction_dataset(
    df,
    direction
):

    result = df.copy()

    if direction == "LONG":

        result[
            "RETURN"
        ] = result[
            "LONG_RETURN"
        ]

    else:

        result[
            "RETURN"
        ] = result[
            "SHORT_RETURN"
        ]

    return result


# ============================================================
# STATISTICS
# ============================================================

def stats(
    returns
):

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
        net[
            net > 0
        ].sum()
    )

    gross_loss = abs(
        net[
            net < 0
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

        "trades":
            len(net),

        "win_rate":
            win_rate,

        "average":
            net.mean(),

        "profit_factor":
            pf,

        "total":
            net.sum()
    }


# ============================================================
# BASE DIRECTION RESULTS
# ============================================================

def print_base_results(
    train,
    validation,
    test,
    direction
):

    print()
    print("=" * 100)
    print(
        f"BASE {direction}"
    )
    print("=" * 100)

    for name, data in [
        ("TRAIN", train),
        ("VALIDATION", validation),
        ("FINAL TEST", test)
    ]:

        s = stats(
            data["RETURN"]
        )

        print()
        print(name)

        print(
            f"Trades       : "
            f"{s['trades']:,}"
        )

        print(
            f"Win rate     : "
            f"{s['win_rate']:.2f}%"
        )

        print(
            f"Average      : "
            f"{s['average']:.4f}%"
        )

        print(
            f"Profit factor: "
            f"{s['profit_factor']:.3f}"
        )

        print(
            f"Total        : "
            f"{s['total']:.4f}%"
        )


# ============================================================
# CONDITION GENERATOR
# ============================================================

def generate_conditions(
    df
):

    conditions = {}

    ignored = {
        "symbol",
        "signal_date",
        "trade_date",
        "ENTRY",
        "EXIT",
        "LONG_RETURN",
        "SHORT_RETURN",
        "RETURN"
    }

    for column in df.columns:

        if column in ignored:
            continue

        series = df[
            column
        ]

        # ----------------------------------------------------
        # Boolean conditions
        # ----------------------------------------------------

        if pd.api.types.is_bool_dtype(
            series
        ):

            conditions[
                f"{column}=TRUE"
            ] = (
                series
                .fillna(False)
                .astype(bool)
            )

            conditions[
                f"{column}=FALSE"
            ] = (
                ~series
                .fillna(False)
                .astype(bool)
            )

            continue

        # ----------------------------------------------------
        # Numeric conditions
        # ----------------------------------------------------

        if not pd.api.types.is_numeric_dtype(
            series
        ):
            continue

        values = pd.to_numeric(
            series,
            errors="coerce"
        )

        valid = values.dropna()

        if len(valid) < 100:
            continue

        # Use quantiles rather than thousands of arbitrary
        # thresholds.
        try:

            q = valid.quantile(
                [
                    .10,
                    .20,
                    .30,
                    .40,
                    .50,
                    .60,
                    .70,
                    .80,
                    .90
                ]
            )

        except Exception:

            continue

        for quantile, value in q.items():

            if not np.isfinite(
                value
            ):
                continue

            conditions[
                f"{column}<=Q{quantile:.2f}"
            ] = (
                values <= value
            )

            conditions[
                f"{column}>=Q{quantile:.2f}"
            ] = (
                values >= value
            )

    return conditions


# ============================================================
# CANDIDATE EVALUATION
# ============================================================

def evaluate_candidates(
    df,
    conditions,
    min_trades
):

    candidates = []

    for name, mask in conditions.items():

        subset = df[
            mask
        ]

        if len(subset) < min_trades:
            continue

        s = stats(
            subset["RETURN"]
        )

        if s is None:
            continue

        candidates.append({

            "pattern":
                name,

            "_mask":
                mask,

            **s
        })

    if not candidates:
        return []

    # We want both high win rate and good economics.
    candidates.sort(
        key=lambda x: (
            x["win_rate"],
            x["profit_factor"],
            x["average"]
        ),
        reverse=True
    )

    return candidates[
        :MAX_CANDIDATES
    ]


# ============================================================
# COMBINATION SEARCH
# ============================================================

def combine_candidates(
    df,
    survivors,
    conditions,
    min_trades
):

    results = []

    names = list(
        conditions.keys()
    )

    for candidate in survivors:

        existing = set(
            candidate[
                "pattern"
            ].split(" + ")
        )

        for name in names:

            if name in existing:
                continue

            # Avoid duplicate combinations.
            if name <= max(existing):
                continue

            mask = (
                candidate["_mask"]
                &
                conditions[name]
            )

            if mask.sum() < min_trades:
                continue

            subset = df[
                mask
            ]

            s = stats(
                subset["RETURN"]
            )

            if s is None:
                continue

            pattern = (
                " + ".join(
                    sorted(
                        existing |
                        {name}
                    )
                )
            )

            results.append({

                "pattern":
                    pattern,

                "_mask":
                    mask,

                **s
            })

    if not results:
        return []

    # Remove identical masks.
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
        key=lambda x: (
            x["win_rate"],
            x["profit_factor"],
            x["average"]
        ),
        reverse=True
    )

    return results[
        :MAX_CANDIDATES
    ]


# ============================================================
# APPLY PATTERN
# ============================================================

def apply_pattern(
    df,
    pattern
):

    conditions = generate_conditions(
        df
    )

    mask = pd.Series(
        True,
        index=df.index
    )

    for part in pattern.split(
        " + "
    ):

        if part not in conditions:
            return None

        mask &= (
            conditions[
                part
            ]
        )

    return mask


# ============================================================
# VALIDATE
# ============================================================

def validate_patterns(
    patterns,
    validation
):

    rows = []

    for candidate in patterns:

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

        if len(subset) < MIN_VALIDATION_TRADES:
            continue

        s = stats(
            subset["RETURN"]
        )

        rows.append({

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
                s["trades"],

            "validation_win":
                s["win_rate"],

            "validation_average":
                s["average"],

            "validation_pf":
                s["profit_factor"]
        })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(
        rows
    )

    return result.sort_values(
        [
            "validation_win",
            "validation_pf",
            "validation_average"
        ],
        ascending=False
    ).head(
        MAX_CANDIDATES
    )


# ============================================================
# FINAL TEST
# ============================================================

def final_test(
    validation_results,
    test
):

    rows = []

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

        if len(subset) < MIN_TEST_TRADES:
            continue

        s = stats(
            subset["RETURN"]
        )

        rows.append({

            "pattern":
                candidate["pattern"],

            "train_win":
                candidate["train_win"],

            "train_pf":
                candidate["train_pf"],

            "validation_win":
                candidate[
                    "validation_win"
                ],

            "validation_pf":
                candidate[
                    "validation_pf"
                ],

            "test_trades":
                s["trades"],

            "test_win":
                s["win_rate"],

            "test_average":
                s["average"],

            "test_pf":
                s["profit_factor"],

            "test_total":
                s["total"]
        })

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "test_win",
                "test_pf",
                "test_average"
            ],
            ascending=False
        )
        .reset_index(drop=True)
    )


# ============================================================
# SEARCH ONE DIRECTION
# ============================================================

def search_direction(
    df,
    direction
):

    data = make_direction_dataset(
        df,
        direction
    )

    train, validation, test, train_end, validation_end = (
        split_dataset(data)
    )

    print()
    print("=" * 100)
    print(
        f"SEARCHING {direction}"
    )
    print("=" * 100)

    print_base_results(
        train,
        validation,
        test,
        direction
    )

    print()
    print(
        "Generating conditions..."
    )

    conditions = generate_conditions(
        train
    )

    print(
        f"Conditions: "
        f"{len(conditions):,}"
    )

    # --------------------------------------------------------
    # DEPTH 1
    # --------------------------------------------------------

    survivors = evaluate_candidates(
        train,
        conditions,
        MIN_TRAIN_TRADES
    )

    print(
        f"Depth 1 survivors: "
        f"{len(survivors):,}"
    )

    # --------------------------------------------------------
    # DEPTH 2+
    # --------------------------------------------------------

    for depth in range(
        2,
        MAX_DEPTH + 1
    ):

        if not survivors:
            break

        survivors = combine_candidates(
            train,
            survivors,
            conditions,
            MIN_TRAIN_TRADES
        )

        print(
            f"Depth {depth} survivors: "
            f"{len(survivors):,}"
        )

    if not survivors:

        print(
            "No candidates."
        )

        return pd.DataFrame()

    # --------------------------------------------------------
    # VALIDATION
    # --------------------------------------------------------

    validation_results = (
        validate_patterns(
            survivors,
            validation
        )
    )

    if validation_results.empty:

        print(
            "No patterns survived validation."
        )

        return pd.DataFrame()

    validation_results.to_csv(
        f"{direction}_VALIDATION.csv",
        index=False
    )

    print()
    print(
        "TOP VALIDATION RESULTS"
    )

    print(
        validation_results
        .head(20)
        .to_string(
            index=False
        )
    )

    # --------------------------------------------------------
    # FINAL TEST
    # --------------------------------------------------------

    test_results = final_test(
        validation_results,
        test
    )

    if test_results.empty:

        print(
            "No patterns survived final test."
        )

        return pd.DataFrame()

    test_results[
        "direction"
    ] = direction

    test_results.to_csv(
        f"{direction}_FINAL_TEST.csv",
        index=False
    )

    print()
    print(
        "=" * 100
    )
    print(
        f"{direction} FINAL UNSEEN TEST"
    )
    print(
        "=" * 100
    )

    print(
        test_results
        .head(30)
        .to_string(
            index=False
        )
    )

    return test_results


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 100)
    print(
        "09:15 OPEN / 15:27 EXIT — OPENING VOLATILITY RESEARCH"
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
        "SIGNAL INFORMATION MUST EXIST BEFORE 09:15."
    )

    print(
        "LONG AND SHORT WILL BOTH BE TESTED."
    )

    # --------------------------------------------------------
    # DATA
    # --------------------------------------------------------

    df = create_dataset()

    print()
    print(
        f"TOTAL OBSERVATIONS: "
        f"{len(df):,}"
    )

    # --------------------------------------------------------
    # LONG
    # --------------------------------------------------------

    long_results = search_direction(
        df,
        "LONG"
    )

    # --------------------------------------------------------
    # SHORT
    # --------------------------------------------------------

    short_results = search_direction(
        df,
        "SHORT"
    )

    # --------------------------------------------------------
    # COMBINE
    # --------------------------------------------------------

    results = []

    if not long_results.empty:

        results.append(
            long_results
        )

    if not short_results.empty:

        results.append(
            short_results
        )

    if not results:

        print(
            "\nNO VALID STRATEGY FOUND."
        )

        return

    final = pd.concat(
        results,
        ignore_index=True
    )

    final = final.sort_values(
        [
            "test_win",
            "test_pf",
            "test_average"
        ],
        ascending=False
    ).reset_index(
        drop=True
    )

    final.to_csv(
        "OPENING_VOLATILITY_FINAL_RESULTS.csv",
        index=False
    )

    # --------------------------------------------------------
    # BEST
    # --------------------------------------------------------

    print()
    print("=" * 100)
    print(
        "BEST FINAL UNSEEN RESULTS"
    )
    print("=" * 100)

    print(
        final
        .head(30)
        .to_string(
            index=False
        )
    )

    # --------------------------------------------------------
    # >= 85%
    # --------------------------------------------------------

    over_85 = final[
        final[
            "test_win"
        ] >= TARGET_WIN_RATE
    ]

    print()
    print("=" * 100)
    print(
        "FINAL TEST >= 85%"
    )
    print("=" * 100)

    if over_85.empty:

        print(
            "NONE"
        )

    else:

        print(
            over_85.to_string(
                index=False
            )
        )

    # --------------------------------------------------------
    # BEST STRATEGY
    # --------------------------------------------------------

    best = final.iloc[0]

    print()
    print("=" * 100)
    print(
        "BEST STRATEGY FOUND"
    )
    print("=" * 100)

    print(
        f"Direction      : "
        f"{best['direction']}"
    )

    print(
        f"Pattern        :\n"
        f"{best['pattern']}"
    )

    print(
        f"\nTest trades    : "
        f"{int(best['test_trades']):,}"
    )

    print(
        f"Test win rate  : "
        f"{best['test_win']:.2f}%"
    )

    print(
        f"Test average   : "
        f"{best['test_average']:.4f}%"
    )

    print(
        f"Test PF        : "
        f"{best['test_pf']:.3f}"
    )

    print(
        f"Test total     : "
        f"{best['test_total']:.4f}%"
    )

    print()
    print("=" * 100)
    print(
        "RESEARCH COMPLETE"
    )
    print("=" * 100)

    print()
    print(
        "Output:"
    )

    print(
        "LONG_FINAL_TEST.csv"
    )

    print(
        "SHORT_FINAL_TEST.csv"
    )

    print(
        "OPENING_VOLATILITY_FINAL_RESULTS.csv"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
