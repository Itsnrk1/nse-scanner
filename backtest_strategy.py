# ============================================================
# BASE STRATEGY OPTIMIZER
# ============================================================
#
# BASE:
#   Previous day 15-minute:
#       Candle 1 = BULL
#       Candle 2 = BEAR
#
# TRADE:
#       SHORT
#
# ENTRY:
#       NEXT DAY 09:15 OPEN
#
# EXIT:
#       NEXT DAY 15:27 OPEN
#
# IMPORTANT:
#   ALL SIGNAL INFORMATION COMES FROM THE PREVIOUS DAY.
#   NO NEXT-DAY GAP OR 09:15 INFORMATION IS USED.
#
# OBJECTIVE:
#   Start from the 15m reversal base and find simple
#   filters that improve:
#
#       1. Win rate
#       2. Profit factor
#       3. Average return
#
#   HARD TARGET:
#       >= 85% FINAL TEST WIN RATE
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

TARGET_WIN_RATE = 85.0

MIN_TRAIN_TRADES = 100
MIN_VALIDATION_TRADES = 30
MIN_TEST_TRADES = 30

# Maximum filters in one strategy.
MAX_DEPTH = 3

# Number of candidates carried forward.
MAX_SURVIVORS = 300


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
# CANDLE WINDOWS
# ============================================================
#
# Previous day only.
#
# We construct:
#
# 1m:
#   opening candles
#   late-day candles
#
# 3m:
#   opening structure
#   late-day structure
#
# etc.
#
# The 15m BASE is:
#
#   15:00-15:14 = first candle
#   15:15-15:29 = second candle
#
# BASE:
#   first BULL
#   second BEAR
#
# ============================================================

OPENING_ENDS = {
    "1m": [15, 16, 17, 18, 19, 20],
    "2m": [17, 19, 21, 23],
    "3m": [17, 20, 23, 26, 29],
    "5m": [19, 24, 29],
    "10m": [24, 29],
    "15m": [29]
}


# ============================================================
# UTILITY
# ============================================================

def minute_key(minutes):

    return (
        f"{minutes // 60:02d}:"
        f"{minutes % 60:02d}"
    )


def direction(candle):

    if candle["close"] > candle["open"]:
        return "BULL"

    if candle["close"] < candle["open"]:
        return "BEAR"

    return "DOJI"


def candle_metrics(candle):

    rng = candle["high"] - candle["low"]

    body = abs(
        candle["close"] -
        candle["open"]
    )

    if rng > 0:

        body_ratio = body / rng

        close_position = (
            candle["close"] -
            candle["low"]
        ) / rng

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
        "range": rng,
        "body": body_ratio,
        "close_pos": close_position,
        "momentum": momentum
    }


# ============================================================
# BUILD CANDLE
# ============================================================

def build_candle(
    day,
    start_minutes,
    end_minutes
):

    rows = []

    for minute in range(
        start_minutes,
        end_minutes + 1
    ):

        key = minute_key(minute)

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
            .strftime(
                "%Y-%m-%d"
            )
        )

        df["hm"] = (
            df["datetime"]
            .dt
            .strftime(
                "%H:%M"
            )
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
# BUILD FEATURES
# ============================================================

def build_features(day):

    features = {}

    # ========================================================
    # 15m BASE
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

    # --------------------------------------------------------
    # BASE MUST BE:
    #
    # first = BULL
    # second = BEAR
    # --------------------------------------------------------

    if (
        direction(first_15) != "BULL"
        or
        direction(second_15) != "BEAR"
    ):

        return None

    m1 = candle_metrics(
        first_15
    )

    m2 = candle_metrics(
        second_15
    )

    features[
        "BASE_15M_FIRST_BODY"
    ] = m1["body"]

    features[
        "BASE_15M_SECOND_BODY"
    ] = m2["body"]

    features[
        "BASE_15M_FIRST_CLOSE_POS"
    ] = m1["close_pos"]

    features[
        "BASE_15M_SECOND_CLOSE_POS"
    ] = m2["close_pos"]

    features[
        "BASE_15M_FIRST_VOLUME"
    ] = first_15["volume"]

    features[
        "BASE_15M_SECOND_VOLUME"
    ] = second_15["volume"]

    features[
        "BASE_15M_FIRST_RANGE"
    ] = m1["range"]

    features[
        "BASE_15M_SECOND_RANGE"
    ] = m2["range"]

    features[
        "BASE_15M_FIRST_MOMENTUM"
    ] = m1["momentum"]

    features[
        "BASE_15M_SECOND_MOMENTUM"
    ] = m2["momentum"]

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
        "BASE_SECOND_RANGE_MORE"
    ] = (
        m2["range"]
        >
        m1["range"]
    )

    features[
        "BASE_FIRST_RANGE_MORE"
    ] = (
        m1["range"]
        >
        m2["range"]
    )

    features[
        "BASE_SECOND_BODY_MORE"
    ] = (
        m2["body"]
        >
        m1["body"]
    )

    features[
        "BASE_FIRST_BODY_MORE"
    ] = (
        m1["body"]
        >
        m2["body"]
    )

    # ========================================================
    # PREVIOUS DAY WHOLE-DAY FEATURES
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

    day_high = max(
        x["high"]
        for x in day.values()
    )

    day_low = min(
        x["low"]
        for x in day.values()
    )

    if day_open <= 0:
        return None

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

    day_range = (
        day_high -
        day_low
    )

    if day_range > 0:

        day_close_position = (
            day_close -
            day_low
        ) / day_range

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
    # OPENING STRUCTURES
    # ========================================================

    for tf, minutes in TIMEFRAMES.items():

        for end_minute in (
            OPENING_ENDS[tf]
        ):

            start = (
                9 * 60 +
                15
            )

            end = (
                9 * 60 +
                end_minute
            )

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
                f"{tf}_OPEN_{end_minute}"
            )

            features[
                prefix + "_BULL"
            ] = (
                direction(candle)
                ==
                "BULL"
            )

            features[
                prefix + "_BEAR"
            ] = (
                direction(candle)
                ==
                "BEAR"
            )

            features[
                prefix + "_BODY"
            ] = metrics[
                "body"
            ]

            features[
                prefix + "_CLOSE_POS"
            ] = metrics[
                "close_pos"
            ]

            features[
                prefix + "_RANGE"
            ] = metrics[
                "range"
            ]

            features[
                prefix + "_MOMENTUM"
            ] = metrics[
                "momentum"
            ]

            features[
                prefix + "_VOLUME"
            ] = candle[
                "volume"
            ]

    # ========================================================
    # LATE-DAY CANDLES FROM OTHER TIMEFRAMES
    # ========================================================

    late_specs = {

        "1m":
            [(15*60+24, 15*60+24),
             (15*60+25, 15*60+25),
             (15*60+26, 15*60+26),
             (15*60+27, 15*60+27),
             (15*60+28, 15*60+28),
             (15*60+29, 15*60+29)],

        "2m":
            [(15*60+23, 15*60+24),
             (15*60+25, 15*60+26),
             (15*60+27, 15*60+28)],

        "3m":
            [(15*60+18, 15*60+20),
             (15*60+21, 15*60+23),
             (15*60+24, 15*60+26),
             (15*60+27, 15*60+29)],

        "5m":
            [(15*60+10, 15*60+14),
             (15*60+15, 15*60+19),
             (15*60+20, 15*60+24),
             (15*60+25, 15*60+29)],

        "10m":
            [(15*60+10, 15*60+19),
             (15*60+20, 15*60+29)],

    }

    for tf, ranges in late_specs.items():

        for idx, (
            start,
            end
        ) in enumerate(ranges):

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
                f"{tf}_EOD_{idx+1}"
            )

            features[
                prefix + "_BULL"
            ] = (
                direction(candle)
                ==
                "BULL"
            )

            features[
                prefix + "_BEAR"
            ] = (
                direction(candle)
                ==
                "BEAR"
            )

            features[
                prefix + "_BODY"
            ] = metrics[
                "body"
            ]

            features[
                prefix + "_CLOSE_POS"
            ] = metrics[
                "close_pos"
            ]

            features[
                prefix + "_RANGE"
            ] = metrics[
                "range"
            ]

            features[
                prefix + "_MOMENTUM"
            ] = metrics[
                "momentum"
            ]

            features[
                prefix + "_VOLUME"
            ] = candle[
                "volume"
            ]

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
        # NEXT DAY EXECUTION DATA
        # ----------------------------------------------------

        if ENTRY_TIME not in next_day:
            continue

        if EXIT_TIME not in next_day:
            continue

        # ----------------------------------------------------
        # PREVIOUS DAY BASE SIGNAL
        # ----------------------------------------------------

        features = build_features(
            previous_day
        )

        if features is None:
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
                exit_price -
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

            # BASE IS SHORT
            "RETURN":
                -raw_return,

            **features

        })

    return events


# ============================================================
# DOWNLOAD
# ============================================================

def download_data():

    print(
        "=" * 100
    )

    print(
        "DOWNLOADING DATA"
    )

    print(
        "=" * 100
    )

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

    events = []

    for i, filename in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing "
            f"{i:,}/{len(files):,}",
            end=""
        )

        try:

            events.extend(
                extract_events(
                    z,
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
            "No qualifying BASE events found."
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

def split_data(df):

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
# BASE STATISTICS
# ============================================================

def calculate_stats(
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
        net[net > 0].sum()
    )

    gross_loss = abs(
        net[net < 0].sum()
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
# CONDITION GENERATION
# ============================================================

def generate_conditions(
    df
):

    conditions = {}

    numeric_columns = [
        "BASE_15M_FIRST_BODY",
        "BASE_15M_SECOND_BODY",
        "BASE_15M_FIRST_CLOSE_POS",
        "BASE_15M_SECOND_CLOSE_POS",
        "BASE_15M_FIRST_VOLUME",
        "BASE_15M_SECOND_VOLUME",
        "BASE_15M_FIRST_RANGE",
        "BASE_15M_SECOND_RANGE",
        "BASE_15M_FIRST_MOMENTUM",
        "BASE_15M_SECOND_MOMENTUM",
        "DAY_RETURN",
        "DAY_RANGE",
        "DAY_CLOSE_POSITION"
    ]

    # Add all other numeric feature columns.
    for column in df.columns:

        if column in [
            "RETURN",
            "symbol",
            "signal_date",
            "event_date"
        ]:
            continue

        if pd.api.types.is_numeric_dtype(
            df[column]
        ):

            if column not in numeric_columns:

                numeric_columns.append(
                    column
                )

    # --------------------------------------------------------
    # BOOLEAN FEATURES
    # --------------------------------------------------------

    for column in df.columns:

        if column in [
            "RETURN",
            "symbol",
            "signal_date",
            "event_date"
        ]:
            continue

        if df[column].dtype == bool:

            conditions[
                column
            ] = (
                df[column]
                .fillna(False)
                .astype(bool)
            )

    # --------------------------------------------------------
    # NUMERIC THRESHOLDS
    # --------------------------------------------------------

    for column in numeric_columns:

        if column not in df.columns:
            continue

        s = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        if s.notna().sum() < 100:
            continue

        # Quantile-based thresholds.
        qs = [
            0.10,
            0.20,
            0.30,
            0.50,
            0.70,
            0.80,
            0.90
        ]

        values = (
            s.dropna()
            .quantile(qs)
            .unique()
        )

        for value in values:

            if not np.isfinite(value):
                continue

            conditions[
                f"{column}<= {value:.8g}"
            ] = (
                s <= value
            )

            conditions[
                f"{column}>= {value:.8g}"
            ] = (
                s >= value
            )

    # --------------------------------------------------------
    # REMOVE DUPLICATE MASKS
    # --------------------------------------------------------

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
# SCORE
# ============================================================

def score(result):

    pf = result[
        "profit_factor"
    ]

    if not np.isfinite(pf):
        pf = 10

    return (
        result["win_rate"]
        +
        5 *
        np.log1p(
            max(pf, 0)
        )
        +
        3 *
        result["average"]
    )


# ============================================================
# SEARCH
# ============================================================

def search(
    train,
    conditions
):

    print()
    print(
        "=" * 100
    )

    print(
        "SEARCHING FILTERS AROUND BASE STRATEGY"
    )

    print(
        "=" * 100
    )

    candidates = []

    # --------------------------------------------------------
    # DEPTH 1
    # --------------------------------------------------------

    for name, mask in conditions.items():

        subset = train[
            mask
        ]

        if len(subset) < MIN_TRAIN_TRADES:
            continue

        result = calculate_stats(
            subset["RETURN"]
        )

        if result is None:
            continue

        candidates.append({

            "pattern":
                name,

            "_mask":
                mask,

            **result
        })

    candidates.sort(
        key=score,
        reverse=True
    )

    candidates = candidates[
        :MAX_SURVIVORS
    ]

    print(
        f"Depth 1 survivors: "
        f"{len(candidates):,}"
    )

    all_results = []

    for c in candidates:

        all_results.append(
            c.copy()
        )

    # --------------------------------------------------------
    # DEPTH 2 / 3
    # --------------------------------------------------------

    for depth in [
        2,
        3
    ]:

        new_candidates = []

        checked = 0

        for candidate in candidates:

            existing = set(
                candidate[
                    "pattern"
                ].split(
                    " + "
                )
            )

            for name, mask in conditions.items():

                if name in existing:
                    continue

                checked += 1

                combined = (
                    candidate["_mask"]
                    &
                    mask
                )

                subset = train[
                    combined
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

                new_pattern = (
                    " + ".join(
                        sorted(
                            existing |
                            {name}
                        )
                    )
                )

                new_candidates.append({

                    "pattern":
                        new_pattern,

                    "_mask":
                        combined,

                    **result
                })

        print()
        print(
            f"Depth {depth}: "
            f"{checked:,} combinations checked"
        )

        # ----------------------------------------------------
        # Remove duplicate masks.
        # ----------------------------------------------------

        unique = {}

        for c in new_candidates:

            signature = np.packbits(
                c["_mask"]
                .to_numpy(
                    dtype=np.uint8
                )
            ).tobytes()

            if signature not in unique:

                unique[
                    signature
                ] = c

        new_candidates = list(
            unique.values()
        )

        new_candidates.sort(
            key=score,
            reverse=True
        )

        candidates = new_candidates[
            :MAX_SURVIVORS
        ]

        print(
            f"Depth {depth} survivors: "
            f"{len(candidates):,}"
        )

        for c in candidates:

            all_results.append(
                c.copy()
            )

    return all_results


# ============================================================
# VALIDATION
# ============================================================

def validate(
    training_results,
    validation
):

    conditions = generate_conditions(
        validation
    )

    results = []

    for candidate in training_results:

        parts = candidate[
            "pattern"
        ].split(
            " + "
        )

        mask = pd.Series(
            True,
            index=validation.index
        )

        valid = True

        for part in parts:

            if part not in conditions:

                valid = False
                break

            mask &= conditions[
                part
            ]

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

def final_test(
    validation_results,
    test
):

    conditions = generate_conditions(
        test
    )

    results = []

    for _, candidate in (
        validation_results.iterrows()
    ):

        parts = candidate[
            "pattern"
        ].split(
            " + "
        )

        mask = pd.Series(
            True,
            index=test.index
        )

        valid = True

        for part in parts:

            if part not in conditions:

                valid = False
                break

            mask &= conditions[
                part
            ]

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

def yearly_analysis(
    test,
    pattern,
    conditions
):

    parts = pattern.split(
        " + "
    )

    mask = pd.Series(
        True,
        index=test.index
    )

    for part in parts:

        if part not in conditions:

            return pd.DataFrame()

        mask &= conditions[
            part
        ]

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

        s = calculate_stats(
            group["RETURN"]
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
# MAIN
# ============================================================

def main():

    print()
    print(
        "=" * 100
    )

    print(
        "15-MINUTE REVERSAL BASE STRATEGY OPTIMIZER"
    )

    print(
        "=" * 100
    )

    print()
    print(
        "BASE:"
    )

    print(
        "Previous day 15m first candle = BULL"
    )

    print(
        "Previous day 15m second candle = BEAR"
    )

    print(
        "SHORT next day"
    )

    print()
    print(
        "ENTRY : NEXT DAY 09:15 OPEN"
    )

    print(
        "EXIT  : NEXT DAY 15:27 OPEN"
    )

    print()
    print(
        "NO NEXT-DAY INFORMATION IS USED."
    )

    # ========================================================
    # DATA
    # ========================================================

    df = create_dataset()

    print()
    print(
        f"BASE EVENTS: "
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
    print(
        "=" * 100
    )

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
        f"TRAIN ENDS  : "
        f"{train_end}"
    )

    print(
        f"VALIDATION ENDS : "
        f"{validation_end}"
    )

    # ========================================================
    # BASE PERFORMANCE
    # ========================================================

    base = calculate_stats(
        train["RETURN"]
    )

    print()
    print(
        "=" * 100
    )

    print(
        "BASE TRAINING PERFORMANCE"
    )

    print(
        "=" * 100
    )

    print(
        f"Trades       : "
        f"{base['trades']:,}"
    )

    print(
        f"Win rate     : "
        f"{base['win_rate']:.2f}%"
    )

    print(
        f"Average      : "
        f"{base['average']:.4f}%"
    )

    print(
        f"Profit factor: "
        f"{base['profit_factor']:.3f}"
    )

    # ========================================================
    # CONDITIONS
    # ========================================================

    print()
    print(
        "GENERATING FILTER CONDITIONS..."
    )

    conditions = generate_conditions(
        train
    )

    print(
        f"Conditions: "
        f"{len(conditions):,}"
    )

    # ========================================================
    # SEARCH
    # ========================================================

    training_results = search(
        train,
        conditions
    )

    # Remove private masks.
    training_output = pd.DataFrame([
        {
            k: v
            for k, v in row.items()
            if k != "_mask"
        }
        for row in training_results
    ])

    training_output.to_csv(
        "BASE_STRATEGY_TRAINING.csv",
        index=False
    )

    print()
    print(
        "=" * 100
    )

    print(
        "TOP TRAINING FILTERS"
    )

    print(
        "=" * 100
    )

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

    validation_results = validate(
        training_results,
        validation
    )

    if validation_results.empty:

        print(
            "\nNO PATTERNS SURVIVED VALIDATION."
        )

        return

    validation_results.to_csv(
        "BASE_STRATEGY_VALIDATION.csv",
        index=False
    )

    print()
    print(
        "=" * 100
    )

    print(
        "TOP VALIDATION RESULTS"
    )

    print(
        "=" * 100
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

    final_results = final_test(
        validation_results,
        test
    )

    if final_results.empty:

        print(
            "\nNO PATTERNS SURVIVED FINAL TEST."
        )

        return

    final_results.to_csv(
        "BASE_STRATEGY_FINAL_TEST.csv",
        index=False
    )

    print()
    print(
        "=" * 100
    )

    print(
        "FINAL UNSEEN TEST"
    )

    print(
        "=" * 100
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

    target = final_results[
        final_results[
            "test_win"
        ] >= TARGET_WIN_RATE
    ]

    print()
    print(
        "=" * 100
    )

    print(
        "FINAL TEST >= 85%"
    )

    print(
        "=" * 100
    )

    if target.empty:

        print(
            "NONE"
        )

    else:

        print(
            target.to_string(
                index=False
            )
        )

    # ========================================================
    # BEST
    # ========================================================

    best = final_results.iloc[0]

    print()
    print(
        "=" * 100
    )

    print(
        "BEST FINAL TEST STRATEGY"
    )

    print(
        "=" * 100
    )

    print(
        f"\nPattern:"
    )

    print(
        best["pattern"]
    )

    print(
        f"\nTest trades:"
        f" {int(best['test_trades']):,}"
    )

    print(
        f"Test win rate:"
        f" {best['test_win']:.2f}%"
    )

    print(
        f"Test average:"
        f" {best['test_average']:.4f}%"
    )

    print(
        f"Test profit factor:"
        f" {best['test_pf']:.3f}"
    )

    print(
        f"Test total:"
        f" {best['test_total']:.4f}%"
    )

    # ========================================================
    # YEARLY
    # ========================================================

    test_conditions = generate_conditions(
        test
    )

    yearly = yearly_analysis(
        test,
        best["pattern"],
        test_conditions
    )

    print()
    print(
        "=" * 100
    )

    print(
        "YEAR-BY-YEAR PERFORMANCE"
    )

    print(
        "=" * 100
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
    print(
        "=" * 100
    )

    print(
        "FILES CREATED"
    )

    print(
        "=" * 100
    )

    print(
        "BASE_STRATEGY_TRAINING.csv"
    )

    print(
        "BASE_STRATEGY_VALIDATION.csv"
    )

    print(
        "BASE_STRATEGY_FINAL_TEST.csv"
    )

    print()
    print(
        "SEARCH COMPLETE."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
