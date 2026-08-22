import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os


# ============================================================
# SETTINGS
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/nse-fno-1min-data/"
    "releases/download/v1.0.0/stocks_1m_csvs.zip"
)

TRAIN_PERCENT = 0.70

MIN_TRAIN_EVENTS = 100

TOP_RANKS = [5, 10, 20, 50]

TARGET_WIN_RATE = 85.0

TIMEFRAMES = {
    "1m": 1,
    "2m": 2,
    "3m": 3,
    "5m": 5,
    "10m": 10,
    "15m": 15
}


# ============================================================
# DOWNLOAD DATA
# ============================================================

def download_dataset():

    print("=" * 80)
    print("DOWNLOADING NSE 1-MINUTE DATA")
    print("=" * 80)

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
                    f"\rDownloaded: "
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
# CANDLE
# ============================================================

def build_candle(
    day,
    start_minute,
    minutes
):

    rows = []

    for i in range(minutes):

        minute = (
            start_minute + i
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
# CANDLE STRUCTURE
# ============================================================

def structure(c):

    if c is None:

        return None

    rng = (
        c["high"] -
        c["low"]
    )

    body = abs(
        c["close"] -
        c["open"]
    )

    if rng > 0:

        body_ratio = (
            body / rng
        )

        close_position = (
            c["close"] -
            c["low"]
        ) / rng

    else:

        body_ratio = 0

        close_position = 0.5

    return {

        "range":
            rng,

        "body":
            body,

        "body_ratio":
            body_ratio,

        "close_position":
            close_position
    }


# ============================================================
# DIRECTION
# ============================================================

def direction(c):

    if c["close"] > c["open"]:

        return "BULL"

    if c["close"] < c["open"]:

        return "BEAR"

    return "DOJI"


# ============================================================
# BUILD ALL EOD FEATURES
# ============================================================

def build_features(day):

    result = {}

    for tf, minutes in TIMEFRAMES.items():

        if minutes == 1:

            starts = [
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

        candles = {}

        for start in starts:

            c = build_candle(
                day,
                start,
                minutes
            )

            if c is not None:

                candles[start] = c

        if len(candles) < 2:

            continue

        ordered = sorted(
            candles.keys()
        )

        second_last = candles[
            ordered[-2]
        ]

        last = candles[
            ordered[-1]
        ]

        s = structure(
            last
        )

        previous = [
            candles[x]
            for x in ordered[:-1]
        ]

        if previous:

            avg_range = np.mean([
                structure(x)["range"]
                for x in previous
            ])

            avg_volume = np.mean([
                x["volume"]
                for x in previous
            ])

        else:

            avg_range = 0

            avg_volume = 0

        last_direction = direction(
            last
        )

        second_direction = direction(
            second_last
        )

        result[
            f"{tf}_last"
        ] = last_direction

        result[
            f"{tf}_second"
        ] = second_direction

        result[
            f"{tf}_same"
        ] = (
            last_direction ==
            second_direction
            and
            last_direction != "DOJI"
        )

        result[
            f"{tf}_opposite"
        ] = (
            last_direction !=
            second_direction
            and
            last_direction != "DOJI"
            and
            second_direction != "DOJI"
        )

        result[
            f"{tf}_body"
        ] = s["body_ratio"]

        result[
            f"{tf}_close"
        ] = s["close_position"]

        result[
            f"{tf}_range_ratio"
        ] = (
            s["range"] /
            avg_range
            if avg_range > 0
            else 0
        )

        result[
            f"{tf}_relative_volume"
        ] = (
            last["volume"] /
            avg_volume
            if avg_volume > 0
            else 0
        )

        result[
            f"{tf}_volume_ratio"
        ] = (
            last["volume"] /
            second_last["volume"]
            if second_last["volume"] > 0
            else 0
        )

        result[
            f"{tf}_momentum"
        ] = (
            (
                last["close"] -
                last["open"]
            )
            /
            last["open"]
            * 100
            if last["open"] > 0
            else 0
        )

    return result


# ============================================================
# EXTRACT DAILY STOCK DATA
# ============================================================

def extract_daily_data(
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

    symbol = os.path.basename(
        filename
    ).replace(
        "_1m.csv.gz",
        ""
    )

    rows = []

    for date, group in grouped.items():

        day = make_day_lookup(
            group
        )

        # Need both entry and exit
        if "09:15" not in day:
            continue

        if "15:27" not in day:
            continue

        if "15:29" not in day:
            continue

        day_open = day[
            "09:15"
        ]["open"]

        day_exit = day[
            "15:27"
        ]["open"]

        day_close = day[
            "15:29"
        ]["close"]

        if day_open <= 0:

            continue

        daily_return = (
            (
                day_close -
                day_open
            )
            /
            day_open
            * 100
        )

        rows.append({

            "symbol":
                symbol,

            "date":
                date,

            "day_open":
                day_open,

            "exit_15_27":
                day_exit,

            "day_close":
                day_close,

            "daily_return":
                daily_return,

            "long_return_15_27":
                (
                    (
                        day_exit -
                        day_open
                    )
                    /
                    day_open
                    * 100
                ),

            "short_return_15_27":
                (
                    (
                        day_open -
                        day_exit
                    )
                    /
                    day_open
                    * 100
                )
        })

    return rows


# ============================================================
# BUILD PREVIOUS-DAY SIGNALS
# ============================================================

def build_event_dataset(
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

        previous_date = dates[
            i - 1
        ]

        event_date = dates[
            i
        ]

        previous_day = make_day_lookup(
            grouped[
                previous_date
            ]
        )

        event_day = make_day_lookup(
            grouped[
                event_date
            ]
        )

        # ----------------------------------------------------
        # Previous day = signal
        # Event day = outcome
        # ----------------------------------------------------

        features = build_features(
            previous_day
        )

        if not features:

            continue

        if "09:15" not in event_day:

            continue

        if "15:27" not in event_day:

            continue

        event_open = event_day[
            "09:15"
        ]["open"]

        event_exit = event_day[
            "15:27"
        ]["open"]

        if event_open <= 0:

            continue

        event_return = (
            (
                event_exit -
                event_open
            )
            /
            event_open
            * 100
        )

        rows.append({

            "symbol":
                symbol,

            "signal_date":
                previous_date,

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
# DAILY EXTREME CLASSIFICATION
# ============================================================

def classify_daily_events(
    df,
    top_n
):

    df = df.copy()

    df["rank_gain"] = (
        df.groupby(
            "event_date"
        )["event_return"]
        .rank(
            method="first",
            ascending=False
        )
    )

    df["rank_loss"] = (
        df.groupby(
            "event_date"
        )["event_return"]
        .rank(
            method="first",
            ascending=True
        )
    )

    gainers = df[
        df["rank_gain"] <= top_n
    ].copy()

    losers = df[
        df["rank_loss"] <= top_n
    ].copy()

    gainers["event_type"] = (
        "TOP_GAINER"
    )

    losers["event_type"] = (
        "TOP_LOSER"
    )

    return pd.concat(
        [
            gainers,
            losers
        ],
        ignore_index=True
    )


# ============================================================
# PATTERN GENERATION
# ============================================================

def generate_patterns(df):

    patterns = {}

    for tf in TIMEFRAMES.keys():

        last = (
            df[f"{tf}_last"]
        )

        second = (
            df[f"{tf}_second"]
        )

        # ----------------------------------------------------
        # Direction
        # ----------------------------------------------------

        patterns[
            f"{tf}_BEAR"
        ] = (
            last == "BEAR"
        )

        patterns[
            f"{tf}_BULL"
        ] = (
            last == "BULL"
        )

        # ----------------------------------------------------
        # Same / opposite
        # ----------------------------------------------------

        patterns[
            f"{tf}_SAME"
        ] = df[
            f"{tf}_same"
        ]

        patterns[
            f"{tf}_OPPOSITE"
        ] = df[
            f"{tf}_opposite"
        ]

        # ----------------------------------------------------
        # Body
        # ----------------------------------------------------

        for level in [
            0.50,
            0.60,
            0.70,
            0.80,
            0.90
        ]:

            patterns[
                f"{tf}_BEAR_BODY>={level:.2f}"
            ] = (
                (last == "BEAR")
                &
                (
                    df[f"{tf}_body"]
                    >= level
                )
            )

            patterns[
                f"{tf}_BULL_BODY>={level:.2f}"
            ] = (
                (last == "BULL")
                &
                (
                    df[f"{tf}_body"]
                    >= level
                )
            )

        # ----------------------------------------------------
        # Relative volume
        # ----------------------------------------------------

        for level in [
            1.25,
            1.50,
            2.00,
            2.50,
            3.00
        ]:

            patterns[
                f"{tf}_BEAR_REL_VOL>={level:.2f}"
            ] = (
                (last == "BEAR")
                &
                (
                    df[
                        f"{tf}_relative_volume"
                    ]
                    >= level
                )
            )

            patterns[
                f"{tf}_BULL_REL_VOL>={level:.2f}"
            ] = (
                (last == "BULL")
                &
                (
                    df[
                        f"{tf}_relative_volume"
                    ]
                    >= level
                )
            )

        # ----------------------------------------------------
        # Volume compared with previous candle
        # ----------------------------------------------------

        for level in [
            1.25,
            1.50,
            2.00
        ]:

            patterns[
                f"{tf}_BEAR_VOL_MORE"
            ] = (
                (last == "BEAR")
                &
                (
                    df[
                        f"{tf}_volume_ratio"
                    ]
                    >= level
                )
            )

            patterns[
                f"{tf}_BULL_VOL_MORE"
            ] = (
                (last == "BULL")
                &
                (
                    df[
                        f"{tf}_volume_ratio"
                    ]
                    >= level
                )
            )

        # ----------------------------------------------------
        # Range
        # ----------------------------------------------------

        for level in [
            1.25,
            1.50,
            2.00,
            2.50
        ]:

            patterns[
                f"{tf}_BEAR_RANGE>={level:.2f}"
            ] = (
                (last == "BEAR")
                &
                (
                    df[
                        f"{tf}_range_ratio"
                    ]
                    >= level
                )
            )

            patterns[
                f"{tf}_BULL_RANGE>={level:.2f}"
            ] = (
                (last == "BULL")
                &
                (
                    df[
                        f"{tf}_range_ratio"
                    ]
                    >= level
                )
            )

        # ----------------------------------------------------
        # Momentum
        # ----------------------------------------------------

        for level in [
            0.05,
            0.10,
            0.20,
            0.30,
            0.50
        ]:

            patterns[
                f"{tf}_BEAR_MOM<={-level:.2f}"
            ] = (
                (last == "BEAR")
                &
                (
                    df[
                        f"{tf}_momentum"
                    ]
                    <= -level
                )
            )

            patterns[
                f"{tf}_BULL_MOM>={level:.2f}"
            ] = (
                (last == "BULL")
                &
                (
                    df[
                        f"{tf}_momentum"
                    ]
                    >= level
                )
            )

        # ----------------------------------------------------
        # Direction + opposite
        # ----------------------------------------------------

        patterns[
            f"{tf}_BEAR_OPPOSITE"
        ] = (
            (last == "BEAR")
            &
            df[f"{tf}_opposite"]
        )

        patterns[
            f"{tf}_BULL_OPPOSITE"
        ] = (
            (last == "BULL")
            &
            df[f"{tf}_opposite"]
        )

        # ----------------------------------------------------
        # Direction + same
        # ----------------------------------------------------

        patterns[
            f"{tf}_BEAR_SAME"
        ] = (
            (last == "BEAR")
            &
            df[f"{tf}_same"]
        )

        patterns[
            f"{tf}_BULL_SAME"
        ] = (
            (last == "BULL")
            &
            df[f"{tf}_same"]
        )

    # ========================================================
    # CROSS-TIMEFRAME ALIGNMENT
    # ========================================================

    for tf1 in TIMEFRAMES.keys():

        for tf2 in TIMEFRAMES.keys():

            if tf1 == tf2:

                continue

            patterns[
                f"{tf1}_BEAR + {tf2}_BEAR"
            ] = (
                (df[f"{tf1}_last"] == "BEAR")
                &
                (df[f"{tf2}_last"] == "BEAR")
            )

            patterns[
                f"{tf1}_BULL + {tf2}_BULL"
            ] = (
                (df[f"{tf1}_last"] == "BULL")
                &
                (df[f"{tf2}_last"] == "BULL")
            )

            patterns[
                f"{tf1}_BEAR + {tf2}_BULL"
            ] = (
                (df[f"{tf1}_last"] == "BEAR")
                &
                (df[f"{tf2}_last"] == "BULL")
            )

            patterns[
                f"{tf1}_BULL + {tf2}_BEAR"
            ] = (
                (df[f"{tf1}_last"] == "BULL")
                &
                (df[f"{tf2}_last"] == "BEAR")
            )

    # ========================================================
    # ALL-TIMEFRAME ALIGNMENT
    # ========================================================

    bear_conditions = [
        df[f"{tf}_last"] == "BEAR"
        for tf in TIMEFRAMES
    ]

    bull_conditions = [
        df[f"{tf}_last"] == "BULL"
        for tf in TIMEFRAMES
    ]

    patterns[
        "ALL_TIMEFRAMES_BEAR"
    ] = np.logical_and.reduce(
        bear_conditions
    )

    patterns[
        "ALL_TIMEFRAMES_BULL"
    ] = np.logical_and.reduce(
        bull_conditions
    )

    return patterns


# ============================================================
# EVENT STATISTICS
# ============================================================

def event_statistics(
    subset,
    predicted_direction
):

    if len(subset) == 0:

        return None

    if predicted_direction == "LONG":

        returns = (
            subset["event_return"]
        )

    else:

        returns = (
            -subset["event_return"]
        )

    wins = (
        returns > 0
    ).sum()

    win_rate = (
        wins /
        len(returns)
        *
        100
    )

    avg = returns.mean()

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

        pf = float("inf")

    return {

        "events":
            len(returns),

        "win_rate":
            win_rate,

        "average":
            avg,

        "profit_factor":
            pf,

        "total":
            returns.sum()
    }


# ============================================================
# FIND BEST DIRECTION
# ============================================================

def evaluate_pattern(
    df,
    mask
):

    subset = df[
        mask
    ]

    if len(subset) < MIN_TRAIN_EVENTS:

        return None

    long_stats = event_statistics(
        subset,
        "LONG"
    )

    short_stats = event_statistics(
        subset,
        "SHORT"
    )

    if (
        long_stats["win_rate"]
        >=
        short_stats["win_rate"]
    ):

        direction_selected = "LONG"

        selected = long_stats

    else:

        direction_selected = "SHORT"

        selected = short_stats

    return {

        "direction":
            direction_selected,

        "events":
            selected["events"],

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
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 80)
    print(
        "DAILY TOP GAINER / TOP LOSER "
        "PREVIOUS-DAY PATTERN SCANNER"
    )
    print("=" * 80)

    print(
        "\nSignal:"
    )

    print(
        "Previous trading day EOD"
    )

    print(
        "\nTrade:"
    )

    print(
        "Next day 09:15 open -> 15:27 open"
    )

    print(
        "\nTimeframes:"
    )

    print(
        "1m | 2m | 3m | 5m | 10m | 15m"
    )

    print(
        "\nTop ranks:"
    )

    print(
        TOP_RANKS
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
        f"\nStocks found: "
        f"{len(files)}"
    )

    # ========================================================
    # BUILD EVENT DATASET
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

        rows = build_event_dataset(
            z,
            filename
        )

        all_rows.extend(
            rows
        )

    print("\n")

    if not all_rows:

        print(
            "No observations."
        )

        return

    df = pd.DataFrame(
        all_rows
    )

    df["event_date"] = pd.to_datetime(
        df["event_date"]
    )

    df["signal_date"] = pd.to_datetime(
        df["signal_date"]
    )

    print(
        f"Total stock/day observations: "
        f"{len(df):,}"
    )

    # ========================================================
    # CHRONOLOGICAL SPLIT
    # ========================================================

    dates = sorted(
        df["event_date"].unique()
    )

    split_index = int(
        len(dates)
        *
        TRAIN_PERCENT
    )

    split_date = dates[
        split_index
    ]

    train_base = df[
        df["event_date"] <
        split_date
    ].copy()

    test_base = df[
        df["event_date"] >=
        split_date
    ].copy()

    print(
        f"\nTraining observations: "
        f"{len(train_base):,}"
    )

    print(
        f"Test observations: "
        f"{len(test_base):,}"
    )

    print(
        f"Training ends before: "
        f"{split_date}"
    )

    print(
        f"Test starts: "
        f"{split_date}"
    )

    # ========================================================
    # RUN EACH TOP-N
    # ========================================================

    for top_n in TOP_RANKS:

        print("\n")
        print("=" * 80)
        print(
            f"TOP {top_n} DAILY GAINERS / LOSERS"
        )
        print("=" * 80)

        train = classify_daily_events(
            train_base,
            top_n
        )

        test = classify_daily_events(
            test_base,
            top_n
        )

        print(
            f"\nTraining events: "
            f"{len(train):,}"
        )

        print(
            f"Test events: "
            f"{len(test):,}"
        )

        # ----------------------------------------------------
        # Generate patterns
        # ----------------------------------------------------

        train_patterns = generate_patterns(
            train
        )

        test_patterns = generate_patterns(
            test
        )

        discovered = []

        for name, mask in (
            train_patterns.items()
        ):

            result = evaluate_pattern(
                train,
                mask
            )

            if result is None:

                continue

            discovered.append({

                "top_n":
                    top_n,

                "pattern":
                    name,

                **result
            })

        if not discovered:

            print(
                "\nNo qualifying patterns."
            )

            continue

        discovered_df = pd.DataFrame(
            discovered
        )

        # ----------------------------------------------------
        # TOP TRAINING PATTERNS
        # ----------------------------------------------------

        top_training = (
            discovered_df
            .sort_values(
                [
                    "win_rate",
                    "events"
                ],
                ascending=[
                    False,
                    False
                ]
            )
            .head(30)
        )

        print("\n")
        print(
            "TOP TRAINING PATTERNS"
        )

        print(
            top_training.to_string(
                index=False
            )
        )

        # ----------------------------------------------------
        # 85% TRAINING
        # ----------------------------------------------------

        high_training = discovered_df[
            discovered_df[
                "win_rate"
            ] >= TARGET_WIN_RATE
        ]

        print("\n")
        print(
            f"85%+ TRAINING PATTERNS"
        )

        if high_training.empty:

            print(
                "NONE"
            )

        else:

            print(
                high_training
                .sort_values(
                    "win_rate",
                    ascending=False
                )
                .to_string(
                    index=False
                )
            )

        # ----------------------------------------------------
        # UNSEEN TEST
        # ----------------------------------------------------

        validation = []

        for _, row in top_training.iterrows():

            name = row[
                "pattern"
            ]

            side = row[
                "direction"
            ]

            if name not in test_patterns:

                continue

            mask = test_patterns[
                name
            ]

            subset = test[
                mask
            ]

            if len(subset) == 0:

                continue

            result = event_statistics(
                subset,
                side
            )

            if result is None:

                continue

            validation.append({

                "top_n":
                    top_n,

                "pattern":
                    name,

                "direction":
                    side,

                "train_events":
                    row["events"],

                "train_win_rate":
                    row["win_rate"],

                "train_average":
                    row["average"],

                "train_pf":
                    row["profit_factor"],

                "test_events":
                    result["events"],

                "test_win_rate":
                    result["win_rate"],

                "test_average":
                    result["average"],

                "test_pf":
                    result["profit_factor"],

                "test_total":
                    result["total"]
            })

        validation_df = pd.DataFrame(
            validation
        )

        # ----------------------------------------------------
        # TEST RESULTS
        # ----------------------------------------------------

        print("\n")
        print(
            "UNSEEN TEST RESULTS"
        )

        if validation_df.empty:

            print(
                "NONE"
            )

        else:

            print(
                validation_df
                .sort_values(
                    [
                        "test_win_rate",
                        "test_events"
                    ],
                    ascending=[
                        False,
                        False
                    ]
                )
                .to_string(
                    index=False
                )
            )

        # ----------------------------------------------------
        # 85% UNSEEN
        # ----------------------------------------------------

        if not validation_df.empty:

            high_test = validation_df[
                validation_df[
                    "test_win_rate"
                ] >= TARGET_WIN_RATE
            ]

        else:

            high_test = pd.DataFrame()

        print("\n")
        print(
            "85%+ UNSEEN PATTERNS"
        )

        if high_test.empty:

            print(
                "NONE"
            )

        else:

            print(
                high_test
                .sort_values(
                    "test_win_rate",
                    ascending=False
                )
                .to_string(
                    index=False
                )
            )

        # ----------------------------------------------------
        # SAVE
        # ----------------------------------------------------

        discovered_df.to_csv(
            f"daily_extreme_training_top{top_n}.csv",
            index=False
        )

        validation_df.to_csv(
            f"daily_extreme_test_top{top_n}.csv",
            index=False
        )

    # ========================================================
    # FINISH
    # ========================================================

    print("\n")
    print("=" * 80)
    print(
        "DAILY EXTREME-MOVER SCAN COMPLETE"
    )
    print("=" * 80)

    print(
        "\nImportant:"
    )

    print(
        "The signal comes ONLY from the "
        "previous trading day's EOD."
    )

    print(
        "The extreme-mover classification "
        "comes from the following day."
    )

    print(
        "Therefore the pattern does not "
        "know in advance which stock will "
        "be a top gainer or loser."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
