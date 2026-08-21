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

MIN_TRAIN_TRADES = 500

TOP_PER_TIMEFRAME = 15

TARGET_WIN_RATE = 85.0


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
# DOWNLOAD
# ============================================================

def download_dataset():

    print("=" * 80)
    print("DOWNLOADING NSE 1-MINUTE DATA")
    print("=" * 80)

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

    data = b"".join(chunks)

    print(
        f"Dataset size: "
        f"{len(data) / (1024 * 1024):.1f} MB"
    )

    return data


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
# CREATE MINUTE LOOKUP
# ============================================================

def make_day_lookup(group):

    lookup = {}

    for _, row in group.iterrows():

        lookup[row["hm"]] = {
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

    return lookup


# ============================================================
# BUILD TIMEFRAME CANDLE
# ============================================================

def build_candle(
    day,
    start_minute,
    minutes
):

    hour = 15

    rows = []

    for i in range(minutes):

        total_minute = (
            start_minute + i
        )

        timestamp = (
            f"{hour:02d}:"
            f"{total_minute:02d}"
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
                r["high"]
                for r in rows
            ),

        "low":
            min(
                r["low"]
                for r in rows
            ),

        "close":
            rows[-1]["close"],

        "volume":
            sum(
                r["volume"]
                for r in rows
            )
    }


# ============================================================
# CANDLE STRUCTURE
# ============================================================

def structure(c):

    if c is None:
        return None

    candle_range = (
        c["high"] -
        c["low"]
    )

    body = abs(
        c["close"] -
        c["open"]
    )

    if candle_range > 0:

        body_ratio = (
            body /
            candle_range
        )

        close_position = (
            c["close"] -
            c["low"]
        ) / candle_range

        upper_wick = (
            c["high"] -
            max(
                c["open"],
                c["close"]
            )
        ) / candle_range

        lower_wick = (
            min(
                c["open"],
                c["close"]
            ) -
            c["low"]
        ) / candle_range

    else:

        body_ratio = 0

        close_position = 0.5

        upper_wick = 0

        lower_wick = 0

    return {

        "range":
            candle_range,

        "body":
            body,

        "body_ratio":
            body_ratio,

        "close_position":
            close_position,

        "upper_wick":
            upper_wick,

        "lower_wick":
            lower_wick
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
# BUILD TIMEFRAME CANDLES
# ============================================================

def build_timeframe_candles(
    day,
    minutes
):

    candles = {}

    # We only need the final part of the day.
    #
    # Generate candles ending around 15:29.
    #
    # Each timeframe is aligned to NSE session-style
    # minute boundaries ending before 15:30.

    starts = []

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

    for start in starts:

        candle = build_candle(
            day,
            start,
            minutes
        )

        if candle is not None:

            candles[start] = candle

    return candles


# ============================================================
# BUILD FEATURES FOR ALL TIMEFRAMES
# ============================================================

def build_features(day):

    result = {}

    for tf, minutes in TIMEFRAMES.items():

        candles = build_timeframe_candles(
            day,
            minutes
        )

        if len(candles) < 2:
            continue

        starts = sorted(
            candles.keys()
        )

        # Last two completed candles
        last_start = starts[-1]

        second_last_start = starts[-2]

        last = candles[
            last_start
        ]

        second_last = candles[
            second_last_start
        ]

        s_last = structure(
            last
        )

        s_second = structure(
            second_last
        )

        d_last = direction(
            last
        )

        d_second = direction(
            second_last
        )

        # Average range/volume of previous candles
        previous = [
            candles[x]
            for x in starts[:-1]
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

        if avg_range > 0:

            range_ratio = (
                s_last["range"] /
                avg_range
            )

        else:

            range_ratio = 0

        if avg_volume > 0:

            relative_volume = (
                last["volume"] /
                avg_volume
            )

        else:

            relative_volume = 0

        if second_last["volume"] > 0:

            volume_ratio = (
                last["volume"] /
                second_last["volume"]
            )

        else:

            volume_ratio = 0

        if last["open"] > 0:

            momentum = (
                (
                    last["close"] -
                    last["open"]
                )
                /
                last["open"]
                *
                100
            )

        else:

            momentum = 0

        prefix = tf

        result[
            f"{prefix}_last"
        ] = d_last

        result[
            f"{prefix}_second"
        ] = d_second

        result[
            f"{prefix}_same"
        ] = (
            d_last ==
            d_second
            and
            d_last != "DOJI"
        )

        result[
            f"{prefix}_opposite"
        ] = (
            d_last !=
            d_second
            and
            d_last != "DOJI"
            and
            d_second != "DOJI"
        )

        result[
            f"{prefix}_body_ratio"
        ] = s_last[
            "body_ratio"
        ]

        result[
            f"{prefix}_close_position"
        ] = s_last[
            "close_position"
        ]

        result[
            f"{prefix}_range_ratio"
        ] = range_ratio

        result[
            f"{prefix}_relative_volume"
        ] = relative_volume

        result[
            f"{prefix}_volume_ratio"
        ] = volume_ratio

        result[
            f"{prefix}_momentum"
        ] = momentum

    return result


# ============================================================
# EXTRACT STOCK OBSERVATIONS
# ============================================================

def extract_stock(
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

    for i, signal_date in enumerate(
        dates
    ):

        if i + 1 >= len(dates):
            break

        trade_date = dates[
            i + 1
        ]

        signal_day = make_day_lookup(
            grouped[
                signal_date
            ]
        )

        features = build_features(
            signal_day
        )

        if not features:
            continue

        trade_day = make_day_lookup(
            grouped[
                trade_date
            ]
        )

        if "09:15" not in trade_day:
            continue

        if "15:29" not in trade_day:
            continue

        entry = trade_day[
            "09:15"
        ]["open"]

        exit_price = trade_day[
            "15:29"
        ]["close"]

        if entry <= 0:
            continue

        long_return = (
            exit_price -
            entry
        ) / entry * 100

        short_return = (
            -long_return
        )

        rows.append({

            "symbol":
                symbol,

            "signal_date":
                signal_date,

            "trade_date":
                trade_date,

            **features,

            "long_return":
                long_return,

            "short_return":
                short_return
        })

    return rows


# ============================================================
# GENERATE PATTERNS
# ============================================================

def generate_patterns(df):

    patterns = {}

    # ========================================================
    # EACH TIMEFRAME INDEPENDENTLY
    # ========================================================

    for tf in TIMEFRAMES.keys():

        last = f"{tf}_last"
        second = f"{tf}_second"

        same = f"{tf}_same"
        opposite = f"{tf}_opposite"

        body = f"{tf}_body_ratio"

        close = f"{tf}_close_position"

        range_ratio = (
            f"{tf}_range_ratio"
        )

        relative_volume = (
            f"{tf}_relative_volume"
        )

        volume_ratio = (
            f"{tf}_volume_ratio"
        )

        momentum = (
            f"{tf}_momentum"
        )

        # ----------------------------------------------------
        # Direction
        # ----------------------------------------------------

        patterns[
            f"{tf}_LAST_BEAR"
        ] = (
            df[last] == "BEAR"
        )

        patterns[
            f"{tf}_LAST_BULL"
        ] = (
            df[last] == "BULL"
        )

        # ----------------------------------------------------
        # Same / opposite
        # ----------------------------------------------------

        patterns[
            f"{tf}_SAME"
        ] = df[same]

        patterns[
            f"{tf}_OPPOSITE"
        ] = df[opposite]

        # ----------------------------------------------------
        # Direction + same/opposite
        # ----------------------------------------------------

        patterns[
            f"{tf}_BEAR_SAME"
        ] = (
            (df[last] == "BEAR")
            &
            df[same]
        )

        patterns[
            f"{tf}_BULL_SAME"
        ] = (
            (df[last] == "BULL")
            &
            df[same]
        )

        patterns[
            f"{tf}_BEAR_OPPOSITE"
        ] = (
            (df[last] == "BEAR")
            &
            df[opposite]
        )

        patterns[
            f"{tf}_BULL_OPPOSITE"
        ] = (
            (df[last] == "BULL")
            &
            df[opposite]
        )

        # ----------------------------------------------------
        # Body strength
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
                (df[last] == "BEAR")
                &
                (df[body] >= level)
            )

            patterns[
                f"{tf}_BULL_BODY>={level:.2f}"
            ] = (
                (df[last] == "BULL")
                &
                (df[body] >= level)
            )

        # ----------------------------------------------------
        # Range expansion
        # ----------------------------------------------------

        for level in [
            1.25,
            1.50,
            1.75,
            2.00,
            2.50,
            3.00
        ]:

            patterns[
                f"{tf}_BEAR_RANGE>={level:.2f}x"
            ] = (
                (df[last] == "BEAR")
                &
                (df[range_ratio] >= level)
            )

            patterns[
                f"{tf}_BULL_RANGE>={level:.2f}x"
            ] = (
                (df[last] == "BULL")
                &
                (df[range_ratio] >= level)
            )

        # ----------------------------------------------------
        # Relative volume
        # ----------------------------------------------------

        for level in [
            1.25,
            1.50,
            1.75,
            2.00,
            2.50,
            3.00
        ]:

            patterns[
                f"{tf}_BEAR_REL_VOL>={level:.2f}x"
            ] = (
                (df[last] == "BEAR")
                &
                (
                    df[relative_volume]
                    >= level
                )
            )

            patterns[
                f"{tf}_BULL_REL_VOL>={level:.2f}x"
            ] = (
                (df[last] == "BULL")
                &
                (
                    df[relative_volume]
                    >= level
                )
            )

            patterns[
                f"{tf}_BEAR_VOL_RATIO>={level:.2f}x"
            ] = (
                (df[last] == "BEAR")
                &
                (
                    df[volume_ratio]
                    >= level
                )
            )

            patterns[
                f"{tf}_BULL_VOL_RATIO>={level:.2f}x"
            ] = (
                (df[last] == "BULL")
                &
                (
                    df[volume_ratio]
                    >= level
                )
            )

        # ----------------------------------------------------
        # Momentum
        # ----------------------------------------------------

        for level in [
            0.05,
            0.10,
            0.15,
            0.20,
            0.30,
            0.50
        ]:

            patterns[
                f"{tf}_BEAR_MOM<={-level:.2f}%"
            ] = (
                (df[last] == "BEAR")
                &
                (
                    df[momentum]
                    <= -level
                )
            )

            patterns[
                f"{tf}_BULL_MOM>={level:.2f}%"
            ] = (
                (df[last] == "BULL")
                &
                (
                    df[momentum]
                    >= level
                )
            )

        # ----------------------------------------------------
        # High conviction combinations
        # ----------------------------------------------------

        for body_level in [
            0.60,
            0.70,
            0.80
        ]:

            for volume_level in [
                1.25,
                1.50,
                2.00
            ]:

                patterns[
                    (
                        f"{tf}_BEAR_"
                        f"BODY>={body_level:.2f}_"
                        f"REL_VOL>={volume_level:.2f}"
                    )
                ] = (
                    (df[last] == "BEAR")
                    &
                    (
                        df[body]
                        >= body_level
                    )
                    &
                    (
                        df[relative_volume]
                        >= volume_level
                    )
                )

                patterns[
                    (
                        f"{tf}_BULL_"
                        f"BODY>={body_level:.2f}_"
                        f"REL_VOL>={volume_level:.2f}"
                    )
                ] = (
                    (df[last] == "BULL")
                    &
                    (
                        df[body]
                        >= body_level
                    )
                    &
                    (
                        df[relative_volume]
                        >= volume_level
                    )
                )

        # ----------------------------------------------------
        # Body + volume + same/opposite
        # ----------------------------------------------------

        for body_level in [
            0.60,
            0.70,
            0.80
        ]:

            for volume_level in [
                1.50,
                2.00
            ]:

                patterns[
                    (
                        f"{tf}_BEAR_"
                        f"BODY>={body_level:.2f}_"
                        f"VOL>={volume_level:.2f}_"
                        f"OPPOSITE"
                    )
                ] = (
                    (df[last] == "BEAR")
                    &
                    (
                        df[body]
                        >= body_level
                    )
                    &
                    (
                        df[relative_volume]
                        >= volume_level
                    )
                    &
                    df[opposite]
                )

                patterns[
                    (
                        f"{tf}_BULL_"
                        f"BODY>={body_level:.2f}_"
                        f"VOL>={volume_level:.2f}_"
                        f"OPPOSITE"
                    )
                ] = (
                    (df[last] == "BULL")
                    &
                    (
                        df[body]
                        >= body_level
                    )
                    &
                    (
                        df[relative_volume]
                        >= volume_level
                    )
                    &
                    df[opposite]
                )

    # ========================================================
    # CROSS-TIMEFRAME ALIGNMENT
    # ========================================================

    for tf1 in TIMEFRAMES.keys():

        for tf2 in TIMEFRAMES.keys():

            if tf1 == tf2:
                continue

            patterns[
                f"{tf1}_BEAR = {tf2}_BEAR"
            ] = (
                (df[f"{tf1}_last"] == "BEAR")
                &
                (df[f"{tf2}_last"] == "BEAR")
            )

            patterns[
                f"{tf1}_BULL = {tf2}_BULL"
            ] = (
                (df[f"{tf1}_last"] == "BULL")
                &
                (df[f"{tf2}_last"] == "BULL")
            )

    return patterns


# ============================================================
# STATISTICS
# ============================================================

def stats(series):

    if len(series) == 0:
        return None

    wins = (
        series > 0
    ).sum()

    win_rate = (
        wins /
        len(series) *
        100
    )

    gross_profit = (
        series[
            series > 0
        ].sum()
    )

    gross_loss = abs(
        series[
            series < 0
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

        "trades":
            len(series),

        "win_rate":
            win_rate,

        "average":
            series.mean(),

        "profit_factor":
            pf,

        "total":
            series.sum()
    }


# ============================================================
# EVALUATE TRAINING PATTERN
# ============================================================

def evaluate_training(
    df,
    mask
):

    subset = df[
        mask
    ]

    if len(subset) < MIN_TRAIN_TRADES:
        return None

    long_stats = stats(
        subset["long_return"]
    )

    short_stats = stats(
        subset["short_return"]
    )

    # Select direction ONLY from training data.
    if (
        long_stats["average"]
        >=
        short_stats["average"]
    ):

        side = "LONG"

        selected = long_stats

    else:

        side = "SHORT"

        selected = short_stats

    return {

        "direction":
            side,

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
# VALIDATE ON UNSEEN DATA
# ============================================================

def validate(
    df,
    mask,
    side
):

    subset = df[
        mask
    ]

    if len(subset) == 0:
        return None

    if side == "LONG":

        returns = (
            subset["long_return"]
        )

    else:

        returns = (
            subset["short_return"]
        )

    return stats(
        returns
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 80)
    print(
        "MULTI-TIMEFRAME HIGH-WIN-RATE SCANNER"
    )
    print("=" * 80)

    print(
        "\nTimeframes:"
    )

    print(
        "1m | 2m | 3m | 5m | 10m | 15m"
    )

    print(
        f"\nTarget: "
        f"{TARGET_WIN_RATE:.0f}%+"
    )

    print(
        f"Minimum training trades: "
        f"{MIN_TRAIN_TRADES}"
    )

    print(
        "70% training / 30% unseen test"
    )

    # ========================================================
    # DOWNLOAD
    # ========================================================

    zip_bytes = download_dataset()

    z = zipfile.ZipFile(
        io.BytesIO(zip_bytes)
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
    # PROCESS
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

        rows = extract_stock(
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

    df["signal_date"] = pd.to_datetime(
        df["signal_date"]
    )

    print(
        f"Total observations: "
        f"{len(df):,}"
    )

    # ========================================================
    # SPLIT DATA
    # ========================================================

    dates = sorted(
        df["signal_date"].unique()
    )

    split_index = int(
        len(dates) *
        TRAIN_PERCENT
    )

    split_date = dates[
        split_index
    ]

    train = df[
        df["signal_date"] <
        split_date
    ].copy()

    test = df[
        df["signal_date"] >=
        split_date
    ].copy()

    print(
        f"\nTraining observations: "
        f"{len(train):,}"
    )

    print(
        f"Test observations: "
        f"{len(test):,}"
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
    # GENERATE PATTERNS
    # ========================================================

    train_patterns = generate_patterns(
        train
    )

    test_patterns = generate_patterns(
        test
    )

    print(
        f"\nTotal patterns tested: "
        f"{len(train_patterns)}"
    )

    # ========================================================
    # SEARCH
    # ========================================================

    discovered = []

    for name, mask in train_patterns.items():

        result = evaluate_training(
            train,
            mask
        )

        if result is None:
            continue

        # Determine timeframe from name
        timeframe = "mixed"

        for tf in TIMEFRAMES:

            if name.startswith(
                tf + "_"
            ):

                timeframe = tf

                break

        result["pattern"] = name

        result["timeframe"] = timeframe

        discovered.append(
            result
        )

    if not discovered:

        print(
            "No qualifying patterns."
        )

        return

    discovered_df = pd.DataFrame(
        discovered
    )

    # ========================================================
    # TOP BY TIMEFRAME
    # ========================================================

    print("\n")
    print("=" * 80)
    print(
        "BEST TRAINING PATTERN "
        "FOR EACH TIMEFRAME"
    )
    print("=" * 80)

    for tf in TIMEFRAMES.keys():

        subset = discovered_df[
            discovered_df[
                "timeframe"
            ] == tf
        ]

        if subset.empty:
            continue

        best = subset.sort_values(
            [
                "win_rate",
                "trades"
            ],
            ascending=[
                False,
                False
            ]
        ).iloc[0]

        print(
            f"\n{tf}"
        )

        print(
            f"  Pattern: "
            f"{best['pattern']}"
        )

        print(
            f"  Direction: "
            f"{best['direction']}"
        )

        print(
            f"  Trades: "
            f"{int(best['trades'])}"
        )

        print(
            f"  Win rate: "
            f"{best['win_rate']:.2f}%"
        )

        print(
            f"  Average: "
            f"{best['average']:.4f}%"
        )

        print(
            f"  PF: "
            f"{best['profit_factor']:.3f}"
        )

    # ========================================================
    # TOP OVERALL
    # ========================================================

    top_overall = (
        discovered_df
        .sort_values(
            [
                "win_rate",
                "trades"
            ],
            ascending=[
                False,
                False
            ]
        )
        .head(
            TOP_PER_TIMEFRAME
            * len(TIMEFRAMES)
        )
    )

    print("\n")
    print("=" * 80)
    print(
        "TOP TRAINING PATTERNS "
        "ACROSS ALL TIMEFRAMES"
    )
    print("=" * 80)

    print(
        top_overall[
            [
                "timeframe",
                "pattern",
                "trades",
                "direction",
                "win_rate",
                "average",
                "profit_factor"
            ]
        ].to_string(
            index=False
        )
    )

    # ========================================================
    # 85% TRAINING PATTERNS
    # ========================================================

    high_training = discovered_df[
        discovered_df[
            "win_rate"
        ] >= TARGET_WIN_RATE
    ]

    print("\n")
    print("=" * 80)
    print(
        "85%+ TRAINING PATTERNS"
    )
    print("=" * 80)

    if high_training.empty:

        print(
            "NONE reached 85% with "
            f"{MIN_TRAIN_TRADES}+ trades."
        )

    else:

        print(
            high_training[
                [
                    "timeframe",
                    "pattern",
                    "trades",
                    "direction",
                    "win_rate",
                    "average",
                    "profit_factor"
                ]
            ]
            .sort_values(
                "win_rate",
                ascending=False
            )
            .to_string(
                index=False
            )
        )

    # ========================================================
    # VALIDATE TOP PATTERNS
    # ========================================================

    validation = []

    for _, row in top_overall.iterrows():

        name = row[
            "pattern"
        ]

        side = row[
            "direction"
        ]

        if name not in test_patterns:
            continue

        result = validate(
            test,
            test_patterns[name],
            side
        )

        if result is None:
            continue

        validation.append({

            "timeframe":
                row["timeframe"],

            "pattern":
                name,

            "direction":
                side,

            "train_trades":
                row["trades"],

            "train_win_rate":
                row["win_rate"],

            "train_average":
                row["average"],

            "train_pf":
                row["profit_factor"],

            "test_trades":
                result["trades"],

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

    # ========================================================
    # UNSEEN RESULTS
    # ========================================================

    print("\n")
    print("=" * 80)
    print(
        "UNSEEN TEST RESULTS"
    )
    print("=" * 80)

    if validation_df.empty:

        print(
            "No validation results."
        )

    else:

        print(
            validation_df[
                [
                    "timeframe",
                    "pattern",
                    "direction",
                    "train_win_rate",
                    "test_trades",
                    "test_win_rate",
                    "test_average",
                    "test_pf"
                ]
            ]
            .sort_values(
                [
                    "test_win_rate",
                    "test_trades"
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

    # ========================================================
    # 85% UNSEEN RESULTS
    # ========================================================

    if not validation_df.empty:

        high_test = validation_df[
            (
                validation_df[
                    "test_win_rate"
                ] >= TARGET_WIN_RATE
            )
        ]

    else:

        high_test = pd.DataFrame()

    print("\n")
    print("=" * 80)
    print(
        "85%+ UNSEEN PATTERNS"
    )
    print("=" * 80)

    if high_test.empty:

        print(
            "NONE of the tested top patterns "
            "reached 85% on unseen data."
        )

    else:

        print(
            high_test[
                [
                    "timeframe",
                    "pattern",
                    "direction",
                    "test_trades",
                    "test_win_rate",
                    "test_average",
                    "test_pf"
                ]
            ]
            .sort_values(
                "test_win_rate",
                ascending=False
            )
            .to_string(
                index=False
            )
        )

    # ========================================================
    # BEST TIMEFRAME OUT OF SAMPLE
    # ========================================================

    if not validation_df.empty:

        print("\n")
        print("=" * 80)
        print(
            "BEST OUT-OF-SAMPLE "
            "PATTERN BY TIMEFRAME"
        )
        print("=" * 80)

        for tf in TIMEFRAMES.keys():

            subset = validation_df[
                validation_df[
                    "timeframe"
                ] == tf
            ]

            if subset.empty:
                continue

            best = subset.sort_values(
                [
                    "test_win_rate",
                    "test_trades"
                ],
                ascending=[
                    False,
                    False
                ]
            ).iloc[0]

            print(
                f"\n{tf}"
            )

            print(
                f"  Pattern: "
                f"{best['pattern']}"
            )

            print(
                f"  Direction: "
                f"{best['direction']}"
            )

            print(
                f"  Test trades: "
                f"{int(best['test_trades'])}"
            )

            print(
                f"  Test win rate: "
                f"{best['test_win_rate']:.2f}%"
            )

            print(
                f"  Test average: "
                f"{best['test_average']:.4f}%"
            )

            print(
                f"  Test PF: "
                f"{best['test_pf']:.3f}"
            )

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    discovered_df.to_csv(
        "all_timeframe_training_results.csv",
        index=False
    )

    validation_df.to_csv(
        "all_timeframe_unseen_results.csv",
        index=False
    )

    print("\n")
    print("=" * 80)
    print(
        "SCAN COMPLETE"
    )
    print("=" * 80)

    print(
        "\nCreated:"
    )

    print(
        "all_timeframe_training_results.csv"
    )

    print(
        "all_timeframe_unseen_results.csv"
    )

    print(
        "\nRemember:"
    )

    print(
        "An 85% result is only interesting "
        "if it also survives the unseen period "
        "with a meaningful number of trades."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
