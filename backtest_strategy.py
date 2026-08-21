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

MIN_TRAIN_TRADES = 200
TOP_PATTERNS = 10

# 70% training / 30% unseen test
TRAIN_PERCENT = 0.70


# ============================================================
# TREND
# ============================================================

def direction(open_price, close_price):

    if close_price > open_price:
        return "BULL"

    if close_price < open_price:
        return "BEAR"

    return "DOJI"


# ============================================================
# DOWNLOAD
# ============================================================

def download_dataset():

    print("=" * 75)
    print("DOWNLOADING NSE 1-MINUTE DATA")
    print("=" * 75)

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
# BUILD CANDLE
# ============================================================

def build_candle(
    day,
    start_time,
    minutes
):

    hour, minute = map(
        int,
        start_time.split(":")
    )

    rows = []

    for i in range(minutes):

        timestamp = (
            f"{hour:02d}:{minute + i:02d}"
        )

        if timestamp not in day:
            return None

        rows.append(
            day[timestamp]
        )

    return {

        "open":
            float(rows[0]["open"]),

        "high":
            max(
                float(x["high"])
                for x in rows
            ),

        "low":
            min(
                float(x["low"])
                for x in rows
            ),

        "close":
            float(rows[-1]["close"]),

        "volume":
            sum(
                float(x["volume"])
                for x in rows
            )
    }


# ============================================================
# FEATURES
# ============================================================

def build_features(day):

    c5_1520 = build_candle(
        day,
        "15:20",
        5
    )

    c5_1525 = build_candle(
        day,
        "15:25",
        5
    )

    c3_1524 = build_candle(
        day,
        "15:24",
        3
    )

    c3_1527 = build_candle(
        day,
        "15:27",
        3
    )

    c1_1528 = build_candle(
        day,
        "15:28",
        1
    )

    c1_1529 = build_candle(
        day,
        "15:29",
        1
    )

    candles = [
        c5_1520,
        c5_1525,
        c3_1524,
        c3_1527,
        c1_1528,
        c1_1529
    ]

    if any(
        x is None
        for x in candles
    ):
        return None

    d5_1520 = direction(
        c5_1520["open"],
        c5_1520["close"]
    )

    d5_1525 = direction(
        c5_1525["open"],
        c5_1525["close"]
    )

    d3_1524 = direction(
        c3_1524["open"],
        c3_1524["close"]
    )

    d3_1527 = direction(
        c3_1527["open"],
        c3_1527["close"]
    )

    d1_1528 = direction(
        c1_1528["open"],
        c1_1528["close"]
    )

    d1_1529 = direction(
        c1_1529["open"],
        c1_1529["close"]
    )

    return {

        "5m_1520": d5_1520,
        "5m_1525": d5_1525,

        "3m_1524": d3_1524,
        "3m_1527": d3_1527,

        "1m_1528": d1_1528,
        "1m_1529": d1_1529,

        "5m_same":
            (
                d5_1520 == d5_1525
                and d5_1520 != "DOJI"
            ),

        "5m_opposite":
            (
                d5_1520 != d5_1525
                and d5_1520 != "DOJI"
                and d5_1525 != "DOJI"
            ),

        "3m_same":
            (
                d3_1524 == d3_1527
                and d3_1524 != "DOJI"
            ),

        "3m_opposite":
            (
                d3_1524 != d3_1527
                and d3_1524 != "DOJI"
                and d3_1527 != "DOJI"
            ),

        "1m_same":
            (
                d1_1528 == d1_1529
                and d1_1528 != "DOJI"
            ),

        "1m_opposite":
            (
                d1_1528 != d1_1529
                and d1_1528 != "DOJI"
                and d1_1529 != "DOJI"
            ),

        "5m3m_same":
            (
                d5_1520 == d3_1524
                and d5_1520 != "DOJI"
            ),

        "5m3m_opposite":
            (
                d5_1520 != d3_1524
                and d5_1520 != "DOJI"
                and d3_1524 != "DOJI"
            ),

        "5m1m_same":
            (
                d5_1520 == d1_1528
                and d5_1520 != "DOJI"
            ),

        "5m1m_opposite":
            (
                d5_1520 != d1_1528
                and d5_1520 != "DOJI"
                and d1_1528 != "DOJI"
            ),

        "3m1m_same":
            (
                d3_1524 == d1_1528
                and d3_1524 != "DOJI"
            ),

        "3m1m_opposite":
            (
                d3_1524 != d1_1528
                and d3_1524 != "DOJI"
                and d1_1528 != "DOJI"
            ),

        "all_three_same":
            (
                d5_1520 ==
                d3_1524 ==
                d1_1528
                and
                d5_1520 != "DOJI"
            ),

        "5m_volume_more":
            (
                c5_1520["volume"] >
                c5_1525["volume"]
            ),

        "5m_volume_less":
            (
                c5_1520["volume"] <
                c5_1525["volume"]
            ),

        "3m_volume_more":
            (
                c3_1524["volume"] >
                c3_1527["volume"]
            ),

        "3m_volume_less":
            (
                c3_1524["volume"] <
                c3_1527["volume"]
            ),

        "1m_volume_more":
            (
                c1_1528["volume"] >
                c1_1529["volume"]
            ),

        "1m_volume_less":
            (
                c1_1528["volume"] <
                c1_1529["volume"]
            )
    }


# ============================================================
# EXTRACT ALL OBSERVATIONS
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

        trade_date = dates[i + 1]

        signal_group = grouped[
            signal_date
        ]

        trade_group = grouped[
            trade_date
        ]

        day = {}

        for _, row in signal_group.iterrows():

            day[row["hm"]] = row.to_dict()

        features = build_features(
            day
        )

        if features is None:
            continue

        next_day = {}

        for _, row in trade_group.iterrows():

            next_day[row["hm"]] = row.to_dict()

        required = [
            "09:15",
            "15:27",
            "15:29"
        ]

        if not all(
            x in next_day
            for x in required
        ):
            continue

        entry = float(
            next_day["09:15"]["open"]
        )

        exit_1527 = float(
            next_day["15:27"]["open"]
        )

        exit_1529 = float(
            next_day["15:29"]["close"]
        )

        if entry <= 0:
            continue

        long_1527 = (
            exit_1527 - entry
        ) / entry * 100

        long_1529 = (
            exit_1529 - entry
        ) / entry * 100

        rows.append({

            "symbol":
                symbol,

            "signal_date":
                signal_date,

            "trade_date":
                trade_date,

            **features,

            "long_1527":
                long_1527,

            "long_1529":
                long_1529,

            "short_1527":
                -long_1527,

            "short_1529":
                -long_1529
        })

    return rows


# ============================================================
# PATTERN DEFINITIONS
# ============================================================

def get_patterns(df):

    return {

        # ----------------------------------------------------
        # INDIVIDUAL CANDLES
        # ----------------------------------------------------

        "3m_15:24_BEAR":
            df["3m_1524"] == "BEAR",

        "3m_15:24_BULL":
            df["3m_1524"] == "BULL",

        "3m_15:27_BEAR":
            df["3m_1527"] == "BEAR",

        "3m_15:27_BULL":
            df["3m_1527"] == "BULL",

        "5m_15:20_BEAR":
            df["5m_1520"] == "BEAR",

        "5m_15:20_BULL":
            df["5m_1520"] == "BULL",

        "1m_15:28_BEAR":
            df["1m_1528"] == "BEAR",

        "1m_15:28_BULL":
            df["1m_1528"] == "BULL",

        # ----------------------------------------------------
        # SAME / OPPOSITE
        # ----------------------------------------------------

        "5m_OPPOSITE":
            df["5m_opposite"],

        "5m_SAME":
            df["5m_same"],

        "3m_OPPOSITE":
            df["3m_opposite"],

        "3m_SAME":
            df["3m_same"],

        "1m_OPPOSITE":
            df["1m_opposite"],

        "1m_SAME":
            df["1m_same"],

        # ----------------------------------------------------
        # TIMEFRAME ALIGNMENT
        # ----------------------------------------------------

        "5m15:20 = 3m15:24":
            df["5m3m_same"],

        "5m15:20 != 3m15:24":
            df["5m3m_opposite"],

        "5m15:20 = 1m15:28":
            df["5m1m_same"],

        "5m15:20 != 1m15:28":
            df["5m1m_opposite"],

        "3m15:24 = 1m15:28":
            df["3m1m_same"],

        "3m15:24 != 1m15:28":
            df["3m1m_opposite"],

        "ALL THREE SAME":
            df["all_three_same"],

        # ----------------------------------------------------
        # VOLUME
        # ----------------------------------------------------

        "5m_15:20_VOLUME_MORE":
            df["5m_volume_more"],

        "5m_15:20_VOLUME_LESS":
            df["5m_volume_less"],

        "3m_15:24_VOLUME_MORE":
            df["3m_volume_more"],

        "3m_15:24_VOLUME_LESS":
            df["3m_volume_less"],

        "1m_15:28_VOLUME_MORE":
            df["1m_volume_more"],

        "1m_15:28_VOLUME_LESS":
            df["1m_volume_less"],

        # ----------------------------------------------------
        # IMPORTANT COMBINATIONS
        # ----------------------------------------------------

        (
            "3m 15:24 BEAR + "
            "3m 15:24 VOLUME MORE"
        ):
            (
                (df["3m_1524"] == "BEAR")
                &
                df["3m_volume_more"]
            ),

        (
            "3m OPPOSITE + "
            "3m VOLUME MORE"
        ):
            (
                df["3m_opposite"]
                &
                df["3m_volume_more"]
            ),

        (
            "5m OPPOSITE + "
            "5m VOLUME MORE"
        ):
            (
                df["5m_opposite"]
                &
                df["5m_volume_more"]
            ),

        (
            "5m = 3m + "
            "3m OPPOSITE"
        ):
            (
                df["5m3m_same"]
                &
                df["3m_opposite"]
            ),

        (
            "5m = 1m + "
            "1m VOLUME MORE"
        ):
            (
                df["5m1m_same"]
                &
                df["1m_volume_more"]
            ),

        (
            "3m = 1m + "
            "1m VOLUME MORE"
        ):
            (
                df["3m1m_same"]
                &
                df["1m_volume_more"]
            ),

        (
            "ALL THREE SAME + "
            "1m VOLUME MORE"
        ):
            (
                df["all_three_same"]
                &
                df["1m_volume_more"]
            ),

        (
            "ALL THREE SAME + "
            "3m VOLUME MORE"
        ):
            (
                df["all_three_same"]
                &
                df["3m_volume_more"]
            ),

        (
            "ALL THREE SAME + "
            "5m VOLUME MORE"
        ):
            (
                df["all_three_same"]
                &
                df["5m_volume_more"]
            ),

        (
            "ALL THREE SAME + "
            "ALL VOLUME MORE"
        ):
            (
                df["all_three_same"]
                &
                df["5m_volume_more"]
                &
                df["3m_volume_more"]
                &
                df["1m_volume_more"]
            )
    }


# ============================================================
# EVALUATE PATTERN
# ============================================================

def evaluate_pattern(
    df,
    mask,
    min_trades
):

    subset = df[
        mask
    ].copy()

    if len(subset) < min_trades:
        return None

    # --------------------------------------------------------
    # Determine direction ONLY from TRAINING DATA
    # --------------------------------------------------------

    long_avg = (
        subset["long_1529"]
        .mean()
    )

    short_avg = (
        subset["short_1529"]
        .mean()
    )

    if long_avg >= short_avg:

        direction_name = "LONG"

        r1527 = (
            subset["long_1527"]
        )

        r1529 = (
            subset["long_1529"]
        )

    else:

        direction_name = "SHORT"

        r1527 = (
            subset["short_1527"]
        )

        r1529 = (
            subset["short_1529"]
        )

    def statistics(series):

        wins = (
            series > 0
        ).sum()

        losses = (
            series < 0
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

            profit_factor = (
                gross_profit /
                gross_loss
            )

        else:

            profit_factor = float("inf")

        return {

            "trades":
                len(series),

            "win_rate":
                win_rate,

            "average":
                series.mean(),

            "median":
                series.median(),

            "profit_factor":
                profit_factor,

            "total":
                series.sum()
        }

    s1527 = statistics(
        r1527
    )

    s1529 = statistics(
        r1529
    )

    return {

        "direction":
            direction_name,

        "trades":
            len(subset),

        "15:27_win_rate":
            s1527["win_rate"],

        "15:27_average":
            s1527["average"],

        "15:27_profit_factor":
            s1527["profit_factor"],

        "15:29_win_rate":
            s1529["win_rate"],

        "15:29_average":
            s1529["average"],

        "15:29_profit_factor":
            s1529["profit_factor"],

        "15:29_total":
            s1529["total"]
    }


# ============================================================
# TEST TOP PATTERN ON UNSEEN DATA
# ============================================================

def test_pattern(
    df,
    pattern_mask,
    direction_name
):

    subset = df[
        pattern_mask
    ].copy()

    if len(subset) == 0:
        return None

    if direction_name == "LONG":

        r1527 = (
            subset["long_1527"]
        )

        r1529 = (
            subset["long_1529"]
        )

    else:

        r1527 = (
            subset["short_1527"]
        )

        r1529 = (
            subset["short_1529"]
        )

    def stats(series):

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

        return (
            len(series),
            win_rate,
            series.mean(),
            series.median(),
            pf,
            series.sum()
        )

    a = stats(
        r1527
    )

    b = stats(
        r1529
    )

    return {

        "trades":
            len(subset),

        "direction":
            direction_name,

        "15:27_win_rate":
            a[1],

        "15:27_average":
            a[2],

        "15:27_profit_factor":
            a[4],

        "15:27_total":
            a[5],

        "15:29_win_rate":
            b[1],

        "15:29_average":
            b[2],

        "15:29_profit_factor":
            b[4],

        "15:29_total":
            b[5]
    }


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 75)
    print("OUT-OF-SAMPLE EDGE VALIDATION")
    print("=" * 75)

    print(
        "\nTraining period: "
        f"{TRAIN_PERCENT * 100:.0f}%"
    )

    print(
        "Unseen test period: "
        f"{(1 - TRAIN_PERCENT) * 100:.0f}%"
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
    # LOAD
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

    df = pd.DataFrame(
        all_rows
    )

    if df.empty:

        print(
            "No usable observations."
        )

        return

    df["signal_date"] = pd.to_datetime(
        df["signal_date"]
    )

    # ========================================================
    # TIME SPLIT
    # ========================================================

    unique_dates = sorted(
        df["signal_date"]
        .unique()
    )

    split_index = int(
        len(unique_dates) *
        TRAIN_PERCENT
    )

    split_date = (
        unique_dates[
            split_index
        ]
    )

    train = df[
        df["signal_date"] <
        split_date
    ].copy()

    test = df[
        df["signal_date"] >=
        split_date
    ].copy()

    print(
        f"Training observations: "
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
    # PATTERNS
    # ========================================================

    train_patterns = get_patterns(
        train
    )

    test_patterns = get_patterns(
        test
    )

    # ========================================================
    # DISCOVER PATTERNS ONLY IN TRAINING
    # ========================================================

    discovered = []

    for name, mask in train_patterns.items():

        result = evaluate_pattern(
            train,
            mask,
            MIN_TRAIN_TRADES
        )

        if result is None:
            continue

        result["pattern"] = name

        discovered.append(
            result
        )

    discovered_df = pd.DataFrame(
        discovered
    )

    if discovered_df.empty:

        print(
            "No patterns reached "
            f"{MIN_TRAIN_TRADES} trades."
        )

        return

    # ========================================================
    # RANK TRAINING PATTERNS
    # ========================================================

    discovered_df = (
        discovered_df
        .sort_values(
            by="15:29_average",
            ascending=False
        )
    )

    top = discovered_df.head(
        TOP_PATTERNS
    )

    print("\n")
    print("=" * 75)
    print(
        "TOP PATTERNS DISCOVERED "
        "IN TRAINING DATA"
    )
    print("=" * 75)

    print(
        top[
            [
                "pattern",
                "trades",
                "direction",
                "15:29_win_rate",
                "15:29_average",
                "15:29_profit_factor"
            ]
        ].to_string(
            index=False
        )
    )

    # ========================================================
    # VALIDATE EACH TOP PATTERN
    # ========================================================

    validation_rows = []

    for _, row in top.iterrows():

        pattern_name = row[
            "pattern"
        ]

        direction_name = row[
            "direction"
        ]

        train_mask = (
            train_patterns[
                pattern_name
            ]
        )

        test_mask = (
            test_patterns[
                pattern_name
            ]
        )

        validation = test_pattern(
            test,
            test_mask,
            direction_name
        )

        if validation is None:
            continue

        validation_rows.append({

            "pattern":
                pattern_name,

            "direction":
                direction_name,

            "train_trades":
                row["trades"],

            "train_win_rate":
                row["15:29_win_rate"],

            "train_average":
                row["15:29_average"],

            "train_profit_factor":
                row["15:29_profit_factor"],

            "test_trades":
                validation["trades"],

            "test_15:27_win_rate":
                validation[
                    "15:27_win_rate"
                ],

            "test_15:27_average":
                validation[
                    "15:27_average"
                ],

            "test_15:27_profit_factor":
                validation[
                    "15:27_profit_factor"
                ],

            "test_15:29_win_rate":
                validation[
                    "15:29_win_rate"
                ],

            "test_15:29_average":
                validation[
                    "15:29_average"
                ],

            "test_15:29_profit_factor":
                validation[
                    "15:29_profit_factor"
                ],

            "test_15:29_total":
                validation[
                    "15:29_total"
                ]
        })

    validation_df = pd.DataFrame(
        validation_rows
    )

    # ========================================================
    # FINAL VALIDATION TABLE
    # ========================================================

    print("\n")
    print("=" * 75)
    print(
        "UNSEEN TEST RESULTS"
    )
    print("=" * 75)

    if validation_df.empty:

        print(
            "No validation results."
        )

    else:

        print(
            validation_df[
                [
                    "pattern",
                    "direction",
                    "train_average",
                    "train_profit_factor",
                    "test_trades",
                    "test_15:29_win_rate",
                    "test_15:29_average",
                    "test_15:29_profit_factor"
                ]
            ].to_string(
                index=False
            )
        )

    # ========================================================
    # YEARLY TEST OF TOP PATTERNS
    # ========================================================

    print("\n")
    print("=" * 75)
    print(
        "YEAR-BY-YEAR PERFORMANCE "
        "OF TOP PATTERNS"
    )
    print("=" * 75)

    test["year"] = (
        test["signal_date"]
        .dt.year
    )

    for _, row in top.iterrows():

        pattern_name = row[
            "pattern"
        ]

        direction_name = row[
            "direction"
        ]

        mask = test_patterns[
            pattern_name
        ]

        subset = test[
            mask
        ].copy()

        if subset.empty:
            continue

        print(
            f"\n{pattern_name} "
            f"→ {direction_name}"
        )

        for year in sorted(
            subset["year"].unique()
        ):

            yearly = subset[
                subset["year"] == year
            ]

            if direction_name == "LONG":

                returns = yearly[
                    "long_1529"
                ]

            else:

                returns = yearly[
                    "short_1529"
                ]

            if len(returns) < 20:
                continue

            win_rate = (
                (returns > 0).sum()
                /
                len(returns)
                *
                100
            )

            print(
                f"  {year}: "
                f"N={len(returns)} | "
                f"Win={win_rate:.2f}% | "
                f"Avg={returns.mean():.4f}%"
            )

    # ========================================================
    # SAVE
    # ========================================================

    discovered_df.to_csv(
        "training_pattern_rankings.csv",
        index=False
    )

    validation_df.to_csv(
        "out_of_sample_validation.csv",
        index=False
    )

    print("\n")
    print("=" * 75)
    print("VALIDATION COMPLETE")
    print("=" * 75)

    print(
        "\nFiles created:"
    )

    print(
        "training_pattern_rankings.csv"
    )

    print(
        "out_of_sample_validation.csv"
    )

    print(
        "\nThe most important numbers are "
        "the TEST average return and TEST profit factor."
    )

    print(
        "If a pattern remains positive on the "
        "unseen test period, it becomes a candidate "
        "for a proper strategy backtest."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
