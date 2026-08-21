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

# A pattern must have at least this many observations
# in the training set before it can be considered.
MIN_TRAIN_TRADES = 500

# Show the best candidates
TOP_PATTERNS = 30

# Target we're searching for
TARGET_WIN_RATE = 85.0


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
# DOWNLOAD DATA
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

        # ----------------------------------------------------
        # TIME
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # OHLC
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # VOLUME
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
# BUILD MULTI-MINUTE CANDLE
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
# CANDLE STRUCTURE
# ============================================================

def candle_structure(candle):

    open_price = candle["open"]
    high = candle["high"]
    low = candle["low"]
    close = candle["close"]

    candle_range = high - low

    body = abs(
        close - open_price
    )

    if candle_range > 0:

        body_ratio = (
            body /
            candle_range
        )

        close_position = (
            close - low
        ) / candle_range

        upper_wick = (
            high -
            max(open_price, close)
        ) / candle_range

        lower_wick = (
            min(open_price, close) -
            low
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

        "upper_wick_ratio":
            upper_wick,

        "lower_wick_ratio":
            lower_wick
    }


# ============================================================
# FEATURES
# ============================================================

def build_features(day):

    # --------------------------------------------------------
    # MAIN CANDLES
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # PREVIOUS 3-MINUTE CANDLES
    #
    # Used to determine whether 15:24 is unusually large.
    # --------------------------------------------------------

    c3_1515 = build_candle(
        day,
        "15:15",
        3
    )

    c3_1518 = build_candle(
        day,
        "15:18",
        3
    )

    c3_1521 = build_candle(
        day,
        "15:21",
        3
    )

    candles = [
        c5_1520,
        c5_1525,
        c3_1524,
        c3_1527,
        c1_1528,
        c1_1529,
        c3_1515,
        c3_1518,
        c3_1521
    ]

    if any(
        x is None
        for x in candles
    ):
        return None

    # --------------------------------------------------------
    # DIRECTIONS
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # STRUCTURE
    # --------------------------------------------------------

    s5_1520 = candle_structure(
        c5_1520
    )

    s5_1525 = candle_structure(
        c5_1525
    )

    s3_1524 = candle_structure(
        c3_1524
    )

    s3_1527 = candle_structure(
        c3_1527
    )

    s1_1528 = candle_structure(
        c1_1528
    )

    s1_1529 = candle_structure(
        c1_1529
    )

    s3_1515 = candle_structure(
        c3_1515
    )

    s3_1518 = candle_structure(
        c3_1518
    )

    s3_1521 = candle_structure(
        c3_1521
    )

    # --------------------------------------------------------
    # AVERAGE PREVIOUS 3M RANGE
    # --------------------------------------------------------

    previous_3m_ranges = [
        s3_1515["range"],
        s3_1518["range"],
        s3_1521["range"]
    ]

    average_previous_range = np.mean(
        previous_3m_ranges
    )

    # --------------------------------------------------------
    # RELATIVE VOLUME
    # --------------------------------------------------------

    avg_previous_3m_volume = np.mean([
        c3_1515["volume"],
        c3_1518["volume"],
        c3_1521["volume"]
    ])

    avg_previous_5m_volume = (
        (
            c5_1520["volume"] +
            c5_1525["volume"]
        ) / 2
    )

    # --------------------------------------------------------
    # VOLUME RATIOS
    # --------------------------------------------------------

    v3_ratio = (
        c3_1524["volume"] /
        c3_1527["volume"]
        if c3_1527["volume"] > 0
        else 0
    )

    v1_ratio = (
        c1_1528["volume"] /
        c1_1529["volume"]
        if c1_1529["volume"] > 0
        else 0
    )

    v5_ratio = (
        c5_1520["volume"] /
        c5_1525["volume"]
        if c5_1525["volume"] > 0
        else 0
    )

    relative_3m_volume = (
        c3_1524["volume"] /
        avg_previous_3m_volume
        if avg_previous_3m_volume > 0
        else 0
    )

    # --------------------------------------------------------
    # RANGE RATIO
    # --------------------------------------------------------

    range_ratio_3m = (
        s3_1524["range"] /
        average_previous_range
        if average_previous_range > 0
        else 0
    )

    # --------------------------------------------------------
    # MOMENTUM
    #
    # Close of 15:24 relative to its open.
    # --------------------------------------------------------

    if c3_1524["open"] > 0:

        momentum_3m = (
            (
                c3_1524["close"] -
                c3_1524["open"]
            )
            /
            c3_1524["open"]
            *
            100
        )

    else:

        momentum_3m = 0

    # --------------------------------------------------------
    # RETURN ALL FEATURES
    # --------------------------------------------------------

    return {

        # ----------------------------------------------------
        # DIRECTIONS
        # ----------------------------------------------------

        "5m_1520":
            d5_1520,

        "5m_1525":
            d5_1525,

        "3m_1524":
            d3_1524,

        "3m_1527":
            d3_1527,

        "1m_1528":
            d1_1528,

        "1m_1529":
            d1_1529,

        # ----------------------------------------------------
        # 3M 15:24 STRUCTURE
        # ----------------------------------------------------

        "3m_body_ratio":
            s3_1524["body_ratio"],

        "3m_close_position":
            s3_1524["close_position"],

        "3m_upper_wick":
            s3_1524["upper_wick_ratio"],

        "3m_lower_wick":
            s3_1524["lower_wick_ratio"],

        "3m_range":
            s3_1524["range"],

        "3m_body":
            s3_1524["body"],

        # ----------------------------------------------------
        # OTHER STRUCTURE
        # ----------------------------------------------------

        "5m_body_ratio":
            s5_1520["body_ratio"],

        "1m_body_ratio":
            s1_1528["body_ratio"],

        # ----------------------------------------------------
        # RELATIVE RANGE
        # ----------------------------------------------------

        "3m_range_ratio":
            range_ratio_3m,

        # ----------------------------------------------------
        # VOLUME
        # ----------------------------------------------------

        "3m_volume_ratio":
            v3_ratio,

        "1m_volume_ratio":
            v1_ratio,

        "5m_volume_ratio":
            v5_ratio,

        "3m_relative_volume":
            relative_3m_volume,

        # ----------------------------------------------------
        # MOMENTUM
        # ----------------------------------------------------

        "3m_momentum":
            momentum_3m,

        # ----------------------------------------------------
        # RELATIONSHIPS
        # ----------------------------------------------------

        "5m_same":
            (
                d5_1520 ==
                d5_1525
                and
                d5_1520 != "DOJI"
            ),

        "5m_opposite":
            (
                d5_1520 !=
                d5_1525
                and
                d5_1520 != "DOJI"
                and
                d5_1525 != "DOJI"
            ),

        "3m_same":
            (
                d3_1524 ==
                d3_1527
                and
                d3_1524 != "DOJI"
            ),

        "3m_opposite":
            (
                d3_1524 !=
                d3_1527
                and
                d3_1524 != "DOJI"
                and
                d3_1527 != "DOJI"
            ),

        "1m_same":
            (
                d1_1528 ==
                d1_1529
                and
                d1_1528 != "DOJI"
            ),

        "1m_opposite":
            (
                d1_1528 !=
                d1_1529
                and
                d1_1528 != "DOJI"
                and
                d1_1529 != "DOJI"
            ),

        "5m3m_same":
            (
                d5_1520 ==
                d3_1524
                and
                d5_1520 != "DOJI"
            ),

        "5m1m_same":
            (
                d5_1520 ==
                d1_1528
                and
                d5_1520 != "DOJI"
            ),

        "3m1m_same":
            (
                d3_1524 ==
                d1_1528
                and
                d3_1524 != "DOJI"
            ),

        "all_three_same":
            (
                d5_1520 ==
                d3_1524 ==
                d1_1528
                and
                d5_1520 != "DOJI"
            )
    }


# ============================================================
# EXTRACT STOCK DATA
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

        if not all(
            x in next_day
            for x in [
                "09:15",
                "15:27",
                "15:29"
            ]
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
            exit_1527 -
            entry
        ) / entry * 100

        long_1529 = (
            exit_1529 -
            entry
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
# CREATE PATTERN MASKS
# ============================================================

def generate_patterns(df):

    patterns = {}

    # ========================================================
    # BASE DIRECTION
    # ========================================================

    patterns[
        "3m_15:24_BEAR"
    ] = (
        df["3m_1524"] ==
        "BEAR"
    )

    patterns[
        "3m_15:24_BULL"
    ] = (
        df["3m_1524"] ==
        "BULL"
    )

    patterns[
        "5m_15:20_BEAR"
    ] = (
        df["5m_1520"] ==
        "BEAR"
    )

    patterns[
        "5m_15:20_BULL"
    ] = (
        df["5m_1520"] ==
        "BULL"
    )

    patterns[
        "1m_15:28_BEAR"
    ] = (
        df["1m_1528"] ==
        "BEAR"
    )

    patterns[
        "1m_15:28_BULL"
    ] = (
        df["1m_1528"] ==
        "BULL"
    )

    # ========================================================
    # CANDLE BODY STRENGTH
    # ========================================================

    body_levels = [
        0.50,
        0.60,
        0.70,
        0.80,
        0.90
    ]

    for level in body_levels:

        patterns[
            f"3m_BEAR_body>={level:.2f}"
        ] = (
            (df["3m_1524"] == "BEAR")
            &
            (df["3m_body_ratio"] >= level)
        )

        patterns[
            f"3m_BULL_body>={level:.2f}"
        ] = (
            (df["3m_1524"] == "BULL")
            &
            (df["3m_body_ratio"] >= level)
        )

    # ========================================================
    # CLOSE POSITION
    # ========================================================

    close_levels = [
        0.10,
        0.20,
        0.30,
        0.40
    ]

    for level in close_levels:

        patterns[
            f"3m_BEAR_close<={level:.2f}"
        ] = (
            (df["3m_1524"] == "BEAR")
            &
            (df["3m_close_position"] <= level)
        )

        patterns[
            f"3m_BULL_close>={1-level:.2f}"
        ] = (
            (df["3m_1524"] == "BULL")
            &
            (df["3m_close_position"] >= 1-level)
        )

    # ========================================================
    # RELATIVE RANGE
    # ========================================================

    range_levels = [
        1.25,
        1.50,
        1.75,
        2.00,
        2.50,
        3.00
    ]

    for level in range_levels:

        patterns[
            f"3m_BEAR_range>={level:.2f}x"
        ] = (
            (df["3m_1524"] == "BEAR")
            &
            (df["3m_range_ratio"] >= level)
        )

        patterns[
            f"3m_BULL_range>={level:.2f}x"
        ] = (
            (df["3m_1524"] == "BULL")
            &
            (df["3m_range_ratio"] >= level)
        )

    # ========================================================
    # RELATIVE VOLUME
    # ========================================================

    volume_levels = [
        1.25,
        1.50,
        1.75,
        2.00,
        2.50,
        3.00
    ]

    for level in volume_levels:

        patterns[
            f"3m_BEAR_relative_volume>={level:.2f}x"
        ] = (
            (df["3m_1524"] == "BEAR")
            &
            (df["3m_relative_volume"] >= level)
        )

        patterns[
            f"3m_BULL_relative_volume>={level:.2f}x"
        ] = (
            (df["3m_1524"] == "BULL")
            &
            (df["3m_relative_volume"] >= level)
        )

        patterns[
            f"3m_BEAR_15:24_vs_15:27_volume>={level:.2f}x"
        ] = (
            (df["3m_1524"] == "BEAR")
            &
            (df["3m_volume_ratio"] >= level)
        )

        patterns[
            f"3m_BULL_15:24_vs_15:27_volume>={level:.2f}x"
        ] = (
            (df["3m_1524"] == "BULL")
            &
            (df["3m_volume_ratio"] >= level)
        )

    # ========================================================
    # MOMENTUM
    # ========================================================

    momentum_levels = [
        0.05,
        0.10,
        0.15,
        0.20,
        0.30,
        0.50
    ]

    for level in momentum_levels:

        patterns[
            f"3m_BEAR_momentum<={-level:.2f}%"
        ] = (
            (df["3m_1524"] == "BEAR")
            &
            (df["3m_momentum"] <= -level)
        )

        patterns[
            f"3m_BULL_momentum>={level:.2f}%"
        ] = (
            (df["3m_1524"] == "BULL")
            &
            (df["3m_momentum"] >= level)
        )

    # ========================================================
    # RELATIONSHIPS
    # ========================================================

    patterns[
        "3m_BEAR + 5m_BEAR"
    ] = (
        (df["3m_1524"] == "BEAR")
        &
        (df["5m_1520"] == "BEAR")
    )

    patterns[
        "3m_BEAR + 1m_BEAR"
    ] = (
        (df["3m_1524"] == "BEAR")
        &
        (df["1m_1528"] == "BEAR")
    )

    patterns[
        "3m_BEAR + 5m_BEAR + 1m_BEAR"
    ] = (
        (df["3m_1524"] == "BEAR")
        &
        (df["5m_1520"] == "BEAR")
        &
        (df["1m_1528"] == "BEAR")
    )

    patterns[
        "3m_BEAR + 3m_OPPOSITE"
    ] = (
        (df["3m_1524"] == "BEAR")
        &
        (
            df["3m_1524"] !=
            df["3m_1527"]
        )
    )

    # ========================================================
    # HIGH-CONVICTION COMBINATIONS
    # ========================================================

    for body in [
        0.60,
        0.70,
        0.80
    ]:

        for volume in [
            1.25,
            1.50,
            2.00
        ]:

            patterns[
                (
                    f"BEAR + body>={body:.2f} "
                    f"+ relative_volume>={volume:.2f}x"
                )
            ] = (
                (df["3m_1524"] == "BEAR")
                &
                (df["3m_body_ratio"] >= body)
                &
                (df["3m_relative_volume"] >= volume)
            )

    # ========================================================
    # EXTREME COMBINATIONS
    # ========================================================

    for body in [
        0.60,
        0.70,
        0.80
    ]:

        for volume in [
            1.50,
            2.00
        ]:

            patterns[
                (
                    f"BEAR + body>={body:.2f} "
                    f"+ volume>={volume:.2f}x "
                    f"+ 3m OPPOSITE"
                )
            ] = (
                (df["3m_1524"] == "BEAR")
                &
                (df["3m_body_ratio"] >= body)
                &
                (df["3m_relative_volume"] >= volume)
                &
                (
                    df["3m_1524"] !=
                    df["3m_1527"]
                )
            )

    return patterns


# ============================================================
# STATISTICS
# ============================================================

def statistics(series):

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

        profit_factor = (
            gross_profit /
            gross_loss
        )

    else:

        profit_factor = float("inf")

    return {

        "trades":
            len(series),

        "wins":
            wins,

        "win_rate":
            win_rate,

        "average":
            series.mean(),

        "median":
            series.median(),

        "profit_factor":
            profit_factor,

        "total":
            series.sum(),

        "best":
            series.max(),

        "worst":
            series.min()
    }


# ============================================================
# EVALUATE PATTERN
# ============================================================

def evaluate_pattern(
    df,
    mask
):

    subset = df[
        mask
    ].copy()

    if len(subset) < MIN_TRAIN_TRADES:
        return None

    # --------------------------------------------------------
    # Determine direction ONLY from training data
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

        side = "LONG"

        r1527 = (
            subset["long_1527"]
        )

        r1529 = (
            subset["long_1529"]
        )

    else:

        side = "SHORT"

        r1527 = (
            subset["short_1527"]
        )

        r1529 = (
            subset["short_1529"]
        )

    s27 = statistics(
        r1527
    )

    s29 = statistics(
        r1529
    )

    return {

        "direction":
            side,

        "trades":
            s29["trades"],

        "win_rate_15:27":
            s27["win_rate"],

        "avg_15:27":
            s27["average"],

        "pf_15:27":
            s27["profit_factor"],

        "win_rate_15:29":
            s29["win_rate"],

        "avg_15:29":
            s29["average"],

        "pf_15:29":
            s29["profit_factor"],

        "total_15:29":
            s29["total"]
    }


# ============================================================
# OUT-OF-SAMPLE TEST
# ============================================================

def validate_pattern(
    df,
    mask,
    side
):

    subset = df[
        mask
    ].copy()

    if len(subset) == 0:
        return None

    if side == "LONG":

        r27 = (
            subset["long_1527"]
        )

        r29 = (
            subset["long_1529"]
        )

    else:

        r27 = (
            subset["short_1527"]
        )

        r29 = (
            subset["short_1529"]
        )

    s27 = statistics(
        r27
    )

    s29 = statistics(
        r29
    )

    return {

        "test_trades":
            s29["trades"],

        "test_15:27_win_rate":
            s27["win_rate"],

        "test_15:27_average":
            s27["average"],

        "test_15:27_pf":
            s27["profit_factor"],

        "test_15:29_win_rate":
            s29["win_rate"],

        "test_15:29_average":
            s29["average"],

        "test_15:29_pf":
            s29["profit_factor"],

        "test_15:29_total":
            s29["total"]
    }


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 75)
    print(
        "HIGH-WIN-RATE PATTERN DISCOVERY"
    )
    print("=" * 75)

    print(
        "\nTARGET WIN RATE: "
        f"{TARGET_WIN_RATE:.0f}%"
    )

    print(
        f"MINIMUM TRAINING TRADES: "
        f"{MIN_TRAIN_TRADES}"
    )

    print(
        "\nThe scanner searches candle structure, "
        "relative volume, range and momentum."
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
    # PROCESS STOCKS
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
            "No usable observations."
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
    # TIME SPLIT
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
        f"\nPatterns being tested: "
        f"{len(train_patterns)}"
    )

    # ========================================================
    # DISCOVER
    # ========================================================

    discovered = []

    for name, mask in train_patterns.items():

        result = evaluate_pattern(
            train,
            mask
        )

        if result is None:
            continue

        result["pattern"] = name

        discovered.append(
            result
        )

    if not discovered:

        print(
            "No patterns passed the "
            "minimum trade requirement."
        )

        return

    discovered_df = pd.DataFrame(
        discovered
    )

    # --------------------------------------------------------
    # Sort by win rate first
    # --------------------------------------------------------

    discovered_df = (
        discovered_df
        .sort_values(
            [
                "win_rate_15:29",
                "trades",
                "pf_15:29"
            ],
            ascending=[
                False,
                False,
                False
            ]
        )
    )

    # ========================================================
    # TOP TRAINING PATTERNS
    # ========================================================

    print("\n")
    print("=" * 75)
    print(
        "TOP TRAINING PATTERNS "
        "BY WIN RATE"
    )
    print("=" * 75)

    print(
        discovered_df[
            [
                "pattern",
                "trades",
                "direction",
                "win_rate_15:29",
                "avg_15:29",
                "pf_15:29"
            ]
        ]
        .head(TOP_PATTERNS)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # FIND PATTERNS CLOSE TO 85%
    # ========================================================

    high_win = discovered_df[
        discovered_df[
            "win_rate_15:29"
        ] >= TARGET_WIN_RATE
    ]

    print("\n")
    print("=" * 75)
    print(
        f"PATTERNS REACHING "
        f"{TARGET_WIN_RATE:.0f}%+ "
        "IN TRAINING"
    )
    print("=" * 75)

    if high_win.empty:

        print(
            "NONE reached the target "
            "with the required sample size."
        )

    else:

        print(
            high_win[
                [
                    "pattern",
                    "trades",
                    "direction",
                    "win_rate_15:29",
                    "avg_15:29",
                    "pf_15:29"
                ]
            ]
            .to_string(
                index=False
            )
        )

    # ========================================================
    # VALIDATE TOP 30
    # ========================================================

    top = discovered_df.head(
        TOP_PATTERNS
    )

    validation = []

    for _, row in top.iterrows():

        name = row[
            "pattern"
        ]

        side = row[
            "direction"
        ]

        test_mask = test_patterns[
            name
        ]

        result = validate_pattern(
            test,
            test_mask,
            side
        )

        if result is None:
            continue

        validation.append({

            "pattern":
                name,

            "direction":
                side,

            "train_trades":
                row["trades"],

            "train_win_rate":
                row["win_rate_15:29"],

            "train_average":
                row["avg_15:29"],

            "train_pf":
                row["pf_15:29"],

            **result
        })

    validation_df = pd.DataFrame(
        validation
    )

    # ========================================================
    # VALIDATION
    # ========================================================

    print("\n")
    print("=" * 75)
    print(
        "UNSEEN TEST — TOP PATTERNS"
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
                    "train_win_rate",
                    "train_pf",
                    "test_trades",
                    "test_15:29_win_rate",
                    "test_15:29_average",
                    "test_15:29_pf"
                ]
            ]
            .sort_values(
                "test_15:29_win_rate",
                ascending=False
            )
            .to_string(
                index=False
            )
        )

    # ========================================================
    # PATTERNS THAT SURVIVE
    # ========================================================

    if not validation_df.empty:

        survivors = validation_df[
            (
                validation_df[
                    "test_15:29_win_rate"
                ] >= 60
            )
            &
            (
                validation_df[
                    "test_15:29_average"
                ] > 0
            )
            &
            (
                validation_df[
                    "test_15:29_pf"
                ] > 1
            )
        ].copy()

    else:

        survivors = pd.DataFrame()

    print("\n")
    print("=" * 75)
    print(
        "POSITIVE OUT-OF-SAMPLE "
        "CANDIDATES"
    )
    print("=" * 75)

    if survivors.empty:

        print(
            "No strong positive "
            "out-of-sample candidates."
        )

    else:

        print(
            survivors[
                [
                    "pattern",
                    "direction",
                    "test_trades",
                    "test_15:29_win_rate",
                    "test_15:29_average",
                    "test_15:29_pf"
                ]
            ]
            .sort_values(
                "test_15:29_win_rate",
                ascending=False
            )
            .to_string(
                index=False
            )
        )

    # ========================================================
    # SAVE
    # ========================================================

    discovered_df.to_csv(
        "structure_training_results.csv",
        index=False
    )

    validation_df.to_csv(
        "structure_out_of_sample_results.csv",
        index=False
    )

    # ========================================================
    # DONE
    # ========================================================

    print("\n")
    print("=" * 75)
    print(
        "HIGH-WIN-RATE SCAN COMPLETE"
    )
    print("=" * 75)

    print(
        "\nCreated:"
    )

    print(
        "structure_training_results.csv"
    )

    print(
        "structure_out_of_sample_results.csv"
    )

    print(
        "\nIMPORTANT:"
    )

    print(
        "An 85% training result is NOT enough."
    )

    print(
        "The pattern must also survive "
        "the unseen test period."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
