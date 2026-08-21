import pandas as pd
import requests
import zipfile
import io
import os
from itertools import combinations


# ============================================================
# SETTINGS
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/nse-fno-1min-data/"
    "releases/download/v1.0.0/stocks_1m_csvs.zip"
)

MIN_TRADES = 100


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
# BUILD CANDLE
# ============================================================

def build_candle(day, start_time, minutes):

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
# BUILD DAILY FEATURE SET
# ============================================================

def build_features(day):

    # --------------------------------------------------------
    # 5-MINUTE
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

    # --------------------------------------------------------
    # 3-MINUTE
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 1-MINUTE
    # --------------------------------------------------------

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

    required = [
        c5_1520,
        c5_1525,
        c3_1524,
        c3_1527,
        c1_1528,
        c1_1529
    ]

    if any(
        x is None
        for x in required
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
    # VOLUME RATIOS
    # --------------------------------------------------------

    v5_ratio = (
        c5_1520["volume"] /
        c5_1525["volume"]
        if c5_1525["volume"] > 0
        else None
    )

    v3_ratio = (
        c3_1524["volume"] /
        c3_1527["volume"]
        if c3_1527["volume"] > 0
        else None
    )

    v1_ratio = (
        c1_1528["volume"] /
        c1_1529["volume"]
        if c1_1529["volume"] > 0
        else None
    )

    # --------------------------------------------------------
    # FEATURE ROW
    # --------------------------------------------------------

    return {

        "5m_1520": d5_1520,
        "5m_1525": d5_1525,

        "3m_1524": d3_1524,
        "3m_1527": d3_1527,

        "1m_1528": d1_1528,
        "1m_1529": d1_1529,

        "5m_same":
            d5_1520 == d5_1525,

        "5m_opposite":
            (
                d5_1520 != d5_1525
                and
                d5_1520 != "DOJI"
                and
                d5_1525 != "DOJI"
            ),

        "3m_same":
            d3_1524 == d3_1527,

        "3m_opposite":
            (
                d3_1524 != d3_1527
                and
                d3_1524 != "DOJI"
                and
                d3_1527 != "DOJI"
            ),

        "1m_same":
            d1_1528 == d1_1529,

        "1m_opposite":
            (
                d1_1528 != d1_1529
                and
                d1_1528 != "DOJI"
                and
                d1_1529 != "DOJI"
            ),

        "5m_1520_volume":
            c5_1520["volume"],

        "5m_1525_volume":
            c5_1525["volume"],

        "5m_volume_ratio":
            v5_ratio,

        "5m_1520_more_volume":
            c5_1520["volume"] >
            c5_1525["volume"],

        "3m_1524_volume":
            c3_1524["volume"],

        "3m_1527_volume":
            c3_1527["volume"],

        "3m_volume_ratio":
            v3_ratio,

        "3m_1524_more_volume":
            c3_1524["volume"] >
            c3_1527["volume"],

        "1m_1528_volume":
            c1_1528["volume"],

        "1m_1529_volume":
            c1_1529["volume"],

        "1m_volume_ratio":
            v1_ratio,

        "1m_1528_more_volume":
            c1_1528["volume"] >
            c1_1529["volume"]
    }


# ============================================================
# CREATE DAILY DATA
# ============================================================

def extract_stock_days(
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

    output = []

    symbol = os.path.basename(
        filename
    ).replace(
        "_1m.csv.gz",
        ""
    )

    # --------------------------------------------------------
    # Each signal day has the NEXT trading day's return
    # --------------------------------------------------------

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

        # ----------------------------------------------------
        # SIGNAL DAY
        # ----------------------------------------------------

        day = {}

        for _, row in signal_group.iterrows():

            day[row["hm"]] = row.to_dict()

        features = build_features(
            day
        )

        if features is None:
            continue

        # ----------------------------------------------------
        # NEXT DAY
        # ----------------------------------------------------

        next_day = {}

        for _, row in trade_group.iterrows():

            next_day[row["hm"]] = row.to_dict()

        if (
            "09:15" not in next_day
            or
            "15:27" not in next_day
            or
            "15:29" not in next_day
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

        # ----------------------------------------------------
        # NEXT-DAY RETURNS
        #
        # We calculate BOTH directions so that the scanner
        # can determine which direction is actually better.
        # ----------------------------------------------------

        long_1527 = (
            exit_1527 - entry
        ) / entry * 100

        long_1529 = (
            exit_1529 - entry
        ) / entry * 100

        short_1527 = -long_1527

        short_1529 = -long_1529

        row = {

            "symbol":
                symbol,

            "signal_date":
                signal_date,

            "trade_date":
                trade_date,

            **features,

            "long_1527_return":
                long_1527,

            "long_1529_return":
                long_1529,

            "short_1527_return":
                short_1527,

            "short_1529_return":
                short_1529
        }

        output.append(row)

    return output


# ============================================================
# ADD TREND RELATIONSHIP
# ============================================================

def add_relationships(df):

    # --------------------------------------------------------
    # 5m 15:20 vs 3m 15:24
    # --------------------------------------------------------

    df["5m3m_same"] = (
        df["5m_1520"] ==
        df["3m_1524"]
    )

    df["5m3m_opposite"] = (
        (df["5m_1520"] != df["3m_1524"])
        &
        (df["5m_1520"] != "DOJI")
        &
        (df["3m_1524"] != "DOJI")
    )

    # --------------------------------------------------------
    # 5m 15:20 vs 1m 15:28
    # --------------------------------------------------------

    df["5m1m_same"] = (
        df["5m_1520"] ==
        df["1m_1528"]
    )

    df["5m1m_opposite"] = (
        (df["5m_1520"] != df["1m_1528"])
        &
        (df["5m_1520"] != "DOJI")
        &
        (df["1m_1528"] != "DOJI")
    )

    # --------------------------------------------------------
    # 3m 15:24 vs 1m 15:28
    # --------------------------------------------------------

    df["3m1m_same"] = (
        df["3m_1524"] ==
        df["1m_1528"]
    )

    df["3m1m_opposite"] = (
        (df["3m_1524"] != df["1m_1528"])
        &
        (df["3m_1524"] != "DOJI")
        &
        (df["1m_1528"] != "DOJI")
    )

    # --------------------------------------------------------
    # ALL THREE ALIGNMENT
    # --------------------------------------------------------

    df["all_three_same"] = (
        (df["5m_1520"] == df["3m_1524"])
        &
        (df["3m_1524"] == df["1m_1528"])
        &
        (df["5m_1520"] != "DOJI")
    )

    return df


# ============================================================
# ANALYZE CONDITION
# ============================================================

def analyze_condition(
    df,
    condition_name,
    mask
):

    subset = df[mask].copy()

    trades = len(subset)

    if trades < MIN_TRADES:
        return None

    # --------------------------------------------------------
    # Long
    # --------------------------------------------------------

    long_returns = subset[
        "long_1529_return"
    ]

    long_1527 = subset[
        "long_1527_return"
    ]

    # --------------------------------------------------------
    # Short
    # --------------------------------------------------------

    short_returns = subset[
        "short_1529_return"
    ]

    short_1527 = subset[
        "short_1527_return"
    ]

    # --------------------------------------------------------
    # Select better direction automatically
    # --------------------------------------------------------

    long_avg = long_returns.mean()
    short_avg = short_returns.mean()

    if long_avg >= short_avg:

        chosen_direction = "LONG"

        returns_1529 = long_returns
        returns_1527 = long_1527

    else:

        chosen_direction = "SHORT"

        returns_1529 = short_returns
        returns_1527 = short_1527

    # --------------------------------------------------------
    # Statistics
    # --------------------------------------------------------

    def stats(series):

        wins = (
            series > 0
        ).sum()

        losses = (
            series < 0
        ).sum()

        win_rate = (
            wins / len(series) * 100
        )

        gross_profit = series[
            series > 0
        ].sum()

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
            win_rate,
            series.mean(),
            series.median(),
            pf,
            series.sum()
        )

    (
        wr_1529,
        avg_1529,
        med_1529,
        pf_1529,
        total_1529
    ) = stats(
        returns_1529
    )

    (
        wr_1527,
        avg_1527,
        med_1527,
        pf_1527,
        total_1527
    ) = stats(
        returns_1527
    )

    return {

        "condition":
            condition_name,

        "trades":
            trades,

        "direction":
            chosen_direction,

        "win_rate_15:29":
            wr_1529,

        "avg_return_15:29":
            avg_1529,

        "median_15:29":
            med_1529,

        "profit_factor_15:29":
            pf_1529,

        "total_return_15:29":
            total_1529,

        "win_rate_15:27":
            wr_1527,

        "avg_return_15:27":
            avg_1527,

        "median_15:27":
            med_1527,

        "profit_factor_15:27":
            pf_1527,

        "total_return_15:27":
            total_1527
    }


# ============================================================
# GENERATE PATTERN TESTS
# ============================================================

def generate_tests(df):

    tests = []

    # ========================================================
    # BASIC CANDLE DIRECTIONS
    # ========================================================

    directions = [
        ("5m_1520_BULL",
         df["5m_1520"] == "BULL"),

        ("5m_1520_BEAR",
         df["5m_1520"] == "BEAR"),

        ("5m_1525_BULL",
         df["5m_1525"] == "BULL"),

        ("5m_1525_BEAR",
         df["5m_1525"] == "BEAR"),

        ("3m_1524_BULL",
         df["3m_1524"] == "BULL"),

        ("3m_1524_BEAR",
         df["3m_1524"] == "BEAR"),

        ("3m_1527_BULL",
         df["3m_1527"] == "BULL"),

        ("3m_1527_BEAR",
         df["3m_1527"] == "BEAR"),

        ("1m_1528_BULL",
         df["1m_1528"] == "BULL"),

        ("1m_1528_BEAR",
         df["1m_1528"] == "BEAR"),

        ("1m_1529_BULL",
         df["1m_1529"] == "BULL"),

        ("1m_1529_BEAR",
         df["1m_1529"] == "BEAR")
    ]

    tests.extend(
        directions
    )

    # ========================================================
    # SAME / OPPOSITE
    # ========================================================

    relationship_tests = [

        ("5m_same",
         df["5m_same"]),

        ("5m_opposite",
         df["5m_opposite"]),

        ("3m_same",
         df["3m_same"]),

        ("3m_opposite",
         df["3m_opposite"]),

        ("1m_same",
         df["1m_same"]),

        ("1m_opposite",
         df["1m_opposite"]),

        ("5m15:20_3m15:24_same",
         df["5m3m_same"]),

        ("5m15:20_3m15:24_opposite",
         df["5m3m_opposite"]),

        ("5m15:20_1m15:28_same",
         df["5m1m_same"]),

        ("5m15:20_1m15:28_opposite",
         df["5m1m_opposite"]),

        ("3m15:24_1m15:28_same",
         df["3m1m_same"]),

        ("3m15:24_1m15:28_opposite",
         df["3m1m_opposite"]),

        ("ALL_THREE_SAME",
         df["all_three_same"])
    ]

    tests.extend(
        relationship_tests
    )

    # ========================================================
    # VOLUME RELATIONSHIPS
    # ========================================================

    volume_tests = [

        (
            "5m_15:20_volume_MORE",
            df["5m_1520_more_volume"]
        ),

        (
            "5m_15:20_volume_LESS",
            ~df["5m_1520_more_volume"]
        ),

        (
            "3m_15:24_volume_MORE",
            df["3m_1524_more_volume"]
        ),

        (
            "3m_15:24_volume_LESS",
            ~df["3m_1524_more_volume"]
        ),

        (
            "1m_15:28_volume_MORE",
            df["1m_1528_more_volume"]
        ),

        (
            "1m_15:28_volume_LESS",
            ~df["1m_1528_more_volume"]
        )
    ]

    tests.extend(
        volume_tests
    )

    # ========================================================
    # COMBINATIONS
    # ========================================================

    combination_tests = [

        (
            "5m_OPPOSITE + 5m_15:20_volume_MORE",
            df["5m_opposite"]
            &
            df["5m_1520_more_volume"]
        ),

        (
            "3m_OPPOSITE + 3m_15:24_volume_MORE",
            df["3m_opposite"]
            &
            df["3m_1524_more_volume"]
        ),

        (
            "1m_OPPOSITE + 1m_15:28_volume_MORE",
            df["1m_opposite"]
            &
            df["1m_1528_more_volume"]
        ),

        (
            "5m15:20 = 3m15:24 + 3m_OPPOSITE",
            df["5m3m_same"]
            &
            df["3m_opposite"]
        ),

        (
            "5m15:20 = 1m15:28 + 1m_VOLUME_MORE",
            df["5m1m_same"]
            &
            df["1m_1528_more_volume"]
        ),

        (
            "3m15:24 = 1m15:28 + 1m_VOLUME_MORE",
            df["3m1m_same"]
            &
            df["1m_1528_more_volume"]
        ),

        (
            "ALL_THREE_SAME + 1m_VOLUME_MORE",
            df["all_three_same"]
            &
            df["1m_1528_more_volume"]
        ),

        (
            "ALL_THREE_SAME + 3m_VOLUME_MORE",
            df["all_three_same"]
            &
            df["3m_1524_more_volume"]
        ),

        (
            "ALL_THREE_SAME + 5m_VOLUME_MORE",
            df["all_three_same"]
            &
            df["5m_1520_more_volume"]
        ),

        (
            "ALL_THREE_SAME + ALL_VOLUME_MORE",
            df["all_three_same"]
            &
            df["5m_1520_more_volume"]
            &
            df["3m_1524_more_volume"]
            &
            df["1m_1528_more_volume"]
        )
    ]

    tests.extend(
        combination_tests
    )

    return tests


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 75)
    print("NSE MULTI-TIMEFRAME PATTERN SCANNER")
    print("=" * 75)

    print(
        f"\nMinimum observations per pattern: "
        f"{MIN_TRADES}"
    )

    print(
        "\nThe scanner does NOT assume a trading direction."
    )

    print(
        "It tests LONG and SHORT and chooses the stronger "
        "historical direction for each pattern."
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
        f"\nStock files found: {len(files)}"
    )

    # ========================================================
    # EXTRACT ALL DAILY DATA
    # ========================================================

    all_rows = []

    for number, filename in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing stocks "
            f"{number}/{len(files)}",
            end=""
        )

        rows = extract_stock_days(
            z,
            filename
        )

        all_rows.extend(
            rows
        )

    print("\n")

    if not all_rows:

        print(
            "No usable data found."
        )

        return

    df = pd.DataFrame(
        all_rows
    )

    print(
        f"Total signal-day observations: "
        f"{len(df):,}"
    )

    # ========================================================
    # RELATIONSHIPS
    # ========================================================

    df = add_relationships(
        df
    )

    # ========================================================
    # GENERATE TESTS
    # ========================================================

    tests = generate_tests(
        df
    )

    results = []

    print(
        f"\nTesting "
        f"{len(tests)} patterns..."
    )

    # ========================================================
    # ANALYZE
    # ========================================================

    for name, mask in tests:

        result = analyze_condition(
            df,
            name,
            mask
        )

        if result is not None:

            results.append(
                result
            )

    if not results:

        print(
            "No patterns reached "
            f"the {MIN_TRADES} trade minimum."
        )

        return

    results_df = pd.DataFrame(
        results
    )

    # ========================================================
    # SORT BY AVERAGE RETURN
    # ========================================================

    results_df = results_df.sort_values(
        by="avg_return_15:29",
        ascending=False
    )

    # ========================================================
    # TOP PATTERNS — 15:29
    # ========================================================

    print("\n")
    print("=" * 75)
    print("TOP 20 PATTERNS — 15:29 EXIT")
    print("=" * 75)

    print(
        results_df[
            [
                "condition",
                "trades",
                "direction",
                "win_rate_15:29",
                "avg_return_15:29",
                "profit_factor_15:29"
            ]
        ]
        .head(20)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # TOP PATTERNS — 15:27
    # ========================================================

    results_1527 = results_df.sort_values(
        by="avg_return_15:27",
        ascending=False
    )

    print("\n")
    print("=" * 75)
    print("TOP 20 PATTERNS — 15:27 EXIT")
    print("=" * 75)

    print(
        results_1527[
            [
                "condition",
                "trades",
                "direction",
                "win_rate_15:27",
                "avg_return_15:27",
                "profit_factor_15:27"
            ]
        ]
        .head(20)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # TOP BY PROFIT FACTOR
    # ========================================================

    results_pf = results_df.sort_values(
        by="profit_factor_15:29",
        ascending=False
    )

    print("\n")
    print("=" * 75)
    print("TOP 20 PATTERNS — PROFIT FACTOR")
    print("=" * 75)

    print(
        results_pf[
            [
                "condition",
                "trades",
                "direction",
                "win_rate_15:29",
                "avg_return_15:29",
                "profit_factor_15:29"
            ]
        ]
        .head(20)
        .to_string(
            index=False
        )
    )

    # ========================================================
    # SAVE PATTERN RESULTS
    # ========================================================

    pattern_file = (
        "pattern_scanner_results.csv"
    )

    results_df.to_csv(
        pattern_file,
        index=False
    )

    # ========================================================
    # SAVE RAW DAILY FEATURES
    # ========================================================

    raw_file = (
        "pattern_scanner_daily_data.csv"
    )

    df.to_csv(
        raw_file,
        index=False
    )

    # ========================================================
    # YEAR-BY-YEAR TEST OF TOP 10
    # ========================================================

    results_df["rank"] = range(
        1,
        len(results_df) + 1
    )

    top_patterns = results_df.head(
        10
    )["condition"].tolist()

    print("\n")
    print("=" * 75)
    print("TOP PATTERNS — YEAR-BY-YEAR CHECK")
    print("=" * 75)

    df["year"] = (
        df["trade_date"]
        .astype(str)
        .str[:4]
    )

    for pattern in top_patterns:

        mask = None

        for name, test_mask in tests:

            if name == pattern:

                mask = test_mask
                break

        if mask is None:
            continue

        subset = df[mask].copy()

        print(
            f"\n{pattern}"
        )

        for year in sorted(
            subset["year"].unique()
        ):

            yearly = subset[
                subset["year"] == year
            ]

            if len(yearly) < 20:
                continue

            # Determine direction using that year's data
            long_avg = (
                yearly["long_1529_return"]
                .mean()
            )

            short_avg = (
                yearly["short_1529_return"]
                .mean()
            )

            if long_avg >= short_avg:

                chosen = (
                    yearly[
                        "long_1529_return"
                    ]
                )

                side = "LONG"

            else:

                chosen = (
                    yearly[
                        "short_1529_return"
                    ]
                )

                side = "SHORT"

            wins = (
                chosen > 0
            ).sum()

            win_rate = (
                wins /
                len(chosen) *
                100
            )

            print(
                f"  {year}: "
                f"{side} | "
                f"N={len(chosen)} | "
                f"Win={win_rate:.2f}% | "
                f"Avg={chosen.mean():.4f}%"
            )

    # ========================================================
    # FINISHED
    # ========================================================

    print("\n")
    print("=" * 75)
    print("PATTERN SCAN COMPLETE")
    print("=" * 75)

    print(
        f"\nPattern results saved to:"
        f"\n{pattern_file}"
    )

    print(
        f"\nDaily feature data saved to:"
        f"\n{raw_file}"
    )

    print(
        "\nThe next step is to take the strongest "
        "patterns and test them on unseen data."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
