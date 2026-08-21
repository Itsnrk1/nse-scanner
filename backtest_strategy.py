import pandas as pd
import requests
import zipfile
import io
import os


# ============================================================
# DATA SOURCE
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/nse-fno-1min-data/"
    "releases/download/v1.0.0/stocks_1m_csvs.zip"
)


# ============================================================
# DIRECTION
# ============================================================

def direction(open_price, close_price):

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


# ============================================================
# DOWNLOAD DATA
# ============================================================

def download_dataset():

    print("=" * 70)
    print("DOWNLOADING NSE 1-MINUTE DATA")
    print("=" * 70)

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
        f"Downloaded: "
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
        # Convert timestamp to IST
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
        # Volume
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

        # ----------------------------------------------------
        # Prices
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

    except Exception as e:

        print(
            f"\nError loading {filename}: {e}"
        )

        return None


# ============================================================
# BUILD 15-MINUTE CANDLE FROM 1-MINUTE DATA
# ============================================================

def fifteen_minute_candle(
    day,
    start_time
):

    hour, minute = map(
        int,
        start_time.split(":")
    )

    times = [
        f"{hour:02d}:{minute + i:02d}"
        for i in range(15)
    ]

    rows = []

    for t in times:

        if t not in day:
            return None

        rows.append(
            day[t]
        )

    # IMPORTANT:
    # Open = first 1-minute candle open
    # Close = last 1-minute candle close
    # Volume = sum of all 15 one-minute volumes

    return {

        "open": float(
            rows[0]["open"]
        ),

        "high": max(
            float(row["high"])
            for row in rows
        ),

        "low": min(
            float(row["low"])
            for row in rows
        ),

        "close": float(
            rows[-1]["close"]
        ),

        "volume": sum(
            float(row["volume"])
            for row in rows
        )
    }


# ============================================================
# EVALUATE COMMON CONDITIONS
# ============================================================

def get_common_signal(day):

    # ========================================================
    # REQUIRED 1-MINUTE CANDLES
    # ========================================================

    if "15:28" not in day:
        return None

    if "15:29" not in day:
        return None

    # ========================================================
    # 1-MINUTE 15:28
    # ========================================================

    dir_1528 = direction(
        day["15:28"]["open"],
        day["15:28"]["close"]
    )

    if dir_1528 == 0:
        return None

    # ========================================================
    # 1-MINUTE 15:29
    # ========================================================

    dir_1529 = direction(
        day["15:29"]["open"],
        day["15:29"]["close"]
    )

    if dir_1529 == 0:
        return None

    # ========================================================
    # COMMON CONDITION 1
    #
    # 15:28 AND 15:29
    # MUST BE SAME TREND
    # ========================================================

    if dir_1528 != dir_1529:
        return None

    # ========================================================
    # COMMON CONDITION 2
    #
    # 15:28 VOLUME > 15:29 VOLUME
    # ========================================================

    volume_1528 = float(
        day["15:28"]["volume"]
    )

    volume_1529 = float(
        day["15:29"]["volume"]
    )

    if not (
        volume_1528 >
        volume_1529
    ):
        return None

    # ========================================================
    # BUILD 15-MINUTE CANDLES
    # ========================================================

    candle_1500 = fifteen_minute_candle(
        day,
        "15:00"
    )

    candle_1515 = fifteen_minute_candle(
        day,
        "15:15"
    )

    if (
        candle_1500 is None
        or
        candle_1515 is None
    ):
        return None

    # ========================================================
    # 15-MINUTE TRENDS
    # ========================================================

    dir_1500 = direction(
        candle_1500["open"],
        candle_1500["close"]
    )

    dir_1515 = direction(
        candle_1515["open"],
        candle_1515["close"]
    )

    if dir_1500 == 0:
        return None

    if dir_1515 == 0:
        return None

    # ========================================================
    # COMMON CONDITION 3
    #
    # 15:15 VOLUME > 15:00 VOLUME
    # ========================================================

    if not (
        candle_1515["volume"]
        >
        candle_1500["volume"]
    ):
        return None

    return {
        "dir_1528": dir_1528,
        "dir_1529": dir_1529,
        "dir_1500": dir_1500,
        "dir_1515": dir_1515
    }


# ============================================================
# BACKTEST ONE STOCK
# ============================================================

def backtest_stock(
    z,
    filename
):

    df = load_stock(
        z,
        filename
    )

    if df is None or df.empty:
        return []

    symbol = os.path.basename(
        filename
    ).replace(
        "_1m.csv.gz",
        ""
    )

    # --------------------------------------------------------
    # Group by date
    # --------------------------------------------------------

    grouped = {
        date: group
        for date, group
        in df.groupby("date")
    }

    dates = sorted(
        grouped.keys()
    )

    results = []

    # ========================================================
    # LOOP THROUGH SIGNAL DAYS
    # ========================================================

    for i, signal_date in enumerate(
        dates
    ):

        if i + 1 >= len(dates):
            break

        trade_date = dates[i + 1]

        signal_data = grouped[
            signal_date
        ]

        trade_data = grouped[
            trade_date
        ]

        # ----------------------------------------------------
        # Build signal-day dictionary
        # ----------------------------------------------------

        day = {}

        for _, row in signal_data.iterrows():

            day[row["hm"]] = row.to_dict()

        # ----------------------------------------------------
        # Common conditions
        # ----------------------------------------------------

        signal = get_common_signal(
            day
        )

        if signal is None:
            continue

        # ====================================================
        # BUILD NEXT DAY
        # ====================================================

        next_day = {}

        for _, row in trade_data.iterrows():

            next_day[row["hm"]] = row.to_dict()

        # ====================================================
        # ENTRY
        # ====================================================

        if "09:15" not in next_day:
            continue

        entry_price = float(
            next_day["09:15"]["open"]
        )

        if entry_price <= 0:
            continue

        # ====================================================
        # EXIT
        # ====================================================

        if "15:29" not in next_day:
            continue

        exit_price = float(
            next_day["15:29"]["close"]
        )

        # ====================================================
        # DIRECTION
        #
        # Trade direction is the aligned
        # 1-minute 15:28 / 15:29 trend.
        # ====================================================

        if signal["dir_1528"] == 1:

            trade_direction = "LONG"

            return_pct = (
                exit_price -
                entry_price
            ) / entry_price * 100

        else:

            trade_direction = "SHORT"

            return_pct = (
                entry_price -
                exit_price
            ) / entry_price * 100

        # ====================================================
        # BACKTEST 1
        #
        # BOTH 15-MINUTE CANDLES MUST ALIGN
        #
        # 1m 15:28
        #     =
        # 1m 15:29
        #     =
        # 15m 15:00
        #     =
        # 15m 15:15
        # ====================================================

        both_align = (
            signal["dir_1528"]
            ==
            signal["dir_1500"]
            ==
            signal["dir_1515"]
        )

        # ====================================================
        # BACKTEST 2
        #
        # ONLY 15:15 MUST ALIGN
        #
        # 1m 15:28
        #     =
        # 1m 15:29
        #     =
        # 15m 15:15
        #
        # 15m 15:00 trend can be different.
        # ====================================================

        only_1515_align = (
            signal["dir_1528"]
            ==
            signal["dir_1515"]
        )

        # ====================================================
        # SAVE ONLY WHEN APPLICABLE
        # ====================================================

        if both_align:

            results.append({

                "test": "BOTH_15M_ALIGN",

                "symbol": symbol,

                "signal_date":
                    signal_date,

                "trade_date":
                    trade_date,

                "direction":
                    trade_direction,

                "1m_15:28_trend":
                    signal["dir_1528"],

                "1m_15:29_trend":
                    signal["dir_1529"],

                "15m_15:00_trend":
                    signal["dir_1500"],

                "15m_15:15_trend":
                    signal["dir_1515"],

                "entry_09:15_open":
                    entry_price,

                "exit_15:29_close":
                    exit_price,

                "return_pct":
                    return_pct,

                "result": (
                    "WIN"
                    if return_pct > 0
                    else "LOSS"
                )
            })

        elif only_1515_align:

            results.append({

                "test": "ONLY_15M_15:15_ALIGN",

                "symbol": symbol,

                "signal_date":
                    signal_date,

                "trade_date":
                    trade_date,

                "direction":
                    trade_direction,

                "1m_15:28_trend":
                    signal["dir_1528"],

                "1m_15:29_trend":
                    signal["dir_1529"],

                "15m_15:00_trend":
                    signal["dir_1500"],

                "15m_15:15_trend":
                    signal["dir_1515"],

                "entry_09:15_open":
                    entry_price,

                "exit_15:29_close":
                    exit_price,

                "return_pct":
                    return_pct,

                "result": (
                    "WIN"
                    if return_pct > 0
                    else "LOSS"
                )
            })

    return results


# ============================================================
# STATISTICS
# ============================================================

def calculate_statistics(
    results
):

    total = len(results)

    if total == 0:

        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0,
            "average": 0,
            "median": 0,
            "profit_factor": 0,
            "total_return": 0,
            "best": 0,
            "worst": 0
        }

    wins = (
        results["result"] == "WIN"
    ).sum()

    losses = (
        results["result"] == "LOSS"
    ).sum()

    win_rate = (
        wins /
        total *
        100
    )

    average = (
        results["return_pct"].mean()
    )

    median = (
        results["return_pct"].median()
    )

    total_return = (
        results["return_pct"].sum()
    )

    best = (
        results["return_pct"].max()
    )

    worst = (
        results["return_pct"].min()
    )

    gross_profit = results.loc[
        results["return_pct"] > 0,
        "return_pct"
    ].sum()

    gross_loss = abs(
        results.loc[
            results["return_pct"] < 0,
            "return_pct"
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
        "trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "average": average,
        "median": median,
        "profit_factor": profit_factor,
        "total_return": total_return,
        "best": best,
        "worst": worst
    }


# ============================================================
# PRINT STATISTICS
# ============================================================

def print_statistics(
    title,
    results
):

    stats = calculate_statistics(
        results
    )

    print("\n")
    print("=" * 70)
    print(title)
    print("=" * 70)

    print(
        f"Trades        : "
        f"{stats['trades']}"
    )

    print(
        f"Wins          : "
        f"{stats['wins']}"
    )

    print(
        f"Losses        : "
        f"{stats['losses']}"
    )

    print(
        f"Win rate      : "
        f"{stats['win_rate']:.2f}%"
    )

    print(
        f"Average trade : "
        f"{stats['average']:.4f}%"
    )

    print(
        f"Median trade  : "
        f"{stats['median']:.4f}%"
    )

    print(
        f"Profit factor : "
        f"{stats['profit_factor']:.3f}"
    )

    print(
        f"Total raw %   : "
        f"{stats['total_return']:.2f}%"
    )

    print(
        f"Best trade    : "
        f"{stats['best']:.4f}%"
    )

    print(
        f"Worst trade   : "
        f"{stats['worst']:.4f}%"
    )


# ============================================================
# LONG / SHORT BREAKDOWN
# ============================================================

def print_direction_breakdown(
    results
):

    for side in [
        "LONG",
        "SHORT"
    ]:

        subset = results[
            results["direction"] == side
        ]

        print_statistics(
            side,
            subset
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 70)
    print("15-MINUTE + 1-MINUTE TWO-WAY ALIGNMENT BACKTEST")
    print("=" * 70)

    print("\nCOMMON CONDITIONS:")

    print(
        "1. 1m 15:28 and 15:29 = SAME TREND"
    )

    print(
        "2. 1m 15:28 volume > 15:29 volume"
    )

    print(
        "3. 15m 15:15 volume > 15m 15:00 volume"
    )

    print("\nBACKTEST 1:")

    print(
        "1m 15:28 = 1m 15:29 "
        "= 15m 15:00 = 15m 15:15"
    )

    print("\nBACKTEST 2:")

    print(
        "1m 15:28 = 1m 15:29 "
        "= 15m 15:15"
    )

    print(
        "15m 15:00 trend is NOT required "
        "to align in Backtest 2"
    )

    print("\nTRADE:")

    print(
        "ENTRY = NEXT TRADING DAY 09:15 OPEN"
    )

    print(
        "EXIT  = NEXT TRADING DAY 15:29 CLOSE"
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
        f"\nStock files found: "
        f"{len(files)}"
    )

    # ========================================================
    # PROCESS
    # ========================================================

    all_results = []

    for number, filename in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing "
            f"{number}/{len(files)}",
            end=""
        )

        trades = backtest_stock(
            z,
            filename
        )

        all_results.extend(
            trades
        )

    print("\n")

    # ========================================================
    # NO RESULTS
    # ========================================================

    if not all_results:

        print("=" * 70)
        print("NO TRADES FOUND")
        print("=" * 70)

        return

    results = pd.DataFrame(
        all_results
    )

    # ========================================================
    # SPLIT TESTS
    # ========================================================

    both_align = results[
        results["test"]
        ==
        "BOTH_15M_ALIGN"
    ]

    only_1515 = results[
        results["test"]
        ==
        "ONLY_15M_15:15_ALIGN"
    ]

    # ========================================================
    # TEST 1
    # ========================================================

    print_statistics(
        "BACKTEST 1 — BOTH 15-MINUTE CANDLES ALIGN",
        both_align
    )

    print("\nLONG / SHORT:")

    print_direction_breakdown(
        both_align
    )

    # ========================================================
    # TEST 2
    # ========================================================

    print_statistics(
        "BACKTEST 2 — ONLY 15:15 ALIGNS",
        only_1515
    )

    print("\nLONG / SHORT:")

    print_direction_breakdown(
        only_1515
    )

    # ========================================================
    # COMPARISON
    # ========================================================

    stats_1 = calculate_statistics(
        both_align
    )

    stats_2 = calculate_statistics(
        only_1515
    )

    print("\n")
    print("=" * 70)
    print("TWO-BACKTEST COMPARISON")
    print("=" * 70)

    print(
        f"\n{'METRIC':<25}"
        f"{'BOTH ALIGN':>18}"
        f"{'ONLY 15:15':>18}"
    )

    print("-" * 65)

    print(
        f"{'Trades':<25}"
        f"{stats_1['trades']:>18}"
        f"{stats_2['trades']:>18}"
    )

    print(
        f"{'Win rate':<25}"
        f"{stats_1['win_rate']:>17.2f}%"
        f"{stats_2['win_rate']:>17.2f}%"
    )

    print(
        f"{'Average trade':<25}"
        f"{stats_1['average']:>17.4f}%"
        f"{stats_2['average']:>17.4f}%"
    )

    print(
        f"{'Median trade':<25}"
        f"{stats_1['median']:>17.4f}%"
        f"{stats_2['median']:>17.4f}%"
    )

    print(
        f"{'Profit factor':<25}"
        f"{stats_1['profit_factor']:>18.3f}"
        f"{stats_2['profit_factor']:>18.3f}"
    )

    print(
        f"{'Total raw return':<25}"
        f"{stats_1['total_return']:>17.2f}%"
        f"{stats_2['total_return']:>17.2f}%"
    )

    # ========================================================
    # YEARLY BREAKDOWN
    # ========================================================

    print("\n")
    print("=" * 70)
    print("YEARLY BREAKDOWN")
    print("=" * 70)

    results["year"] = (
        results["trade_date"]
        .astype(str)
        .str[:4]
    )

    for test_name in [
        "BOTH_15M_ALIGN",
        "ONLY_15M_15:15_ALIGN"
    ]:

        test_data = results[
            results["test"] == test_name
        ]

        print("\n")
        print(
            f"--- {test_name} ---"
        )

        for year in sorted(
            test_data["year"].unique()
        ):

            yearly = test_data[
                test_data["year"] == year
            ]

            stats = calculate_statistics(
                yearly
            )

            print(
                f"{year}: "
                f"Trades={stats['trades']} | "
                f"Win={stats['win_rate']:.2f}% | "
                f"Avg={stats['average']:.4f}% | "
                f"PF={stats['profit_factor']:.3f}"
            )

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    output_file = (
        "15m_1m_two_alignment_backtest.csv"
    )

    results.to_csv(
        output_file,
        index=False
    )

    print("\n")
    print("=" * 70)
    print("BACKTEST COMPLETE")
    print("=" * 70)

    print(
        f"Detailed results saved to:"
        f"\n{output_file}"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
