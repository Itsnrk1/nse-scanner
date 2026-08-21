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
# TREND
# ============================================================

def direction(open_price, close_price):

    if close_price > open_price:
        return 1       # Bullish

    if close_price < open_price:
        return -1      # Bearish

    return 0           # Doji


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
# BUILD 3-MINUTE CANDLE
# ============================================================

def three_minute_candle(day, start_time):

    hour, minute = map(
        int,
        start_time.split(":")
    )

    times = [
        f"{hour:02d}:{minute + i:02d}"
        for i in range(3)
    ]

    rows = []

    for t in times:

        if t not in day:
            return None

        rows.append(
            day[t]
        )

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
# BUILD 5-MINUTE CANDLE
# ============================================================

def five_minute_candle(day, start_time):

    hour, minute = map(
        int,
        start_time.split(":")
    )

    times = [
        f"{hour:02d}:{minute + i:02d}"
        for i in range(5)
    ]

    rows = []

    for t in times:

        if t not in day:
            return None

        rows.append(
            day[t]
        )

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
# EVALUATE SIGNAL
# ============================================================

def evaluate_signal_day(day):

    # ========================================================
    # 5-MINUTE 15:20
    #
    # 15:20 - 15:24
    # ========================================================

    candle_1520 = five_minute_candle(
        day,
        "15:20"
    )

    # ========================================================
    # 5-MINUTE 15:25
    #
    # 15:25 - 15:29
    # ========================================================

    candle_1525 = five_minute_candle(
        day,
        "15:25"
    )

    if (
        candle_1520 is None
        or
        candle_1525 is None
    ):
        return None

    dir_1520 = direction(
        candle_1520["open"],
        candle_1520["close"]
    )

    dir_1525 = direction(
        candle_1525["open"],
        candle_1525["close"]
    )

    if dir_1520 == 0:
        return None

    if dir_1525 == 0:
        return None

    # ========================================================
    # 5-MINUTE VOLUME
    #
    # 15:20 volume > 15:25 volume
    # ========================================================

    volume_1520 = float(
        candle_1520["volume"]
    )

    volume_1525 = float(
        candle_1525["volume"]
    )

    if volume_1520 <= volume_1525:
        return None


    # ========================================================
    # 3-MINUTE 15:24
    #
    # 15:24 - 15:26
    # ========================================================

    candle_1524 = three_minute_candle(
        day,
        "15:24"
    )

    # ========================================================
    # 3-MINUTE 15:27
    #
    # 15:27 - 15:29
    # ========================================================

    candle_1527 = three_minute_candle(
        day,
        "15:27"
    )

    if (
        candle_1524 is None
        or
        candle_1527 is None
    ):
        return None

    dir_1524 = direction(
        candle_1524["open"],
        candle_1524["close"]
    )

    dir_1527 = direction(
        candle_1527["open"],
        candle_1527["close"]
    )

    if dir_1524 == 0:
        return None

    if dir_1527 == 0:
        return None

    # ========================================================
    # 3-MINUTE TREND
    #
    # 15:24 AND 15:27 OPPOSITE
    # ========================================================

    if dir_1524 == dir_1527:
        return None

    # ========================================================
    # 3-MINUTE VOLUME
    #
    # 15:24 volume > 15:27 volume
    # ========================================================

    volume_1524 = float(
        candle_1524["volume"]
    )

    volume_1527 = float(
        candle_1527["volume"]
    )

    if volume_1524 <= volume_1527:
        return None

    # ========================================================
    # 3-MINUTE / 5-MINUTE ALIGNMENT
    #
    # 3m 15:24 = 5m 15:20
    # ========================================================

    if dir_1524 != dir_1520:
        return None


    # ========================================================
    # 1-MINUTE 15:28
    # ========================================================

    if "15:28" not in day:
        return None

    dir_1528 = direction(
        day["15:28"]["open"],
        day["15:28"]["close"]
    )

    if dir_1528 == 0:
        return None


    # ========================================================
    # 1-MINUTE / 5-MINUTE ALIGNMENT
    #
    # 1m 15:28 = 5m 15:20
    # ========================================================

    if dir_1528 != dir_1520:
        return None


    # ========================================================
    # 1-MINUTE 15:29
    # ========================================================

    if "15:29" not in day:
        return None


    # ========================================================
    # 1-MINUTE VOLUME
    #
    # 15:28 volume > 15:29 volume
    # ========================================================

    volume_1528 = float(
        day["15:28"]["volume"]
    )

    volume_1529 = float(
        day["15:29"]["volume"]
    )

    if volume_1528 <= volume_1529:
        return None


    # ========================================================
    # FINAL DIRECTION
    # ========================================================

    if dir_1528 == 1:

        trade_direction = "LONG"

    else:

        trade_direction = "SHORT"


    return {

        "direction":
            trade_direction,

        "3m_15:24_trend":
            dir_1524,

        "3m_15:27_trend":
            dir_1527,

        "5m_15:20_trend":
            dir_1520,

        "5m_15:25_trend":
            dir_1525,

        "1m_15:28_trend":
            dir_1528,

        "3m_15:24_volume":
            volume_1524,

        "3m_15:27_volume":
            volume_1527,

        "5m_15:20_volume":
            volume_1520,

        "5m_15:25_volume":
            volume_1525,

        "1m_15:28_volume":
            volume_1528,

        "1m_15:29_volume":
            volume_1529
    }


# ============================================================
# BACKTEST ONE STOCK
# ============================================================

def backtest_stock(z, filename):

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

    grouped = {
        date: group
        for date, group
        in df.groupby("date")
    }

    dates = sorted(
        grouped.keys()
    )

    trades = []

    # ========================================================
    # SIGNAL DAY LOOP
    # ========================================================

    for i, signal_date in enumerate(dates):

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
        # Signal-day dictionary
        # ----------------------------------------------------

        day = {}

        for _, row in signal_data.iterrows():

            day[row["hm"]] = row.to_dict()

        # ----------------------------------------------------
        # Evaluate signal
        # ----------------------------------------------------

        signal = evaluate_signal_day(
            day
        )

        if signal is None:
            continue

        # ----------------------------------------------------
        # Next trading day dictionary
        # ----------------------------------------------------

        next_day = {}

        for _, row in trade_data.iterrows():

            next_day[row["hm"]] = row.to_dict()

        # ====================================================
        # ENTRY
        #
        # NEXT DAY 09:15 OPEN
        # ====================================================

        if "09:15" not in next_day:
            continue

        entry_price = float(
            next_day["09:15"]["open"]
        )

        if entry_price <= 0:
            continue

        # ====================================================
        # EXIT A
        #
        # NEXT DAY 15:29 CLOSE
        # ====================================================

        if "15:29" not in next_day:
            continue

        exit_1529 = float(
            next_day["15:29"]["close"]
        )

        # ====================================================
        # EXIT B
        #
        # NEXT DAY 15:27 OPEN
        #
        # This is the OPEN of the 15:27
        # 3-minute candle.
        # ====================================================

        if "15:27" not in next_day:
            continue

        exit_1527 = float(
            next_day["15:27"]["open"]
        )

        # ====================================================
        # RETURN — EXIT A
        # ====================================================

        if signal["direction"] == "LONG":

            return_1529 = (
                exit_1529 -
                entry_price
            ) / entry_price * 100

        else:

            return_1529 = (
                entry_price -
                exit_1529
            ) / entry_price * 100

        # ====================================================
        # RETURN — EXIT B
        # ====================================================

        if signal["direction"] == "LONG":

            return_1527 = (
                exit_1527 -
                entry_price
            ) / entry_price * 100

        else:

            return_1527 = (
                entry_price -
                exit_1527
            ) / entry_price * 100

        # ====================================================
        # SAVE TRADE
        # ====================================================

        trades.append({

            "symbol":
                symbol,

            "signal_date":
                signal_date,

            "trade_date":
                trade_date,

            "direction":
                signal["direction"],

            "3m_15:24_trend":
                signal["3m_15:24_trend"],

            "3m_15:27_trend":
                signal["3m_15:27_trend"],

            "5m_15:20_trend":
                signal["5m_15:20_trend"],

            "5m_15:25_trend":
                signal["5m_15:25_trend"],

            "1m_15:28_trend":
                signal["1m_15:28_trend"],

            "3m_15:24_volume":
                signal["3m_15:24_volume"],

            "3m_15:27_volume":
                signal["3m_15:27_volume"],

            "5m_15:20_volume":
                signal["5m_15:20_volume"],

            "5m_15:25_volume":
                signal["5m_15:25_volume"],

            "1m_15:28_volume":
                signal["1m_15:28_volume"],

            "1m_15:29_volume":
                signal["1m_15:29_volume"],

            "entry_09:15_open":
                entry_price,

            "exit_15:27_open":
                exit_1527,

            "exit_15:29_close":
                exit_1529,

            "return_15:27_exit":
                return_1527,

            "return_15:29_exit":
                return_1529,

            "result_15:27_exit":
                (
                    "WIN"
                    if return_1527 > 0
                    else "LOSS"
                ),

            "result_15:29_exit":
                (
                    "WIN"
                    if return_1529 > 0
                    else "LOSS"
                )
        })

    return trades


# ============================================================
# STATISTICS
# ============================================================

def calculate_statistics(results, return_column):

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
        results[return_column] > 0
    ).sum()

    losses = (
        results[return_column] <= 0
    ).sum()

    win_rate = (
        wins / total * 100
    )

    average = (
        results[return_column].mean()
    )

    median = (
        results[return_column].median()
    )

    total_return = (
        results[return_column].sum()
    )

    best = (
        results[return_column].max()
    )

    worst = (
        results[return_column].min()
    )

    gross_profit = results.loc[
        results[return_column] > 0,
        return_column
    ].sum()

    gross_loss = abs(
        results.loc[
            results[return_column] < 0,
            return_column
        ].sum()
    )

    if gross_loss > 0:

        profit_factor = (
            gross_profit / gross_loss
        )

    else:

        profit_factor = float("inf")

    return {

        "trades":
            total,

        "wins":
            wins,

        "losses":
            losses,

        "win_rate":
            win_rate,

        "average":
            average,

        "median":
            median,

        "profit_factor":
            profit_factor,

        "total_return":
            total_return,

        "best":
            best,

        "worst":
            worst
    }


# ============================================================
# PRINT STATISTICS
# ============================================================

def print_statistics(
    title,
    results,
    return_column
):

    stats = calculate_statistics(
        results,
        return_column
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
    results,
    return_column
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
            subset,
            return_column
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 70)
    print("MULTI-TIMEFRAME STRATEGY — TWO EXITS")
    print("=" * 70)

    print("\nSTRATEGY CONDITIONS:")

    print(
        "5m 15:20 = 3m 15:24 = 1m 15:28"
    )

    print(
        "3m 15:24 and 15:27 = OPPOSITE"
    )

    print(
        "3m 15:24 volume > 3m 15:27 volume"
    )

    print(
        "5m 15:20 volume > 5m 15:25 volume"
    )

    print(
        "1m 15:28 volume > 1m 15:29 volume"
    )

    print("\nENTRY:")

    print(
        "NEXT TRADING DAY 09:15 OPEN"
    )

    print("\nEXIT A:")

    print(
        "NEXT TRADING DAY 15:29 CLOSE"
    )

    print("\nEXIT B:")

    print(
        "NEXT TRADING DAY 15:27 OPEN"
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

    all_trades = []

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

        all_trades.extend(
            trades
        )

    print("\n")

    # ========================================================
    # CHECK RESULTS
    # ========================================================

    if not all_trades:

        print("=" * 70)
        print("NO TRADES FOUND")
        print("=" * 70)

        return

    results = pd.DataFrame(
        all_trades
    )

    # ========================================================
    # EXIT A — 15:29 CLOSE
    # ========================================================

    print_statistics(
        "EXIT A — 15:29 CLOSE",
        results,
        "return_15:29_exit"
    )

    print("\nLONG / SHORT — EXIT A")

    print_direction_breakdown(
        results,
        "return_15:29_exit"
    )

    # ========================================================
    # EXIT B — 15:27 OPEN
    # ========================================================

    print_statistics(
        "EXIT B — 15:27 OPEN",
        results,
        "return_15:27_exit"
    )

    print("\nLONG / SHORT — EXIT B")

    print_direction_breakdown(
        results,
        "return_15:27_exit"
    )

    # ========================================================
    # DIRECT COMPARISON
    # ========================================================

    stats_a = calculate_statistics(
        results,
        "return_15:29_exit"
    )

    stats_b = calculate_statistics(
        results,
        "return_15:27_exit"
    )

    print("\n")
    print("=" * 70)
    print("EXIT COMPARISON")
    print("=" * 70)

    print(
        f"\n{'METRIC':<25}"
        f"{'15:27 OPEN':>18}"
        f"{'15:29 CLOSE':>18}"
    )

    print("-" * 65)

    print(
        f"{'Trades':<25}"
        f"{stats_b['trades']:>18}"
        f"{stats_a['trades']:>18}"
    )

    print(
        f"{'Win rate':<25}"
        f"{stats_b['win_rate']:>17.2f}%"
        f"{stats_a['win_rate']:>17.2f}%"
    )

    print(
        f"{'Average trade':<25}"
        f"{stats_b['average']:>17.4f}%"
        f"{stats_a['average']:>17.4f}%"
    )

    print(
        f"{'Median trade':<25}"
        f"{stats_b['median']:>17.4f}%"
        f"{stats_a['median']:>17.4f}%"
    )

    print(
        f"{'Profit factor':<25}"
        f"{stats_b['profit_factor']:>18.3f}"
        f"{stats_a['profit_factor']:>18.3f}"
    )

    print(
        f"{'Total raw return':<25}"
        f"{stats_b['total_return']:>17.2f}%"
        f"{stats_a['total_return']:>17.2f}%"
    )

    print(
        f"{'Best trade':<25}"
        f"{stats_b['best']:>17.4f}%"
        f"{stats_a['best']:>17.4f}%"
    )

    print(
        f"{'Worst trade':<25}"
        f"{stats_b['worst']:>17.4f}%"
        f"{stats_a['worst']:>17.4f}%"
    )

    # ========================================================
    # YEARLY COMPARISON
    # ========================================================

    results["year"] = (
        results["trade_date"]
        .astype(str)
        .str[:4]
    )

    print("\n")
    print("=" * 70)
    print("YEARLY COMPARISON")
    print("=" * 70)

    for year in sorted(
        results["year"].unique()
    ):

        yearly = results[
            results["year"] == year
        ]

        stats_a = calculate_statistics(
            yearly,
            "return_15:29_exit"
        )

        stats_b = calculate_statistics(
            yearly,
            "return_15:27_exit"
        )

        print(
            f"\n{year}"
        )

        print(
            f"  15:27 exit: "
            f"Trades={stats_b['trades']} | "
            f"Win={stats_b['win_rate']:.2f}% | "
            f"Avg={stats_b['average']:.4f}% | "
            f"PF={stats_b['profit_factor']:.3f}"
        )

        print(
            f"  15:29 exit: "
            f"Trades={stats_a['trades']} | "
            f"Win={stats_a['win_rate']:.2f}% | "
            f"Avg={stats_a['average']:.4f}% | "
            f"PF={stats_a['profit_factor']:.3f}"
        )

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    output_file = (
        "multi_timeframe_two_exit_backtest.csv"
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
        f"Detailed results saved to:\n"
        f"{output_file}"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
