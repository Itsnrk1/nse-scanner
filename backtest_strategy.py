import pandas as pd
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

EXIT_TIME = "15:27"


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
        response.headers.get("content-length", 0)
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
# DIRECTION
# ============================================================

def direction(open_price, close_price):

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


# ============================================================
# BUILD 3-MINUTE CANDLE
# ============================================================

def three_minute(day, times):

    rows = []

    for t in times:

        if t not in day:
            return None

        rows.append(day[t])

    return {
        "open": float(rows[0]["open"]),
        "close": float(rows[-1]["close"]),
        "volume": sum(
            float(row["volume"])
            for row in rows
        )
    }


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

        for column in ["open", "close"]:

            if column not in df.columns:
                return None

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

        df = df.dropna(
            subset=[
                "open",
                "close",
                "volume"
            ]
        )

        return df[
            [
                "date",
                "hm",
                "open",
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
# CHECK SIGNAL
# ============================================================

def evaluate_signal_day(day):

    # ========================================================
    # 3-MINUTE REFERENCE = 15:24
    # ========================================================

    candle_1524 = three_minute(
        day,
        [
            "15:24",
            "15:25",
            "15:26"
        ]
    )

    # 3-minute 15:27 candle
    candle_1527 = three_minute(
        day,
        [
            "15:27",
            "15:28",
            "15:29"
        ]
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

    # ========================================================
    # CONDITION 1
    #
    # 15:24 and 15:27 opposite
    # 15:24 volume > 15:27 volume
    # ========================================================

    if not (
        dir_1524 != 0
        and
        dir_1527 != 0
        and
        dir_1524 != dir_1527
        and
        candle_1524["volume"]
        >
        candle_1527["volume"]
    ):
        return None

    # ========================================================
    # CONDITION 2
    #
    # 3-MINUTE MAJORITY
    #
    # Reference = 15:24
    #
    # Other candles:
    # 15:15
    # 15:18
    # 15:21
    # 15:27
    #
    # At least 2 must match 15:24.
    # ========================================================

    matching_3m = 0

    for start in [
        "15:15",
        "15:18",
        "15:21",
        "15:27"
    ]:

        hour, minute = map(
            int,
            start.split(":")
        )

        times = [
            f"{hour:02d}:{minute:02d}",
            f"{hour:02d}:{minute + 1:02d}",
            f"{hour:02d}:{minute + 2:02d}"
        ]

        candle = three_minute(
            day,
            times
        )

        if candle is None:
            continue

        candle_dir = direction(
            candle["open"],
            candle["close"]
        )

        if candle_dir == dir_1524:
            matching_3m += 1

    if matching_3m < 2:
        return None

    # ========================================================
    # CONDITION 3
    #
    # MORNING 3-MINUTE
    #
    # 09:15 = 15:24 trend
    # 09:18 = 15:24 trend
    # ========================================================

    candle_0915 = three_minute(
        day,
        [
            "09:15",
            "09:16",
            "09:17"
        ]
    )

    candle_0918 = three_minute(
        day,
        [
            "09:18",
            "09:19",
            "09:20"
        ]
    )

    if (
        candle_0915 is None
        or
        candle_0918 is None
    ):
        return None

    dir_0915_3m = direction(
        candle_0915["open"],
        candle_0915["close"]
    )

    dir_0918_3m = direction(
        candle_0918["open"],
        candle_0918["close"]
    )

    if not (
        dir_0915_3m == dir_1524
        and
        dir_0918_3m == dir_1524
    ):
        return None

    # ========================================================
    # 1-MINUTE REFERENCE = 15:29
    # ========================================================

    dir_1529 = direction(
        day["15:29"]["open"],
        day["15:29"]["close"]
    )

    # ========================================================
    # CONDITION 4
    #
    # 15:29 trend = 15:24 trend
    # 15:29 volume > 15:28 volume
    # ========================================================

    if not (
        dir_1529 != 0
        and
        dir_1529 == dir_1524
        and
        float(day["15:29"]["volume"])
        >
        float(day["15:28"]["volume"])
    ):
        return None

    # ========================================================
    # CONDITION 5
    #
    # 1-MINUTE MAJORITY
    #
    # Five candles:
    #
    # 15:25
    # 15:26
    # 15:27
    # 15:28
    # 15:29 <- REFERENCE
    #
    # At least 2 of the OTHER 4
    # must match 15:29.
    # ========================================================

    matching_1m = 0

    for t in [
        "15:25",
        "15:26",
        "15:27",
        "15:28"
    ]:

        candle_dir = direction(
            day[t]["open"],
            day[t]["close"]
        )

        if candle_dir == dir_1529:
            matching_1m += 1

    if matching_1m < 2:
        return None

    # ========================================================
    # CONDITION 6
    #
    # MORNING 1-MINUTE
    #
    # 09:15 = 15:29 trend
    # 09:16 = 15:29 trend
    # ========================================================

    dir_0915_1m = direction(
        day["09:15"]["open"],
        day["09:15"]["close"]
    )

    dir_0916_1m = direction(
        day["09:16"]["open"],
        day["09:16"]["close"]
    )

    if not (
        dir_0915_1m == dir_1529
        and
        dir_0916_1m == dir_1529
    ):
        return None

    # ========================================================
    # SIGNAL PASSED
    # ========================================================

    if dir_1524 == 1:
        trade_direction = "LONG"
    else:
        trade_direction = "SHORT"

    return {
        "direction": trade_direction,
        "matching_3m": matching_3m,
        "matching_1m": matching_1m
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
        # Evaluate strategy
        # ----------------------------------------------------

        signal = evaluate_signal_day(day)

        if signal is None:
            continue

        # ----------------------------------------------------
        # Next-day dictionary
        # ----------------------------------------------------

        next_day = {}

        for _, row in trade_data.iterrows():

            next_day[row["hm"]] = row.to_dict()

        if "09:15" not in next_day:
            continue

        if "15:27" not in next_day:
            continue

        # ====================================================
        # ENTRY = NEXT DAY 09:15 OPEN
        # ====================================================

        entry = float(
            next_day["09:15"]["open"]
        )

        # ====================================================
        # EXIT = NEXT DAY 15:27 CLOSE
        # ====================================================

        exit_price = float(
            next_day["15:27"]["close"]
        )

        if entry <= 0:
            continue

        # ====================================================
        # RETURN
        # ====================================================

        if signal["direction"] == "LONG":

            return_pct = (
                exit_price - entry
            ) / entry * 100

        else:

            return_pct = (
                entry - exit_price
            ) / entry * 100

        trades.append({

            "symbol": symbol,
            "signal_date": signal_date,
            "trade_date": trade_date,
            "direction": signal["direction"],

            "3m_matching_candles":
                signal["matching_3m"],

            "1m_matching_candles":
                signal["matching_1m"],

            "entry_09:15_open": entry,
            "exit_15:27_close": exit_price,

            "return_pct": return_pct,

            "result": (
                "WIN"
                if return_pct > 0
                else "LOSS"
            )
        })

    return trades


# ============================================================
# STATISTICS
# ============================================================

def calculate_statistics(results):

    total = len(results)

    wins = (
        results["result"] == "WIN"
    ).sum()

    losses = (
        results["result"] == "LOSS"
    ).sum()

    win_rate = (
        wins / total * 100
        if total > 0
        else 0
    )

    average = (
        results["return_pct"].mean()
        if total > 0
        else 0
    )

    median = (
        results["return_pct"].median()
        if total > 0
        else 0
    )

    total_return = (
        results["return_pct"].sum()
    )

    best = (
        results["return_pct"].max()
        if total > 0
        else 0
    )

    worst = (
        results["return_pct"].min()
        if total > 0
        else 0
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

    profit_factor = (
        gross_profit / gross_loss
        if gross_loss > 0
        else float("inf")
    )

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
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 70)
    print("15:29 HIGHLIGHTED CANDLE BACKTEST")
    print("=" * 70)

    print("\n3-MINUTE:")
    print("Reference candle       : 15:24")
    print("Opposite candle        : 15:27")
    print("15:24 volume > 15:27  : YES")
    print("2-of-4 majority        : 15:15, 15:18, 15:21, 15:27")
    print("Morning 3-min          : 09:15 & 09:18 = 15:24")

    print("\n1-MINUTE:")
    print("Reference candle       : 15:29")
    print("15:29 = 15:24 trend    : YES")
    print("15:29 volume > 15:28  : YES")
    print("2-of-4 majority        : 15:25, 15:26, 15:27, 15:28")
    print("Morning 1-min          : 09:15 & 09:16 = 15:29")

    print("\nTRADE:")
    print("Entry                  : Next day 09:15 OPEN")
    print("Exit                   : Next day 15:27 CLOSE")

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

        all_trades.extend(trades)

    print("\n")

    # ========================================================
    # NO TRADES
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
    # OVERALL
    # ========================================================

    stats = calculate_statistics(
        results
    )

    print("\n")
    print("=" * 70)
    print("FINAL BACKTEST RESULTS")
    print("=" * 70)

    print(
        f"Total trades : {stats['trades']}"
    )

    print(
        f"Wins         : {stats['wins']}"
    )

    print(
        f"Losses       : {stats['losses']}"
    )

    print(
        f"Win rate     : {stats['win_rate']:.2f}%"
    )

    print(
        f"Average trade: {stats['average']:.4f}%"
    )

    print(
        f"Median trade : {stats['median']:.4f}%"
    )

    print(
        f"Profit factor: {stats['profit_factor']:.3f}"
    )

    print(
        f"Total raw %  : {stats['total_return']:.2f}%"
    )

    print(
        f"Best trade   : {stats['best']:.4f}%"
    )

    print(
        f"Worst trade  : {stats['worst']:.4f}%"
    )

    # ========================================================
    # LONG / SHORT
    # ========================================================

    print("\n")
    print("=" * 70)
    print("LONG / SHORT BREAKDOWN")
    print("=" * 70)

    for side in ["LONG", "SHORT"]:

        subset = results[
            results["direction"] == side
        ]

        if subset.empty:
            continue

        side_stats = calculate_statistics(
            subset
        )

        print(f"\n{side}:")

        print(
            f"  Trades       : "
            f"{side_stats['trades']}"
        )

        print(
            f"  Wins         : "
            f"{side_stats['wins']}"
        )

        print(
            f"  Losses       : "
            f"{side_stats['losses']}"
        )

        print(
            f"  Win rate     : "
            f"{side_stats['win_rate']:.2f}%"
        )

        print(
            f"  Avg return   : "
            f"{side_stats['average']:.4f}%"
        )

        print(
            f"  Profit factor: "
            f"{side_stats['profit_factor']:.3f}"
        )

    # ========================================================
    # YEARLY
    # ========================================================

    results["year"] = (
        results["trade_date"]
        .astype(str)
        .str[:4]
    )

    print("\n")
    print("=" * 70)
    print("YEARLY BREAKDOWN")
    print("=" * 70)

    for year in sorted(
        results["year"].unique()
    ):

        yearly = results[
            results["year"] == year
        ]

        year_stats = calculate_statistics(
            yearly
        )

        print(f"\n{year}:")

        print(
            f"  Trades     : "
            f"{year_stats['trades']}"
        )

        print(
            f"  Wins       : "
            f"{year_stats['wins']}"
        )

        print(
            f"  Losses     : "
            f"{year_stats['losses']}"
        )

        print(
            f"  Win rate   : "
            f"{year_stats['win_rate']:.2f}%"
        )

        print(
            f"  Avg return : "
            f"{year_stats['average']:.4f}%"
        )

        print(
            f"  Profit fac.: "
            f"{year_stats['profit_factor']:.3f}"
        )

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    output_file = (
        "highlighted_15_29_backtest.csv"
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
        f"Results saved to: {output_file}"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
