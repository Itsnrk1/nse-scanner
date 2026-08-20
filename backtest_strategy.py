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
# DOWNLOAD DATASET
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
        # Convert timestamp to Indian Standard Time
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
        # Price columns
        # ----------------------------------------------------

        for column in [
            "open",
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
# BUILD 5-MINUTE CANDLE
# ============================================================

def five_minute_candle(
    day,
    start_minute
):

    hour, minute = map(
        int,
        start_minute.split(":")
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

        "close": float(
            rows[-1]["close"]
        ),

        "volume": sum(
            float(row["volume"])
            for row in rows
        )
    }


# ============================================================
# EVALUATE STRATEGY
# ============================================================

def evaluate_signal_day(day):

    # ========================================================
    # 5-MINUTE 15:20 CANDLE
    #
    # 15:20 - 15:24
    # ========================================================

    candle_1520 = five_minute_candle(
        day,
        "15:20"
    )

    # ========================================================
    # 5-MINUTE 15:25 CANDLE
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

    # ========================================================
    # TRENDS
    # ========================================================

    dir_1520 = direction(
        candle_1520["open"],
        candle_1520["close"]
    )

    dir_1525 = direction(
        candle_1525["open"],
        candle_1525["close"]
    )

    # ========================================================
    # CONDITION 1
    #
    # 5-MINUTE 15:20 AND 15:25
    # MUST BE SAME TREND
    # ========================================================

    if dir_1520 == 0:
        return None

    if dir_1525 == 0:
        return None

    if dir_1520 != dir_1525:
        return None

    # ========================================================
    # CONDITION 2
    #
    # 5-MINUTE 15:25 VOLUME
    # MUST BE GREATER THAN 15:20
    # ========================================================

    if not (
        candle_1525["volume"]
        >
        candle_1520["volume"]
    ):
        return None

    # ========================================================
    # 1-MINUTE 15:28
    # ========================================================

    if "15:28" not in day:
        return None

    if "15:29" not in day:
        return None

    dir_1528 = direction(
        day["15:28"]["open"],
        day["15:28"]["close"]
    )

    if dir_1528 == 0:
        return None

    # ========================================================
    # CONDITION 3
    #
    # 1-MINUTE 15:28 TREND
    # MUST MATCH 5-MINUTE 15:20 TREND
    # ========================================================

    if dir_1528 != dir_1520:
        return None

    # ========================================================
    # CONDITION 4
    #
    # 1-MINUTE 15:28 VOLUME
    # MUST BE GREATER THAN 15:29 VOLUME
    #
    # IMPORTANT:
    # NO CONDITION is imposed on the TREND
    # of 15:29.
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
    # SIGNAL PASSED
    # ========================================================

    if dir_1520 == 1:

        trade_direction = "LONG"

    else:

        trade_direction = "SHORT"

    return {
        "direction": trade_direction
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
    # Group by trading date
    # --------------------------------------------------------

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
        # Signal-day dictionary
        # ----------------------------------------------------

        day = {}

        for _, row in signal_data.iterrows():

            day[row["hm"]] = row.to_dict()

        # ----------------------------------------------------
        # Check strategy
        # ----------------------------------------------------

        signal = evaluate_signal_day(
            day
        )

        if signal is None:
            continue

        # ----------------------------------------------------
        # Next-day dictionary
        # ----------------------------------------------------

        next_day = {}

        for _, row in trade_data.iterrows():

            next_day[row["hm"]] = row.to_dict()

        # ====================================================
        # NEXT DAY ENTRY
        #
        # 09:15 OPEN
        # ====================================================

        if "09:15" not in next_day:
            continue

        entry_price = float(
            next_day["09:15"]["open"]
        )

        # ====================================================
        # NEXT DAY EXIT
        #
        # MARKET CLOSE = 15:29 CLOSE
        # ====================================================

        if "15:29" not in next_day:
            continue

        exit_price = float(
            next_day["15:29"]["close"]
        )

        if entry_price <= 0:
            continue

        # ====================================================
        # RETURN
        # ====================================================

        if signal["direction"] == "LONG":

            return_pct = (
                exit_price -
                entry_price
            ) / entry_price * 100

        else:

            return_pct = (
                entry_price -
                exit_price
            ) / entry_price * 100

        # ====================================================
        # SAVE TRADE
        # ====================================================

        trades.append({

            "symbol": symbol,

            "signal_date": signal_date,

            "trade_date": trade_date,

            "direction": signal["direction"],

            "entry_09:15_open": entry_price,

            "exit_15:29_close": exit_price,

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

def calculate_statistics(
    results
):

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
    print("5-MINUTE + 1-MINUTE VOLUME CONFIRMATION BACKTEST")
    print("=" * 70)

    print("\nCONDITIONS:")

    print(
        "1. 5m 15:20 and 15:25 = SAME TREND"
    )

    print(
        "2. 5m 15:25 volume > 15:20 volume"
    )

    print(
        "3. 1m 15:28 = 5m 15:20 TREND"
    )

    print(
        "4. 1m 15:28 volume > 15:29 volume"
    )

    print(
        "\nIMPORTANT:"
    )

    print(
        "15:28 and 15:29 DO NOT need to "
        "have the same trend."
    )

    print("\nTRADE:")

    print(
        "Entry = NEXT TRADING DAY 09:15 OPEN"
    )

    print(
        "Exit  = NEXT TRADING DAY 15:29 CLOSE"
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
    # PROCESS ALL STOCKS
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
    # OVERALL RESULTS
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
    # LONG / SHORT BREAKDOWN
    # ========================================================

    print("\n")
    print("=" * 70)
    print("LONG / SHORT BREAKDOWN")
    print("=" * 70)

    for side in [
        "LONG",
        "SHORT"
    ]:

        subset = results[
            results["direction"] == side
        ]

        if subset.empty:
            continue

        side_stats = calculate_statistics(
            subset
        )

        print(
            f"\n{side}:"
        )

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
    # YEARLY BREAKDOWN
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

        print(
            f"\n{year}:"
        )

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
        "5m_1m_volume_confirmation_backtest.csv"
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
