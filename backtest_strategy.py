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
# CANDLE DIRECTION
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
# BUILD 5-MINUTE CANDLE
# ============================================================

def five_minute_candle(
    day,
    start_time
):

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
# BUILD 15-MINUTE CANDLE
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
# EVALUATE SIGNAL DAY
# ============================================================

def evaluate_signal_day(day):

    # ========================================================
    # 5-MINUTE 15:20
    # 15:20 - 15:24
    # ========================================================

    candle_1520 = five_minute_candle(
        day,
        "15:20"
    )

    # ========================================================
    # 5-MINUTE 15:25
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

    # ========================================================
    # CONDITION 1
    #
    # 5m 15:20 AND 15:25 SAME TREND
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
    # 5m 15:25 VOLUME > 15:20 VOLUME
    # ========================================================

    if not (
        candle_1525["volume"]
        >
        candle_1520["volume"]
    ):
        return None

    # ========================================================
    # 15-MINUTE 15:00
    #
    # 15:00 - 15:14
    # ========================================================

    candle_1500 = fifteen_minute_candle(
        day,
        "15:00"
    )

    # ========================================================
    # 15-MINUTE 15:15
    #
    # 15:15 - 15:29
    # ========================================================

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

    dir_1500 = direction(
        candle_1500["open"],
        candle_1500["close"]
    )

    dir_1515 = direction(
        candle_1515["open"],
        candle_1515["close"]
    )

    # ========================================================
    # CONDITION 3
    #
    # 15m 15:00 AND 15:15 SAME TREND
    # ========================================================

    if dir_1500 == 0:
        return None

    if dir_1515 == 0:
        return None

    if dir_1500 != dir_1515:
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
    # CONDITION 4
    #
    # 1m 15:28 VOLUME > 15:29 VOLUME
    # ========================================================

    if not (
        float(day["15:28"]["volume"])
        >
        float(day["15:29"]["volume"])
    ):
        return None

    # ========================================================
    # FINAL TRADE DIRECTION
    #
    # IMPORTANT:
    # Direction comes ONLY from 1m 15:28.
    #
    # There is NO requirement that:
    #
    # 5m trend = 15m trend
    # 5m trend = 1m trend
    # 15m trend = 1m trend
    # ========================================================

    if dir_1528 == 1:

        trade_direction = "LONG"

    else:

        trade_direction = "SHORT"

    return {
        "direction": trade_direction,

        "trend_5m": dir_1520,

        "trend_15m": dir_1500,

        "trend_1m": dir_1528
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
        # Signal day dictionary
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
        # Next trading day
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

        # ====================================================
        # EXIT
        #
        # NEXT DAY 15:29 CLOSE
        # ====================================================

        if "15:29" not in next_day:
            continue

        exit_price = float(
            next_day["15:29"]["close"]
        )

        if entry_price <= 0:
            continue

        # ====================================================
        # CALCULATE RETURN
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

            "5m_trend":
                signal["trend_5m"],

            "15m_trend":
                signal["trend_15m"],

            "1m_15:28_trend":
                signal["trend_1m"],

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

    return trades


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
    print("5M + 15M + 1M STRATEGY BACKTEST")
    print("=" * 70)

    print("\n5-MINUTE CONDITIONS:")

    print(
        "15:20 and 15:25 = SAME TREND"
    )

    print(
        "15:25 volume > 15:20 volume"
    )

    print("\n15-MINUTE CONDITIONS:")

    print(
        "15:00 and 15:15 = SAME TREND"
    )

    print(
        "NO 15-MINUTE VOLUME CONDITION"
    )

    print("\n1-MINUTE CONDITIONS:")

    print(
        "15:28 = TRADE DIRECTION"
    )

    print(
        "15:28 volume > 15:29 volume"
    )

    print("\nIMPORTANT:")

    print(
        "3-MINUTE CONDITIONS REMOVED"
    )

    print(
        "NO CROSS-TIMEFRAME TREND ALIGNMENT"
    )

    print(
        "1m 15:28 DOES NOT need to match "
        "5m or 15m trend"
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

    print_statistics(
        "FINAL BACKTEST RESULTS",
        results
    )

    # ========================================================
    # LONG / SHORT
    # ========================================================

    print_direction_breakdown(
        results
    )

    # ========================================================
    # TREND RELATIONSHIP ANALYSIS
    #
    # This does NOT affect trade selection.
    # It is only reported so we can see what happened.
    # ========================================================

    print("\n")
    print("=" * 70)
    print("TREND RELATIONSHIP")
    print("=" * 70)

    results["5m_vs_15m"] = (
        results["5m_trend"]
        ==
        results["15m_trend"]
    )

    results["15m_vs_1m"] = (
        results["15m_trend"]
        ==
        results["1m_15:28_trend"]
    )

    results["5m_vs_1m"] = (
        results["5m_trend"]
        ==
        results["1m_15:28_trend"]
    )

    print(
        "5m and 15m same: "
        f"{results['5m_vs_15m'].sum()} / "
        f"{len(results)}"
    )

    print(
        "15m and 1m same: "
        f"{results['15m_vs_1m'].sum()} / "
        f"{len(results)}"
    )

    print(
        "5m and 1m same: "
        f"{results['5m_vs_1m'].sum()} / "
        f"{len(results)}"
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

        print_statistics(
            str(year),
            yearly
        )

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    output_file = (
        "5m_15m_1m_strategy_backtest.csv"
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
