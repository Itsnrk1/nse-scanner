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
# BUILD 3-MINUTE CANDLE
# ============================================================

def three_minute_candle(
    day,
    start_time
):

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
    # ========================================================

    candle_1520 = five_minute_candle(
        day,
        "15:20"
    )

    # ========================================================
    # 5-MINUTE 15:25
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
    # 5-MINUTE TRENDS
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
    # 15:20 AND 15:25 SAME TREND
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
    # 15:25 VOLUME > 15:20 VOLUME
    # ========================================================

    if not (
        candle_1525["volume"]
        >
        candle_1520["volume"]
    ):
        return None

    # ========================================================
    # 3-MINUTE 15:24
    # ========================================================

    candle_1524 = three_minute_candle(
        day,
        "15:24"
    )

    # ========================================================
    # 3-MINUTE 15:27
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

    # ========================================================
    # 3-MINUTE 15:24 TREND
    # ========================================================

    dir_1524 = direction(
        candle_1524["open"],
        candle_1524["close"]
    )

    if dir_1524 == 0:
        return None

    # ========================================================
    # CONDITION 3
    #
    # 3-MINUTE 15:24 VOLUME
    # > 3-MINUTE 15:27 VOLUME
    # ========================================================

    if not (
        candle_1524["volume"]
        >
        candle_1527["volume"]
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
    # CONDITION 4
    #
    # 1-MINUTE 15:28 TREND
    # = 3-MINUTE 15:24 TREND
    # ========================================================

    if dir_1528 != dir_1524:
        return None

    # ========================================================
    # CONDITION 5
    #
    # 1-MINUTE 15:28 VOLUME
    # > 15:29 VOLUME
    # ========================================================

    volume_1528 = float(
        day["15:28"]["volume"]
    )

    volume_1529 = float(
        day["15:29"]["volume"]
    )

    if not (
        volume_1528
        >
        volume_1529
    ):
        return None

    # ========================================================
    # FINAL SIGNAL
    #
    # Direction = 3-minute 15:24
    #              and 1-minute 15:28
    # ========================================================

    if dir_1524 == 1:

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
        # Build signal-day dictionary
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
        # Build next-day dictionary
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
        # NEXT DAY 09:20 CLOSE
        # ====================================================

        if "09:20" not in next_day:
            continue

        exit_0920 = float(
            next_day["09:20"]["close"]
        )

        # ====================================================
        # EXIT B
        #
        # NEXT DAY 15:29 CLOSE
        # ====================================================

        if "15:29" not in next_day:
            continue

        exit_1529 = float(
            next_day["15:29"]["close"]
        )

        # ====================================================
        # RETURN — 09:20 EXIT
        # ====================================================

        if signal["direction"] == "LONG":

            return_0920 = (
                exit_0920 -
                entry_price
            ) / entry_price * 100

        else:

            return_0920 = (
                entry_price -
                exit_0920
            ) / entry_price * 100

        # ====================================================
        # RETURN — 15:29 EXIT
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
        # SAVE BOTH RESULTS
        # ====================================================

        trades.append({

            "symbol": symbol,

            "signal_date": signal_date,

            "trade_date": trade_date,

            "direction": signal["direction"],

            "entry_09:15_open": entry_price,

            "exit_09:20_close": exit_0920,

            "return_09:20_pct":
                return_0920,

            "result_09:20": (
                "WIN"
                if return_0920 > 0
                else "LOSS"
            ),

            "exit_15:29_close": exit_1529,

            "return_15:29_pct":
                return_1529,

            "result_15:29": (
                "WIN"
                if return_1529 > 0
                else "LOSS"
            )
        })

    return trades


# ============================================================
# STATISTICS
# ============================================================

def calculate_statistics(
    returns
):

    returns = pd.Series(
        returns
    ).dropna()

    total = len(
        returns
    )

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
        returns > 0
    ).sum()

    losses = (
        returns <= 0
    ).sum()

    win_rate = (
        wins /
        total *
        100
    )

    average = (
        returns.mean()
    )

    median = (
        returns.median()
    )

    total_return = (
        returns.sum()
    )

    best = (
        returns.max()
    )

    worst = (
        returns.min()
    )

    gross_profit = returns[
        returns > 0
    ].sum()

    gross_loss = abs(
        returns[
            returns <= 0
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
    returns
):

    stats = calculate_statistics(
        returns
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
# PRINT LONG / SHORT
# ============================================================

def print_direction_statistics(
    results,
    return_column,
    title
):

    print("\n")
    print("=" * 70)
    print(title)
    print("=" * 70)

    for side in [
        "LONG",
        "SHORT"
    ]:

        subset = results[
            results["direction"]
            == side
        ]

        if subset.empty:
            continue

        print_statistics(
            side,
            subset[return_column]
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 70)
    print("NEW STRATEGY — TWO EXIT BACKTEST")
    print("=" * 70)

    print("\n5-MINUTE CONDITIONS:")

    print(
        "15:20 and 15:25 = SAME TREND"
    )

    print(
        "15:25 volume > 15:20 volume"
    )

    print("\n3-MINUTE CONDITIONS:")

    print(
        "15:24 volume > 15:27 volume"
    )

    print(
        "15:24 = REFERENCE TREND"
    )

    print("\n1-MINUTE CONDITIONS:")

    print(
        "15:28 trend = 3-minute 15:24 trend"
    )

    print(
        "15:28 volume > 15:29 volume"
    )

    print("\nTRADE DIRECTION:")

    print(
        "Direction = 3-minute 15:24 "
        "and 1-minute 15:28 trend"
    )

    print("\nEXIT TESTS:")

    print(
        "TEST A: 09:15 OPEN -> 09:20 CLOSE"
    )

    print(
        "TEST B: 09:15 OPEN -> 15:29 CLOSE"
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
    # CHECK
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
    # TEST A — 09:20
    # ========================================================

    print_statistics(
        "TEST A — 09:15 ENTRY / 09:20 EXIT",
        results["return_09:20_pct"]
    )

    print_direction_statistics(
        results,
        "return_09:20_pct",
        "TEST A — LONG / SHORT"
    )

    # ========================================================
    # TEST B — 15:29
    # ========================================================

    print_statistics(
        "TEST B — 09:15 ENTRY / 15:29 EXIT",
        results["return_15:29_pct"]
    )

    print_direction_statistics(
        results,
        "return_15:29_pct",
        "TEST B — LONG / SHORT"
    )

    # ========================================================
    # DIRECT COMPARISON
    # ========================================================

    stats_0920 = calculate_statistics(
        results["return_09:20_pct"]
    )

    stats_1529 = calculate_statistics(
        results["return_15:29_pct"]
    )

    print("\n")
    print("=" * 70)
    print("EXIT COMPARISON")
    print("=" * 70)

    print(
        "\n"
        f"{'METRIC':<25}"
        f"{'09:20 EXIT':>18}"
        f"{'15:29 EXIT':>18}"
    )

    print("-" * 65)

    print(
        f"{'Trades':<25}"
        f"{stats_0920['trades']:>18}"
        f"{stats_1529['trades']:>18}"
    )

    print(
        f"{'Win rate':<25}"
        f"{stats_0920['win_rate']:>17.2f}%"
        f"{stats_1529['win_rate']:>17.2f}%"
    )

    print(
        f"{'Average trade':<25}"
        f"{stats_0920['average']:>17.4f}%"
        f"{stats_1529['average']:>17.4f}%"
    )

    print(
        f"{'Median trade':<25}"
        f"{stats_0920['median']:>17.4f}%"
        f"{stats_1529['median']:>17.4f}%"
    )

    print(
        f"{'Profit factor':<25}"
        f"{stats_0920['profit_factor']:>18.3f}"
        f"{stats_1529['profit_factor']:>18.3f}"
    )

    print(
        f"{'Total raw return':<25}"
        f"{stats_0920['total_return']:>17.2f}%"
        f"{stats_1529['total_return']:>17.2f}%"
    )

    # ========================================================
    # SAVE
    # ========================================================

    output_file = (
        "new_strategy_two_exit_backtest.csv"
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
