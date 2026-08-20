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
# EVALUATE SIGNAL
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
    # 1-MINUTE 15:28 AND 15:29
    # ========================================================

    if "15:28" not in day:
        return None

    if "15:29" not in day:
        return None

    dir_1528 = direction(
        day["15:28"]["open"],
        day["15:28"]["close"]
    )

    dir_1529 = direction(
        day["15:29"]["open"],
        day["15:29"]["close"]
    )

    if dir_1528 == 0:
        return None

    if dir_1529 == 0:
        return None

    # ========================================================
    # CONDITION 3
    #
    # 15:28 MUST MATCH 5-MIN 15:20
    # ========================================================

    if dir_1528 != dir_1520:
        return None

    # ========================================================
    # CONDITION 4
    #
    # 15:28 VOLUME > 15:29 VOLUME
    # ========================================================

    if not (
        float(day["15:28"]["volume"])
        >
        float(day["15:29"]["volume"])
    ):
        return None

    # ========================================================
    # DETERMINE DIRECTION
    # ========================================================

    if dir_1520 == 1:

        trade_direction = "LONG"

    else:

        trade_direction = "SHORT"

    # ========================================================
    # DETERMINE 15:28 / 15:29 RELATIONSHIP
    # ========================================================

    if dir_1528 == dir_1529:

        candle_relationship = "SAME"

    else:

        candle_relationship = "OPPOSITE"

    return {
        "direction": trade_direction,
        "candle_relationship":
            candle_relationship
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

            "relationship":
                signal["candle_relationship"],

            "entry": entry_price,

            "exit": exit_price,

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
        wins / total * 100
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
    print("-" * 70)
    print(title)
    print("-" * 70)

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
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 70)
    print("SHORT / LONG + 15:28 / 15:29 RELATIONSHIP TEST")
    print("=" * 70)

    print("\nCORE CONDITIONS:")
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

    print("\nSPLIT INTO FOUR TESTS:")

    print(
        "A. SHORT + 15:28/15:29 SAME"
    )

    print(
        "B. SHORT + 15:28/15:29 OPPOSITE"
    )

    print(
        "C. LONG + 15:28/15:29 SAME"
    )

    print(
        "D. LONG + 15:28/15:29 OPPOSITE"
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
    # PROCESS ALL STOCKS ONCE
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
    # OVERALL
    # ========================================================

    print_statistics(
        "ALL TRADES",
        results
    )

    # ========================================================
    # SHORT - SAME
    # ========================================================

    short_same = results[
        (
            results["direction"]
            == "SHORT"
        )
        &
        (
            results["relationship"]
            == "SAME"
        )
    ]

    print_statistics(
        "SHORT — 15:28 & 15:29 SAME TREND",
        short_same
    )

    # ========================================================
    # SHORT - OPPOSITE
    # ========================================================

    short_opposite = results[
        (
            results["direction"]
            == "SHORT"
        )
        &
        (
            results["relationship"]
            == "OPPOSITE"
        )
    ]

    print_statistics(
        "SHORT — 15:28 & 15:29 OPPOSITE TREND",
        short_opposite
    )

    # ========================================================
    # LONG - SAME
    # ========================================================

    long_same = results[
        (
            results["direction"]
            == "LONG"
        )
        &
        (
            results["relationship"]
            == "SAME"
        )
    ]

    print_statistics(
        "LONG — 15:28 & 15:29 SAME TREND",
        long_same
    )

    # ========================================================
    # LONG - OPPOSITE
    # ========================================================

    long_opposite = results[
        (
            results["direction"]
            == "LONG"
        )
        &
        (
            results["relationship"]
            == "OPPOSITE"
        )
    ]

    print_statistics(
        "LONG — 15:28 & 15:29 OPPOSITE TREND",
        long_opposite
    )

    # ========================================================
    # COMPARISON TABLE
    # ========================================================

    print("\n")
    print("=" * 70)
    print("FOUR-WAY COMPARISON")
    print("=" * 70)

    groups = [
        (
            "SHORT SAME",
            short_same
        ),
        (
            "SHORT OPPOSITE",
            short_opposite
        ),
        (
            "LONG SAME",
            long_same
        ),
        (
            "LONG OPPOSITE",
            long_opposite
        )
    ]

    print(
        "\n"
        f"{'SETUP':<25}"
        f"{'TRADES':>10}"
        f"{'WIN%':>10}"
        f"{'AVG%':>12}"
        f"{'PF':>10}"
    )

    print("-" * 70)

    for name, subset in groups:

        stats = calculate_statistics(
            subset
        )

        print(
            f"{name:<25}"
            f"{stats['trades']:>10}"
            f"{stats['win_rate']:>9.2f}%"
            f"{stats['average']:>11.4f}%"
            f"{stats['profit_factor']:>10.3f}"
        )

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    output_file = (
        "four_way_short_long_backtest.csv"
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
