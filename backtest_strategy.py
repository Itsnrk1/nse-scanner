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

REQUIRED_TIMES = [
    "09:15",
    "09:16",
    "09:17",
    "09:18",
    "09:19",
    "09:20",
    "15:24",
    "15:25",
    "15:26",
    "15:27",
    "15:28",
    "15:29",
]


# ============================================================
# DOWNLOAD DATASET
# ============================================================

def download_dataset():

    print("=" * 70)
    print("DOWNLOADING NSE 1-MINUTE DATASET")
    print("=" * 70)

    print("\nSource:")
    print(DATA_URL)

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

                percent = (
                    downloaded /
                    total *
                    100
                )

                print(
                    f"\rDownloaded: "
                    f"{percent:.1f}%",
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
# CANDLE DIRECTION
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
# LOAD ONE STOCK
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
# EVALUATE NEW STRATEGY
# ============================================================

def evaluate_signal_day(day):

    # --------------------------------------------------------
    # Make sure every required 1-minute candle exists
    # --------------------------------------------------------

    for required_time in REQUIRED_TIMES:

        if required_time not in day:
            return None

    # ========================================================
    # 3-MINUTE 15:24 CANDLE
    #
    # 15:24 + 15:25 + 15:26
    # ========================================================

    candle_1524 = three_minute(
        day,
        [
            "15:24",
            "15:25",
            "15:26"
        ]
    )

    # ========================================================
    # 3-MINUTE 15:27 CANDLE
    #
    # 15:27 + 15:28 + 15:29
    # ========================================================

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

    # --------------------------------------------------------
    # Direction of 15:24 and 15:27
    # --------------------------------------------------------

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
    # 15:24 and 15:27 must be opposite trends.
    #
    # NEW volume rule:
    #
    # 15:27 volume > 15:24 volume
    # ========================================================

    condition_1 = (
        dir_1524 != 0
        and
        dir_1527 != 0
        and
        dir_1524 != dir_1527
        and
        candle_1527["volume"]
        >
        candle_1524["volume"]
    )

    if not condition_1:
        return None

    # ========================================================
    # CONDITION 2
    #
    # Same-day 3-minute:
    #
    # 09:15 must match 15:27
    # 09:18 must match 15:27
    # ========================================================

    candle_0915_3m = three_minute(
        day,
        [
            "09:15",
            "09:16",
            "09:17"
        ]
    )

    candle_0918_3m = three_minute(
        day,
        [
            "09:18",
            "09:19",
            "09:20"
        ]
    )

    dir_0915_3m = direction(
        candle_0915_3m["open"],
        candle_0915_3m["close"]
    )

    dir_0918_3m = direction(
        candle_0918_3m["open"],
        candle_0918_3m["close"]
    )

    condition_2 = (
        dir_0915_3m == dir_1527
        and
        dir_0918_3m == dir_1527
    )

    if not condition_2:
        return None

    # ========================================================
    # CONDITION 3
    #
    # 1-minute 15:29 must match
    # 3-minute 15:27.
    #
    # NEW volume rule:
    #
    # 15:29 volume > 15:28 volume
    # ========================================================

    dir_1529 = direction(
        day["15:29"]["open"],
        day["15:29"]["close"]
    )

    condition_3 = (
        dir_1529 != 0
        and
        dir_1529 == dir_1527
        and
        float(day["15:29"]["volume"])
        >
        float(day["15:28"]["volume"])
    )

    if not condition_3:
        return None

    # ========================================================
    # CONDITION 4
    #
    # Same-day 1-minute:
    #
    # 09:15 must match 15:29
    # 09:16 must match 15:29
    # ========================================================

    dir_0915_1m = direction(
        day["09:15"]["open"],
        day["09:15"]["close"]
    )

    dir_0916_1m = direction(
        day["09:16"]["open"],
        day["09:16"]["close"]
    )

    condition_4 = (
        dir_0915_1m == dir_1529
        and
        dir_0916_1m == dir_1529
    )

    if not condition_4:
        return None

    # ========================================================
    # SIGNAL PASSED
    # ========================================================

    if dir_1527 == 1:

        trade_direction = "LONG"

    else:

        trade_direction = "SHORT"

    return {
        "direction": trade_direction
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

    # --------------------------------------------------------
    # Group data by trading date
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

    # --------------------------------------------------------
    # Every signal day needs a following
    # trading day.
    # --------------------------------------------------------

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
        # Evaluate new strategy
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

        # ----------------------------------------------------
        # Need next-day 09:15 and 15:27
        # ----------------------------------------------------

        if "09:15" not in next_day:
            continue

        if "15:27" not in next_day:
            continue

        # ====================================================
        # ENTRY
        #
        # NEXT TRADING DAY 09:15 OPEN
        # ====================================================

        entry_price = float(
            next_day["09:15"]["open"]
        )

        # ====================================================
        # EXIT
        #
        # NEXT TRADING DAY 15:27 CLOSE
        # ====================================================

        exit_price = float(
            next_day["15:27"]["close"]
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

            "entry_09:15_open": entry_price,

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
# MAIN
# ============================================================

def main():

    print("\n")
    print("=" * 70)
    print("UPDATED STRATEGY BACKTEST")
    print("=" * 70)

    print("\nNEW RULES:")
    print(
        "3-min 15:24 vs 15:27 = opposite"
    )
    print(
        "15:27 volume > 15:24 volume"
    )
    print(
        "3-min 09:15 & 09:18 = 15:27 trend"
    )
    print(
        "1-min 15:29 = 15:27 trend"
    )
    print(
        "15:29 volume > 15:28 volume"
    )
    print(
        "1-min 09:15 & 09:16 = 15:29 trend"
    )
    print(
        "Entry = next trading day 09:15 OPEN"
    )
    print(
        "Exit = next trading day 15:27 CLOSE"
    )

    # --------------------------------------------------------
    # Download
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Process all stocks
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Check results
    # --------------------------------------------------------

    if not all_trades:

        print("=" * 70)
        print("NO TRADES FOUND")
        print("=" * 70)

        return

    results = pd.DataFrame(
        all_trades
    )

    # --------------------------------------------------------
    # Statistics
    # --------------------------------------------------------

    total_trades = len(
        results
    )

    wins = (
        results["result"] == "WIN"
    ).sum()

    losses = (
        results["result"] == "LOSS"
    ).sum()

    win_rate = (
        wins /
        total_trades *
        100
    )

    average_return = (
        results["return_pct"].mean()
    )

    median_return = (
        results["return_pct"].median()
    )

    total_raw_return = (
        results["return_pct"].sum()
    )

    best_trade = (
        results["return_pct"].max()
    )

    worst_trade = (
        results["return_pct"].min()
    )

    # --------------------------------------------------------
    # Profit factor
    # --------------------------------------------------------

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

    # ========================================================
    # FINAL RESULTS
    # ========================================================

    print("\n")
    print("=" * 70)
    print("FINAL BACKTEST RESULTS")
    print("=" * 70)

    print(
        f"\nTotal trades : {total_trades}"
    )

    print(
        f"Wins         : {wins}"
    )

    print(
        f"Losses       : {losses}"
    )

    print(
        f"Win rate     : {win_rate:.2f}%"
    )

    print(
        f"Average trade: {average_return:.4f}%"
    )

    print(
        f"Median trade : {median_return:.4f}%"
    )

    print(
        f"Profit factor: {profit_factor:.3f}"
    )

    print(
        f"Total raw %  : {total_raw_return:.2f}%"
    )

    print(
        f"Best trade   : {best_trade:.4f}%"
    )

    print(
        f"Worst trade  : {worst_trade:.4f}%"
    )

    # ========================================================
    # LONG / SHORT
    # ========================================================

    print("\n")
    print("=" * 70)
    print("LONG / SHORT BREAKDOWN")
    print("=" * 70)

    for trade_direction in [
        "LONG",
        "SHORT"
    ]:

        subset = results[
            results["direction"]
            ==
            trade_direction
        ]

        if subset.empty:
            continue

        direction_trades = len(
            subset
        )

        direction_wins = (
            subset["result"] == "WIN"
        ).sum()

        direction_losses = (
            subset["result"] == "LOSS"
        ).sum()

        direction_win_rate = (
            direction_wins /
            direction_trades *
            100
        )

        direction_avg = (
            subset["return_pct"].mean()
        )

        direction_profit = (
            subset.loc[
                subset["return_pct"] > 0,
                "return_pct"
            ].sum()
        )

        direction_loss = abs(
            subset.loc[
                subset["return_pct"] < 0,
                "return_pct"
            ].sum()
        )

        if direction_loss > 0:

            direction_pf = (
                direction_profit /
                direction_loss
            )

        else:

            direction_pf = float("inf")

        print(
            f"\n{trade_direction}:"
        )

        print(
            f"  Trades       : "
            f"{direction_trades}"
        )

        print(
            f"  Wins         : "
            f"{direction_wins}"
        )

        print(
            f"  Losses       : "
            f"{direction_losses}"
        )

        print(
            f"  Win rate     : "
            f"{direction_win_rate:.2f}%"
        )

        print(
            f"  Avg return   : "
            f"{direction_avg:.4f}%"
        )

        print(
            f"  Profit factor: "
            f"{direction_pf:.3f}"
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

    for year in sorted(
        results["year"].unique()
    ):

        yearly = results[
            results["year"] == year
        ]

        yearly_trades = len(
            yearly
        )

        yearly_wins = (
            yearly["result"] == "WIN"
        ).sum()

        yearly_win_rate = (
            yearly_wins /
            yearly_trades *
            100
        )

        yearly_average = (
            yearly["return_pct"].mean()
        )

        print(
            f"\n{year}:"
        )

        print(
            f"  Trades     : "
            f"{yearly_trades}"
        )

        print(
            f"  Win rate   : "
            f"{yearly_win_rate:.2f}%"
        )

        print(
            f"  Avg return : "
            f"{yearly_average:.4f}%"
        )

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    output_file = (
        "updated_strategy_backtest.csv"
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
        f"\nDetailed trades saved to:"
        f"\n{output_file}"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
