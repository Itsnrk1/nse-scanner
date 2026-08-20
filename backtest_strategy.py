import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
from datetime import datetime, timedelta


# ============================================================
# SETTINGS
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/nse-fno-1min-data/"
    "releases/latest/download/stocks_1m_csvs.zip"
)

OUTPUT_FILE = "backtest_results.csv"

# Exit at the CLOSE of the 15:27 candle
EXIT_TIME = "15:27"

# Required morning and afternoon timestamps
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
        timeout=300
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
        f"Downloaded "
        f"{len(data) / (1024 * 1024):.1f} MB"
    )

    return data


# ============================================================
# FIND CSV FILES
# ============================================================

def get_csv_files(zip_bytes):

    z = zipfile.ZipFile(
        io.BytesIO(zip_bytes)
    )

    files = [
        name
        for name in z.namelist()
        if name.endswith(".csv.gz")
    ]

    print(
        f"\nFound {len(files)} stock files."
    )

    return z, files


# ============================================================
# HELPERS
# ============================================================

def candle_direction(open_price, close_price):

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


def get_3min_candle(day, times):

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
# PREPARE ONE STOCK
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
        # Convert timestamp
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
        # Standardize columns
        # ----------------------------------------------------

        df = df.rename(
            columns={
                "Volume": "volume"
            }
        )

        needed = [
            "date",
            "hm",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]

        for column in needed:

            if column not in df.columns:
                return None

        df = df[needed].copy()

        df["open"] = pd.to_numeric(
            df["open"],
            errors="coerce"
        )

        df["close"] = pd.to_numeric(
            df["close"],
            errors="coerce"
        )

        df["volume"] = pd.to_numeric(
            df["volume"],
            errors="coerce"
        )

        df = df.dropna(
            subset=[
                "open",
                "close",
                "volume"
            ]
        )

        return df

    except Exception as e:

        print(
            f"\nError loading {filename}: {e}"
        )

        return None


# ============================================================
# FIND NEXT TRADING DAY
# ============================================================

def get_next_trading_day(
    all_dates,
    current_date
):

    later_dates = [
        d
        for d in all_dates
        if d > current_date
    ]

    if not later_dates:
        return None

    return later_dates[0]


# ============================================================
# TEST ONE SIGNAL DAY
# ============================================================

def evaluate_signal_day(
    stock,
    date_data
):

    # --------------------------------------------------------
    # Make dictionary:
    #
    # "15:24" -> candle row
    # --------------------------------------------------------

    day = {}

    for _, row in date_data.iterrows():

        day[row["hm"]] = row.to_dict()

    # --------------------------------------------------------
    # Make sure every required candle exists
    # --------------------------------------------------------

    for required in REQUIRED_TIMES:

        if required not in day:
            return None

    # --------------------------------------------------------
    # CONDITION 1
    #
    # 3-min 15:24
    # vs
    # 3-min 15:27
    #
    # Must be opposite.
    #
    # 15:24 volume > 15:27 volume
    # --------------------------------------------------------

    candle_1524 = get_3min_candle(
        day,
        ["15:24", "15:25", "15:26"]
    )

    candle_1527 = get_3min_candle(
        day,
        ["15:27", "15:28", "15:29"]
    )

    dir_1524 = candle_direction(
        candle_1524["open"],
        candle_1524["close"]
    )

    dir_1527 = candle_direction(
        candle_1527["open"],
        candle_1527["close"]
    )

    cond1 = (
        dir_1524 != 0
        and
        dir_1527 != 0
        and
        dir_1524 != dir_1527
        and
        candle_1524["volume"]
        >
        candle_1527["volume"]
    )

    # --------------------------------------------------------
    # CONDITION 2
    #
    # Same-day 3-min 09:15 and 09:18
    # must both match 15:24.
    # --------------------------------------------------------

    candle_0915 = get_3min_candle(
        day,
        ["09:15", "09:16", "09:17"]
    )

    candle_0918 = get_3min_candle(
        day,
        ["09:18", "09:19", "09:20"]
    )

    dir_0915_3m = candle_direction(
        candle_0915["open"],
        candle_0915["close"]
    )

    dir_0918_3m = candle_direction(
        candle_0918["open"],
        candle_0918["close"]
    )

    cond2 = (
        dir_0915_3m == dir_1524
        and
        dir_0918_3m == dir_1524
    )

    # --------------------------------------------------------
    # CONDITION 3
    #
    # 1-min 15:28 must match 3-min 15:24.
    #
    # 15:28 volume > 15:29 volume.
    # --------------------------------------------------------

    dir_1528 = candle_direction(
        day["15:28"]["open"],
        day["15:28"]["close"]
    )

    cond3 = (
        dir_1528 != 0
        and
        dir_1528 == dir_1524
        and
        float(day["15:28"]["volume"])
        >
        float(day["15:29"]["volume"])
    )

    # --------------------------------------------------------
    # CONDITION 4
    #
    # Same-day 1-min 09:15 and 09:16
    # must both match 15:28.
    # --------------------------------------------------------

    dir_0915_1m = candle_direction(
        day["09:15"]["open"],
        day["09:15"]["close"]
    )

    dir_0916_1m = candle_direction(
        day["09:16"]["open"],
        day["09:16"]["close"]
    )

    cond4 = (
        dir_0915_1m == dir_1528
        and
        dir_0916_1m == dir_1528
    )

    # --------------------------------------------------------
    # FINAL SIGNAL
    # --------------------------------------------------------

    passed = (
        cond1
        and
        cond2
        and
        cond3
        and
        cond4
    )

    if not passed:
        return None

    return {
        "signal_date": stock["date"],
        "direction": (
            "LONG"
            if dir_1524 == 1
            else "SHORT"
        )
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

    stock_symbol = os.path.basename(
        filename
    ).replace(
        "_1m.csv.gz",
        ""
    )

    print(
        f"\nProcessing {stock_symbol}..."
    )

    # --------------------------------------------------------
    # Create date groups
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
    # Every day can become a signal day.
    #
    # The actual trade happens on the
    # NEXT trading day at 09:15.
    # --------------------------------------------------------

    for i, signal_date in enumerate(
        dates
    ):

        # Need a following trading day
        if i + 1 >= len(dates):
            break

        next_date = dates[i + 1]

        signal_data = grouped[
            signal_date
        ]

        next_data = grouped[
            next_date
        ]

        # ----------------------------------------------------
        # Evaluate signal day
        # ----------------------------------------------------

        signal = evaluate_signal_day(
            stock={
                "date": signal_date
            },
            date_data=signal_data
        )

        if signal is None:
            continue

        # ----------------------------------------------------
        # Need next day's 09:15 and 15:27
        # ----------------------------------------------------

        next_day = {}

        for _, row in next_data.iterrows():

            next_day[
                row["hm"]
            ] = row.to_dict()

        if "09:15" not in next_day:
            continue

        if EXIT_TIME not in next_day:
            continue

        # ----------------------------------------------------
        # ENTRY
        #
        # Next trading day 09:15 OPEN
        # ----------------------------------------------------

        entry_price = float(
            next_day["09:15"]["open"]
        )

        # ----------------------------------------------------
        # EXIT
        #
        # Next trading day 15:27 CLOSE
        # ----------------------------------------------------

        exit_price = float(
            next_day[EXIT_TIME]["close"]
        )

        if entry_price <= 0:
            continue

        # ----------------------------------------------------
        # RETURN
        # ----------------------------------------------------

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

        win = return_pct > 0

        trades.append({
            "symbol": stock_symbol,
            "signal_date": signal_date,
            "trade_date": next_date,
            "direction": signal["direction"],
            "entry_09:15_open": entry_price,
            "exit_15:27_close": exit_price,
            "return_pct": return_pct,
            "result": (
                "WIN"
                if win
                else "LOSS"
            )
        })

    return trades


# ============================================================
# MAIN BACKTEST
# ============================================================

def main():

    print("\n")
    print("=" * 70)
    print("NSE STRATEGY BACKTEST")
    print("=" * 70)

    print("\nStrategy conditions:")

    print(
        "1. 3-min 15:24 and 15:27 opposite"
    )

    print(
        "2. 15:24 volume > 15:27 volume"
    )

    print(
        "3. Same-day 3-min 09:15 and 09:18 "
        "match 15:24"
    )

    print(
        "4. 1-min 15:28 matches 3-min 15:24"
    )

    print(
        "5. 15:28 volume > 15:29 volume"
    )

    print(
        "6. Same-day 1-min 09:15 and 09:16 "
        "match 15:28"
    )

    print(
        "7. Entry = NEXT trading day's 09:15 OPEN"
    )

    print(
        "8. Exit = NEXT trading day's 15:27 CLOSE"
    )

    # --------------------------------------------------------
    # Download
    # --------------------------------------------------------

    zip_bytes = download_dataset()

    z, files = get_csv_files(
        zip_bytes
    )

    # --------------------------------------------------------
    # Backtest
    # --------------------------------------------------------

    all_trades = []

    total_files = len(files)

    for number, filename in enumerate(
        files,
        1
    ):

        print(
            f"\n[{number}/{total_files}] "
            f"{filename}"
        )

        trades = backtest_stock(
            z,
            filename
        )

        all_trades.extend(
            trades
        )

    # --------------------------------------------------------
    # Results
    # --------------------------------------------------------

    if not all_trades:

        print("\n" + "=" * 70)
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
        results["return_pct"]
        .mean()
    )

    median_return = (
        results["return_pct"]
        .median()
    )

    total_return = (
        results["return_pct"]
        .sum()
    )

    best_trade = (
        results["return_pct"]
        .max()
    )

    worst_trade = (
        results["return_pct"]
        .min()
    )

    # --------------------------------------------------------
    # Print results
    # --------------------------------------------------------

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
        f"Total raw %  : {total_return:.2f}%"
    )

    print(
        f"Best trade   : {best_trade:.4f}%"
    )

    print(
        f"Worst trade  : {worst_trade:.4f}%"
    )

    # --------------------------------------------------------
    # Direction statistics
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("LONG / SHORT BREAKDOWN")
    print("=" * 70)

    for direction in [
        "LONG",
        "SHORT"
    ]:

        subset = results[
            results["direction"] ==
            direction
        ]

        if subset.empty:
            continue

        direction_wins = (
            subset["result"] ==
            "WIN"
        ).sum()

        direction_total = len(
            subset
        )

        direction_win_rate = (
            direction_wins /
            direction_total *
            100
        )

        print(
            f"\n{direction}:"
        )

        print(
            f"  Trades    : "
            f"{direction_total}"
        )

        print(
            f"  Wins      : "
            f"{direction_wins}"
        )

        print(
            f"  Win rate  : "
            f"{direction_win_rate:.2f}%"
        )

        print(
            f"  Avg return: "
            f"{subset['return_pct'].mean():.4f}%"
        )

    # --------------------------------------------------------
    # Save CSV
    # --------------------------------------------------------

    results.to_csv(
        OUTPUT_FILE,
        index=False
    )

    print("\n" + "=" * 70)

    print(
        f"Detailed trades saved to: "
        f"{OUTPUT_FILE}"
    )

    print("=" * 70)


if __name__ == "__main__":
    main()
