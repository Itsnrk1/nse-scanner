import os
import glob
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


# ============================================================
# CONFIGURATION
# ============================================================

DATA_DIRS = [
    "Nse_Historical_Data",
    "Nse_Historical_Data_2026",
]

# Trade direction:
# "BOTH"  = take long and short signals
# "LONG"  = long only
# "SHORT" = short only
TRADE_SIDE = "BOTH"

ENTRY_TIME = "09:15"

# No target / no stop-loss.
# Every trade is exited at 15:27 open.
EXIT_TIME = "15:27"

MARKET_OPEN = "09:15"
MARKET_CLOSE = "15:29"

MAX_WORKERS = max(1, (os.cpu_count() or 2) - 1)

RESULTS_DIR = "results"


# ============================================================
# CANDLE TREND
# ============================================================

def candle_trend(open_price, close_price):
    """
    Returns:
        1  = bullish / green
       -1  = bearish / red
        0  = doji
    """

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


# ============================================================
# LOAD PARQUET
# ============================================================

def load_parquet(path):

    df = pd.read_parquet(path)

    # Normalize column names
    df.columns = [
        str(c).strip().lower().replace(" ", "_")
        for c in df.columns
    ]

    # --------------------------------------------------------
    # Find datetime column
    # --------------------------------------------------------

    timestamp_candidates = [
        "datetime",
        "timestamp",
        "date",
        "time",
        "index",
    ]

    timestamp_col = None

    for col in timestamp_candidates:
        if col in df.columns:
            timestamp_col = col
            break

    if timestamp_col is None:

        if isinstance(df.index, pd.DatetimeIndex):

            df = df.reset_index()

            timestamp_col = df.columns[0]

        else:

            raise ValueError(
                f"No datetime column found in {path}. "
                f"Columns: {list(df.columns)}"
            )

    df["datetime"] = pd.to_datetime(
        df[timestamp_col],
        errors="coerce"
    )

    # --------------------------------------------------------
    # Find OHLCV columns
    # --------------------------------------------------------

    def find_column(candidates):

        for c in candidates:

            if c in df.columns:
                return c

        return None

    open_col = find_column([
        "open",
        "o"
    ])

    high_col = find_column([
        "high",
        "h"
    ])

    low_col = find_column([
        "low",
        "l"
    ])

    close_col = find_column([
        "close",
        "c"
    ])

    volume_col = find_column([
        "volume",
        "vol",
        "qty",
        "quantity"
    ])

    required = {
        "open": open_col,
        "high": high_col,
        "low": low_col,
        "close": close_col,
        "volume": volume_col,
    }

    missing = [
        name
        for name, col in required.items()
        if col is None
    ]

    if missing:

        raise ValueError(
            f"Missing columns {missing} in {path}. "
            f"Columns: {list(df.columns)}"
        )

    df = df.rename(columns={
        open_col: "open",
        high_col: "high",
        low_col: "low",
        close_col: "close",
        volume_col: "volume",
    })

    df = df[
        [
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ].copy()

    # Remove invalid data
    df = df.dropna(
        subset=[
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    )

    # Sort
    df = df.sort_values("datetime")

    # Remove duplicate timestamps
    df = df.drop_duplicates(
        subset="datetime",
        keep="first"
    )

    # --------------------------------------------------------
    # Date / Time columns
    # --------------------------------------------------------

    df["date"] = df["datetime"].dt.date

    df["time"] = df["datetime"].dt.strftime(
        "%H:%M"
    )

    # Only regular NSE session
    df = df[
        (df["time"] >= MARKET_OPEN)
        &
        (df["time"] <= MARKET_CLOSE)
    ]

    return df


# ============================================================
# PREPARE DAILY DATA
# ============================================================

def prepare_days(df):

    days = {}

    for day, day_df in df.groupby(
        "date",
        sort=True
    ):

        day_df = day_df.sort_values(
            "datetime"
        ).copy()

        if len(day_df) == 0:
            continue

        days[day] = day_df

    return days


# ============================================================
# GET 1-MINUTE 15:28 / 15:29 PATTERN
# ============================================================

def get_1m_pattern(day_df):
    """
    Required:

        15:28 and 15:29 opposite trends

        15:28 volume > 15:29 volume

    Returns trend of 15:28.

        1  = bullish
       -1  = bearish
        0  = invalid
    """

    candle_1528 = day_df[
        day_df["time"] == "15:28"
    ]

    candle_1529 = day_df[
        day_df["time"] == "15:29"
    ]

    if candle_1528.empty:
        return 0

    if candle_1529.empty:
        return 0

    c1528 = candle_1528.iloc[0]
    c1529 = candle_1529.iloc[0]

    trend_1528 = candle_trend(
        c1528["open"],
        c1528["close"]
    )

    trend_1529 = candle_trend(
        c1529["open"],
        c1529["close"]
    )

    # Dojis are invalid
    if trend_1528 == 0:
        return 0

    if trend_1529 == 0:
        return 0

    # Must be opposite
    if trend_1528 == trend_1529:
        return 0

    # 15:28 must have greater volume
    if c1528["volume"] <= c1529["volume"]:
        return 0

    return trend_1528


# ============================================================
# GET LAST 3-MINUTE CANDLE TREND
# ============================================================

def get_last_3m_trend(day_df):
    """
    Last 3-minute candle:

        15:27
        15:28
        15:29

    Trend is:

        15:27 OPEN
             ->
        15:29 CLOSE
    """

    required_times = {
        "15:27",
        "15:28",
        "15:29",
    }

    available_times = set(
        day_df["time"].values
    )

    if not required_times.issubset(
        available_times
    ):
        return 0

    candle_1527 = day_df[
        day_df["time"] == "15:27"
    ].iloc[0]

    candle_1529 = day_df[
        day_df["time"] == "15:29"
    ].iloc[0]

    trend = candle_trend(
        candle_1527["open"],
        candle_1529["close"]
    )

    return trend


# ============================================================
# SIMULATE TRADE
# ============================================================

def simulate_trade(
    trade_day_df,
    direction,
):
    """
    Entry:
        09:15 OPEN

    Exit:
        15:27 OPEN

    No target.
    No stop-loss.
    """

    entry_rows = trade_day_df[
        trade_day_df["time"] == ENTRY_TIME
    ]

    exit_rows = trade_day_df[
        trade_day_df["time"] == EXIT_TIME
    ]

    if entry_rows.empty:
        return None

    if exit_rows.empty:
        return None

    entry_candle = entry_rows.iloc[0]

    exit_candle = exit_rows.iloc[0]

    entry_price = float(
        entry_candle["open"]
    )

    exit_price = float(
        exit_candle["open"]
    )

    if entry_price <= 0:
        return None

    # --------------------------------------------------------
    # LONG
    # --------------------------------------------------------

    if direction == 1:

        gross_return = (
            (exit_price - entry_price)
            / entry_price
        ) * 100

        side = "LONG"

    # --------------------------------------------------------
    # SHORT
    # --------------------------------------------------------

    elif direction == -1:

        gross_return = (
            (entry_price - exit_price)
            / entry_price
        ) * 100

        side = "SHORT"

    else:
        return None

    win = gross_return > 0

    return {
        "entry_time": ENTRY_TIME,
        "exit_time": EXIT_TIME,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "direction": side,
        "return_pct": gross_return,
        "win": win,
    }


# ============================================================
# PROCESS ONE STOCK
# ============================================================

def process_stock(path):

    symbol = os.path.basename(path)

    if symbol.lower().endswith(".parquet"):
        symbol = symbol[:-8]

    try:

        df = load_parquet(path)

        if df.empty:
            return {
                "symbol": symbol,
                "trades": [],
                "error": "No data",
            }

        days = prepare_days(df)

        dates = sorted(days.keys())

        trades = []

        diagnostics = {
            "days_checked": 0,
            "incomplete_days": 0,
            "day_minus_2_pattern": 0,
            "day_minus_1_pattern": 0,
            "same_trend": 0,
            "3m_confirmation": 0,
            "final_signals": 0,
        }

        # ----------------------------------------------------
        # Need:
        #
        # i-2 = Day -2
        # i-1 = previous day
        # i   = trading day
        #
        # ----------------------------------------------------

        for i in range(2, len(dates)):

            day_minus_2_date = dates[i - 2]
            previous_date = dates[i - 1]
            trade_date = dates[i]

            day_minus_2 = days[
                day_minus_2_date
            ]

            previous_day = days[
                previous_date
            ]

            trade_day = days[
                trade_date
            ]

            diagnostics["days_checked"] += 1

            # ------------------------------------------------
            # Required candles
            # ------------------------------------------------

            required_minus_2 = {
                "15:28",
                "15:29",
            }

            required_previous = {
                "15:27",
                "15:28",
                "15:29",
            }

            times_minus_2 = set(
                day_minus_2["time"].values
            )

            times_previous = set(
                previous_day["time"].values
            )

            times_trade = set(
                trade_day["time"].values
            )

            if not required_minus_2.issubset(
                times_minus_2
            ):
                diagnostics[
                    "incomplete_days"
                ] += 1
                continue

            if not required_previous.issubset(
                times_previous
            ):
                diagnostics[
                    "incomplete_days"
                ] += 1
                continue

            if ENTRY_TIME not in times_trade:
                diagnostics[
                    "incomplete_days"
                ] += 1
                continue

            if EXIT_TIME not in times_trade:
                diagnostics[
                    "incomplete_days"
                ] += 1
                continue

            # ------------------------------------------------
            # DAY -2
            # ------------------------------------------------

            trend_minus_2 = get_1m_pattern(
                day_minus_2
            )

            if trend_minus_2 == 0:
                continue

            diagnostics[
                "day_minus_2_pattern"
            ] += 1

            # ------------------------------------------------
            # DAY -1
            # ------------------------------------------------

            trend_previous = get_1m_pattern(
                previous_day
            )

            if trend_previous == 0:
                continue

            diagnostics[
                "day_minus_1_pattern"
            ] += 1

            # ------------------------------------------------
            # Day -2 and Day -1 must have
            # SAME 15:28 trend
            # ------------------------------------------------

            if trend_previous != trend_minus_2:
                continue

            diagnostics[
                "same_trend"
            ] += 1

            # ------------------------------------------------
            # Previous day 3-minute last candle
            # must have SAME trend as its 1-minute 15:28
            # ------------------------------------------------

            trend_3m = get_last_3m_trend(
                previous_day
            )

            if trend_3m == 0:
                continue

            if trend_3m != trend_previous:
                continue

            diagnostics[
                "3m_confirmation"
            ] += 1

            # ------------------------------------------------
            # FINAL SIGNAL
            # ------------------------------------------------

            if TRADE_SIDE == "LONG" and trend_previous != 1:
                continue

            if TRADE_SIDE == "SHORT" and trend_previous != -1:
                continue

            diagnostics[
                "final_signals"
            ] += 1

            # ------------------------------------------------
            # NEXT DAY 09:15 TRADE
            # ------------------------------------------------

            trade = simulate_trade(
                trade_day,
                trend_previous
            )

            if trade is None:
                continue

            trade["symbol"] = symbol
            trade["signal_day_minus_2"] = str(
                day_minus_2_date
            )
            trade["signal_previous_day"] = str(
                previous_date
            )
            trade["trade_date"] = str(
                trade_date
            )

            trade[
                "day_minus_2_15_28_trend"
            ] = (
                "LONG"
                if trend_minus_2 == 1
                else "SHORT"
            )

            trade[
                "previous_15_28_trend"
            ] = (
                "LONG"
                if trend_previous == 1
                else "SHORT"
            )

            trade[
                "previous_3m_trend"
            ] = (
                "LONG"
                if trend_3m == 1
                else "SHORT"
            )

            trades.append(trade)

        return {
            "symbol": symbol,
            "trades": trades,
            "diagnostics": diagnostics,
            "error": None,
        }

    except Exception as e:

        return {
            "symbol": symbol,
            "trades": [],
            "diagnostics": {},
            "error": str(e),
        }


# ============================================================
# FIND ALL PARQUET FILES
# ============================================================

def find_parquet_files():

    files = []

    for directory in DATA_DIRS:

        if not os.path.exists(directory):
            continue

        found = glob.glob(
            os.path.join(
                directory,
                "**",
                "*.parquet"
            ),
            recursive=True
        )

        files.extend(found)

    # Remove duplicates
    files = sorted(set(files))

    return files


# ============================================================
# STATISTICS
# ============================================================

def calculate_statistics(
    trades_df
):

    if trades_df.empty:
        return {}

    returns = trades_df[
        "return_pct"
    ].astype(float)

    wins = returns > 0

    losses = returns < 0

    total_trades = len(
        trades_df
    )

    winning_trades = int(
        wins.sum()
    )

    losing_trades = int(
        losses.sum()
    )

    win_rate = (
        winning_trades
        / total_trades
        * 100
    )

    average_return = returns.mean()

    total_return = returns.sum()

    gross_profit = returns[
        returns > 0
    ].sum()

    gross_loss = abs(
        returns[
            returns < 0
        ].sum()
    )

    if gross_loss > 0:
        profit_factor = (
            gross_profit
            / gross_loss
        )
    else:
        profit_factor = np.inf

    return {
        "trades": total_trades,
        "wins": winning_trades,
        "losses": losing_trades,
        "win_rate": win_rate,
        "average_return": average_return,
        "total_return": total_return,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": profit_factor,
        "best_trade": returns.max(),
        "worst_trade": returns.min(),
        "median_return": returns.median(),
    }


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    os.makedirs(
        RESULTS_DIR,
        exist_ok=True
    )

    print("=" * 70)
    print("TWO-DAY EOD MOMENTUM BACKTEST")
    print("=" * 70)

    print()
    print("STRATEGY")
    print("-" * 70)

    print(
        "Day -2: 1m 15:28 & 15:29 opposite"
    )

    print(
        "Day -2: 15:28 volume > 15:29 volume"
    )

    print(
        "Day -1: 1m 15:28 & 15:29 opposite"
    )

    print(
        "Day -1: 15:28 volume > 15:29 volume"
    )

    print(
        "Day -1: 15:28 trend = Day -2 15:28 trend"
    )

    print(
        "Day -1: 3m 15:27-15:29 trend = "
        "Day -1 1m 15:28 trend"
    )

    print()
    print("TRADE")
    print("-" * 70)

    print("Entry : Next trading day 09:15 OPEN")
    print("Exit  : Same day 15:27 OPEN")
    print("Target: NONE")
    print("SL    : NONE")
    print()

    print(
        f"Trade side: {TRADE_SIDE}"
    )

    print(
        f"CPU workers: {MAX_WORKERS}"
    )

    # --------------------------------------------------------
    # Find data
    # --------------------------------------------------------

    files = find_parquet_files()

    print()
    print(
        f"Parquet files found: {len(files):,}"
    )

    if not files:

        print(
            "ERROR: No Parquet files found."
        )

        return

    # --------------------------------------------------------
    # Process stocks in parallel
    # --------------------------------------------------------

    all_trades = []

    total_diagnostics = {
        "days_checked": 0,
        "incomplete_days": 0,
        "day_minus_2_pattern": 0,
        "day_minus_1_pattern": 0,
        "same_trend": 0,
        "3m_confirmation": 0,
        "final_signals": 0,
    }

    completed = 0

    print()
    print("Starting parallel backtest...")
    print()

    with ProcessPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                process_stock,
                path
            ): path
            for path in files
        }

        for future in as_completed(
            futures
        ):

            result = future.result()

            completed += 1

            # Trades
            if result.get("trades"):

                all_trades.extend(
                    result["trades"]
                )

            # Diagnostics
            diagnostics = result.get(
                "diagnostics",
                {}
            )

            for key in total_diagnostics:

                total_diagnostics[key] += (
                    diagnostics.get(
                        key,
                        0
                    )
                )

            if completed % 25 == 0:

                print(
                    f"Processed "
                    f"{completed:,}/"
                    f"{len(files):,} stocks | "
                    f"Trades: "
                    f"{len(all_trades):,}"
                )

    # --------------------------------------------------------
    # Create dataframe
    # --------------------------------------------------------

    trades_df = pd.DataFrame(
        all_trades
    )

    # --------------------------------------------------------
    # No trades
    # --------------------------------------------------------

    if trades_df.empty:

        print()
        print(
            "No trades generated."
        )

        print()
        print(
            "Diagnostics:"
        )

        for key, value in total_diagnostics.items():

            print(
                f"{key}: {value:,}"
            )

        return

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    trades_df = trades_df.sort_values(
        [
            "trade_date",
            "symbol"
        ]
    ).reset_index(
        drop=True
    )

    # --------------------------------------------------------
    # Save all trades
    # --------------------------------------------------------

    trades_path = os.path.join(
        RESULTS_DIR,
        "all_trades.csv"
    )

    trades_df.to_csv(
        trades_path,
        index=False
    )

    # --------------------------------------------------------
    # Overall statistics
    # --------------------------------------------------------

    stats = calculate_statistics(
        trades_df
    )

    # --------------------------------------------------------
    # Print results
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("BACKTEST RESULTS")
    print("=" * 70)

    print(
        f"Total trades       : "
        f"{stats['trades']:,}"
    )

    print(
        f"Wins               : "
        f"{stats['wins']:,}"
    )

    print(
        f"Losses             : "
        f"{stats['losses']:,}"
    )

    print(
        f"Win rate           : "
        f"{stats['win_rate']:.2f}%"
    )

    print(
        f"Average return     : "
        f"{stats['average_return']:.4f}%"
    )

    print(
        f"Total return       : "
        f"{stats['total_return']:.4f}%"
    )

    print(
        f"Profit factor      : "
        f"{stats['profit_factor']:.4f}"
    )

    print(
        f"Best trade         : "
        f"{stats['best_trade']:.4f}%"
    )

    print(
        f"Worst trade        : "
        f"{stats['worst_trade']:.4f}%"
    )

    print(
        f"Median return      : "
        f"{stats['median_return']:.4f}%"
    )

    # --------------------------------------------------------
    # Long / Short statistics
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("LONG / SHORT BREAKDOWN")
    print("=" * 70)

    for direction in [
        "LONG",
        "SHORT"
    ]:

        subset = trades_df[
            trades_df["direction"]
            == direction
        ]

        if subset.empty:
            continue

        direction_stats = calculate_statistics(
            subset
        )

        print()
        print(direction)

        print(
            f"Trades       : "
            f"{direction_stats['trades']:,}"
        )

        print(
            f"Wins         : "
            f"{direction_stats['wins']:,}"
        )

        print(
            f"Losses       : "
            f"{direction_stats['losses']:,}"
        )

        print(
            f"Win rate     : "
            f"{direction_stats['win_rate']:.2f}%"
        )

        print(
            f"Avg return   : "
            f"{direction_stats['average_return']:.4f}%"
        )

        print(
            f"Total return : "
            f"{direction_stats['total_return']:.4f}%"
        )

        print(
            f"Profit factor: "
            f"{direction_stats['profit_factor']:.4f}"
        )

    # --------------------------------------------------------
    # Yearly results
    # --------------------------------------------------------

    trades_df["year"] = pd.to_datetime(
        trades_df["trade_date"]
    ).dt.year

    yearly_rows = []

    print()
    print("=" * 70)
    print("YEARLY RESULTS")
    print("=" * 70)

    for year, group in trades_df.groupby(
        "year"
    ):

        year_stats = calculate_statistics(
            group
        )

        yearly_rows.append({
            "year": year,
            **year_stats
        })

        print()
        print(year)

        print(
            f"Trades       : "
            f"{year_stats['trades']:,}"
        )

        print(
            f"Win rate     : "
            f"{year_stats['win_rate']:.2f}%"
        )

        print(
            f"Avg return   : "
            f"{year_stats['average_return']:.4f}%"
        )

        print(
            f"Total return : "
            f"{year_stats['total_return']:.4f}%"
        )

        print(
            f"Profit factor: "
            f"{year_stats['profit_factor']:.4f}"
        )

    yearly_df = pd.DataFrame(
        yearly_rows
    )

    yearly_df.to_csv(
        os.path.join(
            RESULTS_DIR,
            "yearly_results.csv"
        ),
        index=False
    )

    # --------------------------------------------------------
    # Signal diagnostics
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("SIGNAL DIAGNOSTICS")
    print("=" * 70)

    for key, value in total_diagnostics.items():

        print(
            f"{key:<25}: "
            f"{value:,}"
        )

    # --------------------------------------------------------
    # Top / Worst trades
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("BEST 20 TRADES")
    print("=" * 70)

    best = trades_df.nlargest(
        20,
        "return_pct"
    )

    print(
        best[
            [
                "symbol",
                "trade_date",
                "direction",
                "entry_price",
                "exit_price",
                "return_pct",
            ]
        ].to_string(index=False)
    )

    print()
    print("=" * 70)
    print("WORST 20 TRADES")
    print("=" * 70)

    worst = trades_df.nsmallest(
        20,
        "return_pct"
    )

    print(
        worst[
            [
                "symbol",
                "trade_date",
                "direction",
                "entry_price",
                "exit_price",
                "return_pct",
            ]
        ].to_string(index=False)
    )

    # --------------------------------------------------------
    # Monthly results
    # --------------------------------------------------------

    trades_df["month"] = pd.to_datetime(
        trades_df["trade_date"]
    ).dt.to_period("M").astype(str)

    monthly_rows = []

    for month, group in trades_df.groupby(
        "month"
    ):

        month_stats = calculate_statistics(
            group
        )

        monthly_rows.append({
            "month": month,
            **month_stats
        })

    monthly_df = pd.DataFrame(
        monthly_rows
    )

    monthly_df.to_csv(
        os.path.join(
            RESULTS_DIR,
            "monthly_results.csv"
        ),
        index=False
    )

    # --------------------------------------------------------
    # Daily results
    # --------------------------------------------------------

    daily_rows = []

    for date, group in trades_df.groupby(
        "trade_date"
    ):

        daily_stats = calculate_statistics(
            group
        )

        daily_rows.append({
            "trade_date": date,
            **daily_stats
        })

    daily_df = pd.DataFrame(
        daily_rows
    )

    daily_df.to_csv(
        os.path.join(
            RESULTS_DIR,
            "daily_results.csv"
        ),
        index=False
    )

    # --------------------------------------------------------
    # Save diagnostics
    # --------------------------------------------------------

    diagnostics_df = pd.DataFrame(
        [
            total_diagnostics
        ]
    )

    diagnostics_df.to_csv(
        os.path.join(
            RESULTS_DIR,
            "diagnostics.csv"
        ),
        index=False
    )

    # --------------------------------------------------------
    # Save summary
    # --------------------------------------------------------

    summary_path = os.path.join(
        RESULTS_DIR,
        "summary.txt"
    )

    with open(
        summary_path,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(
            "TWO-DAY EOD MOMENTUM BACKTEST\n"
        )

        f.write(
            "=" * 60 + "\n\n"
        )

        f.write(
            "STRATEGY\n"
        )

        f.write(
            "Day -2: 1m 15:28 & 15:29 opposite\n"
        )

        f.write(
            "Day -2: 15:28 volume > 15:29\n"
        )

        f.write(
            "Day -1: 1m 15:28 & 15:29 opposite\n"
        )

        f.write(
            "Day -1: 15:28 volume > 15:29\n"
        )

        f.write(
            "Day -1: same trend as Day -2\n"
        )

        f.write(
            "Day -1: 3m 15:27-15:29 same trend\n"
        )

        f.write(
            "\nTRADE\n"
        )

        f.write(
            "Entry: next trading day 09:15 open\n"
        )

        f.write(
            "Exit: 15:27 open\n"
        )

        f.write(
            "Target: NONE\n"
        )

        f.write(
            "Stop-loss: NONE\n\n"
        )

        for key, value in stats.items():

            f.write(
                f"{key}: {value}\n"
            )

        f.write(
            "\nDIAGNOSTICS\n"
        )

        for key, value in total_diagnostics.items():

            f.write(
                f"{key}: {value}\n"
            )

    # --------------------------------------------------------
    # Finish
    # --------------------------------------------------------

    elapsed = (
        time.time()
        - start_time
    )

    print()
    print("=" * 70)
    print("FILES SAVED")
    print("=" * 70)

    print(
        f"{trades_path}"
    )

    print(
        f"{RESULTS_DIR}/yearly_results.csv"
    )

    print(
        f"{RESULTS_DIR}/monthly_results.csv"
    )

    print(
        f"{RESULTS_DIR}/daily_results.csv"
    )

    print(
        f"{RESULTS_DIR}/diagnostics.csv"
    )

    print(
        f"{summary_path}"
    )

    print()
    print(
        f"Backtest completed in "
        f"{elapsed:.2f} seconds."
    )

    print("=" * 70)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
