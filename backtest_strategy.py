# ============================================================
# 5-MIN EOD + PREVIOUS-DAY OPENING TREND STRATEGY
# ============================================================
#
# SIGNAL DAY = previous trading day
#
# PREVIOUS DAY CONDITIONS:
#
# 5-MIN OPENING:
#   09:15 candle
#   09:20 candle
#
# 5-MIN EOD:
#   15:20 candle
#   15:25 candle
#
# ALL FOUR MUST HAVE THE SAME TREND.
#
# LONG:
#   09:15 bullish
#   09:20 bullish
#   15:20 bullish
#   15:25 bullish
#
# SHORT:
#   09:15 bearish
#   09:20 bearish
#   15:20 bearish
#   15:25 bearish
#
# ENTRY:
#   NEXT TRADING DAY 09:15 OPEN
#
# EXIT:
#   SAME TRADING DAY 15:27
#
# ============================================================

import pandas as pd
import numpy as np
import os
import glob


# ============================================================
# CONFIGURATION
# ============================================================

DATA_FOLDER = "./data"

ENTRY_TIME = "09:15"
EXIT_TIME = "15:27"

# Set to 0 for raw results.
# Example: 0.10 means 0.10% total round-trip cost.
ROUND_TRIP_COST = 0.10


# ============================================================
# TREND FUNCTION
# ============================================================

def candle_trend(row):
    """
    Returns:
        1  = bullish
        -1 = bearish
         0 = doji
    """

    if row["close"] > row["open"]:
        return 1

    if row["close"] < row["open"]:
        return -1

    return 0


# ============================================================
# LOAD DATA
# ============================================================

def load_data(file_path):

    df = pd.read_csv(file_path)

    # --------------------------------------------------------
    # Try to identify datetime column
    # --------------------------------------------------------

    datetime_col = None

    for col in [
        "datetime",
        "Datetime",
        "date_time",
        "timestamp",
        "time",
        "Date"
    ]:

        if col in df.columns:

            datetime_col = col
            break

    if datetime_col is None:

        raise ValueError(
            f"No datetime column found in {file_path}"
        )

    df["datetime"] = pd.to_datetime(
        df[datetime_col]
    )

    # --------------------------------------------------------
    # Standardize OHLCV
    # --------------------------------------------------------

    rename_map = {}

    for col in df.columns:

        lower = col.lower()

        if lower == "open":
            rename_map[col] = "open"

        elif lower == "high":
            rename_map[col] = "high"

        elif lower == "low":
            rename_map[col] = "low"

        elif lower == "close":
            rename_map[col] = "close"

        elif lower in [
            "volume",
            "vol"
        ]:
            rename_map[col] = "volume"

    df = df.rename(
        columns=rename_map
    )

    required = [
        "open",
        "high",
        "low",
        "close"
    ]

    for col in required:

        if col not in df.columns:

            raise ValueError(
                f"Missing column: {col}"
            )

    if "volume" not in df.columns:

        df["volume"] = 0

    # --------------------------------------------------------
    # Clean
    # --------------------------------------------------------

    df = df.dropna(
        subset=[
            "datetime",
            "open",
            "high",
            "low",
            "close"
        ]
    )

    df = df.sort_values(
        "datetime"
    )

    df["date"] = (
        df["datetime"]
        .dt.date
    )

    df["time"] = (
        df["datetime"]
        .dt.strftime("%H:%M")
    )

    return df


# ============================================================
# BUILD 5-MINUTE CANDLES
# ============================================================

def build_5min_candles(df):

    df = df.copy()

    df["datetime"] = pd.to_datetime(
        df["datetime"]
    )

    # --------------------------------------------------------
    # Only NSE regular session
    # --------------------------------------------------------

    df = df[
        (
            df["time"] >= "09:15"
        )
        &
        (
            df["time"] <= "15:29"
        )
    ].copy()

    # --------------------------------------------------------
    # Create 5-minute bucket
    #
    # 09:15-09:19
    # 09:20-09:24
    # ...
    # 15:20-15:24
    # 15:25-15:29
    # --------------------------------------------------------

    minutes_from_open = (
        df["datetime"].dt.hour * 60
        +
        df["datetime"].dt.minute
        -
        (9 * 60 + 15)
    )

    df["bucket"] = (
        minutes_from_open // 5
    )

    df["candle_time"] = (
        9 * 60
        + 15
        +
        df["bucket"] * 5
    )

    df["candle_h"] = (
        df["candle_time"] // 60
    )

    df["candle_m"] = (
        df["candle_time"] % 60
    )

    df["candle_time"] = (
        df["candle_h"].astype(str)
        .str.zfill(2)
        +
        ":"
        +
        df["candle_m"].astype(str)
        .str.zfill(2)
    )

    # --------------------------------------------------------
    # Aggregate
    # --------------------------------------------------------

    candles = (
        df
        .groupby(
            [
                "date",
                "candle_time"
            ],
            sort=True
        )
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            bars=("close", "count")
        )
        .reset_index()
    )

    return candles


# ============================================================
# CHECK PREVIOUS DAY SIGNAL
# ============================================================

def get_signal(
    previous_day
):

    required_times = [
        "09:15",
        "09:20",
        "15:20",
        "15:25"
    ]

    day = (
        previous_day
        .set_index("candle_time")
    )

    # --------------------------------------------------------
    # Make sure all four candles exist.
    # --------------------------------------------------------

    for t in required_times:

        if t not in day.index:

            return 0

    # --------------------------------------------------------
    # Extract trends
    # --------------------------------------------------------

    trends = []

    for t in required_times:

        row = day.loc[t]

        trends.append(
            candle_trend(row)
        )

    # --------------------------------------------------------
    # Do NOT allow dojis.
    # --------------------------------------------------------

    if 0 in trends:

        return 0

    # --------------------------------------------------------
    # All four must be identical.
    # --------------------------------------------------------

    if (
        trends[0]
        ==
        trends[1]
        ==
        trends[2]
        ==
        trends[3]
    ):

        return trends[0]

    return 0


# ============================================================
# BACKTEST ONE STOCK
# ============================================================

def backtest_stock(
    file_path
):

    df = load_data(
        file_path
    )

    candles = build_5min_candles(
        df
    )

    if candles.empty:

        return []

    dates = sorted(
        candles["date"]
        .unique()
    )

    results = []

    # --------------------------------------------------------
    # Day D-1 = signal day
    # Day D   = trading day
    # --------------------------------------------------------

    for i in range(
        1,
        len(dates)
    ):

        previous_date = dates[
            i - 1
        ]

        trade_date = dates[
            i
        ]

        previous_day = candles[
            candles["date"]
            ==
            previous_date
        ].copy()

        current_day = candles[
            candles["date"]
            ==
            trade_date
        ].copy()

        # ----------------------------------------------------
        # SIGNAL
        # ----------------------------------------------------

        signal = get_signal(
            previous_day
        )

        if signal == 0:

            continue

        # ----------------------------------------------------
        # 09:15 ENTRY
        # ----------------------------------------------------

        entry_rows = current_day[
            current_day[
                "candle_time"
            ]
            ==
            ENTRY_TIME
        ]

        if entry_rows.empty:

            continue

        entry_price = float(
            entry_rows.iloc[0]["open"]
        )

        # ----------------------------------------------------
        # 15:27 EXIT
        #
        # 15:27 is inside the 15:25-15:29 candle.
        #
        # Therefore we use the ACTUAL 15:27 1-minute
        # open from the original data rather than the
        # 5-minute candle open.
        # ----------------------------------------------------

        exit_rows = df[
            (
                df["date"]
                ==
                trade_date
            )
            &
            (
                df["time"]
                ==
                EXIT_TIME
            )
        ]

        if exit_rows.empty:

            continue

        exit_price = float(
            exit_rows.iloc[0]["open"]
        )

        if entry_price <= 0:

            continue

        # ----------------------------------------------------
        # RETURN
        # ----------------------------------------------------

        if signal == 1:

            direction = "LONG"

            gross_return = (
                (
                    exit_price -
                    entry_price
                )
                /
                entry_price
                *
                100
            )

        else:

            direction = "SHORT"

            gross_return = (
                (
                    entry_price -
                    exit_price
                )
                /
                entry_price
                *
                100
            )

        net_return = (
            gross_return -
            ROUND_TRIP_COST
        )

        results.append({

            "symbol":
                os.path.basename(
                    file_path
                ),

            "signal_date":
                previous_date,

            "trade_date":
                trade_date,

            "direction":
                direction,

            "entry":
                entry_price,

            "exit":
                exit_price,

            "gross_return":
                gross_return,

            "net_return":
                net_return
        })

    return results


# ============================================================
# STATISTICS
# ============================================================

def calculate_statistics(
    trades
):

    if trades.empty:

        return {}

    returns = (
        trades["net_return"]
        .dropna()
    )

    wins = (
        returns > 0
    )

    losses = (
        returns < 0
    )

    win_rate = (
        wins.sum()
        /
        len(returns)
        *
        100
    )

    average_return = (
        returns.mean()
    )

    total_return = (
        returns.sum()
    )

    gross_profit = (
        returns[
            returns > 0
        ].sum()
    )

    gross_loss = abs(
        returns[
            returns < 0
        ].sum()
    )

    if gross_loss > 0:

        profit_factor = (
            gross_profit /
            gross_loss
        )

    else:

        profit_factor = np.inf

    return {

        "trades":
            len(returns),

        "wins":
            int(wins.sum()),

        "losses":
            int(losses.sum()),

        "win_rate":
            win_rate,

        "average_return":
            average_return,

        "total_return":
            total_return,

        "profit_factor":
            profit_factor,

        "best_trade":
            returns.max(),

        "worst_trade":
            returns.min()
    }


# ============================================================
# MAIN BACKTEST
# ============================================================

def main():

    print("=" * 100)
    print(
        "5-MIN EOD + PREVIOUS-DAY OPENING TREND STRATEGY"
    )
    print("=" * 100)

    print()
    print(
        "PREVIOUS DAY:"
    )

    print(
        "09:15 + 09:20 = SAME TREND"
    )

    print(
        "15:20 + 15:25 = SAME TREND"
    )

    print(
        "OPENING TREND = EOD TREND"
    )

    print()
    print(
        "NEXT DAY:"
    )

    print(
        "ENTRY = 09:15 OPEN"
    )

    print(
        "EXIT  = 15:27 OPEN"
    )

    print()
    print(
        "Round-trip cost:",
        ROUND_TRIP_COST,
        "%"
    )

    # --------------------------------------------------------
    # FIND FILES
    # --------------------------------------------------------

    files = []

    for pattern in [
        "*.csv",
        "*.CSV"
    ]:

        files.extend(
            glob.glob(
                os.path.join(
                    DATA_FOLDER,
                    pattern
                )
            )
        )

    if not files:

        raise FileNotFoundError(
            f"No CSV files found in {DATA_FOLDER}"
        )

    print()
    print(
        "Stocks found:",
        len(files)
    )

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------

    all_results = []

    for i, file_path in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing "
            f"{i}/{len(files)}",
            end=""
        )

        try:

            result = backtest_stock(
                file_path
            )

            if result:

                all_results.extend(
                    result
                )

        except Exception as e:

            print(
                f"\nError in "
                f"{file_path}: "
                f"{e}"
            )

    print()

    # --------------------------------------------------------
    # RESULTS
    # --------------------------------------------------------

    if not all_results:

        print()
        print(
            "NO TRADES FOUND."
        )

        return

    trades = pd.DataFrame(
        all_results
    )

    trades = trades.sort_values(
        [
            "trade_date",
            "symbol"
        ]
    ).reset_index(
        drop=True
    )

    # --------------------------------------------------------
    # OVERALL
    # --------------------------------------------------------

    overall = calculate_statistics(
        trades
    )

    print()
    print("=" * 100)
    print(
        "OVERALL RESULTS"
    )
    print("=" * 100)

    print(
        f"Trades          : "
        f"{overall['trades']:,}"
    )

    print(
        f"Wins            : "
        f"{overall['wins']:,}"
    )

    print(
        f"Losses          : "
        f"{overall['losses']:,}"
    )

    print(
        f"Win rate        : "
        f"{overall['win_rate']:.2f}%"
    )

    print(
        f"Average return  : "
        f"{overall['average_return']:.4f}%"
    )

    print(
        f"Total return    : "
        f"{overall['total_return']:.4f}%"
    )

    print(
        f"Profit factor   : "
        f"{overall['profit_factor']:.3f}"
    )

    print(
        f"Best trade      : "
        f"{overall['best_trade']:.4f}%"
    )

    print(
        f"Worst trade     : "
        f"{overall['worst_trade']:.4f}%"
    )

    # --------------------------------------------------------
    # LONG
    # --------------------------------------------------------

    long_trades = trades[
        trades["direction"]
        ==
        "LONG"
    ].copy()

    short_trades = trades[
        trades["direction"]
        ==
        "SHORT"
    ].copy()

    if not long_trades.empty:

        s = calculate_statistics(
            long_trades
        )

        print()
        print("=" * 100)
        print("LONG RESULTS")
        print("=" * 100)

        print(
            f"Trades         : "
            f"{s['trades']:,}"
        )

        print(
            f"Win rate       : "
            f"{s['win_rate']:.2f}%"
        )

        print(
            f"Average        : "
            f"{s['average_return']:.4f}%"
        )

        print(
            f"Profit factor  : "
            f"{s['profit_factor']:.3f}"
        )

    # --------------------------------------------------------
    # SHORT
    # --------------------------------------------------------

    if not short_trades.empty:

        s = calculate_statistics(
            short_trades
        )

        print()
        print("=" * 100)
        print("SHORT RESULTS")
        print("=" * 100)

        print(
            f"Trades         : "
            f"{s['trades']:,}"
        )

        print(
            f"Win rate       : "
            f"{s['win_rate']:.2f}%"
        )

        print(
            f"Average        : "
            f"{s['average_return']:.4f}%"
        )

        print(
            f"Profit factor  : "
            f"{s['profit_factor']:.3f}"
        )

    # --------------------------------------------------------
    # YEARLY
    # --------------------------------------------------------

    trades["year"] = (
        pd.to_datetime(
            trades["trade_date"]
        )
        .dt.year
    )

    yearly = []

    for year, group in (
        trades.groupby("year")
    ):

        s = calculate_statistics(
            group
        )

        yearly.append({

            "year":
                year,

            "trades":
                s["trades"],

            "win_rate":
                s["win_rate"],

            "average_return":
                s["average_return"],

            "profit_factor":
                s["profit_factor"],

            "total_return":
                s["total_return"]
        })

    yearly_df = pd.DataFrame(
        yearly
    )

    print()
    print("=" * 100)
    print("YEARLY RESULTS")
    print("=" * 100)

    print(
        yearly_df.to_string(
            index=False
        )
    )

    # --------------------------------------------------------
    # SAVE
    # --------------------------------------------------------

    trades.to_csv(
        "EOD_OPENING_5MIN_TRADES.csv",
        index=False
    )

    yearly_df.to_csv(
        "EOD_OPENING_5MIN_YEARLY.csv",
        index=False
    )

    print()
    print("=" * 100)
    print("BACKTEST COMPLETE")
    print("=" * 100)

    print()
    print(
        "Saved:"
    )

    print(
        "EOD_OPENING_5MIN_TRADES.csv"
    )

    print(
        "EOD_OPENING_5MIN_YEARLY.csv"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
