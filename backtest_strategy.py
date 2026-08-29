# ============================================================
# EXACT STRATEGY BACKTEST
# ============================================================
#
# SIGNAL DAY = PREVIOUS TRADING DAY
#
# ============================================================
#
# 3-MINUTE CONDITIONS
# ============================================================
#
# 15:24 and 15:27 = OPPOSITE TRENDS
#
# Volume(15:27) > Volume(15:24)
#
# 09:15 and 09:18 = SAME TREND AS 15:27
#
# ============================================================
#
# 1-MINUTE CONDITIONS
# ============================================================
#
# 15:28 and 15:29 = OPPOSITE TRENDS
#
# Volume(15:28) > Volume(15:29)
#
# 15:28 = SAME TREND AS 3-MIN 15:27
#
# 09:15 and 09:16 = SAME TREND AS 15:28
#
# ============================================================
#
# TRADE
# ============================================================
#
# DIRECTION = TREND OF 3-MIN 15:27
#
# NEXT DAY:
#
# ENTRY = 09:15 OPEN
# EXIT  = 15:27 OPEN
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

# Total round-trip cost in percentage points.
#
# Example:
# 0.10 = subtract 0.10% from every trade.
#
# Set to 0.0 for raw/gross results.
ROUND_TRIP_COST = 0.10


# ============================================================
# TREND
# ============================================================

def trend(open_price, close_price):
    """
    Returns:
        +1 = bullish / GREEN
        -1 = bearish / RED
         0 = doji
    """

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


# ============================================================
# LOAD ONE STOCK
# ============================================================

def load_stock(file_path):

    df = pd.read_csv(
        file_path
    )

    # --------------------------------------------------------
    # Find datetime column
    # --------------------------------------------------------

    datetime_column = None

    for column in [
        "datetime",
        "Datetime",
        "date_time",
        "timestamp",
        "time",
        "Date"
    ]:

        if column in df.columns:

            datetime_column = column
            break

    if datetime_column is None:

        raise ValueError(
            f"No datetime column found in "
            f"{file_path}"
        )

    # --------------------------------------------------------
    # Datetime
    # --------------------------------------------------------

    df["datetime"] = pd.to_datetime(
        df[datetime_column]
    )

    # --------------------------------------------------------
    # Standardize OHLCV
    # --------------------------------------------------------

    rename = {}

    for column in df.columns:

        name = column.lower()

        if name == "open":
            rename[column] = "open"

        elif name == "high":
            rename[column] = "high"

        elif name == "low":
            rename[column] = "low"

        elif name == "close":
            rename[column] = "close"

        elif name in [
            "volume",
            "vol"
        ]:
            rename[column] = "volume"

    df = df.rename(
        columns=rename
    )

    required = [
        "open",
        "high",
        "low",
        "close"
    ]

    for column in required:

        if column not in df.columns:

            raise ValueError(
                f"{column} column missing "
                f"in {file_path}"
            )

    if "volume" not in df.columns:

        raise ValueError(
            f"Volume column missing "
            f"in {file_path}"
        )

    # --------------------------------------------------------
    # Clean
    # --------------------------------------------------------

    df = df.dropna(
        subset=[
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    ).copy()

    df = df.sort_values(
        "datetime"
    ).reset_index(
        drop=True
    )

    # --------------------------------------------------------
    # Date/time columns
    # --------------------------------------------------------

    df["date"] = (
        df["datetime"]
        .dt.date
    )

    df["time"] = (
        df["datetime"]
        .dt.strftime(
            "%H:%M"
        )
    )

    # --------------------------------------------------------
    # NSE regular trading session
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

    return df


# ============================================================
# CREATE EXACT 3-MINUTE CANDLES
# ============================================================

def create_3min_candles(df):
    """
    Creates candles aligned to the NSE open:

        09:15 - 09:17
        09:18 - 09:20
        09:21 - 09:23
        ...
        15:24 - 15:26
        15:27 - 15:29

    Therefore:

        09:15 = 09:15-09:17
        09:18 = 09:18-09:20

        15:24 = 15:24-15:26
        15:27 = 15:27-15:29
    """

    x = df.copy()

    # --------------------------------------------------------
    # Minutes since 09:15
    # --------------------------------------------------------

    total_minutes = (
        x["datetime"].dt.hour * 60
        +
        x["datetime"].dt.minute
    )

    market_open_minutes = (
        9 * 60 + 15
    )

    minutes_from_open = (
        total_minutes -
        market_open_minutes
    )

    # --------------------------------------------------------
    # 3-minute bucket
    # --------------------------------------------------------

    x["bucket"] = (
        minutes_from_open // 3
    )

    # --------------------------------------------------------
    # Starting minute of each bucket
    # --------------------------------------------------------

    candle_start = (
        market_open_minutes
        +
        x["bucket"] * 3
    )

    candle_hour = (
        candle_start // 60
    )

    candle_minute = (
        candle_start % 60
    )

    x["candle_time"] = (
        candle_hour.astype(str)
        .str.zfill(2)
        +
        ":"
        +
        candle_minute.astype(str)
        .str.zfill(2)
    )

    # --------------------------------------------------------
    # Aggregate 1-minute data
    # --------------------------------------------------------

    candles = (
        x
        .groupby(
            [
                "date",
                "candle_time"
            ],
            sort=True
        )
        .agg(

            open=(
                "open",
                "first"
            ),

            high=(
                "high",
                "max"
            ),

            low=(
                "low",
                "min"
            ),

            close=(
                "close",
                "last"
            ),

            volume=(
                "volume",
                "sum"
            ),

            minute_count=(
                "close",
                "count"
            )
        )
        .reset_index()
    )

    return candles


# ============================================================
# GET EXACT 3-MIN CANDLE
# ============================================================

def get_3min_candle(
    candles,
    date,
    time
):

    result = candles[
        (
            candles["date"]
            ==
            date
        )
        &
        (
            candles["candle_time"]
            ==
            time
        )
    ]

    if result.empty:

        return None

    return result.iloc[0]


# ============================================================
# GET EXACT 1-MIN CANDLE
# ============================================================

def get_1min_candle(
    df,
    date,
    time
):

    result = df[
        (
            df["date"]
            ==
            date
        )
        &
        (
            df["time"]
            ==
            time
        )
    ]

    if result.empty:

        return None

    return result.iloc[0]


# ============================================================
# CHECK PREVIOUS-DAY SIGNAL
# ============================================================

def check_previous_day_signal(
    df,
    candles_3m,
    previous_date
):
    """
    Returns:

        +1 = LONG
        -1 = SHORT
         0 = NO SIGNAL
    """

    # ========================================================
    # 3-MINUTE CANDLES
    # ========================================================

    c3_0915 = get_3min_candle(
        candles_3m,
        previous_date,
        "09:15"
    )

    c3_0918 = get_3min_candle(
        candles_3m,
        previous_date,
        "09:18"
    )

    c3_1524 = get_3min_candle(
        candles_3m,
        previous_date,
        "15:24"
    )

    c3_1527 = get_3min_candle(
        candles_3m,
        previous_date,
        "15:27"
    )

    # --------------------------------------------------------
    # All required 3-min candles must exist
    # --------------------------------------------------------

    if any(
        c is None
        for c in [
            c3_0915,
            c3_0918,
            c3_1524,
            c3_1527
        ]
    ):

        return 0

    # --------------------------------------------------------
    # 3-min trends
    # --------------------------------------------------------

    t3_0915 = trend(
        c3_0915["open"],
        c3_0915["close"]
    )

    t3_0918 = trend(
        c3_0918["open"],
        c3_0918["close"]
    )

    t3_1524 = trend(
        c3_1524["open"],
        c3_1524["close"]
    )

    t3_1527 = trend(
        c3_1527["open"],
        c3_1527["close"]
    )

    # --------------------------------------------------------
    # No dojis
    # --------------------------------------------------------

    if 0 in [
        t3_0915,
        t3_0918,
        t3_1524,
        t3_1527
    ]:

        return 0

    # ========================================================
    # CONDITION 1
    #
    # 15:24 and 15:27 OPPOSITE
    # ========================================================

    if t3_1524 == t3_1527:

        return 0

    # ========================================================
    # CONDITION 2
    #
    # Volume(15:27) > Volume(15:24)
    # ========================================================

    if not (
        c3_1527["volume"]
        >
        c3_1524["volume"]
    ):

        return 0

    # ========================================================
    # CONDITION 3
    #
    # 09:15 and 09:18 SAME AS 15:27
    # ========================================================

    if t3_0915 != t3_1527:

        return 0

    if t3_0918 != t3_1527:

        return 0

    # ========================================================
    # 1-MINUTE CANDLES
    # ========================================================

    c1_0915 = get_1min_candle(
        df,
        previous_date,
        "09:15"
    )

    c1_0916 = get_1min_candle(
        df,
        previous_date,
        "09:16"
    )

    c1_1528 = get_1min_candle(
        df,
        previous_date,
        "15:28"
    )

    c1_1529 = get_1min_candle(
        df,
        previous_date,
        "15:29"
    )

    # --------------------------------------------------------
    # All required 1-min candles must exist
    # --------------------------------------------------------

    if any(
        c is None
        for c in [
            c1_0915,
            c1_0916,
            c1_1528,
            c1_1529
        ]
    ):

        return 0

    # --------------------------------------------------------
    # 1-min trends
    # --------------------------------------------------------

    t1_0915 = trend(
        c1_0915["open"],
        c1_0915["close"]
    )

    t1_0916 = trend(
        c1_0916["open"],
        c1_0916["close"]
    )

    t1_1528 = trend(
        c1_1528["open"],
        c1_1528["close"]
    )

    t1_1529 = trend(
        c1_1529["open"],
        c1_1529["close"]
    )

    # --------------------------------------------------------
    # No dojis
    # --------------------------------------------------------

    if 0 in [
        t1_0915,
        t1_0916,
        t1_1528,
        t1_1529
    ]:

        return 0

    # ========================================================
    # CONDITION 4
    #
    # 15:28 and 15:29 OPPOSITE
    # ========================================================

    if t1_1528 == t1_1529:

        return 0

    # ========================================================
    # CONDITION 5
    #
    # Volume(15:28) > Volume(15:29)
    # ========================================================

    if not (
        c1_1528["volume"]
        >
        c1_1529["volume"]
    ):

        return 0

    # ========================================================
    # CONDITION 6
    #
    # 1-MIN 15:28 = 3-MIN 15:27
    # ========================================================

    if t1_1528 != t3_1527:

        return 0

    # ========================================================
    # CONDITION 7
    #
    # 1-MIN 09:15 and 09:16
    # SAME AS 1-MIN 15:28
    # ========================================================

    if t1_0915 != t1_1528:

        return 0

    if t1_0916 != t1_1528:

        return 0

    # ========================================================
    # EVERYTHING PASSED
    #
    # DIRECTION = 3-MIN 15:27
    # ========================================================

    return t3_1527


# ============================================================
# BACKTEST ONE STOCK
# ============================================================

def backtest_stock(
    file_path
):

    df = load_stock(
        file_path
    )

    candles_3m = create_3min_candles(
        df
    )

    if candles_3m.empty:

        return []

    dates = sorted(
        df["date"]
        .unique()
    )

    results = []

    # ========================================================
    # PREVIOUS DAY -> NEXT DAY
    # ========================================================

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

        # ----------------------------------------------------
        # SIGNAL FROM PREVIOUS DAY ONLY
        # ----------------------------------------------------

        signal = check_previous_day_signal(
            df,
            candles_3m,
            previous_date
        )

        if signal == 0:

            continue

        # ====================================================
        # NEXT DAY ENTRY
        # ====================================================

        entry_candle = get_1min_candle(
            df,
            trade_date,
            ENTRY_TIME
        )

        if entry_candle is None:

            continue

        entry_price = float(
            entry_candle["open"]
        )

        # ====================================================
        # NEXT DAY EXIT
        # ====================================================

        exit_candle = get_1min_candle(
            df,
            trade_date,
            EXIT_TIME
        )

        if exit_candle is None:

            continue

        exit_price = float(
            exit_candle["open"]
        )

        if entry_price <= 0:

            continue

        # ====================================================
        # RETURN
        # ====================================================

        if signal == 1:

            direction_name = "LONG"

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

            direction_name = "SHORT"

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
                direction_name,

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

        return None

    returns = (
        trades[
            "net_return"
        ]
        .dropna()
    )

    if returns.empty:

        return None

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
# MAIN
# ============================================================

def main():

    print()
    print("=" * 100)
    print(
        "EXACT 3-MIN + 1-MIN EOD/OPENING STRATEGY"
    )
    print("=" * 100)

    print()
    print("PREVIOUS DAY CONDITIONS:")
    print()
    print(
        "3M 15:24 != 3M 15:27"
    )

    print(
        "3M Volume(15:27) > Volume(15:24)"
    )

    print(
        "3M 09:15 = 3M 09:18 = 3M 15:27"
    )

    print()
    print(
        "1M 15:28 != 1M 15:29"
    )

    print(
        "1M Volume(15:28) > Volume(15:29)"
    )

    print(
        "1M 15:28 = 3M 15:27"
    )

    print(
        "1M 09:15 = 1M 09:16 = 1M 15:28"
    )

    print()
    print(
        "DIRECTION = 3M 15:27"
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
        "ROUND-TRIP COST =",
        ROUND_TRIP_COST,
        "%"
    )

    # ========================================================
    # FIND FILES
    # ========================================================

    files = []

    files.extend(
        glob.glob(
            os.path.join(
                DATA_FOLDER,
                "*.csv"
            )
        )
    )

    files.extend(
        glob.glob(
            os.path.join(
                DATA_FOLDER,
                "*.CSV"
            )
        )
    )

    files = sorted(
        set(files)
    )

    if not files:

        raise FileNotFoundError(
            f"No CSV files found in "
            f"{DATA_FOLDER}"
        )

    print()
    print(
        "Stocks found:",
        len(files)
    )

    # ========================================================
    # BACKTEST
    # ========================================================

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

            results = backtest_stock(
                file_path
            )

            if results:

                all_results.extend(
                    results
                )

        except Exception as e:

            print(
                f"\nERROR in "
                f"{file_path}: "
                f"{e}"
            )

    print()

    # ========================================================
    # NO TRADES
    # ========================================================

    if not all_results:

        print()
        print("=" * 100)
        print(
            "NO QUALIFYING TRADES FOUND"
        )
        print("=" * 100)

        return

    # ========================================================
    # DATAFRAME
    # ========================================================

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

    # ========================================================
    # OVERALL RESULTS
    # ========================================================

    stats = calculate_statistics(
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
        f"{stats['trades']:,}"
    )

    print(
        f"Wins            : "
        f"{stats['wins']:,}"
    )

    print(
        f"Losses          : "
        f"{stats['losses']:,}"
    )

    print(
        f"Win rate        : "
        f"{stats['win_rate']:.2f}%"
    )

    print(
        f"Average return  : "
        f"{stats['average_return']:.4f}%"
    )

    print(
        f"Total return    : "
        f"{stats['total_return']:.4f}%"
    )

    print(
        f"Profit factor   : "
        f"{stats['profit_factor']:.3f}"
    )

    print(
        f"Best trade      : "
        f"{stats['best_trade']:.4f}%"
    )

    print(
        f"Worst trade     : "
        f"{stats['worst_trade']:.4f}%"
    )

    # ========================================================
    # LONG / SHORT
    # ========================================================

    for side in [
        "LONG",
        "SHORT"
    ]:

        subset = trades[
            trades["direction"]
            ==
            side
        ]

        if subset.empty:
            continue

        s = calculate_statistics(
            subset
        )

        print()
        print("=" * 100)
        print(
            f"{side} RESULTS"
        )
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
            f"Average return : "
            f"{s['average_return']:.4f}%"
        )

        print(
            f"Profit factor  : "
            f"{s['profit_factor']:.3f}"
        )

    # ========================================================
    # YEARLY RESULTS
    # ========================================================

    trades["year"] = (
        pd.to_datetime(
            trades["trade_date"]
        ).dt.year
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

            "wins":
                s["wins"],

            "losses":
                s["losses"],

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
    print(
        "YEARLY RESULTS"
    )
    print("=" * 100)

    print(
        yearly_df.to_string(
            index=False
        )
    )

    # ========================================================
    # SAVE TRADES
    # ========================================================

    trades.to_csv(
        "EXACT_3M_1M_STRATEGY_TRADES.csv",
        index=False
    )

    yearly_df.to_csv(
        "EXACT_3M_1M_STRATEGY_YEARLY.csv",
        index=False
    )

    # ========================================================
    # FINAL
    # ========================================================

    print()
    print("=" * 100)
    print(
        "BACKTEST COMPLETE"
    )
    print("=" * 100)

    print()
    print(
        "Saved:"
    )

    print(
        "EXACT_3M_1M_STRATEGY_TRADES.csv"
    )

    print(
        "EXACT_3M_1M_STRATEGY_YEARLY.csv"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
