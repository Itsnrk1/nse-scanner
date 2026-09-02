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

# Search these repositories/folders
DATA_DIRS = [
    "Nse_Historical_Data",
    "Nse_Historical_Data_2026",
]

# BOTH = Long + Short
# LONG = Long only
# SHORT = Short only
TRADE_SIDE = "BOTH"

# Entry / Exit
ENTRY_TIME = "09:15"
EXIT_TIME = "15:27"

# NSE regular session
MARKET_OPEN = "09:15"
MARKET_CLOSE = "15:29"

# Use almost all CPU cores
MAX_WORKERS = max(
    1,
    (os.cpu_count() or 2) - 1
)

RESULTS_DIR = "results"


# ============================================================
# TREND
# ============================================================

def trend_from_prices(open_price, close_price):
    """
    1  = bullish
    -1 = bearish
    0  = doji
    """

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


# ============================================================
# FIND PARQUET FILES
# ============================================================

def find_parquet_files():

    files = []

    # --------------------------------------------------------
    # Search current repository
    # --------------------------------------------------------

    for directory in DATA_DIRS:

        if not os.path.exists(directory):
            continue

        files.extend(
            glob.glob(
                os.path.join(
                    directory,
                    "**",
                    "*.parquet"
                ),
                recursive=True
            )
        )

    # --------------------------------------------------------
    # Fallback: search entire current workspace
    # --------------------------------------------------------

    if not files:

        files.extend(
            glob.glob(
                "./**/*.parquet",
                recursive=True
            )
        )

    # --------------------------------------------------------
    # GitHub Actions workspace fallback
    # --------------------------------------------------------

    runner_workspace = "/home/runner/work"

    if os.path.exists(
        runner_workspace
    ):

        files.extend(
            glob.glob(
                os.path.join(
                    runner_workspace,
                    "**",
                    "*.parquet"
                ),
                recursive=True
            )
        )

    # Remove duplicates
    files = sorted(
        set(
            os.path.abspath(f)
            for f in files
            if os.path.isfile(f)
        )
    )

    return files


# ============================================================
# LOAD DATA
# ============================================================

def load_data(path):

    """
    Only load the columns required for this strategy.

    This reduces memory usage and improves speed.
    """

    # First inspect parquet metadata
    try:

        available_columns = pd.read_parquet(
            path,
            engine="pyarrow"
        ).columns.tolist()

    except Exception:

        available_columns = None

    # --------------------------------------------------------
    # If we cannot inspect efficiently, normal load
    # --------------------------------------------------------

    if available_columns is None:

        df = pd.read_parquet(path)

    else:

        columns_lower = {
            str(c).strip().lower().replace(" ", "_"): c
            for c in available_columns
        }

        # Find columns
        datetime_col = None
        open_col = None
        high_col = None
        low_col = None
        close_col = None
        volume_col = None

        for name, original in columns_lower.items():

            if name in [
                "datetime",
                "timestamp",
                "date",
                "time",
                "index"
            ]:
                if datetime_col is None:
                    datetime_col = original

            elif name in ["open", "o"]:
                open_col = original

            elif name in ["high", "h"]:
                high_col = original

            elif name in ["low", "l"]:
                low_col = original

            elif name in ["close", "c"]:
                close_col = original

            elif name in [
                "volume",
                "vol",
                "qty",
                "quantity"
            ]:
                volume_col = original

        required = [
            datetime_col,
            open_col,
            high_col,
            low_col,
            close_col,
            volume_col
        ]

        if all(
            column is not None
            for column in required
        ):

            df = pd.read_parquet(
                path,
                columns=required,
                engine="pyarrow"
            )

            df.columns = [
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]

        else:

            df = pd.read_parquet(
                path,
                engine="pyarrow"
            )

            df.columns = [
                str(c)
                .strip()
                .lower()
                .replace(" ", "_")
                for c in df.columns
            ]

            # Find again
            def find_col(candidates):

                for candidate in candidates:

                    if candidate in df.columns:
                        return candidate

                return None

            datetime_col = find_col([
                "datetime",
                "timestamp",
                "date",
                "time",
                "index"
            ])

            open_col = find_col([
                "open",
                "o"
            ])

            high_col = find_col([
                "high",
                "h"
            ])

            low_col = find_col([
                "low",
                "l"
            ])

            close_col = find_col([
                "close",
                "c"
            ])

            volume_col = find_col([
                "volume",
                "vol",
                "qty",
                "quantity"
            ])

            if datetime_col is None:
                raise ValueError(
                    "Datetime column not found"
                )

            if open_col is None:
                raise ValueError(
                    "Open column not found"
                )

            if high_col is None:
                raise ValueError(
                    "High column not found"
                )

            if low_col is None:
                raise ValueError(
                    "Low column not found"
                )

            if close_col is None:
                raise ValueError(
                    "Close column not found"
                )

            if volume_col is None:
                raise ValueError(
                    "Volume column not found"
                )

            df = df.rename(
                columns={
                    datetime_col: "datetime",
                    open_col: "open",
                    high_col: "high",
                    low_col: "low",
                    close_col: "close",
                    volume_col: "volume"
                }
            )

            df = df[
                [
                    "datetime",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                ]
            ]

    # --------------------------------------------------------
    # Datetime
    # --------------------------------------------------------

    df["datetime"] = pd.to_datetime(
        df["datetime"],
        errors="coerce"
    )

    df = df.dropna(
        subset=[
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    )

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    df = df.sort_values(
        "datetime"
    )

    df = df.drop_duplicates(
        subset="datetime",
        keep="first"
    )

    # --------------------------------------------------------
    # Time / Date
    # --------------------------------------------------------

    df["date"] = df["datetime"].dt.date

    df["time"] = df["datetime"].dt.strftime(
        "%H:%M"
    )

    # --------------------------------------------------------
    # We only need:
    #
    # 09:15
    # 15:24 - 15:29
    #
    # Everything else is unnecessary for signal detection.
    # --------------------------------------------------------

    df = df[
        (
            df["time"] == ENTRY_TIME
        )
        |
        (
            df["time"] >= "15:24"
        )
        &
        (
            df["time"] <= "15:29"
        )
    ].copy()

    return df


# ============================================================
# BUILD DAILY SIGNAL DATA
# ============================================================

def build_daily_data(df):

    """
    Convert the raw 1-minute data into a compact dictionary.

    For each day we retain only information required by the
    strategy.

    This is a major speed optimization.
    """

    daily = {}

    for day, group in df.groupby(
        "date",
        sort=True
    ):

        # Map minute -> row
        rows = {}

        for row in group.itertuples(
            index=False
        ):

            rows[row.time] = (
                float(row.open),
                float(row.high),
                float(row.low),
                float(row.close),
                float(row.volume)
            )

        daily[day] = rows

    return daily


# ============================================================
# GET 1-MINUTE PATTERN
# ============================================================

def get_1m_pattern(rows):

    """
    1-minute conditions:

        15:28 and 15:29 opposite trends

        15:28 volume > 15:29 volume

    Returns trend of 15:28.
    """

    if "15:28" not in rows:
        return 0

    if "15:29" not in rows:
        return 0

    c28 = rows["15:28"]
    c29 = rows["15:29"]

    trend28 = trend_from_prices(
        c28[0],
        c28[3]
    )

    trend29 = trend_from_prices(
        c29[0],
        c29[3]
    )

    # No dojis
    if trend28 == 0:
        return 0

    if trend29 == 0:
        return 0

    # Must be opposite
    if trend28 == trend29:
        return 0

    # 15:28 volume > 15:29 volume
    if c28[4] <= c29[4]:
        return 0

    return trend28


# ============================================================
# GET 3-MINUTE PATTERN
# ============================================================

def get_3m_pattern(
    rows,
    one_minute_1528_trend
):

    """
    3-minute candles:

        First last candle:
            15:24 - 15:26

        Last candle:
            15:27 - 15:29

    Conditions:

        15:24 and 15:27 opposite trends

        15:27 3m volume > 15:24 3m volume

        15:27 trend =
            1m 15:28 trend
    """

    required = [
        "15:24",
        "15:25",
        "15:26",
        "15:27",
        "15:28",
        "15:29"
    ]

    for t in required:

        if t not in rows:
            return False, 0, 0, 0

    # --------------------------------------------------------
    # 3-minute candle 15:24
    # --------------------------------------------------------

    c24 = rows["15:24"]
    c25 = rows["15:25"]
    c26 = rows["15:26"]

    candle_3m_1524_open = c24[0]
    candle_3m_1524_close = c26[3]

    candle_3m_1524_volume = (
        c24[4]
        + c25[4]
        + c26[4]
    )

    trend_3m_1524 = trend_from_prices(
        candle_3m_1524_open,
        candle_3m_1524_close
    )

    # --------------------------------------------------------
    # 3-minute candle 15:27
    # --------------------------------------------------------

    c27 = rows["15:27"]
    c28 = rows["15:28"]
    c29 = rows["15:29"]

    candle_3m_1527_open = c27[0]
    candle_3m_1527_close = c29[3]

    candle_3m_1527_volume = (
        c27[4]
        + c28[4]
        + c29[4]
    )

    trend_3m_1527 = trend_from_prices(
        candle_3m_1527_open,
        candle_3m_1527_close
    )

    # Doji invalid
    if trend_3m_1524 == 0:
        return False, 0, 0, 0

    if trend_3m_1527 == 0:
        return False, 0, 0, 0

    # --------------------------------------------------------
    # Last two 3-minute candles must be opposite
    # --------------------------------------------------------

    if trend_3m_1524 == trend_3m_1527:
        return False, 0, 0, 0

    # --------------------------------------------------------
    # 15:27 3-minute volume must be greater
    # than 15:24 3-minute volume
    # --------------------------------------------------------

    if (
        candle_3m_1527_volume
        <= candle_3m_1524_volume
    ):
        return False, 0, 0, 0

    # --------------------------------------------------------
    # 3-minute 15:27 trend must match
    # 1-minute 15:28 trend
    # --------------------------------------------------------

    if (
        trend_3m_1527
        != one_minute_1528_trend
    ):
        return False, 0, 0, 0

    return (
        True,
        trend_3m_1524,
        trend_3m_1527,
        candle_3m_1527_volume
    )


# ============================================================
# PROCESS ONE STOCK
# ============================================================

def process_stock(path):

    filename = os.path.basename(path)

    symbol = filename

    if symbol.lower().endswith(
        ".parquet"
    ):
        symbol = symbol[:-8]

    try:

        # ----------------------------------------------------
        # Load only required data
        # ----------------------------------------------------

        df = load_data(path)

        if df.empty:

            return {
                "symbol": symbol,
                "trades": [],
                "diagnostics": {},
                "error": "Empty data"
            }

        # ----------------------------------------------------
        # Build compact daily dictionary
        # ----------------------------------------------------

        daily = build_daily_data(
            df
        )

        dates = sorted(
            daily.keys()
        )

        trades = []

        diagnostics = {
            "sequences_checked": 0,

            "day_minus_2_1m_pass": 0,
            "day_minus_2_3m_pass": 0,

            "day_minus_1_1m_pass": 0,
            "day_minus_1_3m_pass": 0,

            "same_1m_trend": 0,

            "final_signals": 0,
            "executed_trades": 0,

            "incomplete_sequences": 0,
        }

        # ----------------------------------------------------
        # Need three trading days:
        #
        # i-2 = Day -2
        # i-1 = Day -1
        # i   = Trade day
        # ----------------------------------------------------

        for i in range(
            2,
            len(dates)
        ):

            day_minus_2_date = dates[
                i - 2
            ]

            previous_date = dates[
                i - 1
            ]

            trade_date = dates[
                i
            ]

            day_minus_2 = daily[
                day_minus_2_date
            ]

            previous_day = daily[
                previous_date
            ]

            trade_day = daily[
                trade_date
            ]

            diagnostics[
                "sequences_checked"
            ] += 1

            # ------------------------------------------------
            # Required data
            # ------------------------------------------------

            required_signal_times = {
                "15:24",
                "15:25",
                "15:26",
                "15:27",
                "15:28",
                "15:29"
            }

            if not required_signal_times.issubset(
                day_minus_2.keys()
            ):

                diagnostics[
                    "incomplete_sequences"
                ] += 1

                continue

            if not required_signal_times.issubset(
                previous_day.keys()
            ):

                diagnostics[
                    "incomplete_sequences"
                ] += 1

                continue

            # ------------------------------------------------
            # Trade day
            # ------------------------------------------------

            if ENTRY_TIME not in trade_day:

                diagnostics[
                    "incomplete_sequences"
                ] += 1

                continue

            if EXIT_TIME not in trade_day:

                diagnostics[
                    "incomplete_sequences"
                ] += 1

                continue

            # =================================================
            # DAY -2
            # =================================================

            trend_minus_2 = get_1m_pattern(
                day_minus_2
            )

            if trend_minus_2 == 0:
                continue

            diagnostics[
                "day_minus_2_1m_pass"
            ] += 1

            # Day -2 3-minute conditions
            pass_3m_minus_2, trend_3m_24_minus_2, trend_3m_27_minus_2, volume_3m_27_minus_2 = get_3m_pattern(
                day_minus_2,
                trend_minus_2
            )

            if not pass_3m_minus_2:
                continue

            diagnostics[
                "day_minus_2_3m_pass"
            ] += 1

            # =================================================
            # DAY -1
            # =================================================

            trend_previous = get_1m_pattern(
                previous_day
            )

            if trend_previous == 0:
                continue

            diagnostics[
                "day_minus_1_1m_pass"
            ] += 1

            # ------------------------------------------------
            # Day -1 3-minute conditions
            # ------------------------------------------------

            pass_3m_previous, trend_3m_24_previous, trend_3m_27_previous, volume_3m_27_previous = get_3m_pattern(
                previous_day,
                trend_previous
            )

            if not pass_3m_previous:
                continue

            diagnostics[
                "day_minus_1_3m_pass"
            ] += 1

            # =================================================
            # BOTH DAYS MUST HAVE SAME 1-MINUTE TREND
            # =================================================

            if (
                trend_previous
                != trend_minus_2
            ):
                continue

            diagnostics[
                "same_1m_trend"
            ] += 1

            # =================================================
            # FINAL TRADE SIDE
            # =================================================

            if (
                TRADE_SIDE == "LONG"
                and trend_previous != 1
            ):
                continue

            if (
                TRADE_SIDE == "SHORT"
                and trend_previous != -1
            ):
                continue

            diagnostics[
                "final_signals"
            ] += 1

            # =================================================
            # TRADE NEXT DAY
            # =================================================

            entry_row = trade_day[
                ENTRY_TIME
            ]

            exit_row = trade_day[
                EXIT_TIME
            ]

            entry_price = float(
                entry_row[0]
            )

            exit_price = float(
                exit_row[0]
            )

            if entry_price <= 0:
                continue

            # ------------------------------------------------
            # LONG
            # ------------------------------------------------

            if trend_previous == 1:

                return_pct = (
                    (
                        exit_price
                        - entry_price
                    )
                    / entry_price
                ) * 100

                direction = "LONG"

            # ------------------------------------------------
            # SHORT
            # ------------------------------------------------

            else:

                return_pct = (
                    (
                        entry_price
                        - exit_price
                    )
                    / entry_price
                ) * 100

                direction = "SHORT"

            diagnostics[
                "executed_trades"
            ] += 1

            # ------------------------------------------------
            # Store trade
            # ------------------------------------------------

            trades.append({
                "symbol": symbol,

                "day_minus_2": str(
                    day_minus_2_date
                ),

                "previous_day": str(
                    previous_date
                ),

                "trade_date": str(
                    trade_date
                ),

                "direction": direction,

                "entry_price": entry_price,

                "exit_price": exit_price,

                "return_pct": return_pct,

                "win": return_pct > 0,

                # Day -2
                "day_minus_2_1m_15_28":
                    "LONG"
                    if trend_minus_2 == 1
                    else "SHORT",

                "day_minus_2_1m_15_29":
                    "SHORT"
                    if trend_minus_2 == 1
                    else "LONG",

                "day_minus_2_3m_15_24":
                    "LONG"
                    if trend_3m_24_minus_2 == 1
                    else "SHORT",

                "day_minus_2_3m_15_27":
                    "LONG"
                    if trend_3m_27_minus_2 == 1
                    else "SHORT",

                "day_minus_2_3m_15_27_volume":
                    volume_3m_27_minus_2,

                # Day -1
                "previous_1m_15_28":
                    "LONG"
                    if trend_previous == 1
                    else "SHORT",

                "previous_1m_15_29":
                    "SHORT"
                    if trend_previous == 1
                    else "LONG",

                "previous_3m_15_24":
                    "LONG"
                    if trend_3m_24_previous == 1
                    else "SHORT",

                "previous_3m_15_27":
                    "LONG"
                    if trend_3m_27_previous == 1
                    else "SHORT",

                "previous_3m_15_27_volume":
                    volume_3m_27_previous,
            })

        return {
            "symbol": symbol,
            "trades": trades,
            "diagnostics": diagnostics,
            "error": None
        }

    except Exception as e:

        return {
            "symbol": symbol,
            "trades": [],
            "diagnostics": {},
            "error": str(e)
        }


# ============================================================
# STATISTICS
# ============================================================

def calculate_statistics(
    trades
):

    if trades.empty:
        return {}

    returns = trades[
        "return_pct"
    ].astype(float)

    wins = returns > 0
    losses = returns < 0

    total = len(
        returns
    )

    win_count = int(
        wins.sum()
    )

    loss_count = int(
        losses.sum()
    )

    win_rate = (
        win_count
        / total
        * 100
    )

    avg_return = returns.mean()

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
        "trades": total,
        "wins": win_count,
        "losses": loss_count,
        "win_rate": win_rate,
        "average_return": avg_return,
        "total_return": total_return,
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

    print()
    print("=" * 75)
    print("TWO-DAY 1M + 3M EOD STRATEGY BACKTEST")
    print("=" * 75)

    print()
    print("DAY -2 CONDITIONS")
    print("-" * 75)
    print(
        "1m 15:28 & 15:29 opposite"
    )
    print(
        "1m 15:28 volume > 15:29"
    )
    print(
        "3m 15:24 & 15:27 opposite"
    )
    print(
        "3m 15:27 volume > 15:24"
    )
    print(
        "3m 15:27 trend = 1m 15:28 trend"
    )

    print()
    print("DAY -1 CONDITIONS")
    print("-" * 75)
    print(
        "1m 15:28 & 15:29 opposite"
    )
    print(
        "1m 15:28 volume > 15:29"
    )
    print(
        "3m 15:24 & 15:27 opposite"
    )
    print(
        "3m 15:27 volume > 15:24"
    )
    print(
        "3m 15:27 trend = 1m 15:28 trend"
    )
    print(
        "Day -1 1m 15:28 trend = "
        "Day -2 1m 15:28 trend"
    )

    print()
    print("TRADE")
    print("-" * 75)
    print(
        "Entry: Next trading day 09:15 OPEN"
    )
    print(
        "Exit : Same day 15:27 OPEN"
    )
    print(
        "Target: NONE"
    )
    print(
        "Stop-loss: NONE"
    )
    print(
        f"Side: {TRADE_SIDE}"
    )

    print()
    print(
        f"CPU workers: {MAX_WORKERS}"
    )

    # --------------------------------------------------------
    # Find files
    # --------------------------------------------------------

    files = find_parquet_files()

    print()
    print(
        f"Parquet files found: {len(files):,}"
    )

    if not files:

        print()
        print(
            "ERROR: No Parquet files found."
        )

        print()
        print(
            "Check the GitHub Actions data download step."
        )

        return

    print()
    print("Example files:")

    for path in files[:10]:

        print(
            f"  {path}"
        )

    # --------------------------------------------------------
    # Global diagnostics
    # --------------------------------------------------------

    diagnostics_total = {
        "sequences_checked": 0,

        "day_minus_2_1m_pass": 0,
        "day_minus_2_3m_pass": 0,

        "day_minus_1_1m_pass": 0,
        "day_minus_1_3m_pass": 0,

        "same_1m_trend": 0,

        "final_signals": 0,
        "executed_trades": 0,

        "incomplete_sequences": 0,
    }

    all_trades = []

    completed = 0

    print()
    print("=" * 75)
    print("STARTING BACKTEST")
    print("=" * 75)

    # --------------------------------------------------------
    # Parallel processing
    # --------------------------------------------------------

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

            try:

                result = future.result()

            except Exception as e:

                print(
                    f"Worker error: {e}"
                )

                continue

            completed += 1

            # Trades
            if result.get(
                "trades"
            ):

                all_trades.extend(
                    result["trades"]
                )

            # Diagnostics
            diag = result.get(
                "diagnostics",
                {}
            )

            for key in diagnostics_total:

                diagnostics_total[key] += (
                    diag.get(
                        key,
                        0
                    )
                )

            # Progress
            if (
                completed % 25 == 0
                or completed == len(files)
            ):

                print(
                    f"Processed "
                    f"{completed:,}/"
                    f"{len(files):,} | "
                    f"Trades: "
                    f"{len(all_trades):,}"
                )

    # --------------------------------------------------------
    # No trades
    # --------------------------------------------------------

    if not all_trades:

        print()
        print("=" * 75)
        print("NO TRADES FOUND")
        print("=" * 75)

        print()

        for key, value in (
            diagnostics_total.items()
        ):

            print(
                f"{key:<35}: "
                f"{value:,}"
            )

        # Save diagnostics
        pd.DataFrame(
            [diagnostics_total]
        ).to_csv(
            os.path.join(
                RESULTS_DIR,
                "diagnostics.csv"
            ),
            index=False
        )

        return

    # --------------------------------------------------------
    # Dataframe
    # --------------------------------------------------------

    trades_df = pd.DataFrame(
        all_trades
    )

    trades_df = trades_df.sort_values(
        [
            "trade_date",
            "symbol"
        ]
    ).reset_index(
        drop=True
    )

    # --------------------------------------------------------
    # Overall statistics
    # --------------------------------------------------------

    stats = calculate_statistics(
        trades_df
    )

    # --------------------------------------------------------
    # Save all trades
    # --------------------------------------------------------

    trades_df.to_csv(
        os.path.join(
            RESULTS_DIR,
            "all_trades.csv"
        ),
        index=False
    )

    # ========================================================
    # RESULTS
    # ========================================================

    print()
    print("=" * 75)
    print("OVERALL RESULTS")
    print("=" * 75)

    print(
        f"Trades           : "
        f"{stats['trades']:,}"
    )

    print(
        f"Wins             : "
        f"{stats['wins']:,}"
    )

    print(
        f"Losses           : "
        f"{stats['losses']:,}"
    )

    print(
        f"Win rate         : "
        f"{stats['win_rate']:.2f}%"
    )

    print(
        f"Average return   : "
        f"{stats['average_return']:.4f}%"
    )

    print(
        f"Total return     : "
        f"{stats['total_return']:.4f}%"
    )

    print(
        f"Profit factor    : "
        f"{stats['profit_factor']:.4f}"
    )

    print(
        f"Best trade       : "
        f"{stats['best_trade']:.4f}%"
    )

    print(
        f"Worst trade      : "
        f"{stats['worst_trade']:.4f}%"
    )

    print(
        f"Median return    : "
        f"{stats['median_return']:.4f}%"
    )

    # ========================================================
    # LONG / SHORT
    # ========================================================

    print()
    print("=" * 75)
    print("LONG / SHORT")
    print("=" * 75)

    direction_results = []

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

        direction_stats = (
            calculate_statistics(
                subset
            )
        )

        direction_results.append({
            "direction": direction,
            **direction_stats
        })

        print()
        print(direction)

        print(
            f"Trades           : "
            f"{direction_stats['trades']:,}"
        )

        print(
            f"Wins             : "
            f"{direction_stats['wins']:,}"
        )

        print(
            f"Losses           : "
            f"{direction_stats['losses']:,}"
        )

        print(
            f"Win rate         : "
            f"{direction_stats['win_rate']:.2f}%"
        )

        print(
            f"Average return   : "
            f"{direction_stats['average_return']:.4f}%"
        )

        print(
            f"Total return     : "
            f"{direction_stats['total_return']:.4f}%"
        )

        print(
            f"Profit factor    : "
            f"{direction_stats['profit_factor']:.4f}"
        )

    pd.DataFrame(
        direction_results
    ).to_csv(
        os.path.join(
            RESULTS_DIR,
            "long_short_results.csv"
        ),
        index=False
    )

    # ========================================================
    # YEARLY RESULTS
    # ========================================================

    trades_df["year"] = pd.to_datetime(
        trades_df["trade_date"]
    ).dt.year

    yearly_results = []

    print()
    print("=" * 75)
    print("YEARLY RESULTS")
    print("=" * 75)

    for year, group in trades_df.groupby(
        "year"
    ):

        year_stats = (
            calculate_statistics(
                group
            )
        )

        yearly_results.append({
            "year": year,
            **year_stats
        })

        print()
        print(
            f"{year}"
        )

        print(
            f"Trades           : "
            f"{year_stats['trades']:,}"
        )

        print(
            f"Win rate         : "
            f"{year_stats['win_rate']:.2f}%"
        )

        print(
            f"Average return   : "
            f"{year_stats['average_return']:.4f}%"
        )

        print(
            f"Total return     : "
            f"{year_stats['total_return']:.4f}%"
        )

        print(
            f"Profit factor    : "
            f"{year_stats['profit_factor']:.4f}"
        )

    pd.DataFrame(
        yearly_results
    ).to_csv(
        os.path.join(
            RESULTS_DIR,
            "yearly_results.csv"
        ),
        index=False
    )

    # ========================================================
    # MONTHLY RESULTS
    # ========================================================

    trades_df["month"] = pd.to_datetime(
        trades_df["trade_date"]
    ).dt.to_period(
        "M"
    ).astype(str)

    monthly_results = []

    for month, group in trades_df.groupby(
        "month"
    ):

        month_stats = (
            calculate_statistics(
                group
            )
        )

        monthly_results.append({
            "month": month,
            **month_stats
        })

    pd.DataFrame(
        monthly_results
    ).to_csv(
        os.path.join(
            RESULTS_DIR,
            "monthly_results.csv"
        ),
        index=False
    )

    # ========================================================
    # DAILY RESULTS
    # ========================================================

    daily_results = []

    for date, group in trades_df.groupby(
        "trade_date"
    ):

        day_stats = (
            calculate_statistics(
                group
            )
        )

        daily_results.append({
            "trade_date": date,
            **day_stats
        })

    pd.DataFrame(
        daily_results
    ).to_csv(
        os.path.join(
            RESULTS_DIR,
            "daily_results.csv"
        ),
        index=False
    )

    # ========================================================
    # DIAGNOSTICS
    # ========================================================

    print()
    print("=" * 75)
    print("SIGNAL DIAGNOSTICS")
    print("=" * 75)

    for key, value in (
        diagnostics_total.items()
    ):

        print(
            f"{key:<35}: "
            f"{value:,}"
        )

    pd.DataFrame(
        [diagnostics_total]
    ).to_csv(
        os.path.join(
            RESULTS_DIR,
            "diagnostics.csv"
        ),
        index=False
    )

    # ========================================================
    # BEST TRADES
    # ========================================================

    print()
    print("=" * 75)
    print("BEST 20 TRADES")
    print("=" * 75)

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
                "return_pct"
            ]
        ].to_string(
            index=False
        )
    )

    # ========================================================
    # WORST TRADES
    # ========================================================

    print()
    print("=" * 75)
    print("WORST 20 TRADES")
    print("=" * 75)

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
                "return_pct"
            ]
        ].to_string(
            index=False
        )
    )

    # ========================================================
    # SUMMARY
    # ========================================================

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
            "TWO-DAY 1M + 3M EOD STRATEGY\n"
        )

        f.write(
            "=" * 70
            + "\n\n"
        )

        f.write(
            "DAY -2\n"
        )

        f.write(
            "1m 15:28 and 15:29 opposite\n"
        )

        f.write(
            "1m 15:28 volume > 15:29\n"
        )

        f.write(
            "3m 15:24 and 15:27 opposite\n"
        )

        f.write(
            "3m 15:27 volume > 15:24\n"
        )

        f.write(
            "3m 15:27 trend = 1m 15:28 trend\n\n"
        )

        f.write(
            "DAY -1\n"
        )

        f.write(
            "1m 15:28 and 15:29 opposite\n"
        )

        f.write(
            "1m 15:28 volume > 15:29\n"
        )

        f.write(
            "3m 15:24 and 15:27 opposite\n"
        )

        f.write(
            "3m 15:27 volume > 15:24\n"
        )

        f.write(
            "3m 15:27 trend = 1m 15:28 trend\n"
        )

        f.write(
            "Day -1 1m 15:28 = Day -2 1m 15:28\n\n"
        )

        f.write(
            "TRADE\n"
        )

        f.write(
            "Entry: Next day 09:15 open\n"
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

        f.write(
            "RESULTS\n"
        )

        for key, value in stats.items():

            f.write(
                f"{key}: {value}\n"
            )

        f.write(
            "\nDIAGNOSTICS\n"
        )

        for key, value in (
            diagnostics_total.items()
        ):

            f.write(
                f"{key}: {value}\n"
            )

    # ========================================================
    # FINISH
    # ========================================================

    elapsed = (
        time.time()
        - start_time
    )

    print()
    print("=" * 75)
    print("BACKTEST COMPLETE")
    print("=" * 75)

    print(
        f"Time taken: "
        f"{elapsed:.2f} seconds"
    )

    print()
    print(
        "Results saved to:"
    )

    print(
        "results/all_trades.csv"
    )

    print(
        "results/yearly_results.csv"
    )

    print(
        "results/monthly_results.csv"
    )

    print(
        "results/daily_results.csv"
    )

    print(
        "results/long_short_results.csv"
    )

    print(
        "results/diagnostics.csv"
    )

    print(
        "results/summary.txt"
    )

    print("=" * 75)


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
