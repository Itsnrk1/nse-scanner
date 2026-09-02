"""
NSE 3-Minute EOD Momentum Backtest

STRATEGY
--------

SIGNAL DAY:

1. 3-minute 15:24 and 15:27 must have opposite trends.

2. 15:24 volume must be greater than 15:27 volume.

3. Among the three 3-minute candles:
       15:15
       15:18
       15:21

   At least TWO must have the same trend as 15:24.

4. Morning 3-minute candles:
       09:15
       09:18

   BOTH must have the opposite trend to 15:24.

TRADE:

- Direction = trend of 15:24
- Entry = next trading day 09:15 OPEN
- Exit = next trading day 15:27 OPEN
- No target
- No stop loss

IMPORTANT:

3-minute candles are created from 1-minute data:

09:15 = 09:15, 09:16, 09:17
09:18 = 09:18, 09:19, 09:20
...
15:15 = 15:15, 15:16, 15:17
15:18 = 15:18, 15:19, 15:20
15:21 = 15:21, 15:22, 15:23
15:24 = 15:24, 15:25, 15:26
15:27 = 15:27, 15:28, 15:29

CANDLE TREND:

GREEN/BULLISH:
    Close > Open

RED/BEARISH:
    Close < Open

DOJI:
    Close == Open

Doji candles do NOT count as matching a trend.

NO 1-MINUTE SIGNAL CONDITIONS ARE USED.
"""

from __future__ import annotations

import os
import glob
import warnings
from pathlib import Path
from datetime import datetime, time

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

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_TRADES = RESULTS_DIR / "trades.csv"
OUTPUT_STOCKS = RESULTS_DIR / "stock_summary.csv"
OUTPUT_DAILY = RESULTS_DIR / "daily_summary.csv"
OUTPUT_SUMMARY = RESULTS_DIR / "summary.txt"

# Assumed total round-trip transaction/slippage cost.
# Change to 0.0 if you want completely raw results.
ROUND_TRIP_COST_PCT = 0.10


# ============================================================
# TIME DEFINITIONS
# ============================================================

SIGNAL_TIMES = [
    "09:15",
    "09:18",
    "15:15",
    "15:18",
    "15:21",
    "15:24",
    "15:27",
]

ENTRY_TIME = "09:15"
EXIT_TIME = "15:27"


# ============================================================
# TREND FUNCTION
# ============================================================

def candle_trend(open_price, close_price):
    """
    Return:
        1  = bullish / green
       -1  = bearish / red
        0  = doji
    """

    if pd.isna(open_price) or pd.isna(close_price):
        return 0

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

    print("\nSearching for Parquet files...")

    for directory in DATA_DIRS:

        if not os.path.exists(directory):
            print(f"Directory not found: {directory}")
            continue

        found = glob.glob(
            os.path.join(directory, "**", "*.parquet"),
            recursive=True
        )

        print(f"{directory}: {len(found)} files")

        files.extend(found)

    # Remove duplicates
    files = sorted(set(files))

    print(f"\nTotal Parquet files found: {len(files)}")

    return files


# ============================================================
# NORMALIZE COLUMN NAMES
# ============================================================

def normalize_columns(df):

    df = df.copy()

    # Flatten MultiIndex columns if necessary
    if isinstance(df.columns, pd.MultiIndex):

        new_columns = []

        for col in df.columns:

            parts = []

            for value in col:

                value = str(value)

                if value.lower() != "nan":
                    parts.append(value)

            new_columns.append("_".join(parts))

        df.columns = new_columns

    rename_map = {}

    for column in df.columns:

        c = str(column).strip().lower()

        if c in ["datetime", "date_time", "timestamp", "time"]:
            rename_map[column] = "datetime"

        elif c in ["open", "open_price"]:
            rename_map[column] = "open"

        elif c in ["high", "high_price"]:
            rename_map[column] = "high"

        elif c in ["low", "low_price"]:
            rename_map[column] = "low"

        elif c in ["close", "close_price", "adj close"]:
            rename_map[column] = "close"

        elif c in ["volume", "vol"]:
            rename_map[column] = "volume"

    df = df.rename(columns=rename_map)

    return df


# ============================================================
# LOAD PARQUET
# ============================================================

def load_parquet(path):

    try:

        df = pd.read_parquet(path)

    except Exception as e:

        print(f"Could not read {path}: {e}")
        return None

    df = normalize_columns(df)

    required = ["open", "high", "low", "close", "volume"]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:

        print(
            f"Skipping {path} - missing columns: {missing}"
        )

        return None

    # --------------------------------------------------------
    # DATETIME
    # --------------------------------------------------------

    if "datetime" in df.columns:

        dt = pd.to_datetime(
            df["datetime"],
            errors="coerce"
        )

    elif isinstance(df.index, pd.DatetimeIndex):

        dt = pd.to_datetime(
            df.index,
            errors="coerce"
        )

    else:

        print(f"Skipping {path} - no datetime information")
        return None

    # Remove timezone if present
    try:

        if getattr(dt.dt, "tz", None) is not None:
            dt = dt.dt.tz_localize(None)

    except Exception:
        pass

    df["datetime"] = dt

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

    if df.empty:
        return None

    # Numeric conversion
    for column in [
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]:

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
            "volume",
        ]
    )

    df = df.sort_values("datetime")

    df = df.drop_duplicates(
        subset=["datetime"],
        keep="last"
    )

    df = df.set_index("datetime")

    return df


# ============================================================
# STOCK NAME FROM FILE
# ============================================================

def get_symbol(path):

    name = Path(path).stem.upper()

    # Remove common suffixes
    for suffix in [
        "_1MIN",
        "_1MINUTE",
        "_MINUTE",
        "_DATA",
        "_HISTORICAL",
    ]:

        if name.endswith(suffix):
            name = name[:-len(suffix)]

    return name


# ============================================================
# CREATE 3-MINUTE DATA
# ============================================================

def create_3min(df):

    """
    Resample 1-minute data into NSE-style 3-minute candles.

    Candles begin at:
        09:15
        09:18
        09:21
        ...
    """

    data = df.copy()

    # --------------------------------------------------------
    # Keep NSE regular market hours
    # --------------------------------------------------------

    data = data.between_time(
        "09:15",
        "15:29"
    )

    if data.empty:
        return pd.DataFrame()

    # --------------------------------------------------------
    # Resample from 09:15 alignment
    # --------------------------------------------------------

    candles = (
        data
        .resample(
            "3min",
            origin="start_day",
            offset="15min",
            label="left",
            closed="left"
        )
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
    )

    candles = candles.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close",
        ]
    )

    # Keep only actual NSE 3-minute candle starts
    candles = candles[
        candles.index.time >= time(9, 15)
    ]

    candles = candles[
        candles.index.time <= time(15, 27)
    ]

    return candles


# ============================================================
# GET SPECIFIC 3-MINUTE CANDLE
# ============================================================

def get_3m_candle(day_data, hhmm):

    target = pd.Timestamp(
        f"{day_data.index[0].date()} {hhmm}"
    )

    # More reliable: compare time
    matches = day_data[
        day_data.index.strftime("%H:%M") == hhmm
    ]

    if matches.empty:
        return None

    return matches.iloc[0]


# ============================================================
# CHECK SIGNAL
# ============================================================

def check_signal(day_1m, day_3m):

    """
    Returns signal information if all conditions pass.

    Otherwise returns None.
    """

    # ========================================================
    # REQUIRED 3-MINUTE CANDLES
    # ========================================================

    required_times = [
        "09:15",
        "09:18",
        "15:15",
        "15:18",
        "15:21",
        "15:24",
        "15:27",
    ]

    candles = {}

    for hhmm in required_times:

        candle = get_3m_candle(
            day_3m,
            hhmm
        )

        if candle is None:
            return None

        candles[hhmm] = candle

    # ========================================================
    # TRENDS
    # ========================================================

    trends = {}

    for hhmm, candle in candles.items():

        trends[hhmm] = candle_trend(
            candle["open"],
            candle["close"]
        )

    # --------------------------------------------------------
    # 15:24 MUST HAVE A VALID TREND
    # --------------------------------------------------------

    direction = trends["15:24"]

    if direction == 0:
        return None

    # ========================================================
    # CONDITION 1
    #
    # 15:24 AND 15:27 MUST BE OPPOSITE
    # ========================================================

    if trends["15:27"] == 0:
        return None

    cond_1 = (
        trends["15:24"]
        == -trends["15:27"]
    )

    if not cond_1:
        return None

    # ========================================================
    # CONDITION 2
    #
    # 15:24 VOLUME > 15:27 VOLUME
    # ========================================================

    volume_1524 = candles["15:24"]["volume"]
    volume_1527 = candles["15:27"]["volume"]

    cond_2 = (
        volume_1524
        > volume_1527
    )

    if not cond_2:
        return None

    # ========================================================
    # CONDITION 3
    #
    # OUT OF 15:15, 15:18, 15:21
    # AT LEAST TWO MUST MATCH 15:24
    # ========================================================

    afternoon_confirmation_times = [
        "15:15",
        "15:18",
        "15:21",
    ]

    afternoon_matches = sum(
        trends[hhmm] == direction
        for hhmm in afternoon_confirmation_times
    )

    cond_3 = afternoon_matches >= 2

    if not cond_3:
        return None

    # ========================================================
    # CONDITION 4
    #
    # MORNING 09:15 AND 09:18 MUST BOTH BE
    # OPPOSITE TO 15:24
    # ========================================================

    morning_0915 = trends["09:15"]
    morning_0918 = trends["09:18"]

    cond_4 = (
        morning_0915 != 0
        and morning_0918 != 0
        and morning_0915 == -direction
        and morning_0918 == -direction
    )

    if not cond_4:
        return None

    # ========================================================
    # SIGNAL PASSED
    # ========================================================

    return {
        "direction": direction,

        "trend_0915": morning_0915,
        "trend_0918": morning_0918,

        "trend_1515": trends["15:15"],
        "trend_1518": trends["15:18"],
        "trend_1521": trends["15:21"],
        "trend_1524": trends["15:24"],
        "trend_1527": trends["15:27"],

        "volume_1524": volume_1524,
        "volume_1527": volume_1527,

        "afternoon_matches": afternoon_matches,

        "condition_1524_1527_opposite": True,
        "condition_1524_volume_higher": True,
        "condition_2_of_3_afternoon": True,
        "condition_morning_opposite": True,
    }


# ============================================================
# FIND NEXT TRADING DAY
# ============================================================

def find_next_trading_day(
    available_days,
    signal_date
):

    future_days = [
        day
        for day in available_days
        if day > signal_date
    ]

    if not future_days:
        return None

    return future_days[0]


# ============================================================
# GET PRICE AT TIME
# ============================================================

def get_minute_price(
    day_data,
    hhmm
):

    matches = day_data[
        day_data.index.strftime("%H:%M") == hhmm
    ]

    if matches.empty:
        return None

    return matches.iloc[0]


# ============================================================
# PROCESS ONE STOCK
# ============================================================

def process_stock(
    path,
    stock_number,
    total_stocks
):

    symbol = get_symbol(path)

    print(
        f"[{stock_number}/{total_stocks}] "
        f"{symbol}"
    )

    df = load_parquet(path)

    if df is None or df.empty:
        return []

    # --------------------------------------------------------
    # Create 3-minute candles
    # --------------------------------------------------------

    df_3m = create_3min(df)

    if df_3m.empty:
        return []

    # --------------------------------------------------------
    # Trading days
    # --------------------------------------------------------

    available_days = sorted(
        set(df.index.date)
        & set(df_3m.index.date)
    )

    if len(available_days) < 2:
        return []

    trades = []

    # ========================================================
    # TEST EVERY SIGNAL DAY
    # ========================================================

    for signal_date in available_days:

        # ----------------------------------------------------
        # Need next trading day for entry
        # ----------------------------------------------------

        trade_date = find_next_trading_day(
            available_days,
            signal_date
        )

        if trade_date is None:
            continue

        # ----------------------------------------------------
        # Signal day data
        # ----------------------------------------------------

        day_1m = df[
            df.index.date == signal_date
        ]

        day_3m = df_3m[
            df_3m.index.date == signal_date
        ]

        if day_1m.empty or day_3m.empty:
            continue

        # ----------------------------------------------------
        # Check signal
        # ----------------------------------------------------

        signal = check_signal(
            day_1m,
            day_3m
        )

        if signal is None:
            continue

        # ====================================================
        # NEXT DAY ENTRY
        # ====================================================

        trade_day_1m = df[
            df.index.date == trade_date
        ]

        if trade_day_1m.empty:
            continue

        entry_candle = get_minute_price(
            trade_day_1m,
            ENTRY_TIME
        )

        exit_candle = get_minute_price(
            trade_day_1m,
            EXIT_TIME
        )

        if entry_candle is None:
            continue

        if exit_candle is None:
            continue

        entry_price = float(
            entry_candle["open"]
        )

        exit_price = float(
            exit_candle["open"]
        )

        if not np.isfinite(entry_price):
            continue

        if not np.isfinite(exit_price):
            continue

        if entry_price <= 0:
            continue

        # ====================================================
        # RAW RETURN
        # ====================================================

        direction = signal["direction"]

        if direction == 1:

            # Long
            gross_return_pct = (
                (exit_price - entry_price)
                / entry_price
            ) * 100

        else:

            # Short
            gross_return_pct = (
                (entry_price - exit_price)
                / entry_price
            ) * 100

        # ====================================================
        # COST
        # ====================================================

        net_return_pct = (
            gross_return_pct
            - ROUND_TRIP_COST_PCT
        )

        # ====================================================
        # WIN / LOSS
        # ====================================================

        win = net_return_pct > 0

        # ====================================================
        # STORE TRADE
        # ====================================================

        trades.append(
            {
                "symbol": symbol,

                "signal_date": str(
                    signal_date
                ),

                "trade_date": str(
                    trade_date
                ),

                "direction": (
                    "LONG"
                    if direction == 1
                    else "SHORT"
                ),

                "entry_time": ENTRY_TIME,
                "exit_time": EXIT_TIME,

                "entry_price": entry_price,
                "exit_price": exit_price,

                "gross_return_pct": gross_return_pct,
                "cost_pct": ROUND_TRIP_COST_PCT,
                "net_return_pct": net_return_pct,

                "win": int(win),

                # Signal diagnostics
                "09:15_trend": signal["trend_0915"],
                "09:18_trend": signal["trend_0918"],

                "15:15_trend": signal["trend_1515"],
                "15:18_trend": signal["trend_1518"],
                "15:21_trend": signal["trend_1521"],
                "15:24_trend": signal["trend_1524"],
                "15:27_trend": signal["trend_1527"],

                "15:24_volume": signal["volume_1524"],
                "15:27_volume": signal["volume_1527"],

                "15:15_18_21_matches":
                    signal["afternoon_matches"],
            }
        )

    return trades


# ============================================================
# CALCULATE MAX DRAWDOWN
# ============================================================

def calculate_max_drawdown(returns):

    if len(returns) == 0:
        return 0.0

    equity = (
        1
        + returns / 100
    ).cumprod()

    peak = equity.cummax()

    drawdown = (
        equity / peak - 1
    ) * 100

    return float(
        drawdown.min()
    )


# ============================================================
# PROFIT FACTOR
# ============================================================

def calculate_profit_factor(returns):

    gross_profit = returns[
        returns > 0
    ].sum()

    gross_loss = abs(
        returns[
            returns < 0
        ].sum()
    )

    if gross_loss == 0:

        if gross_profit > 0:
            return float("inf")

        return 0.0

    return float(
        gross_profit / gross_loss
    )


# ============================================================
# CREATE SUMMARY
# ============================================================

def create_summary(trades_df):

    if trades_df.empty:

        text = """
============================================================
BACKTEST RESULTS
============================================================

No trades were generated.

============================================================
"""

        OUTPUT_SUMMARY.write_text(
            text,
            encoding="utf-8"
        )

        print(text)

        return

    returns = trades_df[
        "net_return_pct"
    ]

    total_trades = len(
        trades_df
    )

    wins = int(
        trades_df["win"].sum()
    )

    losses = (
        total_trades
        - wins
    )

    win_rate = (
        wins
        / total_trades
        * 100
    )

    average_return = (
        returns.mean()
    )

    total_return = (
        returns.sum()
    )

    profit_factor = (
        calculate_profit_factor(
            returns
        )
    )

    max_drawdown = (
        calculate_max_drawdown(
            returns
        )
    )

    best_trade = (
        returns.max()
    )

    worst_trade = (
        returns.min()
    )

    long_df = trades_df[
        trades_df["direction"] == "LONG"
    ]

    short_df = trades_df[
        trades_df["direction"] == "SHORT"
    ]

    long_win_rate = 0.0

    if len(long_df) > 0:

        long_win_rate = (
            long_df["win"].mean()
            * 100
        )

    short_win_rate = 0.0

    if len(short_df) > 0:

        short_win_rate = (
            short_df["win"].mean()
            * 100
        )

    # --------------------------------------------------------
    # Signal statistics
    # --------------------------------------------------------

    avg_volume_ratio = (
        trades_df["15:24_volume"]
        / trades_df["15:27_volume"]
    ).mean()

    avg_confirmation_matches = (
        trades_df[
            "15:15_18_21_matches"
        ].mean()
    )

    # --------------------------------------------------------
    # Summary text
    # --------------------------------------------------------

    text = f"""
============================================================
NSE 3-MINUTE EOD STRATEGY BACKTEST
============================================================

STRATEGY CONDITIONS
------------------------------------------------------------

1. 3-min 15:24 and 15:27:
   - Opposite trends
   - 15:24 volume > 15:27 volume

2. 3-min 15:15 / 15:18 / 15:21:
   - At least 2 match 15:24 trend

3. Morning:
   - 09:15 opposite to 15:24
   - 09:18 opposite to 15:24

4. Trade:
   - Next trading day 09:15 open
   - Direction = 15:24 trend
   - Exit = 15:27 open

5. All 1-minute conditions:
   - REMOVED

------------------------------------------------------------
BACKTEST RESULTS
------------------------------------------------------------

Total trades       : {total_trades:,}
Winning trades     : {wins:,}
Losing trades      : {losses:,}

Win rate           : {win_rate:.2f}%

Average trade      : {average_return:.4f}%
Total return       : {total_return:.4f}%

Profit factor      : {profit_factor:.4f}

Best trade         : {best_trade:.4f}%
Worst trade        : {worst_trade:.4f}%

Max drawdown       : {max_drawdown:.4f}%

------------------------------------------------------------
DIRECTION
------------------------------------------------------------

Long trades        : {len(long_df):,}
Long win rate      : {long_win_rate:.2f}%

Short trades       : {len(short_df):,}
Short win rate     : {short_win_rate:.2f}%

------------------------------------------------------------
SIGNAL DIAGNOSTICS
------------------------------------------------------------

Average 15:24 / 15:27
volume ratio       : {avg_volume_ratio:.2f}x

Average number of
15:15/18/21 matches : {avg_confirmation_matches:.2f}

------------------------------------------------------------
COST ASSUMPTION
------------------------------------------------------------

Round-trip cost    : {ROUND_TRIP_COST_PCT:.2f}%

============================================================
"""

    OUTPUT_SUMMARY.write_text(
        text,
        encoding="utf-8"
    )

    print(text)


# ============================================================
# STOCK SUMMARY
# ============================================================

def create_stock_summary(trades_df):

    if trades_df.empty:

        pd.DataFrame().to_csv(
            OUTPUT_STOCKS,
            index=False
        )

        return

    rows = []

    for symbol, group in trades_df.groupby(
        "symbol"
    ):

        returns = group[
            "net_return_pct"
        ]

        trades = len(group)

        wins = int(
            group["win"].sum()
        )

        losses = (
            trades
            - wins
        )

        win_rate = (
            wins / trades * 100
            if trades > 0
            else 0
        )

        rows.append(
            {
                "symbol": symbol,
                "trades": trades,
                "wins": wins,
                "losses": losses,
                "win_rate_pct": win_rate,
                "avg_return_pct": returns.mean(),
                "total_return_pct": returns.sum(),
                "profit_factor":
                    calculate_profit_factor(
                        returns
                    ),
                "best_trade_pct":
                    returns.max(),
                "worst_trade_pct":
                    returns.min(),
            }
        )

    result = pd.DataFrame(rows)

    result = result.sort_values(
        [
            "total_return_pct",
            "win_rate_pct",
        ],
        ascending=False
    )

    result.to_csv(
        OUTPUT_STOCKS,
        index=False
    )


# ============================================================
# DAILY SUMMARY
# ============================================================

def create_daily_summary(trades_df):

    if trades_df.empty:

        pd.DataFrame().to_csv(
            OUTPUT_DAILY,
            index=False
        )

        return

    daily = (
        trades_df
        .groupby("trade_date")
        .agg(
            trades=("net_return_pct", "count"),
            wins=("win", "sum"),
            net_return_pct=(
                "net_return_pct",
                "sum"
            ),
            avg_return_pct=(
                "net_return_pct",
                "mean"
            ),
        )
        .reset_index()
    )

    daily["losses"] = (
        daily["trades"]
        - daily["wins"]
    )

    daily["win_rate_pct"] = (
        daily["wins"]
        / daily["trades"]
        * 100
    )

    daily = daily.sort_values(
        "trade_date"
    )

    daily.to_csv(
        OUTPUT_DAILY,
        index=False
    )


# ============================================================
# HTML REPORT
# ============================================================

def create_html_report(trades_df):

    html_path = RESULTS_DIR / "index.html"

    if trades_df.empty:

        html = """
<!DOCTYPE html>
<html>
<head>
<title>Backtest Results</title>
</head>
<body>
<h1>NSE 3-Minute Strategy</h1>
<p>No trades generated.</p>
</body>
</html>
"""

        html_path.write_text(
            html,
            encoding="utf-8"
        )

        return

    returns = trades_df[
        "net_return_pct"
    ]

    total_trades = len(
        trades_df
    )

    wins = int(
        trades_df["win"].sum()
    )

    win_rate = (
        wins
        / total_trades
        * 100
    )

    avg_return = (
        returns.mean()
    )

    total_return = (
        returns.sum()
    )

    profit_factor = (
        calculate_profit_factor(
            returns
        )
    )

    max_dd = (
        calculate_max_drawdown(
            returns
        )
    )

    # --------------------------------------------------------
    # Latest trades
    # --------------------------------------------------------

    latest = trades_df.tail(
        500
    )

    table_html = latest.to_html(
        index=False,
        classes="trades"
    )

    html = f"""
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<title>NSE 3-Minute Strategy Backtest</title>

<style>

body {{
    font-family: Arial, sans-serif;
    margin: 30px;
    background: #f5f5f5;
}}

.container {{
    max-width: 1400px;
    margin: auto;
}}

.card {{
    background: white;
    padding: 20px;
    margin-bottom: 20px;
    border-radius: 10px;
}}

.metric {{
    display: inline-block;
    width: 18%;
    min-width: 180px;
    padding: 15px;
    margin: 5px;
    background: #fafafa;
    border-radius: 8px;
}}

.metric h3 {{
    margin: 0;
    font-size: 14px;
}}

.metric p {{
    font-size: 24px;
    margin: 10px 0 0 0;
}}

table {{
    border-collapse: collapse;
    width: 100%;
    background: white;
}}

th, td {{
    padding: 8px;
    border: 1px solid #ddd;
    text-align: right;
}}

th {{
    background: #eee;
}}

h1 {{
    margin-bottom: 5px;
}}

</style>

</head>

<body>

<div class="container">

<div class="card">

<h1>NSE 3-Minute EOD Strategy</h1>

<p>
Next-day 09:15 entry → 15:27 exit
</p>

</div>

<div class="card">

<div class="metric">
<h3>Total Trades</h3>
<p>{total_trades:,}</p>
</div>

<div class="metric">
<h3>Win Rate</h3>
<p>{win_rate:.2f}%</p>
</div>

<div class="metric">
<h3>Average Trade</h3>
<p>{avg_return:.4f}%</p>
</div>

<div class="metric">
<h3>Total Return</h3>
<p>{total_return:.4f}%</p>
</div>

<div class="metric">
<h3>Profit Factor</h3>
<p>{profit_factor:.3f}</p>
</div>

<div class="metric">
<h3>Max Drawdown</h3>
<p>{max_dd:.4f}%</p>
</div>

</div>

<div class="card">

<h2>Strategy Rules</h2>

<ul>

<li>3-minute 15:24 and 15:27 are opposite.</li>

<li>15:24 volume is greater than 15:27.</li>

<li>At least 2 of 15:15, 15:18 and 15:21 match 15:24.</li>

<li>09:15 and 09:18 are both opposite to 15:24.</li>

<li>Next trading day 09:15 open entry.</li>

<li>Direction follows 15:24.</li>

<li>Exit at 15:27 open.</li>

<li>No 1-minute conditions.</li>

<li>No target or stop loss.</li>

</ul>

</div>

<div class="card">

<h2>Trades</h2>

{table_html}

</div>

</div>

</body>

</html>
"""

    html_path.write_text(
        html,
        encoding="utf-8"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("NSE 3-MINUTE STRATEGY BACKTEST")
    print("=" * 70)

    print("\nStrategy:")
    print("15:24 vs 15:27 = opposite")
    print("15:24 volume > 15:27 volume")
    print("At least 2 of 15:15/18/21 = 15:24 trend")
    print("09:15 and 09:18 = opposite to 15:24")
    print("Next day 09:15 OPEN entry")
    print("15:27 OPEN exit")
    print("ALL 1-MINUTE CONDITIONS REMOVED")
    print()

    # --------------------------------------------------------
    # Find files
    # --------------------------------------------------------

    files = find_parquet_files()

    if not files:

        print(
            "\nERROR: No Parquet files found."
        )

        print(
            "Make sure the historical-data repositories "
            "were cloned correctly."
        )

        return

    # --------------------------------------------------------
    # Process stocks
    # --------------------------------------------------------

    all_trades = []

    total = len(files)

    for i, path in enumerate(
        files,
        start=1
    ):

        try:

            trades = process_stock(
                path,
                i,
                total
            )

            if trades:
                all_trades.extend(
                    trades
                )

        except Exception as e:

            print(
                f"ERROR processing "
                f"{path}: {e}"
            )

    # --------------------------------------------------------
    # Convert to DataFrame
    # --------------------------------------------------------

    trades_df = pd.DataFrame(
        all_trades
    )

    if trades_df.empty:

        print(
            "\n=================================================="
        )

        print(
            "BACKTEST FINISHED"
        )

        print(
            "No qualifying trades were found."
        )

        print(
            "=================================================="
        )

        # Still create output files
        trades_df.to_csv(
            OUTPUT_TRADES,
            index=False
        )

        create_stock_summary(
            trades_df
        )

        create_daily_summary(
            trades_df
        )

        create_summary(
            trades_df
        )

        create_html_report(
            trades_df
        )

        return

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    trades_df = trades_df.sort_values(
        [
            "trade_date",
            "symbol",
        ]
    ).reset_index(
        drop=True
    )

    # --------------------------------------------------------
    # Save trades
    # --------------------------------------------------------

    trades_df.to_csv(
        OUTPUT_TRADES,
        index=False
    )

    # --------------------------------------------------------
    # Create reports
    # --------------------------------------------------------

    create_stock_summary(
        trades_df
    )

    create_daily_summary(
        trades_df
    )

    create_summary(
        trades_df
    )

    create_html_report(
        trades_df
    )

    # --------------------------------------------------------
    # Final console output
    # --------------------------------------------------------

    print(
        "\n============================================================"
    )

    print(
        "BACKTEST COMPLETE"
    )

    print(
        "============================================================"
    )

    print(
        f"Trades: {len(trades_df):,}"
    )

    print(
        f"Wins: {int(trades_df['win'].sum()):,}"
    )

    print(
        f"Win rate: "
        f"{trades_df['win'].mean() * 100:.2f}%"
    )

    print(
        f"Average return: "
        f"{trades_df['net_return_pct'].mean():.4f}%"
    )

    print(
        f"Total return: "
        f"{trades_df['net_return_pct'].sum():.4f}%"
    )

    print(
        f"Profit factor: "
        f"{calculate_profit_factor(trades_df['net_return_pct']):.4f}"
    )

    print(
        f"Max drawdown: "
        f"{calculate_max_drawdown(trades_df['net_return_pct']):.4f}%"
    )

    print(
        "\nResults saved to:"
    )

    print(
        f"  {OUTPUT_TRADES}"
    )

    print(
        f"  {OUTPUT_STOCKS}"
    )

    print(
        f"  {OUTPUT_DAILY}"
    )

    print(
        f"  {OUTPUT_SUMMARY}"
    )

    print(
        f"  {RESULTS_DIR / 'index.html'}"
    )

    print(
        "============================================================"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
