"""
============================================================
NSE FAST MAIN SCANNER
============================================================

STRATEGY
--------

SIGNAL DAY
----------

3-MINUTE CONDITIONS:

1. 15:24 and 15:27 must be opposite trends.

2. 15:24 volume > 15:27 volume.

3. 09:15 and 09:18 must BOTH be opposite
   to the 15:24 trend.


1-MINUTE CONDITIONS:

4. 15:28 and 15:29 must be opposite trends.

5. 15:28 volume > 15:29 volume.

6. 1-minute 15:28 trend must be the SAME as
   3-minute 15:24 trend.


TRADE
-----

Direction:
    3-minute 15:24 trend

Entry:
    NEXT TRADING DAY 09:15 OPEN

Exit:
    NEXT TRADING DAY 15:27 OPEN


NO OTHER CONDITIONS ARE USED.


============================================================
FAST DESIGN
============================================================

- Reads Parquet files recursively.
- Processes each stock independently.
- Uses 1-minute data as the base.
- Builds 3-minute candles once.
- Does not repeatedly resample the same data.
- Does not use unnecessary indicators.
- Only required timestamps are evaluated.
- Results are written to CSV and HTML.
"""

from __future__ import annotations

import os
import glob
import warnings
from pathlib import Path
from datetime import time

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
RESULTS_DIR.mkdir(
    parents=True,
    exist_ok=True
)

TRADES_FILE = (
    RESULTS_DIR / "scanner_signals.csv"
)

STOCK_SUMMARY_FILE = (
    RESULTS_DIR / "stock_summary.csv"
)

HTML_FILE = (
    RESULTS_DIR / "index.html"
)

SUMMARY_FILE = (
    RESULTS_DIR / "summary.txt"
)


# ============================================================
# REQUIRED TIMES
# ============================================================

# 3-minute candles
THREE_MIN_TIMES = {
    "09:15",
    "09:18",
    "15:24",
    "15:27",
}

# 1-minute candles
ONE_MIN_TIMES = {
    "15:28",
    "15:29",
}


# ============================================================
# TREND
# ============================================================

def get_trend(open_price, close_price):
    """
    1  = GREEN / bullish
    -1 = RED / bearish
    0  = DOJI
    """

    if (
        pd.isna(open_price)
        or pd.isna(close_price)
    ):
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

    print(
        "\nSearching for Parquet files..."
    )

    for directory in DATA_DIRS:

        if not os.path.isdir(directory):

            print(
                f"Not found: {directory}"
            )

            continue

        found = glob.glob(
            os.path.join(
                directory,
                "**",
                "*.parquet"
            ),
            recursive=True
        )

        print(
            f"{directory}: "
            f"{len(found):,} files"
        )

        files.extend(found)

    files = sorted(
        set(files)
    )

    print(
        f"Total files: {len(files):,}"
    )

    return files


# ============================================================
# SYMBOL
# ============================================================

def get_symbol(path):

    symbol = Path(
        path
    ).stem.upper()

    for suffix in (
        "_1MIN",
        "_1MINUTE",
        "_MINUTE",
        "_DATA",
        "_HISTORICAL",
    ):

        if symbol.endswith(suffix):

            symbol = symbol[
                :-len(suffix)
            ]

    return symbol


# ============================================================
# NORMALIZE COLUMNS
# ============================================================

def normalize_columns(df):

    df = df.copy()

    # --------------------------------------------------------
    # Flatten MultiIndex
    # --------------------------------------------------------

    if isinstance(
        df.columns,
        pd.MultiIndex
    ):

        columns = []

        for col in df.columns:

            parts = []

            for value in col:

                value = str(value)

                if value.lower() != "nan":

                    parts.append(value)

            columns.append(
                "_".join(parts)
            )

        df.columns = columns

    # --------------------------------------------------------
    # Rename columns
    # --------------------------------------------------------

    rename = {}

    for column in df.columns:

        name = str(
            column
        ).strip().lower()

        if name in (
            "datetime",
            "date_time",
            "timestamp",
            "time",
            "date",
        ):

            rename[column] = "datetime"

        elif name in (
            "open",
            "open_price",
        ):

            rename[column] = "open"

        elif name in (
            "high",
            "high_price",
        ):

            rename[column] = "high"

        elif name in (
            "low",
            "low_price",
        ):

            rename[column] = "low"

        elif name in (
            "close",
            "close_price",
            "adj close",
            "adj_close",
        ):

            rename[column] = "close"

        elif name in (
            "volume",
            "vol",
        ):

            rename[column] = "volume"

    df = df.rename(
        columns=rename
    )

    return df


# ============================================================
# LOAD DATA
# ============================================================

def load_data(path):

    try:

        df = pd.read_parquet(
            path
        )

    except Exception as error:

        print(
            f"Read error: {path}"
        )

        print(error)

        return None

    df = normalize_columns(
        df
    )

    required = [
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    for column in required:

        if column not in df.columns:

            return None

    # ========================================================
    # DATETIME
    # ========================================================

    if "datetime" in df.columns:

        dt = pd.to_datetime(
            df["datetime"],
            errors="coerce"
        )

    elif isinstance(
        df.index,
        pd.DatetimeIndex
    ):

        dt = pd.Series(
            pd.to_datetime(
                df.index,
                errors="coerce"
            ),
            index=df.index
        )

    else:

        return None

    # Remove timezone
    try:

        if dt.dt.tz is not None:

            dt = (
                dt
                .dt
                .tz_localize(None)
            )

    except Exception:
        pass

    df["datetime"] = dt

    # ========================================================
    # NUMERIC
    # ========================================================

    for column in required:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    # ========================================================
    # CLEAN
    # ========================================================

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

    df = df.sort_values(
        "datetime"
    )

    df = df.drop_duplicates(
        subset="datetime",
        keep="last"
    )

    df = df.set_index(
        "datetime"
    )

    return df


# ============================================================
# CREATE 3-MINUTE CANDLES
# ============================================================

def create_3m(df):

    """
    Build NSE 3-minute candles.

    Example:

    09:15 = 09:15-09:17
    09:18 = 09:18-09:20
    ...
    15:24 = 15:24-15:26
    15:27 = 15:27-15:29
    """

    data = df[
        (df.index.time >= time(9, 15))
        &
        (df.index.time <= time(15, 29))
    ]

    if data.empty:

        return pd.DataFrame()

    candles = (
        data
        .resample(
            "3min",
            origin="start_day",
            offset="15min",
            label="left",
            closed="left",
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

    # Keep only NSE session
    candles = candles[
        (candles.index.time >= time(9, 15))
        &
        (candles.index.time <= time(15, 27))
    ]

    return candles


# ============================================================
# GET CANDLE
# ============================================================

def get_candle(
    df,
    hhmm
):

    if df is None or df.empty:

        return None

    mask = (
        df.index.strftime("%H:%M")
        == hhmm
    )

    if not mask.any():

        return None

    return df.loc[
        mask
    ].iloc[0]


# ============================================================
# GET DAY
# ============================================================

def get_day(
    df,
    date_value
):

    return df[
        df.index.date
        == date_value
    ]


# ============================================================
# CHECK SIGNAL
# ============================================================

def check_signal(
    day_1m,
    day_3m
):

    # ========================================================
    # REQUIRED 3-MIN CANDLES
    # ========================================================

    c0915 = get_candle(
        day_3m,
        "09:15"
    )

    c0918 = get_candle(
        day_3m,
        "09:18"
    )

    c1524 = get_candle(
        day_3m,
        "15:24"
    )

    c1527 = get_candle(
        day_3m,
        "15:27"
    )

    if any(
        candle is None
        for candle in (
            c0915,
            c0918,
            c1524,
            c1527,
        )
    ):

        return None

    # ========================================================
    # REQUIRED 1-MIN CANDLES
    # ========================================================

    c1528 = get_candle(
        day_1m,
        "15:28"
    )

    c1529 = get_candle(
        day_1m,
        "15:29"
    )

    if c1528 is None:

        return None

    if c1529 is None:

        return None

    # ========================================================
    # TRENDS
    # ========================================================

    trend_0915 = get_trend(
        c0915["open"],
        c0915["close"]
    )

    trend_0918 = get_trend(
        c0918["open"],
        c0918["close"]
    )

    trend_1524 = get_trend(
        c1524["open"],
        c1524["close"]
    )

    trend_1527 = get_trend(
        c1527["open"],
        c1527["close"]
    )

    trend_1528 = get_trend(
        c1528["open"],
        c1528["close"]
    )

    trend_1529 = get_trend(
        c1529["open"],
        c1529["close"]
    )

    # ========================================================
    # INVALID / DOJI CHECK
    # ========================================================

    if trend_1524 == 0:

        return None

    if trend_1527 == 0:

        return None

    if trend_0915 == 0:

        return None

    if trend_0918 == 0:

        return None

    if trend_1528 == 0:

        return None

    if trend_1529 == 0:

        return None

    # ========================================================
    # CONDITION 1
    #
    # 15:24 AND 15:27 OPPOSITE
    # ========================================================

    if (
        trend_1524
        != -trend_1527
    ):

        return None

    # ========================================================
    # CONDITION 2
    #
    # 15:24 VOLUME > 15:27 VOLUME
    # ========================================================

    volume_1524 = float(
        c1524["volume"]
    )

    volume_1527 = float(
        c1527["volume"]
    )

    if not (
        volume_1524
        > volume_1527
    ):

        return None

    # ========================================================
    # CONDITION 3
    #
    # MORNING 09:15 AND 09:18
    # OPPOSITE TO 15:24
    # ========================================================

    if (
        trend_0915
        != -trend_1524
    ):

        return None

    if (
        trend_0918
        != -trend_1524
    ):

        return None

    # ========================================================
    # CONDITION 4
    #
    # 1-MIN 15:28 AND 15:29 OPPOSITE
    # ========================================================

    if (
        trend_1528
        != -trend_1529
    ):

        return None

    # ========================================================
    # CONDITION 5
    #
    # 1-MIN 15:28 VOLUME > 15:29
    # ========================================================

    volume_1528 = float(
        c1528["volume"]
    )

    volume_1529 = float(
        c1529["volume"]
    )

    if not (
        volume_1528
        > volume_1529
    ):

        return None

    # ========================================================
    # CONDITION 6
    #
    # 1-MIN 15:28 SAME AS 3-MIN 15:24
    # ========================================================

    if (
        trend_1528
        != trend_1524
    ):

        return None

    # ========================================================
    # SIGNAL PASSED
    # ========================================================

    return {
        "direction": trend_1524,

        "trend_0915": trend_0915,
        "trend_0918": trend_0918,

        "trend_1524": trend_1524,
        "trend_1527": trend_1527,

        "trend_1528": trend_1528,
        "trend_1529": trend_1529,

        "volume_1524": volume_1524,
        "volume_1527": volume_1527,

        "volume_1528": volume_1528,
        "volume_1529": volume_1529,

        "volume_ratio_3m": (
            volume_1524
            / volume_1527
            if volume_1527 > 0
            else np.nan
        ),

        "volume_ratio_1m": (
            volume_1528
            / volume_1529
            if volume_1529 > 0
            else np.nan
        ),
    }


# ============================================================
# PROCESS STOCK
# ============================================================

def process_stock(
    path,
    number,
    total
):

    symbol = get_symbol(
        path
    )

    print(
        f"[{number}/{total}] {symbol}"
    )

    # ========================================================
    # LOAD
    # ========================================================

    df = load_data(
        path
    )

    if df is None:

        return []

    # ========================================================
    # CREATE 3-MIN ONCE
    # ========================================================

    df_3m = create_3m(
        df
    )

    if df_3m.empty:

        return []

    # ========================================================
    # TRADING DAYS
    # ========================================================

    days = sorted(
        set(df.index.date)
    )

    if len(days) < 3:

        return []

    trades = []

    # ========================================================
    # LOOP
    #
    # i-1 = signal day
    # i   = trade day
    #
    # We start at 1 because we need the next day.
    # ========================================================

    for i in range(
        0,
        len(days) - 1
    ):

        signal_date = days[i]
        trade_date = days[i + 1]

        # ----------------------------------------------------
        # Signal day
        # ----------------------------------------------------

        day_1m = get_day(
            df,
            signal_date
        )

        day_3m = get_day(
            df_3m,
            signal_date
        )

        if day_1m.empty:

            continue

        if day_3m.empty:

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
        # TRADE DAY
        # ====================================================

        trade_day = get_day(
            df,
            trade_date
        )

        if trade_day.empty:

            continue

        # ====================================================
        # ENTRY
        # ====================================================

        entry = get_candle(
            trade_day,
            ENTRY_TIME
        )

        # ====================================================
        # EXIT
        # ====================================================

        exit_candle = get_candle(
            trade_day,
            EXIT_TIME
        )

        if entry is None:

            continue

        if exit_candle is None:

            continue

        entry_price = float(
            entry["open"]
        )

        exit_price = float(
            exit_candle["open"]
        )

        if (
            not np.isfinite(
                entry_price
            )
            or
            not np.isfinite(
                exit_price
            )
            or
            entry_price <= 0
        ):

            continue

        # ====================================================
        # RETURN
        # ====================================================

        direction = signal[
            "direction"
        ]

        if direction == 1:

            # LONG
            return_pct = (
                (
                    exit_price
                    - entry_price
                )
                / entry_price
            ) * 100

            direction_name = "LONG"

        else:

            # SHORT
            return_pct = (
                (
                    entry_price
                    - exit_price
                )
                / entry_price
            ) * 100

            direction_name = "SHORT"

        # ====================================================
        # STORE
        # ====================================================

        trades.append(
            {
                "symbol":
                    symbol,

                "signal_date":
                    str(signal_date),

                "trade_date":
                    str(trade_date),

                "direction":
                    direction_name,

                # --------------------------------------------
                # Morning
                # --------------------------------------------

                "09:15_trend":
                    trend_name(
                        signal["trend_0915"]
                    ),

                "09:18_trend":
                    trend_name(
                        signal["trend_0918"]
                    ),

                # --------------------------------------------
                # 3-minute
                # --------------------------------------------

                "15:24_trend":
                    trend_name(
                        signal["trend_1524"]
                    ),

                "15:27_trend":
                    trend_name(
                        signal["trend_1527"]
                    ),

                "15:24_volume":
                    signal["volume_1524"],

                "15:27_volume":
                    signal["volume_1527"],

                "3m_volume_ratio":
                    signal["volume_ratio_3m"],

                # --------------------------------------------
                # 1-minute
                # --------------------------------------------

                "15:28_trend":
                    trend_name(
                        signal["trend_1528"]
                    ),

                "15:29_trend":
                    trend_name(
                        signal["trend_1529"]
                    ),

                "15:28_volume":
                    signal["volume_1528"],

                "15:29_volume":
                    signal["volume_1529"],

                "1m_volume_ratio":
                    signal["volume_ratio_1m"],

                # --------------------------------------------
                # Trade
                # --------------------------------------------

                "entry_time":
                    ENTRY_TIME,

                "entry_price":
                    entry_price,

                "exit_time":
                    EXIT_TIME,

                "exit_price":
                    exit_price,

                "return_pct":
                    return_pct,

                "win":
                    int(
                        return_pct > 0
                    ),
            }
        )

    return trades


# ============================================================
# TREND NAME
# ============================================================

def trend_name(
    trend
):

    if trend == 1:

        return "GREEN"

    if trend == -1:

        return "RED"

    return "DOJI"


# ============================================================
# STOCK SUMMARY
# ============================================================

def create_stock_summary(
    trades_df
):

    if trades_df.empty:

        pd.DataFrame().to_csv(
            STOCK_SUMMARY_FILE,
            index=False
        )

        return

    rows = []

    for symbol, group in (
        trades_df.groupby(
            "symbol"
        )
    ):

        returns = group[
            "return_pct"
        ]

        count = len(
            group
        )

        wins = int(
            group["win"].sum()
        )

        rows.append(
            {
                "symbol":
                    symbol,

                "trades":
                    count,

                "wins":
                    wins,

                "losses":
                    count - wins,

                "win_rate_pct":
                    wins / count * 100,

                "avg_return_pct":
                    returns.mean(),

                "total_return_pct":
                    returns.sum(),

                "best_trade_pct":
                    returns.max(),

                "worst_trade_pct":
                    returns.min(),
            }
        )

    summary = pd.DataFrame(
        rows
    )

    summary = summary.sort_values(
        [
            "win_rate_pct",
            "total_return_pct",
        ],
        ascending=False
    )

    summary.to_csv(
        STOCK_SUMMARY_FILE,
        index=False
    )


# ============================================================
# HTML REPORT
# ============================================================

def create_html(
    trades_df
):

    if trades_df.empty:

        html = """
<!DOCTYPE html>

<html>

<head>
<meta charset="UTF-8">
<title>NSE Scanner</title>
</head>

<body>

<h1>NSE Scanner</h1>

<p>
No qualifying signals found.
</p>

</body>

</html>
"""

        HTML_FILE.write_text(
            html,
            encoding="utf-8"
        )

        return

    total = len(
        trades_df
    )

    wins = int(
        trades_df["win"].sum()
    )

    win_rate = (
        wins / total * 100
    )

    average_return = (
        trades_df[
            "return_pct"
        ].mean()
    )

    total_return = (
        trades_df[
            "return_pct"
        ].sum()
    )

    latest = trades_df.tail(
        1000
    )

    table = latest.to_html(
        index=False
    )

    html = f"""
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<title>
NSE Main Scanner
</title>

<style>

body {{
    font-family: Arial, sans-serif;
    margin: 25px;
    background: #f5f5f5;
}}

.card {{
    background: white;
    padding: 20px;
    margin-bottom: 20px;
    border-radius: 10px;
}}

.metric {{
    display: inline-block;
    padding: 15px;
    margin: 5px;
    min-width: 160px;
    background: #fafafa;
    border-radius: 8px;
}}

.metric h3 {{
    margin: 0;
    font-size: 14px;
}}

.metric p {{
    font-size: 24px;
    margin: 8px 0 0 0;
}}

table {{
    border-collapse: collapse;
    width: 100%;
    background: white;
}}

th, td {{
    border: 1px solid #ddd;
    padding: 7px;
    white-space: nowrap;
}}

th {{
    background: #eee;
}}

</style>

</head>

<body>

<div class="card">

<h1>
NSE Main Scanner
</h1>

<p>
3-minute + 1-minute confirmation
</p>

</div>


<div class="card">

<div class="metric">

<h3>
Signals
</h3>

<p>
{total:,}
</p>

</div>


<div class="metric">

<h3>
Winning Trades
</h3>

<p>
{wins:,}
</p>

</div>


<div class="metric">

<h3>
Win Rate
</h3>

<p>
{win_rate:.2f}%
</p>

</div>


<div class="metric">

<h3>
Average Return
</h3>

<p>
{average_return:.4f}%
</p>

</div>


<div class="metric">

<h3>
Total Return
</h3>

<p>
{total_return:.4f}%
</p>

</div>

</div>


<div class="card">

<h2>
Strategy Conditions
</h2>

<ul>

<li>
3-min 15:24 and 15:27 are opposite.
</li>

<li>
3-min 15:24 volume &gt; 15:27 volume.
</li>

<li>
3-min 09:15 is opposite to 15:24.
</li>

<li>
3-min 09:18 is opposite to 15:24.
</li>

<li>
1-min 15:28 and 15:29 are opposite.
</li>

<li>
1-min 15:28 volume &gt; 15:29 volume.
</li>

<li>
1-min 15:28 matches 3-min 15:24.
</li>

<li>
Next-day 09:15 open entry.
</li>

<li>
15:27 open exit.
</li>

</ul>

</div>


<div class="card">

<h2>
Signals / Trades
</h2>

{table}

</div>

</body>

</html>
"""

    HTML_FILE.write_text(
        html,
        encoding="utf-8"
    )


# ============================================================
# SUMMARY
# ============================================================

def create_summary(
    trades_df
):

    if trades_df.empty:

        text = """
============================================================
NSE MAIN SCANNER
============================================================

No qualifying signals found.

============================================================
"""

        SUMMARY_FILE.write_text(
            text,
            encoding="utf-8"
        )

        print(text)

        return

    total = len(
        trades_df
    )

    wins = int(
        trades_df["win"].sum()
    )

    losses = (
        total - wins
    )

    returns = trades_df[
        "return_pct"
    ]

    win_rate = (
        wins
        / total
        * 100
    )

    average = (
        returns.mean()
    )

    total_return = (
        returns.sum()
    )

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

        profit_factor = float(
            "inf"
        )

    text = f"""
============================================================
NSE MAIN SCANNER RESULTS
============================================================

STRATEGY
------------------------------------------------------------

3-MINUTE:

15:24 vs 15:27:
    Opposite trend

15:24 volume:
    Greater than 15:27

09:15:
    Opposite to 15:24

09:18:
    Opposite to 15:24


1-MINUTE:

15:28 vs 15:29:
    Opposite trend

15:28 volume:
    Greater than 15:29

15:28:
    Same trend as 3-min 15:24


TRADE:

Next trading day 09:15:
    Entry

Direction:
    3-min 15:24

15:27:
    Exit

------------------------------------------------------------
RESULTS
------------------------------------------------------------

Total trades       : {total:,}

Winning trades     : {wins:,}

Losing trades      : {losses:,}

Win rate           : {win_rate:.2f}%

Average return     : {average:.4f}%

Total return       : {total_return:.4f}%

Profit factor      : {profit_factor:.4f}%

Best trade         : {returns.max():.4f}%

Worst trade        : {returns.min():.4f}%

============================================================
"""

    SUMMARY_FILE.write_text(
        text,
        encoding="utf-8"
    )

    print(text)


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 70)
    print("NSE FAST MAIN SCANNER")
    print("=" * 70)

    print()
    print("3m 15:24 opposite 15:27")
    print("3m 15:24 volume > 15:27")
    print("3m 09:15 opposite 15:24")
    print("3m 09:18 opposite 15:24")
    print("1m 15:28 opposite 15:29")
    print("1m 15:28 volume > 15:29")
    print("1m 15:28 = 3m 15:24")
    print("Next day 09:15 entry")
    print("15:27 exit")
    print()

    # ========================================================
    # FIND FILES
    # ========================================================

    files = find_parquet_files()

    if not files:

        print(
            "\nERROR: No Parquet files found."
        )

        return

    # ========================================================
    # PROCESS
    # ========================================================

    all_signals = []

    total_files = len(
        files
    )

    for number, path in enumerate(
        files,
        start=1
    ):

        try:

            results = process_stock(
                path,
                number,
                total_files
            )

            if results:

                all_signals.extend(
                    results
                )

        except Exception as error:

            print(
                f"ERROR: {path}"
            )

            print(
                repr(error)
            )

    # ========================================================
    # DATAFRAME
    # ========================================================

    signals_df = pd.DataFrame(
        all_signals
    )

    # ========================================================
    # SAVE
    # ========================================================

    signals_df.to_csv(
        TRADES_FILE,
        index=False
    )

    create_stock_summary(
        signals_df
    )

    create_summary(
        signals_df
    )

    create_html(
        signals_df
    )

    # ========================================================
    # FINAL
    # ========================================================

    print()
    print("=" * 70)
    print("SCANNER COMPLETE")
    print("=" * 70)

    if signals_df.empty:

        print(
            "No qualifying signals."
        )

    else:

        total = len(
            signals_df
        )

        wins = int(
            signals_df["win"].sum()
        )

        print(
            f"Signals/trades: {total:,}"
        )

        print(
            f"Wins: {wins:,}"
        )

        print(
            f"Losses: {total - wins:,}"
        )

        print(
            f"Win rate: "
            f"{wins / total * 100:.2f}%"
        )

        print(
            f"Average return: "
            f"{signals_df['return_pct'].mean():.4f}%"
        )

        print(
            f"Total return: "
            f"{signals_df['return_pct'].sum():.4f}%"
        )

    print()
    print("Output files:")

    print(
        f"  {TRADES_FILE}"
    )

    print(
        f"  {STOCK_SUMMARY_FILE}"
    )

    print(
        f"  {SUMMARY_FILE}"
    )

    print(
        f"  {HTML_FILE}"
    )

    print("=" * 70)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
