# =============================================================================
# NSE SCANNER BACKTEST
# =============================================================================
#
# CORE LOGIC = USER'S CURRENT NSE SCANNER
#
# PREVIOUS TRADING DAY
#
# 3-MIN:
#   15:24 and 15:27 opposite
#   15:27 volume > 15:24
#   09:15 and 09:18 both match 15:27
#
# 1-MIN:
#   15:28 and 15:29 opposite
#   15:28 volume > 15:29
#   15:28 matches 3-MIN 15:27
#   09:15 and 09:16 both match 15:28
#
# NEXT TRADING DAY:
#
#   ENTRY = 09:15 OPEN
#   EXIT  = 15:27 OPEN
#
# =============================================================================


import os
import glob
import time
import traceback

import numpy as np
import pandas as pd


# =============================================================================
# SETTINGS
# =============================================================================

START_DATE = os.getenv(
    "START_DATE",
    "2018-01-01"
)

END_DATE = os.getenv(
    "END_DATE",
    "2026-12-31"
)

TRADE_SIDE = os.getenv(
    "TRADE_SIDE",
    "BOTH"
).upper()


# Round-trip cost in percentage points.

ROUND_TRIP_COST = 0.10


# Minimum required 1-minute candles.

REQUIRED_MINUTES = {

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

}


# =============================================================================
# DIRECTORIES
# =============================================================================

RESULTS_DIR = "results"

os.makedirs(
    RESULTS_DIR,
    exist_ok=True
)


# =============================================================================
# HELPERS
# =============================================================================

def trend(open_price, close_price):

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


def direction_name(value):

    if value == 1:
        return "LONG"

    if value == -1:
        return "SHORT"

    return None


def safe_float(value):

    try:

        value = float(value)

        if np.isfinite(value):

            return value

    except Exception:

        pass

    return np.nan


# =============================================================================
# LOAD PARQUET
# =============================================================================

def load_parquet(path):

    df = pd.read_parquet(
        path
    )


    if df.empty:

        return None


    # ---------------------------------------------------------
    # NORMALIZE COLUMN NAMES
    # ---------------------------------------------------------

    rename = {}

    for column in df.columns:

        c = str(column).strip().lower()


        if c in (
            "datetime",
            "date",
            "timestamp",
            "time"
        ):

            rename[column] = "datetime"


        elif c == "open":

            rename[column] = "open"


        elif c == "high":

            rename[column] = "high"


        elif c == "low":

            rename[column] = "low"


        elif c == "close":

            rename[column] = "close"


        elif c in (
            "volume",
            "vol"
        ):

            rename[column] = "volume"


    df = df.rename(
        columns=rename
    )


    required = [

        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume"

    ]


    missing = [

        c
        for c in required
        if c not in df.columns

    ]


    if missing:

        raise ValueError(
            f"Missing columns "
            f"{missing}"
        )


    # ---------------------------------------------------------
    # DATETIME
    # ---------------------------------------------------------

    df["datetime"] = pd.to_datetime(
        df["datetime"],
        errors="coerce"
    )


    df = df.dropna(
        subset=[
            "datetime"
        ]
    )


    # ---------------------------------------------------------
    # TIMEZONE
    # ---------------------------------------------------------

    if getattr(
        df["datetime"].dt,
        "tz",
        None
    ) is not None:

        df["datetime"] = (
            df["datetime"]
            .dt.tz_convert(
                "Asia/Kolkata"
            )
            .dt.tz_localize(
                None
            )
        )


    # ---------------------------------------------------------
    # NUMERIC
    # ---------------------------------------------------------

    for column in [

        "open",
        "high",
        "low",
        "close",
        "volume"

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
            "volume"
        ]
    )


    # ---------------------------------------------------------
    # NSE SESSION
    # ---------------------------------------------------------

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


    df = df[
        (
            df["time"] >= "09:15"
        )
        &
        (
            df["time"] <= "15:29"
        )
    ].copy()


    # ---------------------------------------------------------
    # DATE FILTER
    # ---------------------------------------------------------

    df = df[
        (
            df["date"]
            >= pd.to_datetime(
                START_DATE
            ).date()
        )
        &
        (
            df["date"]
            <= pd.to_datetime(
                END_DATE
            ).date()
        )
    ].copy()


    if df.empty:

        return None


    # ---------------------------------------------------------
    # SORT / DUPLICATES
    # ---------------------------------------------------------

    df = df.sort_values(
        "datetime"
    )


    df = df.drop_duplicates(
        subset=[
            "datetime"
        ],
        keep="last"
    )


    df = df.reset_index(
        drop=True
    )


    return df


# =============================================================================
# CREATE EXACT 3-MIN CANDLES
# =============================================================================

def create_3m(df):

    x = df.copy()


    minutes = (

        x["datetime"].dt.hour * 60
        +
        x["datetime"].dt.minute

    )


    market_open = (
        9 * 60 + 15
    )


    minutes_from_open = (
        minutes - market_open
    )


    x["bucket"] = (
        minutes_from_open // 3
    )


    candle_start = (

        market_open
        +
        x["bucket"] * 3

    )


    hour = (
        candle_start // 60
    )


    minute = (
        candle_start % 60
    )


    x["candle_time"] = (

        hour.astype(int)
        .astype(str)
        .str.zfill(2)

        +

        ":"

        +

        minute.astype(int)
        .astype(str)
        .str.zfill(2)

    )


    candles = (

        x.groupby(
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

            source_bars=(
                "close",
                "count"
            )

        )

        .reset_index()

    )


    return candles


# =============================================================================
# BUILD LOOKUPS
# =============================================================================

def build_1m_lookup(df):

    lookup = {}


    for row in df.itertuples(
        index=False
    ):

        lookup[
            (
                row.date,
                row.time
            )
        ] = row


    return lookup


def build_3m_lookup(candles):

    lookup = {}


    for row in candles.itertuples(
        index=False
    ):

        # IMPORTANT:
        #
        # A valid 3-minute candle must
        # contain all 3 one-minute bars.
        #

        if row.source_bars != 3:

            continue


        lookup[
            (
                row.date,
                row.candle_time
            )
        ] = row


    return lookup


# =============================================================================
# SIGNAL DETECTION
# =============================================================================

def find_signals(
    df,
    candles_3m
):

    lookup_1m = build_1m_lookup(
        df
    )

    lookup_3m = build_3m_lookup(
        candles_3m
    )


    dates = sorted(
        df["date"]
        .unique()
    )


    signals = []


    diagnostics = {

        "days_seen": 0,

        "missing_3m": 0,

        "missing_1m": 0,

        "cond1": 0,

        "cond2": 0,

        "cond3": 0,

        "cond4": 0,

        "cond5": 0,

        "signals": 0,

        "long_signals": 0,

        "short_signals": 0,

    }


    # ---------------------------------------------------------
    # PREVIOUS DAY -> NEXT TRADING DAY
    # ---------------------------------------------------------

    for i in range(
        1,
        len(dates)
    ):

        signal_date = (
            dates[i - 1]
        )

        trade_date = (
            dates[i]
        )


        diagnostics[
            "days_seen"
        ] += 1


        # =====================================================
        # 3-MIN CANDLES
        # =====================================================

        c1524 = lookup_3m.get(
            (
                signal_date,
                "15:24"
            )
        )


        c1527 = lookup_3m.get(
            (
                signal_date,
                "15:27"
            )
        )


        c0915 = lookup_3m.get(
            (
                signal_date,
                "09:15"
            )
        )


        c0918 = lookup_3m.get(
            (
                signal_date,
                "09:18"
            )
        )


        if any(
            x is None
            for x in [
                c1524,
                c1527,
                c0915,
                c0918
            ]
        ):

            diagnostics[
                "missing_3m"
            ] += 1

            continue


        # =====================================================
        # 3-MIN DIRECTIONS
        # =====================================================

        t1524 = trend(
            c1524.open,
            c1524.close
        )


        t1527 = trend(
            c1527.open,
            c1527.close
        )


        t0915 = trend(
            c0915.open,
            c0915.close
        )


        t0918 = trend(
            c0918.open,
            c0918.close
        )


        if 0 in [
            t1524,
            t1527,
            t0915,
            t0918
        ]:

            continue


        # =====================================================
        # CONDITION 1
        #
        # 15:24 / 15:27 OPPOSITE
        # 15:27 VOLUME > 15:24
        # =====================================================

        cond1 = (

            t1524 != t1527

            and

            c1527.volume
            >
            c1524.volume

        )


        if not cond1:

            continue


        diagnostics[
            "cond1"
        ] += 1


        # =====================================================
        # CONDITION 2
        #
        # 09:15 AND 09:18
        # BOTH MATCH 15:27
        # =====================================================

        cond2 = (

            t0915 == t1527

            and

            t0918 == t1527

        )


        if not cond2:

            continue


        diagnostics[
            "cond2"
        ] += 1


        # =====================================================
        # 1-MIN CANDLES
        # =====================================================

        c1528 = lookup_1m.get(
            (
                signal_date,
                "15:28"
            )
        )


        c1529 = lookup_1m.get(
            (
                signal_date,
                "15:29"
            )
        )


        c1_0915 = lookup_1m.get(
            (
                signal_date,
                "09:15"
            )
        )


        c1_0916 = lookup_1m.get(
            (
                signal_date,
                "09:16"
            )
        )


        if any(
            x is None
            for x in [
                c1528,
                c1529,
                c1_0915,
                c1_0916
            ]
        ):

            diagnostics[
                "missing_1m"
            ] += 1

            continue


        # =====================================================
        # 1-MIN DIRECTIONS
        # =====================================================

        t1528 = trend(
            c1528.open,
            c1528.close
        )


        t1529 = trend(
            c1529.open,
            c1529.close
        )


        t1_0915 = trend(
            c1_0915.open,
            c1_0915.close
        )


        t1_0916 = trend(
            c1_0916.open,
            c1_0916.close
        )


        if 0 in [
            t1528,
            t1529,
            t1_0915,
            t1_0916
        ]:

            continue


        # =====================================================
        # CONDITION 3
        #
        # 15:28 / 15:29 OPPOSITE
        # 15:28 VOLUME > 15:29
        # =====================================================

        cond3 = (

            t1528 != t1529

            and

            c1528.volume
            >
            c1529.volume

        )


        if not cond3:

            continue


        diagnostics[
            "cond3"
        ] += 1


        # =====================================================
        # CONDITION 4
        #
        # 1-MIN 15:28 = 3-MIN 15:27
        # =====================================================

        cond4 = (
            t1528 == t1527
        )


        if not cond4:

            continue


        diagnostics[
            "cond4"
        ] += 1


        # =====================================================
        # CONDITION 5
        #
        # 1-MIN 09:15 AND 09:16
        # BOTH MATCH 15:28
        # =====================================================

        cond5 = (

            t1_0915 == t1528

            and

            t1_0916 == t1528

        )


        if not cond5:

            continue


        diagnostics[
            "cond5"
        ] += 1


        # =====================================================
        # FINAL SIGNAL
        # =====================================================

        side = direction_name(
            t1527
        )


        diagnostics[
            "signals"
        ] += 1


        if side == "LONG":

            diagnostics[
                "long_signals"
            ] += 1


        else:

            diagnostics[
                "short_signals"
            ] += 1


        signals.append({

            "signal_date":
                signal_date,

            "trade_date":
                trade_date,

            "direction":
                side,

            "signal_trend":
                t1527,

        })


    return (
        signals,
        diagnostics
    )


# =============================================================================
# EXECUTE SIGNALS
# =============================================================================

def execute_trades(
    df,
    signals
):

    lookup = build_1m_lookup(
        df
    )


    trades = []


    for signal in signals:

        side = (
            signal["signal_trend"]
        )


        trade_date = (
            signal["trade_date"]
        )


        # -----------------------------------------------------
        # NEXT DAY 09:15 OPEN
        # -----------------------------------------------------

        entry = lookup.get(
            (
                trade_date,
                "09:15"
            )
        )


        # -----------------------------------------------------
        # NEXT DAY 15:27 OPEN
        # -----------------------------------------------------

        exit_ = lookup.get(
            (
                trade_date,
                "15:27"
            )
        )


        if entry is None:
            continue


        if exit_ is None:
            continue


        entry_price = safe_float(
            entry.open
        )


        exit_price = safe_float(
            exit_.open
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


        # -----------------------------------------------------
        # RETURN
        # -----------------------------------------------------

        if side == 1:

            gross = (

                (
                    exit_price
                    -
                    entry_price
                )
                /
                entry_price
                *
                100

            )

        else:

            gross = (

                (
                    entry_price
                    -
                    exit_price
                )
                /
                entry_price
                *
                100

            )


        net = (
            gross
            -
            ROUND_TRIP_COST
        )


        trades.append({

            "signal_date":
                signal["signal_date"],

            "trade_date":
                trade_date,

            "direction":
                direction_name(
                    side
                ),

            "entry":
                entry_price,

            "exit":
                exit_price,

            "gross_return_pct":
                gross,

            "net_return_pct":
                net,

        })


    return trades


# =============================================================================
# PROCESS ONE STOCK
# =============================================================================

def process_stock(
    path
):

    symbol = os.path.basename(
        path
    )

    symbol = os.path.splitext(
        symbol
    )[0]


    try:

        df = load_parquet(
            path
        )


        if df is None:

            return [], {

                "symbol":
                    symbol,

                "status":
                    "NO_DATA"

            }


        candles_3m = create_3m(
            df
        )


        signals, diagnostics = (
            find_signals(
                df,
                candles_3m
            )
        )


        trades = execute_trades(
            df,
            signals
        )


        for trade in trades:

            trade[
                "symbol"
            ] = symbol


        diagnostics[
            "symbol"
        ] = symbol


        diagnostics[
            "status"
        ] = "OK"


        return (
            trades,
            diagnostics
        )


    except Exception as e:

        return [], {

            "symbol":
                symbol,

            "status":
                "ERROR",

            "error":
                str(e)

        }


# =============================================================================
# STATISTICS
# =============================================================================

def calculate_stats(
    trades
):

    if not trades:

        return {

            "trades": 0,

            "wins": 0,

            "losses": 0,

            "win_rate": 0.0,

            "average_return": 0.0,

            "total_return": 0.0,

            "profit_factor": 0.0,

            "best_trade": 0.0,

            "worst_trade": 0.0,

        }


    returns = pd.Series(

        [
            float(
                x["net_return_pct"]
            )

            for x in trades

        ]

    )


    wins = (
        returns > 0
    )


    losses = (
        returns < 0
    )


    gross_profit = (
        returns[wins]
        .sum()
    )


    gross_loss = abs(
        returns[losses]
        .sum()
    )


    if gross_loss > 0:

        profit_factor = (
            gross_profit
            /
            gross_loss
        )

    else:

        profit_factor = np.inf


    return {

        "trades":
            len(returns),

        "wins":
            int(
                wins.sum()
            ),

        "losses":
            int(
                losses.sum()
            ),

        "win_rate":
            float(
                wins.mean()
                *
                100
            ),

        "average_return":
            float(
                returns.mean()
            ),

        "total_return":
            float(
                returns.sum()
            ),

        "profit_factor":
            float(
                profit_factor
            ),

        "best_trade":
            float(
                returns.max()
            ),

        "worst_trade":
            float(
                returns.min()
            ),

    }


# =============================================================================
# MAIN
# =============================================================================

def main():

    start_time = time.time()


    print()
    print("=" * 90)
    print(
        "NSE SCANNER HISTORICAL BACKTEST"
    )
    print("=" * 90)

    print(
        f"Start date : {START_DATE}"
    )

    print(
        f"End date   : {END_DATE}"
    )

    print(
        f"Trade side : {TRADE_SIDE}"
    )

    print(
        f"Cost       : {ROUND_TRIP_COST}%"
    )

    print("=" * 90)
    print()


    # =====================================================================
    # FIND PARQUET FILES
    # =====================================================================

    paths = []


    paths.extend(

        glob.glob(
            "historical_2018_2025/*.parquet"
        )

    )


    paths.extend(

        glob.glob(
            "historical_2026/*.parquet"
        )

    )


    # Remove duplicates.

    paths = sorted(
        set(paths)
    )


    # Exclude index files.

    paths = [

        p
        for p in paths

        if "INDEX" not in
        os.path.basename(p).upper()

    ]


    if not paths:

        raise RuntimeError(
            "No Parquet stock files found."
        )


    print(
        f"Stock files found: "
        f"{len(paths)}"
    )

    print()


    all_trades = []

    diagnostics = []


    # =====================================================================
    # PROCESS STOCKS
    # =====================================================================

    for number, path in enumerate(
        paths,
        1
    ):

        symbol = os.path.basename(
            path
        )


        print(

            f"[{number:>4}/{len(paths)}] "
            f"{symbol:<25}",

            end="",

            flush=True

        )


        trades, diag = process_stock(
            path
        )


        diagnostics.append(
            diag
        )


        # -------------------------------------------------------------
        # SIDE FILTER
        # -------------------------------------------------------------

        if TRADE_SIDE == "LONG":

            trades = [

                t
                for t in trades

                if t["direction"]
                == "LONG"

            ]


        elif TRADE_SIDE == "SHORT":

            trades = [

                t
                for t in trades

                if t["direction"]
                == "SHORT"

            ]


        all_trades.extend(
            trades
        )


        print(

            f"signals="
            f"{diag.get('signals', 0):>4} "
            f"trades="
            f"{len(trades):>4} "
            f"{diag.get('status')}"

        )


    # =====================================================================
    # DATAFRAMES
    # =====================================================================

    trades_df = pd.DataFrame(
        all_trades
    )


    diagnostics_df = pd.DataFrame(
        diagnostics
    )


    # =====================================================================
    # SAVE DIAGNOSTICS
    # =====================================================================

    diagnostics_df.to_csv(

        os.path.join(
            RESULTS_DIR,
            "diagnostics_by_stock.csv"
        ),

        index=False

    )


    # =====================================================================
    # NO TRADES
    # =====================================================================

    if trades_df.empty:

        print()
        print("=" * 90)
        print(
            "NO EXECUTABLE TRADES"
        )
        print("=" * 90)


        print()
        print(
            "The diagnostic files have still "
            "been generated."
        )


        save_diagnostic_summary(
            diagnostics_df
        )


        return


    # =====================================================================
    # SORT
    # =====================================================================

    trades_df = trades_df.sort_values(

        [
            "trade_date",
            "symbol"

        ]

    ).reset_index(
        drop=True
    )


    # =====================================================================
    # SAVE TRADES
    # =====================================================================

    trades_df.to_csv(

        os.path.join(
            RESULTS_DIR,
            "all_trades.csv"
        ),

        index=False

    )


    # =====================================================================
    # OVERALL STATISTICS
    # =====================================================================

    overall_stats = calculate_stats(
        trades_df.to_dict(
            "records"
        )
    )


    # =====================================================================
    # PRINT OVERALL
    # =====================================================================

    print()
    print("=" * 90)
    print(
        "FINAL BACKTEST RESULT"
    )
    print("=" * 90)


    print_stats(
        overall_stats
    )


    # =====================================================================
    # LONG / SHORT
    # =====================================================================

    side_rows = []


    for side in [
        "LONG",
        "SHORT"
    ]:

        subset = trades_df[
            trades_df["direction"]
            ==
            side
        ]


        stats = calculate_stats(
            subset.to_dict(
                "records"
            )
        )


        side_rows.append({

            "direction":
                side,

            **stats

        })


        print()
        print(
            f"{side} ONLY"
        )

        print("-" * 50)

        print_stats(
            stats
        )


    side_df = pd.DataFrame(
        side_rows
    )


    side_df.to_csv(

        os.path.join(
            RESULTS_DIR,
            "long_short_statistics.csv"
        ),

        index=False

    )


    # =====================================================================
    # YEARLY
    # =====================================================================

    trades_df["year"] = (

        pd.to_datetime(
            trades_df["trade_date"]
        )
        .dt.year

    )


    yearly_rows = []


    for year, group in (
        trades_df.groupby(
            "year"
        )
    ):

        stats = calculate_stats(
            group.to_dict(
                "records"
            )
        )


        yearly_rows.append({

            "year":
                year,

            **stats

        })


    yearly_df = pd.DataFrame(
        yearly_rows
    )


    yearly_df.to_csv(

        os.path.join(
            RESULTS_DIR,
            "yearly_statistics.csv"
        ),

        index=False

    )


    # =====================================================================
    # YEAR + SIDE
    # =====================================================================

    yearly_side_rows = []


    for (
        year,
        side
    ), group in trades_df.groupby(

        [
            "year",
            "direction"
        ]

    ):

        stats = calculate_stats(
            group.to_dict(
                "records"
            )
        )


        yearly_side_rows.append({

            "year":
                year,

            "direction":
                side,

            **stats

        })


    yearly_side_df = pd.DataFrame(
        yearly_side_rows
    )


    yearly_side_df.to_csv(

        os.path.join(
            RESULTS_DIR,
            "yearly_long_short.csv"
        ),

        index=False

    )


    # =====================================================================
    # DIAGNOSTIC SUMMARY
    # =====================================================================

    save_diagnostic_summary(
        diagnostics_df
    )


    # =====================================================================
    # FINISHED
    # =====================================================================

    elapsed = (
        time.time()
        -
        start_time
    )


    print()
    print("=" * 90)
    print(
        "BACKTEST COMPLETE"
    )
    print("=" * 90)


    print(
        f"Runtime: "
        f"{elapsed / 60:.2f} minutes"
    )


    print()
    print(
        "Results:"
    )


    print(
        "  results/all_trades.csv"
    )

    print(
        "  results/diagnostics_by_stock.csv"
    )

    print(
        "  results/diagnostic_summary.csv"
    )

    print(
        "  results/long_short_statistics.csv"
    )

    print(
        "  results/yearly_statistics.csv"
    )

    print(
        "  results/yearly_long_short.csv"
    )


# =============================================================================
# PRINT STATS
# =============================================================================

def print_stats(stats):

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


# =============================================================================
# DIAGNOSTIC SUMMARY
# =============================================================================

def save_diagnostic_summary(
    diagnostics_df
):

    if diagnostics_df.empty:

        return


    numeric_columns = [

        "days_seen",
        "missing_3m",
        "missing_1m",
        "cond1",
        "cond2",
        "cond3",
        "cond4",
        "cond5",
        "signals",
        "long_signals",
        "short_signals"

    ]


    summary = {}


    for column in numeric_columns:

        if column in diagnostics_df:

            summary[
                column
            ] = diagnostics_df[
                column
            ].fillna(0).sum()


    summary_df = pd.DataFrame(
        [
            summary
        ]
    )


    summary_df.to_csv(

        os.path.join(
            RESULTS_DIR,
            "diagnostic_summary.csv"
        ),

        index=False

    )


# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":

    main()
