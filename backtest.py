import os
import glob
import math
import time
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# SETTINGS
# ============================================================

START_DATE = os.getenv(
    "START_DATE",
    "2020-01-01"
)

END_DATE = os.getenv(
    "END_DATE",
    "2025-12-31"
)

TRADE_SIDE = os.getenv(
    "TRADE_SIDE",
    "SHORT"
).upper()

# Round-trip transaction cost.
# Set to 0 if you want raw price-movement results.
ROUND_TRIP_COST_PCT = float(
    os.getenv(
        "ROUND_TRIP_COST_PCT",
        "0.10"
    )
)


# ============================================================
# RESULT DIRECTORY
# ============================================================

RESULTS_DIR = Path("results")

RESULTS_DIR.mkdir(
    exist_ok=True
)


# ============================================================
# CANDLE DIRECTION
# ============================================================

def direction(open_price, close_price):

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

    return ""


# ============================================================
# FIND PARQUET FILES
# ============================================================

def find_parquet_files():

    files = []

    directories = [

        "historical_2018_2025",

        "historical_2026"

    ]

    for directory in directories:

        if not os.path.isdir(directory):

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

        files.extend(
            glob.glob(
                os.path.join(
                    directory,
                    "**",
                    "*.pq"
                ),
                recursive=True
            )
        )

    return sorted(
        set(files)
    )


# ============================================================
# NORMALIZE PARQUET DATA
# ============================================================

def normalize_data(df):

    # --------------------------------------------------------
    # Handle MultiIndex columns
    # --------------------------------------------------------

    if isinstance(
        df.columns,
        pd.MultiIndex
    ):

        df.columns = [

            "_".join(

                str(x)

                for x in column

                if str(x)
                not in (
                    "",
                    "None"
                )

            )

            for column in df.columns

        ]


    # --------------------------------------------------------
    # Find columns
    # --------------------------------------------------------

    column_map = {

        str(column)
        .strip()
        .lower():
        column

        for column in df.columns

    }


    aliases = {

        "datetime": [

            "datetime",
            "timestamp",
            "date_time",
            "time",
            "date"

        ],

        "open": [

            "open",
            "o"

        ],

        "close": [

            "close",
            "c"

        ],

        "volume": [

            "volume",
            "vol",
            "v"

        ]

    }


    selected = {}


    for target, names in aliases.items():

        for name in names:

            if name in column_map:

                selected[target] = (
                    column_map[name]
                )

                break


    # --------------------------------------------------------
    # Datetime may be the index
    # --------------------------------------------------------

    if "datetime" not in selected:

        index_name = ""

        if df.index.name is not None:

            index_name = (
                str(df.index.name)
                .strip()
                .lower()
            )


        if index_name in aliases["datetime"]:

            df = df.reset_index()

            column_map = {

                str(column)
                .strip()
                .lower():
                column

                for column in df.columns

            }


            for name in aliases["datetime"]:

                if name in column_map:

                    selected["datetime"] = (
                        column_map[name]
                    )

                    break


    required = [

        "datetime",
        "open",
        "close",
        "volume"

    ]


    missing = [

        item

        for item in required

        if item not in selected

    ]


    if missing:

        raise ValueError(

            "Could not identify columns: "
            f"{missing}. "

            f"Actual columns: "
            f"{list(df.columns)}"

        )


    # --------------------------------------------------------
    # Create clean dataframe
    # --------------------------------------------------------

    output = pd.DataFrame({

        "datetime":
            df[selected["datetime"]],

        "open":
            pd.to_numeric(
                df[selected["open"]],
                errors="coerce"
            ),

        "close":
            pd.to_numeric(
                df[selected["close"]],
                errors="coerce"
            ),

        "volume":
            pd.to_numeric(
                df[selected["volume"]],
                errors="coerce"
            )

    })


    output["datetime"] = pd.to_datetime(

        output["datetime"],

        errors="coerce"

    )


    output = output.dropna(

        subset=[

            "datetime",
            "open",
            "close",
            "volume"

        ]

    )


    # --------------------------------------------------------
    # Convert timezone to IST
    # --------------------------------------------------------

    if getattr(
        output["datetime"].dt,
        "tz",
        None
    ) is not None:

        output["datetime"] = (

            output["datetime"]

            .dt.tz_convert(
                "Asia/Kolkata"
            )

            .dt.tz_localize(
                None
            )

        )


    # --------------------------------------------------------
    # Date / minute
    # --------------------------------------------------------

    output["date"] = (

        output["datetime"]
        .dt.date

    )


    output["hm"] = (

        output["datetime"]
        .dt.hour
        * 100

        +

        output["datetime"]
        .dt.minute

    )


    # --------------------------------------------------------
    # NSE session
    # --------------------------------------------------------

    output = output[

        (output["hm"] >= 915)

        &

        (output["hm"] <= 1529)

    ]


    # --------------------------------------------------------
    # Date range
    # --------------------------------------------------------

    start = pd.Timestamp(
        START_DATE
    ).date()

    end = pd.Timestamp(
        END_DATE
    ).date()


    output = output[

        (output["date"] >= start)

        &

        (output["date"] <= end)

    ]


    # --------------------------------------------------------
    # Sort and remove duplicate minutes
    # --------------------------------------------------------

    output = (

        output

        .sort_values(
            "datetime"
        )

        .drop_duplicates(

            subset=[
                "date",
                "hm"
            ],

            keep="last"

        )

        .reset_index(
            drop=True
        )

    )


    return output


# ============================================================
# CREATE EXACT 3-MINUTE CANDLE
# ============================================================

def make_3m_candle(
    day,
    start_minute
):

    minutes = [

        start_minute,

        start_minute + 1,

        start_minute + 2

    ]


    rows = day[

        day["hm"].isin(
            minutes
        )

    ].sort_values(
        "hm"
    )


    # Must contain all three 1-minute candles.

    if len(rows) != 3:

        return None


    if set(
        rows["hm"].astype(int)
    ) != set(minutes):

        return None


    return {

        "open":
            float(
                rows.iloc[0]["open"]
            ),

        "close":
            float(
                rows.iloc[-1]["close"]
            ),

        "volume":
            float(
                rows["volume"].sum()
            )

    }


# ============================================================
# EVALUATE ONE SIGNAL DAY
# ============================================================

def evaluate_signal_day(day):

    # --------------------------------------------------------
    # Required 1-minute candles
    # --------------------------------------------------------

    required = {

        915,
        916,
        917,

        918,
        919,
        920,

        1524,
        1525,
        1526,

        1527,
        1528,
        1529

    }


    available = set(

        day["hm"]
        .astype(int)

    )


    if not required.issubset(
        available
    ):

        return None


    # --------------------------------------------------------
    # 3-minute candles
    # --------------------------------------------------------

    candle_1524 = make_3m_candle(
        day,
        1524
    )


    candle_1527 = make_3m_candle(
        day,
        1527
    )


    candle_0915 = make_3m_candle(
        day,
        915
    )


    candle_0918 = make_3m_candle(
        day,
        918
    )


    if any(

        candle is None

        for candle in [

            candle_1524,
            candle_1527,
            candle_0915,
            candle_0918

        ]

    ):

        return None


    # --------------------------------------------------------
    # 3-minute directions
    # --------------------------------------------------------

    trend_1524 = direction(

        candle_1524["open"],
        candle_1524["close"]

    )


    trend_1527 = direction(

        candle_1527["open"],
        candle_1527["close"]

    )


    trend_0915_3m = direction(

        candle_0915["open"],
        candle_0915["close"]

    )


    trend_0918_3m = direction(

        candle_0918["open"],
        candle_0918["close"]

    )


    # --------------------------------------------------------
    # CONDITION 1
    #
    # 15:24 and 15:27 opposite
    # 15:27 volume > 15:24
    # --------------------------------------------------------

    condition_1 = (

        trend_1524 != 0

        and

        trend_1527 != 0

        and

        trend_1524 != trend_1527

        and

        candle_1527["volume"]
        >
        candle_1524["volume"]

    )


    # --------------------------------------------------------
    # CONDITION 2
    #
    # 09:15 and 09:18 both
    # match 15:27
    # --------------------------------------------------------

    condition_2 = (

        trend_1527 != 0

        and

        trend_0915_3m
        ==
        trend_1527

        and

        trend_0918_3m
        ==
        trend_1527

    )


    # --------------------------------------------------------
    # 1-minute candles
    # --------------------------------------------------------

    minute_rows = {

        int(row["hm"]):
        row

        for _, row
        in day.iterrows()

    }


    row_1528 = minute_rows[1528]

    row_1529 = minute_rows[1529]

    row_0915 = minute_rows[915]

    row_0916 = minute_rows[916]


    # --------------------------------------------------------
    # 1-minute directions
    # --------------------------------------------------------

    trend_1528 = direction(

        float(row_1528["open"]),

        float(row_1528["close"])

    )


    trend_1529 = direction(

        float(row_1529["open"]),

        float(row_1529["close"])

    )


    trend_0915_1m = direction(

        float(row_0915["open"]),

        float(row_0915["close"])

    )


    trend_0916_1m = direction(

        float(row_0916["open"]),

        float(row_0916["close"])

    )


    # --------------------------------------------------------
    # CONDITION 3
    #
    # 15:28 and 15:29 opposite
    # 15:28 volume > 15:29
    # --------------------------------------------------------

    condition_3 = (

        trend_1528 != 0

        and

        trend_1529 != 0

        and

        trend_1528 != trend_1529

        and

        float(row_1528["volume"])
        >
        float(row_1529["volume"])

    )


    # --------------------------------------------------------
    # CONDITION 4
    #
    # 15:28 matches 3-minute 15:27
    # --------------------------------------------------------

    condition_4 = (

        trend_1528 != 0

        and

        trend_1527 != 0

        and

        trend_1528
        ==
        trend_1527

    )


    # --------------------------------------------------------
    # CONDITION 5
    #
    # 1-minute 09:15 and 09:16
    # both match 15:28
    # --------------------------------------------------------

    condition_5 = (

        trend_1528 != 0

        and

        trend_0915_1m
        ==
        trend_1528

        and

        trend_0916_1m
        ==
        trend_1528

    )


    passed = (

        condition_1

        and

        condition_2

        and

        condition_3

        and

        condition_4

        and

        condition_5

    )


    return {

        "passed":
            passed,

        "direction":
            direction_name(
                trend_1527
            ),

        "condition_1":
            condition_1,

        "condition_2":
            condition_2,

        "condition_3":
            condition_3,

        "condition_4":
            condition_4,

        "condition_5":
            condition_5

    }


# ============================================================
# PROCESS ONE STOCK
# ============================================================

def process_stock(
    filepath
):

    symbol = Path(
        filepath
    ).stem


    try:

        raw = pd.read_parquet(
            filepath
        )


        data = normalize_data(
            raw
        )


        if data.empty:

            return [], {

                "symbol":
                    symbol,

                "status":
                    "NO_DATA"

            }


        days = {

            date: group

            for date, group

            in data.groupby(
                "date",
                sort=True
            )

        }


        dates = sorted(
            days.keys()
        )


        trades = []


        diagnostic = {

            "symbol":
                symbol,

            "status":
                "OK",

            "signal_days":
                0,

            "incomplete_days":
                0,

            "condition_1":
                0,

            "condition_2":
                0,

            "condition_3":
                0,

            "condition_4":
                0,

            "condition_5":
                0,

            "signals":
                0,

            "long_signals":
                0,

            "short_signals":
                0,

            "trades":
                0

        }


        # ----------------------------------------------------
        # Each signal day uses the next available
        # trading date as the trade day.
        # ----------------------------------------------------

        for i in range(
            len(dates) - 1
        ):

            signal_date = dates[i]

            trade_date = dates[i + 1]


            diagnostic[
                "signal_days"
            ] += 1


            signal = evaluate_signal_day(
                days[signal_date]
            )


            if signal is None:

                diagnostic[
                    "incomplete_days"
                ] += 1

                continue


            # ------------------------------------------------
            # Diagnostic condition counts
            # ------------------------------------------------

            for n in range(
                1,
                6
            ):

                if signal[
                    f"condition_{n}"
                ]:

                    diagnostic[
                        f"condition_{n}"
                    ] += 1


            if not signal["passed"]:

                continue


            diagnostic[
                "signals"
            ] += 1


            side = signal[
                "direction"
            ]


            if side == "LONG":

                diagnostic[
                    "long_signals"
                ] += 1

            elif side == "SHORT":

                diagnostic[
                    "short_signals"
                ] += 1


            # ------------------------------------------------
            # User-selected side
            # ------------------------------------------------

            if (

                TRADE_SIDE != "BOTH"

                and

                side != TRADE_SIDE

            ):

                continue


            # ------------------------------------------------
            # Next trading day
            # ------------------------------------------------

            next_day = days[
                trade_date
            ]


            entry_rows = next_day[

                next_day["hm"] == 915

            ]


            exit_rows = next_day[

                next_day["hm"] == 1527

            ]


            if (

                entry_rows.empty

                or

                exit_rows.empty

            ):

                continue


            entry_price = float(

                entry_rows.iloc[0]["open"]

            )


            exit_price = float(

                exit_rows.iloc[0]["open"]

            )


            if (

                not math.isfinite(
                    entry_price
                )

                or

                not math.isfinite(
                    exit_price
                )

                or

                entry_price <= 0

            ):

                continue


            # ------------------------------------------------
            # P/L
            # ------------------------------------------------

            if side == "LONG":

                gross_return = (

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

                gross_return = (

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


            net_return = (

                gross_return
                -
                ROUND_TRIP_COST_PCT

            )


            trades.append({

                "symbol":
                    symbol,

                "signal_date":
                    str(signal_date),

                "trade_date":
                    str(trade_date),

                "direction":
                    side,

                "entry_0915_open":
                    entry_price,

                "exit_1527_open":
                    exit_price,

                "gross_return_pct":
                    gross_return,

                "net_return_pct":
                    net_return

            })


            diagnostic[
                "trades"
            ] += 1


        return trades, diagnostic


    except Exception as error:

        return [], {

            "symbol":
                symbol,

            "status":
                "ERROR",

            "error":
                repr(error)

        }


# ============================================================
# STATISTICS
# ============================================================

def calculate_statistics(
    records
):

    if not records:

        return {

            "trades":
                0,

            "wins":
                0,

            "losses":
                0,

            "win_rate_pct":
                0.0,

            "average_return_pct":
                0.0,

            "total_return_pct":
                0.0,

            "profit_factor":
                0.0,

            "best_trade_pct":
                0.0,

            "worst_trade_pct":
                0.0

        }


    returns = pd.Series([

        float(
            trade[
                "net_return_pct"
            ]
        )

        for trade in records

    ])


    wins = (
        returns > 0
    )


    losses = (
        returns < 0
    )


    gross_profit = float(

        returns[wins].sum()

    )


    gross_loss = float(

        abs(
            returns[losses].sum()
        )

    )


    if gross_loss > 0:

        profit_factor = (

            gross_profit
            /
            gross_loss

        )

    else:

        profit_factor = float(
            "inf"
        )


    return {

        "trades":
            int(
                len(returns)
            ),

        "wins":
            int(
                wins.sum()
            ),

        "losses":
            int(
                losses.sum()
            ),

        "win_rate_pct":
            float(
                wins.mean()
                *
                100
            ),

        "average_return_pct":
            float(
                returns.mean()
            ),

        "total_return_pct":
            float(
                returns.sum()
            ),

        "profit_factor":
            profit_factor,

        "best_trade_pct":
            float(
                returns.max()
            ),

        "worst_trade_pct":
            float(
                returns.min()
            )

    }


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()


    print()
    print("=" * 80)
    print("NSE SCANNER BACKTEST")
    print("=" * 80)

    print(
        f"Start date       : {START_DATE}"
    )

    print(
        f"End date         : {END_DATE}"
    )

    print(
        f"Trade side       : {TRADE_SIDE}"
    )

    print(
        f"Trading cost     : "
        f"{ROUND_TRIP_COST_PCT}%"
    )

    print("=" * 80)
    print()


    # --------------------------------------------------------
    # Find files
    # --------------------------------------------------------

    parquet_files = (
        find_parquet_files()
    )


    print(
        f"Parquet files found: "
        f"{len(parquet_files)}"
    )


    if not parquet_files:

        raise RuntimeError(

            "No Parquet files found."

        )


    all_trades = []

    all_diagnostics = []


    # --------------------------------------------------------
    # Process every stock
    # --------------------------------------------------------

    for number, filepath in enumerate(

        parquet_files,
        start=1

    ):

        symbol = Path(
            filepath
        ).stem


        print(

            f"[{number:>4}/"
            f"{len(parquet_files):>4}] "
            f"{symbol:<20}",

            end="",

            flush=True

        )


        trades, diagnostic = (
            process_stock(
                filepath
            )
        )


        all_trades.extend(
            trades
        )


        all_diagnostics.append(
            diagnostic
        )


        print(

            f"status="
            f"{diagnostic.get('status'):<10} "
            f"signals="
            f"{diagnostic.get('signals', 0):>5} "
            f"trades="
            f"{len(trades):>5}"

        )


    # --------------------------------------------------------
    # Diagnostics
    # --------------------------------------------------------

    diagnostics_df = pd.DataFrame(
        all_diagnostics
    )


    diagnostics_df.to_csv(

        RESULTS_DIR
        /
        "diagnostics_by_stock.csv",

        index=False

    )


    numeric_columns = [

        "signal_days",

        "incomplete_days",

        "condition_1",

        "condition_2",

        "condition_3",

        "condition_4",

        "condition_5",

        "signals",

        "long_signals",

        "short_signals",

        "trades"

    ]


    summary = {}


    for column in numeric_columns:

        if column in diagnostics_df:

            summary[column] = int(

                pd.to_numeric(

                    diagnostics_df[
                        column
                    ],

                    errors="coerce"

                )

                .fillna(0)

                .sum()

            )


    pd.DataFrame(
        [summary]
    ).to_csv(

        RESULTS_DIR
        /
        "diagnostic_summary.csv",

        index=False

    )


    # --------------------------------------------------------
    # Trades
    # --------------------------------------------------------

    trades_df = pd.DataFrame(
        all_trades
    )


    if trades_df.empty:

        trades_df = pd.DataFrame(

            columns=[

                "symbol",

                "signal_date",

                "trade_date",

                "direction",

                "entry_0915_open",

                "exit_1527_open",

                "gross_return_pct",

                "net_return_pct"

            ]

        )

    else:

        trades_df = (

            trades_df

            .sort_values(

                [
                    "trade_date",
                    "symbol"

                ]

            )

            .reset_index(
                drop=True
            )

        )


    trades_df.to_csv(

        RESULTS_DIR
        /
        "all_trades.csv",

        index=False

    )


    records = trades_df.to_dict(
        "records"
    )


    # --------------------------------------------------------
    # Overall statistics
    # --------------------------------------------------------

    overall = calculate_statistics(
        records
    )


    # --------------------------------------------------------
    # LONG / SHORT statistics
    # --------------------------------------------------------

    side_rows = []


    for side in [

        "LONG",
        "SHORT"

    ]:

        side_records = [

            trade

            for trade in records

            if trade[
                "direction"
            ]
            ==
            side

        ]


        side_rows.append({

            "direction":
                side,

            **calculate_statistics(
                side_records
            )

        })


    pd.DataFrame(
        side_rows
    ).to_csv(

        RESULTS_DIR
        /
        "long_short_statistics.csv",

        index=False

    )


    # --------------------------------------------------------
    # Yearly statistics
    # --------------------------------------------------------

    if records:

        trades_df[
            "year"
        ] = (

            pd.to_datetime(

                trades_df[
                    "trade_date"
                ]

            )

            .dt.year

        )


        yearly_rows = []


        for year, group in (

            trades_df.groupby(
                "year"
            )

        ):

            yearly_rows.append({

                "year":
                    int(year),

                **calculate_statistics(

                    group.to_dict(
                        "records"
                    )

                )

            })


        pd.DataFrame(
            yearly_rows
        ).to_csv(

            RESULTS_DIR
            /
            "yearly_statistics.csv",

            index=False

        )


        yearly_side_rows = []


        for (

            year,
            side

        ), group in (

            trades_df.groupby(

                [
                    "year",
                    "direction"
                ]

            )

        ):

            yearly_side_rows.append({

                "year":
                    int(year),

                "direction":
                    side,

                **calculate_statistics(

                    group.to_dict(
                        "records"
                    )

                )

            })


        pd.DataFrame(
            yearly_side_rows
        ).to_csv(

            RESULTS_DIR
            /
            "yearly_long_short.csv",

            index=False

        )


    # --------------------------------------------------------
    # Console result
    # --------------------------------------------------------

    print()
    print("=" * 80)
    print("FINAL RESULT")
    print("=" * 80)


    for key, value in overall.items():

        if isinstance(
            value,
            float
        ):

            if math.isinf(
                value
            ):

                print(
                    f"{key:<25}: INF"
                )

            else:

                print(

                    f"{key:<25}: "
                    f"{value:.4f}"

                )

        else:

            print(

                f"{key:<25}: "
                f"{value:,}"

            )


    print()
    print("LONG / SHORT")
    print("-" * 80)


    for row in side_rows:

        print(

            f"{row['direction']:<8} "

            f"Trades="
            f"{row['trades']:>6} "

            f"Wins="
            f"{row['wins']:>6} "

            f"Losses="
            f"{row['losses']:>6} "

            f"Win rate="
            f"{row['win_rate_pct']:>7.2f}% "

            f"Avg="
            f"{row['average_return_pct']:>8.4f}% "

            f"PF="
            f"{row['profit_factor']:>7.3f}"

        )


    print()
    print(

        f"Runtime: "
        f"{(time.time() - start_time) / 60:.2f} minutes"

    )

    print()
    print(
        "Results saved in results/"
    )


if __name__ == "__main__":

    main()
