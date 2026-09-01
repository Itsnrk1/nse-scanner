import os
import glob
import math
import time
from pathlib import Path

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

ROUND_TRIP_COST_PCT = float(
    os.getenv(
        "ROUND_TRIP_COST_PCT",
        "0.10"
    )
)

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)


# ============================================================
# DIRECTION
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

    for directory in (
        "historical_2018_2025",
        "historical_2026"
    ):

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

    return sorted(set(files))


# ============================================================
# NORMALIZE DATA
# ============================================================

def normalize_data(df):

    if isinstance(
        df.columns,
        pd.MultiIndex
    ):

        df.columns = [
            "_".join(
                str(x)
                for x in column
                if str(x) not in ("", "None")
            )
            for column in df.columns
        ]


    column_map = {
        str(column).strip().lower(): column
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

                selected[target] = column_map[name]

                break


    # --------------------------------------------------------
    # Datetime may be index
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
                str(column).strip().lower(): column
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
        x
        for x in required
        if x not in selected
    ]


    if missing:

        raise ValueError(
            f"Missing columns: {missing}. "
            f"Actual columns: {list(df.columns)}"
        )


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


    # Convert timezone-aware data to IST.
    if getattr(
        output["datetime"].dt,
        "tz",
        None
    ) is not None:

        output["datetime"] = (
            output["datetime"]
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )


    output["date"] = (
        output["datetime"].dt.date
    )


    output["hm"] = (
        output["datetime"].dt.hour * 100
        +
        output["datetime"].dt.minute
    )


    # NSE regular session.
    output = output[
        (output["hm"] >= 915)
        &
        (output["hm"] <= 1529)
    ]


    start_date = pd.Timestamp(
        START_DATE
    ).date()

    end_date = pd.Timestamp(
        END_DATE
    ).date()


    output = output[
        (output["date"] >= start_date)
        &
        (output["date"] <= end_date)
    ]


    output = (
        output
        .sort_values("datetime")
        .drop_duplicates(
            ["date", "hm"],
            keep="last"
        )
        .reset_index(drop=True)
    )


    return output


# ============================================================
# BUILD EXACT 3-MINUTE CANDLE
# ============================================================

def make_3m_candle(
    day,
    start_minute
):

    required_minutes = [
        start_minute,
        start_minute + 1,
        start_minute + 2
    ]


    rows = (
        day[
            day["hm"].isin(
                required_minutes
            )
        ]
        .sort_values("hm")
    )


    # Must have all 3 one-minute candles.
    if len(rows) != 3:
        return None


    if set(
        rows["hm"].astype(int)
    ) != set(required_minutes):

        return None


    return {

        "open":
            float(rows.iloc[0]["open"]),

        "close":
            float(rows.iloc[-1]["close"]),

        "volume":
            float(rows["volume"].sum())

    }


# ============================================================
# EVALUATE SIGNAL DAY
# ============================================================

def evaluate_signal_day(day):

    required = {

        # Morning 3-minute candles
        915,
        916,
        917,

        918,
        919,
        920,

        # EOD 3-minute candles
        1524,
        1525,
        1526,

        1527,
        1528,
        1529

    }


    available = set(
        day["hm"].astype(int)
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
        for candle in (
            candle_1524,
            candle_1527,
            candle_0915,
            candle_0918
        )
    ):

        return None


    # --------------------------------------------------------
    # Trends
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
    # 15:24 volume > 15:27
    # --------------------------------------------------------

    condition_1 = (

        trend_1524 != 0

        and

        trend_1527 != 0

        and

        trend_1524 != trend_1527

        and

        candle_1524["volume"]
        >
        candle_1527["volume"]

    )


    # --------------------------------------------------------
    # CONDITION 2
    #
    # 09:15 and 09:18 both match 15:24
    # --------------------------------------------------------

    condition_2 = (

        trend_1524 != 0

        and

        trend_0915_3m
        ==
        trend_1524

        and

        trend_0918_3m
        ==
        trend_1524

    )


    # --------------------------------------------------------
    # 1-minute candles
    # --------------------------------------------------------

    minute_rows = {
        int(row["hm"]): row
        for _, row in day.iterrows()
    }


    row_1528 = minute_rows[1528]
    row_1529 = minute_rows[1529]


    trend_1528 = direction(
        float(row_1528["open"]),
        float(row_1528["close"])
    )


    trend_1529 = direction(
        float(row_1529["open"]),
        float(row_1529["close"])
    )


    # --------------------------------------------------------
    # CONDITION 3
    #
    # 15:28 volume > 15:29 volume
    #
    # AND
    #
    # 15:28 matches 3-minute 15:24
    # --------------------------------------------------------

    condition_3 = (

        trend_1528 != 0

        and

        trend_1524 != 0

        and

        float(row_1528["volume"])
        >
        float(row_1529["volume"])

        and

        trend_1528
        ==
        trend_1524

    )


    # --------------------------------------------------------
    # FINAL SIGNAL
    # --------------------------------------------------------

    passed = (

        condition_1

        and

        condition_2

        and

        condition_3

    )


    return {

        "passed":
            passed,

        "signal_direction":
            direction_name(
                trend_1524
            ),

        "trend_1524":
            trend_1524,

        "trend_1527":
            trend_1527,

        "condition_1":
            condition_1,

        "condition_2":
            condition_2,

        "condition_3":
            condition_3

    }


# ============================================================
# PROCESS ONE STOCK
# ============================================================

def process_stock(filepath):

    symbol = Path(
        filepath
    ).stem


    try:

        raw_data = pd.read_parquet(
            filepath
        )


        data = normalize_data(
            raw_data
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


        diagnostics = {

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

            "signals":
                0,

            "long_signals":
                0,

            "short_signals":
                0,

            "trades":
                0

        }


        # ====================================================
        # LOOP THROUGH SIGNAL DAYS
        # ====================================================

        for i in range(
            len(dates) - 1
        ):

            signal_date = dates[i]

            trade_date = dates[i + 1]


            diagnostics[
                "signal_days"
            ] += 1


            signal = evaluate_signal_day(
                days[signal_date]
            )


            if signal is None:

                diagnostics[
                    "incomplete_days"
                ] += 1

                continue


            # ------------------------------------------------
            # Diagnostic counts
            # ------------------------------------------------

            for number in range(
                1,
                4
            ):

                if signal[
                    f"condition_{number}"
                ]:

                    diagnostics[
                        f"condition_{number}"
                    ] += 1


            if not signal["passed"]:

                continue


            diagnostics[
                "signals"
            ] += 1


            side = signal[
                "signal_direction"
            ]


            if side == "LONG":

                diagnostics[
                    "long_signals"
                ] += 1

            elif side == "SHORT":

                diagnostics[
                    "short_signals"
                ] += 1


            # ------------------------------------------------
            # Side filter
            # ------------------------------------------------

            if (

                TRADE_SIDE != "BOTH"

                and

                side != TRADE_SIDE

            ):

                continue


            # =================================================
            # NEXT TRADING DAY
            # =================================================

            next_day = days[
                trade_date
            ]


            # Entry 09:15
            entry_rows = next_day[
                next_day["hm"] == 915
            ]


            # Exit 1: 09:18
            exit_0918_rows = next_day[
                next_day["hm"] == 918
            ]


            # Exit 2: 15:27
            exit_1527_rows = next_day[
                next_day["hm"] == 1527
            ]


            if (

                entry_rows.empty

                or

                exit_0918_rows.empty

                or

                exit_1527_rows.empty

            ):

                continue


            entry_price = float(
                entry_rows.iloc[0]["open"]
            )


            exit_0918_price = float(
                exit_0918_rows.iloc[0]["open"]
            )


            exit_1527_price = float(
                exit_1527_rows.iloc[0]["open"]
            )


            if (

                not math.isfinite(
                    entry_price
                )

                or

                entry_price <= 0

            ):

                continue


            # =================================================
            # CALCULATE RETURNS
            # =================================================

            if side == "LONG":

                return_0918_gross = (

                    (
                        exit_0918_price
                        -
                        entry_price
                    )
                    /
                    entry_price
                    *
                    100

                )


                return_1527_gross = (

                    (
                        exit_1527_price
                        -
                        entry_price
                    )
                    /
                    entry_price
                    *
                    100

                )

            else:

                return_0918_gross = (

                    (
                        entry_price
                        -
                        exit_0918_price
                    )
                    /
                    entry_price
                    *
                    100

                )


                return_1527_gross = (

                    (
                        entry_price
                        -
                        exit_1527_price
                    )
                    /
                    entry_price
                    *
                    100

                )


            # =================================================
            # HALF-POSITION COST
            # =================================================
            #
            # We split the position 50/50.
            #
            # Each half receives half of the total
            # round-trip cost.
            #
            # =================================================

            half_cost = (
                ROUND_TRIP_COST_PCT
                /
                2
            )


            return_0918_net = (
                return_0918_gross
                -
                half_cost
            )


            return_1527_net = (
                return_1527_gross
                -
                half_cost
            )


            # Combined result of both halves.
            combined_return = (

                (
                    return_0918_net
                    *
                    0.5
                )

                +

                (
                    return_1527_net
                    *
                    0.5
                )

            )


            # =================================================
            # STORE TRADE
            # =================================================

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

                "exit_0918_open":
                    exit_0918_price,

                "exit_1527_open":
                    exit_1527_price,

                "return_0918_gross_pct":
                    return_0918_gross,

                "return_0918_net_pct":
                    return_0918_net,

                "return_1527_gross_pct":
                    return_1527_gross,

                "return_1527_net_pct":
                    return_1527_net,

                "combined_net_return_pct":
                    combined_return

            })


            diagnostics[
                "trades"
            ] += 1


        return trades, diagnostics


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
    returns
):

    if not returns:

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


    series = pd.Series(
        returns,
        dtype=float
    )


    wins = (
        series > 0
    )


    losses = (
        series < 0
    )


    gross_profit = float(
        series[wins].sum()
    )


    gross_loss = float(
        abs(
            series[losses].sum()
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
            int(len(series)),

        "wins":
            int(wins.sum()),

        "losses":
            int(losses.sum()),

        "win_rate_pct":
            float(
                wins.mean() * 100
            ),

        "average_return_pct":
            float(
                series.mean()
            ),

        "total_return_pct":
            float(
                series.sum()
            ),

        "profit_factor":
            profit_factor,

        "best_trade_pct":
            float(
                series.max()
            ),

        "worst_trade_pct":
            float(
                series.min()
            )

    }


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()


    print()
    print("=" * 90)
    print("NSE SCANNER — NEW STRATEGY BACKTEST")
    print("=" * 90)

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
        f"Round-trip cost  : "
        f"{ROUND_TRIP_COST_PCT}%"
    )

    print()
    print(
        "EXIT 1            : 09:18 OPEN"
    )

    print(
        "EXIT 2            : 15:27 OPEN"
    )

    print(
        "POSITION SPLIT    : 50% / 50%"
    )

    print("=" * 90)
    print()


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


    # ========================================================
    # PROCESS STOCKS
    # ========================================================

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


    # ========================================================
    # DATAFRAME
    # ========================================================

    diagnostics_df = pd.DataFrame(
        all_diagnostics
    )


    diagnostics_df.to_csv(

        RESULTS_DIR
        /
        "diagnostics_by_stock.csv",

        index=False

    )


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

                "exit_0918_open",

                "exit_1527_open",

                "return_0918_gross_pct",

                "return_0918_net_pct",

                "return_1527_gross_pct",

                "return_1527_net_pct",

                "combined_net_return_pct"

            ]

        )


    trades_df.to_csv(

        RESULTS_DIR
        /
        "all_trades.csv",

        index=False

    )


    # ========================================================
    # THREE PERFORMANCE MEASURES
    # ========================================================

    exit_0918_returns = (
        trades_df[
            "return_0918_net_pct"
        ]
        .tolist()
        if not trades_df.empty
        else []
    )


    exit_1527_returns = (
        trades_df[
            "return_1527_net_pct"
        ]
        .tolist()
        if not trades_df.empty
        else []
    )


    combined_returns = (
        trades_df[
            "combined_net_return_pct"
        ]
        .tolist()
        if not trades_df.empty
        else []
    )


    statistics = {

        "09:18 EXIT": calculate_statistics(
            exit_0918_returns
        ),

        "15:27 EXIT": calculate_statistics(
            exit_1527_returns
        ),

        "50/50 COMBINED": calculate_statistics(
            combined_returns
        )

    }


    # ========================================================
    # SAVE EXIT STATISTICS
    # ========================================================

    exit_rows = []


    for exit_name, result in statistics.items():

        exit_rows.append({

            "exit":
                exit_name,

            **result

        })


    pd.DataFrame(
        exit_rows
    ).to_csv(

        RESULTS_DIR
        /
        "exit_statistics.csv",

        index=False

    )


    # ========================================================
    # LONG / SHORT STATISTICS
    # ========================================================

    side_rows = []


    for side in (
        "LONG",
        "SHORT"
    ):

        subset = trades_df[
            trades_df["direction"]
            ==
            side
        ]


        combined = (

            subset[
                "combined_net_return_pct"
            ]
            .tolist()

            if not subset.empty

            else []

        )


        early = (

            subset[
                "return_0918_net_pct"
            ]
            .tolist()

            if not subset.empty

            else []

        )


        full_day = (

            subset[
                "return_1527_net_pct"
            ]
            .tolist()

            if not subset.empty

            else []

        )


        combined_stats = (
            calculate_statistics(
                combined
            )
        )


        early_stats = (
            calculate_statistics(
                early
            )
        )


        full_stats = (
            calculate_statistics(
                full_day
            )
        )


        side_rows.append({

            "direction":
                side,

            "trades":
                combined_stats["trades"],

            "combined_win_rate_pct":
                combined_stats[
                    "win_rate_pct"
                ],

            "combined_average_return_pct":
                combined_stats[
                    "average_return_pct"
                ],

            "combined_total_return_pct":
                combined_stats[
                    "total_return_pct"
                ],

            "combined_profit_factor":
                combined_stats[
                    "profit_factor"
                ],

            "09:18_win_rate_pct":
                early_stats[
                    "win_rate_pct"
                ],

            "09:18_average_return_pct":
                early_stats[
                    "average_return_pct"
                ],

            "15:27_win_rate_pct":
                full_stats[
                    "win_rate_pct"
                ],

            "15:27_average_return_pct":
                full_stats[
                    "average_return_pct"
                ]

        })


    pd.DataFrame(
        side_rows
    ).to_csv(

        RESULTS_DIR
        /
        "long_short_statistics.csv",

        index=False

    )


    # ========================================================
    # PRINT FINAL RESULTS
    # ========================================================

    print()
    print("=" * 90)
    print("FINAL BACKTEST RESULT")
    print("=" * 90)


    for exit_name, result in statistics.items():

        print()
        print(exit_name)
        print("-" * 60)

        print(
            f"Trades              : "
            f"{result['trades']}"
        )

        print(
            f"Wins                : "
            f"{result['wins']}"
        )

        print(
            f"Losses              : "
            f"{result['losses']}"
        )

        print(
            f"Win rate            : "
            f"{result['win_rate_pct']:.2f}%"
        )

        print(
            f"Average return      : "
            f"{result['average_return_pct']:.4f}%"
        )

        print(
            f"Total return        : "
            f"{result['total_return_pct']:.4f}%"
        )

        print(
            f"Profit factor       : "
            f"{result['profit_factor']}"
        )

        print(
            f"Best trade          : "
            f"{result['best_trade_pct']:.4f}%"
        )

        print(
            f"Worst trade         : "
            f"{result['worst_trade_pct']:.4f}%"
        )


    print()
    print("=" * 90)
    print("DIAGNOSTICS")
    print("=" * 90)


    if not diagnostics_df.empty:

        print(
            f"Signal days checked : "
            f"{diagnostics_df['signal_days'].sum()}"
        )

        print(
            f"Incomplete days     : "
            f"{diagnostics_df['incomplete_days'].sum()}"
        )

        print(
            f"Condition 1 passed  : "
            f"{diagnostics_df['condition_1'].sum()}"
        )

        print(
            f"Condition 2 passed  : "
            f"{diagnostics_df['condition_2'].sum()}"
        )

        print(
            f"Condition 3 passed  : "
            f"{diagnostics_df['condition_3'].sum()}"
        )

        print(
            f"Final signals       : "
            f"{diagnostics_df['signals'].sum()}"
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


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
