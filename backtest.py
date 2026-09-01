import os
import glob
import math
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd


# ============================================================
# SETTINGS
# ============================================================

START_DATE = os.getenv("START_DATE", "2020-01-01")
END_DATE = os.getenv("END_DATE", "2025-12-31")

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

TARGET_PCT = float(
    os.getenv(
        "TARGET_PCT",
        "1.0"
    )
)

# Maximum number of simultaneous stock processes.
# GitHub-hosted runners generally have limited CPU.
MAX_WORKERS = int(
    os.getenv(
        "MAX_WORKERS",
        str(min(4, os.cpu_count() or 2))
    )
)


RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(
    exist_ok=True
)


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


    columns = {
        str(c).strip().lower(): c
        for c in df.columns
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

        "high": [
            "high",
            "h"
        ],

        "low": [
            "low",
            "l"
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

            if name in columns:

                selected[target] = columns[name]

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

            columns = {
                str(c).strip().lower(): c
                for c in df.columns
            }


            for name in aliases["datetime"]:

                if name in columns:

                    selected["datetime"] = (
                        columns[name]
                    )

                    break


    required = [
        "datetime",
        "open",
        "high",
        "low",
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
            f"Missing columns: {missing}; "
            f"actual columns: {list(df.columns)}"
        )


    # --------------------------------------------------------
    # Only keep columns actually required.
    # --------------------------------------------------------

    output = pd.DataFrame({

        "datetime":
            df[selected["datetime"]],

        "open":
            pd.to_numeric(
                df[selected["open"]],
                errors="coerce"
            ),

        "high":
            pd.to_numeric(
                df[selected["high"]],
                errors="coerce"
            ),

        "low":
            pd.to_numeric(
                df[selected["low"]],
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
            "high",
            "low",
            "close",
            "volume"
        ]
    )


    # --------------------------------------------------------
    # Convert timezone-aware data to IST.
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # NSE regular session.
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # Sort and remove duplicate minute candles.
    # --------------------------------------------------------

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
# GET EXACT 3-MINUTE CANDLE
# ============================================================

def make_3m(day, start):

    rows = day.loc[
        day["hm"].between(
            start,
            start + 2
        )
    ]


    if len(rows) != 3:

        return None


    if set(
        rows["hm"].astype(int)
    ) != {
        start,
        start + 1,
        start + 2
    }:

        return None


    first = rows.iloc[0]
    last = rows.iloc[-1]


    return {

        "open":
            float(first["open"]),

        "close":
            float(last["close"]),

        "volume":
            float(rows["volume"].sum())

    }


# ============================================================
# EVALUATE SIGNAL
# ============================================================

def evaluate_signal(day):

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
        day["hm"].astype(int)
    )


    if not required.issubset(
        available
    ):

        return None


    # --------------------------------------------------------
    # 3-minute candles
    # --------------------------------------------------------

    c1524 = make_3m(
        day,
        1524
    )

    c1527 = make_3m(
        day,
        1527
    )

    c0915 = make_3m(
        day,
        915
    )

    c0918 = make_3m(
        day,
        918
    )


    if any(
        x is None
        for x in (
            c1524,
            c1527,
            c0915,
            c0918
        )
    ):

        return None


    # --------------------------------------------------------
    # Trends
    # --------------------------------------------------------

    trend_1524 = direction(
        c1524["open"],
        c1524["close"]
    )

    trend_1527 = direction(
        c1527["open"],
        c1527["close"]
    )

    trend_0915 = direction(
        c0915["open"],
        c0915["close"]
    )

    trend_0918 = direction(
        c0918["open"],
        c0918["close"]
    )


    # --------------------------------------------------------
    # CONDITION 1
    #
    # 15:24 and 15:27 opposite
    # 15:24 has MORE volume
    # --------------------------------------------------------

    condition_1 = (

        trend_1524 != 0

        and

        trend_1527 != 0

        and

        trend_1524 != trend_1527

        and

        c1524["volume"]
        >
        c1527["volume"]

    )


    # --------------------------------------------------------
    # CONDITION 2
    #
    # 09:15 and 09:18 same trend
    # as 15:24
    # --------------------------------------------------------

    condition_2 = (

        trend_1524 != 0

        and

        trend_0915
        ==
        trend_1524

        and

        trend_0918
        ==
        trend_1524

    )


    # --------------------------------------------------------
    # 1-minute EOD candles
    # --------------------------------------------------------

    minute = day.set_index(
        "hm",
        drop=False
    )


    if (
        1528 not in minute.index
        or
        1529 not in minute.index
    ):

        return None


    row_1528 = minute.loc[
        1528
    ]

    row_1529 = minute.loc[
        1529
    ]


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
    # 15:28 and 15:29 opposite
    # 15:28 volume > 15:29
    # 15:28 same trend as 3m 15:24
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

        and

        trend_1528
        ==
        trend_1524

    )


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

        "direction":
            direction_name(
                trend_1524
            ),

        "condition_1":
            condition_1,

        "condition_2":
            condition_2,

        "condition_3":
            condition_3

    }


# ============================================================
# TARGET CHECK
# ============================================================

def target_hit(
    trade_day,
    entry_price,
    side,
    start_hm,
    end_hm
):

    rows = trade_day.loc[
        trade_day["hm"].between(
            start_hm,
            end_hm
        )
    ]


    if rows.empty:

        return False


    if side == "LONG":

        target_price = (
            entry_price
            *
            (
                1
                +
                TARGET_PCT / 100
            )
        )


        # LONG target is reached by HIGH.
        return bool(
            (
                rows["high"]
                >=
                target_price
            ).any()
        )


    else:

        target_price = (
            entry_price
            *
            (
                1
                -
                TARGET_PCT / 100
            )
        )


        # SHORT target is reached by LOW.
        return bool(
            (
                rows["low"]
                <=
                target_price
            ).any()
        )


# ============================================================
# PROCESS ONE STOCK
# ============================================================

def process_stock(filepath):

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

            date:
                group.reset_index(
                    drop=True
                )

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

            "final_signals":
                0,

            "long_signals":
                0,

            "short_signals":
                0,

            "executed_trades":
                0,

            "target_hits_0918":
                0,

            "target_hits_1527":
                0

        }


        # ====================================================
        # SIGNAL DAYS
        # ====================================================

        for i in range(
            len(dates) - 1
        ):

            signal_date = dates[i]

            trade_date = dates[i + 1]


            diagnostic[
                "signal_days"
            ] += 1


            signal = evaluate_signal(
                days[signal_date]
            )


            if signal is None:

                diagnostic[
                    "incomplete_days"
                ] += 1

                continue


            for n in range(
                1,
                4
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
                "final_signals"
            ] += 1


            side = signal[
                "direction"
            ]


            if side == "LONG":

                diagnostic[
                    "long_signals"
                ] += 1

            else:

                diagnostic[
                    "short_signals"
                ] += 1


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


            entry_rows = next_day.loc[
                next_day["hm"] == 915
            ]


            exit_0918_rows = next_day.loc[
                next_day["hm"] == 918
            ]


            exit_1527_rows = next_day.loc[
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
            # TARGET PRICES
            # =================================================

            if side == "LONG":

                target_price = (

                    entry_price
                    *
                    (
                        1
                        +
                        TARGET_PCT / 100
                    )

                )

            else:

                target_price = (

                    entry_price
                    *
                    (
                        1
                        -
                        TARGET_PCT / 100
                    )

                )


            # =================================================
            # 09:18 HALF
            # =================================================

            hit_0918 = target_hit(

                next_day,

                entry_price,

                side,

                915,

                917

            )


            if hit_0918:

                exit1_price = (
                    target_price
                )

                exit1_type = (
                    f"{TARGET_PCT:.2f}% TARGET"
                )

                diagnostic[
                    "target_hits_0918"
                ] += 1

            else:

                exit1_price = (
                    exit_0918_price
                )

                exit1_type = (
                    "09:18 OPEN"
                )


            # =================================================
            # 15:27 HALF
            # =================================================

            hit_1527 = target_hit(

                next_day,

                entry_price,

                side,

                915,

                1526

            )


            if hit_1527:

                exit2_price = (
                    target_price
                )

                exit2_type = (
                    f"{TARGET_PCT:.2f}% TARGET"
                )

                diagnostic[
                    "target_hits_1527"
                ] += 1

            else:

                exit2_price = (
                    exit_1527_price
                )

                exit2_type = (
                    "15:27 OPEN"
                )


            # =================================================
            # RETURNS
            # =================================================

            if side == "LONG":

                gross1 = (

                    (
                        exit1_price
                        -
                        entry_price
                    )
                    /
                    entry_price
                    *
                    100

                )


                gross2 = (

                    (
                        exit2_price
                        -
                        entry_price
                    )
                    /
                    entry_price
                    *
                    100

                )

            else:

                gross1 = (

                    (
                        entry_price
                        -
                        exit1_price
                    )
                    /
                    entry_price
                    *
                    100

                )


                gross2 = (

                    (
                        entry_price
                        -
                        exit2_price
                    )
                    /
                    entry_price
                    *
                    100

                )


            # Each half represents 50% of the position.
            #
            # Total cost is allocated proportionally.

            half_cost = (
                ROUND_TRIP_COST_PCT
                /
                2
            )


            net1 = (
                gross1
                -
                half_cost
            )


            net2 = (
                gross2
                -
                half_cost
            )


            combined = (

                net1 * 0.5

                +

                net2 * 0.5

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

                "target_price":
                    target_price,

                "exit1_time":
                    exit1_type,

                "exit1_price":
                    exit1_price,

                "exit1_gross_pct":
                    gross1,

                "exit1_net_pct":
                    net1,

                "exit2_time":
                    exit2_type,

                "exit2_price":
                    exit2_price,

                "exit2_gross_pct":
                    gross2,

                "exit2_net_pct":
                    net2,

                "combined_net_return_pct":
                    combined

            })


            diagnostic[
                "executed_trades"
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

def statistics(
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


    profit_factor = (

        gross_profit
        /
        gross_loss

        if gross_loss > 0

        else float("inf")

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
    print("NSE SCANNER FAST BACKTEST")
    print("=" * 90)

    print(
        f"Date range          : "
        f"{START_DATE} → {END_DATE}"
    )

    print(
        f"Trade side          : "
        f"{TRADE_SIDE}"
    )

    print(
        f"Target              : "
        f"{TARGET_PCT}%"
    )

    print(
        f"Round-trip cost     : "
        f"{ROUND_TRIP_COST_PCT}%"
    )

    print(
        f"Parallel workers    : "
        f"{MAX_WORKERS}"
    )

    print()
    print(
        "Entry               : 09:15 OPEN"
    )

    print(
        "Exit 1              : 09:18 OPEN "
        "or target"
    )

    print(
        "Exit 2              : 15:27 OPEN "
        "or target"
    )

    print(
        "Position split      : 50% / 50%"
    )

    print("=" * 90)
    print()


    files = find_parquet_files()


    print(
        f"Parquet files found: "
        f"{len(files)}"
    )


    if not files:

        raise RuntimeError(
            "No Parquet files found."
        )


    all_trades = []

    all_diagnostics = []


    # ========================================================
    # PARALLEL PROCESSING
    # ========================================================

    with ProcessPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {

            executor.submit(
                process_stock,
                filepath
            ):
            filepath

            for filepath in files

        }


        completed = 0


        for future in as_completed(
            futures
        ):

            filepath = futures[
                future
            ]


            completed += 1


            trades, diagnostic = (
                future.result()
            )


            all_trades.extend(
                trades
            )


            all_diagnostics.append(
                diagnostic
            )


            print(

                f"[{completed:>4}/"
                f"{len(files):>4}] "

                f"{diagnostic.get('symbol',''):<18} "

                f"{diagnostic.get('status',''):<8} "

                f"signals="
                f"{diagnostic.get('final_signals',0):>4} "

                f"trades="
                f"{len(trades):>4}"

            )


    # ========================================================
    # DIAGNOSTICS
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


    # ========================================================
    # TRADES
    # ========================================================

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
                "target_price",

                "exit1_time",
                "exit1_price",
                "exit1_gross_pct",
                "exit1_net_pct",

                "exit2_time",
                "exit2_price",
                "exit2_gross_pct",
                "exit2_net_pct",

                "combined_net_return_pct"

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


    # ========================================================
    # PERFORMANCE
    # ========================================================

    combined_returns = (

        trades_df[
            "combined_net_return_pct"
        ].tolist()

        if not trades_df.empty

        else []

    )


    exit1_returns = (

        trades_df[
            "exit1_net_pct"
        ].tolist()

        if not trades_df.empty

        else []

    )


    exit2_returns = (

        trades_df[
            "exit2_net_pct"
        ].tolist()

        if not trades_df.empty

        else []

    )


    results = {

        "50/50 COMBINED":
            statistics(
                combined_returns
            ),

        "09:18 EXIT":
            statistics(
                exit1_returns
            ),

        "15:27 EXIT":
            statistics(
                exit2_returns
            )

    }


    result_rows = []


    for name_, result in results.items():

        result_rows.append({

            "exit":
                name_,

            **result

        })


    pd.DataFrame(
        result_rows
    ).to_csv(

        RESULTS_DIR
        /
        "exit_statistics.csv",

        index=False

    )


    # ========================================================
    # LONG / SHORT
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


        returns = (

            subset[
                "combined_net_return_pct"
            ].tolist()

            if not subset.empty

            else []

        )


        result = statistics(
            returns
        )


        side_rows.append({

            "direction":
                side,

            **result

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
    # YEARLY RESULTS
    # ========================================================

    if not trades_df.empty:

        trades_df["year"] = (

            pd.to_datetime(
                trades_df["trade_date"]
            )
            .dt.year

        )


        yearly_rows = []


        for year, group in (
            trades_df.groupby("year")
        ):

            returns = group[
                "combined_net_return_pct"
            ].tolist()


            yearly_rows.append({

                "year":
                    int(year),

                **statistics(
                    returns
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


    # ========================================================
    # FINAL OUTPUT
    # ========================================================

    print()
    print("=" * 90)
    print("FINAL RESULT")
    print("=" * 90)


    for name_, result in results.items():

        print()
        print(name_)
        print("-" * 60)

        print(
            f"Trades          : "
            f"{result['trades']}"
        )

        print(
            f"Wins            : "
            f"{result['wins']}"
        )

        print(
            f"Losses          : "
            f"{result['losses']}"
        )

        print(
            f"Win rate        : "
            f"{result['win_rate_pct']:.2f}%"
        )

        print(
            f"Average return  : "
            f"{result['average_return_pct']:.4f}%"
        )

        print(
            f"Total return    : "
            f"{result['total_return_pct']:.4f}%"
        )

        print(
            f"Profit factor   : "
            f"{result['profit_factor']}"
        )


    print()
    print("=" * 90)
    print("SIGNAL DIAGNOSTICS")
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
            f"{diagnostics_df['final_signals'].sum()}"
        )

        print(
            f"Target hits 09:18   : "
            f"{diagnostics_df['target_hits_0918'].sum()}"
        )

        print(
            f"Target hits 15:27   : "
            f"{diagnostics_df['target_hits_1527'].sum()}"
        )


    print()
    print(
        f"Runtime: "
        f"{(time.time() - start_time) / 60:.2f} minutes"
    )

    print()
    print(
        "Results saved to results/"
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
