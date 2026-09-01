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

START_DATE = os.getenv(
    "START_DATE",
    "2018-01-01"
)

END_DATE = os.getenv(
    "END_DATE",
    "2026-08-31"
)

TRADE_SIDE = os.getenv(
    "TRADE_SIDE",
    "SHORT"
).upper()

# Total estimated cost for one complete trade.
ROUND_TRIP_COST_PCT = float(
    os.getenv(
        "ROUND_TRIP_COST_PCT",
        "0.10"
    )
)

# Target percentage.
TARGET_PCT = float(
    os.getenv(
        "TARGET_PCT",
        "1.0"
    )
)

# Number of stocks processed simultaneously.
MAX_WORKERS = int(
    os.getenv(
        "MAX_WORKERS",
        str(min(4, os.cpu_count() or 2))
    )
)


# ============================================================
# RESULTS DIRECTORY
# ============================================================

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
# FIND HISTORICAL PARQUET FILES
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
    # Flatten MultiIndex columns if present.
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
                not in ("", "None")

            )

            for column in df.columns

        ]


    # --------------------------------------------------------
    # Column lookup.
    # --------------------------------------------------------

    columns = {

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

                selected[target] = (
                    columns[name]
                )

                break


    # --------------------------------------------------------
    # Datetime may be stored as index.
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

                str(column)
                .strip()
                .lower():

                column

                for column in df.columns

            }


            for name in aliases["datetime"]:

                if name in columns:

                    selected["datetime"] = (
                        columns[name]
                    )

                    break


    # --------------------------------------------------------
    # Verify required columns.
    # --------------------------------------------------------

    required = [

        "datetime",
        "open",
        "high",
        "low",
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

            f"Missing required columns: "
            f"{missing}. "

            f"Available columns: "
            f"{list(df.columns)}"

        )


    # --------------------------------------------------------
    # Keep only required data.
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


    # --------------------------------------------------------
    # Convert datetime.
    # --------------------------------------------------------

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
    # Convert timezone to IST if necessary.
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
    # Date and HHMM.
    # --------------------------------------------------------

    output["date"] = (

        output["datetime"]
        .dt.date

    )


    output["hm"] = (

        output["datetime"].dt.hour
        *
        100

        +

        output["datetime"].dt.minute

    )


    # --------------------------------------------------------
    # NSE session.
    # --------------------------------------------------------

    output = output[

        (output["hm"] >= 915)

        &

        (output["hm"] <= 1529)

    ]


    # --------------------------------------------------------
    # Requested date range.
    # --------------------------------------------------------

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
    # Sort and remove duplicate minute records.
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

    required_minutes = [

        start_minute,

        start_minute + 1,

        start_minute + 2

    ]


    rows = day.loc[

        day["hm"].isin(
            required_minutes
        )

    ]


    if len(rows) != 3:

        return None


    if set(

        rows["hm"].astype(int)

    ) != set(

        required_minutes

    ):

        return None


    rows = rows.sort_values(
        "hm"
    )


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
# EVALUATE PREVIOUS DAY SIGNAL
# ============================================================

def evaluate_signal(day):

    # --------------------------------------------------------
    # Required 1-minute candles.
    # --------------------------------------------------------

    required_minutes = {

        # 09:15 3-minute candle
        915,
        916,
        917,

        # 09:18 3-minute candle
        918,
        919,
        920,

        # 15:24 3-minute candle
        1524,
        1525,
        1526,

        # 15:27 3-minute candle
        1527,
        1528,
        1529

    }


    available = set(

        day["hm"].astype(int)

    )


    if not required_minutes.issubset(
        available
    ):

        return None


    # ========================================================
    # 3-MINUTE CANDLES
    # ========================================================

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


    # ========================================================
    # 3-MINUTE TRENDS
    # ========================================================

    trend_1524 = direction(

        candle_1524["open"],

        candle_1524["close"]

    )


    trend_1527 = direction(

        candle_1527["open"],

        candle_1527["close"]

    )


    trend_0915 = direction(

        candle_0915["open"],

        candle_0915["close"]

    )


    trend_0918 = direction(

        candle_0918["open"],

        candle_0918["close"]

    )


    # ========================================================
    # CONDITION 1
    #
    # NEW RULE:
    #
    # 15:24 and 15:27 MUST BE SAME TREND
    #
    # 15:24 volume MUST BE GREATER
    # ========================================================

    condition_1 = (

        trend_1524 != 0

        and

        trend_1527 != 0

        and

        trend_1524 == trend_1527

        and

        candle_1524["volume"]
        >
        candle_1527["volume"]

    )


    # ========================================================
    # CONDITION 2
    #
    # 3-MINUTE 09:15 AND 09:18
    #
    # BOTH MUST MATCH 15:24
    # ========================================================

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


    # ========================================================
    # 1-MINUTE DATA
    # ========================================================

    minute_data = (

        day

        .set_index(
            "hm",
            drop=False
        )

    )


    if (

        1528 not in minute_data.index

        or

        1529 not in minute_data.index

    ):

        return None


    row_1528 = minute_data.loc[
        1528
    ]


    row_1529 = minute_data.loc[
        1529
    ]


    # ========================================================
    # 1-MINUTE TRENDS
    # ========================================================

    trend_1528 = direction(

        float(
            row_1528["open"]
        ),

        float(
            row_1528["close"]
        )

    )


    trend_1529 = direction(

        float(
            row_1529["open"]
        ),

        float(
            row_1529["close"]
        )

    )


    # ========================================================
    # CONDITION 3
    #
    # 15:28 and 15:29 MUST BE OPPOSITE
    #
    # 15:28 volume > 15:29
    #
    # 15:28 trend = 3m 15:24 trend
    # ========================================================

    condition_3 = (

        trend_1528 != 0

        and

        trend_1529 != 0

        and

        trend_1528 != trend_1529

        and

        float(
            row_1528["volume"]
        )
        >
        float(
            row_1529["volume"]
        )

        and

        trend_1528
        ==
        trend_1524

    )


    # ========================================================
    # FINAL SIGNAL
    # ========================================================

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
# CHECK 1% TARGET
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


        # LONG target uses HIGH.

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


        # SHORT target uses LOW.

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

        # ----------------------------------------------------
        # Read data
        # ----------------------------------------------------

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
                    "NO_DATA",

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

                "target_hits":
                    0

            }


        # ----------------------------------------------------
        # Group by trading day.
        # ----------------------------------------------------

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

            "final_signals":
                0,

            "long_signals":
                0,

            "short_signals":
                0,

            "executed_trades":
                0,

            "target_hits":
                0

        }


        # ====================================================
        # SIGNAL-DAY LOOP
        # ====================================================

        for i in range(
            len(dates) - 1
        ):

            signal_date = dates[i]

            trade_date = dates[i + 1]


            diagnostics[
                "signal_days"
            ] += 1


            signal = evaluate_signal(

                days[signal_date]

            )


            if signal is None:

                diagnostics[
                    "incomplete_days"
                ] += 1

                continue


            # ------------------------------------------------
            # Diagnostics
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
                "final_signals"
            ] += 1


            side = signal[
                "direction"
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
            # Trade-side filter.
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


            # -------------------------------------------------
            # Entry
            # -------------------------------------------------

            entry_rows = next_day.loc[

                next_day["hm"] == 915

            ]


            # -------------------------------------------------
            # Scheduled exit
            # -------------------------------------------------

            exit_rows = next_day.loc[

                next_day["hm"] == 1527

            ]


            if (

                entry_rows.empty

                or

                exit_rows.empty

            ):

                continue


            entry_price = float(

                entry_rows.iloc[0][
                    "open"
                ]

            )


            scheduled_exit_price = float(

                exit_rows.iloc[0][
                    "open"
                ]

            )


            if (

                not math.isfinite(
                    entry_price
                )

                or

                not math.isfinite(
                    scheduled_exit_price
                )

                or

                entry_price <= 0

            ):

                continue


            # =================================================
            # TARGET PRICE
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
            # TARGET CHECK
            #
            # We check from 09:15 through 15:26.
            #
            # 15:27 is the scheduled exit.
            # =================================================

            target_reached = target_hit(

                next_day,

                entry_price,

                side,

                915,

                1526

            )


            if target_reached:

                exit_price = target_price

                exit_type = (

                    f"{TARGET_PCT:.2f}% TARGET"

                )

                diagnostics[
                    "target_hits"
                ] += 1

            else:

                exit_price = (
                    scheduled_exit_price
                )

                exit_type = (
                    "15:27 OPEN"
                )


            # =================================================
            # RETURN
            # =================================================

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

                "target_price":
                    target_price,

                "exit_type":
                    exit_type,

                "exit_price":
                    exit_price,

                "gross_return_pct":
                    gross_return,

                "net_return_pct":
                    net_return,

                "target_hit":
                    target_reached

            })


            diagnostics[
                "executed_trades"
            ] += 1


        return trades, diagnostics


    except Exception as error:

        return [], {

            "symbol":
                symbol,

            "status":
                "ERROR",

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

            "target_hits":
                0,

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
            int(
                len(series)
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
    print("NSE SCANNER BACKTEST")
    print("15:24 / 15:27 SAME TREND VERSION")
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
        f"Target           : {TARGET_PCT}%"
    )

    print(
        f"Round-trip cost  : "
        f"{ROUND_TRIP_COST_PCT}%"
    )

    print(
        f"Workers          : {MAX_WORKERS}"
    )

    print()
    print(
        "Entry            : NEXT DAY 09:15 OPEN"
    )

    print(
        "Exit             : 1% TARGET OR 15:27 OPEN"
    )

    print("=" * 90)
    print()


    # ========================================================
    # FIND FILES
    # ========================================================

    files = find_parquet_files()


    print(
        f"Parquet files found: "
        f"{len(files)}"
    )


    if not files:

        raise RuntimeError(
            "No historical Parquet files found."
        )


    all_trades = []

    all_diagnostics = []


    # ========================================================
    # PARALLEL STOCK PROCESSING
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

            completed += 1


            filepath = futures[
                future
            ]


            try:

                trades, diagnostic = (
                    future.result()
                )

            except Exception as error:

                trades = []

                diagnostic = {

                    "symbol":
                        Path(
                            filepath
                        ).stem,

                    "status":
                        "ERROR",

                    "error":
                        repr(error)

                }


            all_trades.extend(
                trades
            )


            all_diagnostics.append(
                diagnostic
            )


            print(

                f"[{completed:>4}/"
                f"{len(files):>4}] "

                f"{diagnostic.get('symbol', ''):<18} "

                f"{diagnostic.get('status', ''):<10} "

                f"signals="
                f"{diagnostic.get('final_signals', 0):>5} "

                f"trades="
                f"{len(trades):>5}"

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
    # TRADE DATA
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

                "exit_type",

                "exit_price",

                "gross_return_pct",

                "net_return_pct",

                "target_hit"

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
    # OVERALL STATISTICS
    # ========================================================

    returns = (

        trades_df[
            "net_return_pct"
        ].tolist()

        if not trades_df.empty

        else []

    )


    overall = calculate_statistics(
        returns
    )


    pd.DataFrame(

        [overall]

    ).to_csv(

        RESULTS_DIR
        /
        "overall_statistics.csv",

        index=False

    )


    # ========================================================
    # TARGET STATISTICS
    # ========================================================

    target_hits = int(

        trades_df[
            "target_hit"
        ].sum()

        if not trades_df.empty

        else 0

    )


    total_trades = len(
        trades_df
    )


    target_hit_pct = (

        target_hits
        /
        total_trades
        *
        100

        if total_trades > 0

        else 0

    )


    target_statistics = {

        "total_trades":
            total_trades,

        "target_hits":
            target_hits,

        "target_hit_pct":
            target_hit_pct

    }


    pd.DataFrame(

        [target_statistics]

    ).to_csv(

        RESULTS_DIR
        /
        "target_statistics.csv",

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

            trades_df[
                "direction"
            ]
            ==
            side

        ]


        side_returns = (

            subset[
                "net_return_pct"
            ].tolist()

            if not subset.empty

            else []

        )


        stats = calculate_statistics(
            side_returns
        )


        side_target_hits = int(

            subset[
                "target_hit"
            ].sum()

            if not subset.empty

            else 0

        )


        side_target_pct = (

            side_target_hits
            /
            len(subset)
            *
            100

            if len(subset) > 0

            else 0

        )


        side_rows.append({

            "direction":
                side,

            **stats,

            "target_hits":
                side_target_hits,

            "target_hit_pct":
                side_target_pct

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
    # YEARLY STATISTICS
    # ========================================================

    if not trades_df.empty:

        trades_df["year"] = (

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

            year_returns = group[

                "net_return_pct"

            ].tolist()


            yearly_rows.append({

                "year":
                    int(year),

                **calculate_statistics(
                    year_returns
                ),

                "target_hits":
                    int(
                        group[
                            "target_hit"
                        ].sum()
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
    # FINAL RESULT
    # ========================================================

    print()
    print("=" * 90)
    print("FINAL RESULT")
    print("=" * 90)


    print(
        f"Trades          : "
        f"{overall['trades']}"
    )


    print(
        f"Wins            : "
        f"{overall['wins']}"
    )


    print(
        f"Losses          : "
        f"{overall['losses']}"
    )


    print(
        f"Win rate        : "
        f"{overall['win_rate_pct']:.2f}%"
    )


    print(
        f"Average return  : "
        f"{overall['average_return_pct']:.4f}%"
    )


    print(
        f"Total return    : "
        f"{overall['total_return_pct']:.4f}%"
    )


    print(
        f"Profit factor   : "
        f"{overall['profit_factor']}"
    )


    print(
        f"Best trade      : "
        f"{overall['best_trade_pct']:.4f}%"
    )


    print(
        f"Worst trade     : "
        f"{overall['worst_trade_pct']:.4f}%"
    )


    print()
    print("=" * 90)
    print("TARGET")
    print("=" * 90)


    print(
        f"Target hits     : "
        f"{target_hits}"
    )


    print(
        f"Target hit rate : "
        f"{target_hit_pct:.2f}%"
    )


    print()
    print("=" * 90)
    print("SIGNAL DIAGNOSTICS")
    print("=" * 90)


    if not diagnostics_df.empty:

        print(

            f"Signal days checked : "
            f"{diagnostics_df['signal_days'].fillna(0).sum():,.0f}"

        )


        print(

            f"Incomplete days     : "
            f"{diagnostics_df['incomplete_days'].fillna(0).sum():,.0f}"

        )


        print(

            f"Condition 1 passed  : "
            f"{diagnostics_df['condition_1'].fillna(0).sum():,.0f}"

        )


        print(

            f"Condition 2 passed  : "
            f"{diagnostics_df['condition_2'].fillna(0).sum():,.0f}"

        )


        print(

            f"Condition 3 passed  : "
            f"{diagnostics_df['condition_3'].fillna(0).sum():,.0f}"

        )


        print(

            f"Final signals       : "
            f"{diagnostics_df['final_signals'].fillna(0).sum():,.0f}"

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
