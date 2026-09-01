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


# ============================================================
# NEW RISK MANAGEMENT
# ============================================================

TARGET_PCT = float(
    os.getenv(
        "TARGET_PCT",
        "2.0"
    )
)

STOP_LOSS_PCT = float(
    os.getenv(
        "STOP_LOSS_PCT",
        "1.0"
    )
)


# Estimated total cost for entry + exit.
ROUND_TRIP_COST_PCT = float(
    os.getenv(
        "ROUND_TRIP_COST_PCT",
        "0.10"
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
# RESULTS
# ============================================================

RESULTS_DIR = Path("results")

RESULTS_DIR.mkdir(
    exist_ok=True
)


# ============================================================
# DIRECTION
# ============================================================

def direction(
    open_price,
    close_price
):

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

        if not os.path.isdir(
            directory
        ):

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
# NORMALIZE DATA
# ============================================================

def normalize_data(df):

    # --------------------------------------------------------
    # Flatten MultiIndex columns.
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
    # Datetime could be the index.
    # --------------------------------------------------------

    if "datetime" not in selected:

        index_name = ""


        if df.index.name is not None:

            index_name = (

                str(df.index.name)
                .strip()
                .lower()

            )


        if index_name in aliases[
            "datetime"
        ]:

            df = df.reset_index()


            columns = {

                str(column)
                .strip()
                .lower():

                column

                for column in df.columns

            }


            for name in aliases[
                "datetime"
            ]:

                if name in columns:

                    selected[
                        "datetime"
                    ] = columns[name]

                    break


    # --------------------------------------------------------
    # Required columns.
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

            f"Missing columns: "
            f"{missing}; "

            f"Available columns: "
            f"{list(df.columns)}"

        )


    # --------------------------------------------------------
    # Keep only needed columns.
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
    # Convert timezone to IST.
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
    # Date / HHMM.
    # --------------------------------------------------------

    output["date"] = (

        output["datetime"]
        .dt.date

    )


    output["hm"] = (

        output["datetime"]
        .dt.hour
        *
        100

        +

        output["datetime"]
        .dt.minute

    )


    # --------------------------------------------------------
    # NSE regular session.
    # --------------------------------------------------------

    output = output[

        (output["hm"] >= 915)

        &

        (output["hm"] <= 1529)

    ]


    # --------------------------------------------------------
    # Date range.
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
    # Sort / deduplicate.
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
# CREATE 3-MINUTE CANDLE
# ============================================================

def make_3m_candle(
    day,
    start_minute
):

    required = [

        start_minute,

        start_minute + 1,

        start_minute + 2

    ]


    rows = day.loc[

        day["hm"].isin(
            required
        )

    ]


    if len(rows) != 3:

        return None


    if set(

        rows["hm"].astype(int)

    ) != set(required):

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
# EVALUATE PREVIOUS-DAY SIGNAL
# ============================================================

def evaluate_signal(day):

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

    c1524 = make_3m_candle(

        day,

        1524

    )


    c1527 = make_3m_candle(

        day,

        1527

    )


    c0915 = make_3m_candle(

        day,

        915

    )


    c0918 = make_3m_candle(

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


    # ========================================================
    # TRENDS
    # ========================================================

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


    # ========================================================
    # CONDITION 1
    #
    # 15:24 AND 15:27 MUST BE SAME TREND
    #
    # 15:24 VOLUME > 15:27 VOLUME
    # ========================================================

    condition_1 = (

        trend_1524 != 0

        and

        trend_1527 != 0

        and

        trend_1524 == trend_1527

        and

        c1524["volume"]
        >
        c1527["volume"]

    )


    # ========================================================
    # CONDITION 2
    #
    # 09:15 AND 09:18 MUST MATCH 15:24
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
    # 1-MINUTE 15:28 / 15:29
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
    # 15:28 AND 15:29 OPPOSITE
    #
    # 15:28 VOLUME > 15:29
    #
    # 15:28 TREND = 3-MIN 15:24 TREND
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
# CHECK TARGET / STOP-LOSS
# ============================================================

def check_exit(

    trade_day,

    entry_price,

    side

):

    # --------------------------------------------------------
    # We start checking AFTER the 09:15 entry.
    #
    # 15:27 is the scheduled exit.
    #
    # Therefore target / SL are checked through 15:26.
    # --------------------------------------------------------

    rows = trade_day.loc[

        trade_day["hm"].between(
            915,
            1526
        )

    ].sort_values(
        "hm"
    )


    if rows.empty:

        return {

            "exit_type":
                "15:27 OPEN",

            "exit_price":
                None,

            "target_hit":
                False,

            "stop_loss_hit":
                False,

            "exit_hm":
                None

        }


    # ========================================================
    # TARGET / STOP PRICE
    # ========================================================

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


        stop_price = (

            entry_price

            *

            (
                1
                -
                STOP_LOSS_PCT / 100
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


        stop_price = (

            entry_price

            *

            (
                1
                +
                STOP_LOSS_PCT / 100
            )

        )


    # ========================================================
    # CHECK EVERY 1-MINUTE CANDLE
    # ========================================================

    for _, row in rows.iterrows():

        high = float(
            row["high"]
        )

        low = float(
            row["low"]
        )


        if side == "LONG":

            target_touched = (

                high
                >=
                target_price

            )


            stop_touched = (

                low
                <=
                stop_price

            )

        else:

            target_touched = (

                low
                <=
                target_price

            )


            stop_touched = (

                high
                >=
                stop_price

            )


        # ====================================================
        # BOTH HIT IN SAME CANDLE
        #
        # Conservative assumption:
        # STOP-LOSS happens first.
        # ====================================================

        if (
            target_touched
            and
            stop_touched
        ):

            return {

                "exit_type":
                    "STOP LOSS",

                "exit_price":
                    stop_price,

                "target_hit":
                    False,

                "stop_loss_hit":
                    True,

                "exit_hm":
                    int(row["hm"])

            }


        if target_touched:

            return {

                "exit_type":
                    f"{TARGET_PCT:.2f}% TARGET",

                "exit_price":
                    target_price,

                "target_hit":
                    True,

                "stop_loss_hit":
                    False,

                "exit_hm":
                    int(row["hm"])

            }


        if stop_touched:

            return {

                "exit_type":
                    f"{STOP_LOSS_PCT:.2f}% STOP LOSS",

                "exit_price":
                    stop_price,

                "target_hit":
                    False,

                "stop_loss_hit":
                    True,

                "exit_hm":
                    int(row["hm"])

            }


    # ========================================================
    # NEITHER TARGET NOR SL
    # ========================================================

    return {

        "exit_type":
            "15:27 OPEN",

        "exit_price":
            None,

        "target_hit":
            False,

        "stop_loss_hit":
            False,

        "exit_hm":
            None

    }


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
                    0,

                "stop_loss_hits":
                    0

            }


        # ----------------------------------------------------
        # Group data by date.
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
                0,

            "stop_loss_hits":
                0

        }


        # ====================================================
        # SIGNAL LOOP
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
            # Condition diagnostics.
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
            # Side filter.
            # ------------------------------------------------

            if (

                TRADE_SIDE != "BOTH"

                and

                side != TRADE_SIDE

            ):

                continue


            # =================================================
            # NEXT DAY
            # =================================================

            next_day = days[
                trade_date
            ]


            entry_rows = next_day.loc[

                next_day["hm"] == 915

            ]


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
            # EXIT LOGIC
            # =================================================

            exit_result = check_exit(

                next_day,

                entry_price,

                side

            )


            if exit_result[
                "exit_price"
            ] is None:

                exit_price = (
                    scheduled_exit_price
                )

            else:

                exit_price = (
                    exit_result[
                        "exit_price"
                    ]
                )


            exit_type = (
                exit_result[
                    "exit_type"
                ]
            )


            if exit_result[
                "target_hit"
            ]:

                diagnostics[
                    "target_hits"
                ] += 1


            if exit_result[
                "stop_loss_hit"
            ]:

                diagnostics[
                    "stop_loss_hits"
                ] += 1


            # =================================================
            # CALCULATE GROSS RETURN
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


            # =================================================
            # NET RETURN
            # =================================================

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
                    str(
                        signal_date
                    ),

                "trade_date":
                    str(
                        trade_date
                    ),

                "direction":
                    side,

                "entry_0915_open":
                    entry_price,

                "target_price":

                    (

                        entry_price
                        *
                        (
                            1
                            +
                            TARGET_PCT / 100
                        )

                        if side == "LONG"

                        else

                        entry_price
                        *
                        (
                            1
                            -
                            TARGET_PCT / 100
                        )

                    ),

                "stop_loss_price":

                    (

                        entry_price
                        *
                        (
                            1
                            -
                            STOP_LOSS_PCT / 100
                        )

                        if side == "LONG"

                        else

                        entry_price
                        *
                        (
                            1
                            +
                            STOP_LOSS_PCT / 100
                        )

                    ),

                "exit_type":
                    exit_type,

                "exit_time":
                    (
                        exit_result[
                            "exit_hm"
                        ]

                        if exit_result[
                            "exit_hm"
                        ] is not None

                        else 1527
                    ),

                "exit_price":
                    exit_price,

                "gross_return_pct":
                    gross_return,

                "net_return_pct":
                    net_return,

                "target_hit":
                    exit_result[
                        "target_hit"
                    ],

                "stop_loss_hit":
                    exit_result[
                        "stop_loss_hit"
                    ]

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

            "stop_loss_hits":
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
    print("2% TARGET / 1% STOP-LOSS")
    print("=" * 90)


    print(
        f"Start date       : "
        f"{START_DATE}"
    )


    print(
        f"End date         : "
        f"{END_DATE}"
    )


    print(
        f"Trade side       : "
        f"{TRADE_SIDE}"
    )


    print(
        f"Target           : "
        f"{TARGET_PCT}%"
    )


    print(
        f"Stop-loss        : "
        f"{STOP_LOSS_PCT}%"
    )


    print(
        f"Round-trip cost  : "
        f"{ROUND_TRIP_COST_PCT}%"
    )


    print(
        f"Workers          : "
        f"{MAX_WORKERS}"
    )


    print()
    print(
        "Entry            : "
        "NEXT DAY 09:15 OPEN"
    )


    print(
        "Exit             : "
        "2% TARGET / 1% SL / 15:27 OPEN"
    )


    print()
    print(
        "If target + SL are both touched "
        "in the same minute:"
    )


    print(
        "CONSERVATIVE RULE → STOP-LOSS FIRST"
    )


    print("=" * 90)
    print()


    # ========================================================
    # FILES
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
    # DIAGNOSTICS FILE
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
    # TRADES DATAFRAME
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

                "stop_loss_price",

                "exit_type",

                "exit_time",

                "exit_price",

                "gross_return_pct",

                "net_return_pct",

                "target_hit",

                "stop_loss_hit"

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


    # ========================================================
    # EXIT TYPE STATISTICS
    # ========================================================

    exit_rows = []


    for exit_type, group in (

        trades_df.groupby(
            "exit_type"
        )

        if not trades_df.empty

        else []

    ):

        exit_returns = group[
            "net_return_pct"
        ].tolist()


        stats = calculate_statistics(
            exit_returns
        )


        exit_rows.append({

            "exit_type":
                exit_type,

            **stats

        })


    pd.DataFrame(
        exit_rows
    ).to_csv(

        RESULTS_DIR
        /
        "exit_type_statistics.csv",

        index=False

    )


    # ========================================================
    # TARGET / STOP STATISTICS
    # ========================================================

    total_trades = len(
        trades_df
    )


    target_hits = int(

        trades_df[
            "target_hit"
        ].sum()

        if not trades_df.empty

        else 0

    )


    stop_losses = int(

        trades_df[
            "stop_loss_hit"
        ].sum()

        if not trades_df.empty

        else 0

    )


    scheduled_exits = (

        total_trades
        -
        target_hits
        -
        stop_losses

    )


    target_rate = (

        target_hits
        /
        total_trades
        *
        100

        if total_trades > 0

        else 0

    )


    stop_rate = (

        stop_losses
        /
        total_trades
        *
        100

        if total_trades > 0

        else 0

    )


    scheduled_rate = (

        scheduled_exits
        /
        total_trades
        *
        100

        if total_trades > 0

        else 0

    )


    pd.DataFrame([{

        "total_trades":
            total_trades,

        "target_hits":
            target_hits,

        "target_hit_rate_pct":
            target_rate,

        "stop_loss_hits":
            stop_losses,

        "stop_loss_rate_pct":
            stop_rate,

        "15:27_exits":
            scheduled_exits,

        "15:27_exit_rate_pct":
            scheduled_rate

    }]).to_csv(

        RESULTS_DIR
        /
        "risk_management_statistics.csv",

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


        side_targets = int(

            subset[
                "target_hit"
            ].sum()

            if not subset.empty

            else 0

        )


        side_stops = int(

            subset[
                "stop_loss_hit"
            ].sum()

            if not subset.empty

            else 0

        )


        side_rows.append({

            "direction":
                side,

            **stats,

            "target_hits":
                side_targets,

            "stop_loss_hits":
                side_stops

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
                    ),

                "stop_loss_hits":
                    int(
                        group[
                            "stop_loss_hit"
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
    # WORST TRADES
    # ========================================================

    if not trades_df.empty:

        worst_trades = (

            trades_df

            .sort_values(

                "net_return_pct",

                ascending=True

            )

            .head(50)

        )


        worst_trades.to_csv(

            RESULTS_DIR
            /
            "worst_50_trades.csv",

            index=False

        )


    # ========================================================
    # BEST TRADES
    # ========================================================

    if not trades_df.empty:

        best_trades = (

            trades_df

            .sort_values(

                "net_return_pct",

                ascending=False

            )

            .head(50)

        )


        best_trades.to_csv(

            RESULTS_DIR
            /
            "best_50_trades.csv",

            index=False

        )


    # ========================================================
    # PRINT FINAL RESULT
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


    # ========================================================
    # RISK MANAGEMENT RESULTS
    # ========================================================

    print()
    print("=" * 90)
    print("TARGET / STOP-LOSS")
    print("=" * 90)


    print(

        f"2% target hits  : "
        f"{target_hits}"

    )


    print(

        f"Target hit rate : "
        f"{target_rate:.2f}%"

    )


    print(

        f"1% SL hits      : "
        f"{stop_losses}"

    )


    print(

        f"Stop-loss rate  : "
        f"{stop_rate:.2f}%"

    )


    print(

        f"15:27 exits     : "
        f"{scheduled_exits}"

    )


    print(

        f"15:27 exit rate : "
        f"{scheduled_rate:.2f}%"

    )


    # ========================================================
    # DIAGNOSTICS
    # ========================================================

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
