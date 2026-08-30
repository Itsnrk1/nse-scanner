# ============================================================
# DIAGNOSTIC BACKTEST
#
# 3-MIN + 1-MIN PREVIOUS-DAY PATTERN
#
# ============================================================
#
# PREVIOUS DAY:
#
# 3-MIN:
#   15:24 and 15:27 opposite
#   Volume(15:27) > Volume(15:24)
#   09:15 and 09:18 same as 15:27
#
# 1-MIN:
#   15:28 and 15:29 opposite
#   Volume(15:28) > Volume(15:29)
#   15:28 same trend as 3-min 15:27
#   09:15 and 09:16 same as 15:28
#
# NEXT DAY:
#   09:15 OPEN = ENTRY
#   15:27 OPEN = EXIT
#
# ============================================================

import pandas as pd
import numpy as np
import os
import glob


# ============================================================
# SETTINGS
# ============================================================

DATA_FOLDER = "./data"

ROUND_TRIP_COST = 0.10


# ============================================================
# TREND
# ============================================================

def get_trend(open_price, close_price):

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


# ============================================================
# LOAD DATA
# ============================================================

def load_data(file_path):

    df = pd.read_csv(file_path)

    # --------------------------------------------------------
    # Find datetime column
    # --------------------------------------------------------

    datetime_col = None

    for col in [
        "datetime",
        "Datetime",
        "date_time",
        "timestamp",
        "time",
        "Date"
    ]:

        if col in df.columns:
            datetime_col = col
            break

    if datetime_col is None:

        raise ValueError(
            f"No datetime column found in {file_path}"
        )

    df["datetime"] = pd.to_datetime(
        df[datetime_col],
        errors="coerce"
    )

    # --------------------------------------------------------
    # Standardize columns
    # --------------------------------------------------------

    rename = {}

    for col in df.columns:

        c = col.lower()

        if c == "open":
            rename[col] = "open"

        elif c == "high":
            rename[col] = "high"

        elif c == "low":
            rename[col] = "low"

        elif c == "close":
            rename[col] = "close"

        elif c in ["volume", "vol"]:
            rename[col] = "volume"

    df = df.rename(columns=rename)

    required = [
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    missing = [
        x for x in required
        if x not in df.columns
    ]

    if missing:

        raise ValueError(
            f"Missing columns: {missing}"
        )

    # --------------------------------------------------------
    # Numeric conversion
    # --------------------------------------------------------

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

    # --------------------------------------------------------
    # Remove bad rows
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
    # Date and time
    # --------------------------------------------------------

    df["date"] = (
        df["datetime"]
        .dt.date
    )

    df["time"] = (
        df["datetime"]
        .dt.strftime("%H:%M")
    )

    # --------------------------------------------------------
    # Regular NSE session
    # --------------------------------------------------------

    df = df[
        (df["time"] >= "09:15")
        &
        (df["time"] <= "15:29")
    ].copy()

    return df


# ============================================================
# CREATE 3-MIN CANDLES
# ============================================================

def create_3min(df):

    x = df.copy()

    minutes = (
        x["datetime"].dt.hour * 60
        +
        x["datetime"].dt.minute
    )

    market_open = 9 * 60 + 15

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

    hour = candle_start // 60
    minute = candle_start % 60

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
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            source_bars=("close", "count")
        )
        .reset_index()
    )

    return candles


# ============================================================
# GET 3-MIN CANDLE
# ============================================================

def get_3m(
    candles,
    date,
    time
):

    result = candles[
        (candles["date"] == date)
        &
        (candles["candle_time"] == time)
    ]

    if result.empty:
        return None

    return result.iloc[0]


# ============================================================
# GET 1-MIN CANDLE
# ============================================================

def get_1m(
    df,
    date,
    time
):

    result = df[
        (df["date"] == date)
        &
        (df["time"] == time)
    ]

    if result.empty:
        return None

    return result.iloc[0]


# ============================================================
# DIAGNOSTIC COUNTERS
# ============================================================

def empty_counter():

    return {
        "total_previous_days": 0,

        "has_3m_1524": 0,
        "has_3m_1527": 0,
        "has_3m_opening": 0,

        "opening_3m_same": 0,

        "3m_eod_opposite": 0,

        "3m_volume_condition": 0,

        "3m_opening_matches_1527": 0,

        "has_1m_1528": 0,
        "has_1m_1529": 0,
        "has_1m_opening": 0,

        "opening_1m_same": 0,

        "1m_eod_opposite": 0,

        "1m_volume_condition": 0,

        "1m_1528_matches_3m_1527": 0,

        "final_signal": 0,

        "long_signals": 0,
        "short_signals": 0
    }


# ============================================================
# DIAGNOSTIC ONE STOCK
# ============================================================

def diagnose_stock(
    df,
    candles_3m
):

    counter = empty_counter()

    signal_days = []

    dates = sorted(
        df["date"].unique()
    )

    # --------------------------------------------------------
    # Need a previous day AND a next trading day
    # --------------------------------------------------------

    for i in range(
        1,
        len(dates)
    ):

        previous_date = dates[i - 1]
        trade_date = dates[i]

        counter[
            "total_previous_days"
        ] += 1

        # ====================================================
        # 3-MIN CANDLES
        # ====================================================

        c3_0915 = get_3m(
            candles_3m,
            previous_date,
            "09:15"
        )

        c3_0918 = get_3m(
            candles_3m,
            previous_date,
            "09:18"
        )

        c3_1524 = get_3m(
            candles_3m,
            previous_date,
            "15:24"
        )

        c3_1527 = get_3m(
            candles_3m,
            previous_date,
            "15:27"
        )

        # ----------------------------------------------------
        # Existence
        # ----------------------------------------------------

        if c3_1524 is not None:
            counter["has_3m_1524"] += 1

        if c3_1527 is not None:
            counter["has_3m_1527"] += 1

        if (
            c3_0915 is not None
            and
            c3_0918 is not None
        ):
            counter["has_3m_opening"] += 1

        if any(
            x is None
            for x in [
                c3_0915,
                c3_0918,
                c3_1524,
                c3_1527
            ]
        ):
            continue

        # ----------------------------------------------------
        # Trends
        # ----------------------------------------------------

        t3_0915 = get_trend(
            c3_0915["open"],
            c3_0915["close"]
        )

        t3_0918 = get_trend(
            c3_0918["open"],
            c3_0918["close"]
        )

        t3_1524 = get_trend(
            c3_1524["open"],
            c3_1524["close"]
        )

        t3_1527 = get_trend(
            c3_1527["open"],
            c3_1527["close"]
        )

        # ----------------------------------------------------
        # No dojis
        # ----------------------------------------------------

        if 0 in [
            t3_0915,
            t3_0918,
            t3_1524,
            t3_1527
        ]:
            continue

        # ====================================================
        # CONDITION 1
        #
        # 3M 09:15 == 09:18
        # ====================================================

        if t3_0915 == t3_0918:

            counter[
                "opening_3m_same"
            ] += 1

        else:

            continue

        # ====================================================
        # CONDITION 2
        #
        # 3M 15:24 != 15:27
        # ====================================================

        if t3_1524 != t3_1527:

            counter[
                "3m_eod_opposite"
            ] += 1

        else:

            continue

        # ====================================================
        # CONDITION 3
        #
        # 15:27 VOLUME > 15:24
        # ====================================================

        if (
            c3_1527["volume"]
            >
            c3_1524["volume"]
        ):

            counter[
                "3m_volume_condition"
            ] += 1

        else:

            continue

        # ====================================================
        # CONDITION 4
        #
        # 09:15 & 09:18 = 15:27
        # ====================================================

        if (
            t3_0915 == t3_1527
            and
            t3_0918 == t3_1527
        ):

            counter[
                "3m_opening_matches_1527"
            ] += 1

        else:

            continue

        # ====================================================
        # 1-MINUTE CANDLES
        # ====================================================

        c1_0915 = get_1m(
            df,
            previous_date,
            "09:15"
        )

        c1_0916 = get_1m(
            df,
            previous_date,
            "09:16"
        )

        c1_1528 = get_1m(
            df,
            previous_date,
            "15:28"
        )

        c1_1529 = get_1m(
            df,
            previous_date,
            "15:29"
        )

        if c1_1528 is not None:
            counter["has_1m_1528"] += 1

        if c1_1529 is not None:
            counter["has_1m_1529"] += 1

        if (
            c1_0915 is not None
            and
            c1_0916 is not None
        ):
            counter["has_1m_opening"] += 1

        if any(
            x is None
            for x in [
                c1_0915,
                c1_0916,
                c1_1528,
                c1_1529
            ]
        ):
            continue

        # ----------------------------------------------------
        # 1-minute trends
        # ----------------------------------------------------

        t1_0915 = get_trend(
            c1_0915["open"],
            c1_0915["close"]
        )

        t1_0916 = get_trend(
            c1_0916["open"],
            c1_0916["close"]
        )

        t1_1528 = get_trend(
            c1_1528["open"],
            c1_1528["close"]
        )

        t1_1529 = get_trend(
            c1_1529["open"],
            c1_1529["close"]
        )

        if 0 in [
            t1_0915,
            t1_0916,
            t1_1528,
            t1_1529
        ]:
            continue

        # ====================================================
        # CONDITION 5
        #
        # 1M 09:15 == 09:16
        # ====================================================

        if t1_0915 == t1_0916:

            counter[
                "opening_1m_same"
            ] += 1

        else:

            continue

        # ====================================================
        # CONDITION 6
        #
        # 1M 15:28 != 15:29
        # ====================================================

        if t1_1528 != t1_1529:

            counter[
                "1m_eod_opposite"
            ] += 1

        else:

            continue

        # ====================================================
        # CONDITION 7
        #
        # 15:28 VOLUME > 15:29
        # ====================================================

        if (
            c1_1528["volume"]
            >
            c1_1529["volume"]
        ):

            counter[
                "1m_volume_condition"
            ] += 1

        else:

            continue

        # ====================================================
        # CONDITION 8
        #
        # 1M 15:28 == 3M 15:27
        # ====================================================

        if t1_1528 == t3_1527:

            counter[
                "1m_1528_matches_3m_1527"
            ] += 1

        else:

            continue

        # ====================================================
        # CONDITION 9
        #
        # 1M 09:15 & 09:16 = 1M 15:28
        # ====================================================

        if (
            t1_0915 == t1_1528
            and
            t1_0916 == t1_1528
        ):

            counter[
                "final_signal"
            ] += 1

            if t3_1527 == 1:

                counter[
                    "long_signals"
                ] += 1

            elif t3_1527 == -1:

                counter[
                    "short_signals"
                ] += 1

            signal_days.append({
                "signal_date":
                    previous_date,

                "trade_date":
                    trade_date,

                "direction":
                    (
                        "LONG"
                        if t3_1527 == 1
                        else
                        "SHORT"
                    ),

                "signal_trend":
                    t3_1527
            })

    return counter, signal_days


# ============================================================
# BACKTEST SIGNAL DAYS
# ============================================================

def execute_trades(
    df,
    signal_days
):

    results = []

    for signal in signal_days:

        trade_date = (
            signal["trade_date"]
        )

        direction = (
            signal["signal_trend"]
        )

        entry = get_1m(
            df,
            trade_date,
            "09:15"
        )

        exit_ = get_1m(
            df,
            trade_date,
            "15:27"
        )

        if entry is None:
            continue

        if exit_ is None:
            continue

        entry_price = float(
            entry["open"]
        )

        exit_price = float(
            exit_["open"]
        )

        if entry_price <= 0:
            continue

        if direction == 1:

            gross = (
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

            gross = (
                (
                    entry_price -
                    exit_price
                )
                /
                entry_price
                *
                100
            )

        net = (
            gross -
            ROUND_TRIP_COST
        )

        results.append({

            "signal_date":
                signal["signal_date"],

            "trade_date":
                trade_date,

            "direction":
                (
                    "LONG"
                    if direction == 1
                    else "SHORT"
                ),

            "entry":
                entry_price,

            "exit":
                exit_price,

            "gross_return":
                gross,

            "net_return":
                net
        })

    return results


# ============================================================
# STATISTICS
# ============================================================

def statistics(
    trades
):

    if trades.empty:

        return None

    r = (
        trades["net_return"]
        .astype(float)
    )

    wins = r > 0
    losses = r < 0

    gross_profit = (
        r[wins].sum()
    )

    gross_loss = abs(
        r[losses].sum()
    )

    if gross_loss > 0:

        pf = (
            gross_profit /
            gross_loss
        )

    else:

        pf = np.inf

    return {

        "trades":
            len(r),

        "wins":
            int(wins.sum()),

        "losses":
            int(losses.sum()),

        "win_rate":
            wins.mean() * 100,

        "average":
            r.mean(),

        "total":
            r.sum(),

        "profit_factor":
            pf,

        "best":
            r.max(),

        "worst":
            r.min()
    }


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 100)
    print(
        "DIAGNOSTIC BACKTEST"
    )
    print(
        "3-MIN + 1-MIN PREVIOUS-DAY STRATEGY"
    )
    print("=" * 100)

    print()
    print(
        "ENTRY : NEXT DAY 09:15 OPEN"
    )

    print(
        "EXIT  : NEXT DAY 15:27 OPEN"
    )

    print(
        "COST  :",
        ROUND_TRIP_COST,
        "%"
    )

    # ========================================================
    # FILES
    # ========================================================

    files = sorted(
        set(
            glob.glob(
                os.path.join(
                    DATA_FOLDER,
                    "*.csv"
                )
            )
            +
            glob.glob(
                os.path.join(
                    DATA_FOLDER,
                    "*.CSV"
                )
            )
        )
    )

    if not files:

        raise FileNotFoundError(
            f"No CSV files found in "
            f"{DATA_FOLDER}"
        )

    print()
    print(
        "Files found:",
        len(files)
    )

    # ========================================================
    # GLOBAL COUNTERS
    # ========================================================

    global_counter = empty_counter()

    all_signal_days = []

    all_trades = []

    # ========================================================
    # PROCESS STOCKS
    # ========================================================

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

            df = load_data(
                file_path
            )

            candles_3m = create_3min(
                df
            )

            counter, signals = (
                diagnose_stock(
                    df,
                    candles_3m
                )
            )

            # ------------------------------------------------
            # Add counters
            # ------------------------------------------------

            for key in global_counter:

                global_counter[key] += (
                    counter[key]
                )

            # ------------------------------------------------
            # Add stock name
            # ------------------------------------------------

            for signal in signals:

                signal["symbol"] = (
                    os.path.basename(
                        file_path
                    )
                )

            all_signal_days.extend(
                signals
            )

            # ------------------------------------------------
            # Execute
            # ------------------------------------------------

            trade_results = execute_trades(
                df,
                signals
            )

            for trade in trade_results:

                trade["symbol"] = (
                    os.path.basename(
                        file_path
                    )
                )

            all_trades.extend(
                trade_results
            )

        except Exception as e:

            print()
            print(
                "ERROR:",
                os.path.basename(
                    file_path
                )
            )

            print(
                str(e)
            )

    print()

    # ========================================================
    # DIAGNOSTIC REPORT
    # ========================================================

    print()
    print("=" * 100)
    print(
        "CONDITION-BY-CONDITION DIAGNOSTIC"
    )
    print("=" * 100)

    total = (
        global_counter[
            "total_previous_days"
        ]
    )

    print()
    print(
        f"All previous trading days       : "
        f"{total:,}"
    )

    print()
    print(
        "--- 3-MINUTE DATA AVAILABILITY ---"
    )

    print(
        f"3M 15:24 available              : "
        f"{global_counter['has_3m_1524']:,}"
    )

    print(
        f"3M 15:27 available              : "
        f"{global_counter['has_3m_1527']:,}"
    )

    print(
        f"3M 09:15 + 09:18 available      : "
        f"{global_counter['has_3m_opening']:,}"
    )

    print()
    print(
        "--- 3-MINUTE CONDITIONS ---"
    )

    print(
        f"09:15 = 09:18                  : "
        f"{global_counter['opening_3m_same']:,}"
    )

    print(
        f"15:24 != 15:27                 : "
        f"{global_counter['3m_eod_opposite']:,}"
    )

    print(
        f"Volume 15:27 > 15:24           : "
        f"{global_counter['3m_volume_condition']:,}"
    )

    print(
        f"09:15 = 09:18 = 15:27          : "
        f"{global_counter['3m_opening_matches_1527']:,}"
    )

    print()
    print(
        "--- 1-MINUTE DATA AVAILABILITY ---"
    )

    print(
        f"1M 15:28 available              : "
        f"{global_counter['has_1m_1528']:,}"
    )

    print(
        f"1M 15:29 available              : "
        f"{global_counter['has_1m_1529']:,}"
    )

    print(
        f"1M 09:15 + 09:16 available      : "
        f"{global_counter['has_1m_opening']:,}"
    )

    print()
    print(
        "--- 1-MINUTE CONDITIONS ---"
    )

    print(
        f"09:15 = 09:16                  : "
        f"{global_counter['opening_1m_same']:,}"
    )

    print(
        f"15:28 != 15:29                 : "
        f"{global_counter['1m_eod_opposite']:,}"
    )

    print(
        f"Volume 15:28 > 15:29           : "
        f"{global_counter['1m_volume_condition']:,}"
    )

    print(
        f"1M 15:28 = 3M 15:27            : "
        f"{global_counter['1m_1528_matches_3m_1527']:,}"
    )

    print()
    print(
        "--- FINAL ---"
    )

    print(
        f"FINAL SIGNALS                   : "
        f"{global_counter['final_signal']:,}"
    )

    print(
        f"LONG SIGNALS                    : "
        f"{global_counter['long_signals']:,}"
    )

    print(
        f"SHORT SIGNALS                   : "
        f"{global_counter['short_signals']:,}"
    )

    # ========================================================
    # FINAL TRADES
    # ========================================================

    if not all_trades:

        print()
        print("=" * 100)
        print(
            "NO EXECUTABLE TRADES"
        )
        print("=" * 100)

        # Still save signal diagnostics
        if all_signal_days:

            pd.DataFrame(
                all_signal_days
            ).to_csv(
                "DIAGNOSTIC_SIGNALS.csv",
                index=False
            )

        return

    trades = pd.DataFrame(
        all_trades
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
    # PERFORMANCE
    # ========================================================

    s = statistics(
        trades
    )

    print()
    print("=" * 100)
    print(
        "FINAL BACKTEST PERFORMANCE"
    )
    print("=" * 100)

    print(
        f"Trades          : "
        f"{s['trades']:,}"
    )

    print(
        f"Wins            : "
        f"{s['wins']:,}"
    )

    print(
        f"Losses          : "
        f"{s['losses']:,}"
    )

    print(
        f"Win rate        : "
        f"{s['win_rate']:.2f}%"
    )

    print(
        f"Average return  : "
        f"{s['average']:.4f}%"
    )

    print(
        f"Total return    : "
        f"{s['total']:.4f}%"
    )

    print(
        f"Profit factor   : "
        f"{s['profit_factor']:.3f}"
    )

    print(
        f"Best trade      : "
        f"{s['best']:.4f}%"
    )

    print(
        f"Worst trade     : "
        f"{s['worst']:.4f}%"
    )

    # ========================================================
    # LONG / SHORT
    # ========================================================

    for side in [
        "LONG",
        "SHORT"
    ]:

        subset = trades[
            trades["direction"] == side
        ]

        if subset.empty:
            continue

        ss = statistics(
            subset
        )

        print()
        print(
            f"{side}:"
        )

        print(
            f"  Trades        : "
            f"{ss['trades']:,}"
        )

        print(
            f"  Win rate      : "
            f"{ss['win_rate']:.2f}%"
        )

        print(
            f"  Average       : "
            f"{ss['average']:.4f}%"
        )

        print(
            f"  Profit factor : "
            f"{ss['profit_factor']:.3f}"
        )

    # ========================================================
    # YEARLY
    # ========================================================

    trades["year"] = (
        pd.to_datetime(
            trades["trade_date"]
        ).dt.year
    )

    yearly_rows = []

    for year, group in (
        trades.groupby("year")
    ):

        ss = statistics(
            group
        )

        yearly_rows.append({

            "year":
                year,

            "trades":
                ss["trades"],

            "win_rate":
                ss["win_rate"],

            "average_return":
                ss["average"],

            "profit_factor":
                ss["profit_factor"],

            "total_return":
                ss["total"]
        })

    yearly = pd.DataFrame(
        yearly_rows
    )

    print()
    print("=" * 100)
    print(
        "YEARLY PERFORMANCE"
    )
    print("=" * 100)

    print(
        yearly.to_string(
            index=False
        )
    )

    # ========================================================
    # SAVE EVERYTHING
    # ========================================================

    trades.to_csv(
        "DIAGNOSTIC_FINAL_TRADES.csv",
        index=False
    )

    if all_signal_days:

        pd.DataFrame(
            all_signal_days
        ).to_csv(
            "DIAGNOSTIC_SIGNALS.csv",
            index=False
        )

    yearly.to_csv(
        "DIAGNOSTIC_YEARLY.csv",
        index=False
    )

    # ========================================================
    # COMPLETE
    # ========================================================

    print()
    print("=" * 100)
    print(
        "DIAGNOSTIC BACKTEST COMPLETE"
    )
    print("=" * 100)

    print()
    print(
        "Saved:"
    )

    print(
        "DIAGNOSTIC_FINAL_TRADES.csv"
    )

    print(
        "DIAGNOSTIC_SIGNALS.csv"
    )

    print(
        "DIAGNOSTIC_YEARLY.csv"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
