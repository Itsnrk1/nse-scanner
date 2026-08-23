# ============================================================
# SIMPLE EOD 15-MINUTE REVERSAL BACKTEST
# ============================================================
#
# STRATEGY
#
# PREVIOUS TRADING DAY:
#
#   15:00 - 15:14  = BULLISH
#   15:15 - 15:29  = BEARISH
#
# SIGNAL:
#   SHORT
#
# NEXT TRADING DAY:
#
#   09:15 OPEN = ENTRY
#   15:27 OPEN = EXIT
#
# NO:
#   - gap-open filter
#   - opening-candle filter
#   - next-day information in signal
#   - optimization
#   - machine learning
#
# ============================================================

import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import warnings

warnings.filterwarnings("ignore")


# ============================================================
# SETTINGS
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/nse-fno-1min-data/"
    "releases/download/v1.0.0/stocks_1m_csvs.zip"
)

ENTRY_TIME = "09:15"
EXIT_TIME = "15:27"

# Estimated round-trip trading cost.
# Change to 0 if you want the raw price result.
ROUND_TRIP_COST = 0.10


# ============================================================
# DOWNLOAD DATA
# ============================================================

def download_data():

    print("=" * 90)
    print("DOWNLOADING NSE 1-MINUTE DATA")
    print("=" * 90)

    response = requests.get(
        DATA_URL,
        timeout=900
    )

    response.raise_for_status()

    print(
        f"Downloaded: "
        f"{len(response.content) / 1024 / 1024:.1f} MB"
    )

    return response.content


# ============================================================
# LOAD STOCK
# ============================================================

def load_stock(
    zip_file,
    filename
):

    try:

        raw = zip_file.read(
            filename
        )

        df = pd.read_csv(
            io.BytesIO(raw),
            compression="gzip"
        )

        if df.empty:
            return None

        if "time" not in df.columns:
            return None

        # ----------------------------------------------------
        # Convert timestamp to IST
        # ----------------------------------------------------

        df["datetime"] = (
            pd.to_datetime(
                df["time"],
                unit="s",
                utc=True
            )
            .dt
            .tz_convert(
                "Asia/Kolkata"
            )
        )

        df["date"] = (
            df["datetime"]
            .dt
            .strftime(
                "%Y-%m-%d"
            )
        )

        df["time_str"] = (
            df["datetime"]
            .dt
            .strftime(
                "%H:%M"
            )
        )

        # ----------------------------------------------------
        # OHLC
        # ----------------------------------------------------

        for column in [
            "open",
            "high",
            "low",
            "close"
        ]:

            if column not in df.columns:
                return None

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

        # ----------------------------------------------------
        # Volume
        # ----------------------------------------------------

        if "Volume" in df.columns:

            df["volume"] = pd.to_numeric(
                df["Volume"],
                errors="coerce"
            )

        elif "volume" in df.columns:

            df["volume"] = pd.to_numeric(
                df["volume"],
                errors="coerce"
            )

        else:

            return None

        # ----------------------------------------------------
        # Remove bad rows
        # ----------------------------------------------------

        df = df.dropna(
            subset=[
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]
        )

        # ----------------------------------------------------
        # NSE regular session only
        # ----------------------------------------------------

        df = df[
            (df["time_str"] >= "09:15")
            &
            (df["time_str"] <= "15:29")
        ].copy()

        return df[
            [
                "date",
                "time_str",
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]
        ]

    except Exception:

        return None


# ============================================================
# CREATE DAY LOOKUP
# ============================================================

def make_day_lookup(group):

    result = {}

    for _, row in group.iterrows():

        result[row["time_str"]] = {

            "open":
                float(row["open"]),

            "high":
                float(row["high"]),

            "low":
                float(row["low"]),

            "close":
                float(row["close"]),

            "volume":
                float(row["volume"])
        }

    return result


# ============================================================
# BUILD 15-MINUTE CANDLE
# ============================================================

def build_15m_candle(
    day,
    start_minute,
    end_minute
):

    rows = []

    for minute in range(
        start_minute,
        end_minute + 1
    ):

        hour = minute // 60
        minute_part = minute % 60

        key = (
            f"{hour:02d}:"
            f"{minute_part:02d}"
        )

        if key not in day:
            return None

        rows.append(
            day[key]
        )

    if len(rows) != (
        end_minute -
        start_minute +
        1
    ):

        return None

    return {

        "open":
            rows[0]["open"],

        "high":
            max(
                row["high"]
                for row in rows
            ),

        "low":
            min(
                row["low"]
                for row in rows
            ),

        "close":
            rows[-1]["close"],

        "volume":
            sum(
                row["volume"]
                for row in rows
            )
    }


# ============================================================
# CHECK BASE SIGNAL
# ============================================================

def has_base_signal(day):

    # --------------------------------------------------------
    # FIRST 15-MINUTE EOD CANDLE
    #
    # 15:00 -> 15:14
    # --------------------------------------------------------

    candle_1 = build_15m_candle(
        day,
        15 * 60,
        15 * 60 + 14
    )

    # --------------------------------------------------------
    # SECOND 15-MINUTE EOD CANDLE
    #
    # 15:15 -> 15:29
    # --------------------------------------------------------

    candle_2 = build_15m_candle(
        day,
        15 * 60 + 15,
        15 * 60 + 29
    )

    if (
        candle_1 is None
        or
        candle_2 is None
    ):

        return False

    # --------------------------------------------------------
    # REVERSAL CONDITION
    #
    # FIRST = BULLISH
    # SECOND = BEARISH
    # --------------------------------------------------------

    first_bullish = (
        candle_1["close"]
        >
        candle_1["open"]
    )

    second_bearish = (
        candle_2["close"]
        <
        candle_2["open"]
    )

    return (
        first_bullish
        and
        second_bearish
    )


# ============================================================
# PROCESS ONE STOCK
# ============================================================

def process_stock(
    zip_file,
    filename
):

    df = load_stock(
        zip_file,
        filename
    )

    if df is None:
        return []

    grouped = {
        date: group
        for date, group
        in df.groupby("date")
    }

    dates = sorted(
        grouped.keys()
    )

    if len(dates) < 2:
        return []

    symbol = os.path.basename(
        filename
    )

    symbol = symbol.replace(
        ".csv.gz",
        ""
    )

    symbol = symbol.replace(
        "_1m",
        ""
    )

    trades = []

    # --------------------------------------------------------
    # Previous day -> next trading day
    # --------------------------------------------------------

    for i in range(
        1,
        len(dates)
    ):

        previous_date = dates[
            i - 1
        ]

        current_date = dates[
            i
        ]

        previous_day = make_day_lookup(
            grouped[
                previous_date
            ]
        )

        current_day = make_day_lookup(
            grouped[
                current_date
            ]
        )

        # ----------------------------------------------------
        # SIGNAL
        #
        # Previous day only.
        # ----------------------------------------------------

        if not has_base_signal(
            previous_day
        ):

            continue

        # ----------------------------------------------------
        # NEXT DAY ENTRY
        # ----------------------------------------------------

        if ENTRY_TIME not in current_day:
            continue

        # ----------------------------------------------------
        # NEXT DAY EXIT
        # ----------------------------------------------------

        if EXIT_TIME not in current_day:
            continue

        entry_price = current_day[
            ENTRY_TIME
        ]["open"]

        exit_price = current_day[
            EXIT_TIME
        ]["open"]

        if entry_price <= 0:
            continue

        # ----------------------------------------------------
        # SHORT RETURN
        #
        # Profit when price falls.
        # ----------------------------------------------------

        gross_return = (
            (
                entry_price -
                exit_price
            )
            /
            entry_price
        ) * 100

        net_return = (
            gross_return -
            ROUND_TRIP_COST
        )

        trades.append({

            "symbol":
                symbol,

            "signal_date":
                pd.Timestamp(
                    previous_date
                ),

            "trade_date":
                pd.Timestamp(
                    current_date
                ),

            "entry":
                entry_price,

            "exit":
                exit_price,

            "gross_return":
                gross_return,

            "net_return":
                net_return

        })

    return trades


# ============================================================
# BUILD COMPLETE BACKTEST
# ============================================================

def run_backtest():

    raw_data = download_data()

    zip_file = zipfile.ZipFile(
        io.BytesIO(raw_data)
    )

    files = [
        filename
        for filename
        in zip_file.namelist()
        if filename.endswith(
            ".csv.gz"
        )
    ]

    print()
    print(
        f"Stocks found: "
        f"{len(files):,}"
    )

    all_trades = []

    for number, filename in enumerate(
        files,
        1
    ):

        print(
            f"\rProcessing "
            f"{number:,}/"
            f"{len(files):,}",
            end=""
        )

        try:

            trades = process_stock(
                zip_file,
                filename
            )

            all_trades.extend(
                trades
            )

        except Exception:

            continue

    print()

    result = pd.DataFrame(
        all_trades
    )

    if result.empty:

        raise RuntimeError(
            "No trades were generated."
        )

    result = result.sort_values(
        "trade_date"
    ).reset_index(
        drop=True
    )

    return result


# ============================================================
# STATISTICS
# ============================================================

def calculate_statistics(
    trades
):

    if trades.empty:

        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0,
            "average": 0,
            "profit_factor": 0,
            "total": 0
        }

    returns = (
        trades[
            "net_return"
        ]
    )

    wins = (
        returns > 0
    ).sum()

    losses = (
        returns <= 0
    ).sum()

    win_rate = (
        wins /
        len(returns)
        *
        100
    )

    average = returns.mean()

    gross_profit = (
        returns[
            returns > 0
        ].sum()
    )

    gross_loss = abs(
        returns[
            returns < 0
        ].sum()
    )

    if gross_loss > 0:

        profit_factor = (
            gross_profit /
            gross_loss
        )

    else:

        profit_factor = np.inf

    total = returns.sum()

    return {

        "trades":
            len(returns),

        "wins":
            int(wins),

        "losses":
            int(losses),

        "win_rate":
            win_rate,

        "average":
            average,

        "profit_factor":
            profit_factor,

        "total":
            total
    }


# ============================================================
# CHRONOLOGICAL SPLIT
# ============================================================

def split_data(
    trades
):

    dates = sorted(
        trades[
            "trade_date"
        ]
        .dt
        .normalize()
        .unique()
    )

    n = len(dates)

    train_end_index = int(
        n * 0.60
    )

    validation_end_index = int(
        n * 0.80
    )

    train_end = dates[
        train_end_index
    ]

    validation_end = dates[
        validation_end_index
    ]

    train = trades[
        trades[
            "trade_date"
        ].dt.normalize()
        <
        train_end
    ].copy()

    validation = trades[
        (
            trades[
                "trade_date"
            ].dt.normalize()
            >=
            train_end
        )
        &
        (
            trades[
                "trade_date"
            ].dt.normalize()
            <
            validation_end
        )
    ].copy()

    test = trades[
        trades[
            "trade_date"
        ].dt.normalize()
        >=
        validation_end
    ].copy()

    return (
        train,
        validation,
        test,
        train_end,
        validation_end
    )


# ============================================================
# YEARLY PERFORMANCE
# ============================================================

def yearly_performance(
    trades
):

    if trades.empty:
        return pd.DataFrame()

    data = trades.copy()

    data["year"] = (
        data[
            "trade_date"
        ]
        .dt
        .year
    )

    rows = []

    for year, group in (
        data.groupby("year")
    ):

        stats = calculate_statistics(
            group
        )

        rows.append({

            "year":
                year,

            "trades":
                stats["trades"],

            "win_rate":
                stats["win_rate"],

            "average":
                stats["average"],

            "profit_factor":
                stats["profit_factor"],

            "total":
                stats["total"]
        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# PRINT STATISTICS
# ============================================================

def print_statistics(
    name,
    trades
):

    stats = calculate_statistics(
        trades
    )

    print()
    print(
        "=" * 90
    )

    print(name)

    print(
        "=" * 90
    )

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
        f"Average/trade   : "
        f"{stats['average']:.4f}%"
    )

    print(
        f"Profit factor   : "
        f"{stats['profit_factor']:.3f}"
    )

    print(
        f"Total return    : "
        f"{stats['total']:.4f}%"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print(
        "=" * 90
    )

    print(
        "15-MINUTE EOD REVERSAL STRATEGY"
    )

    print(
        "=" * 90
    )

    print()
    print(
        "PREVIOUS DAY:"
    )

    print(
        "15:00-15:14 = BULLISH"
    )

    print(
        "15:15-15:29 = BEARISH"
    )

    print()
    print(
        "TRADE:"
    )

    print(
        "SHORT"
    )

    print(
        "NEXT DAY 09:15 OPEN -> ENTRY"
    )

    print(
        "NEXT DAY 15:27 OPEN -> EXIT"
    )

    print()
    print(
        "No opening-candle filters."
    )

    print(
        "No gap-open filter."
    )

    print(
        "No optimization."
    )

    print(
        "No next-day signal information."
    )

    # --------------------------------------------------------
    # BACKTEST
    # --------------------------------------------------------

    trades = run_backtest()

    print()
    print(
        f"TOTAL TRADES: "
        f"{len(trades):,}"
    )

    # --------------------------------------------------------
    # SPLIT
    # --------------------------------------------------------

    (
        train,
        validation,
        test,
        train_end,
        validation_end
    ) = split_data(
        trades
    )

    print()
    print(
        f"TRAIN: "
        f"{len(train):,}"
    )

    print(
        f"VALIDATION: "
        f"{len(validation):,}"
    )

    print(
        f"FINAL TEST: "
        f"{len(test):,}"
    )

    print()
    print(
        f"TRAIN ENDS: "
        f"{train_end}"
    )

    print(
        f"VALIDATION ENDS: "
        f"{validation_end}"
    )

    # --------------------------------------------------------
    # RESULTS
    # --------------------------------------------------------

    print_statistics(
        "TRAIN",
        train
    )

    print_statistics(
        "VALIDATION",
        validation
    )

    print_statistics(
        "FINAL UNSEEN TEST",
        test
    )

    # --------------------------------------------------------
    # YEARLY TEST
    # --------------------------------------------------------

    print()
    print(
        "=" * 90
    )

    print(
        "FINAL TEST YEAR-BY-YEAR"
    )

    print(
        "=" * 90
    )

    yearly = yearly_performance(
        test
    )

    if yearly.empty:

        print(
            "No yearly data."
        )

    else:

        print(
            yearly.to_string(
                index=False
            )
        )

    # --------------------------------------------------------
    # SAVE TRADES
    # --------------------------------------------------------

    trades.to_csv(
        "EOD_15M_REVERSAL_TRADES.csv",
        index=False
    )

    yearly.to_csv(
        "EOD_15M_REVERSAL_YEARLY.csv",
        index=False
    )

    # --------------------------------------------------------
    # FINAL SUMMARY
    # --------------------------------------------------------

    final_stats = calculate_statistics(
        test
    )

    print()
    print(
        "=" * 90
    )

    print(
        "FINAL SUMMARY"
    )

    print(
        "=" * 90
    )

    print(
        f"Final test trades : "
        f"{final_stats['trades']:,}"
    )

    print(
        f"Final test win    : "
        f"{final_stats['win_rate']:.2f}%"
    )

    print(
        f"Average/trade     : "
        f"{final_stats['average']:.4f}%"
    )

    print(
        f"Profit factor     : "
        f"{final_stats['profit_factor']:.3f}"
    )

    print(
        f"Total return      : "
        f"{final_stats['total']:.4f}%"
    )

    print()
    print(
        "Files created:"
    )

    print(
        "EOD_15M_REVERSAL_TRADES.csv"
    )

    print(
        "EOD_15M_REVERSAL_YEARLY.csv"
    )

    print()
    print(
        "BACKTEST COMPLETE."
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
