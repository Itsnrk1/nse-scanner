import os
import io
import gzip
import zipfile
import requests

import numpy as np
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

DATA_URL = (
    "https://github.com/voletiramu/"
    "nse-fno-1min-data/releases/download/"
    "v1.0.0/stocks_1m_csvs.zip"
)

ZIP_FILE = "stocks_1m_csvs.zip"

OUTPUT_FILE = "backtest_results.csv"

ROUND_TRIP_COST = 0.10

ENTRY_TIME = "09:15"
EXIT_TIME = "15:27"


# ============================================================
# DOWNLOAD DATASET
# ============================================================

def download_dataset():

    if os.path.exists(ZIP_FILE):

        size_mb = (
            os.path.getsize(ZIP_FILE)
            / 1024
            / 1024
        )

        print(
            f"Dataset already exists: "
            f"{size_mb:.1f} MB"
        )

        return

    print("=" * 90)
    print("DOWNLOADING NSE 1-MINUTE DATA")
    print("=" * 90)

    print()
    print(
        "Source: voletiramu/nse-fno-1min-data"
    )

    print(
        "Dataset: 214 NSE F&O stocks"
    )

    print(
        "Period: 2024-04-01 to 2026-04-30"
    )

    print()

    response = requests.get(
        DATA_URL,
        stream=True,
        timeout=120
    )

    response.raise_for_status()

    total_size = int(
        response.headers.get(
            "content-length",
            0
        )
    )

    downloaded = 0

    last_percent = -1

    with open(
        ZIP_FILE,
        "wb"
    ) as f:

        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):

            if not chunk:
                continue

            f.write(chunk)

            downloaded += len(chunk)

            if total_size:

                percent = int(
                    downloaded
                    /
                    total_size
                    *
                    100
                )

                if percent != last_percent:

                    print(
                        f"Downloaded "
                        f"{percent}%",
                        flush=True
                    )

                    last_percent = percent

    print()
    print("Download complete.")
    print()


# ============================================================
# DIRECTION
# ============================================================

def candle_direction(
    opens,
    closes
):

    result = np.zeros(
        len(opens),
        dtype=np.int8
    )

    result[closes > opens] = 1
    result[closes < opens] = -1

    return result


# ============================================================
# PREPARE 1-MINUTE DATA
# ============================================================

def prepare_data(raw):

    # --------------------------------------------------------
    # The repository format is:
    #
    # time
    # open
    # high
    # low
    # close
    # CE Breakout Level
    # PE Breakout Level
    # Volume
    # --------------------------------------------------------

    raw.columns = [
        str(c).strip().lower()
        for c in raw.columns
    ]

    # --------------------------------------------------------
    # Rename possible variants
    # --------------------------------------------------------

    rename = {}

    for col in raw.columns:

        if col == "volume":
            rename[col] = "volume"

        elif col == "vol":
            rename[col] = "volume"

    raw = raw.rename(
        columns=rename
    )

    required = [
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    missing = [
        c
        for c in required
        if c not in raw.columns
    ]

    if missing:

        raise ValueError(
            f"Missing columns: {missing}"
        )

    # --------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------

    raw["datetime"] = pd.to_datetime(
        raw["time"],
        unit="s",
        utc=True
    ).dt.tz_convert(
        "Asia/Kolkata"
    ).dt.tz_localize(
        None
    )

    # --------------------------------------------------------
    # Numeric columns
    # --------------------------------------------------------

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]:

        raw[col] = pd.to_numeric(
            raw[col],
            errors="coerce"
        )

    raw = raw.dropna(
        subset=[
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    )

    raw = raw.sort_values(
        "datetime"
    )

    # --------------------------------------------------------
    # NSE regular session
    #
    # We need 09:15 through 15:29.
    # --------------------------------------------------------

    raw["time_only"] = (
        raw["datetime"]
        .dt.strftime("%H:%M")
    )

    raw = raw[
        (
            raw["time_only"] >= "09:15"
        )
        &
        (
            raw["time_only"] <= "15:29"
        )
    ].copy()

    raw["date"] = (
        raw["datetime"]
        .dt.date
    )

    return raw


# ============================================================
# BUILD 3-MINUTE DATA
# ============================================================

def build_3min_data(df):

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # The 3-minute candles are constructed directly from
    # the 1-minute candles.
    #
    # 09:15 = 09:15, 09:16, 09:17
    # 09:18 = 09:18, 09:19, 09:20
    #
    # ...
    #
    # 15:24 = 15:24, 15:25, 15:26
    # 15:27 = 15:27, 15:28, 15:29
    # --------------------------------------------------------

    temp = df.copy()

    # Anchor at 09:15.
    minutes_from_open = (
        temp["datetime"].dt.hour * 60
        +
        temp["datetime"].dt.minute
        -
        (9 * 60 + 15)
    )

    temp["bucket"] = (
        minutes_from_open // 3
    )

    temp["candle_start"] = (
        pd.to_datetime(
            temp["date"].astype(str)
        )
        +
        pd.to_timedelta(
            9 * 60 + 15
            +
            temp["bucket"] * 3,
            unit="m"
        )
    )

    # --------------------------------------------------------
    # Aggregate OHLCV.
    # --------------------------------------------------------

    candles = (
        temp
        .groupby(
            [
                "date",
                "candle_start"
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

            minute_count=(
                "close",
                "count"
            )

        )
        .reset_index()
    )

    # --------------------------------------------------------
    # Only accept COMPLETE 3-minute candles.
    #
    # This prevents a missing 1-minute bar from creating a
    # falsely constructed 3-minute candle.
    # --------------------------------------------------------

    candles = candles[
        candles["minute_count"] == 3
    ].copy()

    candles["time"] = (
        candles["candle_start"]
        .dt.strftime("%H:%M")
    )

    candles["direction"] = candle_direction(
        candles["open"].to_numpy(),
        candles["close"].to_numpy()
    )

    return candles


# ============================================================
# GET CANDLE
# ============================================================

def get_candle(
    df,
    date,
    time_string
):

    result = df[
        (
            df["date"] == date
        )
        &
        (
            df["time"] == time_string
        )
    ]

    if result.empty:
        return None

    return result.iloc[0]


# ============================================================
# BUILD DAILY FEATURES
# ============================================================

def build_daily_features(
    one_min,
    three_min
):

    dates = sorted(
        one_min["date"].unique()
    )

    rows = []

    # ========================================================
    # PROCESS EACH SIGNAL DAY
    # ========================================================

    for date in dates:

        # ----------------------------------------------------
        # 3-MIN CANDLES
        # ----------------------------------------------------

        c_0915_3m = get_candle(
            three_min,
            date,
            "09:15"
        )

        c_0918_3m = get_candle(
            three_min,
            date,
            "09:18"
        )

        c_1524_3m = get_candle(
            three_min,
            date,
            "15:24"
        )

        c_1527_3m = get_candle(
            three_min,
            date,
            "15:27"
        )

        # ----------------------------------------------------
        # 1-MIN CANDLES
        # ----------------------------------------------------

        c_0915_1m = get_candle(
            one_min,
            date,
            "09:15"
        )

        c_0916_1m = get_candle(
            one_min,
            date,
            "09:16"
        )

        c_1528_1m = get_candle(
            one_min,
            date,
            "15:28"
        )

        c_1529_1m = get_candle(
            one_min,
            date,
            "15:29"
        )

        required = [
            c_0915_3m,
            c_0918_3m,
            c_1524_3m,
            c_1527_3m,
            c_0915_1m,
            c_0916_1m,
            c_1528_1m,
            c_1529_1m
        ]

        if any(
            x is None
            for x in required
        ):

            continue

        # ----------------------------------------------------
        # DIRECTIONS
        # ----------------------------------------------------

        d_0915_3m = int(
            c_0915_3m["direction"]
        )

        d_0918_3m = int(
            c_0918_3m["direction"]
        )

        d_1524_3m = int(
            c_1524_3m["direction"]
        )

        d_1527_3m = int(
            c_1527_3m["direction"]
        )

        d_0915_1m = int(
            candle_direction(
                np.array([
                    c_0915_1m["open"]
                ]),
                np.array([
                    c_0915_1m["close"]
                ])
            )[0]
        )

        d_0916_1m = int(
            candle_direction(
                np.array([
                    c_0916_1m["open"]
                ]),
                np.array([
                    c_0916_1m["close"]
                ])
            )[0]
        )

        d_1528_1m = int(
            candle_direction(
                np.array([
                    c_1528_1m["open"]
                ]),
                np.array([
                    c_1528_1m["close"]
                ])
            )[0]
        )

        d_1529_1m = int(
            candle_direction(
                np.array([
                    c_1529_1m["open"]
                ]),
                np.array([
                    c_1529_1m["close"]
                ])
            )[0]
        )

        # ----------------------------------------------------
        # Ignore dojis.
        # ----------------------------------------------------

        if 0 in [
            d_0915_3m,
            d_0918_3m,
            d_1524_3m,
            d_1527_3m,
            d_0915_1m,
            d_0916_1m,
            d_1528_1m,
            d_1529_1m
        ]:

            continue

        # ====================================================
        # CONDITION 1
        #
        # 3M 15:24 and 15:27 opposite.
        #
        # 3M 15:27 volume > 15:24 volume.
        # ====================================================

        condition_1 = (

            d_1524_3m
            !=
            d_1527_3m

            and

            c_1527_3m["volume"]
            >
            c_1524_3m["volume"]

        )

        # ====================================================
        # CONDITION 2
        #
        # 3M 09:15 and 09:18 both match 15:27.
        # ====================================================

        condition_2 = (

            d_0915_3m
            ==
            d_1527_3m

            and

            d_0918_3m
            ==
            d_1527_3m

        )

        # ====================================================
        # CONDITION 3
        #
        # 1M 15:28 and 15:29 opposite.
        #
        # 1M 15:28 volume > 15:29 volume.
        # ====================================================

        condition_3 = (

            d_1528_1m
            !=
            d_1529_1m

            and

            c_1528_1m["volume"]
            >
            c_1529_1m["volume"]

        )

        # ====================================================
        # CONDITION 4
        #
        # 1M 15:28 matches 3M 15:27.
        # ====================================================

        condition_4 = (

            d_1528_1m
            ==
            d_1527_3m

        )

        # ====================================================
        # CONDITION 5
        #
        # 1M 09:15 and 09:16 both match 15:28.
        # ====================================================

        condition_5 = (

            d_0915_1m
            ==
            d_1528_1m

            and

            d_0916_1m
            ==
            d_1528_1m

        )

        # ====================================================
        # FINAL SIGNAL
        # ====================================================

        if not (
            condition_1
            and
            condition_2
            and
            condition_3
            and
            condition_4
            and
            condition_5
        ):

            continue

        # ----------------------------------------------------
        # Direction = 3M 15:27.
        # ----------------------------------------------------

        direction = d_1527_3m

        rows.append({

            "signal_date":
                date,

            "direction":
                direction

        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# BACKTEST ONE STOCK
# ============================================================

def backtest_stock(
    symbol,
    raw
):

    one_min = prepare_data(
        raw
    )

    if one_min.empty:

        return []

    # --------------------------------------------------------
    # Build exact 3-minute candles.
    # --------------------------------------------------------

    three_min = build_3min_data(
        one_min
    )

    if three_min.empty:

        return []

    # --------------------------------------------------------
    # Generate previous-day signals.
    # --------------------------------------------------------

    signals = build_daily_features(
        one_min,
        three_min
    )

    if signals.empty:

        return []

    signals = signals.sort_values(
        "signal_date"
    )

    trading_dates = sorted(
        one_min["date"].unique()
    )

    date_to_index = {
        d: i
        for i, d in enumerate(
            trading_dates
        )
    }

    trades = []

    # ========================================================
    # EACH SIGNAL DAY
    # ========================================================

    for _, signal in signals.iterrows():

        signal_date = (
            signal["signal_date"]
        )

        signal_index = (
            date_to_index.get(
                signal_date
            )
        )

        if signal_index is None:

            continue

        # ----------------------------------------------------
        # Next actual trading day.
        # ----------------------------------------------------

        next_index = (
            signal_index + 1
        )

        if (
            next_index
            >=
            len(trading_dates)
        ):

            continue

        trade_date = (
            trading_dates[
                next_index
            ]
        )

        # ----------------------------------------------------
        # Next day 09:15.
        # ----------------------------------------------------

        entry_row = get_candle(
            one_min,
            trade_date,
            ENTRY_TIME
        )

        # ----------------------------------------------------
        # Next day 15:27.
        # ----------------------------------------------------

        exit_row = get_candle(
            one_min,
            trade_date,
            EXIT_TIME
        )

        if (
            entry_row is None
            or
            exit_row is None
        ):

            continue

        entry_price = float(
            entry_row["open"]
        )

        exit_price = float(
            exit_row["open"]
        )

        if (
            entry_price <= 0
            or
            exit_price <= 0
        ):

            continue

        direction = int(
            signal["direction"]
        )

        # ====================================================
        # RETURN
        # ====================================================

        if direction == 1:

            direction_name = "LONG"

            gross_return = (
                (
                    exit_price
                    -
                    entry_price
                )
                /
                entry_price
            ) * 100.0

        else:

            direction_name = "SHORT"

            gross_return = (
                (
                    entry_price
                    -
                    exit_price
                )
                /
                entry_price
            ) * 100.0

        net_return = (
            gross_return
            -
            ROUND_TRIP_COST
        )

        trades.append({

            "symbol":
                symbol,

            "signal_date":
                signal_date,

            "trade_date":
                trade_date,

            "direction":
                direction_name,

            "entry_time":
                "09:15",

            "exit_time":
                "15:27",

            "entry_price":
                entry_price,

            "exit_price":
                exit_price,

            "gross_return_pct":
                gross_return,

            "net_return_pct":
                net_return

        })

    return trades


# ============================================================
# READ ONE STOCK FROM ZIP
# ============================================================

def read_stock_from_zip(
    zf,
    filename
):

    with zf.open(
        filename,
        "r"
    ) as compressed_file:

        with gzip.GzipFile(
            fileobj=compressed_file
        ) as gz:

            data = gz.read()

    return pd.read_csv(
        io.BytesIO(data)
    )


# ============================================================
# PERFORMANCE STATISTICS
# ============================================================

def calculate_statistics(
    trades
):

    if trades.empty:

        return {

            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "average_return": 0.0,
            "total_return": 0.0,
            "profit_factor": 0.0,
            "best_trade": 0.0,
            "worst_trade": 0.0

        }

    returns = (
        trades[
            "net_return_pct"
        ]
        .astype(float)
    )

    wins = (
        returns > 0
    )

    losses = (
        returns < 0
    )

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
            int(wins.sum()),

        "losses":
            int(losses.sum()),

        "win_rate":
            (
                wins.sum()
                /
                len(returns)
                *
                100
            ),

        "average_return":
            returns.mean(),

        "total_return":
            returns.sum(),

        "profit_factor":
            profit_factor,

        "best_trade":
            returns.max(),

        "worst_trade":
            returns.min()

    }


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 90)
    print("HIGH-WIN-RATE STRATEGY BACKTEST")
    print("=" * 90)

    print()
    print("FIXED ENTRY : NEXT DAY 09:15 OPEN")
    print("FIXED EXIT  : NEXT DAY 15:27 OPEN")

    print()
    print("PREVIOUS DAY CONDITIONS:")
    print()

    print(
        "3M 15:24 != 3M 15:27"
    )

    print(
        "3M Volume(15:27) > Volume(15:24)"
    )

    print(
        "3M 09:15 = 3M 09:18 = 3M 15:27"
    )

    print()

    print(
        "1M 15:28 != 1M 15:29"
    )

    print(
        "1M Volume(15:28) > Volume(15:29)"
    )

    print(
        "1M 15:28 = 3M 15:27"
    )

    print(
        "1M 09:15 = 1M 09:16 = 1M 15:28"
    )

    print()

    print(
        "DIRECTION = 3M 15:27"
    )

    print()

    print(
        "NEXT DAY:"
    )

    print(
        "ENTRY = 09:15 OPEN"
    )

    print(
        "EXIT  = 15:27 OPEN"
    )

    print()

    print(
        "ROUND-TRIP COST = "
        f"{ROUND_TRIP_COST}%"
    )

    print()
    print("=" * 90)

    # ========================================================
    # DOWNLOAD
    # ========================================================

    download_dataset()

    # ========================================================
    # OPEN ZIP
    # ========================================================

    print(
        "Opening dataset..."
    )

    all_trades = []

    with zipfile.ZipFile(
        ZIP_FILE,
        "r"
    ) as zf:

        # ----------------------------------------------------
        # Find gzipped stock files.
        # ----------------------------------------------------

        stock_files = [
            name
            for name in zf.namelist()
            if name.lower().endswith(
                ".csv.gz"
            )
        ]

        stock_files = sorted(
            stock_files
        )

        print()
        print(
            f"Stocks found: "
            f"{len(stock_files)}"
        )

        print()

        # ====================================================
        # PROCESS EACH STOCK
        # ====================================================

        for i, filename in enumerate(
            stock_files,
            1
        ):

            print(
                f"\rProcessing "
                f"{i}/{len(stock_files)}",
                end="",
                flush=True
            )

            try:

                raw = read_stock_from_zip(
                    zf,
                    filename
                )

                # ------------------------------------------------
                # Remove .gz and _1m.csv
                # ------------------------------------------------

                symbol = os.path.basename(
                    filename
                )

                if symbol.endswith(
                    ".csv.gz"
                ):

                    symbol = symbol[
                        :-7
                    ]

                if symbol.endswith(
                    "_1m"
                ):

                    symbol = symbol[
                        :-3
                    ]

                trades = backtest_stock(
                    symbol,
                    raw
                )

                if trades:

                    all_trades.extend(
                        trades
                    )

            except Exception as e:

                print()

                print(
                    f"ERROR processing "
                    f"{filename}: {e}"
                )

        print()

    # ========================================================
    # RESULTS
    # ========================================================

    if not all_trades:

        print()
        print("=" * 90)
        print("NO TRADES FOUND")
        print("=" * 90)

        # Create an empty output file so GitHub Actions
        # can still upload the artifact.

        empty_columns = [
            "symbol",
            "signal_date",
            "trade_date",
            "direction",
            "entry_time",
            "exit_time",
            "entry_price",
            "exit_price",
            "gross_return_pct",
            "net_return_pct"
        ]

        pd.DataFrame(
            columns=empty_columns
        ).to_csv(
            OUTPUT_FILE,
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
    # OVERALL STATISTICS
    # ========================================================

    stats = calculate_statistics(
        trades
    )

    print()
    print("=" * 90)
    print("BACKTEST RESULTS")
    print("=" * 90)

    print(
        f"Total trades    : "
        f"{stats['trades']:,}"
    )

    print(
        f"Winners         : "
        f"{stats['wins']:,}"
    )

    print(
        f"Losers          : "
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

    # ========================================================
    # LONG / SHORT
    # ========================================================

    for direction in [
        "LONG",
        "SHORT"
    ]:

        subset = trades[
            trades["direction"]
            ==
            direction
        ]

        if subset.empty:
            continue

        s = calculate_statistics(
            subset
        )

        print()
        print(
            f"{direction} RESULTS"
        )

        print(
            f"Trades         : "
            f"{s['trades']:,}"
        )

        print(
            f"Win rate       : "
            f"{s['win_rate']:.2f}%"
        )

        print(
            f"Average return : "
            f"{s['average_return']:.4f}%"
        )

        print(
            f"Profit factor  : "
            f"{s['profit_factor']:.3f}"
        )

    # ========================================================
    # YEARLY RESULTS
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

        s = calculate_statistics(
            group
        )

        yearly_rows.append({

            "year":
                year,

            "trades":
                s["trades"],

            "wins":
                s["wins"],

            "losses":
                s["losses"],

            "win_rate":
                s["win_rate"],

            "average_return":
                s["average_return"],

            "profit_factor":
                s["profit_factor"],

            "total_return":
                s["total_return"]

        })

    yearly = pd.DataFrame(
        yearly_rows
    )

    if not yearly.empty:

        print()
        print("=" * 90)
        print("YEARLY RESULTS")
        print("=" * 90)

        print(
            yearly.to_string(
                index=False
            )
        )

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    trades.to_csv(
        OUTPUT_FILE,
        index=False
    )

    print()
    print("=" * 90)
    print("BACKTEST COMPLETE")
    print("=" * 90)

    print()
    print(
        f"Results saved to: "
        f"{OUTPUT_FILE}"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
