# =============================================================================
# NSE DAILY SCANNER — FAST + RELIABLE VERSION
# =============================================================================
#
# EXACT STRATEGY CONDITIONS
#
# 1) 3-MIN:
#       15:24 and 15:27 must be OPPOSITE trends
#       15:24 volume > 15:27 volume
#
# 2) 3-MIN MORNING:
#       09:15 and 09:18 must BOTH be in the SAME trend as 15:24
#
# 3) 1-MIN:
#       15:28 and 15:29 must be OPPOSITE trends
#       15:28 volume > 15:29 volume
#
# 4) 1-MIN / 3-MIN CONFIRMATION:
#       1-min 15:28 must match 3-min 15:24 direction
#
# FINAL DIRECTION:
#       3-min 15:24
#
# ENTRY:
#       Next trading day at 09:15 open
#
# EXIT:
#       15:27
#
# =============================================================================
#
# INSTALL
# =============================================================================
#
# pip install yfinance pandas requests nselib
#
# =============================================================================


import sys
import time
import random
import warnings

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

warnings.filterwarnings("ignore")


# =============================================================================
# IMPORT YFINANCE
# =============================================================================

try:

    import yfinance as yf

except ImportError:

    print()
    print("yfinance is missing.")
    print()
    print("Install with:")
    print("pip install yfinance pandas requests nselib")
    print()

    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

MAX_WORKERS = 12

MAX_RETRIES = 4

INITIAL_PERIOD = "3d"

FIVE_DAY_FALLBACK = "7d"

REQUEST_TIMEOUT = 20

MIN_RETRY_SLEEP = 1.0

MAX_RETRY_SLEEP = 3.0


# =============================================================================
# NSE UNIVERSE
# =============================================================================

NIFTY_50 = [

    "RELIANCE",
    "TCS",
    "HDFCBANK",
    "ICICIBANK",
    "INFY",
    "HINDUNILVR",
    "ITC",
    "SBIN",
    "BHARTIARTL",
    "KOTAKBANK",
    "LT",
    "AXISBANK",
    "BAJFINANCE",
    "ASIANPAINT",
    "MARUTI",
    "HCLTECH",
    "SUNPHARMA",
    "TITAN",
    "ULTRACEMCO",
    "NESTLEIND",
    "WIPRO",
    "ADANIENT",
    "ONGC",
    "NTPC",
    "POWERGRID",
    "M&M",
    "JSWSTEEL",
    "TATASTEEL",
    "TATAMOTORS",
    "COALINDIA",
    "BAJAJFINSV",
    "TECHM",
    "INDUSINDBK",
    "HDFCLIFE",
    "SBILIFE",
    "GRASIM",
    "DRREDDY",
    "DIVISLAB",
    "EICHERMOT",
    "BRITANNIA",
    "CIPLA",
    "APOLLOHOSP",
    "HEROMOTOCO",
    "BPCL",
    "TATACONSUM",
    "ADANIPORTS",
    "HINDALCO",
    "BAJAJ-AUTO",
    "SHRIRAMFIN",
    "LTIM",
    "UPL"

]


NIFTY_NEXT_150 = [

    "ABB",
    "ADANIENSOL",
    "ADANIGREEN",
    "ADANIPOWER",
    "AMBUJACEM",
    "DMART",
    "BANKBARODA",
    "BERGEPAINT",
    "BEL",
    "BOSCHLTD",
    "CANBK",
    "CHOLAFIN",
    "COLPAL",
    "DABUR",
    "DLF",
    "GAIL",
    "GODREJCP",
    "HAVELLS",
    "HAL",
    "ICICIGI",
    "ICICIPRULI",
    "IOC",
    "IRCTC",
    "IRFC",
    "JINDALSTEL",
    "JIOFIN",
    "LICI",
    "LODHA",
    "LUPIN",
    "MARICO",
    "MOTHERSON",
    "MRF",
    "NAUKRI",
    "NHPC",
    "PIDILITIND",
    "PFC",
    "PNB",
    "RECLTD",
    "SIEMENS",
    "SRF",
    "TATAPOWER",
    "TORNTPHARM",
    "TVSMOTOR",
    "UNIONBANK",
    "VBL",
    "VEDL",
    "ZOMATO",
    "ZYDUSLIFE",
    "PAYTM",
    "POLICYBZR",
    "PERSISTENT",
    "COFORGE",
    "MPHASIS",
    "OBEROIRLTY",
    "PIIND",
    "ASHOKLEY",
    "AUROPHARMA",
    "BANDHANBNK",
    "BATAINDIA",
    "BHARATFORG",
    "BHEL",
    "CGPOWER",
    "CONCOR",
    "CUMMINSIND",
    "DEEPAKNTR",
    "DIXON",
    "ESCORTS",
    "EXIDEIND",
    "FEDERALBNK",
    "GLAND",
    "GMRAIRPORT",
    "GODREJPROP",
    "GUJGASLTD",
    "HDFCAMC",
    "HINDPETRO",
    "IDEA",
    "IDFCFIRSTB",
    "IGL",
    "INDHOTEL",
    "INDIGO",
    "INDUSTOWER",
    "IPCALAB",
    "JSWENERGY",
    "JUBLFOOD",
    "KALYANKJIL",
    "L&TFH",
    "LALPATHLAB",
    "LAURUSLABS",
    "LTTS",
    "M&MFIN",
    "MANKIND",
    "MAXHEALTH",
    "METROPOLIS",
    "MFSL",
    "MUTHOOTFIN",
    "NATIONALUM",
    "NAVINFLUOR",
    "NMDC",
    "OFSS",
    "PAGEIND",
    "PATANJALI",
    "PETRONET",
    "PHOENIXLTD",
    "POLYCAB",
    "PRESTIGE",
    "RAMCOCEM",
    "RVNL",
    "SAIL",
    "SBICARD",
    "SCHAEFFLER",
    "SHREECEM",
    "SJVN",
    "SOLARINDS",
    "SONACOMS",
    "STARHEALTH",
    "SUNDARMFIN",
    "SUPREMEIND",
    "SUZLON",
    "SYNGENE",
    "TATACHEM",
    "TATACOMM",
    "TATAELXSI",
    "THERMAX",
    "TIINDIA",
    "TORNTPOWER",
    "TRENT",
    "TRIDENT",
    "UBL",
    "UCOBANK",
    "VOLTAS",
    "WHIRLPOOL",
    "YESBANK",
    "ZEEL",
    "ABCAPITAL",
    "ABFRL",
    "ALKEM",
    "APLAPOLLO",
    "APOLLOTYRE",
    "ASTRAL",
    "AUBANK",
    "BALKRISIND",
    "BANKINDIA",
    "BSOFT",
    "CANFINHOME",
    "CENTRALBK",
    "CROMPTON",
    "CYIENT",
    "DALBHARAT",
    "DELHIVERY",
    "DEVYANI",
    "EMAMILTD",
    "GICRE",
    "GLENMARK",
    "GNFC",
    "GODIGIT",
    "GRANULES",
    "GRSE",
    "HFCL",
    "HONAUT"

]


STOCK_UNIVERSE = list(
    dict.fromkeys(
        NIFTY_50 + NIFTY_NEXT_150
    )
)


# =============================================================================
# TRY TO LOAD LIVE NIFTY 500
# =============================================================================

try:

    from nselib import indices

    print("Attempting to load live Nifty 500 universe...")

    df = indices.constituent_stock_list(
        index_category="BroadMarketIndices",
        index_name="Nifty 500"
    )

    if (
        df is not None
        and not df.empty
        and "Symbol" in df.columns
    ):

        fetched = (
            df["Symbol"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .tolist()
        )

        fetched = list(
            dict.fromkeys(fetched)
        )

        if len(fetched) > len(STOCK_UNIVERSE):

            STOCK_UNIVERSE = fetched

            print(
                f"Loaded {len(STOCK_UNIVERSE)} "
                f"symbols from Nifty 500."
            )

except Exception as e:

    print(
        f"Nifty 500 loading failed: {e}"
    )

    print(
        f"Using existing universe: "
        f"{len(STOCK_UNIVERSE)} stocks."
    )


# =============================================================================
# TRY NSE OFFICIAL EQUITY LIST
# =============================================================================

try:

    import requests
    from io import StringIO

    print(
        "Attempting to load NSE official equity list..."
    )

    headers = {

        "User-Agent":
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/131.0 Safari/537.36",

        "Accept":
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,*/*;q=0.8",

        "Accept-Language":
            "en-US,en;q=0.9"

    }


    session = requests.Session()

    session.headers.update(headers)


    session.get(
        "https://www.nseindia.com",
        timeout=15
    )


    response = session.get(
        "https://nsearchives.nseindia.com/"
        "content/equities/sec_list.csv",
        timeout=20
    )


    response.raise_for_status()


    full_df = pd.read_csv(
        StringIO(response.text)
    )


    symbol_col = next(
        (
            c
            for c in full_df.columns
            if "symbol" in c.lower()
        ),
        None
    )


    series_col = next(
        (
            c
            for c in full_df.columns
            if "series" in c.lower()
        ),
        None
    )


    if symbol_col is not None:

        if series_col is not None:

            full_df = full_df[
                full_df[series_col]
                .astype(str)
                .str.strip()
                .str.upper()
                == "EQ"
            ]


        symbols = (
            full_df[symbol_col]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .tolist()
        )


        banned = (

            "ETF",
            "IETF",
            "BEES",
            "LIQUID",
            "GILT",
            "MUTUAL",
            "FUND",
            "INDEX"

        )


        symbols = [

            s
            for s in symbols
            if not any(
                word in s
                for word in banned
            )

        ]


        symbols = list(
            dict.fromkeys(symbols)
        )


        if len(symbols) > len(STOCK_UNIVERSE):

            STOCK_UNIVERSE = symbols

            print(
                f"Loaded {len(STOCK_UNIVERSE)} "
                f"stocks from NSE official list."
            )


except Exception as e:

    print(
        f"NSE official list unavailable: {e}"
    )


print()

print("=" * 70)

print(
    f"FINAL STOCK UNIVERSE: "
    f"{len(STOCK_UNIVERSE)}"
)

print("=" * 70)

print()


# =============================================================================
# REQUIRED MINUTE CANDLES
# =============================================================================

NEEDED_HM = {

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


# =============================================================================
# HELPER: DIRECTION
# =============================================================================

def candle_direction(
    open_price,
    close_price
):

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


# =============================================================================
# HELPER: CLEAN YAHOO DATA
# =============================================================================

def clean_yahoo_data(df):

    if df is None or df.empty:
        return None


    if isinstance(
        df.columns,
        pd.MultiIndex
    ):

        df.columns = [
            c[0]
            for c in df.columns
        ]


    df = df.reset_index()


    timestamp_col = None


    for candidate in (
        "Datetime",
        "Date",
        "datetime",
        "date"
    ):

        if candidate in df.columns:

            timestamp_col = candidate

            break


    if timestamp_col is None:

        timestamp_col = df.columns[0]


    ts = pd.to_datetime(
        df[timestamp_col],
        errors="coerce"
    )


    valid_ts = ts.notna()


    if not valid_ts.any():
        return None


    df = df.loc[
        valid_ts
    ].copy()


    ts = ts.loc[
        valid_ts
    ]


    if ts.dt.tz is not None:

        ts = ts.dt.tz_convert(
            "Asia/Kolkata"
        )

    else:

        ts = ts.dt.tz_localize(
            "Asia/Kolkata"
        )


    required = [
        "Open",
        "Close",
        "Volume"
    ]


    if not all(
        c in df.columns
        for c in required
    ):

        return None


    out = pd.DataFrame({

        "date":
            ts.dt.strftime(
                "%Y-%m-%d"
            ),

        "hm":
            (
                ts.dt.hour * 100
                +
                ts.dt.minute
            ),

        "open":
            pd.to_numeric(
                df["Open"],
                errors="coerce"
            ).values,

        "close":
            pd.to_numeric(
                df["Close"],
                errors="coerce"
            ).values,

        "volume":
            pd.to_numeric(
                df["Volume"],
                errors="coerce"
            ).values

    })


    # NSE regular session only.

    out = out[
        out["hm"].between(
            915,
            1529
        )
    ]


    out = out.dropna(
        subset=[
            "open",
            "close",
            "volume"
        ]
    )


    out = out.drop_duplicates(
        subset=[
            "date",
            "hm"
        ],
        keep="last"
    )


    return out


# =============================================================================
# DATA COMPLETENESS
# =============================================================================

def find_complete_days(rows):

    if rows is None or rows.empty:
        return []


    by_date = rows.groupby(
        "date"
    )


    complete_days = []


    for date, group in by_date:

        available = set(
            group["hm"].astype(int)
        )


        if NEEDED_HM.issubset(
            available
        ):

            complete_days.append(
                date
            )


    return sorted(
        complete_days
    )


# =============================================================================
# FETCH DATA
# =============================================================================

def download_symbol(
    symbol,
    period
):

    ticker = f"{symbol}.NS"

    last_error = None


    for attempt in range(
        MAX_RETRIES
    ):

        try:

            df = yf.download(

                ticker,

                period=period,

                interval="1m",

                progress=False,

                auto_adjust=False,

                actions=False,

                threads=False,

                timeout=REQUEST_TIMEOUT

            )


            if (
                df is not None
                and not df.empty
            ):

                cleaned = clean_yahoo_data(
                    df
                )


                if (
                    cleaned is not None
                    and not cleaned.empty
                ):

                    return cleaned


        except Exception as e:

            last_error = e


        if attempt < MAX_RETRIES - 1:

            sleep_time = min(

                MAX_RETRY_SLEEP,

                MIN_RETRY_SLEEP
                *
                (2 ** attempt)

            )


            sleep_time += random.uniform(
                0,
                0.75
            )


            time.sleep(
                sleep_time
            )


    return None


# =============================================================================
# FETCH SYMBOL WITH FALLBACK
# =============================================================================

def fetch_symbol_rows(symbol):

    rows = download_symbol(
        symbol,
        INITIAL_PERIOD
    )


    if rows is not None:

        complete_days = find_complete_days(
            rows
        )


        if complete_days:

            return rows, "OK"


    rows = download_symbol(
        symbol,
        FIVE_DAY_FALLBACK
    )


    if rows is not None:

        complete_days = find_complete_days(
            rows
        )


        if complete_days:

            return rows, "FALLBACK"


        return rows, "INCOMPLETE"


    return None, "NO_DATA"


# =============================================================================
# EVALUATE STRATEGY
# =============================================================================

def evaluate_rows(rows):

    if rows is None or rows.empty:

        return {
            "status": "NO_DATA"
        }


    # -------------------------------------------------------------------------
    # GROUP BY DATE
    # -------------------------------------------------------------------------

    by_date = {}


    for _, row in rows.iterrows():

        date = row["date"]

        hm = int(row["hm"])


        if date not in by_date:

            by_date[date] = {}


        by_date[date][hm] = row


    # -------------------------------------------------------------------------
    # VALID CANDLE
    # -------------------------------------------------------------------------

    def valid_candle(row):

        if row is None:
            return False

        try:

            return (
                pd.notna(row["open"])
                and
                pd.notna(row["close"])
                and
                pd.notna(row["volume"])
            )

        except Exception:

            return False


    # -------------------------------------------------------------------------
    # FIND LATEST COMPLETE SIGNAL DAY
    # -------------------------------------------------------------------------

    complete_days = []


    for date, day in by_date.items():

        if all(
            hm in day
            for hm in NEEDED_HM
        ):

            complete_days.append(
                date
            )


    if not complete_days:

        return {
            "status": "INCOMPLETE"
        }


    signal_date = max(
        complete_days
    )


    day = by_date[
        signal_date
    ]


    # -------------------------------------------------------------------------
    # REQUIRED 1-MIN CANDLES
    # -------------------------------------------------------------------------

    required_rows = [

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

    ]


    if not all(
        hm in day
        for hm in required_rows
    ):

        return {
            "status": "INCOMPLETE",
            "signal_date": signal_date
        }


    for hm in required_rows:

        if not valid_candle(
            day[hm]
        ):

            return {
                "status": "INCOMPLETE",
                "signal_date": signal_date
            }


    # -------------------------------------------------------------------------
    # 3-MINUTE CANDLE AGGREGATION
    # -------------------------------------------------------------------------

    def aggregate_3m(
        minute_rows
    ):

        if not minute_rows:
            return None


        first = minute_rows[0]

        last = minute_rows[-1]


        if any(
            not valid_candle(r)
            for r in minute_rows
        ):

            return None


        return {

            "open":
                float(first["open"]),

            "close":
                float(last["close"]),

            "volume":
                sum(
                    float(r["volume"])
                    for r in minute_rows
                )

        }


    # -------------------------------------------------------------------------
    # 09:15 3-MIN
    #
    # 09:15 = 09:15, 09:16, 09:17
    # -------------------------------------------------------------------------

    candle_915_3m = aggregate_3m(
        [
            day[915],
            day[916],
            day[917]
        ]
    )


    # -------------------------------------------------------------------------
    # 09:18 3-MIN
    #
    # 09:18 = 09:18, 09:19, 09:20
    # -------------------------------------------------------------------------

    candle_918_3m = aggregate_3m(
        [
            day[918],
            day[919],
            day[920]
        ]
    )


    # -------------------------------------------------------------------------
    # 15:24 3-MIN
    #
    # 15:24 = 15:24, 15:25, 15:26
    # -------------------------------------------------------------------------

    candle_1524_3m = aggregate_3m(
        [
            day[1524],
            day[1525],
            day[1526]
        ]
    )


    # -------------------------------------------------------------------------
    # 15:27 3-MIN
    #
    # 15:27 = 15:27, 15:28, 15:29
    # -------------------------------------------------------------------------

    candle_1527_3m = aggregate_3m(
        [
            day[1527],
            day[1528],
            day[1529]
        ]
    )


    if any(
        x is None
        for x in [
            candle_915_3m,
            candle_918_3m,
            candle_1524_3m,
            candle_1527_3m
        ]
    ):

        return {
            "status": "INCOMPLETE",
            "signal_date": signal_date
        }


    # -------------------------------------------------------------------------
    # DIRECTIONS
    # -------------------------------------------------------------------------

    dir_915_3m = candle_direction(

        candle_915_3m["open"],

        candle_915_3m["close"]

    )


    dir_918_3m = candle_direction(

        candle_918_3m["open"],

        candle_918_3m["close"]

    )


    dir_1524 = candle_direction(

        candle_1524_3m["open"],

        candle_1524_3m["close"]

    )


    dir_1527 = candle_direction(

        candle_1527_3m["open"],

        candle_1527_3m["close"]

    )


    vol_1524 = candle_1524_3m[
        "volume"
    ]


    vol_1527 = candle_1527_3m[
        "volume"
    ]


    # -------------------------------------------------------------------------
    # 1-MINUTE 15:28 / 15:29
    # -------------------------------------------------------------------------

    candle_1528_1m = day[1528]

    candle_1529_1m = day[1529]


    dir_1528_1m = candle_direction(

        candle_1528_1m["open"],

        candle_1528_1m["close"]

    )


    dir_1529_1m = candle_direction(

        candle_1529_1m["open"],

        candle_1529_1m["close"]

    )


    vol_1528 = float(
        candle_1528_1m["volume"]
    )


    vol_1529 = float(
        candle_1529_1m["volume"]
    )


    # =========================================================================
    # STRATEGY CONDITIONS
    # =========================================================================

    # -------------------------------------------------------------------------
    # CONDITION 1
    #
    # 15:24 and 15:27 must be opposite.
    # 15:24 volume must be greater than 15:27.
    # -------------------------------------------------------------------------

    cond1 = (

        dir_1524 != 0

        and

        dir_1527 != 0

        and

        dir_1524 != dir_1527

        and

        vol_1524 > vol_1527

    )


    # -------------------------------------------------------------------------
    # CONDITION 2
    #
    # 09:15 and 09:18 must BOTH be in the same
    # trend as 15:24.
    # -------------------------------------------------------------------------

    cond2 = (

        dir_1524 != 0

        and

        dir_915_3m != 0

        and

        dir_918_3m != 0

        and

        dir_915_3m == dir_1524

        and

        dir_918_3m == dir_1524

    )


    # -------------------------------------------------------------------------
    # CONDITION 3
    #
    # 15:28 and 15:29 must be opposite.
    # 15:28 volume must be greater than 15:29.
    # -------------------------------------------------------------------------

    cond3 = (

        dir_1528_1m != 0

        and

        dir_1529_1m != 0

        and

        dir_1528_1m != dir_1529_1m

        and

        vol_1528 > vol_1529

    )


    # -------------------------------------------------------------------------
    # CONDITION 4
    #
    # 1-minute 15:28 must match 3-minute 15:24.
    # -------------------------------------------------------------------------

    cond4 = (

        dir_1528_1m != 0

        and

        dir_1524 != 0

        and

        dir_1528_1m == dir_1524

    )


    # -------------------------------------------------------------------------
    # FINAL RESULT
    # -------------------------------------------------------------------------

    passed = (

        cond1
        and
        cond2
        and
        cond3
        and
        cond4

    )


    # -------------------------------------------------------------------------
    # FINAL DIRECTION
    # -------------------------------------------------------------------------

    if dir_1524 == 1:

        direction = "LONG"

    elif dir_1524 == -1:

        direction = "SHORT"

    else:

        direction = None


    # =========================================================================
    # RETURN RESULT
    # =========================================================================

    return {

        "status": "MATCH" if passed else "NO_MATCH",

        "signal_date": signal_date,

        "direction": direction,

        "dir_915_3m":
            dir_915_3m,

        "dir_918_3m":
            dir_918_3m,

        "dir_1524":
            dir_1524,

        "dir_1527":
            dir_1527,

        "dir_1528_1m":
            dir_1528_1m,

        "dir_1529_1m":
            dir_1529_1m,

        "vol_1524":
            vol_1524,

        "vol_1527":
            vol_1527,

        "vol_1528":
            vol_1528,

        "vol_1529":
            vol_1529,

        "cond1":
            cond1,

        "cond2":
            cond2,

        "cond3":
            cond3,

        "cond4":
            cond4,

        "passed":
            passed

    }


# =============================================================================
# SCAN ONE SYMBOL
# =============================================================================

def scan_symbol(symbol):

    try:

        rows, fetch_status = fetch_symbol_rows(
            symbol
        )


        if rows is None:

            return {

                "symbol":
                    symbol,

                "status":
                    "NO_DATA"

            }


        result = evaluate_rows(
            rows
        )


        result["symbol"] = symbol

        result["fetch_status"] = fetch_status


        return result


    except Exception as e:

        return {

            "symbol":
                symbol,

            "status":
                "ERROR",

            "error":
                str(e)

        }


# =============================================================================
# FORMAT DIRECTION
# =============================================================================

def direction_text(direction):

    if direction == 1:
        return "GREEN"

    if direction == -1:
        return "RED"

    return "DOJI"


# =============================================================================
# FORMAT VOLUME
# =============================================================================

def format_volume(value):

    if value is None:
        return "-"


    try:

        value = float(value)


        if value >= 1_000_000:

            return (
                f"{value / 1_000_000:.2f}M"
            )


        if value >= 1_000:

            return (
                f"{value / 1_000:.1f}K"
            )


        return f"{value:.0f}"


    except Exception:

        return "-"


# =============================================================================
# HTML REPORT
# =============================================================================

def generate_html(
    results,
    signal_date,
    output_file="index.html"
):

    matches = [

        r
        for r in results
        if r.get("status") == "MATCH"

    ]


    matches = sorted(

        matches,

        key=lambda x: (

            0
            if x.get("direction") == "LONG"
            else 1,

            x.get("symbol", "")

        )

    )


    total = len(results)

    match_count = len(matches)


    long_count = sum(

        1
        for r in matches
        if r.get("direction") == "LONG"

    )


    short_count = sum(

        1
        for r in matches
        if r.get("direction") == "SHORT"

    )


    rows_html = ""


    for r in matches:

        symbol = r.get(
            "symbol",
            ""
        )


        direction = r.get(
            "direction",
            ""
        )


        if direction == "LONG":

            direction_class = "long"


        else:

            direction_class = "short"


        rows_html += f"""

        <tr>

            <td>
                <strong>{symbol}</strong>
            </td>

            <td class="{direction_class}">
                {direction}
            </td>

            <td>
                {direction_text(r.get("dir_915_3m"))}
            </td>

            <td>
                {direction_text(r.get("dir_918_3m"))}
            </td>

            <td>
                {direction_text(r.get("dir_1524"))}
            </td>

            <td>
                {direction_text(r.get("dir_1527"))}
            </td>

            <td>
                {direction_text(r.get("dir_1528_1m"))}
            </td>

            <td>
                {direction_text(r.get("dir_1529_1m"))}
            </td>

            <td>
                {format_volume(r.get("vol_1524"))}
            </td>

            <td>
                {format_volume(r.get("vol_1527"))}
            </td>

            <td>
                {format_volume(r.get("vol_1528"))}
            </td>

            <td>
                {format_volume(r.get("vol_1529"))}
            </td>

        </tr>

        """


    if not rows_html:

        rows_html = """

        <tr>

            <td
                colspan="12"
                class="no-match"
            >

                No stocks matched all 4 conditions.

            </td>

        </tr>

        """


    generated_at = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


    html = f"""

<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>
    NSE Daily Scanner
</title>


<style>

body {{

    font-family:
        Arial,
        Helvetica,
        sans-serif;

    margin: 0;

    background: #f5f7fa;

    color: #222;

}}


.container {{

    max-width: 1600px;

    margin: auto;

    padding: 20px;

}}


h1 {{

    margin-bottom: 5px;

}}


.subtitle {{

    color: #666;

    margin-bottom: 20px;

}}


.cards {{

    display: grid;

    grid-template-columns:
        repeat(
            auto-fit,
            minmax(
                180px,
                1fr
            )
        );

    gap: 15px;

    margin-bottom: 25px;

}}


.card {{

    background: white;

    padding: 18px;

    border-radius: 10px;

    box-shadow:
        0 2px 8px
        rgba(
            0,
            0,
            0,
            0.08
        );

}}


.card-title {{

    color: #777;

    font-size: 13px;

    margin-bottom: 8px;

}}


.card-value {{

    font-size: 26px;

    font-weight: bold;

}}


table {{

    width: 100%;

    border-collapse:
        collapse;

    background: white;

    border-radius: 10px;

    overflow: hidden;

}}


th {{

    background: #202938;

    color: white;

    padding: 11px 8px;

    font-size: 12px;

    white-space: nowrap;

}}


td {{

    padding: 11px 8px;

    border-bottom:
        1px solid #eee;

    text-align: center;

    font-size: 13px;

}}


tr:hover {{

    background: #f8fafc;

}}


.long {{

    color: #0a8f4d;

    font-weight: bold;

}}


.short {{

    color: #d12c2c;

    font-weight: bold;

}}


.no-match {{

    padding: 35px;

    color: #777;

}}


.strategy {{

    margin-top: 25px;

    background: white;

    padding: 20px;

    border-radius: 10px;

    box-shadow:
        0 2px 8px
        rgba(
            0,
            0,
            0,
            0.08
        );

}}


.strategy li {{

    margin-bottom: 8px;

}}


.footer {{

    margin-top: 25px;

    color: #777;

    font-size: 12px;

}}


@media (
    max-width: 900px
) {{

    table {{

        display: block;

        overflow-x: auto;

    }}

}}

</style>

</head>


<body>

<div class="container">


<h1>
    NSE Daily Scanner
</h1>


<div class="subtitle">

    Signal date:
    <strong>{signal_date}</strong>

    &nbsp; | &nbsp;

    Generated:
    <strong>{generated_at}</strong>

</div>


<div class="cards">


<div class="card">

    <div class="card-title">
        Stocks Scanned
    </div>

    <div class="card-value">
        {total}
    </div>

</div>


<div class="card">

    <div class="card-title">
        Matches
    </div>

    <div class="card-value">
        {match_count}
    </div>

</div>


<div class="card">

    <div class="card-title">
        LONG
    </div>

    <div class="card-value long">
        {long_count}
    </div>

</div>


<div class="card">

    <div class="card-title">
        SHORT
    </div>

    <div class="card-value short">
        {short_count}
    </div>

</div>


</div>


<table>

<thead>

<tr>

<th>
    Stock
</th>

<th>
    Direction
</th>

<th>
    09:15<br>3M
</th>

<th>
    09:18<br>3M
</th>

<th>
    15:24<br>3M
</th>

<th>
    15:27<br>3M
</th>

<th>
    15:28<br>1M
</th>

<th>
    15:29<br>1M
</th>

<th>
    15:24<br>Vol
</th>

<th>
    15:27<br>Vol
</th>

<th>
    15:28<br>Vol
</th>

<th>
    15:29<br>Vol
</th>

</tr>

</thead>


<tbody>

{rows_html}

</tbody>

</table>


<div class="strategy">

<h2>
    Strategy Conditions
</h2>


<ol>

<li>
    <strong>3-minute:</strong>
    15:24 and 15:27 must be opposite trends,
    and 15:24 volume must be greater than 15:27.
</li>


<li>
    <strong>3-minute morning:</strong>
    09:15 and 09:18 must both be in the same
    trend as 15:24.
</li>


<li>
    <strong>1-minute:</strong>
    15:28 and 15:29 must be opposite trends,
    and 15:28 volume must be greater than 15:29.
</li>


<li>
    <strong>1-minute / 3-minute confirmation:</strong>
    1-minute 15:28 must match 3-minute 15:24.
</li>


<li>
    <strong>Final direction:</strong>
    3-minute 15:24.
</li>


<li>
    <strong>Entry:</strong>
    Next trading day at 09:15 open.
</li>


<li>
    <strong>Exit:</strong>
    15:27.
</li>

</ol>

</div>


<div class="footer">

    Scanner uses Yahoo Finance 1-minute data
    and NSE symbols.

    The scanner only accepts a signal day when
    all required minute candles are available.

</div>


</div>

</body>

</html>

"""


    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(html)


# =============================================================================
# MAIN SCANNER
# =============================================================================

def main():

    start_time = time.time()


    print()

    print("=" * 70)

    print(
        "NSE DAILY SCANNER"
    )

    print("=" * 70)

    print()


    print(
        f"Scanning {len(STOCK_UNIVERSE)} stocks..."
    )

    print()


    results = []


    # -------------------------------------------------------------------------
    # PARALLEL DOWNLOAD
    # -------------------------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:


        future_map = {

            executor.submit(
                scan_symbol,
                symbol
            ):
                symbol

            for symbol in STOCK_UNIVERSE

        }


        completed = 0

        total = len(
            future_map
        )


        for future in as_completed(
            future_map
        ):

            symbol = future_map[
                future
            ]


            try:

                result = future.result()

                results.append(
                    result
                )


            except Exception as e:

                results.append({

                    "symbol":
                        symbol,

                    "status":
                        "ERROR",

                    "error":
                        str(e)

                })


            completed += 1


            if (
                completed % 25 == 0
                or completed == total
            ):

                print(
                    f"Progress: "
                    f"{completed}/{total}"
                )


    # -------------------------------------------------------------------------
    # FIND SIGNAL DATE
    # -------------------------------------------------------------------------

    signal_dates = [

        r.get("signal_date")

        for r in results

        if r.get("signal_date")

    ]


    if signal_dates:

        signal_date = max(
            signal_dates
        )

    else:

        signal_date = (
            datetime.now()
            .strftime("%Y-%m-%d")
        )


    # -------------------------------------------------------------------------
    # MATCHES
    # -------------------------------------------------------------------------

    matches = [

        r
        for r in results
        if r.get("status") == "MATCH"

    ]


    matches = sorted(

        matches,

        key=lambda x:
            x.get("symbol", "")

    )


    # -------------------------------------------------------------------------
    # GENERATE HTML
    # -------------------------------------------------------------------------

    generate_html(

        results,

        signal_date,

        "index.html"

    )


    # -------------------------------------------------------------------------
    # CONSOLE OUTPUT
    # -------------------------------------------------------------------------

    print()

    print("=" * 70)

    print(
        "SCAN COMPLETE"
    )

    print("=" * 70)

    print()


    print(
        f"Signal date : {signal_date}"
    )


    print(
        f"Stocks      : {len(results)}"
    )


    print(
        f"Matches     : {len(matches)}"
    )


    print()


    if matches:

        print(
            "MATCHING STOCKS:"
        )

        print()


        for r in matches:

            print(
                f"{r['symbol']:15s}"
                f" {r['direction']}"
            )


    else:

        print(
            "No stocks matched all conditions."
        )


    print()


    elapsed = (
        time.time()
        -
        start_time
    )


    print(
        f"Time taken: "
        f"{elapsed:.1f} seconds"
    )


    print()

    print(
        "HTML report saved as:"
    )

    print(
        "index.html"
    )

    print()

    print("=" * 70)


# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":

    main()
