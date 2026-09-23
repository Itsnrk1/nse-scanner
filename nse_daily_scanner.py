# =============================================================================
# NSE DAILY MOMENTUM SCANNER â€” FIXED VERSION
# =============================================================================
#
# MAIN LIVE SCANNER
#
# This scanner:
#   - Uses Yahoo Finance 1-minute data
#   - Does NOT use Parquet files
#   - Does NOT require nselib
#   - Attempts to load the current Nifty 500 universe
#   - Falls back to the NSE official equity list
#   - Falls back again to the built-in universe
#   - Generates index.html
#
# =============================================================================
# EXACT CURRENT STRATEGY
# =============================================================================
#
# 1) 3-MIN:
#       15:24 volume > 15:27 volume
#
#       15:24 and 15:27 trend relationship DOES NOT MATTER.
#       They may be SAME or OPPOSITE.
#
# 2) 1-MIN:
#       15:28 and 15:29 must be OPPOSITE trends.
#       15:28 volume > 15:29 volume
#
# 3) 1-MIN / 3-MIN:
#       1-min 15:28 trend must equal 3-min 15:24 trend.
#
# 4) FINAL DIRECTION:
#       1-min 15:28 trend
#
# 5) TRADE:
#       Entry = NEXT trading day 09:15 OPEN
#       Exit  = 15:27
#
#       Morning 09:15 / 09:18 candles are NOT used or required.
#
# =============================================================================
# INSTALL
# =============================================================================
#
# GitHub Actions:
#
#   python -m pip install "yfinance==1.7.0" pandas requests curl_cffi
#
# =============================================================================


import html
import sys
import time
import random
import warnings

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

import pandas as pd
import requests


warnings.filterwarnings("ignore")


# =============================================================================
# YFINANCE
# =============================================================================

try:

    import yfinance as yf

except ImportError:

    print()
    print("ERROR: yfinance is not installed.")
    print()
    print(
        'Install with: '
        'pip install "yfinance==1.7.0" pandas requests curl_cffi'
    )
    print()

    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

# -------------------------------------------------------------------------
# PARALLEL WORKERS
# -------------------------------------------------------------------------
#
# Yahoo can throttle when too many requests are made simultaneously.
#
# 8 is deliberately conservative.
#
MAX_WORKERS = 8


# -------------------------------------------------------------------------
# RETRIES
# -------------------------------------------------------------------------
#
# We do NOT repeatedly retry a dead/invalid ticker.
#
MAX_RETRIES = 2


# -------------------------------------------------------------------------
# DATA PERIOD
# -------------------------------------------------------------------------

INITIAL_PERIOD = "3d"

FALLBACK_PERIOD = "7d"


# -------------------------------------------------------------------------
# REQUEST TIMEOUT
# -------------------------------------------------------------------------

REQUEST_TIMEOUT = 20


# =============================================================================
# OFFICIAL UNIVERSE SOURCES
# =============================================================================

# Current Nifty 500 constituent CSV.
NIFTY500_URL = (
    "https://www.niftyindices.com/"
    "IndexConstituent/"
    "ind_nifty500list.csv"
)


# NSE official equity security list.
NSE_EQUITY_URL = (
    "https://nsearchives.nseindia.com/"
    "content/equities/sec_list.csv"
)


# =============================================================================
# REQUIRED 1-MINUTE CANDLES
# =============================================================================

NEEDED_HM = {
    # Only the afternoon window used by the current strategy.
    # 3m 15:24 = 15:24,25,26
    # 3m 15:27 = 15:27,28,29
    1524,
    1525,
    1526,
    1527,
    1528,
    1529,
}


# =============================================================================
# BUILT-IN FALLBACK UNIVERSE
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
    "UPL",

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
    "HONAUT",

]


FALLBACK_UNIVERSE = list(
    dict.fromkeys(
        NIFTY_50 + NIFTY_NEXT_150
    )
)


# =============================================================================
# HTTP HEADERS
# =============================================================================

NSE_HEADERS = {

    "User-Agent":
        "Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/131.0 Safari/537.36",

    "Accept":
        "text/csv,text/html,application/xhtml+xml,"
        "application/xml;q=0.9,*/*;q=0.8",

    "Accept-Language":
        "en-US,en;q=0.9",

    "Referer":
        "https://www.niftyindices.com/",

}


# =============================================================================
# NORMALIZE SYMBOLS
# =============================================================================

def normalize_symbols(values):

    symbols = []

    for value in values:

        symbol = str(value).strip().upper()

        if not symbol:
            continue

        if symbol == "NAN":
            continue

        symbols.append(symbol)

    return list(
        dict.fromkeys(symbols)
    )


# =============================================================================
# LOAD NIFTY 500
# =============================================================================

def load_nifty500():

    try:

        print()
        print(
            "Attempting to load current "
            "Nifty 500 universe..."
        )

        session = requests.Session()

        session.headers.update(
            NSE_HEADERS
        )

        # Warm the domain.
        try:

            session.get(
                "https://www.niftyindices.com/",
                timeout=15
            )

        except Exception:
            pass

        response = session.get(
            NIFTY500_URL,
            timeout=20
        )

        response.raise_for_status()

        df = pd.read_csv(
            StringIO(response.text)
        )

        symbol_column = None

        for column in df.columns:

            if (
                str(column)
                .strip()
                .lower()
                == "symbol"
            ):

                symbol_column = column
                break

        if symbol_column is None:

            raise ValueError(
                "Symbol column not found"
            )

        symbols = normalize_symbols(
            df[symbol_column]
            .dropna()
            .tolist()
        )

        # A Nifty 500 list should be roughly
        # 500 securities. Reject obviously bad
        # responses.
        if len(symbols) < 450:

            raise ValueError(
                f"Only {len(symbols)} symbols returned"
            )

        print(
            f"Nifty 500 loaded successfully: "
            f"{len(symbols)} symbols"
        )

        return symbols

    except Exception as e:

        print(
            "Nifty 500 loading failed:"
            f" {e}"
        )

        return None


# =============================================================================
# LOAD NSE OFFICIAL EQUITY LIST
# =============================================================================

def load_nse_equity_list():

    try:

        print()
        print(
            "Attempting to load NSE official "
            "equity list..."
        )

        session = requests.Session()

        session.headers.update({

            **NSE_HEADERS,

            "Referer":
                "https://www.nseindia.com/",

        })

        try:

            session.get(
                "https://www.nseindia.com/",
                timeout=15
            )

        except Exception:
            pass

        response = session.get(
            NSE_EQUITY_URL,
            timeout=20
        )

        response.raise_for_status()

        df = pd.read_csv(
            StringIO(response.text)
        )

        symbol_column = next(
            (
                c
                for c in df.columns
                if "symbol"
                in str(c).lower()
            ),
            None
        )

        series_column = next(
            (
                c
                for c in df.columns
                if "series"
                in str(c).lower()
            ),
            None
        )

        if symbol_column is None:

            raise ValueError(
                "NSE Symbol column not found"
            )

        # Keep normal equity series.
        if series_column is not None:

            df = df[
                df[series_column]
                .astype(str)
                .str.strip()
                .str.upper()
                == "EQ"
            ]

        symbols = normalize_symbols(
            df[symbol_column]
            .dropna()
            .tolist()
        )

        if len(symbols) < 300:

            raise ValueError(
                f"Only {len(symbols)} EQ symbols returned"
            )

        print(
            f"NSE official equity list loaded: "
            f"{len(symbols)} symbols"
        )

        return symbols

    except Exception as e:

        print(
            "NSE official list unavailable:"
            f" {e}"
        )

        return None


# =============================================================================
# GET FINAL UNIVERSE
# =============================================================================

def get_stock_universe():

    print()
    print("=" * 70)
    print("LOADING NSE STOCK UNIVERSE")
    print("=" * 70)

    # ---------------------------------------------------------------------
    # FIRST: NIFTY 500
    # ---------------------------------------------------------------------

    symbols = load_nifty500()

    if symbols:

        return (
            symbols,
            "Nifty 500 official CSV"
        )


    # ---------------------------------------------------------------------
    # SECOND: NSE EQUITY LIST
    # ---------------------------------------------------------------------

    symbols = load_nse_equity_list()

    if symbols:

        return (
            symbols,
            "NSE official equity list"
        )


    # ---------------------------------------------------------------------
    # THIRD: BUILT-IN FALLBACK
    # ---------------------------------------------------------------------

    print()

    print(
        "Using built-in fallback universe:"
        f" {len(FALLBACK_UNIVERSE)} symbols"
    )

    return (
        FALLBACK_UNIVERSE,
        "Built-in fallback"
    )


# =============================================================================
# CLEAN YAHOO DATA
# =============================================================================

def clean_yahoo_data(df):

    if df is None:
        return None

    if df.empty:
        return None

    df = df.copy()


    # ---------------------------------------------------------------------
    # FLATTEN MULTIINDEX
    # ---------------------------------------------------------------------

    if isinstance(
        df.columns,
        pd.MultiIndex
    ):

        df.columns = [

            column[0]

            for column in df.columns

        ]


    # ---------------------------------------------------------------------
    # RESET INDEX
    # ---------------------------------------------------------------------

    df = df.reset_index()


    # ---------------------------------------------------------------------
    # FIND TIMESTAMP
    # ---------------------------------------------------------------------

    timestamp_column = None

    for candidate in (

        "Datetime",
        "Date",
        "datetime",
        "date",

    ):

        if candidate in df.columns:

            timestamp_column = candidate
            break


    if timestamp_column is None:

        return None


    ts = pd.to_datetime(

        df[timestamp_column],

        errors="coerce"

    )


    valid = ts.notna()


    if not valid.any():

        return None


    df = df.loc[
        valid
    ].copy()


    ts = ts.loc[
        valid
    ]


    # ---------------------------------------------------------------------
    # INDIA TIME
    # ---------------------------------------------------------------------

    if ts.dt.tz is not None:

        ts = ts.dt.tz_convert(
            "Asia/Kolkata"
        )

    else:

        ts = ts.dt.tz_localize(
            "Asia/Kolkata"
        )


    # ---------------------------------------------------------------------
    # REQUIRED COLUMNS
    # ---------------------------------------------------------------------

    required = [

        "Open",
        "Close",
        "Volume",

    ]


    if not all(

        column in df.columns

        for column in required

    ):

        return None


    # ---------------------------------------------------------------------
    # CREATE CLEAN DATAFRAME
    # ---------------------------------------------------------------------

    out = pd.DataFrame({

        "date":
            ts.dt.strftime(
                "%Y-%m-%d"
            ).values,

        "hm":
            (
                ts.dt.hour * 100
                +
                ts.dt.minute
            ).values,

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
            ).values,

    })


    # ---------------------------------------------------------------------
    # REGULAR NSE SESSION ONLY
    # ---------------------------------------------------------------------

    out = out[
        out["hm"].between(
            915,
            1529
        )
    ]


    # ---------------------------------------------------------------------
    # REMOVE BAD VALUES
    # ---------------------------------------------------------------------

    out = out.dropna(
        subset=[
            "open",
            "close",
            "volume"
        ]
    )


    # ---------------------------------------------------------------------
    # REMOVE DUPLICATE MINUTES
    # ---------------------------------------------------------------------

    out = out.drop_duplicates(

        subset=[
            "date",
            "hm"
        ],

        keep="last"

    )


    if out.empty:

        return None


    return out


# =============================================================================
# FIND COMPLETE DAYS
# =============================================================================

def find_complete_days(rows):

    if rows is None:
        return []

    if rows.empty:
        return []


    complete_days = []


    for date, group in rows.groupby(
        "date"
    ):

        available = set(

            group["hm"]
            .astype(int)

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
# YAHOO DOWNLOAD
# =============================================================================

def yahoo_download(
    symbol,
    period
):

    ticker = f"{symbol}.NS"


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

                timeout=REQUEST_TIMEOUT,

                prepost=False,

            )


            cleaned = clean_yahoo_data(
                df
            )


            if (
                cleaned is not None
                and not cleaned.empty
            ):

                return (
                    cleaned,
                    None
                )


            # Empty Yahoo result normally means
            # no usable data for this symbol.
            return (
                None,
                "NO_DATA"
            )


        except Exception as e:

            message = str(e)

            message = (
                message
                .replace("\n", " ")
            )


            if attempt < (
                MAX_RETRIES - 1
            ):

                # Small backoff.
                time.sleep(

                    (
                        1.5 ** attempt
                    )
                    +
                    random.uniform(
                        0.1,
                        0.5
                    )

                )

            else:

                return (

                    None,
                    message[:240]

                )


    return (
        None,
        "NO_DATA"
    )


# =============================================================================
# FETCH SYMBOL
# =============================================================================

def fetch_symbol_rows(
    symbol
):

    # ---------------------------------------------------------------------
    # FIRST ATTEMPT â€” 3 DAYS
    # ---------------------------------------------------------------------

    rows, error = yahoo_download(

        symbol,
        INITIAL_PERIOD

    )


    if (
        rows is not None
        and find_complete_days(rows)
    ):

        return (
            rows,
            "OK"
        )


    # ---------------------------------------------------------------------
    # SECOND ATTEMPT â€” 7 DAYS
    # ---------------------------------------------------------------------
    #
    # Only one fallback request.
    #
    # This prevents a bad ticker from producing
    # many unnecessary Yahoo requests.
    #

    rows2, error2 = yahoo_download(

        symbol,
        FALLBACK_PERIOD

    )


    if (
        rows2 is not None
        and find_complete_days(rows2)
    ):

        return (
            rows2,
            "FALLBACK"
        )


    if rows2 is not None:

        return (
            rows2,
            "INCOMPLETE"
        )


    return (
        None,
        "NO_DATA"
    )


# =============================================================================
# CANDLE DIRECTION
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
# EVALUATE STRATEGY
# =============================================================================

def evaluate_rows(
    rows
):

    if rows is None:

        return {
            "status": "NO_DATA"
        }


    if rows.empty:

        return {
            "status": "NO_DATA"
        }


    # ---------------------------------------------------------------------
    # BUILD DATE/MINUTE MAP
    # ---------------------------------------------------------------------

    by_date = {}


    for _, row in rows.iterrows():

        date = row["date"]

        hm = int(
            row["hm"]
        )


        if date not in by_date:

            by_date[date] = {}


        by_date[date][hm] = row


    # ---------------------------------------------------------------------
    # FIND COMPLETE DAYS
    # ---------------------------------------------------------------------

    complete_dates = []


    for date, minute_map in (
        by_date.items()
    ):

        if all(

            hm in minute_map

            for hm in NEEDED_HM

        ):

            complete_dates.append(
                date
            )


    if not complete_dates:

        return {
            "status": "INCOMPLETE"
        }


    # Latest complete trading day.
    signal_date = max(
        complete_dates
    )


    m = by_date[
        signal_date
    ]


    # =========================================================================
    # 3-MIN AGGREGATION
    # =========================================================================

    def aggregate_3m(
        minutes
    ):

        candles = [

            m[minute]

            for minute in minutes

        ]


        return {

            "open":
                float(
                    candles[0]["open"]
                ),

            "close":
                float(
                    candles[-1]["close"]
                ),

            "volume":
                sum(

                    float(
                        candle["volume"]
                    )

                    for candle in candles

                ),

        }


    # ---------------------------------------------------------------------
    # AFTERNOON
    # ---------------------------------------------------------------------

    candle_1524 = aggregate_3m([

        1524,
        1525,
        1526,

    ])


    candle_1527 = aggregate_3m([

        1527,
        1528,
        1529,

    ])


    # =========================================================================
    # DIRECTIONS
    # =========================================================================

    d1524 = candle_direction(

        candle_1524["open"],
        candle_1524["close"]

    )


    d1527 = candle_direction(

        candle_1527["open"],
        candle_1527["close"]

    )


    d1528 = candle_direction(

        float(
            m[1528]["open"]
        ),

        float(
            m[1528]["close"]
        )

    )


    d1529 = candle_direction(

        float(
            m[1529]["open"]
        ),

        float(
            m[1529]["close"]
        )

    )


    # =========================================================================
    # VOLUMES
    # =========================================================================

    v1524 = float(
        candle_1524["volume"]
    )


    v1527 = float(
        candle_1527["volume"]
    )


    v1528 = float(
        m[1528]["volume"]
    )


    v1529 = float(
        m[1529]["volume"]
    )


    # =========================================================================
    # CONDITION 1
    # 3m 15:24 volume > 3m 15:27 volume.
    # 15:24 / 15:27 trend relationship is ignored.
    # =========================================================================
    cond1 = (
        d1524 != 0
        and v1524 > v1527
    )


    # =========================================================================
    # CONDITION 2
    # 1m 15:28 and 15:29 must be opposite trends.
    # 15:28 volume > 15:29 volume.
    # =========================================================================
    cond2 = (
        d1528 != 0
        and d1529 != 0
        and d1528 != d1529
        and v1528 > v1529
    )


    # =========================================================================
    # CONDITION 3
    # 1m 15:28 trend = 3m 15:24 trend.
    # =========================================================================
    cond3 = (
        d1528 != 0
        and d1524 != 0
        and d1528 == d1524
    )


    passed = (
        cond1
        and cond2
        and cond3
    )


    # =========================================================================
    # FINAL DIRECTION
    #
    # FINAL DIRECTION = 1-MIN 15:28
    # =========================================================================

    if d1528 == 1:

        direction = "LONG"

    elif d1528 == -1:

        direction = "SHORT"

    else:

        direction = None


    return {

        "status":
            "PASS"
            if passed
            else
            "FAIL",

        "date":
            signal_date,

        "direction":
            direction
            if passed
            else
            None,

        "raw_direction":
            direction,

        "cond1":
            cond1,

        "cond2":
            cond2,

        "cond3":
            cond3,

        "unused_old_condition":
            unused_old_condition,

        "details": {

            "d1524":
                d1524,

            "d1527":
                d1527,

            "d1528":
                d1528,

            "d1529":
                d1529,

            "15:24_vol":
                v1524,

            "15:27_vol":
                v1527,

            "15:28_vol":
                v1528,

            "15:29_vol":
                v1529,

        },

    }


# =============================================================================
# SCAN ONE SYMBOL
# =============================================================================

def scan_one_symbol(
    symbol
):

    try:

        rows, data_status = (
            fetch_symbol_rows(
                symbol
            )
        )


        if rows is None:

            return {

                "symbol":
                    symbol,

                "status":
                    "NO_DATA",

                "data_status":
                    data_status,

            }


        result = evaluate_rows(
            rows
        )


        result["symbol"] = symbol

        result["data_status"] = (
            data_status
        )


        return result


    except Exception as e:

        return {

            "symbol":
                symbol,

            "status":
                "ERROR",

            "data_status":
                "ERROR",

            "error":
                str(e)[:240],

        }


# =============================================================================
# PARALLEL SCAN
# =============================================================================

def scan_all_symbols(
    symbols
):

    total = len(
        symbols
    )

    results = []

    completed = 0

    start = time.time()


    print()
    print("=" * 70)
    print(
        f"Scanning {total} symbols "
        f"with {MAX_WORKERS} workers"
    )
    print("=" * 70)
    print()


    with ThreadPoolExecutor(

        max_workers=MAX_WORKERS

    ) as executor:


        futures = {

            executor.submit(
                scan_one_symbol,
                symbol
            ):
                symbol

            for symbol in symbols

        }


        for future in as_completed(
            futures
        ):

            symbol = futures[
                future
            ]


            try:

                result = (
                    future.result()
                )


            except Exception as e:

                result = {

                    "symbol":
                        symbol,

                    "status":
                        "ERROR",

                    "error":
                        str(e)[:240],

                }


            results.append(
                result
            )


            completed += 1


            # Only print every 10 symbols,
            # plus matches.
            if result.get(
                "status"
            ) == "PASS":

                print(

                    f"[{completed}/{total}] "
                    f"{symbol:<15} "
                    f"MATCH "
                    f"{result.get('direction')}"

                )

            elif (

                completed % 10 == 0

                or

                completed == total

            ):

                print(

                    f"Progress: "
                    f"{completed}/{total}"

                )


    results.sort(

        key=lambda x:
            x.get(
                "symbol",
                ""
            )

    )


    elapsed = (
        time.time()
        -
        start
    )


    return (
        results,
        elapsed
    )


# =============================================================================
# HTML BADGE
# =============================================================================

def badge(value):

    if value is True:

        return (

            '<span class="badge pass">'
            'PASS'
            '</span>'

        )


    if value is False:

        return (

            '<span class="badge fail">'
            'FAIL'
            '</span>'

        )


    return "â€”"


# =============================================================================
# HTML ESCAPE
# =============================================================================

def esc(value):

    return html.escape(
        str(value)
    )


# =============================================================================
# GENERATE HTML
# =============================================================================

def generate_html_report(

    results,
    elapsed,
    universe_source

):

    matches = [

        r

        for r in results

        if r.get(
            "status"
        ) == "PASS"

    ]


    fails = [

        r

        for r in results

        if r.get(
            "status"
        ) == "FAIL"

    ]


    incomplete = [

        r

        for r in results

        if r.get(
            "status"
        ) == "INCOMPLETE"

    ]


    no_data = [

        r

        for r in results

        if r.get(
            "status"
        ) == "NO_DATA"

    ]


    errors = [

        r

        for r in results

        if r.get(
            "status"
        ) == "ERROR"

    ]


    # =========================================================================
    # MATCHES
    # =========================================================================

    if matches:

        match_html = "".join(

            f"""
            <div class="match">

                <span class="direction
                {'long'
                 if r['direction'] == 'LONG'
                 else 'short'}">

                    {esc(r['direction'])}

                </span>

                <b>
                    {esc(r['symbol'])}
                </b>

                <span class="date">

                    Signal day:
                    {esc(r.get('date', 'â€”'))}

                </span>

            </div>
            """

            for r in matches

        )

    else:

        match_html = (

            '<div class="none">'
            'No stocks matched today.'
            '</div>'

        )


    # =========================================================================
    # TABLE
    # =========================================================================

    table_rows = []


    sorted_results = sorted(

        results,

        key=lambda x: (

            x.get(
                "status"
            ) != "PASS",

            x.get(
                "symbol",
                ""
            )

        )

    )


    for r in sorted_results:

        details = r.get(
            "details",
            {}
        )


        status = r.get(
            "status",
            "UNKNOWN"
        )


        if status == "PASS":

            status_class = "pass"

        elif status == "FAIL":

            status_class = "fail"

        else:

            status_class = "skip"


        table_rows.append(

            f"""
            <tr>

                <td class="symbol">
                    {esc(
                        r.get(
                            'symbol',
                            ''
                        )
                    )}
                </td>

                <td>
                    {esc(
                        r.get(
                            'date',
                            'â€”'
                        )
                    )}
                </td>

                <td>
                    {esc(
                        r.get(
                            'direction'
                        )
                        or
                        'â€”'
                    )}
                </td>

                <td>

                    {badge(
                        r.get(
                            'cond1'
                        )
                    )}

                    <br>

                    <small>

                    15:24:
                    {details.get(
                        '15:24_vol',
                        0
                    ):,.0f}

                    &gt;

                    15:27:
                    {details.get(
                        '15:27_vol',
                        0
                    ):,.0f}

                    </small>

                </td>

                <td>

                    {badge(
                        r.get(
                            'cond2'
                        )
                    )}

                    <br>

                    <small>

                    15:28:
                    {details.get(
                        '15:28_vol',
                        0
                    ):,.0f}

                    &gt;

                    15:29:
                    {details.get(
                        '15:29_vol',
                        0
                    ):,.0f}

                    </small>

                </td>

                <td>

                    {badge(
                        r.get(
                            'cond3'
                        )
                    )}

                    <br>

                    <small>
                    15:28 â†” 3M 15:24
                    </small>

                </td>

                <td class="{status_class}">

                    {esc(status)}

                </td>

                <td>

                    {esc(
                        r.get(
                            'data_status',
                            'â€”'
                        )
                    )}

                </td>

            </tr>
            """

        )


    # =========================================================================
    # HTML DOCUMENT
    # =========================================================================

    scan_time = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


    html_document = f"""
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<title>
NSE Daily Momentum Scanner
</title>

<style>

body {{

    font-family:
        Arial,
        sans-serif;

    background:
        #f5f5f5;

    color:
        #222;

    margin:
        0;

    padding:
        25px;

}}

.container {{

    max-width:
        1500px;

    margin:
        auto;

}}

h1 {{

    margin-bottom:
        5px;

}}

.subtitle {{

    color:
        #777;

    line-height:
        1.6;

}}

.stats {{

    display:
        flex;

    flex-wrap:
        wrap;

    gap:
        10px;

    margin:
        20px 0;

}}

.stat {{

    background:
        white;

    padding:
        15px 20px;

    border-radius:
        8px;

    border:
        1px solid #ddd;

}}

.stat b {{

    font-size:
        22px;

    display:
        block;

}}

.match-box {{

    background:
        white;

    border:
        1px solid #ddd;

    border-radius:
        8px;

    padding:
        20px;

    margin-bottom:
        20px;

}}

.match {{

    padding:
        9px 0;

    border-bottom:
        1px solid #eee;

}}

.direction {{

    display:
        inline-block;

    padding:
        3px 7px;

    border-radius:
        4px;

    font-size:
        11px;

    font-weight:
        bold;

    margin-right:
        8px;

}}

.long {{

    background:
        #dff5e5;

    color:
        #08752f;

}}

.short {{

    background:
        #f8dddd;

    color:
        #a52222;

}}

.date {{

    color:
        #777;

    margin-left:
        8px;

}}

table {{

    width:
        100%;

    border-collapse:
        collapse;

    background:
        white;

    font-size:
        13px;

}}

th {{

    background:
        #ededed;

    padding:
        10px;

    text-align:
        left;

    position:
        sticky;

    top:
        0;

}}

td {{

    padding:
        9px 10px;

    border-top:
        1px solid #eee;

}}

.symbol {{

    font-weight:
        bold;

}}

small {{

    color:
        #777;

    font-size:
        10px;

}}

.badge {{

    display:
        inline-block;

    padding:
        2px 6px;

    border-radius:
        4px;

    font-size:
        10px;

    font-weight:
        bold;

}}

.badge.pass {{

    background:
        #dff5e5;

    color:
        #08752f;

}}

.badge.fail {{

    background:
        #f8dddd;

    color:
        #a52222;

}}

.pass {{

    color:
        #08752f;

    font-weight:
        bold;

}}

.fail {{

    color:
        #999;

}}

.skip {{

    color:
        #b07000;

}}

.none {{

    color:
        #777;

}}

.footer {{

    margin-top:
        25px;

    color:
        #777;

    font-size:
        12px;

    line-height:
        1.7;

}}

</style>

</head>


<body>

<div class="container">


<h1>
NSE Daily Momentum Scanner
</h1>


<div class="subtitle">

Generated:
{esc(scan_time)}

<br>

Universe:
{esc(universe_source)}

<br>

Yahoo Finance 1-minute data

</div>


<div class="stats">


<div class="stat">

<b>
{len(results)}
</b>

Stocks scanned

</div>


<div class="stat">

<b>
{len(matches)}
</b>

Matches

</div>


<div class="stat">

<b>
{len(fails)}
</b>

No match

</div>


<div class="stat">

<b>
{len(incomplete)}
</b>

Incomplete

</div>


<div class="stat">

<b>
{len(no_data)}
</b>

No data

</div>


<div class="stat">

<b>
{len(errors)}
</b>

Errors

</div>


<div class="stat">

<b>
{elapsed:.1f}s
</b>

Scan time

</div>


</div>


<div class="match-box">


<h2>
Matches
</h2>


{match_html}


</div>


<table>


<thead>


<tr>

<th>
Symbol
</th>

<th>
Signal Day
</th>

<th>
Direction
</th>

<th>
3M 15:24 / 15:27
</th>

<th>
1M 15:28 / 15:29
</th>

<th>
1M 15:28 / 15:29
</th>

<th>
1M / 3M
</th>

<th>
Result
</th>

<th>
Data
</th>

</tr>


</thead>


<tbody>

{''.join(table_rows)}

</tbody>


</table>


<div class="footer">

<b>
CURRENT STRATEGY
</b>

<br>

1. 3-min 15:24 volume must be greater than
15:27 volume.

<br>

2. 3-min 15:24 and 15:27 trend relationship
is ignored. They can be the same or opposite.

<br>

3. 1-min 15:28 and 15:29 must be opposite trends.

<br>

4. 1-min 15:28 volume must be greater than
15:29 volume.

<br>

5. 1-min 15:28 trend must match
3-min 15:24 trend.

<br>

6. Final direction = 1-min 15:28.

<br>

<b>
Entry:
</b>

Next trading day 09:15 open.

<br>

<b>
Exit:
</b>

15:27.

</div>


</div>

</body>

</html>
"""


    with open(

        "index.html",

        "w",

        encoding="utf-8"

    ) as file:

        file.write(
            html_document
        )


# =============================================================================
# MAIN
# =============================================================================

def main():

    print()

    print("=" * 70)

    print(
        "NSE EOD MOMENTUM SCANNER"
    )

    print("=" * 70)

    print()


    print(
        "yfinance version:",
        getattr(
            yf,
            "__version__",
            "unknown"
        )
    )


    # ---------------------------------------------------------------------
    # LOAD UNIVERSE
    # ---------------------------------------------------------------------

    universe, source = (
        get_stock_universe()
    )


    print()

    print("=" * 70)

    print(
        f"FINAL STOCK UNIVERSE: "
        f"{len(universe)}"
    )

    print(
        f"SOURCE: {source}"
    )

    print("=" * 70)


    # ---------------------------------------------------------------------
    # SCAN
    # ---------------------------------------------------------------------

    results, elapsed = (
        scan_all_symbols(
            universe
        )
    )


    # ---------------------------------------------------------------------
    # MATCHES
    # ---------------------------------------------------------------------

    matches = [

        r

        for r in results

        if r.get(
            "status"
        ) == "PASS"

    ]


    # ---------------------------------------------------------------------
    # FINAL CONSOLE OUTPUT
    # ---------------------------------------------------------------------

    print()

    print("=" * 70)

    print(
        "SCAN COMPLETE"
    )

    print("=" * 70)

    print(
        f"Universe   : "
        f"{len(universe)}"
    )

    print(
        f"Matches    : "
        f"{len(matches)}"
    )

    print(
        f"Time       : "
        f"{elapsed:.1f} seconds"
    )

    print()


    if matches:

        print(
            "MATCHES:"
        )

        for result in matches:

            print(

                f"  "
                f"{result['symbol']:<15}"
                f"{result['direction']:<7}"
                f"{result['date']}"

            )

    else:

        print(
            "NO MATCHES."
        )


    print()


    print(

        "PASS       :",

        sum(

            r.get(
                "status"
            ) == "PASS"

            for r in results

        )

    )


    print(

        "FAIL       :",

        sum(

            r.get(
                "status"
            ) == "FAIL"

            for r in results

        )

    )


    print(

        "INCOMPLETE :",

        sum(

            r.get(
                "status"
            ) == "INCOMPLETE"

            for r in results

        )

    )


    print(

        "NO DATA    :",

        sum(

            r.get(
                "status"
            ) == "NO_DATA"

            for r in results

        )

    )


    print(

        "ERROR      :",

        sum(

            r.get(
                "status"
            ) == "ERROR"

            for r in results

        )

    )


    # ---------------------------------------------------------------------
    # HTML
    # ---------------------------------------------------------------------

    generate_html_report(

        results,

        elapsed,

        source

    )


    print()

    print(
        "HTML report: index.html"
    )

    print()

    print("=" * 70)


# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":

    main()
