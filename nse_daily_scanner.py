# =============================================================================
# NSE DAILY MOMENTUM SCANNER - FIXED VERSION
# =============================================================================
#
# Uses Yahoo Finance 1-minute data. No Parquet, no nselib.
# Universe: Nifty 500 CSV -> NSE equity list -> built-in fallback.
# Output: index.html
#
# =============================================================================
# STRATEGY
# =============================================================================
#
# "Previous day" = the latest trading day present in the data.
# "Day before previous day" (signal day) = the day right before that - every
#   condition below is evaluated on THIS day, at the 3-minute timeframe.
# "Next day" = the day the trade is entered (the session after "previous
#   day").
#
# 1) 15:24 volume > 15:27 volume, AND at least 2 of the four candles at
#    15:15, 15:18, 15:21 and 15:27 share the 15:24 candle's trend.
#
# 2) The 9:15 candle's trend matches the 15:24 candle's trend.
#
# FINAL DIRECTION: 15:24 candle's trend (up = LONG, down = SHORT).
#
# TRADE: Entry = next trading session's 09:15 OPEN, Exit = that session's
#        15:27.
#
# Yahoo has no native 3-minute interval, so every 3-minute candle here is
# built from three 1-minute bars (Open = open of the earliest sub-minute
# with data, Close = close of the latest, Volume = sum). A missing
# sub-minute is treated as "no trade" (0 volume, no trend); when that
# makes a 3-min candle less than fully complete, it's flagged for manual
# verification rather than silently trusted (see "partial_3m" / "Verify"
# tags).
#
# INSTALL:
#   python -m pip install yfinance pandas requests curl_cffi
#
# =============================================================================

import html
import logging
import math
import random
import sys
import time
import warnings

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

import pandas as pd
import requests

warnings.filterwarnings("ignore")

try:
    import yfinance as yf
except ImportError:
    print()
    print("ERROR: yfinance is not installed.")
    print("Install with: pip install yfinance pandas requests curl_cffi")
    print()
    sys.exit(1)

# yfinance logs every failed ticker; we report those ourselves.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Yahoo throttles heavy parallel use. If you see many rate-limit ERRORs,
# lower this to 4-6.
MAX_WORKERS = 8

# Attempts per request (only rate limits / network errors are retried).
MAX_RETRIES = 3

INITIAL_PERIOD = "5d"

# Yahoo keeps roughly 7-8 days of 1-minute data in total, so this is close
# to the practical maximum.
FALLBACK_PERIOD = "7d"

REQUEST_TIMEOUT = 20

# If fewer than this share of symbols have all 18 required one-minute bars
# on the signal day, the report shows a "data looks incomplete" warning.
# Kept low because illiquid stocks often have a few no-trade minutes; when
# Yahoo's data is genuinely broken the share is close to 0%.
SESSION_COMPLETE_SHARE = 0.20


# =============================================================================
# UNIVERSE SIZE  <-- change this to scan more / fewer stocks
# =============================================================================
#
#   "NIFTY500"      ~500 stocks   (large + mid + small caps)
#   "TOTAL_MARKET"  ~750 stocks   (Nifty 500 + Nifty Microcap 250)  [default]
#   "ALL_NSE"       ~1800+ stocks (every EQ-series stock on NSE)
#
# If the chosen list cannot be downloaded, the scanner falls back to the next
# smaller list automatically (see get_stock_universe).
#
UNIVERSE_MODE = "TOTAL_MARKET"


# =============================================================================
# OFFICIAL UNIVERSE SOURCES
# =============================================================================

# Each index is tried on niftyindices.com first, then on NSE's archive server
# (niftyindices.com is sometimes blocked from cloud runners such as GitHub
# Actions).
INDEX_CSV_URLS = {
    "Nifty 500": [
        "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
        "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    ],
    "Nifty Microcap 250": [
        "https://www.niftyindices.com/IndexConstituent/ind_niftymicrocap250_list.csv",
        "https://nsearchives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv",
    ],
    "Nifty Total Market": [
        "https://www.niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv",
        "https://nsearchives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv",
    ],
}

# Reject obviously bad / partial downloads.
INDEX_MIN_SYMBOLS = {
    "Nifty 500": 450,
    "Nifty Microcap 250": 200,
    "Nifty Total Market": 650,
}

# NSE official equity security list (all EQ-series stocks).
NSE_EQUITY_URL = (
    "https://nsearchives.nseindia.com/"
    "content/equities/sec_list.csv"
)


# =============================================================================
# REQUIRED 1-MINUTE CANDLES
# =============================================================================

# The six 3-minute candles the strategy needs, each built from three
# 1-minute bars. All are read from "day before previous day" (signal day).
CANDLE_GROUPS_3M = {
    "0915": [915, 916, 917],
    "1515": [1515, 1516, 1517],
    "1518": [1518, 1519, 1520],
    "1521": [1521, 1522, 1523],
    "1524": [1524, 1525, 1526],
    "1527": [1527, 1528, 1529],
}

NEEDED_HM = {hm for group in CANDLE_GROUPS_3M.values() for hm in group}


# =============================================================================
# BUILT-IN FALLBACK UNIVERSE
# =============================================================================

NIFTY_50 = [
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "HINDUNILVR", "ITC",
    "SBIN", "BHARTIARTL", "KOTAKBANK", "LT", "AXISBANK", "BAJFINANCE",
    "ASIANPAINT", "MARUTI", "HCLTECH", "SUNPHARMA", "TITAN", "ULTRACEMCO",
    "NESTLEIND", "WIPRO", "ADANIENT", "ONGC", "NTPC", "POWERGRID", "M&M",
    "JSWSTEEL", "TATASTEEL", "TATAMOTORS", "COALINDIA", "BAJAJFINSV",
    "TECHM", "INDUSINDBK", "HDFCLIFE", "SBILIFE", "GRASIM", "DRREDDY",
    "DIVISLAB", "EICHERMOT", "BRITANNIA", "CIPLA", "APOLLOHOSP",
    "HEROMOTOCO", "BPCL", "TATACONSUM", "ADANIPORTS", "HINDALCO",
    "BAJAJ-AUTO", "SHRIRAMFIN", "LTIM", "UPL",
]

NIFTY_NEXT_150 = [
    "ABB", "ADANIENSOL", "ADANIGREEN", "ADANIPOWER", "AMBUJACEM", "DMART",
    "BANKBARODA", "BERGEPAINT", "BEL", "BOSCHLTD", "CANBK", "CHOLAFIN",
    "COLPAL", "DABUR", "DLF", "GAIL", "GODREJCP", "HAVELLS", "HAL",
    "ICICIGI", "ICICIPRULI", "IOC", "IRCTC", "IRFC", "JINDALSTEL", "JIOFIN",
    "LICI", "LODHA", "LUPIN", "MARICO", "MOTHERSON", "MRF", "NAUKRI", "NHPC",
    "PIDILITIND", "PFC", "PNB", "RECLTD", "SIEMENS", "SRF", "TATAPOWER",
    "TORNTPHARM", "TVSMOTOR", "UNIONBANK", "VBL", "VEDL", "ZOMATO",
    "ZYDUSLIFE", "PAYTM", "POLICYBZR", "PERSISTENT", "COFORGE", "MPHASIS",
    "OBEROIRLTY", "PIIND", "ASHOKLEY", "AUROPHARMA", "BANDHANBNK",
    "BATAINDIA", "BHARATFORG", "BHEL", "CGPOWER", "CONCOR", "CUMMINSIND",
    "DEEPAKNTR", "DIXON", "ESCORTS", "EXIDEIND", "FEDERALBNK", "GLAND",
    "GMRAIRPORT", "GODREJPROP", "GUJGASLTD", "HDFCAMC", "HINDPETRO", "IDEA",
    "IDFCFIRSTB", "IGL", "INDHOTEL", "INDIGO", "INDUSTOWER", "IPCALAB",
    "JSWENERGY", "JUBLFOOD", "KALYANKJIL", "L&TFH", "LALPATHLAB",
    "LAURUSLABS", "LTTS", "M&MFIN", "MANKIND", "MAXHEALTH", "METROPOLIS",
    "MFSL", "MUTHOOTFIN", "NATIONALUM", "NAVINFLUOR", "NMDC", "OFSS",
    "PAGEIND", "PATANJALI", "PETRONET", "PHOENIXLTD", "POLYCAB", "PRESTIGE",
    "RAMCOCEM", "RVNL", "SAIL", "SBICARD", "SCHAEFFLER", "SHREECEM", "SJVN",
    "SOLARINDS", "SONACOMS", "STARHEALTH", "SUNDARMFIN", "SUPREMEIND",
    "SUZLON", "SYNGENE", "TATACHEM", "TATACOMM", "TATAELXSI", "THERMAX",
    "TIINDIA", "TORNTPOWER", "TRENT", "TRIDENT", "UBL", "UCOBANK", "VOLTAS",
    "WHIRLPOOL", "YESBANK", "ZEEL", "ABCAPITAL", "ABFRL", "ALKEM",
    "APLAPOLLO", "APOLLOTYRE", "ASTRAL", "AUBANK", "BALKRISIND",
    "BANKINDIA", "BSOFT", "CANFINHOME", "CENTRALBK", "CROMPTON", "CYIENT",
    "DALBHARAT", "DELHIVERY", "DEVYANI", "EMAMILTD", "GICRE", "GLENMARK",
    "GNFC", "GODIGIT", "GRANULES", "GRSE", "HFCL", "HONAUT",
]

FALLBACK_UNIVERSE = list(dict.fromkeys(NIFTY_50 + NIFTY_NEXT_150))

# Old NSE symbols that are now listed under a different Yahoo ticker.
# Only matters for the built-in fallback list; the official CSVs already use
# current symbols. Edit if Yahoo lists any of these differently.
SYMBOL_ALIASES = {
    "ZOMATO": "ETERNAL",
    "L&TFH": "LTF",
    "TATAMOTORS": "TMPV",
}


def yahoo_ticker(symbol):
    return SYMBOL_ALIASES.get(symbol, symbol) + ".NS"


# =============================================================================
# HTTP HEADERS
# =============================================================================

NSE_HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0 Safari/537.36",
    "Accept":
        "text/csv,text/html,application/xhtml+xml,"
        "application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.niftyindices.com/",
}


# =============================================================================
# UNIVERSE LOADING
# =============================================================================

def normalize_symbols(values):

    symbols = []

    for value in values:
        symbol = str(value).strip().upper()
        if not symbol or symbol == "NAN":
            continue
        symbols.append(symbol)

    return list(dict.fromkeys(symbols))


def load_index_csv(name):
    """Download one NSE index constituent list. Returns symbols or None."""

    minimum = INDEX_MIN_SYMBOLS[name]

    print()
    print(f"Attempting to load {name} universe...")

    for url in INDEX_CSV_URLS[name]:

        try:
            host = url.split("/")[2]

            session = requests.Session()
            session.headers.update({
                **NSE_HEADERS,
                "Referer": f"https://{host}/",
            })

            try:
                session.get(f"https://{host}/", timeout=15)
            except Exception:
                pass

            response = session.get(url, timeout=20)
            response.raise_for_status()

            df = pd.read_csv(StringIO(response.text))

            symbol_column = None
            for column in df.columns:
                if str(column).strip().lower() == "symbol":
                    symbol_column = column
                    break

            if symbol_column is None:
                raise ValueError("Symbol column not found")

            symbols = normalize_symbols(df[symbol_column].dropna().tolist())

            if len(symbols) < minimum:
                raise ValueError(f"Only {len(symbols)} symbols returned")

            print(f"{name} loaded successfully: {len(symbols)} symbols")
            return symbols

        except Exception as e:
            print(f"{name} loading failed ({url.split('/')[2]}): {e}")

    return None


def load_nifty500():
    return load_index_csv("Nifty 500")


def load_total_market():
    """~750 stocks = Nifty 500 + Nifty Microcap 250."""

    # 1) The official Total Market list, if available.
    symbols = load_index_csv("Nifty Total Market")
    if symbols:
        return symbols

    # 2) Build it ourselves (Total Market is defined as exactly this union).
    base = load_index_csv("Nifty 500")
    micro = load_index_csv("Nifty Microcap 250")

    if base and micro:
        combined = list(dict.fromkeys(base + micro))
        print(
            f"Total Market built from Nifty 500 + Microcap 250: "
            f"{len(combined)} symbols"
        )
        return combined

    print("Could not build the Total Market universe.")
    return None


def load_nse_equity_list():

    try:
        print()
        print("Attempting to load NSE official equity list...")

        session = requests.Session()
        session.headers.update({
            **NSE_HEADERS,
            "Referer": "https://www.nseindia.com/",
        })

        try:
            session.get("https://www.nseindia.com/", timeout=15)
        except Exception:
            pass

        response = session.get(NSE_EQUITY_URL, timeout=20)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))

        symbol_column = next(
            (c for c in df.columns if "symbol" in str(c).lower()), None
        )
        series_column = next(
            (c for c in df.columns if "series" in str(c).lower()), None
        )

        if symbol_column is None:
            raise ValueError("NSE Symbol column not found")

        if series_column is not None:
            df = df[
                df[series_column].astype(str).str.strip().str.upper() == "EQ"
            ]

        symbols = normalize_symbols(df[symbol_column].dropna().tolist())

        if len(symbols) < 300:
            raise ValueError(f"Only {len(symbols)} EQ symbols returned")

        print(f"NSE official equity list loaded: {len(symbols)} symbols")
        return symbols

    except Exception as e:
        print(f"NSE official list unavailable: {e}")
        return None


def get_stock_universe():

    print()
    print("=" * 70)
    print("LOADING NSE STOCK UNIVERSE")
    print(f"Requested mode: {UNIVERSE_MODE}")
    print("=" * 70)

    # Tried in order; the first list that loads is used.
    attempts = []

    if UNIVERSE_MODE == "ALL_NSE":
        attempts.append(("NSE official equity list (all EQ)", load_nse_equity_list))

    if UNIVERSE_MODE in ("ALL_NSE", "TOTAL_MARKET"):
        attempts.append(("Nifty Total Market (Nifty 500 + Microcap 250)", load_total_market))

    attempts.append(("Nifty 500 official CSV", load_nifty500))
    attempts.append(("NSE official equity list", load_nse_equity_list))

    for label, loader in attempts:

        symbols = loader()

        if symbols:
            return symbols, label

    print()
    print(f"Using built-in fallback universe: {len(FALLBACK_UNIVERSE)} symbols")

    return FALLBACK_UNIVERSE, "Built-in fallback"


# =============================================================================
# CLEAN YAHOO DATA
# =============================================================================

def clean_yahoo_data(df):

    if df is None or df.empty:
        return None

    df = df.copy()

    # Flatten MultiIndex columns if present.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [column[0] for column in df.columns]

    df = df.reset_index()

    timestamp_column = None
    for candidate in ("Datetime", "Date", "datetime", "date", "index"):
        if candidate in df.columns:
            timestamp_column = candidate
            break

    if timestamp_column is None:
        return None

    ts = pd.to_datetime(df[timestamp_column], errors="coerce", utc=False)

    valid = ts.notna()
    if not valid.any():
        return None

    df = df.loc[valid].copy()
    ts = ts.loc[valid]

    # Convert to India time.
    if ts.dt.tz is not None:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    else:
        ts = ts.dt.tz_localize("Asia/Kolkata")

    if not all(c in df.columns for c in ("Open", "Close", "Volume")):
        return None

    open_ = pd.to_numeric(df["Open"], errors="coerce")
    close_ = pd.to_numeric(df["Close"], errors="coerce")
    # High/Low power the candlestick visuals; fall back to open/close for
    # any feed that happens to omit them so nothing downstream breaks.
    high_ = pd.to_numeric(df["High"], errors="coerce") if "High" in df.columns else None
    low_ = pd.to_numeric(df["Low"], errors="coerce") if "Low" in df.columns else None

    out = pd.DataFrame({
        "date": ts.dt.strftime("%Y-%m-%d").values,
        "hm": (ts.dt.hour * 100 + ts.dt.minute).values,
        "open": open_.values,
        "high": (high_ if high_ is not None else pd.concat([open_, close_], axis=1).max(axis=1)).values,
        "low": (low_ if low_ is not None else pd.concat([open_, close_], axis=1).min(axis=1)).values,
        "close": close_.values,
        "volume": pd.to_numeric(df["Volume"], errors="coerce").values,
    })

    # Regular NSE session only.
    out = out[out["hm"].between(915, 1529)]

    out = out.dropna(subset=["open", "high", "low", "close", "volume"])

    out = out.drop_duplicates(subset=["date", "hm"], keep="last")

    if out.empty:
        return None

    return out


# =============================================================================
# YAHOO DOWNLOAD
# =============================================================================
#
# yf.Ticker(...).history() is used instead of yf.download(): download() keeps
# results in module-level globals and is NOT safe to call from several threads
# at once.
#

RATE_LIMIT_WORDS = ("rate limit", "rate-limit", "too many requests", "429")

DEAD_TICKER_WORDS = (
    "delisted", "no data found", "no price data", "not found", "404",
)


def yahoo_download(symbol, period):
    """Returns (dataframe, None) on success, or (None, reason).
    reason == "NO_DATA" means Yahoo has nothing for this ticker;
    any other string is a real error message."""

    ticker = yahoo_ticker(symbol)
    last_error = "NO_DATA"

    for attempt in range(MAX_RETRIES):

        try:
            df = yf.Ticker(ticker).history(
                period=period,
                interval="1m",
                auto_adjust=False,
                actions=False,
                prepost=False,
                timeout=REQUEST_TIMEOUT,
                raise_errors=True,
            )

            cleaned = clean_yahoo_data(df)

            if cleaned is not None and not cleaned.empty:
                return cleaned, None

            return None, "NO_DATA"

        except Exception as e:

            message = str(e).replace("\n", " ")
            lowered = message.lower()

            rate_limited = any(w in lowered for w in RATE_LIMIT_WORDS)
            dead = any(w in lowered for w in DEAD_TICKER_WORDS)

            # Invalid / delisted ticker: do not retry.
            if dead and not rate_limited:
                return None, "NO_DATA"

            last_error = message[:240]

            if attempt < MAX_RETRIES - 1:
                base = 4.0 if rate_limited else 1.0
                time.sleep(base * (2 ** attempt) + random.uniform(0.2, 1.0))

    return None, last_error


def fetch_symbol_rows(symbol):
    """Returns (rows, info). If rows is None, info is "NO_DATA" or an
    error message. Otherwise info is "OK" or "FALLBACK"."""

    # Small random delay so the workers do not hit Yahoo in one burst.
    time.sleep(random.uniform(0.05, 0.25))

    rows, error = yahoo_download(symbol, INITIAL_PERIOD)

    if rows is not None:
        return rows, "OK"

    # Only one extra request, and only if Yahoo simply returned nothing.
    # (No point retrying with a longer period after a rate-limit error.)
    if error == "NO_DATA":
        rows2, error2 = yahoo_download(symbol, FALLBACK_PERIOD)
        if rows2 is not None:
            return rows2, "FALLBACK"
        return None, error2

    return None, error


# =============================================================================
# STRATEGY HELPERS
# =============================================================================

def candle_direction(open_price, close_price):

    if open_price is None or close_price is None:
        return 0

    if close_price > open_price:
        return 1

    if close_price < open_price:
        return -1

    return 0


def aggregate_3m(m, minutes):
    """3-minute candle built from whichever of the 3 one-minute candles
    exist. Yahoo has no native 3-minute interval, so this combines three
    1-minute bars: Open = open of the earliest sub-minute that has data,
    Close = close of the latest sub-minute that has data, High/Low = the
    max/min across whichever sub-minutes are present, Volume = sum of the
    sub-minutes' volumes. This is the correct construction as long as a
    missing sub-minute really means "no trades" rather than "Yahoo
    dropped a real bar" - the code cannot tell those two apart, which is
    exactly what a manual cross-check (e.g. on TradingView) is good for.
    m[minute] is a tuple (open, high, low, close, volume).
    """

    present = [minute for minute in minutes if minute in m]
    missing = [minute for minute in minutes if minute not in m]

    if not present:
        return {
            "open": None, "high": None, "low": None, "close": None,
            "volume": 0,
            "minutes_used": [],
            "minutes_missing": missing,
            "partial": True,
        }

    candles = [m[minute] for minute in present]

    return {
        "open": candles[0][0],
        "high": max(c[1] for c in candles),
        "low": min(c[2] for c in candles),
        "close": candles[-1][3],
        "volume": sum(int(round(c[4])) for c in candles),
        "minutes_used": present,
        "minutes_missing": missing,
        # True whenever this 3-min candle was not built from all 3
        # sub-minutes - the ambiguous case worth double-checking manually.
        "partial": len(present) < len(minutes),
    }


# =============================================================================
# EVALUATE STRATEGY
# =============================================================================

def evaluate_rows(rows):
    """Strategy (all on "day before previous day" - two trading days before
    entry - at the 3-minute timeframe):

    1) 15:24 volume > 15:27 volume, AND at least 2 of {15:15, 15:18, 15:21,
       15:27} share 15:24's trend.
    2) The 9:15 candle's trend matches 15:24's trend.

    Direction = LONG if 15:24 trended up, SHORT if down. Entry = next
    trading session's 09:15 open. Exit = that same session's 15:27.
    """

    if rows is None or rows.empty:
        return {"status": "NO_DATA"}

    # ---------------------------------------------------------------------
    # DATE -> {minute: (open, high, low, close, volume)}
    # ---------------------------------------------------------------------

    by_date = {}

    for date, hm, o, h, l, c, v in zip(
        rows["date"], rows["hm"], rows["open"], rows["high"], rows["low"],
        rows["close"], rows["volume"]
    ):
        by_date.setdefault(date, {})[int(hm)] = (
            float(o), float(h), float(l), float(c), float(v)
        )

    # "Today" is fixed to the real calendar date, not inferred from
    # whichever date happens to be latest in the fetched data. That
    # inference broke depending on exactly when the scan was run: if Yahoo
    # had already returned even a few minutes of today's session, today
    # got mistaken for "previous day" and the whole mapping shifted back
    # by a day. Excluding today outright makes the result the same
    # whether the scan runs before the open or mid-session.
    today = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")
    dates = sorted(d for d in by_date if d != today)

    # Need at least 2 distinct trading days strictly before today:
    # "previous day" (yesterday's session) and "day before previous day"
    # (the one the strategy actually reads).
    if len(dates) < 2:
        return {
            "status": "INCOMPLETE",
            "date": dates[-1] if dates else None,
            "previous_day": None,
            "entry_day": today,
            "missing": sorted(NEEDED_HM),
            "note": "fewer than 2 trading days of data available before today",
        }

    previous_day = dates[-1]
    signal_date = dates[-2]

    m = by_date[signal_date]

    missing = [hm for hm in sorted(NEEDED_HM) if hm not in m]

    if len(missing) == len(NEEDED_HM):
        return {
            "status": "INCOMPLETE",
            "date": signal_date,
            "previous_day": previous_day,
            "entry_day": today,
            "missing": missing,
        }

    # ---------------------------------------------------------------------
    # THE SIX 3-MINUTE CANDLES
    # ---------------------------------------------------------------------

    candles = {
        label: aggregate_3m(m, minutes)
        for label, minutes in CANDLE_GROUPS_3M.items()
    }

    d = {
        label: candle_direction(c["open"], c["close"])
        for label, c in candles.items()
    }

    v1524 = candles["1524"]["volume"]
    v1527 = candles["1527"]["volume"]

    # True whenever any of the six 3-min candles was built from fewer than
    # all 3 of its sub-minutes - i.e. "missing minute" is ambiguous between
    # "no trades" and "Yahoo dropped a bar", so the result is worth a
    # manual check rather than fully trusting as-is.
    partial_groups = sorted(
        label for label, c in candles.items() if c.get("partial")
    )
    partial_3m = len(partial_groups) > 0

    d1524 = d["1524"]

    # CONDITION 1a: 15:24 volume > 15:27 volume
    cond1a = d1524 != 0 and v1524 > v1527

    # CONDITION 1b: at least 2 of {15:15, 15:18, 15:21, 15:27} share
    # 15:24's trend.
    compare_labels = ["1515", "1518", "1521", "1527"]
    matches = sum(1 for label in compare_labels if d1524 != 0 and d[label] == d1524)
    cond1b = matches >= 2

    cond1 = cond1a and cond1b

    # CONDITION 2: 9:15 trend == 15:24 trend.
    cond2 = d1524 != 0 and d["0915"] != 0 and d["0915"] == d1524

    passed = cond1 and cond2

    # FINAL DIRECTION = 15:24 trend
    if d1524 == 1:
        direction = "LONG"
    elif d1524 == -1:
        direction = "SHORT"
    else:
        direction = None

    return {
        "status": "PASS" if passed else "FAIL",
        "date": signal_date,
        "previous_day": previous_day,
        "entry_day": today,
        "direction": direction if passed else None,
        "raw_direction": direction,
        "cond1": cond1,
        "cond1a": cond1a,
        "cond1b": cond1b,
        "cond1b_matches": matches,
        "cond2": cond2,
        "missing": missing,
        "partial_3m": partial_3m,
        "partial_groups": partial_groups,
        "candle_minutes_used": {
            label: c.get("minutes_used", []) for label, c in candles.items()
        },
        # Actual OHLC per 3-min candle, for drawing real candlestick charts
        # in the report (not just up/down arrows).
        "ohlc": {
            label: {
                "open": c["open"], "high": c["high"],
                "low": c["low"], "close": c["close"],
            }
            for label, c in candles.items()
        },
        "details": {
            "d0915": d["0915"],
            "d1515": d["1515"],
            "d1518": d["1518"],
            "d1521": d["1521"],
            "d1524": d["1524"],
            "d1527": d["1527"],
            "0915_vol": candles["0915"]["volume"],
            "1515_vol": candles["1515"]["volume"],
            "1518_vol": candles["1518"]["volume"],
            "1521_vol": candles["1521"]["volume"],
            "1524_vol": v1524,
            "1527_vol": v1527,
        },
    }


# =============================================================================
# SCAN ONE SYMBOL
# =============================================================================

def scan_one_symbol(symbol):

    try:
        rows, info = fetch_symbol_rows(symbol)

        if rows is None:

            if info == "NO_DATA":
                return {
                    "symbol": symbol,
                    "status": "NO_DATA",
                    "data_status": "NO_DATA",
                }

            return {
                "symbol": symbol,
                "status": "ERROR",
                "data_status": "ERROR",
                "error": info,
            }

        result = evaluate_rows(rows)
        result["symbol"] = symbol
        result["data_status"] = info

        return result

    except Exception as e:
        return {
            "symbol": symbol,
            "status": "ERROR",
            "data_status": "ERROR",
            "error": f"{type(e).__name__}: {e}"[:240],
        }


# =============================================================================
# PARALLEL SCAN
# =============================================================================

def scan_all_symbols(symbols):

    total = len(symbols)
    results = []
    completed = 0
    start = time.time()

    print()
    print("=" * 70)
    print(f"Scanning {total} symbols with {MAX_WORKERS} workers")
    print("=" * 70)
    print()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {
            executor.submit(scan_one_symbol, symbol): symbol
            for symbol in symbols
        }

        for future in as_completed(futures):

            symbol = futures[future]

            try:
                result = future.result()
            except Exception as e:
                result = {
                    "symbol": symbol,
                    "status": "ERROR",
                    "error": str(e)[:240],
                }

            results.append(result)
            completed += 1

            if result.get("status") == "PASS":
                print(
                    f"[{completed}/{total}] {symbol:<15} "
                    f"MATCH {result.get('direction')}"
                )
            elif completed % 10 == 0 or completed == total:
                print(f"Progress: {completed}/{total}")

    results.sort(key=lambda x: x.get("symbol", ""))

    return results, time.time() - start


# =============================================================================
# POST-SCAN CHECKS
# =============================================================================

def mark_stale(results):
    """Symbols whose latest usable day differs from the day most symbols
    used cannot be traded off today's close, so they are marked STALE."""

    dates = [r["date"] for r in results if r.get("date")]

    if not dates:
        return None

    reference = Counter(dates).most_common(1)[0][0]

    for r in results:
        if r.get("date") and r["date"] != reference:
            r["status"] = "STALE"
            r["direction"] = None

    return reference


def session_warning(results):
    """Warn when a large share of symbols are missing part of the required
    18 one-minute bars on the signal day - a sign of a broad Yahoo
    data-quality issue for that day, not just isolated thin trading."""

    evaluated = [r for r in results if r.get("status") in ("PASS", "FAIL")]

    if not evaluated:
        return None

    complete = sum(1 for r in evaluated if not r.get("missing"))
    share = complete / len(evaluated)

    if share < SESSION_COMPLETE_SHARE:
        return (
            f"Only {share:.0%} of scanned symbols have all 18 required "
            f"one-minute bars on the signal day (day before previous day). "
            f"Yahoo's data for that day may be unusually incomplete - "
            f"treat matches with extra caution and verify on TradingView."
        )

    return None


# =============================================================================
# HTML HELPERS
# =============================================================================

DASH = "&mdash;"


def esc(value):
    return html.escape(str(value))


def cell(value):
    if value is None or value == "":
        return DASH
    return esc(value)


def badge(value):

    if value is None:
        return DASH

    if value:
        return '<span class="badge pass">PASS</span>'

    return '<span class="badge fail">FAIL</span>'


def trend_name(d):
    return {1: "Up", -1: "Down"}.get(d, "Flat")


def hm_text(hm):
    return f"{hm // 100:02d}:{hm % 100:02d}"


# Order the six 3-minute candles are shown in, throughout the report.
CANDLE_ORDER = ["0915", "1515", "1518", "1521", "1524", "1527"]
CANDLE_LABEL = {
    "0915": "09:15", "1515": "15:15", "1518": "15:18",
    "1521": "15:21", "1524": "15:24", "1527": "15:27",
}


def arrow(d):
    return {1: "\u25b2", -1: "\u25bc"}.get(d, "\u2013")  # up, down, dash


def trend_class(d):
    return {1: "up", -1: "down"}.get(d, "flat")


def fmt_day(value, weekday=True):
    """'2026-09-28' -> 'Mon 28 Sep'."""

    if not value:
        return "\u2013"

    try:
        ts = pd.Timestamp(value)
    except Exception:
        return str(value)

    base = f"{ts.day} {ts.strftime('%b')}"

    return f"{ts.strftime('%a')} {base}" if weekday else base


def candle_strip(details, partial_groups):
    """Plain arrow strip, used only for rows that have no OHLC data."""

    if not details:
        return ""

    partial_groups = set(partial_groups or [])
    chips = []

    for label in CANDLE_ORDER:
        d = details.get(f"d{label}")
        flag = " candle-partial" if label in partial_groups else ""
        chips.append(
            f'<span class="candle {trend_class(d)}{flag}">'
            f'{CANDLE_LABEL[label]}<b>{arrow(d)}</b></span>'
        )

    return "".join(chips)


def build_candlestick_svg(ohlc, size="large", volumes=None):
    """Real OHLC candlesticks for the six 3-min candles. The large version
    also draws the volume bars underneath (the strategy compares the 15:24
    and 15:27 volumes), highlights the 15:24 anchor candle and labels every
    candle with its time."""

    large = size == "large"

    if large:
        width, height, pad_x = 320, 162, 14
        c_top, c_h = 18, 72
        v_base, v_h = 130, 26
        label_y = 153
    else:
        width, height, pad_x = 150, 34, 4
        c_top, c_h = 4, 26
        v_base = v_h = label_y = 0

    ohlc = ohlc or {}
    n = len(CANDLE_ORDER)
    slot_w = (width - 2 * pad_x) / n
    body_w = max(2.2, slot_w * (0.42 if large else 0.46))

    highs = [ohlc[l]["high"] for l in CANDLE_ORDER
             if ohlc.get(l) and ohlc[l].get("high") is not None]
    lows = [ohlc[l]["low"] for l in CANDLE_ORDER
            if ohlc.get(l) and ohlc[l].get("low") is not None]

    attrs = (
        f'class="candles-svg candles-{size}" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" preserveAspectRatio="xMidYMid meet" role="img"'
    )

    if not highs or not lows:
        return f'<svg {attrs} aria-label="No candle data"></svg>'

    hi, lo = max(highs), min(lows)
    if hi == lo:
        hi, lo = hi + 0.5, lo - 0.5
    span = hi - lo

    def y(price):
        return c_top + (hi - price) / span * c_h

    parts = []

    if large:
        # Highlight band behind the 15:24 anchor candle.
        ax = pad_x + slot_w * CANDLE_ORDER.index("1524")
        parts.append(
            f'<rect x="{ax + 2:.1f}" y="5" width="{slot_w - 4:.1f}" '
            f'height="{label_y - 12:.1f}" rx="8" fill="var(--accent)" '
            f'fill-opacity="0.07" stroke="var(--accent)" stroke-opacity="0.3" '
            f'stroke-dasharray="3,3"/>'
        )
        for frac in (0, 0.5, 1):
            gy = c_top + c_h * frac
            parts.append(
                f'<line x1="{pad_x}" y1="{gy:.1f}" x2="{width - pad_x}" y2="{gy:.1f}" '
                f'stroke="var(--line)" stroke-width="1" stroke-dasharray="2,4"/>'
            )
    else:
        mid = c_top + c_h / 2
        parts.append(
            f'<line x1="{pad_x}" y1="{mid:.1f}" x2="{width - pad_x}" y2="{mid:.1f}" '
            f'stroke="var(--line)" stroke-width="1" stroke-dasharray="2,3"/>'
        )

    # --- candles ---
    for i, label in enumerate(CANDLE_ORDER):
        cx = pad_x + slot_w * i + slot_w / 2
        c = ohlc.get(label)

        if not c or c.get("open") is None:
            ym = c_top + c_h / 2
            parts.append(
                f'<line x1="{cx - body_w/2:.1f}" y1="{ym:.1f}" x2="{cx + body_w/2:.1f}" '
                f'y2="{ym:.1f}" stroke="var(--text-faint)" stroke-width="1.5" '
                f'stroke-dasharray="1.5,2"/>'
            )
            continue

        o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
        color = "var(--long)" if cl >= o else "var(--short)"
        body_top = min(y(o), y(cl))
        body_h = max(1.6, abs(y(o) - y(cl)))

        parts.append(
            f'<line x1="{cx:.1f}" y1="{y(h):.1f}" x2="{cx:.1f}" y2="{y(l):.1f}" '
            f'stroke="{color}" stroke-width="1.3" stroke-linecap="round"/>'
            f'<rect x="{cx - body_w/2:.1f}" y="{body_top:.1f}" width="{body_w:.1f}" '
            f'height="{body_h:.1f}" fill="{color}" rx="1.5"/>'
        )

    # --- volume bars + time labels (large only) ---
    if large:
        vols = {l: (volumes or {}).get(l) for l in CANDLE_ORDER}
        vmax = max([v for v in vols.values() if v] or [0])

        parts.append(
            f'<line x1="{pad_x}" y1="{v_base}" x2="{width - pad_x}" y2="{v_base}" '
            f'stroke="var(--line-strong)" stroke-width="1"/>'
            f'<text x="{pad_x + 1}" y="{v_base - v_h - 4}" class="axis-label">volume</text>'
        )

        for i, label in enumerate(CANDLE_ORDER):
            cx = pad_x + slot_w * i + slot_w / 2
            v = vols.get(label)
            c = ohlc.get(label)

            if v and vmax > 0:
                bar_h = max(1.5, v / vmax * v_h)
                up = not c or c.get("close") is None or c["close"] >= c["open"]
                color = "var(--long)" if up else "var(--short)"
                key = label in ("1524", "1527")
                parts.append(
                    f'<rect x="{cx - body_w/2:.1f}" y="{v_base - bar_h:.1f}" '
                    f'width="{body_w:.1f}" height="{bar_h:.1f}" rx="1.5" fill="{color}" '
                    f'fill-opacity="{0.95 if key else 0.38}"/>'
                )

            strong = " axis-strong" if label in ("1524", "1527") else ""
            parts.append(
                f'<text x="{cx:.1f}" y="{label_y}" text-anchor="middle" '
                f'class="axis-label{strong}">{CANDLE_LABEL[label]}</text>'
            )

    return f'<svg {attrs} aria-label="6-candle price and volume chart">' + "".join(parts) + '</svg>'


def build_radar_svg(matches):
    """Circular 'signals detected' overview: every match is a blip on the
    radar, colored and labeled by its own real direction and symbol. A
    visual index of the matches; the cards carry the numeric detail."""

    box = 440
    cx = cy = box / 2
    max_r = 122
    label_r = max_r + 40
    show_labels = len(matches) <= 12

    parts = [
        '<defs>'
        '<radialGradient id="radarFill" cx="50%" cy="50%" r="50%">'
        '<stop offset="0%" stop-color="#4FE0F0" stop-opacity="0.10"/>'
        '<stop offset="100%" stop-color="#4FE0F0" stop-opacity="0.015"/>'
        '</radialGradient>'
        '<filter id="blipGlow" x="-150%" y="-150%" width="400%" height="400%">'
        '<feGaussianBlur stdDeviation="3.4" result="b"/>'
        '<feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>'
        '</filter></defs>',
        f'<circle cx="{cx}" cy="{cy}" r="{max_r}" fill="url(#radarFill)"/>',
    ]

    for frac in (0.34, 0.67, 1.0):
        parts.append(
            f'<circle cx="{cx}" cy="{cy}" r="{max_r * frac:.1f}" fill="none" '
            f'stroke="rgba(79,224,240,0.20)" stroke-width="1"/>'
        )

    for deg in range(0, 360, 30):
        rad = math.radians(deg)
        major = deg % 90 == 0
        parts.append(
            f'<line x1="{cx}" y1="{cy}" x2="{cx + max_r*math.cos(rad):.1f}" '
            f'y2="{cy + max_r*math.sin(rad):.1f}" stroke="rgba(79,224,240,'
            f'{0.16 if major else 0.07})" stroke-width="1"/>'
        )

    parts.append(f'<circle cx="{cx}" cy="{cy}" r="3" fill="var(--accent)"/>')

    n = len(matches)

    if n == 0:
        parts.append(
            f'<text x="{cx}" y="{cy + 52}" text-anchor="middle" class="radar-empty">'
            f'no contacts</text>'
        )
    else:
        blip_r = max_r * 0.74
        for i, r in enumerate(matches):
            angle = -90 + (360 / n) * i
            rad = math.radians(angle)
            bx, by = cx + blip_r * math.cos(rad), cy + blip_r * math.sin(rad)
            lx, ly = cx + label_r * math.cos(rad), cy + label_r * math.sin(rad)
            long_ = r.get("direction") == "LONG"
            color = "var(--long)" if long_ else "var(--short)"
            sym = esc(r.get("symbol", ""))

            anchor = "middle"
            if lx < cx - 8:
                anchor = "end"
            elif lx > cx + 8:
                anchor = "start"

            parts.append(
                f'<g><title>{sym} {esc(r.get("direction", ""))}</title>'
                + (
                    f'<line x1="{bx:.1f}" y1="{by:.1f}" x2="{lx:.1f}" y2="{ly:.1f}" '
                    f'stroke="{color}" stroke-width="1" opacity="0.4"/>'
                    if show_labels else ''
                )
                + f'<circle class="ping" cx="{bx:.1f}" cy="{by:.1f}" r="6" fill="none" '
                f'stroke="{color}" stroke-width="1.4" style="animation-delay:{i * 0.45:.2f}s"/>'
                f'<circle cx="{bx:.1f}" cy="{by:.1f}" r="5.5" fill="{color}" filter="url(#blipGlow)"/>'
                f'<circle cx="{bx:.1f}" cy="{by:.1f}" r="2" fill="var(--bg)"/>'
                + (
                    f'<text x="{lx:.1f}" y="{ly:.1f}" text-anchor="{anchor}" '
                    f'dominant-baseline="middle" class="radar-label" fill="{color}">{sym}</text>'
                    if show_labels else ''
                )
                + '</g>'
            )

    return (
        f'<svg class="radar-svg" viewBox="0 0 {box} {box}" width="{box}" height="{box}" '
        f'role="img" aria-label="Radar overview of matched signals">'
        + "".join(parts) + '</svg>'
    )


CSS = """
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

:root {
  --bg: #06090E;
  --panel: rgba(15, 22, 31, 0.72);
  --line: rgba(255, 255, 255, 0.07);
  --line-strong: rgba(255, 255, 255, 0.13);
  --border: rgba(255, 255, 255, 0.13);
  --border-soft: rgba(255, 255, 255, 0.07);
  --text: #EAF0F6;
  --text-dim: #93A1B1;
  --text-faint: #5F6D7D;
  --long: #34D08A;
  --long-soft: rgba(52, 208, 138, 0.14);
  --short: #FF5C66;
  --short-soft: rgba(255, 92, 102, 0.14);
  --caution: #F0B050;
  --caution-soft: rgba(240, 176, 80, 0.14);
  --accent: #4FE0F0;
  --accent-soft: rgba(79, 224, 240, 0.13);
  --sans: 'Sora', ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif;
  --mono: 'IBM Plex Mono', ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
}

* { box-sizing: border-box; }

html { background: var(--bg); }

body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: var(--sans);
  font-size: 15px;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}

:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

/* ---------- atmosphere ---------- */

.aurora, .grid-bg { position: fixed; inset: 0; pointer-events: none; z-index: 0; }
.aurora { overflow: hidden; }

.aurora i {
  position: absolute;
  border-radius: 50%;
  filter: blur(70px);
  opacity: 0.6;
}

.aurora .a {
  width: 640px; height: 640px; left: -140px; top: -200px;
  background: radial-gradient(circle, rgba(79, 224, 240, 0.30), transparent 65%);
  animation: drift-a 26s ease-in-out infinite alternate;
}
.aurora .b {
  width: 580px; height: 580px; right: -160px; top: 140px;
  background: radial-gradient(circle, rgba(52, 208, 138, 0.20), transparent 65%);
  animation: drift-b 32s ease-in-out infinite alternate;
}
.aurora .c {
  width: 720px; height: 720px; left: 28%; bottom: -380px;
  background: radial-gradient(circle, rgba(80, 110, 255, 0.17), transparent 65%);
  animation: drift-a 38s ease-in-out infinite alternate-reverse;
}

@keyframes drift-a { to { transform: translate3d(90px, 60px, 0) scale(1.1); } }
@keyframes drift-b { to { transform: translate3d(-80px, 90px, 0) scale(1.08); } }

.grid-bg {
  background-image:
    linear-gradient(rgba(255, 255, 255, 0.022) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.022) 1px, transparent 1px);
  background-size: 48px 48px;
  -webkit-mask-image: radial-gradient(ellipse at 50% 18%, #000 15%, transparent 72%);
  mask-image: radial-gradient(ellipse at 50% 18%, #000 15%, transparent 72%);
}

.wrap {
  position: relative;
  z-index: 1;
  max-width: 1360px;
  margin: 0 auto;
  padding: 22px 32px 72px;
}

/* ---------- shared surfaces ---------- */

.glass {
  position: relative;
  border-radius: 20px;
  border: 1px solid var(--line);
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.05), rgba(255, 255, 255, 0.01)),
    var(--panel);
  -webkit-backdrop-filter: blur(16px) saturate(130%);
  backdrop-filter: blur(16px) saturate(130%);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06), 0 28px 60px -30px rgba(0, 0, 0, 0.85);
}

.sec-head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
  margin: 0 0 16px;
}

.sec-title { margin: 0; font-size: 1.05rem; font-weight: 600; letter-spacing: -0.01em; }
.sec-note { color: var(--text-faint); font-size: 0.78rem; }

section { margin-bottom: 22px; }

/* ---------- top bar ---------- */

.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 6px 0 26px;
}

.brand { display: flex; align-items: center; gap: 12px; font-weight: 600; letter-spacing: -0.01em; }
.brand small { display: block; color: var(--text-faint); font-weight: 400; font-size: 0.72rem; }

.topmeta {
  display: flex;
  align-items: center;
  gap: 16px;
  color: var(--text-dim);
  font-family: var(--mono);
  font-size: 0.76rem;
}

.live {
  display: inline-flex;
  align-items: center;
  gap: 9px;
  padding: 6px 13px;
  border-radius: 999px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.03);
}

.live i {
  width: 7px; height: 7px; border-radius: 50%;
  background: var(--long);
  box-shadow: 0 0 10px 2px rgba(52, 208, 138, 0.7);
  animation: pulse 2.4s ease-in-out infinite;
}

@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.35; } }

.warn {
  background: var(--caution-soft);
  border: 1px solid rgba(240, 176, 80, 0.38);
  color: #F5D79F;
  border-radius: 14px;
  padding: 13px 18px;
  margin-bottom: 22px;
  font-size: 0.84rem;
  line-height: 1.6;
}

/* ---------- hero ---------- */

.hero {
  display: grid;
  grid-template-columns: minmax(0, 1.12fr) minmax(320px, 0.88fr);
  gap: 22px;
  align-items: stretch;
}

.hero-main { padding: 36px 36px 30px; display: flex; flex-direction: column; gap: 26px; }

h1 {
  margin: 0;
  font-size: clamp(2.1rem, 4.2vw, 3.5rem);
  line-height: 1.05;
  font-weight: 700;
  letter-spacing: -0.035em;
}

.hero-num {
  background: linear-gradient(135deg, #FFFFFF 10%, var(--accent) 130%);
  -webkit-background-clip: text;
  background-clip: text;
  color: transparent;
}

.hero-sub { margin: 14px 0 0; color: var(--text-dim); font-size: 0.95rem; line-height: 1.7; max-width: 60ch; }
.hero-sub b { color: var(--text); font-weight: 500; }

/* day-mapping timeline */

.timeline { list-style: none; margin: 0; padding: 0; display: grid; grid-template-columns: repeat(4, 1fr); }
.tl-step { position: relative; padding-top: 26px; }

.tl-step::before {
  content: '';
  position: absolute;
  top: 5px; left: 16px; right: 0;
  height: 2px;
  background: var(--line-strong);
}
.tl-step:last-child::before { display: none; }
.tl-step.done::before { background: var(--text-faint); }
.tl-step.active::before { background: linear-gradient(90deg, var(--accent), var(--line-strong)); }

.tl-dot {
  position: absolute;
  top: 0; left: 0;
  width: 12px; height: 12px;
  border-radius: 50%;
  border: 2px solid var(--text-faint);
  background: var(--bg);
}
.tl-step.done .tl-dot { background: var(--text-faint); }
.tl-step.active .tl-dot {
  border-color: var(--accent);
  background: var(--accent);
  box-shadow: 0 0 0 5px var(--accent-soft), 0 0 18px 2px rgba(79, 224, 240, 0.7);
}

.tl-label { color: var(--text-faint); font-size: 0.72rem; }
.tl-date { font-weight: 600; font-size: 0.92rem; margin-top: 2px; }
.tl-sub { font-family: var(--mono); color: var(--text-dim); font-size: 0.74rem; margin-top: 2px; }

/* KPI strip */

.kpis { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: auto; }

.kpi {
  padding: 15px 16px;
  border-radius: 14px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.025);
}

.kpi .n { font-family: var(--mono); font-size: 1.55rem; font-weight: 600; letter-spacing: -0.02em; }
.kpi .l { color: var(--text-faint); font-size: 0.74rem; margin-top: 2px; }
.kpi.long .n { color: var(--long); }
.kpi.short .n { color: var(--short); }

/* radar */

.hero-radar { padding: 22px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 6px; }

.radar-wrap { position: relative; width: 100%; max-width: 440px; aspect-ratio: 1; }

.radar-svg { position: absolute; inset: 0; width: 100%; height: 100%; overflow: visible; z-index: 1; }

.radar-sweep {
  position: absolute;
  inset: 22.27%;
  border-radius: 50%;
  background: conic-gradient(from 0deg,
    transparent 0deg, transparent 285deg,
    rgba(79, 224, 240, 0.30) 345deg, rgba(79, 224, 240, 0.85) 360deg);
  animation: spin 6s linear infinite;
  mix-blend-mode: screen;
  z-index: 0;
}

@keyframes spin { to { transform: rotate(360deg); } }

.ping {
  transform-box: fill-box;
  transform-origin: center;
  animation: ping 2.8s ease-out infinite;
}

@keyframes ping { from { transform: scale(1); opacity: 0.7; } to { transform: scale(3.4); opacity: 0; } }

.radar-label { font-family: var(--mono); font-size: 11px; font-weight: 600; }
.radar-empty { font-family: var(--mono); font-size: 13px; fill: var(--text-faint); letter-spacing: 0.04em; }

.radar-cap { color: var(--text-faint); font-size: 0.74rem; text-align: center; }

/* ---------- scan breakdown ---------- */

.panel { padding: 24px 26px; }

.dist-bar { display: flex; gap: 3px; height: 12px; }

.seg {
  min-width: 4px;
  border-radius: 999px;
  transform-origin: left center;
  animation: grow 1s cubic-bezier(0.2, 0.8, 0.2, 1) both;
}

@keyframes grow { from { transform: scaleX(0); opacity: 0; } to { transform: scaleX(1); opacity: 1; } }

.seg-pass { background: linear-gradient(90deg, var(--long), #86F2C0); box-shadow: 0 0 14px -2px var(--long); }
.seg-fail { background: #3B4B60; }
.seg-incomplete { background: var(--caution); }
.seg-stale { background: #D9822B; }
.seg-error { background: var(--short); }
.seg-nodata { background: #263241; }

.legend { display: flex; flex-wrap: wrap; gap: 10px 28px; margin-top: 18px; }
.lg { display: flex; align-items: center; gap: 9px; font-size: 0.8rem; color: var(--text-dim); }
.lg i { width: 9px; height: 9px; border-radius: 3px; }
.lg b { font-family: var(--mono); color: var(--text); font-weight: 500; }
.lg em { font-style: normal; color: var(--text-faint); font-family: var(--mono); font-size: 0.74rem; }

/* ---------- signal cards ---------- */

.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 18px; }

.match-card {
  --dir: var(--long);
  --edge: rgba(52, 208, 138, 0.75);
  position: relative;
  padding: 24px 24px 20px;
  border-radius: 20px;
  border: 1px solid transparent;
  background:
    linear-gradient(180deg, rgba(22, 31, 43, 0.94), rgba(11, 17, 24, 0.96)) padding-box,
    linear-gradient(150deg, var(--edge), rgba(255, 255, 255, 0.07) 36%, rgba(255, 255, 255, 0.03) 64%, var(--edge)) border-box;
  box-shadow: 0 34px 70px -40px var(--edge);
  overflow: hidden;
  animation: rise 0.55s ease-out both;
  transform: perspective(900px) rotateX(var(--rx, 0deg)) rotateY(var(--ry, 0deg));
  transition: transform 0.18s ease-out;
}

.match-card.short { --dir: var(--short); --edge: rgba(255, 92, 102, 0.75); }

@keyframes rise { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }

.match-card::before, .match-card::after {
  content: '';
  position: absolute;
  width: 14px; height: 14px;
  border: 2px solid var(--dir);
  opacity: 0.8;
  pointer-events: none;
}
.match-card::before { top: 8px; left: 8px; border-right: none; border-bottom: none; border-radius: 4px 0 0 0; }
.match-card::after { bottom: 8px; right: 8px; border-left: none; border-top: none; border-radius: 0 0 4px 0; }

.glare {
  position: absolute; inset: 0;
  border-radius: inherit;
  pointer-events: none;
  opacity: 0;
  transition: opacity 0.2s;
  background: radial-gradient(280px circle at var(--mx, 50%) var(--my, 0%), rgba(255, 255, 255, 0.09), transparent 60%);
}
.match-card:hover .glare { opacity: 1; }

.mc-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
.mc-sym { font-family: var(--mono); font-size: 1.6rem; font-weight: 600; letter-spacing: -0.02em; }

.mc-dir {
  font-family: var(--mono);
  font-size: 0.78rem;
  font-weight: 600;
  padding: 5px 11px;
  border-radius: 9px;
  color: var(--dir);
  background: rgba(255, 255, 255, 0.04);
  border: 1px solid var(--edge);
  text-shadow: 0 0 14px var(--edge);
  white-space: nowrap;
}

.mc-price { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 10px; margin-top: 6px; }
.mc-last { font-family: var(--mono); font-size: 1.05rem; }
.mc-chg { font-family: var(--mono); font-size: 0.78rem; padding: 2px 8px; border-radius: 6px; }
.mc-chg.up { color: var(--long); background: var(--long-soft); }
.mc-chg.down { color: var(--short); background: var(--short-soft); }
.mc-note { color: var(--text-faint); font-size: 0.7rem; }

.mc-chart {
  margin: 16px 0 4px;
  padding: 12px 8px 6px;
  border-radius: 14px;
  background: rgba(0, 0, 0, 0.25);
  border: 1px solid var(--line);
}

.candles-svg { display: block; }
.candles-large { width: 100%; height: auto; max-width: 480px; margin: 0 auto; }
.candles-small { width: 150px; height: 34px; }

.axis-label { font-family: var(--mono); font-size: 9.5px; fill: var(--text-faint); }
.axis-strong { fill: var(--text-dim); font-weight: 600; }

.checks { list-style: none; margin: 14px 0 0; padding: 0; display: grid; gap: 9px; }

.checks li {
  display: grid;
  grid-template-columns: 20px 1fr auto;
  gap: 10px;
  align-items: center;
  font-size: 0.78rem;
  color: var(--text-dim);
}

.ck {
  width: 18px; height: 18px;
  border-radius: 50%;
  display: grid; place-items: center;
  font-size: 0.62rem;
  color: var(--long);
  background: var(--long-soft);
  box-shadow: 0 0 12px -2px var(--long);
}

.ck-val { font-family: var(--mono); color: var(--text); font-size: 0.74rem; }

.mc-plan {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
  margin-top: 16px;
  padding-top: 14px;
  border-top: 1px dashed var(--line-strong);
}
.mc-plan .k { display: block; color: var(--text-faint); font-size: 0.68rem; }
.mc-plan .v { font-family: var(--mono); font-size: 0.8rem; }

.mc-verify {
  display: inline-block;
  margin-top: 12px;
  padding: 4px 10px;
  border-radius: 8px;
  background: var(--caution-soft);
  color: var(--caution);
  font-size: 0.7rem;
  line-height: 1.4;
}

.empty-state {
  padding: 34px 28px;
  color: var(--text-dim);
  font-size: 0.92rem;
  line-height: 1.7;
}
.empty-state b { color: var(--text); font-weight: 600; font-size: 1.02rem; }

/* ---------- table ---------- */

.table-panel { overflow: hidden; }

.table-head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  padding: 20px 24px;
  border-bottom: 1px solid var(--line);
}

.search { position: relative; }
.search svg { position: absolute; left: 13px; top: 50%; transform: translateY(-50%); color: var(--text-faint); }

.search input {
  width: 240px;
  max-width: 100%;
  padding: 10px 14px 10px 38px;
  border-radius: 12px;
  border: 1px solid var(--line-strong);
  background: rgba(0, 0, 0, 0.28);
  color: var(--text);
  font: inherit;
  font-size: 0.84rem;
  outline: none;
  transition: border-color 0.15s, box-shadow 0.15s;
}
.search input::placeholder { color: var(--text-faint); }
.search input:focus { border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-soft); }

.filters { display: flex; flex-wrap: wrap; gap: 8px; }

.chip {
  appearance: none;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 7px 13px;
  border-radius: 999px;
  border: 1px solid var(--line-strong);
  background: rgba(255, 255, 255, 0.03);
  color: var(--text-dim);
  font: inherit;
  font-size: 0.76rem;
  transition: border-color 0.15s, background 0.15s, color 0.15s;
}
.chip b { font-family: var(--mono); font-weight: 500; color: var(--text); }
.chip:hover { border-color: rgba(79, 224, 240, 0.6); color: var(--text); }
.chip.active { background: var(--accent-soft); border-color: rgba(79, 224, 240, 0.6); color: var(--text); }

.table-scroll { overflow: auto; max-height: 740px; }

table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }

th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: rgba(14, 20, 29, 0.96);
  color: var(--text-faint);
  font-weight: 500;
  font-size: 0.72rem;
  text-align: left;
  padding: 12px 16px;
  border-bottom: 1px solid var(--line-strong);
  white-space: nowrap;
}

td {
  padding: 13px 16px;
  border-top: 1px solid var(--line);
  vertical-align: middle;
  white-space: nowrap;
}

tbody tr { transition: background 0.12s; }
tbody tr:hover { background: rgba(79, 224, 240, 0.04); }
tbody tr[hidden] { display: none; }
tbody tr[data-status="pass"] td:first-child { box-shadow: inset 3px 0 0 var(--long); }

.symbol { font-family: var(--mono); font-weight: 600; }
.mono { font-family: var(--mono); }
.dim { color: var(--text-dim); }

.sub {
  display: block;
  margin-top: 4px;
  color: var(--text-faint);
  font-family: var(--mono);
  font-size: 0.7rem;
  white-space: normal;
  max-width: 200px;
}

.dir {
  font-family: var(--mono);
  font-size: 0.74rem;
  font-weight: 600;
  padding: 4px 10px;
  border-radius: 8px;
}
.dir.long { color: var(--long); background: var(--long-soft); }
.dir.short { color: var(--short); background: var(--short-soft); }

.chk {
  display: inline-grid;
  place-items: center;
  width: 22px; height: 22px;
  border-radius: 7px;
  font-size: 0.7rem;
}
.chk.yes { color: var(--long); background: var(--long-soft); }
.chk.no { color: var(--short); background: var(--short-soft); }
.chk.na { color: var(--text-faint); background: rgba(255, 255, 255, 0.04); }

.pill {
  display: inline-flex;
  align-items: center;
  padding: 4px 11px;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 500;
}
.pill.pass { color: var(--long); background: var(--long-soft); }
.pill.fail { color: var(--text-dim); background: rgba(255, 255, 255, 0.05); }
.pill.incomplete, .pill.stale { color: var(--caution); background: var(--caution-soft); }
.pill.error { color: var(--short); background: var(--short-soft); }
.pill.nodata { color: var(--text-faint); background: rgba(255, 255, 255, 0.04); }

.verify-tag {
  display: inline-block;
  margin-left: 8px;
  padding: 2px 8px;
  border-radius: 6px;
  background: var(--caution-soft);
  color: var(--caution);
  font-size: 0.68rem;
}

.candle { display: inline-flex; gap: 3px; font-family: var(--mono); font-size: 0.72rem; color: var(--text-faint); margin-right: 8px; }
.candle b { font-weight: 600; }
.candle.up b { color: var(--long); }
.candle.down b { color: var(--short); }

.empty-row { display: none; padding: 36px; text-align: center; color: var(--text-faint); font-size: 0.86rem; }

.table-foot {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 14px 24px;
  border-top: 1px solid var(--line);
  color: var(--text-faint);
  font-size: 0.78rem;
}

.btn {
  appearance: none;
  cursor: pointer;
  padding: 8px 16px;
  border-radius: 10px;
  border: 1px solid rgba(79, 224, 240, 0.5);
  background: var(--accent-soft);
  color: var(--text);
  font: inherit;
  font-size: 0.78rem;
  transition: background 0.15s, box-shadow 0.15s;
}
.btn:hover { background: rgba(79, 224, 240, 0.22); box-shadow: 0 0 18px -4px var(--accent); }

/* ---------- footer ---------- */

.foot-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 18px; }
.foot-card { padding: 24px 26px; color: var(--text-dim); font-size: 0.82rem; line-height: 1.75; }
.foot-card h2 { margin: 0 0 8px; font-size: 0.92rem; font-weight: 600; color: var(--text); }
.foot-card ol { margin: 8px 0; padding-left: 20px; }
.foot-card li { margin-bottom: 4px; }

/* ---------- responsive ---------- */

@media (max-width: 980px) {
  .hero { grid-template-columns: 1fr; }
  .hero-main { padding: 28px 24px 24px; }
}

@media (max-width: 640px) {
  .wrap { padding: 16px 16px 56px; }
  .topbar { flex-direction: column; align-items: flex-start; }
  .kpis { grid-template-columns: repeat(2, 1fr); }
  .timeline { grid-template-columns: 1fr 1fr; row-gap: 22px; }
  .tl-step::before { display: none; }
  .cards { grid-template-columns: 1fr; }
  .table-head { padding: 16px; }
  .search input { width: 100%; }
  .search { width: 100%; }
}

@media (prefers-reduced-motion: reduce) {
  .aurora i, .radar-sweep, .ping, .live i, .seg, .match-card { animation: none !important; }
  .radar-sweep { display: none; }
}
"""


SCRIPT = """<script>
(function () {
  var reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  /* count-up numbers */
  if (!reduce) {
    document.querySelectorAll('[data-count]').forEach(function (el) {
      var target = parseFloat(el.getAttribute('data-count'));
      var dec = parseInt(el.getAttribute('data-dec') || '0', 10);
      var suffix = el.getAttribute('data-suffix') || '';
      var start = null, dur = 900;
      function step(ts) {
        if (start === null) start = ts;
        var p = Math.min(1, (ts - start) / dur);
        var v = target * (1 - Math.pow(1 - p, 3));
        el.textContent = (dec ? v.toFixed(dec) : Math.round(v).toLocaleString('en-US')) + suffix;
        if (p < 1) requestAnimationFrame(step);
      }
      requestAnimationFrame(step);
    });
  }

  /* 3D tilt + cursor glare on signal cards */
  if (!reduce) {
    document.querySelectorAll('.match-card').forEach(function (card) {
      card.addEventListener('mousemove', function (e) {
        var r = card.getBoundingClientRect();
        var px = (e.clientX - r.left) / r.width;
        var py = (e.clientY - r.top) / r.height;
        card.style.setProperty('--rx', ((0.5 - py) * 7).toFixed(2) + 'deg');
        card.style.setProperty('--ry', ((px - 0.5) * 7).toFixed(2) + 'deg');
        card.style.setProperty('--mx', (px * 100).toFixed(1) + '%');
        card.style.setProperty('--my', (py * 100).toFixed(1) + '%');
      });
      card.addEventListener('mouseleave', function () {
        card.style.setProperty('--rx', '0deg');
        card.style.setProperty('--ry', '0deg');
      });
    });
  }

  /* searchable, filterable, paged table */
  var table = document.getElementById('scan-table');
  if (table) {
    var rows = Array.prototype.slice.call(table.tBodies[0].rows);
    var PAGE = 50;
    var state = { f: 'all', q: '', limit: PAGE };
    var more = document.getElementById('more');
    var label = document.getElementById('shown-label');
    var empty = document.getElementById('empty-row');
    var input = document.getElementById('q');
    var chips = Array.prototype.slice.call(document.querySelectorAll('.chip'));

    function apply() {
      var matched = 0, shown = 0;
      rows.forEach(function (r) {
        var ok = (state.f === 'all' || r.getAttribute('data-status') === state.f) &&
                 (!state.q || r.getAttribute('data-symbol').indexOf(state.q) !== -1);
        if (ok) {
          matched++;
          if (shown < state.limit) { r.hidden = false; shown++; } else { r.hidden = true; }
        } else {
          r.hidden = true;
        }
      });
      label.textContent = 'Showing ' + shown + ' of ' + matched;
      more.style.display = shown < matched ? '' : 'none';
      empty.style.display = matched === 0 ? 'block' : 'none';
    }

    chips.forEach(function (chip) {
      chip.addEventListener('click', function () {
        state.f = chip.getAttribute('data-f');
        state.limit = PAGE;
        chips.forEach(function (c) { c.classList.toggle('active', c === chip); });
        apply();
      });
    });

    input.addEventListener('input', function () {
      state.q = input.value.trim().toLowerCase();
      state.limit = PAGE;
      apply();
    });

    more.addEventListener('click', function () {
      state.limit += PAGE;
      apply();
    });

    apply();
  }
})();
</script>"""


STATUS_KEY = {
    "PASS": "pass", "FAIL": "fail", "INCOMPLETE": "incomplete",
    "STALE": "stale", "NO_DATA": "nodata", "ERROR": "error",
}

STATUS_LABEL = {
    "pass": "Match", "fail": "No match", "incomplete": "Incomplete",
    "stale": "Stale", "nodata": "No data", "error": "Error",
}

STATUS_RANK = {"pass": 0, "fail": 1, "incomplete": 2, "stale": 3, "error": 4, "nodata": 5}


def build_table_row(r):

    details = r.get("details")
    status = r.get("status", "UNKNOWN")
    key = STATUS_KEY.get(status, "nodata")
    symbol = str(r.get("symbol", ""))

    if r.get("ohlc"):
        chart = build_candlestick_svg(r["ohlc"], size="small")
    else:
        chart = candle_strip(details, r.get("partial_groups"))

    def chk(value, detail):
        if value is None:
            return '<span class="chk na">&ndash;</span>'
        glyph = "&#10003;" if value else "&#10005;"
        cls = "yes" if value else "no"
        extra = f'<span class="sub">{detail}</span>' if detail else ""
        return f'<span class="chk {cls}">{glyph}</span>{extra}'

    if details:
        d1524 = details["d1524"]
        detail_1a = f'{details["1524_vol"]:,.0f} vs {details["1527_vol"]:,.0f}'
        detail_1b = f'{r.get("cond1b_matches", 0)} of 4 match'
        detail_2 = f'{trend_name(details["d0915"])} / {trend_name(d1524)}'
    else:
        detail_1a = detail_1b = detail_2 = ""

    direction = r.get("direction")
    if direction:
        dir_html = (
            f'<span class="dir {"long" if direction == "LONG" else "short"}">'
            f'{arrow(1 if direction == "LONG" else -1)} {esc(direction)}</span>'
        )
    else:
        dir_html = '<span class="dim">&ndash;</span>'

    verify_html = ""
    if r.get("partial_3m"):
        groups = ", ".join(CANDLE_LABEL[g] for g in r.get("partial_groups", []))
        verify_html = f'<span class="verify-tag">Verify: partial {esc(groups)}</span>'

    data_label = {
        "OK": "OK", "FALLBACK": "Fallback (7d)", "NO_DATA": "No data", "ERROR": "Error",
    }.get(r.get("data_status"), r.get("data_status"))

    data_bits = [cell(data_label)]

    if r.get("missing"):
        data_bits.append(
            '<span class="sub">missing: '
            + ", ".join(hm_text(h) for h in r["missing"]) + "</span>"
        )
    if r.get("error"):
        data_bits.append(f'<span class="sub">{esc(r["error"][:120])}</span>')

    return f"""
<tr data-status="{key}" data-symbol="{esc(symbol.lower())}">
<td class="symbol">{esc(symbol)}</td>
<td class="mono dim">{esc(fmt_day(r.get("date"), weekday=False)) if r.get("date") else "&ndash;"}</td>
<td>{dir_html}</td>
<td>{chart}</td>
<td>{chk(r.get("cond1a"), detail_1a)}</td>
<td>{chk(r.get("cond1b"), detail_1b)}</td>
<td>{chk(r.get("cond2"), detail_2)}</td>
<td><span class="pill {key}">{STATUS_LABEL[key]}</span>{verify_html}</td>
<td class="mono dim">{"".join(data_bits)}</td>
</tr>
"""


def build_match_card(r, entry_day):

    direction = r["direction"]
    long_ = direction == "LONG"
    cls = "long" if long_ else "short"

    ohlc = r.get("ohlc") or {}
    d = r.get("details") or {}

    first_open = (ohlc.get("0915") or {}).get("open")
    last_close = (ohlc.get("1527") or {}).get("close")

    price_html = ""
    if first_open and last_close:
        chg = (last_close / first_open - 1) * 100
        price_html = (
            '<div class="mc-price">'
            f'<span class="mc-last">&#8377;{last_close:,.2f}</span>'
            f'<span class="mc-chg {"up" if chg >= 0 else "down"}">{chg:+.2f}%</span>'
            '<span class="mc-note">09:15 open to 15:27 close</span></div>'
        )

    volumes = {label: d.get(f"{label}_vol") for label in CANDLE_ORDER}
    chart = build_candlestick_svg(ohlc, size="large", volumes=volumes)

    v24 = d.get("1524_vol", 0) or 0
    v27 = d.get("1527_vol", 0) or 0

    checks = [
        ("15:24 volume above 15:27", f"{v24:,.0f} vs {v27:,.0f}"),
        ("2+ of 4 candles share the 15:24 trend", f'{r.get("cond1b_matches", 0)} of 4'),
        ("09:15 trend matches 15:24", f'{trend_name(d.get("d0915"))} / {trend_name(d.get("d1524"))}'),
    ]
    checks_html = "".join(
        f'<li><span class="ck">&#10003;</span><span>{esc(t)}</span>'
        f'<span class="ck-val">{esc(v)}</span></li>'
        for t, v in checks
    )

    verify = ""
    if r.get("partial_3m"):
        groups = ", ".join(CANDLE_LABEL[g] for g in r.get("partial_groups", []))
        verify = (
            f'<div class="mc-verify">Verify on TradingView: {esc(groups)} was built '
            f'from an incomplete set of 1-minute bars.</div>'
        )

    return f"""
<article class="match-card {cls}">
  <span class="glare"></span>
  <div class="mc-head">
    <span class="mc-sym">{esc(r["symbol"])}</span>
    <span class="mc-dir">{arrow(1 if long_ else -1)} {esc(direction)}</span>
  </div>
  {price_html}
  <div class="mc-chart">{chart}</div>
  <ul class="checks">{checks_html}</ul>
  <div class="mc-plan">
    <div><span class="k">Entry</span><span class="v">{esc(fmt_day(entry_day))}, 09:15 open</span></div>
    <div><span class="k">Exit</span><span class="v">15:27</span></div>
  </div>
  {verify}
</article>"""


# =============================================================================
# GENERATE HTML
# =============================================================================

def generate_html_report(results, elapsed, universe_source):

    total = len(results)
    counts = Counter(STATUS_KEY.get(r.get("status"), "nodata") for r in results)

    matches = [r for r in results if r.get("status") == "PASS"]
    longs = sum(1 for r in matches if r.get("direction") == "LONG")
    shorts = len(matches) - longs

    signal_date = Counter(r["date"] for r in results if r.get("date")).most_common(1)
    previous_day = Counter(r["previous_day"] for r in results if r.get("previous_day")).most_common(1)
    entry_dates = Counter(r["entry_day"] for r in results if r.get("entry_day")).most_common(1)

    signal_date = signal_date[0][0] if signal_date else None
    previous_day = previous_day[0][0] if previous_day else None
    entry_day = (
        entry_dates[0][0] if entry_dates
        else pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")
    )

    scan_time = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%d %b %Y, %H:%M IST")

    warning = session_warning(results)
    warning_html = f'<div class="warn">{esc(warning)}</div>' if warning else ""

    # ---- hero ----

    n = len(matches)

    if n:
        headline = (
            f'<span class="hero-num" data-count="{n}">{n}</span> '
            f'signal{"s" if n != 1 else ""} detected'
        )
        split = f' {longs} long, {shorts} short.' if n else ""
    else:
        headline = 'No <span class="hero-num">signals</span> detected'
        split = ""

    hero_sub = (
        f'Scanned <b>{total:,}</b> symbols from {esc(universe_source)} against the '
        f'<b>{esc(fmt_day(signal_date))}</b> session.{split} Trades enter at the '
        f'<b>{esc(fmt_day(entry_day))}</b> 09:15 open and exit at 15:27.'
    )

    timeline = f"""
<ol class="timeline">
  <li class="tl-step done"><span class="tl-dot"></span>
    <div class="tl-label">Signal day</div><div class="tl-date">{esc(fmt_day(signal_date))}</div>
    <div class="tl-sub">conditions read</div></li>
  <li class="tl-step done"><span class="tl-dot"></span>
    <div class="tl-label">Previous day</div><div class="tl-date">{esc(fmt_day(previous_day))}</div>
    <div class="tl-sub">latest session</div></li>
  <li class="tl-step active"><span class="tl-dot"></span>
    <div class="tl-label">Entry</div><div class="tl-date">{esc(fmt_day(entry_day))}</div>
    <div class="tl-sub">09:15 open</div></li>
  <li class="tl-step"><span class="tl-dot"></span>
    <div class="tl-label">Exit</div><div class="tl-date">{esc(fmt_day(entry_day))}</div>
    <div class="tl-sub">15:27</div></li>
</ol>"""

    kpis = f"""
<div class="kpis">
  <div class="kpi"><div class="n" data-count="{total}">{total:,}</div><div class="l">Scanned</div></div>
  <div class="kpi long"><div class="n" data-count="{longs}">{longs}</div><div class="l">Long</div></div>
  <div class="kpi short"><div class="n" data-count="{shorts}">{shorts}</div><div class="l">Short</div></div>
  <div class="kpi"><div class="n" data-count="{elapsed:.1f}" data-dec="1" data-suffix="s">{elapsed:.1f}s</div><div class="l">Scan time</div></div>
</div>"""

    if n == 0:
        radar_caption = "Nothing to plot. No symbol passed both conditions."
    elif n > 12:
        radar_caption = "Each blip is one matched signal. Hover a blip to see its symbol."
    else:
        radar_caption = "Each blip is one matched signal."

    radar = (
        '<div class="radar-wrap"><div class="radar-sweep"></div>'
        + build_radar_svg(matches) + '</div>'
        f'<div class="radar-cap">{radar_caption}</div>'
    )

    # ---- scan breakdown ----

    order = ["pass", "fail", "incomplete", "stale", "nodata", "error"]

    segs = "".join(
        f'<span class="seg seg-{k}" style="flex:{counts[k]};animation-delay:{i * 0.08:.2f}s" '
        f'title="{STATUS_LABEL[k]}: {counts[k]:,}"></span>'
        for i, k in enumerate(order) if counts[k]
    )

    legend = "".join(
        f'<div class="lg"><i class="seg-{k}"></i>{STATUS_LABEL[k]} '
        f'<b>{counts[k]:,}</b><em>{(counts[k] / total if total else 0):.1%}</em></div>'
        for k in order if counts[k]
    )

    # ---- signals ----

    if matches:
        cards = "".join(build_match_card(r, entry_day) for r in matches)
        signals_html = f'<div class="cards">{cards}</div>'
    else:
        signals_html = f"""
<div class="glass empty-state">
  <b>No symbol matched every condition.</b><br>
  {total:,} symbols were checked against the {esc(fmt_day(signal_date))} session and none passed
  both conditions. That is a normal outcome for a strategy this selective.
</div>"""

    # ---- table ----

    sorted_results = sorted(
        results,
        key=lambda x: (
            STATUS_RANK.get(STATUS_KEY.get(x.get("status"), "nodata"), 9),
            x.get("symbol", ""),
        ),
    )

    table_rows = "".join(build_table_row(r) for r in sorted_results)

    chips = [f'<button class="chip active" data-f="all">All <b>{total:,}</b></button>']
    chips += [
        f'<button class="chip" data-f="{k}">{STATUS_LABEL[k]} <b>{counts[k]:,}</b></button>'
        for k in order if counts[k]
    ]

    search_icon = (
        '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/>'
        '<path d="M21 21l-4.3-4.3"/></svg>'
    )

    logo = (
        '<svg width="32" height="32" viewBox="0 0 32 32" aria-hidden="true">'
        '<circle cx="16" cy="16" r="13" fill="none" stroke="#4FE0F0" stroke-opacity=".55" stroke-width="1.5"/>'
        '<circle cx="16" cy="16" r="7" fill="none" stroke="#4FE0F0" stroke-opacity=".35" stroke-width="1.5"/>'
        '<path d="M16 16 L26.5 8.5" stroke="#4FE0F0" stroke-width="2.2" stroke-linecap="round"/>'
        '<circle cx="16" cy="16" r="2.4" fill="#4FE0F0"/></svg>'
    )

    document = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NSE Momentum Scanner</title>
<style>{CSS}</style>
</head>
<body>
<div class="aurora"><i class="a"></i><i class="b"></i><i class="c"></i></div>
<div class="grid-bg"></div>

<div class="wrap">

<header class="topbar">
  <div class="brand">{logo}<div>NSE Momentum Scanner<small>3-minute closing-window strategy</small></div></div>
  <div class="topmeta"><span class="live"><i></i>Scan complete</span><span>{esc(scan_time)}</span></div>
</header>

{warning_html}

<section class="hero">
  <div class="glass hero-main">
    <div>
      <h1>{headline}</h1>
      <p class="hero-sub">{hero_sub}</p>
    </div>
    {timeline}
    {kpis}
  </div>
  <div class="glass hero-radar">{radar}</div>
</section>

<section class="glass panel">
  <div class="sec-head"><h2 class="sec-title">Scan breakdown</h2><span class="sec-note">{total:,} symbols</span></div>
  <div class="dist-bar">{segs}</div>
  <div class="legend">{legend}</div>
</section>

<section>
  <div class="sec-head"><h2 class="sec-title">Signals</h2><span class="sec-note">{n} matched</span></div>
  {signals_html}
</section>

<section class="glass table-panel">
  <div class="table-head">
    <div class="search">{search_icon}<input id="q" type="search" placeholder="Search symbol" autocomplete="off" aria-label="Search symbol"></div>
    <div class="filters">{"".join(chips)}</div>
  </div>
  <div class="table-scroll">
  <table id="scan-table">
    <thead><tr>
      <th>Symbol</th><th>Signal day</th><th>Direction</th><th>Candles</th>
      <th>15:24 vol &gt; 15:27</th><th>Trend match (2+ of 4)</th><th>09:15 = 15:24</th>
      <th>Result</th><th>Data</th>
    </tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
  <div class="empty-row" id="empty-row">No symbols match this search.</div>
  </div>
  <div class="table-foot"><span id="shown-label"></span><button class="btn" id="more" type="button">Show 50 more</button></div>
</section>

<div class="foot-grid">
  <div class="glass foot-card">
    <h2>How the signal works</h2>
    Every condition is read on the signal day, two trading days before entry, on
    3-minute candles.
    <ol>
      <li>The 15:24 candle's volume is higher than the 15:27 candle's, and at least two of
      the candles at 15:15, 15:18, 15:21 and 15:27 share the 15:24 candle's trend.</li>
      <li>The 09:15 candle's trend matches the 15:24 candle's trend.</li>
    </ol>
    Direction follows the 15:24 candle: up is long, down is short. Entry is the 09:15 open
    on entry day; exit is 15:27 the same day.
  </div>
  <div class="glass foot-card">
    <h2>Data notes</h2>
    Yahoo has no native 3-minute interval, so each 3-minute candle is built from three
    1-minute bars. A minute with no trades is absent from Yahoo's data and is treated as
    zero volume with no trend, which is correct when that is what happened, but Yahoo
    cannot tell it apart from a dropped bar. A "Verify" tag means a candle was built from
    an incomplete set of bars, so check it on TradingView before acting. Incomplete means no
    data in the required window; stale means the signal day differs from most symbols.
  </div>
</div>

</div>
{SCRIPT}
</body>
</html>
'''

    with open("index.html", "w", encoding="utf-8") as file:
        file.write(document)


# =============================================================================
# MAIN
# =============================================================================

def main():

    print()
    print("=" * 70)
    print("NSE EOD MOMENTUM SCANNER")
    print("=" * 70)
    print()
    print("yfinance version:", getattr(yf, "__version__", "unknown"))

    universe, source = get_stock_universe()

    print()
    print("=" * 70)
    print(f"FINAL STOCK UNIVERSE: {len(universe)}")
    print(f"SOURCE: {source}")
    print("=" * 70)

    results, elapsed = scan_all_symbols(universe)

    reference_date = mark_stale(results)

    matches = [r for r in results if r.get("status") == "PASS"]

    print()
    print("=" * 70)
    print("SCAN COMPLETE")
    print("=" * 70)
    print(f"Universe    : {len(universe)}")
    print(f"Signal day  : {reference_date}")
    print(f"Matches     : {len(matches)}")
    print(f"Time        : {elapsed:.1f} seconds")
    print()

    if matches:
        print("MATCHES:")
        for result in matches:
            print(
                f"  {result['symbol']:<15}"
                f"{result['direction']:<7}"
                f"{result['date']}"
            )
    else:
        print("NO MATCHES.")

    print()

    for name in ("PASS", "FAIL", "INCOMPLETE", "STALE", "NO_DATA", "ERROR"):
        n = sum(1 for r in results if r.get("status") == name)
        print(f"{name:<11}: {n}")

    # Show a few distinct error messages so problems are easy to diagnose.
    error_messages = list(dict.fromkeys(
        r.get("error", "") for r in results if r.get("status") == "ERROR"
    ))[:5]

    if error_messages:
        print()
        print("SAMPLE ERRORS:")
        for message in error_messages:
            print(f"  - {message}")

    warning = session_warning(results)
    if warning:
        print()
        print("WARNING:", warning)

    generate_html_report(results, elapsed, source)

    print()
    print("HTML report: index.html")
    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
