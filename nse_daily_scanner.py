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

    out = pd.DataFrame({
        "date": ts.dt.strftime("%Y-%m-%d").values,
        "hm": (ts.dt.hour * 100 + ts.dt.minute).values,
        "open": pd.to_numeric(df["Open"], errors="coerce").values,
        "close": pd.to_numeric(df["Close"], errors="coerce").values,
        "volume": pd.to_numeric(df["Volume"], errors="coerce").values,
    })

    # Regular NSE session only.
    out = out[out["hm"].between(915, 1529)]

    out = out.dropna(subset=["open", "close", "volume"])

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
    Close = close of the latest sub-minute that has data, Volume = sum of
    the sub-minutes' volumes. This is the correct construction as long as
    a missing sub-minute really means "no trades" rather than "Yahoo
    dropped a real bar" - the code cannot tell those two apart, which is
    exactly what a manual cross-check (e.g. on TradingView) is good for.
    """

    present = [minute for minute in minutes if minute in m]
    missing = [minute for minute in minutes if minute not in m]

    if not present:
        return {
            "open": None,
            "close": None,
            "volume": 0,
            "minutes_used": [],
            "minutes_missing": missing,
            "partial": True,
        }

    candles = [m[minute] for minute in present]

    return {
        "open": candles[0][0],
        "close": candles[-1][1],
        "volume": sum(int(round(c[2])) for c in candles),
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
    # DATE -> {minute: (open, close, volume)}
    # ---------------------------------------------------------------------

    by_date = {}

    for date, hm, o, c, v in zip(
        rows["date"], rows["hm"], rows["open"], rows["close"], rows["volume"]
    ):
        by_date.setdefault(date, {})[int(hm)] = (float(o), float(c), float(v))

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
    return {1: "\u25b2", -1: "\u25bc"}.get(d, "\u2013")  # ▲ ▼ –


def trend_class(d):
    return {1: "up", -1: "down"}.get(d, "flat")


def candle_strip(details, partial_groups):
    """Compact 6-candle strip shown per row, e.g. 09:15▲ 15:15▲ 15:18▼ ..."""

    if not details:
        return ""

    partial_groups = set(partial_groups or [])
    chips = []

    for label in CANDLE_ORDER:
        d = details.get(f"d{label}")
        cls = trend_class(d)
        flag = " candle-partial" if label in partial_groups else ""
        chips.append(
            f'<span class="candle {cls}{flag}">'
            f'{CANDLE_LABEL[label]}<b>{arrow(d)}</b></span>'
        )

    return "".join(chips)


CSS = """
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

:root {
  --bg: #0B0F14;
  --panel: #131A22;
  --panel-2: #182029;
  --border: #232C38;
  --border-soft: #1B222C;
  --text: #E7ECF1;
  --text-dim: #8493A3;
  --text-faint: #56626F;
  --long: #34C27D;
  --long-soft: rgba(52, 194, 125, 0.13);
  --short: #F0555C;
  --short-soft: rgba(240, 85, 92, 0.13);
  --caution: #E7A94A;
  --caution-soft: rgba(231, 169, 74, 0.13);
  --accent: #45D9E8;
  --accent-soft: rgba(69, 217, 232, 0.13);
  --sans: 'IBM Plex Sans', ui-sans-serif, system-ui, -apple-system, sans-serif;
  --mono: 'IBM Plex Mono', ui-monospace, 'SF Mono', Menlo, monospace;
}

* { box-sizing: border-box; }

html { background: var(--bg); }

body {
  position: relative;
  margin: 0;
  padding: 40px 28px 64px;
  background:
    radial-gradient(ellipse 1100px 520px at 50% -8%, rgba(69, 217, 232, 0.055), transparent 60%),
    var(--bg);
  color: var(--text);
  font-family: var(--sans);
  font-size: 15px;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}

/* faint instrument-panel grid, purely atmospheric */
body::before {
  content: '';
  position: fixed;
  inset: 0;
  background-image:
    linear-gradient(rgba(231, 236, 241, 0.025) 1px, transparent 1px),
    linear-gradient(90deg, rgba(231, 236, 241, 0.025) 1px, transparent 1px);
  background-size: 46px 46px;
  pointer-events: none;
  z-index: 0;
}

a { color: inherit; }

:focus-visible { outline: 2px solid var(--caution); outline-offset: 2px; }

.container { position: relative; z-index: 1; max-width: 1320px; margin: 0 auto; }

/* ---- header ---- */

header { margin-bottom: 28px; }

h1 {
  margin: 0 0 4px;
  font-size: 1.65rem;
  font-weight: 700;
  letter-spacing: -0.01em;
}

.status-chip {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  margin-top: 10px;
  color: var(--text-dim);
  font-family: var(--mono);
  font-size: 0.78rem;
  letter-spacing: 0.02em;
}

.status-dot {
  width: 7px;
  height: 7px;
  margin-right: 7px;
  border-radius: 50%;
  background: var(--long);
  box-shadow: 0 0 8px 1px var(--long);
  animation: pulse 2.4s ease-in-out infinite;
}

@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.35; } }

@media (prefers-reduced-motion: reduce) {
  .status-dot { animation: none; }
}

.meta-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, max-content));
  column-gap: 40px;
  row-gap: 16px;
  margin-top: 18px;
  padding-top: 18px;
  border-top: 1px solid var(--border-soft);
}

.meta-item { max-width: 320px; }

.meta-label {
  color: var(--text-faint);
  font-size: 0.75rem;
  letter-spacing: 0.02em;
  margin-bottom: 3px;
}

.meta-value {
  color: var(--text);
  font-size: 0.9rem;
  font-family: var(--mono);
}

/* ---- warning ---- */

.warn {
  background: var(--caution-soft);
  border: 1px solid rgba(231, 169, 74, 0.35);
  color: #F0CE93;
  border-radius: 8px;
  padding: 13px 16px;
  margin-bottom: 24px;
  font-size: 0.875rem;
  line-height: 1.55;
}

/* ---- matches (hero) ---- */

.matches-section { position: relative; margin-bottom: 28px; padding-top: 4px; }

.scan-line {
  position: absolute;
  left: 0;
  right: 0;
  top: 4px;
  height: 2px;
  background: linear-gradient(90deg, transparent, var(--accent), transparent);
  box-shadow: 0 0 14px 1px var(--accent);
  animation: sweep 1.5s cubic-bezier(0.4, 0, 0.2, 1) 1 both;
  pointer-events: none;
}

@keyframes sweep {
  0%   { top: 4px; opacity: 0; }
  8%   { opacity: 1; }
  85%  { opacity: 1; }
  100% { top: 100%; opacity: 0; }
}

@media (prefers-reduced-motion: reduce) {
  .scan-line { display: none; }
}

.matches-heading {
  display: flex;
  align-items: center;
  gap: 7px;
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--text-dim);
  letter-spacing: 0.02em;
  margin: 0 0 12px;
}

.matches-heading .glyph { color: var(--accent); font-size: 0.85em; margin-right: 7px; }

.matches-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 12px;
}

@keyframes rise {
  from { opacity: 0; transform: translateY(6px); }
  to   { opacity: 1; transform: translateY(0); }
}

@media (prefers-reduced-motion: reduce) {
  .match-card { animation: none !important; }
}

.match-card {
  position: relative;
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 18px 20px;
  animation: rise 0.4s ease-out both;
}

.match-card.long { color: var(--long); box-shadow: 0 0 28px -6px var(--long-soft); }
.match-card.short { color: var(--short); box-shadow: 0 0 28px -6px var(--short-soft); }

/* corner-bracket "target lock" accents, in the card's direction color */
.match-card::before,
.match-card::after {
  content: '';
  position: absolute;
  width: 13px;
  height: 13px;
  border: 2px solid currentColor;
  opacity: 0.75;
}
.match-card::before { top: -1px; left: -1px; border-right: none; border-bottom: none; border-radius: 3px 0 0 0; }
.match-card::after { bottom: -1px; right: -1px; border-left: none; border-top: none; border-radius: 0 0 3px 0; }

.match-top {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
}

.match-symbol {
  color: var(--text);
  font-family: var(--mono);
  font-size: 1.35rem;
  font-weight: 600;
  letter-spacing: -0.01em;
}

.match-dir {
  font-family: var(--mono);
  font-size: 0.8rem;
  font-weight: 600;
  padding: 3px 9px;
  border-radius: 5px;
  white-space: nowrap;
}

.match-dir.long { background: var(--long-soft); color: var(--long); text-shadow: 0 0 12px rgba(52, 194, 125, 0.45); }
.match-dir.short { background: var(--short-soft); color: var(--short); text-shadow: 0 0 12px rgba(240, 85, 92, 0.45); }

.match-sub {
  margin-top: 8px;
  color: var(--text-dim);
  font-size: 0.8rem;
}

.match-verify {
  display: inline-block;
  margin-top: 10px;
  padding: 3px 9px;
  border-radius: 5px;
  background: var(--caution-soft);
  color: var(--caution);
  font-size: 0.72rem;
  font-weight: 500;
}

.empty-state {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 22px 22px;
  color: var(--text-dim);
  font-size: 0.9rem;
  line-height: 1.6;
}

.empty-state b { color: var(--text); }

/* ---- stats strip ---- */

.stats {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-bottom: 28px;
}

.stat {
  background: var(--panel);
  border: 1px solid var(--border-soft);
  border-radius: 8px;
  padding: 11px 16px;
  min-width: 96px;
}

.stat-value {
  font-family: var(--mono);
  font-size: 1.15rem;
  font-weight: 600;
}

.stat-label {
  color: var(--text-faint);
  font-size: 0.72rem;
  margin-top: 2px;
}

/* ---- table ---- */

.table-wrap {
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: auto;
  margin-bottom: 32px;
}

table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }

th {
  background: var(--panel-2);
  color: var(--text-dim);
  font-weight: 500;
  text-align: left;
  padding: 11px 14px;
  position: sticky;
  top: 0;
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}

th small { display: block; color: var(--text-faint); font-weight: 400; margin-top: 2px; }

td {
  padding: 11px 14px;
  border-top: 1px solid var(--border-soft);
  vertical-align: top;
  white-space: nowrap;
}

tbody tr:hover { background: var(--panel); }

.symbol { font-family: var(--mono); font-weight: 600; }

.mono { font-family: var(--mono); }

.dim { color: var(--text-dim); }

/* candle strip */

.candle {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  font-family: var(--mono);
  font-size: 0.72rem;
  color: var(--text-faint);
  margin-right: 8px;
}

.candle b { font-weight: 600; }
.candle.up b { color: var(--long); }
.candle.down b { color: var(--short); }
.candle.flat b { color: var(--text-faint); }
.candle-partial { text-decoration: underline dotted var(--caution); }

/* condition + result badges */

.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 5px;
  font-size: 0.72rem;
  font-weight: 500;
  font-family: var(--mono);
}

.badge.yes { background: var(--long-soft); color: var(--long); }
.badge.no { background: var(--short-soft); color: var(--short); }
.badge.na { background: var(--panel-2); color: var(--text-faint); }

.result {
  font-size: 0.78rem;
  font-weight: 600;
  font-family: var(--mono);
}

.result.pass { color: var(--long); }
.result.fail { color: var(--text-faint); }
.result.other { color: var(--caution); }

.detail-line {
  margin-top: 4px;
  color: var(--text-faint);
  font-size: 0.72rem;
  white-space: normal;
}

.verify-tag {
  display: inline-block;
  margin-top: 4px;
  padding: 1px 7px;
  border-radius: 4px;
  background: var(--caution-soft);
  color: var(--caution);
  font-size: 0.68rem;
}

/* ---- footer ---- */

footer {
  border-top: 1px solid var(--border-soft);
  padding-top: 22px;
  color: var(--text-dim);
  font-size: 0.82rem;
  line-height: 1.7;
}

footer h2 {
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--text);
  margin: 0 0 8px;
}

footer .section { margin-bottom: 18px; max-width: 720px; }
footer ol { margin: 6px 0 0; padding-left: 20px; }
footer ol li { margin-bottom: 4px; }

/* ---- mobile ---- */

@media (max-width: 640px) {
  body { padding: 24px 16px 48px; }
  h1 { font-size: 1.3rem; }
  .matches-grid { grid-template-columns: 1fr; }
  td, th { padding: 9px 10px; }
}
"""


def build_table_row(r):

    details = r.get("details")
    status = r.get("status", "UNKNOWN")

    status_map = {"PASS": "pass", "FAIL": "fail"}
    status_class = status_map.get(status, "other")
    status_label = {"NO_DATA": "No data"}.get(status, status.capitalize())

    strip = candle_strip(details, r.get("partial_groups"))

    def yesno(value):
        if value is None:
            return '<span class="badge na">&ndash;</span>'
        return (
            f'<span class="badge {"yes" if value else "no"}">'
            f'{"Yes" if value else "No"}</span>'
        )

    if details:
        d1524, d1527 = details["d1524"], details["d1527"]
        detail_1a = (
            f'{details["1524_vol"]:,.0f} vs {details["1527_vol"]:,.0f}'
        )
        detail_1b = f'{r.get("cond1b_matches", 0)} of 4 candles match'
        detail_2 = f'09:15 {trend_name(details["d0915"])}, 15:24 {trend_name(d1524)}'
    else:
        detail_1a = detail_1b = detail_2 = ""

    verify_html = ""
    if r.get("partial_3m"):
        groups = ", ".join(CANDLE_LABEL[g] for g in r.get("partial_groups", []))
        verify_html = f'<div class="verify-tag">Verify &mdash; partial: {esc(groups)}</div>'

    data_status_label = {
        "OK": "OK", "FALLBACK": "Fallback (7d)", "NO_DATA": "No data",
        "ERROR": "Error",
    }.get(r.get("data_status"), r.get("data_status"))

    data_bits = [cell(data_status_label)]

    if r.get("missing"):
        data_bits.append(
            '<div class="detail-line">missing: '
            + ", ".join(hm_text(h) for h in r["missing"])
            + "</div>"
        )

    if r.get("error"):
        data_bits.append(f'<div class="detail-line">{esc(r["error"][:120])}</div>')

    data_text = "".join(data_bits)

    return f"""
<tr>
<td class="symbol">{esc(r.get('symbol', ''))}</td>
<td class="mono dim">{cell(r.get('date'))}</td>
<td class="mono">{cell(r.get('direction'))}</td>
<td>{strip}</td>
<td>{yesno(r.get('cond1a'))}<div class="detail-line">{detail_1a}</div></td>
<td>{yesno(r.get('cond1b'))}<div class="detail-line">{detail_1b}</div></td>
<td>{yesno(r.get('cond2'))}<div class="detail-line">{detail_2}</div></td>
<td><span class="result {status_class}">{esc(status_label)}</span>{verify_html}</td>
<td class="mono dim">{data_text}</td>
</tr>
"""


# =============================================================================
# GENERATE HTML
# =============================================================================

def generate_html_report(results, elapsed, universe_source):

    def count(name):
        return sum(1 for r in results if r.get("status") == name)

    matches = [r for r in results if r.get("status") == "PASS"]

    signal_dates = [r["date"] for r in results if r.get("date")]
    prev_dates = [r["previous_day"] for r in results if r.get("previous_day")]
    entry_dates = [r["entry_day"] for r in results if r.get("entry_day")]
    signal_date = Counter(signal_dates).most_common(1)[0][0] if signal_dates else None
    previous_day = Counter(prev_dates).most_common(1)[0][0] if prev_dates else None
    entry_day = (
        Counter(entry_dates).most_common(1)[0][0] if entry_dates
        else pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")
    )

    # ---- matches (hero) ----

    if matches:
        cards = []
        for r in matches:
            direction = r["direction"]
            cls = "long" if direction == "LONG" else "short"
            verify = (
                '<div class="match-verify">Verify on TradingView &mdash; '
                'built from a partial 3-min candle</div>'
                if r.get("partial_3m") else ""
            )
            cards.append(f'''
<div class="match-card {cls}">
  <div class="match-top">
    <span class="match-symbol">{esc(r["symbol"])}</span>
    <span class="match-dir {cls}">{arrow(1 if direction=="LONG" else -1)} {esc(direction)}</span>
  </div>
  <div class="match-sub">Signal day {cell(r.get("date"))}</div>
  {verify}
</div>''')
        matches_html = (
            '<div class="matches-grid">' + "".join(cards) + '</div>'
        )
    else:
        matches_html = f'''
<div class="empty-state">
  <b>No symbols matched every condition.</b><br>
  {len(results):,} scanned against the day-before-previous-day conditions
  &mdash; none passed both.
</div>'''

    # ---- table ----

    sorted_results = sorted(
        results,
        key=lambda x: (x.get("status") != "PASS", x.get("symbol", "")),
    )

    table_rows = "".join(build_table_row(r) for r in sorted_results)

    warning = session_warning(results)
    warning_html = f'<div class="warn">{esc(warning)}</div>' if warning else ""

    scan_time = pd.Timestamp.now(tz="Asia/Kolkata").strftime(
        "%d %b %Y, %H:%M IST"
    )

    stats = [
        (len(results), "Scanned"),
        (len(matches), "Matches"),
        (count("FAIL"), "No match"),
        (count("INCOMPLETE"), "Incomplete"),
        (count("STALE"), "Stale"),
        (count("NO_DATA"), "No data"),
        (count("ERROR"), "Errors"),
        (f"{elapsed:.1f}s", "Scan time"),
    ]

    stats_html = "".join(
        f'<div class="stat"><div class="stat-value">{value}</div>'
        f'<div class="stat-label">{label}</div></div>'
        for value, label in stats
    )

    meta_items = [
        ("Generated", scan_time),
        ("Universe", universe_source),
        ("Day before previous (signal day)", cell(signal_date)),
        ("Previous day", cell(previous_day)),
        ("Entry", f"{cell(entry_day)}, 09:15 open"),
        ("Exit", f"{cell(entry_day)}, 15:27"),
    ]

    meta_html = "".join(
        f'<div class="meta-item"><div class="meta-label">{esc(label)}</div>'
        f'<div class="meta-value">{value}</div></div>'
        for label, value in meta_items
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
<div class="container">

<header>
  <h1>NSE Momentum Scanner</h1>
  <div class="status-chip"><span class="status-dot"></span>Scan complete</div>
  <div class="meta-row">{meta_html}</div>
</header>

{warning_html}

<section class="matches-section">
  <div class="scan-line"></div>
  <h2 class="matches-heading"><span class="glyph">&#9678;</span>Matches</h2>
  {matches_html}
</section>

<div class="stats">{stats_html}</div>

<div class="table-wrap">
<table>
<thead>
<tr>
<th>Symbol</th>
<th>Signal day</th>
<th>Direction</th>
<th>Candles <small>09:15 &middot; 15:15 &middot; 15:18 &middot; 15:21 &middot; 15:24 &middot; 15:27</small></th>
<th>15:24 vol &gt; 15:27 vol</th>
<th>Trend matches <small>&ge;2 of 4</small></th>
<th>09:15 = 15:24 trend</th>
<th>Result</th>
<th>Data</th>
</tr>
</thead>
<tbody>
{table_rows}
</tbody>
</table>
</div>

<footer>
  <div class="section">
    <h2>How this works</h2>
    Every condition is checked on the day before previous day &mdash; two
    trading days before entry &mdash; at the 3-minute timeframe.
    <ol>
      <li>The 15:24 candle's volume is greater than the 15:27 candle's
      volume, and at least two of the four candles at 15:15, 15:18, 15:21
      and 15:27 share the 15:24 candle's trend.</li>
      <li>The 9:15 candle's trend matches the 15:24 candle's trend.</li>
    </ol>
    Direction follows the 15:24 candle: an up trend means long, a down
    trend means short. Entry is the next trading session's 09:15 open;
    exit is that same session's 15:27.
  </div>
  <div class="section">
    <h2>Data notes</h2>
    Yahoo has no native 3-minute interval, so each 3-minute candle here is
    built from three 1-minute bars. A minute with no trades simply doesn't
    appear in Yahoo's data &mdash; it's treated as zero volume with no
    trend, which is correct when that's genuinely what happened, but Yahoo
    can't tell that apart from a dropped bar. A "Verify" tag means at
    least one of the six candles was built from an incomplete set of
    1-minute bars &mdash; check it on TradingView before acting on it.
    Incomplete means no data at all in the required window on the signal
    day. Stale means the resolved signal day doesn't match most other
    symbols.
  </div>
</footer>

</div>
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
