# =============================================================================
# NSE DAILY MOMENTUM SCANNER - 15-MIN / 3-MIN / 1-MIN CLOSING-CANDLE VERSION
# =============================================================================
#
# Downloads NATIVE 15-minute and 1-minute candles (two downloads per symbol)
# from a free source that needs no API key:
#   * NSE's own charting feed (charting.nseindia.com) - the exchange's data
#     (default; see DATA_SOURCE), with
#   * Yahoo Finance as an automatic per-stock fallback (FALLBACK_TO_YAHOO).
# Neither has a 3-minute feed, so the 3-minute candles are built from the
# 1-minute candles.
# Universe: Nifty 500 CSV -> NSE equity list -> built-in fallback.
# Output: index.html (a plain dashboard)
#
# =============================================================================
# STRATEGY  (read on the previous day only)
# =============================================================================
#
# "Previous day" = the most recent COMPLETED trading session:
#   * run BEFORE the market opens, or on a weekend -> the last session
#     (e.g. Tuesday morning reads Monday, Monday morning reads Friday);
#   * run AFTER the market has closed (15:30 + DATA_READY_BUFFER_MINUTES) -> TODAY's
#     session is already complete and is used (Monday evening reads Monday, and
#     the signals are for Tuesday);
#   * run WHILE the market is open -> today is still in progress, so the last
#     completed session (yesterday) is used.
# The entry day is the next trading session after the one being read.
#
# Candles are labelled by their START time. NSE closes at 15:30, so:
#   15-minute : ... 15:00, 15:15       (15:15 is the last, 15:00 the second last)
#   3-minute  : ... 15:24, 15:27       (15:27 is the last, 15:24 the second last)
#               3-minute buckets are anchored to 09:15 (09:15, 09:18, ... 15:27),
#               the same grid TradingView and broker charts use.
#   1-minute  : ... 15:28, 15:29       (15:29 is the last, 15:28 the second last)
#
# ALL of these must hold:
#   1) 15m volume: the 15:00 candle's volume is HIGHER than the 15:15 candle's.
#   2) 3m volume:  the 15:24 candle's volume is HIGHER than the 15:27 candle's.
#   3) 1m volume:  the 15:28 candle's volume is HIGHER than the 15:29 candle's.
#   4) The second-last candles of all three timeframes (15m 15:00, 3m 15:24,
#      1m 15:28) have the SAME trend (the "aligned trend").
#   5) The morning candles (09:15) of all three timeframes (15m 09:15, 3m 09:15,
#      1m 09:15) have that SAME aligned trend too.
#
# No timeframe has an opposite-trend rule: the last two candles of each
# timeframe may have the same or opposite trends. Only their volume order
# matters (and the second-last candle's trend, for the alignment in 4).
#
# "Trend" = candle colour (close > open is Up, close < open is Down). A flat
# candle (close == open) has no trend, so it can never be "the same" as another.
#
# DIRECTION: the strategy does not say which way to trade, so it is set by
# DIRECTION_RULE below (default: trade WITH the aligned second-last candles'
# trend, i.e. the 15m 15:00 / 3m 15:24 / 1m 15:28 trend: Up = LONG, Down = SHORT).
# TRADE:     Entry = the 09:15 OPEN of the entry day, Exit = EXIT_TIME_LABEL.
#
# A candle with no trades is absent from the data. By default a missing
# candle can only BLOCK a match, never create one. Optionally
# MISSING_LAST_CANDLE_IS_ZERO_VOLUME treats a missing LAST 3m/1m candle as
# volume 0 (see CONFIGURATION).
#
# DATA-ACCURACY SAFEGUARDS
#   * Prices are rounded to 2 decimals, so Yahoo's float noise can never turn a
#     flat candle into an Up/Down one.
#   * Zero-volume candles and corrupt candles (price <= 0, close/open outside
#     the high-low range) are dropped and treated as MISSING.
#   * Cross-check: the native 15m 15:00 and 15:15 candles are rebuilt from the
#     1m candles inside them (volume = sum of the 1m volumes). If the two Yahoo
#     feeds disagree on either candle's trend, or on which of the two 15m
#     candles has more volume, the symbol is marked DATA CONFLICT and never
#     produces a signal. (The 3m candles come from the 1m feed, so they have no
#     independent second source.)
#   * Every row in the report shows each candle's open -> close and volume, so
#     any signal can be checked against a chart in seconds.
#   * Single-stock audit:  python scanner.py --check RELIANCE TCS
#   * Source test:         python scanner.py --compare RELIANCE   (NSE vs Yahoo)
#
# INSTALL:
#   python -m pip install pandas requests
#   (plus: yfinance curl_cffi - only needed for Yahoo / the Yahoo fallback)
#
# =============================================================================

import html
import json
import logging
import random
import sys
import threading
import time
import warnings

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

import pandas as pd
import requests

warnings.filterwarnings("ignore")

# yfinance is only needed for DATA_SOURCE = "YAHOO" or the Yahoo fallback.
try:
    import yfinance as yf
except ImportError:
    yf = None

# yfinance logs every failed ticker; we report those ourselves.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Yahoo throttles heavy parallel use. Each symbol needs TWO downloads (15m and
# 1m; the 1-minute download is the heaviest). If you see many rate-limit ERRORs, lower this to 4-6.
MAX_WORKERS = 8

# Attempts per request (only rate limits / network errors are retried).
MAX_RETRIES = 3

# Native candles. Yahoo keeps about 60 days of 15-minute history but only about
# 7-8 days of 1-minute history, so the 1-minute fallback window must not
# exceed "7d" (a longer one is rejected).
INITIAL_PERIOD = {"15m": "5d", "1m": "5d"}

FALLBACK_PERIOD = {"15m": "1mo", "1m": "7d"}

REQUEST_TIMEOUT = 20

# If fewer than this share of scanned symbols have all nine required candles,
# the report shows a "data looks incomplete" warning. Kept low because
# illiquid stocks often have a no-trade candle; when Yahoo's data is genuinely
# broken the share is close to 0%.
SESSION_COMPLETE_SHARE = 0.20

# Cross-check the native 15m candles against the 1m candles inside them (see
# header). Keep True for accuracy; set False only to diagnose.
CROSS_CHECK_FEEDS = True

# If more than this share of scanned symbols show a 15m/1m conflict, the report
# warns that Yahoo's data is inconsistent today.
CONFLICT_WARN_SHARE = 0.10

# A candle with no trades is absent from the data. For the volume rules
# ("second-last volume > last volume"), a LAST candle with no trades has volume
# 0, so:
#   False -> a missing last candle BLOCKS the match (default, strictest)
#   True  -> a missing last candle counts as volume 0: the rule passes whenever
#            the second-last candle traded. Many more stocks qualify, but it
#            admits thin stocks that did not trade in the final minute (1m
#            15:29), the final 3 minutes (3m 15:27) or the last 15 minutes
#            (15m 15:15). The second-last candles must always exist: their
#            trends are needed for the alignment.
MISSING_LAST_CANDLE_IS_ZERO_VOLUME = False

# Where the candles come from (both are free and need no API key):
#   "NSE"   -> NSE's own charting feed (charting.nseindia.com): the exchange's
#              data. Unofficial, so NSE may rate-limit or block it (cloud
#              servers such as GitHub Actions are often blocked).
#   "YAHOO" -> Yahoo Finance via yfinance.
DATA_SOURCE = "NSE"

# If a stock cannot be fetched from NSE (blocked, odd format, no data), try
# Yahoo for that stock. The Data column of the report shows which source each
# stock came from. Set False to use NSE only (yfinance is then not needed).
FALLBACK_TO_YAHOO = True

# Days of history requested from NSE (only the last session is needed).
NSE_HISTORY_DAYS = 8

# Stock -> NSE chart token, saved between runs so each symbol is looked up once.
NSE_TOKEN_CACHE_FILE = "nse_token_cache.json"

# NSE answers a burst of requests with HTTP 403/429, so requests are spaced out
# and only a few run at once, however many worker threads there are:
NSE_MAX_CONNECTIONS = 3

# Minimum gap (seconds) between any two NSE requests, all threads together.
NSE_MIN_INTERVAL = 0.4

# If NSE refuses this many requests in a row, NSE is PAUSED: every worker waits
# NSE_COOLDOWN_SECONDS and then carries on. Each further pause without a success
# in between doubles the wait (up to NSE_COOLDOWN_MAX). After NSE_MAX_COOLDOWNS
# such pauses in a row with no success in between, NSE is given up for the rest
# of the run and the remaining stocks come from Yahoo.
NSE_REFUSALS_BEFORE_COOLDOWN = 4
NSE_COOLDOWN_SECONDS = 45
NSE_COOLDOWN_MAX = 600
NSE_MAX_COOLDOWNS = 4

# If NSE has served NOTHING usable after this many stocks, stop trying it for
# the rest of the run (it is not working from this network / the API changed).
NSE_GIVE_UP_AFTER = 40

# Which way to trade when a match fires. The second-last candles (15m 15:00,
# 3m 15:24, 1m 15:28) always share one trend - the "alignment trend" - so there
# are only two choices:
#   "FOLLOW_ALIGNED" -> trade WITH the alignment trend (default; Up = LONG,
#                       Down = SHORT)
#   "FADE_ALIGNED"   -> trade AGAINST it (Up = SHORT, Down = LONG)
DIRECTION_RULES = {
    "FOLLOW_ALIGNED": 1,
    "FADE_ALIGNED": -1,
}

DIRECTION_RULE = "FOLLOW_ALIGNED"

# Exit time shown in the report.
EXIT_TIME_LABEL = "15:27"

# NSE session times in minutes after midnight (IST).
MARKET_OPEN_MINUTES = 9 * 60 + 15
MARKET_CLOSE_MINUTES = 15 * 60 + 30

# After the 15:30 close, Yahoo needs a few minutes to publish the final
# candles. Today's session is only treated as complete this many minutes after
# the close (default 15:40); before that the scanner still reads yesterday.
DATA_READY_BUFFER_MINUTES = 10


# =============================================================================
# UNIVERSE SIZE  <-- change this to scan more / fewer stocks
# =============================================================================
#
#   "NIFTY500"      ~500 stocks   (large + mid + small caps)
#   "TOTAL_MARKET"  ~750 stocks   (Nifty 500 + Nifty Microcap 250)
#   "ALL_NSE"       ~1800+ stocks (every EQ-series stock on NSE)  [default]
#
# If the chosen list cannot be downloaded, the scanner falls back to the next
# smaller list automatically (see get_stock_universe); the report header shows
# which list was actually used. A bigger universe takes longer (each stock needs
# a few requests) but the stock -> NSE token lookups are cached between runs.
#
UNIVERSE_MODE = "ALL_NSE"


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
NSE_EQUITY_URLS = [
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://nsearchives.nseindia.com/content/equities/sec_list.csv",
]


# =============================================================================
# REQUIRED CANDLES
# =============================================================================

# The nine candles the strategy reads on the previous day: (timeframe, start).
# 15m and 1m come straight from the data source; 3m is built from the 1m candles.
SLOTS = [
    ("15m", 915), ("3m", 915), ("1m", 915),
    ("15m", 1500), ("15m", 1515),
    ("3m", 1524), ("3m", 1527),
    ("1m", 1528), ("1m", 1529),
]

SLOT_KEYS = [f"{tf}_{hm:04d}" for tf, hm in SLOTS]

# Timeframes DOWNLOADED for every symbol (3m is built from 1m, not downloaded).
TIMEFRAMES = ("15m", "1m")


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
    """Every EQ-series stock on NSE, from NSE's official equity list."""

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

    candidate = None

    for url in NSE_EQUITY_URLS:

        try:
            response = session.get(url, timeout=30)
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

            if series_column is None:
                # Keep it as a last resort, but try the next list first: one
                # with a SERIES column lets non-EQ (SME etc.) stocks be dropped.
                if candidate is None:
                    candidate = normalize_symbols(df[symbol_column].dropna().tolist())

                raise ValueError("no SERIES column, so non-EQ stocks cannot be filtered out")

            df = df[
                df[series_column].astype(str).str.strip().str.upper() == "EQ"
            ]

            symbols = normalize_symbols(df[symbol_column].dropna().tolist())

            if len(symbols) < 300:
                raise ValueError(f"Only {len(symbols)} EQ symbols returned")

            print(f"NSE official equity list loaded: {len(symbols)} EQ symbols")
            return symbols

        except Exception as e:
            print(f"NSE equity list unavailable ({url.rsplit('/', 1)[-1]}): {e}")

    if candidate and len(candidate) >= 300:
        print(
            f"Using the equity list without a SERIES filter: {len(candidate)} symbols "
            f"(may include SME / non-EQ stocks, which are skipped during the scan)"
        )
        return candidate

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
    # High/Low are kept for completeness; fall back to open/close for any
    # feed that happens to omit them so nothing downstream breaks.
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

    # Regular NSE session only (09:15 - 15:29; the last 1-minute candle is 15:29).
    out = out[out["hm"].between(915, 1529)]

    out = out.dropna(subset=["open", "high", "low", "close", "volume"])

    # Yahoo prices carry float32 noise (e.g. 2850.800048828125). NSE ticks are
    # 0.01 or larger, so 2 decimals keep every real price and remove the noise.
    for column in ("open", "high", "low", "close"):
        out[column] = out[column].round(2)

    # Drop candles that cannot be trusted; they then count as MISSING, which
    # can only block a signal, never create one:
    #   - volume 0: nothing traded, so there is no real candle;
    #   - a price <= 0, or open/close outside the high-low range: corrupt data.
    tolerance = 0.011
    body_high = out[["open", "close"]].max(axis=1)
    body_low = out[["open", "close"]].min(axis=1)

    sane = (
        (out["volume"] > 0)
        & (out[["open", "high", "low", "close"]].min(axis=1) > 0)
        & (out["high"] + tolerance >= body_high)
        & (out["low"] - tolerance <= body_low)
        & (out["high"] + tolerance >= out["low"])
    )

    out = out[sane]

    out = out.drop_duplicates(subset=["date", "hm"], keep="last")

    if out.empty:
        return None

    return out


# =============================================================================
# NSE CHARTING SOURCE   (free, no API key)
# =============================================================================
#
# NSE's own chart site serves candles through two JSON endpoints, called with
# plain GET requests (the symbol and range go in the URL query string):
#   1) symbolsDynamic       : symbol -> scripcode (token)
#   2) symbolHistoricalData : token + time range + interval -> candles
# (An older method, POSTing a JSON body - used by the `openchart` package - no
# longer works: NSE ignores the body and returns a default list of indices.)
# It is unofficial and undocumented, so it can change or be blocked. Any
# failure for a stock falls back to Yahoo when FALLBACK_TO_YAHOO is True.
#
# The time format of NSE's candles is not documented (and NSE's own 15-minute
# candles turned out to be stamped on a different grid than the 09:15 one the
# strategy needs), so:
#   * only 1-minute candles are downloaded; the 15-minute and 3-minute candles
#     are built from them on the 09:15 grid;
#   * each 1-minute stamp is floored to its minute, and before a scan starts the
#     scanner CHECKS those minutes against Yahoo's 1-minute candles for a liquid
#     stock (matching close price AND volume). If NSE's stamps are offset by a
#     constant number of minutes, that offset is found and corrected; if they
#     cannot be matched with confidence, NSE is not used at all and every stock
#     comes from Yahoo.

NSE_SEARCH_URL = "https://charting.nseindia.com/v1/exchanges/symbolsDynamic"
NSE_HISTORY_URL = "https://charting.nseindia.com/v1/charts/symbolHistoricalData"
NSE_HOME_URL = "https://www.nseindia.com"

# NSE is asked for 1-minute candles only. The 15-minute (and 3-minute) candles
# are built from them on the 09:15 grid, because NSE's own 15-minute candles
# are stamped on a different grid than the one the strategy needs.
NSE_DERIVED_MINUTES = {"15m": 15}

# Liquid stocks used (in order) for the one-time check of NSE's candle times
# against Yahoo's 1-minute candles.
NSE_REFERENCE_SYMBOLS = ("RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "SBIN")

NSE_CHART_HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.5",
    "Authority": "charting.nseindia.com",
    "Referer": "https://charting.nseindia.com/",
}

NSE_PAGE_HEADERS = {
    "User-Agent": NSE_CHART_HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.5",
}

_nse_local = threading.local()
_nse_lock = threading.Lock()
_nse_slots = threading.BoundedSemaphore(NSE_MAX_CONNECTIONS)
_nse_tokens = {}
_nse_state = {
    "failures": 0, "blocked": False, "attempts": 0, "successes": 0,
    "pause_until": 0.0, "cooldowns": 0, "cooldown_s": NSE_COOLDOWN_SECONDS,
    "pauses": 0, "paused_seconds": 0.0,
}
_nse_rate_lock = threading.Lock()
_nse_next = [0.0]
_nse_errors = Counter()
_nse_examples = {}
_nse_misses = {}
_nse_convention = {"labels": None}
_nse_minute_cache = {}
_nse_calibration = {"done": False, "ok": None, "shift": 0, "detail": ""}


class NseNoData(Exception):
    """NSE answered, but has no usable data for this stock."""


def is_no_data(info):
    """True for "NO_DATA" and "NO_DATA: <detail>" (a stock with no data, as
    opposed to a real error)."""

    return str(info).startswith("NO_DATA")


def snippet(value, limit=200):
    """A short, single-line preview of a reply for error messages."""

    try:
        text = value if isinstance(value, str) else json.dumps(value, default=str)
    except Exception:
        text = repr(value)

    text = " ".join(str(text).split())

    return text if len(text) <= limit else text[:limit] + "..."


def nse_throttle():
    """Space NSE requests at least NSE_MIN_INTERVAL seconds apart, across all
    worker threads."""

    with _nse_rate_lock:
        now = time.monotonic()
        wait = _nse_next[0] - now
        _nse_next[0] = max(now, _nse_next[0]) + NSE_MIN_INTERVAL

    if wait > 0:
        time.sleep(wait)


def nse_session(refresh=False):
    """One requests session per worker thread, holding the cookies NSE sets on
    its main site and on the charting site."""

    session = getattr(_nse_local, "session", None)

    if session is None or refresh:
        session = requests.Session()
        session.headers.update(NSE_CHART_HEADERS)

        for page in (NSE_HOME_URL + "/", "https://charting.nseindia.com/"):
            try:
                nse_throttle()
                session.get(page, headers=NSE_PAGE_HEADERS, timeout=REQUEST_TIMEOUT)
            except Exception:
                pass

        _nse_local.session = session

    return session


def nse_get(url, params):
    """GET a JSON reply from NSE's chart API (parameters go in the query
    string). Requests are throttled; rate limits and blocks are retried after a
    wait; raises RuntimeError with a readable reason if every attempt fails."""

    last_error = "NSE request failed"
    refresh = False

    for attempt in range(MAX_RETRIES):

        try:
            nse_throttle()

            with _nse_slots:
                response = nse_session(refresh).get(
                    url, params=params, timeout=REQUEST_TIMEOUT
                )

            refresh = False
            status = response.status_code

            if status in (401, 403):
                last_error = (
                    f"NSE refused the request (HTTP {status}); "
                    f"NSE is probably rate-limiting this network"
                )
                # One fresh session at most per call: opening many new sessions
                # while NSE is refusing us only makes the block worse.
                refresh = attempt == 0
                time.sleep(3.0 * (attempt + 1))
            elif status == 429:
                last_error = "NSE rate limit (HTTP 429)"
                time.sleep(4.0 * (2 ** attempt))
            elif status >= 400:
                last_error = f"NSE HTTP {status}"
            else:
                try:
                    return response.json()
                except ValueError:
                    last_error = "NSE returned a non-JSON reply (probably a block page)"
                    refresh = attempt == 0

        except requests.RequestException as e:
            last_error = f"NSE network error: {type(e).__name__}"
            refresh = attempt == 0

        if attempt < MAX_RETRIES - 1:
            time.sleep(1.0 * (2 ** attempt) + random.uniform(0.2, 1.0))

    raise RuntimeError(last_error)


def nse_lookup(symbol):
    """(token, chart_symbol, symbol_type) for an equity. Raises NseNoData with a
    reason if NSE has no exact match (a stock is never guessed from a partial
    match) and RuntimeError if NSE's reply is not understood."""

    with _nse_lock:
        if symbol in _nse_tokens and _nse_tokens[symbol]:
            return _nse_tokens[symbol]

        if symbol in _nse_misses:
            raise NseNoData(_nse_misses[symbol])

    # segment is left empty: NSE then searches every segment, and the exact
    # symbol match below picks the equity.
    result = nse_get(NSE_SEARCH_URL, {"symbol": symbol, "segment": ""})

    if isinstance(result, list):
        items = result
    elif isinstance(result, dict) and isinstance(result.get("data"), list):
        items = result["data"]
    elif isinstance(result, dict) and result.get("status") is False:
        detail = f"NSE symbol search said status=false | e.g. {symbol}: {snippet(result)}"

        with _nse_lock:
            _nse_misses[symbol] = detail

        raise NseNoData(detail)
    else:
        raise RuntimeError(
            f"NSE symbol search gave an unexpected reply | e.g. {symbol}: {snippet(result)}"
        )

    if not items:
        detail = f"NSE symbol search returned no results | e.g. {symbol}"

        with _nse_lock:
            _nse_misses[symbol] = detail

        raise NseNoData(detail)

    wanted = {symbol.upper(), f"{symbol.upper()}-EQ"}
    matches = [
        i for i in items
        if isinstance(i, dict) and str(i.get("symbol", "")).upper() in wanted
    ]

    # Prefer a real equity over anything else that happens to share the name.
    matches.sort(key=lambda i: str(i.get("type", "")).lower() != "equity")

    if matches:
        item = matches[0]

        try:
            found = (
                str(item["scripcode"]), str(item["symbol"]),
                str(item.get("type") or "Equity"),
            )
        except KeyError as e:
            raise RuntimeError(
                f"NSE search result lacks field {e} | e.g. {symbol}: {snippet(item)}"
            )

        with _nse_lock:
            _nse_tokens[symbol] = found

        return found

    other_series = [
        str(i.get("symbol")) for i in items
        if isinstance(i, dict)
        and str(i.get("symbol", "")).upper().startswith(symbol.upper() + "-")
        and not str(i.get("symbol", "")).upper().endswith("-EQ")
    ]

    if other_series:
        detail = (
            f"not an EQ-series stock (NSE lists it as {other_series[:2]}) "
            f"| e.g. {symbol}"
        )

        with _nse_lock:
            _nse_misses[symbol] = detail

        raise NseNoData(detail)

    first = items[0] if isinstance(items[0], dict) else {}
    names = [str(i.get("symbol")) for i in items[:4] if isinstance(i, dict)]
    detail = (
        f"NSE symbol search returned results but no exact match | e.g. "
        f"{symbol} -> {names}, fields {sorted(first)[:8]}"
    )

    with _nse_lock:
        _nse_misses[symbol] = detail

    raise NseNoData(detail)


def nse_candles_to_frame(data, shift=0):
    """NSE 1-minute candles -> a Yahoo-style frame (Datetime index = the minute
    the candle STARTS, in IST; Open/High/Low/Close/Volume) that clean_yahoo_data
    understands. `shift` (minutes) is the correction found by the alignment
    check against Yahoo. Raises ValueError when the times cannot be read."""

    df = pd.DataFrame(data)

    needed = ("time", "open", "high", "low", "close", "volume")
    absent = [c for c in needed if c not in df.columns]

    if absent:
        raise ValueError(f"unexpected NSE candle columns (missing {absent})")

    raw = pd.to_numeric(df["time"], errors="coerce")
    keep = raw.notna()
    df = df.loc[keep].reset_index(drop=True)
    raw = raw.loc[keep].reset_index(drop=True)

    if raw.empty:
        return None

    unit = "ms" if raw.abs().median() > 1e11 else "s"
    stamp = pd.to_datetime(raw, unit=unit)

    def where():
        return (
            f"raw times {raw.head(3).tolist()} ... {raw.tail(1).tolist()}, "
            f"unit {unit}, first {stamp.iloc[0]}, last {stamp.iloc[-1]}"
        )

    def in_session(values):
        minute_of_day = values.dt.hour * 60 + values.dt.minute
        return ((minute_of_day >= 9 * 60) & (minute_of_day <= 15 * 60 + 35)).mean()

    # India wall-clock (stored as if it were UTC) or real UTC?
    shifted = stamp + pd.Timedelta(hours=5, minutes=30)

    if in_session(shifted) > in_session(stamp) and in_session(shifted) >= 0.9:
        stamp = shifted
    elif in_session(stamp) < 0.9:
        raise ValueError(
            f"NSE candle times do not fall inside trading hours | e.g. {where()}"
        )

    boundary = stamp.dt.floor("min")

    # Stamps on an exact minute may mark a candle's START (09:15 ... 15:29) or
    # its END (09:16 ... 15:30): the day's first/last stamp tells which.
    if (stamp.dt.second == 0).mean() >= 0.99:
        minute_of_day = boundary.dt.hour * 60 + boundary.dt.minute
        opens = int((minute_of_day == MARKET_OPEN_MINUTES).sum())
        closes = int((minute_of_day == MARKET_CLOSE_MINUTES).sum())

        if opens > 0 and closes == 0:
            labels = "start"
        elif closes > 0 and opens == 0:
            labels = "end"
        else:
            labels = None

        with _nse_lock:
            known = _nse_convention["labels"]

            if labels is None:
                labels = known
            elif known is not None and known != labels:
                raise ValueError(
                    f"NSE candle times look like candle {labels.upper()} times, "
                    f"which contradicts the {known.upper()} times found earlier "
                    f"| e.g. {where()}"
                )
            else:
                _nse_convention["labels"] = labels

        if labels is None:
            raise ValueError(
                f"cannot tell whether NSE candle times mark the START or END of a "
                f"candle (stamped 09:15: {opens}, stamped 15:30: {closes}) "
                f"| e.g. {where()}"
            )

        if labels == "end":
            boundary = boundary - pd.Timedelta(minutes=1)

    start = boundary + pd.Timedelta(minutes=shift)

    out = pd.DataFrame({
        "Open": pd.to_numeric(df["open"], errors="coerce"),
        "High": pd.to_numeric(df["high"], errors="coerce"),
        "Low": pd.to_numeric(df["low"], errors="coerce"),
        "Close": pd.to_numeric(df["close"], errors="coerce"),
        "Volume": pd.to_numeric(df["volume"], errors="coerce"),
    })

    index = pd.DatetimeIndex(start).tz_localize("Asia/Kolkata")
    index.name = "Datetime"
    out.index = index

    return out


def nse_fetch_minutes(symbol, shift):
    """1-minute candles for one stock from NSE: (rows, info) like
    fetch_interval. rows is a cleaned DataFrame (start-labelled minutes), or
    None with info "NO_DATA: <reason>" (NSE answered but has nothing for this
    stock) or an error message. Every failure carries a specific reason."""

    try:
        token, chart_symbol, symbol_type = nse_lookup(symbol)

        now = int(time.time())

        result = nse_get(NSE_HISTORY_URL, {
            "token": token,
            "fromDate": now - NSE_HISTORY_DAYS * 86400,
            "toDate": now,
            "symbol": chart_symbol,
            "symbolType": symbol_type,
            "chartType": "I",
            "timeInterval": 1,
        })

        if isinstance(result, list):
            data, rest = result, {}
        elif isinstance(result, dict):
            data = result.get("data")
            rest = {k: v for k, v in result.items() if k != "data"}
        else:
            raise RuntimeError(
                f"NSE history gave an unexpected reply | e.g. {symbol}: {snippet(result)}"
            )

        if not isinstance(data, list) or not data or rest.get("status") is False:
            raise NseNoData(
                f"NSE history returned no candles | e.g. {symbol}: {snippet(rest)}"
            )

        frame = nse_candles_to_frame(data, shift)

        if frame is None:
            raise NseNoData(f"NSE history had no usable timestamps | e.g. {symbol}")

        cleaned = clean_yahoo_data(frame)

        if cleaned is None or cleaned.empty:
            index = frame.index
            minute_of_day = index.hour * 60 + index.minute
            outside = int(((minute_of_day < 555) | (minute_of_day > 929)).sum())
            zero_volume = int((frame["Volume"].fillna(0) <= 0).sum())
            raise RuntimeError(
                f"all NSE candles were dropped by cleaning | e.g. {symbol}: "
                f"{len(frame)} candles, {zero_volume} with zero volume, "
                f"{outside} outside 09:15-15:29, first start "
                f"{index[0].strftime('%Y-%m-%d %H:%M')}, last start "
                f"{index[-1].strftime('%Y-%m-%d %H:%M')}"
            )

        return cleaned, "OK"

    except NseNoData as e:
        return None, f"NO_DATA: {e}"[:300]

    except Exception as e:
        return None, f"{type(e).__name__}: {e}"[:300]


def fetch_interval_nse(symbol, interval):
    """One timeframe for one stock from NSE: (rows, info) like fetch_interval.
    "1m" is NSE's own 1-minute data; "15m" is built from those same 1-minute
    candles on the 09:15 grid. The 1-minute download is shared between the two
    calls for a stock (one history request per stock)."""

    with _nse_lock:
        rows = _nse_minute_cache.pop(symbol, None)

    if rows is None:
        rows, info = nse_fetch_minutes(symbol, _nse_calibration["shift"])

        if rows is None:
            return None, info

        with _nse_lock:
            _nse_minute_cache[symbol] = rows

    if interval == "1m":
        return rows, "OK"

    return resample_from_1m(rows, NSE_DERIVED_MINUTES[interval]), "OK"


def load_nse_token_cache():
    try:
        with open(NSE_TOKEN_CACHE_FILE, encoding="utf-8") as file:
            saved = json.load(file)

        with _nse_lock:
            for symbol, value in saved.items():
                if isinstance(value, list) and len(value) == 3:
                    _nse_tokens[symbol] = tuple(value)
    except Exception:
        pass


def save_nse_token_cache():
    with _nse_lock:
        saved = {k: list(v) for k, v in _nse_tokens.items() if v}

    if not saved:
        return

    try:
        with open(NSE_TOKEN_CACHE_FILE, "w", encoding="utf-8") as file:
            json.dump(saved, file)
    except Exception:
        pass


def hm_add(hm, minutes):
    """HHMM plus a number of minutes (may be negative)."""

    total = (hm // 100) * 60 + hm % 100 + minutes

    return (total // 60) * 100 + total % 60


def nse_alignment(symbol):
    """Compare NSE's 1-minute candles with Yahoo's for one stock to find how
    NSE's candle minutes line up with the real minute. For each possible
    correction (-15 ... +15 minutes) it counts the NSE candles whose close AND
    volume match the Yahoo candle at that minute. Returns a dict with the best
    correction ("shift") and how clearly it beat the runner-up."""

    nse_rows, nse_info = nse_fetch_minutes(symbol, 0)

    if nse_rows is None:
        raise RuntimeError(f"NSE gave no usable data: {nse_info}")

    yahoo_rows, yahoo_info = fetch_interval(symbol, "1m")

    if yahoo_rows is None:
        raise RuntimeError(f"Yahoo gave no data to compare with: {yahoo_info}")

    def full_days(rows):
        counts = rows.groupby("date").size()
        return {d for d, n in counts.items() if n >= 300}

    days = sorted(full_days(nse_rows) & full_days(yahoo_rows))

    if not days:
        raise RuntimeError("no complete session in both NSE and Yahoo data to compare")

    days = days[-2:]

    yahoo = {
        (d, int(h)): (float(c), float(v))
        for d, h, c, v in zip(
            yahoo_rows["date"], yahoo_rows["hm"], yahoo_rows["close"], yahoo_rows["volume"]
        )
        if d in days
    }

    chosen = nse_rows[nse_rows["date"].isin(days)]
    items = [
        (d, int(h), float(c), float(v))
        for d, h, c, v in zip(
            chosen["date"], chosen["hm"], chosen["close"], chosen["volume"]
        )
    ]

    scores = {}

    for k in range(-15, 16):
        hits = 0

        for d, h, c, v in items:
            other = yahoo.get((d, hm_add(h, k)))

            if (
                other is not None
                and abs(other[0] - c) <= 0.011
                and abs(other[1] - v) <= 0.25 * max(v, other[1], 1.0)
            ):
                hits += 1

        scores[k] = hits / len(items)

    ranked = sorted(scores.items(), key=lambda kv: -kv[1])

    return {
        "symbol": symbol,
        "shift": ranked[0][0],
        "share": ranked[0][1],
        "runner_up": ranked[1][1],
        "top": ranked[:4],
        "days": days,
        "candles": len(items),
    }


def nse_alignment_ok(result):
    return result["share"] >= 0.5 and result["share"] >= 1.5 * result["runner_up"]


def nse_preflight():
    """One-time check, before a scan, that NSE's candle minutes line up with
    Yahoo's (see nse_alignment). Sets the correction to apply, or marks NSE as
    unusable (every stock then comes from Yahoo)."""

    if DATA_SOURCE != "NSE" or _nse_calibration["done"]:
        return

    _nse_calibration["done"] = True
    problems = []

    for symbol in NSE_REFERENCE_SYMBOLS:
        try:
            result = nse_alignment(symbol)
        except Exception as e:
            problems.append(f"{symbol}: {e}"[:220])
            continue

        if nse_alignment_ok(result):
            _nse_calibration.update(
                ok=True,
                shift=result["shift"],
                detail=(
                    f"NSE candle times verified against Yahoo ({symbol}, "
                    f"{result['candles']} candles): correction {result['shift']:+d} min, "
                    f"{result['share']:.0%} of minutes match"
                ),
            )
            print()
            print("NSE CHECK:", _nse_calibration["detail"])
            return

        problems.append(
            f"{symbol}: best match only {result['share']:.0%} at {result['shift']:+d} min "
            f"(runner-up {result['runner_up']:.0%}; top {result['top']})"
        )

    detail = "; ".join(problems)[:500]

    _nse_calibration.update(
        ok=False,
        detail="NSE candle times could not be verified against Yahoo: " + detail,
    )

    with _nse_lock:
        _nse_state["blocked"] = True

    print()
    print("NSE CHECK FAILED - NSE will not be used in this run.")
    print("  " + detail)


def nse_note_failure(message):
    """Remember why NSE failed for a stock (shown in the source-health report).
    A message may carry an example after ' | e.g. '; the first example for each
    reason is kept."""

    text = str(message)

    if text.startswith("NO_DATA"):
        text = "no data: " + (text[len("NO_DATA"):].lstrip(": ") or "(no detail)")

    reason, _, example = text.partition(" | e.g. ")
    reason = reason[:140]

    with _nse_lock:
        _nse_errors[reason] += 1

        if example and reason not in _nse_examples:
            _nse_examples[reason] = example[:260]


def nse_wait_if_paused():
    """Block while NSE is cooling down after refusals (every worker waits)."""

    while True:
        with _nse_lock:
            remaining = _nse_state["pause_until"] - time.time()
            blocked = _nse_state["blocked"]

        if blocked or remaining <= 0:
            return

        time.sleep(min(remaining, 5.0))


def nse_record(outcome):
    """Rate-limit handling. outcome: "ok"; "soft" (NSE answered but had no data
    for that stock); "hard" (an error, e.g. HTTP 403). NSE_REFUSALS_BEFORE_COOLDOWN
    hard failures in a row pause NSE for a while; repeated pauses with no success
    in between give NSE up for the run. NSE is also given up if it has served
    nothing at all after NSE_GIVE_UP_AFTER stocks."""

    message = None

    with _nse_lock:
        state = _nse_state
        state["attempts"] += 1

        if outcome == "ok":
            state["successes"] += 1
            state["failures"] = 0
            state["cooldowns"] = 0
            state["cooldown_s"] = NSE_COOLDOWN_SECONDS
            return

        if state["blocked"]:
            return

        if outcome == "hard" and time.time() >= state["pause_until"]:
            state["failures"] += 1

            if state["failures"] >= NSE_REFUSALS_BEFORE_COOLDOWN:
                state["failures"] = 0
                state["cooldowns"] += 1

                if state["cooldowns"] > NSE_MAX_COOLDOWNS:
                    state["blocked"] = True
                    message = (
                        f"NSE kept refusing requests after {NSE_MAX_COOLDOWNS} pauses "
                        f"- not using NSE for the rest of this run."
                    )
                else:
                    pause = state["cooldown_s"]
                    state["cooldown_s"] = min(pause * 2, NSE_COOLDOWN_MAX)
                    state["pause_until"] = time.time() + pause
                    state["pauses"] += 1
                    state["paused_seconds"] += pause
                    message = (
                        f"NSE refused {NSE_REFUSALS_BEFORE_COOLDOWN} requests in a row "
                        f"(rate limit) - pausing NSE for {pause:.0f}s, then carrying on."
                    )

        if (
            message is None
            and state["successes"] == 0
            and state["attempts"] >= NSE_GIVE_UP_AFTER
        ):
            state["blocked"] = True
            message = (
                f"NSE gave nothing usable for the first {state['attempts']} stocks "
                f"- not using NSE for the rest of this run."
            )

    if message:
        print()
        print("NOTE:", message)


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


def yahoo_download(symbol, period, interval):
    """Returns (dataframe, None) on success, or (None, reason).
    reason == "NO_DATA" means Yahoo has nothing for this ticker;
    any other string is a real error message."""

    if yf is None:
        return None, "yfinance is not installed"

    ticker = yahoo_ticker(symbol)
    last_error = "NO_DATA"

    for attempt in range(MAX_RETRIES):

        try:
            df = yf.Ticker(ticker).history(
                period=period,
                interval=interval,
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


def completed_day_count(rows):
    """Number of distinct trading sessions strictly before today."""

    today = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")

    return len({d for d in rows["date"].unique() if d != today})


def fetch_interval(symbol, interval):
    """One timeframe, straight from Yahoo's own feed for that timeframe.
    Returns (rows, info). If rows is None, info is "NO_DATA" or an error
    message. Otherwise info is "OK" or "FALLBACK" (the longer window was
    needed because the first one returned no completed session)."""

    rows, error = yahoo_download(symbol, INITIAL_PERIOD[interval], interval)

    if rows is not None and completed_day_count(rows) >= 1:
        return rows, "OK"

    # One retry with the fallback window, unless Yahoo is rate limiting us
    # (a second request would only be throttled again).
    rate_limited = bool(error) and any(w in str(error).lower() for w in RATE_LIMIT_WORDS)

    if rows is not None or not rate_limited:
        rows2, error2 = yahoo_download(symbol, FALLBACK_PERIOD[interval], interval)

        if rows2 is not None:
            return rows2, "FALLBACK"

        if rows is not None:
            return rows, "OK"          # evaluate_rows will report what is missing

        return None, error2

    return None, error


def fetch_all_timeframes(symbol, fetcher):
    """Both timeframes for one symbol from one source (fetcher). Returns
    (rows_by_timeframe, info); rows_by_timeframe is None unless every timeframe
    came back, and then info is "NO_DATA[: reason]" or an error message."""

    rows_by_tf, infos, errors = {}, [], []

    for tf in TIMEFRAMES:
        rows, info = fetcher(symbol, tf)

        if rows is None:
            errors.append(info)
        else:
            rows_by_tf[tf] = rows
            infos.append(info)

    if len(rows_by_tf) == len(TIMEFRAMES):
        return rows_by_tf, ("FALLBACK" if "FALLBACK" in infos else "OK")

    real_errors = [e for e in errors if not is_no_data(e)]

    if real_errors:
        return None, real_errors[0]

    return None, (errors[0] if errors else "NO_DATA")


def fetch_symbol_rows(symbol):
    """Returns (rows_by_timeframe, info, source): native 15m and 1m candles.
    Both timeframes always come from the SAME source. With DATA_SOURCE "NSE"
    the stock falls back to Yahoo as a whole if NSE fails and
    FALLBACK_TO_YAHOO is True. If rows_by_timeframe is None, info is "NO_DATA"
    or an error message. Otherwise info is "OK" or "FALLBACK" (a longer Yahoo
    window was needed)."""

    # Small random delay so the workers do not hit the source in one burst.
    time.sleep(random.uniform(0.05, 0.25))

    if DATA_SOURCE == "YAHOO":
        rows, info = fetch_all_timeframes(symbol, fetch_interval)
        return rows, info, "Yahoo"

    nse_wait_if_paused()

    if _nse_calibration["ok"] is False:
        nse_info = "NSE skipped: its candle times could not be verified against Yahoo"
    else:
        nse_info = "NSE skipped (given up for the rest of this run after repeated refusals)"

    if not _nse_state["blocked"]:
        rows, nse_info = fetch_all_timeframes(symbol, fetch_interval_nse)

        if rows is not None:
            nse_record("ok")
            return rows, nse_info, "NSE"

        nse_record("soft" if is_no_data(nse_info) else "hard")

    nse_note_failure(nse_info)

    # NSE lists it under another series (SME, trade-to-trade...): not a stock
    # the strategy covers, so there is nothing to fall back to.
    if str(nse_info).startswith("NO_DATA: not an EQ-series"):
        return None, "NO_DATA", "NSE"

    if not FALLBACK_TO_YAHOO:
        return None, ("NO_DATA" if is_no_data(nse_info) else nse_info), "NSE"

    rows, yahoo_info = fetch_all_timeframes(symbol, fetch_interval)

    if rows is not None:
        return rows, yahoo_info, "Yahoo (fallback)"

    if is_no_data(nse_info) and is_no_data(yahoo_info):
        return None, "NO_DATA", "NSE"

    return None, f"NSE: {nse_info} | Yahoo: {yahoo_info}"[:300], "NSE"


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


def entry_session_day(today_str):
    """First weekday on or after the given date (a weekend rolls forward to
    Monday). Exchange holidays are not modelled - there is no holiday
    calendar to check them against."""

    day = pd.Timestamp(today_str)

    while day.weekday() >= 5:
        day += pd.Timedelta(days=1)

    return day.strftime("%Y-%m-%d")


_RUN_CONTEXT = None


def build_run_context(now=None):
    """Decide ONCE per run which session is read and which day is the entry.

    today_complete is True on a weekday once the market has closed and Yahoo
    has had DATA_READY_BUFFER_MINUTES to publish the final candles. Then
    today's own session is the latest completed one, and the entry day is the
    next trading day. Otherwise today is excluded (not started, in progress, or
    not yet final) and today (or the next weekday) is the entry day.
    Returns a dict: today, today_complete, entry, note."""

    now = now if now is not None else pd.Timestamp.now(tz="Asia/Kolkata")
    today = now.strftime("%Y-%m-%d")
    minutes = now.hour * 60 + now.minute
    weekday = now.weekday() < 5
    ready_at = MARKET_CLOSE_MINUTES + DATA_READY_BUFFER_MINUTES

    today_complete = weekday and minutes >= ready_at

    if today_complete:
        tomorrow = (now + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        entry = entry_session_day(tomorrow)
        note = (
            f"The market has closed, so today's session ({fmt_day(today)}) is "
            f"used as the latest completed session. Entry is the next trading "
            f"day, {fmt_day(entry)}."
        )
    else:
        entry = entry_session_day(today)
        note = None

        if weekday and MARKET_OPEN_MINUTES <= minutes < MARKET_CLOSE_MINUTES:
            note = (
                "The market is open, so today's session is not complete yet. "
                "The last completed session is used, and today's 09:15 entry "
                "price has already passed."
            )
        elif weekday and MARKET_CLOSE_MINUTES <= minutes < ready_at:
            note = (
                "The market has just closed and the closing candles are not "
                "final yet, so the previous session is used. Run again after "
                f"{hm_text((ready_at // 60) * 100 + ready_at % 60)} to use today's session."
            )

    return {
        "today": today,
        "today_complete": today_complete,
        "entry": entry,
        "note": note,
    }


def get_run_context():
    """The run context, built once and shared by every worker thread so that
    all symbols read the same session even if the scan crosses the cutoff."""

    global _RUN_CONTEXT

    if _RUN_CONTEXT is None:
        _RUN_CONTEXT = build_run_context()

    return _RUN_CONTEXT


def day_map(rows):
    """DataFrame -> {date: {hm: (open, high, low, close, volume)}}"""

    by_date = {}

    for date, hm, o, h, l, c, v in zip(
        rows["date"], rows["hm"], rows["open"], rows["high"], rows["low"],
        rows["close"], rows["volume"]
    ):
        by_date.setdefault(date, {})[int(hm)] = (
            float(o), float(h), float(l), float(c), float(v)
        )

    return by_date


def slot_label(key):
    """'15m_1500' -> '15m 15:00' (used in missing notes)."""

    tf, hm = key.split("_")

    return f"{tf} {hm_text(int(hm))}"


def resample_from_1m(rows, minutes):
    """Build N-minute candles from 1-minute rows (no source has a 3-minute feed,
    and NSE's own 15-minute grid is not the 09:15 one the strategy needs).

    Buckets are anchored to the 09:15 open (for 3m: 09:15, 09:18, ... 15:24,
    15:27; for 15m: 09:15, 09:30, ... 15:00, 15:15). Within a bucket: open =
    first, high = max, low = min, close = last, volume = sum. A bucket exists if
    at least one of its 1-minute candles traded."""

    if rows is None or rows.empty:
        return rows

    work = rows.sort_values(["date", "hm"]).copy()

    minute_of_day = (work["hm"] // 100) * 60 + (work["hm"] % 100)
    start = (
        MARKET_OPEN_MINUTES
        + ((minute_of_day - MARKET_OPEN_MINUTES) // minutes) * minutes
    )
    work["bucket"] = (start // 60) * 100 + (start % 60)

    return (
        work.groupby(["date", "bucket"], sort=True)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )
        .reset_index()
        .rename(columns={"bucket": "hm"})
    )


def resample_to_3m(rows):
    """3-minute candles from 1-minute rows (see resample_from_1m)."""

    return resample_from_1m(rows, 3)


def window_hms(start_hm, minutes=15, step=1):
    """Start times of the step-minute candles inside the candle that starts at
    start_hm, e.g. 1515 -> [1515, 1516, ... 1529]."""

    start = (start_hm // 100) * 60 + start_hm % 100

    return [
        ((start + k) // 60) * 100 + (start + k) % 60
        for k in range(0, minutes, step)
    ]


def derive_15m_from_1m(day_1m, start_hm):
    """Rebuild the 15-minute candle starting at start_hm from the 1-minute
    candles inside it: open = first 1m open, close = last 1m close, volume =
    sum of the 1m volumes. A 1m candle exists only if it traded, so this equals
    Yahoo's real 15m candle. Returns None if no 1m candle exists in the window."""

    bars = [day_1m[h] for h in window_hms(start_hm) if h in day_1m]

    if not bars:
        return None

    return {
        "open": bars[0][0],
        "close": bars[-1][3],
        "volume": int(round(sum(b[4] for b in bars))),
    }


def evaluate_rows(rows_by_tf):
    """15m / 3m / 1m closing-candle strategy, read on the previous day
    (P = the latest completed session). Entry day = the next trading day.

    1) 15m 15:00 volume is higher than 15m 15:15 volume.
    2) 3m 15:24 volume is higher than 3m 15:27 volume.
    3) 1m 15:28 volume is higher than 1m 15:29 volume.
    4) 15m 15:00, 3m 15:24 and 1m 15:28 candles all have the same trend.
    5) 15m 09:15, 3m 09:15 and 1m 09:15 candles have that same trend too.
    (No timeframe requires its last two candles to have opposite trends.)

    Before a signal is allowed, the 15m candles are cross-checked against the
    1m candles inside them; a disagreement gives status CONFLICT, not a signal.
    """

    if not rows_by_tf:
        return {"status": "NO_DATA"}

    # Downloaded timeframes only (15m, 1m); the 3m candles are added below.
    by_tf = {tf: day_map(rows) for tf, rows in rows_by_tf.items()}

    # Which session is "previous" is decided once per run (see
    # build_run_context): today's session counts only once it is complete.
    ctx = get_run_context()
    entry = ctx["entry"]
    excluded = set() if ctx["today_complete"] else {ctx["today"]}
    dates = sorted({d for days in by_tf.values() for d in days} - excluded)

    if not dates:
        return {
            "status": "INCOMPLETE",
            "date": None,
            "previous_day": None,
            "entry_day": entry,
            "missing": [slot_label(k) for k in SLOT_KEYS],
            "note": "no completed trading day of data before today",
        }

    previous_day = dates[-1]

    # 3-minute candles are built from the previous day's 1-minute candles only.
    rows_1m = rows_by_tf.get("1m")
    by_tf["3m"] = {}

    if rows_1m is not None and not rows_1m.empty:
        rows_3m = resample_to_3m(rows_1m[rows_1m["date"] == previous_day])

        if rows_3m is not None and not rows_3m.empty:
            by_tf["3m"] = day_map(rows_3m)

    candles, missing = {}, []

    for tf, hm in SLOTS:
        key = f"{tf}_{hm:04d}"
        bar = by_tf.get(tf, {}).get(previous_day, {}).get(hm)

        if bar is None:
            candles[key] = None
            missing.append(slot_label(key))
        else:
            o, h, l, c, v = bar
            candles[key] = {"open": o, "close": c, "volume": int(round(v))}

    if len(missing) == len(SLOTS):
        return {
            "status": "INCOMPLETE",
            "date": previous_day,
            "previous_day": previous_day,
            "entry_day": entry,
            "missing": missing,
        }

    def trend(key):
        c = candles[key]
        return candle_direction(c["open"], c["close"]) if c else 0

    def volume(key):
        c = candles[key]
        return c["volume"] if c else None

    t15_00, t15_15 = trend("15m_1500"), trend("15m_1515")
    t3_24, t3_27 = trend("3m_1524"), trend("3m_1527")
    t1_28, t1_29 = trend("1m_1528"), trend("1m_1529")
    t15_0915, t3_0915, t1_0915 = trend("15m_0915"), trend("3m_0915"), trend("1m_0915")

    v15_00, v15_15 = volume("15m_1500"), volume("15m_1515")
    v3_24, v3_27 = volume("3m_1524"), volume("3m_1527")
    v1_28, v1_29 = volume("1m_1528"), volume("1m_1529")

    def higher(first, second):
        # The second-last candle must exist. A missing LAST candle has no
        # trades, i.e. volume 0: it passes only if the switch above is on.
        if first is None:
            return False

        if second is None:
            return MISSING_LAST_CANDLE_IS_ZERO_VOLUME

        return first > second

    # Volume only, on every timeframe. The two candles of a timeframe may have
    # the same or opposite trends.
    vol_15m = higher(v15_00, v15_15)
    vol_3m = higher(v3_24, v3_27)
    vol_1m = higher(v1_28, v1_29)

    cond_15m = vol_15m
    cond_3m = vol_3m
    cond_1m = vol_1m

    # Second-last candles of all three timeframes share one (non-flat) trend.
    cond_align = t15_00 != 0 and t15_00 == t3_24 == t1_28

    # The morning (09:15) candles of all three timeframes have that same trend.
    cond_morning = cond_align and t15_0915 == t3_0915 == t1_0915 == t15_00

    cond = cond_15m and cond_3m and cond_1m and cond_align and cond_morning

    # Cross-check the two Yahoo feeds against each other (15m vs the 1m
    # candles inside it).
    one_day = by_tf.get("1m", {}).get(previous_day, {})
    derived, conflicts = {}, []

    for hm in (915, 1500, 1515):
        key = f"15m_{hm:04d}"
        rebuilt = derive_15m_from_1m(one_day, hm)
        derived[key] = rebuilt
        native = candles[key]

        # The trends of the 09:15 and 15:00 candles matter (morning rule and
        # alignment); the 15:15 candle's trend is not used, so only its volume
        # is cross-checked below.
        if hm == 1515:
            continue

        if not CROSS_CHECK_FEEDS or native is None:
            continue

        if rebuilt is None:
            conflicts.append(
                f"15m {hm_text(hm)} exists but the 1m feed has no candle inside it"
            )
            continue

        native_trend = candle_direction(native["open"], native["close"])
        rebuilt_trend = candle_direction(rebuilt["open"], rebuilt["close"])

        if native_trend != rebuilt_trend:
            conflicts.append(
                f"15m {hm_text(hm)} is {trend_name(native_trend)} in the 15m feed "
                f"but {trend_name(rebuilt_trend)} when rebuilt from 1m candles"
            )

    # The two 15m candles' volume ordering must also agree between the feeds.
    rebuilt_00, rebuilt_15 = derived.get("15m_1500"), derived.get("15m_1515")

    if (
        CROSS_CHECK_FEEDS
        and v15_00 is not None and v15_15 is not None
        and rebuilt_00 is not None and rebuilt_15 is not None
    ):
        native_higher = v15_00 > v15_15
        rebuilt_higher = rebuilt_00["volume"] > rebuilt_15["volume"]

        if native_higher != rebuilt_higher:
            conflicts.append(
                f"15m volume: '15:00 higher than 15:15' is {native_higher} in the "
                f"15m feed but {rebuilt_higher} when rebuilt from 1m candles"
            )

    # Direction anchors on the alignment trend: when cond is True the 15m 15:00,
    # 3m 15:24 and 1m 15:28 candles all share it.
    side = t15_00 * DIRECTION_RULES[DIRECTION_RULE]
    direction = "LONG" if side == 1 else "SHORT" if side == -1 else None

    if conflicts:
        status, direction = "CONFLICT", None
    else:
        status = "PASS" if cond else "FAIL"

    return {
        "status": status,
        "date": previous_day,
        "previous_day": previous_day,
        "entry_day": entry,
        "direction": direction if cond else None,
        "cond": cond,
        "cond_15m": cond_15m,
        "cond_3m": cond_3m,
        "cond_1m": cond_1m,
        "cond_align": cond_align,
        "cond_morning": cond_morning,
        "vol_15m": vol_15m,
        "vol_3m": vol_3m,
        "vol_1m": vol_1m,
        "missing": missing,
        "conflicts": conflicts,
        "candles": candles,
        "derived": derived,
        "tail_1m": sorted(h for h in one_day if h >= 1520),
        "details": {
            "t15_00": t15_00, "t15_15": t15_15,
            "t3_24": t3_24, "t3_27": t3_27,
            "t1_28": t1_28, "t1_29": t1_29,
            "t15_0915": t15_0915, "t3_0915": t3_0915, "t1_0915": t1_0915,
        },
    }


# =============================================================================
# SCAN ONE SYMBOL
# =============================================================================

def scan_one_symbol(symbol):

    source = None

    try:
        rows, info, source = fetch_symbol_rows(symbol)

        if rows is None:

            if is_no_data(info):
                return {
                    "symbol": symbol,
                    "status": "NO_DATA",
                    "data_status": "NO_DATA",
                    "source": source,
                }

            return {
                "symbol": symbol,
                "status": "ERROR",
                "data_status": "ERROR",
                "error": info,
                "source": source,
            }

        result = evaluate_rows(rows)
        result["symbol"] = symbol
        result["data_status"] = info
        result["source"] = source

        return result

    except Exception as e:
        return {
            "symbol": symbol,
            "status": "ERROR",
            "data_status": "ERROR",
            "error": f"{type(e).__name__}: {e}"[:240],
            "source": source,
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
    """A symbol whose previous day differs from the one most symbols used (a
    suspended stock, a missing day) cannot be compared fairly, so it is
    marked STALE instead of producing a signal. Returns the reference day."""

    evaluated = [r for r in results if r.get("status") in ("PASS", "FAIL")]
    days = [r["previous_day"] for r in evaluated if r.get("previous_day")]

    if not days:
        return None

    reference = Counter(days).most_common(1)[0][0]

    for r in evaluated:
        if r.get("previous_day") != reference:
            r["status"] = "STALE"
            r["direction"] = None

    return reference


def session_warning(results):
    """Warn about broad Yahoo data-quality problems: many symbols missing one
    of the 9 required candles, or many 15m/1m conflicts. Returns text or None."""

    evaluated = [
        r for r in results if r.get("status") in ("PASS", "FAIL", "CONFLICT")
    ]

    if not evaluated:
        return None

    messages = []

    complete = sum(1 for r in evaluated if not r.get("missing"))
    share = complete / len(evaluated)

    if share < SESSION_COMPLETE_SHARE:
        messages.append(
            f"Only {share:.0%} of scanned symbols have all nine required candles "
            f"(09:15 on 15m, 3m and 1m; 15m 15:00 and 15:15; 3m 15:24 and 15:27; "
            f"1m 15:28 and 15:29) on the previous day. "
            f"The data may be unusually incomplete - treat matches with "
            f"extra caution and verify on TradingView."
            + (
                " The data feed can lag a few minutes after the close; if you just ran "
                "this, try again shortly."
                if get_run_context()["today_complete"] else ""
            )
        )

    conflicts = sum(1 for r in evaluated if r.get("status") == "CONFLICT")

    if conflicts / len(evaluated) > CONFLICT_WARN_SHARE:
        messages.append(
            f"{conflicts} of {len(evaluated)} symbols have a 15m/1m data conflict. "
            f"The 15m and 1m feeds are inconsistent with each other today; those "
            f"symbols were blocked from signalling."
        )

    return " ".join(messages) or None


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


def trend_name(d):
    return {1: "Up", -1: "Down"}.get(d, "Flat")


def hm_text(hm):
    return f"{hm // 100:02d}:{hm % 100:02d}"


def fmt_day(value):
    """'2026-09-28' -> 'Mon 28 Sep 2026'."""

    if not value:
        return "\u2013"

    try:
        ts = pd.Timestamp(value)
    except Exception:
        return str(value)

    return f"{ts.strftime('%a')} {ts.day} {ts.strftime('%b %Y')}"


def badge(value):

    if value is None:
        return DASH

    if value:
        return '<span class="badge pass">PASS</span>'

    return '<span class="badge fail">FAIL</span>'


STATUS_LABEL = {
    "PASS": "PASS", "FAIL": "FAIL", "CONFLICT": "DATA CONFLICT",
    "INCOMPLETE": "INCOMPLETE", "STALE": "STALE", "NO_DATA": "NO DATA",
    "ERROR": "ERROR",
}

STATUS_RANK = {
    "PASS": 0, "FAIL": 1, "CONFLICT": 2, "INCOMPLETE": 3,
    "STALE": 4, "ERROR": 5, "NO_DATA": 6,
}

CSS = """
body {
    font-family: Arial, sans-serif;
    background: #f5f5f5;
    color: #222;
    margin: 0;
    padding: 25px;
}
.container { max-width: 1500px; margin: auto; }
h1 { margin-bottom: 5px; }
.subtitle { color: #777; line-height: 1.6; }
.warn {
    background: #fff3cd; border: 1px solid #e0c36a; color: #6b5200;
    border-radius: 8px; padding: 12px 16px; margin: 18px 0;
}
.note {
    background: #e8f1fb; border: 1px solid #9bbbe0; color: #1d3f66;
    border-radius: 8px; padding: 12px 16px; margin: 18px 0;
}
.funnel { width: auto; min-width: 460px; }
.funnel td { padding: 6px 12px; }
.funnel .lost { color: #a52222; }
.stats { display: flex; flex-wrap: wrap; gap: 10px; margin: 20px 0; }
.stat {
    background: white; padding: 15px 20px; border-radius: 8px;
    border: 1px solid #ddd;
}
.stat b { font-size: 22px; display: block; }
.match-box {
    background: white; border: 1px solid #ddd; border-radius: 8px;
    padding: 20px; margin-bottom: 20px;
}
.matches { column-width: 300px; column-gap: 28px; }
.match {
    padding: 9px 0; border-bottom: 1px solid #eee;
    break-inside: avoid;
}
.direction {
    display: inline-block; padding: 3px 7px; border-radius: 4px;
    font-size: 11px; font-weight: bold; margin-right: 8px;
}
.long { background: #dff5e5; color: #08752f; }
.short { background: #f8dddd; color: #a52222; }
.date { color: #777; margin-left: 8px; font-size: 12px; }
.table-wrap { overflow-x: auto; }
table {
    width: 100%; border-collapse: collapse; background: white; font-size: 13px;
}
th {
    background: #ededed; padding: 10px; text-align: left;
    position: sticky; top: 0;
}
td { padding: 9px 10px; border-top: 1px solid #eee; vertical-align: top; }
.symbol { font-weight: bold; }
small { color: #777; font-size: 10px; }
.badge {
    display: inline-block; padding: 2px 6px; border-radius: 4px;
    font-size: 10px; font-weight: bold;
}
.badge.pass { background: #dff5e5; color: #08752f; }
.badge.fail { background: #f8dddd; color: #a52222; }
.pass { color: #08752f; font-weight: bold; }
.fail { color: #999; }
.skip { color: #b07000; }
.none { color: #777; }
.footer { margin-top: 25px; color: #777; font-size: 12px; line-height: 1.7; }
"""


def candle_text(candle, label):
    """'15:00 Up (100.00->101.00) vol 1,234' for the report; numbers only."""

    if candle is None:
        return f"{label} missing"

    direction = candle_direction(candle["open"], candle["close"])

    return (
        f'{label} {trend_name(direction)} '
        f'({candle["open"]:.2f}&rarr;{candle["close"]:.2f}) '
        f'vol {candle["volume"]:,}'
    )


def part_badge(value):
    """Small PASS/FAIL text for one part (trend or volume) of a condition."""

    if value is None:
        return "-"

    return "PASS" if value else "FAIL"


# The "last" candle of each timeframe: only its volume is used, so with
# MISSING_LAST_CANDLE_IS_ZERO_VOLUME it may be missing.
LAST_CANDLES = {"15m 15:15", "3m 15:27", "1m 15:29"}


def build_funnel(results):
    """Where stocks drop out, step by step. Returns [(label, count), ...];
    each line counts only the stocks that survived every line above it, so the
    last line is the number of signals. It separates data problems (download,
    missing candles, conflicts) from the strategy simply being strict."""

    got_data = [r for r in results if r.get("status") not in ("NO_DATA", "ERROR")]
    evaluated = [r for r in results if "cond_15m" in r]
    allowed_missing = (
        LAST_CANDLES if MISSING_LAST_CANDLE_IS_ZERO_VOLUME else set()
    )
    full = [r for r in evaluated if set(r.get("missing") or []) <= allowed_missing]
    step_1 = [r for r in full if r.get("vol_15m")]
    step_2 = [r for r in step_1 if r.get("vol_3m")]
    step_3 = [r for r in step_2 if r.get("vol_1m")]
    step_4 = [r for r in step_3 if r.get("cond_align")]
    step_5 = [r for r in step_4 if r.get("cond_morning")]
    step_6 = [r for r in step_5 if r.get("status") != "CONFLICT"]
    step_7 = [r for r in step_6 if r.get("status") != "STALE"]

    return [
        ("Stocks scanned", len(results)),
        ("Data downloaded (no error / no-data)", len(got_data)),
        ("A completed session found", len(evaluated)),
        (
            "All nine candles present"
            + (" (a last candle may be missing)" if allowed_missing else ""),
            len(full),
        ),
        ("15m: 15:00 volume > 15:15 volume", len(step_1)),
        ("3m: 15:24 volume > 15:27 volume", len(step_2)),
        ("1m: 15:28 volume > 15:29 volume", len(step_3)),
        ("Same trend: 15m 15:00 = 3m 15:24 = 1m 15:28", len(step_4)),
        ("Morning 09:15 candles (15m, 3m, 1m) share that trend", len(step_5)),
        ("No 15m/1m data conflict", len(step_6)),
        ("Same session as most stocks (SIGNALS)", len(step_7)),
    ]


def missing_summary(results):
    """How many stocks lack each of the nine candles, and how many lack ONLY the
    last 3m / 1m candle (the ones MISSING_LAST_CANDLE_IS_ZERO_VOLUME would
    recover). Returns (per_slot, only_last, evaluated_count)."""

    evaluated = [r for r in results if "cond_15m" in r]
    counts = Counter()

    for r in evaluated:
        for label in r.get("missing") or []:
            counts[label] += 1

    per_slot = [(slot_label(k), counts.get(slot_label(k), 0)) for k in SLOT_KEYS]

    last_only = sum(
        1 for r in evaluated
        if r.get("missing") and set(r["missing"]) <= LAST_CANDLES
    )

    return per_slot, last_only, len(evaluated)


def source_health(results):
    """Is the NSE source working, or did stocks fall back to Yahoo?
    Returns (lines, level): level is "ok", "partial" or "bad"."""

    counts = Counter(
        r["source"] for r in results
        if r.get("source") and r.get("status") not in ("NO_DATA", "ERROR")
    )

    nse = counts.get("NSE", 0)
    fallback = counts.get("Yahoo (fallback)", 0)
    yahoo = counts.get("Yahoo", 0)

    if DATA_SOURCE != "NSE":
        return [f"Data source is Yahoo (DATA_SOURCE = YAHOO): {yahoo} stocks."], "ok"

    lines = [f"NSE served {nse} stocks; Yahoo fallback served {fallback} stocks."]

    if _nse_calibration["detail"]:
        lines.append(_nse_calibration["detail"])

    if _nse_state["pauses"]:
        lines.append(
            f"NSE rate-limited this scan {_nse_state['pauses']} time(s); the scanner "
            f"paused NSE for {_nse_state['paused_seconds']:.0f}s in total and carried on."
        )

    if _nse_state["blocked"] and _nse_calibration["ok"] is not False:
        lines.append("NSE was given up for the rest of the run after repeated refusals.")

    if nse == 0 and fallback > 0:
        level = "bad"
        lines.append(
            "NSE served nothing: every stock came from Yahoo. The reasons are "
            "below; run  python scanner.py --nse-debug RELIANCE  to see NSE's raw replies."
        )
    elif nse == 0:
        level = "bad"
        lines.append("NSE served no stocks and neither did the fallback.")
    elif fallback == 0:
        level = "ok"
        lines.append("NSE is working: no stock needed the Yahoo fallback.")
    elif fallback <= 0.10 * (nse + fallback):
        level = "ok"
        lines.append(
            "NSE is working: only a few stocks (thin or unlisted symbols) fell back."
        )
    else:
        level = "partial"
        lines.append("NSE is only partly working: many stocks fell back to Yahoo.")

    with _nse_lock:
        reasons = _nse_errors.most_common(5)
        examples = dict(_nse_examples)

    if reasons:
        lines.append("Why NSE failed for those stocks (top reasons):")

        for message, count in reasons:
            lines.append(f"{count} x {message}")

            if message in examples:
                lines.append(f"      e.g. {examples[message]}")

    return lines, level


def near_miss_list(results):
    """Stocks that failed ONLY because their last 3m (15:27) and/or last 1m
    (15:29) or last 15m (15:15) candle is missing - every other condition holds. With
    MISSING_LAST_CANDLE_IS_ZERO_VOLUME = True they would be signals. Returns
    [(symbol, direction)]. Empty when the switch is already on."""

    if MISSING_LAST_CANDLE_IS_ZERO_VOLUME:
        return []

    allowed = LAST_CANDLES
    found = []

    for r in results:
        missing = set(r.get("missing") or [])

        if r.get("status") != "FAIL" or not missing or not missing <= allowed:
            continue

        c = r.get("candles") or {}

        # A missing last candle has volume 0, so it passes if the second-last
        # candle exists (and traded).
        ok_15m = r.get("vol_15m") or (c.get("15m_1515") is None and c.get("15m_1500") is not None)
        ok_3m = r.get("vol_3m") or (c.get("3m_1527") is None and c.get("3m_1524") is not None)
        ok_1m = r.get("vol_1m") or (c.get("1m_1529") is None and c.get("1m_1528") is not None)

        if ok_15m and ok_3m and ok_1m and r.get("cond_align") and r.get("cond_morning"):
            side = r["details"]["t15_00"] * DIRECTION_RULES[DIRECTION_RULE]
            found.append((r["symbol"], "LONG" if side == 1 else "SHORT"))

    return sorted(found)


def closest_misses(results, limit=25):
    """When signals are few (or none): stocks that failed exactly ONE condition
    (every candle present, no data conflict). Returns [(symbol, failed)]."""

    allowed_missing = (
        LAST_CANDLES if MISSING_LAST_CANDLE_IS_ZERO_VOLUME else set()
    )
    found = []

    for r in results:
        if r.get("status") != "FAIL" or "cond_15m" not in r:
            continue

        if not set(r.get("missing") or []) <= allowed_missing:
            continue

        failed = [
            name for name, ok in (
                ("15m volume", r.get("vol_15m")),
                ("3m volume", r.get("vol_3m")),
                ("1m volume", r.get("vol_1m")),
                ("alignment", r.get("cond_align")),
                ("morning candles", r.get("cond_morning")),
            ) if not ok
        ]

        if len(failed) == 1:
            found.append((r["symbol"], failed[0]))

    return sorted(found)[:limit]


def funnel_console(results):

    lines, previous = [], None

    for label, count in build_funnel(results):
        lost = f"  (-{previous - count})" if previous is not None and previous > count else ""
        lines.append(f"  {count:>5}  {label}{lost}")
        previous = count

    per_slot, last_only, total = missing_summary(results)

    lines.append("")
    lines.append(f"  Stocks missing each candle (of {total} with a session):")

    for label, count in per_slot:
        lines.append(f"  {count:>5}  {label}")

    lines.append(
        f"  {last_only:>5}  missing ONLY a last candle (15m 15:15 / 3m 15:27 / 1m 15:29) "
        f"(MISSING_LAST_CANDLE_IS_ZERO_VOLUME = {MISSING_LAST_CANDLE_IS_ZERO_VOLUME})"
    )

    near = near_miss_list(results)

    if near:
        lines.append("")
        lines.append(
            f"  {len(near)} stock(s) fail ONLY because a last candle (15m 15:15 / 3m 15:27 / 1m 15:29) had no trades."
        )
        lines.append(
            "  They would be signals with MISSING_LAST_CANDLE_IS_ZERO_VOLUME = True:"
        )
        lines.append("    " + ", ".join(f"{sym} ({d})" for sym, d in near))

    closest = closest_misses(results)

    if closest:
        lines.append("")
        lines.append("  Closest misses (fail exactly one condition; not signals):")
        lines.append("    " + ", ".join(f"{sym} [{why}]" for sym, why in closest))

    return "\n".join(lines)


def funnel_html(results):

    body, previous = "", None

    for label, count in build_funnel(results):
        lost = (
            f"-{previous - count}"
            if previous is not None and previous > count else ""
        )
        body += (
            f'<tr><td>{esc(label)}</td><td>{count}</td>'
            f'<td class="lost">{lost}</td></tr>'
        )
        previous = count

    per_slot, last_only, total = missing_summary(results)

    missing_body = "".join(
        f'<tr><td>{esc(label)}</td><td>{count}</td></tr>' for label, count in per_slot
    )
    missing_body += (
        f'<tr><td>Missing ONLY the last 3m/1m candle '
        f'(MISSING_LAST_CANDLE_IS_ZERO_VOLUME = '
        f'{esc(MISSING_LAST_CANDLE_IS_ZERO_VOLUME)})</td><td>{last_only}</td></tr>'
    )

    closest = closest_misses(results)
    closest_html = ""

    if closest:
        names = ", ".join(f"{esc(sym)} [{esc(why)}]" for sym, why in closest)
        closest_html = (
            '<h2 style="margin-top:22px">Closest misses (fail exactly one condition)</h2>'
            '<p class="none">Not signals: each of these passed every condition except '
            f'the one in brackets.</p><p>{names}</p>'
        )

    near = near_miss_list(results)

    near_html = ""

    if near:
        names = ", ".join(f"{esc(sym)} ({esc(d)})" for sym, d in near)
        near_html = (
            f'<h2 style="margin-top:22px">Filtered only by a missing last candle ({len(near)})</h2>'
            '<p class="none">Every other condition holds; the last 15m (15:15), 3m (15:27) and/or '
            'last 1m (15:29) candle had no trades. They would be signals with '
            'MISSING_LAST_CANDLE_IS_ZERO_VOLUME = True. Check them on a chart '
            f'before deciding.</p><p>{names}</p>'
        )

    return (
        '<div class="match-box"><h2>Where stocks dropped out</h2>'
        '<div class="table-wrap"><table class="funnel"><tbody>'
        f'{body}</tbody></table></div>'
        f'<h2 style="margin-top:22px">Stocks missing each candle (of {total} with a session)</h2>'
        '<div class="table-wrap"><table class="funnel"><tbody>'
        f'{missing_body}</tbody></table></div>'
        f'{near_html}{closest_html}</div>'
    )


def build_table_row(r):

    details = r.get("details")
    candles = r.get("candles") or {}
    status = r.get("status", "UNKNOWN")
    status_class = {"PASS": "pass", "FAIL": "fail"}.get(status, "skip")

    if details:
        note_15m = (
            candle_text(candles.get("15m_1500"), "15:00") + "<br>"
            + candle_text(candles.get("15m_1515"), "15:15") + "<br>"
            + f'volume {part_badge(r.get("vol_15m"))}'
        )
        note_3m = (
            candle_text(candles.get("3m_1524"), "15:24") + "<br>"
            + candle_text(candles.get("3m_1527"), "15:27") + "<br>"
            + f'volume {part_badge(r.get("vol_3m"))}'
        )
        note_1m = (
            candle_text(candles.get("1m_1528"), "15:28") + "<br>"
            + candle_text(candles.get("1m_1529"), "15:29") + "<br>"
            + f'volume {part_badge(r.get("vol_1m"))}'
        )
        note_align = (
            f'15m 15:00 {trend_name(details["t15_00"])} / '
            f'3m 15:24 {trend_name(details["t3_24"])} / '
            f'1m 15:28 {trend_name(details["t1_28"])}'
        )
        note_morning = (
            candle_text(candles.get("15m_0915"), "15m 09:15") + "<br>"
            + candle_text(candles.get("3m_0915"), "3m 09:15") + "<br>"
            + candle_text(candles.get("1m_0915"), "1m 09:15")
        )
    else:
        note_15m = note_3m = note_1m = note_align = note_morning = ""

    data_label = {
        "OK": "OK", "FALLBACK": "Fallback window", "NO_DATA": "No data", "ERROR": "Error",
    }.get(r.get("data_status"), r.get("data_status"))

    data_text = cell(data_label)

    if r.get("source"):
        data_text += f" ({esc(r['source'])})"

    if r.get("missing"):
        data_text += (
            "<br><small>missing: "
            + ", ".join(esc(m) for m in r["missing"]) + "</small>"
        )

    for conflict in r.get("conflicts") or []:
        data_text += f"<br><small>CONFLICT: {esc(conflict)}</small>"

    if r.get("error"):
        data_text += f"<br><small>{esc(r['error'][:120])}</small>"

    return f"""
<tr>
<td class="symbol">{esc(r.get('symbol', ''))}</td>
<td>{esc(r['previous_day']) if r.get('previous_day') else DASH}</td>
<td>{cell(r.get('direction'))}</td>
<td>{badge(r.get('cond_15m'))}<br><small>{note_15m}</small></td>
<td>{badge(r.get('cond_3m'))}<br><small>{note_3m}</small></td>
<td>{badge(r.get('cond_1m'))}<br><small>{note_1m}</small></td>
<td>{badge(r.get('cond_align'))}<br><small>{note_align}</small></td>
<td>{badge(r.get('cond_morning'))}<br><small>{note_morning}</small></td>
<td class="{status_class}">{esc(STATUS_LABEL.get(status, status))}</td>
<td>{data_text}</td>
</tr>
"""


# =============================================================================
# GENERATE HTML
# =============================================================================

def generate_html_report(results, elapsed, universe_source, run_note=None):

    def count(name):
        return sum(1 for r in results if r.get("status") == name)

    matches = [r for r in results if r.get("status") == "PASS"]
    matches.sort(key=lambda r: (r.get("direction") != "LONG", r.get("symbol", "")))

    longs = sum(1 for r in matches if r.get("direction") == "LONG")
    shorts = len(matches) - longs

    def most_common(field):
        c = Counter(r[field] for r in results if r.get(field)).most_common(1)
        return c[0][0] if c else None

    previous_day = most_common("previous_day")
    entry_day = most_common("entry_day") or get_run_context()["entry"]

    scan_time = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d %H:%M:%S IST")

    source_counts = Counter(
        r["source"] for r in results
        if r.get("source") and r.get("status") not in ("NO_DATA", "ERROR")
    )
    source_text = (
        ", ".join(f"{name}: {n} stocks" for name, n in source_counts.most_common())
        or DATA_SOURCE
    )

    health_lines, health_level = source_health(results)
    health_html = (
        f'<div class="{"note" if health_level == "ok" else "warn"}">'
        + "<br>".join(esc(line) for line in health_lines)
        + "</div>"
    )

    warning = session_warning(results)
    warning_html = (
        (f'<div class="note">{esc(run_note)}</div>' if run_note else "")
        + health_html
        + (f'<div class="warn">{esc(warning)}</div>' if warning else "")
    )

    if matches:
        match_html = "".join(
            '<div class="match">'
            f'<span class="direction {"long" if r["direction"] == "LONG" else "short"}">'
            f'{esc(r["direction"])}</span>'
            f'<b>{esc(r["symbol"])}</b>'
            f'<span class="date">Previous day: {esc(r.get("previous_day", ""))}</span>'
            '</div>'
            for r in matches
        )
    else:
        match_html = '<div class="none">No stocks matched.</div>'

    sorted_results = sorted(
        results,
        key=lambda x: (STATUS_RANK.get(x.get("status"), 9), x.get("symbol", "")),
    )

    table_rows = "".join(build_table_row(r) for r in sorted_results)

    stats = [
        (len(results), "Stocks scanned"),
        (len(matches), "Matches"),
        (longs, "Long"),
        (shorts, "Short"),
        (count("FAIL"), "No match"),
        (count("CONFLICT"), "Data conflict"),
        (count("INCOMPLETE"), "Incomplete"),
        (count("STALE"), "Stale"),
        (count("NO_DATA"), "No data"),
        (count("ERROR"), "Errors"),
        (f"{elapsed:.1f}s", "Scan time"),
    ]

    stats_html = "".join(
        f'<div class="stat"><b>{value}</b>{label}</div>' for value, label in stats
    )

    zero_note = (
        " A last candle (15m 15:15 / 3m 15:27 / 1m 15:29) with no trades counts as volume 0."
        if MISSING_LAST_CANDLE_IS_ZERO_VOLUME else ""
    )

    if DIRECTION_RULES[DIRECTION_RULE] == 1:
        direction_text = (
            "Direction follows the alignment trend (the second-last candles: "
            "15m 15:00 / 3m 15:24 / 1m 15:28): "
            "Up is LONG, Down is SHORT."
        )
    else:
        direction_text = (
            "Direction fades the alignment trend (the second-last candles: "
            "15m 15:00 / 3m 15:24 / 1m 15:28): "
            "Up is SHORT, Down is LONG."
        )

    document = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NSE Momentum Scanner</title>
<style>{CSS}</style>
</head>
<body>
<div class="container">

<h1>NSE Momentum Scanner</h1>

<div class="subtitle">
Generated: {esc(scan_time)}<br>
Universe: {esc(universe_source)}<br>
Data: {esc(source_text)} - 15-minute and 1-minute candles (3-minute built from 1-minute)<br>
Previous day: {esc(fmt_day(previous_day))}<br>
Entry: {esc(fmt_day(entry_day))} at the 09:15 open &nbsp;|&nbsp; Exit: {esc(EXIT_TIME_LABEL)}
</div>

{warning_html}

<div class="stats">{stats_html}</div>

{funnel_html(results)}

<div class="match-box">
<h2>Matches</h2>
<div class="matches">{match_html}</div>
</div>

<div class="table-wrap">
<table>
<thead>
<tr>
<th>Symbol</th>
<th>Previous day</th>
<th>Direction</th>
<th>15m condition<br><small>15:00 vol &gt; 15:15 vol</small></th>
<th>3m condition<br><small>15:24 vol &gt; 15:27 vol</small></th>
<th>1m condition<br><small>15:28 vol &gt; 15:29 vol</small></th>
<th>Alignment<br><small>15m 15:00 = 3m 15:24 = 1m 15:28 trend</small></th>
<th>Morning candles<br><small>09:15 candles (15m, 3m, 1m) = that same trend</small></th>
<th>Result</th>
<th>Data</th>
</tr>
</thead>
<tbody>
{table_rows}
</tbody>
</table>
</div>

<div class="footer">
<b>STRATEGY (15-minute, 3-minute and 1-minute candles)</b><br>
Read on the previous day, the latest completed session. Candles are labelled by start
time: the last candles are 15m 15:15 (second last 15:00), 3m 15:27 (second last 15:24)
and 1m 15:29 (second last 15:28). A stock matches only when ALL of these hold:<br>
1) 15m volume: the 15:00 candle's volume is higher than the 15:15 candle's.<br>
2) 3m volume: the 15:24 candle's volume is higher than the 15:27 candle's.<br>
3) 1m volume: the 15:28 candle's volume is higher than the 15:29 candle's.<br>
4) The 15m 15:00, 3m 15:24 and 1m 15:28 candles all have the same trend.<br>
5) The morning candles (09:15) of the 15m, 3m and 1m timeframes have that same trend too.<br>
No timeframe has an opposite-trend rule: the last two candles of each timeframe may have
the same or opposite trends.<br>
Trend = candle colour (close vs open); a flat candle has no trend, so it is never
the same as another.
{esc(direction_text)}<br>
<b>Entry:</b> the 09:15 open on entry day. <b>Exit:</b> {esc(EXIT_TIME_LABEL)}.<br><br>
<b>Data notes:</b> 15-minute and 1-minute candles come straight from the data source's own
feeds (NSE's charting feed, or Yahoo as a fallback - shown per stock in the Data column);
there is no 3-minute feed, so 3-minute candles are built from the 1-minute candles on a
09:15 grid (open = first, close = last, volume = sum). Prices are rounded to 2 decimals;
zero-volume and corrupt candles are dropped and count as missing, which can only block a
match.{zero_note} DATA CONFLICT means a native 15m candle's trend, or which 15m candle has more
volume, disagrees with the same candles rebuilt from the 1m candles inside them, so the
symbol is blocked rather than trusted. INCOMPLETE means no completed session (or none of
the nine candles) came back. STALE means the previous day differs from most other symbols.
Each candle is shown as open&rarr;close with its volume so a signal can be checked against
a chart.
</div>

</div>
</body>
</html>
"""

    with open("index.html", "w", encoding="utf-8") as file:
        file.write(document)


# =============================================================================
# SINGLE-STOCK AUDIT   (python scanner.py --check RELIANCE TCS)
# =============================================================================

def print_audit(r):

    print("=" * 70)
    print(f"{r.get('symbol')}: {STATUS_LABEL.get(r.get('status'), r.get('status'))}"
          f"   (data: {r.get('data_status')}, source: {r.get('source')})")

    if r.get("error"):
        print("  error:", r["error"])

    if not r.get("previous_day"):
        print("  no completed session found")
        return

    print(f"  Previous day: {r['previous_day']}   Entry day: {r.get('entry_day')}")

    if r.get("tail_1m") is not None:
        tail = ", ".join(hm_text(h) for h in r["tail_1m"]) or "none"
        print(f"  1m candles returned from 15:20 on: {tail}")

    candles = r.get("candles") or {}
    derived = r.get("derived") or {}

    for key in SLOT_KEYS:
        c = candles.get(key)
        label = slot_label(key)

        if c is None:
            print(f"  {label:<10} missing")
            continue

        d = candle_direction(c["open"], c["close"])
        line = (
            f"  {label:<10} {trend_name(d):<5} open {c['open']:.2f}  "
            f"close {c['close']:.2f}  vol {c['volume']:,}"
        )

        if key in derived and derived[key]:
            rb = derived[key]
            rd = candle_direction(rb["open"], rb["close"])
            line += (
                f"   | rebuilt from 1m: {trend_name(rd)} "
                f"({rb['open']:.2f} -> {rb['close']:.2f}) vol {rb['volume']:,}"
            )

        print(line)

    if "cond_15m" in r:
        print(f"  15m 15:00 vol > 15:15 vol  : {r['vol_15m']}")
        print(f"  3m  15:24 vol > 15:27 vol  : {r['vol_3m']}")
        print(f"  1m  15:28 vol > 15:29 vol  : {r['vol_1m']}")
        print(f"  15m 15:00 = 3m 15:24 = 1m 15:28 trend: {r['cond_align']}")
        print(f"  09:15 candles (15m, 3m, 1m) share that trend: {r['cond_morning']}")

    for conflict in r.get("conflicts") or []:
        print("  CONFLICT:", conflict)

    if r.get("direction"):
        print("  Direction:", r["direction"])


def audit_symbols(symbols):

    if not symbols:
        print("Usage: python scanner.py --check SYMBOL [SYMBOL ...]")
        return

    for symbol in symbols:
        print_audit(scan_one_symbol(symbol.strip().upper()))


# =============================================================================
# SOURCE TEST   (python scanner.py --compare RELIANCE TCS)
# =============================================================================

def compare_sources(symbols):
    """Fetch the same stock from NSE and from Yahoo and print the nine strategy
    candles side by side, so the two sources can be checked against each other
    (and against a chart) before trusting a scan."""

    if not symbols:
        print("Usage: python scanner.py --compare SYMBOL [SYMBOL ...]")
        return

    def candle_line(candle):
        if candle is None:
            return "missing"

        return (
            f"{candle['open']:>9.2f} {candle['close']:>9.2f} "
            f"{candle['volume']:>10,}"
        )

    for symbol in symbols:
        symbol = symbol.strip().upper()
        evaluated = {}

        for label, fetcher in (("NSE", fetch_interval_nse), ("Yahoo", fetch_interval)):
            rows_by_tf, errors = {}, []

            for tf in TIMEFRAMES:
                rows, info = fetcher(symbol, tf)

                if rows is None:
                    errors.append(f"{tf}: {info}")
                else:
                    rows_by_tf[tf] = rows

            if len(rows_by_tf) == len(TIMEFRAMES):
                evaluated[label] = (evaluate_rows(rows_by_tf), [])
            else:
                evaluated[label] = (None, errors)

        print("=" * 78)
        print(symbol)

        for label in ("NSE", "Yahoo"):
            result, errors = evaluated[label]

            if result is None:
                print(f"  {label:<6}: no data  ({'; '.join(errors)})")
            else:
                print(f"  {label:<6}: session {result.get('previous_day')}  "
                      f"status {result.get('status')}")

        nse, yahoo = evaluated["NSE"][0], evaluated["Yahoo"][0]

        if nse is None or yahoo is None or "candles" not in nse or "candles" not in yahoo:
            continue

        print(f"  {'candle':<10} {'NSE open':>9} {'close':>9} {'volume':>10}   "
              f"{'Yahoo open':>10} {'close':>9} {'volume':>10}   verdict")

        for key in SLOT_KEYS:
            a, b = nse["candles"].get(key), yahoo["candles"].get(key)

            if a is None or b is None:
                verdict = "one source is missing this candle"
            elif (
                abs(a["open"] - b["open"]) <= 0.011
                and abs(a["close"] - b["close"]) <= 0.011
                and abs(a["volume"] - b["volume"]) <= max(1, 0.02 * max(a["volume"], b["volume"]))
            ):
                verdict = "same"
            else:
                verdict = "DIFFERENT"

            print(f"  {slot_label(key):<10} {candle_line(a)}   {candle_line(b)}   {verdict}")


# =============================================================================
# NSE DEBUG   (python scanner.py --nse-debug RELIANCE)
# =============================================================================

def nse_debug(symbols):
    """Print NSE's raw replies for one stock, and how its candle minutes line up
    with Yahoo's, so a problem can be diagnosed. Paste the output when asking
    for help."""

    symbol = (symbols[0] if symbols else "RELIANCE").strip().upper()
    session = nse_session(refresh=True)

    print("=" * 78)
    print(f"NSE DEBUG for {symbol}")
    print("=" * 78)
    print(f"1) cookies set after visiting NSE: {sorted(set(session.cookies.keys()))}")

    try:
        reply = session.get(
            NSE_SEARCH_URL, params={"symbol": symbol, "segment": ""},
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as e:
        print(f"2) symbol search failed: {type(e).__name__}: {e}")
        return

    print(f"2) symbol search: HTTP {reply.status_code}, content-type {reply.headers.get('content-type')}")
    print("   body:", snippet(reply.text, 400))

    try:
        search = reply.json()
    except ValueError:
        print("   -> not JSON (probably a block page).")
        return

    items = search if isinstance(search, list) else (
        (search.get("data") or []) if isinstance(search, dict) else []
    )
    match = next(
        (i for i in items if isinstance(i, dict)
         and str(i.get("symbol", "")).upper() in {symbol, f"{symbol}-EQ"}),
        None,
    )

    if match is None:
        print("   -> no exact match for the symbol in this reply.")
        return

    print("   -> exact match:", snippet(match, 300))

    for interval, minutes in (("1m", 1), ("15m", 15)):
        now = int(time.time())
        params = {
            "token": str(match["scripcode"]),
            "fromDate": now - NSE_HISTORY_DAYS * 86400,
            "toDate": now,
            "symbol": match["symbol"],
            "symbolType": match.get("type") or "Equity",
            "chartType": "I",
            "timeInterval": minutes,
        }

        try:
            reply = session.get(NSE_HISTORY_URL, params=params, timeout=REQUEST_TIMEOUT)
            body = reply.json()
        except Exception as e:
            print(f"3) {interval} history failed: {type(e).__name__}: {e}")
            continue

        data = body if isinstance(body, list) else (body.get("data") if isinstance(body, dict) else None)
        print(f"3) {interval} history: HTTP {reply.status_code}, "
              f"{0 if not data else len(data)} candles, fields "
              f"{snippet({k: v for k, v in body.items() if k != 'data'}) if isinstance(body, dict) else '(bare list)'}")

        if not data:
            continue

        times = []

        for row in data:
            try:
                value = float(row["time"])
                unit = "ms" if abs(value) > 1e11 else "s"
                times.append(pd.to_datetime(value, unit=unit))
            except Exception:
                pass

        if times:
            last_day = max(t.date() for t in times)
            day_times = [t for t in times if t.date() == last_day]
            labels = [t.strftime("%H:%M:%S") for t in day_times]
            print(f"   time stamps read as clock time; last day {last_day}: "
                  f"{len(day_times)} candles")
            print(f"   first 6: {labels[:6]}")
            print(f"   last 6 : {labels[-6:]}")

    print("4) alignment of NSE 1-minute candles with Yahoo's:")

    try:
        result = nse_alignment(symbol)
        print(f"   compared {result['candles']} candles on {result['days']}")
        print(f"   best correction {result['shift']:+d} min, {result['share']:.0%} of minutes match "
              f"(runner-up {result['runner_up']:.0%}); top 4: {result['top']}")
        print("   verdict:", "VERIFIED" if nse_alignment_ok(result) else "NOT VERIFIED")
    except Exception as e:
        print(f"   could not compare: {type(e).__name__}: {e}")


def main():

    if len(sys.argv) > 1 and sys.argv[1] == "--check":
        load_nse_token_cache()
        nse_preflight()
        audit_symbols(sys.argv[2:])
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--nse-debug":
        nse_debug(sys.argv[2:])
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--compare":
        load_nse_token_cache()
        nse_preflight()
        compare_sources(sys.argv[2:])
        save_nse_token_cache()
        return

    print()
    print("=" * 70)
    print("NSE EOD MOMENTUM SCANNER")
    print("=" * 70)
    print()
    if DATA_SOURCE not in ("NSE", "YAHOO"):
        print('ERROR: DATA_SOURCE must be "NSE" or "YAHOO"')
        sys.exit(1)

    if (DATA_SOURCE == "YAHOO" or FALLBACK_TO_YAHOO) and yf is None:
        print()
        print("ERROR: yfinance is not installed, but it is needed for Yahoo / the Yahoo fallback.")
        print("Install with: pip install yfinance curl_cffi")
        print('Or set DATA_SOURCE = "NSE" and FALLBACK_TO_YAHOO = False in the scanner.')
        print()
        sys.exit(1)

    print("Data source:", DATA_SOURCE, "(Yahoo fallback on)" if FALLBACK_TO_YAHOO and DATA_SOURCE == "NSE" else "")

    if yf is not None:
        print("yfinance version:", getattr(yf, "__version__", "unknown"))

    load_nse_token_cache()
    nse_preflight()

    if DIRECTION_RULE not in DIRECTION_RULES:
        print(f"ERROR: DIRECTION_RULE must be one of {list(DIRECTION_RULES)}")
        sys.exit(1)

    # Fix the session being read for the whole run.
    context = get_run_context()

    if context["note"]:
        print()
        print("NOTE:", context["note"])

    universe, source = get_stock_universe()

    print()
    print("=" * 70)
    print(f"FINAL STOCK UNIVERSE: {len(universe)}")
    print(f"SOURCE: {source}")
    print("=" * 70)

    results, elapsed = scan_all_symbols(universe)
    save_nse_token_cache()

    reference_date = mark_stale(results)

    matches = [r for r in results if r.get("status") == "PASS"]

    print()
    print("=" * 70)
    print("SCAN COMPLETE")
    print("=" * 70)
    print(f"Universe    : {len(universe)}")
    print(f"Previous day: {reference_date}   Entry day: {context['entry']}")
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

    for name in ("PASS", "FAIL", "CONFLICT", "INCOMPLETE", "STALE", "NO_DATA", "ERROR"):
        n = sum(1 for r in results if r.get("status") == name)
        print(f"{name:<11}: {n}")

    health_lines, _ = source_health(results)
    print()
    print("DATA SOURCE HEALTH:")
    for line in health_lines:
        print("  " + line)

    print()
    print("WHERE STOCKS DROPPED OUT:")
    print(funnel_console(results))

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

    generate_html_report(results, elapsed, source, context["note"])

    print()
    print("HTML report: index.html")
    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
