# =============================================================================
# NSE DAILY MOMENTUM SCANNER - 15-MIN / 3-MIN / 1-MIN CLOSING-CANDLE VERSION
#                              (NSE DATA ONLY)
# =============================================================================
#
# Every candle comes from NSE's own charting feed (charting.nseindia.in), the
# feed behind the charts on NSE's website. No other data source is used, not
# even as a fallback or a cross-check: if NSE cannot serve a stock, that stock
# is reported as ERROR / NO DATA, never filled in from somewhere else.
#
# NOTE: NSE publishes no free, documented intraday API. This feed is NSE's own
# data, but the endpoint is undocumented and can change or block you. Truly
# "official" intraday data is a paid product of NSE Data & Analytics.
#
# Universe: NSE's official equity list / Nifty index CSVs -> built-in fallback
# list (symbols only; candles always come from NSE).
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
#   1-minute  : ... 15:28, 15:29       (15:29 is the last, 15:28 the second last)
# NSE's feed is downloaded as 1-minute candles only; the 3-minute and
# 15-minute candles are built from them on the 09:15 grid (09:15, 09:18, ...
# 15:27 and 09:15, 09:30, ... 15:15), the same grid broker charts use.
#
# KNOWN LIMITS OF NSE'S 1-MINUTE FEED (checked against an independent NSE client
# on 24 Nov 2026; the report repeats them):
#   * one row near the close (15:28 or 15:29) carries the whole day's volume. It
#     is corrected to an ESTIMATE (day total minus the other bars) and marked
#     "estimated" in the report; it feeds the closing-volume rules.
#   * NSE has no 1-minute rows for 15:16-15:19, so the 15m 15:15 candle's volume
#     is understated (it only holds 15:15 and 15:20-15:29).
#   Set ESTIMATED_VOLUME_BLOCKS_SIGNAL = True to refuse any signal whose volume
#   rules rest on an estimated candle.
#
# ALL of these must hold:
#   1) 15m volume: the 15:00 candle's volume is HIGHER than the 15:15 candle's.
#   2) 3m volume:  the 15:24 candle's volume is HIGHER than the 15:27 candle's.
#   3) 3m trend:   the last two 3m candles (15:24, 15:27) have the SAME trend.
#   4) 1m volume:  the 15:28 candle's volume is HIGHER than the 15:29 candle's.
#   5) 1m trend:   the last two 1m candles (15:28, 15:29) have the SAME trend.
#   6) The second-last candles of all three timeframes (15m 15:00, 3m 15:24,
#      1m 15:28) have the SAME trend (the "aligned trend").
#   7) The morning candles (09:15) of all three timeframes (15m 09:15, 3m 09:15,
#      1m 09:15) have that SAME aligned trend too.
#
# The 15m timeframe has no opposite-trend rule: its last two candles may have
# the same or opposite trends; only their volume order matters (and the
# second-last candle's trend, for the alignment in 6).
#
# "Trend" = candle colour (close > open is Up, close < open is Down). A flat
# candle (close == open) has no trend, so it can never be "the same" as another.
#
# DIRECTION: set by DIRECTION_RULE below (default: trade WITH the aligned
# second-last candles' trend: Up = LONG, Down = SHORT).
# TRADE:     Entry = the 09:15 OPEN of the entry day, Exit = EXIT_TIME_LABEL.
#
# A candle with no trades is absent from the data. A missing candle can only
# BLOCK a match, never create one.
#
# DATA-ACCURACY SAFEGUARDS
#   * Before a scan, NSE's candle times are VERIFIED against the known session
#     shape: on a liquid reference stock a full session must start at 09:15 and
#     end at 15:29. If it does not, the scan stops instead of guessing.
#   * Whether NSE stamps a candle with its start or its end is detected from the
#     data (a day with a 09:15 stamp and no 15:30 stamp = start stamps).
#   * Prices are rounded to 2 decimals.
#   * Zero-volume and corrupt candles (price <= 0, close/open outside the
#     high-low range) are dropped and treated as MISSING.
#   * Every row in the report shows each candle's open -> close and volume, so
#     any signal can be checked against a chart in seconds.
#   * Single-stock audit:   python scanner.py --check RELIANCE TCS
#   * Raw feed diagnostics: python scanner.py --nse-debug RELIANCE
#
# INSTALL:
#   python -m pip install pandas requests
#
# =============================================================================

import base64
import datetime
import html
import json
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


# =============================================================================
# CONFIGURATION
# =============================================================================

# Worker threads. NSE requests are throttled separately (NSE_MAX_CONNECTIONS /
# NSE_MIN_INTERVAL below), so more workers than connections only wait in line.
MAX_WORKERS = 6

# Attempts per request (only rate limits / network errors are retried).
MAX_RETRIES = 3

REQUEST_TIMEOUT = 20

# If fewer than this share of scanned symbols have all nine required candles,
# the report shows a "data looks incomplete" warning. Kept low because
# illiquid stocks often have a no-trade candle.
SESSION_COMPLETE_SHARE = 0.20

# A short description of the rules, shown at the top of every report and in the
# console so a result can always be tied to the rule set that produced it.
STRATEGY_RULES = (
    "15m volume | 3m volume + same trend | 1m volume + same trend | "
    "aligned second-last trend | morning (09:15) trend"
)

# Shown in every report: what NSE's 1-minute feed cannot give exactly.
NSE_FEED_NOTE = (
    "NSE feed limits: one row near the close (15:28 or 15:29) carries the whole day's "
    "volume and is corrected to an estimate (marked 'est.'); NSE has no 1-minute rows for "
    "15:16-15:19, so the 15m 15:15 candle's volume is understated. Closing-volume rules "
    "use these numbers as they are; set ESTIMATED_VOLUME_BLOCKS_SIGNAL = True to refuse "
    "signals that rest on an estimate."
)

# No stock may be lost to a technical failure. After the main scan, stocks that
# ERRORED are retried (waiting first, with fewer workers).
RETRY_ROUNDS = 3
RETRY_WAIT_SECONDS = (30, 60, 120)
RETRY_WORKERS = 3

# Days of history requested from NSE (only the last session is needed).
NSE_HISTORY_DAYS = 8

# NSE moved its websites from nseindia.com to nseindia.in in November 2026
# (the old hosts answer with a 301 redirect, which silently drops the session
# cookies). A redirect is refused with a message naming the new host: if NSE
# moves again, change these two lines.
NSE_WWW_HOST = "https://www.nseindia.in"
NSE_CHARTING_HOST = "https://charting.nseindia.in"

# One row of NSE's 1-minute feed near the close (15:28 or 15:29) carries the
# whole day's volume. It is replaced by an estimate (day total minus the other
# minutes) and marked "estimated". The volume rules for the closing candles
# use those numbers as they are. Set True to make a volume rule FAIL whenever
# one of its two candles has an estimated volume (stricter: this can remove
# most or all signals).
ESTIMATED_VOLUME_BLOCKS_SIGNAL = False

# Manual correction (minutes) added to every NSE candle time. Leave at 0 unless
# the time check at the start of a run fails AND `--nse-debug` shows a constant
# offset (e.g. the day's first candle stamped 09:16 instead of 09:15 -> -1).
NSE_TIME_SHIFT_MINUTES = 0

# Stock -> NSE chart token, saved between runs so each symbol is looked up once.
NSE_TOKEN_CACHE_FILE = "nse_token_cache.json"

# NSE answers a burst of requests with HTTP 403/429, so requests are spaced out
# and only a few run at once, however many worker threads there are:
NSE_MAX_CONNECTIONS = 3

# Minimum gap (seconds) between any two NSE requests, all threads together.
NSE_MIN_INTERVAL = 0.4

# If NSE refuses this many requests in a row, it is PAUSED: every worker waits
# NSE_COOLDOWN_SECONDS and then carries on. Each further pause without a success
# in between doubles the wait (up to NSE_COOLDOWN_MAX). After NSE_MAX_COOLDOWNS
# such pauses in a row, the rest of the main scan is skipped (those stocks are
# marked ERROR) and the retry rounds try them again after a longer wait.
NSE_REFUSALS_BEFORE_COOLDOWN = 4
NSE_COOLDOWN_SECONDS = 45
NSE_COOLDOWN_MAX = 600
NSE_MAX_COOLDOWNS = 4

# If NSE has served NOTHING usable after this many stocks, stop for the run
# (it is not working from this network / the API changed).
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

# After the 15:30 close, NSE needs a few minutes to publish the final candles.
# Today's session is only treated as complete this many minutes after the close
# (default 15:50); before that the scanner still reads yesterday.
DATA_READY_BUFFER_MINUTES = 20


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
# which list was actually used. NSE needs two requests per stock on the first
# run (token lookup + candles); the token lookups are cached between runs.
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
# All are built from NSE's 1-minute candles (the 1m ones are used as they are).
SLOTS = [
    ("15m", 915), ("3m", 915), ("1m", 915),
    ("15m", 1500), ("15m", 1515),
    ("3m", 1524), ("3m", 1527),
    ("1m", 1528), ("1m", 1529),
]

SLOT_KEYS = [f"{tf}_{hm:04d}" for tf, hm in SLOTS]


# =============================================================================
# BUILT-IN FALLBACK UNIVERSE  (symbols only)
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

# Old symbols that are now listed under a different NSE symbol. Only matters
# for the built-in fallback list; NSE's own CSVs already use current symbols.
# Edit if any of these are listed differently on NSE.
SYMBOL_ALIASES = {
    "ZOMATO": "ETERNAL",
    "L&TFH": "LTF",
    "TATAMOTORS": "TMPV",
}

FALLBACK_UNIVERSE = list(dict.fromkeys(
    SYMBOL_ALIASES.get(s, s) for s in NIFTY_50 + NIFTY_NEXT_150
))


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
        "Referer": NSE_WWW_HOST + "/",
    })

    try:
        session.get(NSE_WWW_HOST + "/", timeout=15)
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
# CLEAN CANDLE DATA
# =============================================================================

def clean_candle_data(df):
    """A frame indexed by candle start time (IST) with Open/High/Low/Close/
    Volume -> a tidy frame (date, hm, open, high, low, close, volume) holding
    only regular-session candles that can be trusted."""

    if df is None or df.empty:
        return None

    df = df.copy().reset_index()

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

    if ts.dt.tz is not None:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    else:
        ts = ts.dt.tz_localize("Asia/Kolkata")

    if not all(c in df.columns for c in ("Open", "High", "Low", "Close", "Volume")):
        return None

    out = pd.DataFrame({
        "date": ts.dt.strftime("%Y-%m-%d").values,
        "hm": (ts.dt.hour * 100 + ts.dt.minute).values,
        "open": pd.to_numeric(df["Open"], errors="coerce").values,
        "high": pd.to_numeric(df["High"], errors="coerce").values,
        "low": pd.to_numeric(df["Low"], errors="coerce").values,
        "close": pd.to_numeric(df["Close"], errors="coerce").values,
        "volume": pd.to_numeric(df["Volume"], errors="coerce").values,
        "est": (
            df["Estimated"].astype(bool).values if "Estimated" in df.columns else False
        ),
    })

    # Regular NSE session only (09:15 - 15:29; the last 1-minute candle is 15:29).
    out = out[out["hm"].between(915, 1529)]

    out = out.dropna(subset=["open", "high", "low", "close", "volume"])

    # NSE ticks are 0.01 or larger, so 2 decimals keep every real price and
    # remove any float noise.
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
# NSE CHARTING SOURCE   (the only data source)
# =============================================================================
#
# NSE's own chart site serves candles through two JSON endpoints, both called
# with plain GET requests (parameters in the URL query string) on the
# charting host (see NSE_CHARTING_HOST):
#   1) symbolsDynamic       : symbol -> scripcode (token)
#   2) symbolHistoricalData : token + time range + interval -> candles
# What NSE's chart page sends, and what this code copies (as of Nov 2026):
#   * the session cookies are collected from the main site (NSE_WWW_HOST) and
#     sent as a Cookie header;
#   * the history "token" is base64("<scripcode>-<DDMMYYYY>") with today's IST
#     date. A bare scripcode is answered with {"status": true, "data": []};
#   * intraday timeInterval is in SECONDS (60 = one minute), and fromDate /
#     toDate are IST-shifted Unix seconds (real time + 5h30m); history is cut
#     by whole sessions, so the range starts at an IST midnight;
#   * each candle's time is IST-shifted epoch milliseconds, so it reads as
#     India clock time. Older days are stamped at the minute's last trade
#     (09:15:55), the latest day at the minute's start (09:15:00): every stamp
#     is floored to its minute, exact repeats are dropped and other rows of the
#     same minute are merged;
#   * rows from 15:30 on are dropped; one row near the close carries the day's
#     total volume and is corrected to an estimate (see nse_fix_day_total).
# It is unofficial and undocumented, so it can change or be blocked.
#
# Only 1-minute candles are downloaded (NSE's own 3m / 15m bars are unreliable);
# the 15-minute and 3-minute candles are built from them on the 09:15 grid.
# Before a scan starts, the day shape is checked on liquid reference stocks
# (see nse_preflight).

NSE_SEARCH_URL = NSE_CHARTING_HOST + "/v1/exchanges/symbolsDynamic"
NSE_HISTORY_URL = NSE_CHARTING_HOST + "/v1/charts/symbolHistoricalData"
NSE_HOME_URL = NSE_WWW_HOST
NSE_WARMUP_URL = NSE_WWW_HOST + "/get-quote/equity/RELIANCE/Reliance-Industries-Limited"

# NSE's chart timestamps and dates are IST-shifted: real Unix time + 5h30m.
IST_OFFSET_SECONDS = 5 * 3600 + 30 * 60

# Liquid stocks used (in order) for the one-time check of NSE's candle times.
NSE_REFERENCE_SYMBOLS = ("RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "SBIN")

NSE_CHART_HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": NSE_WWW_HOST + "/",
    "X-Requested-With": "XMLHttpRequest",
}

NSE_PAGE_HEADERS = {
    "User-Agent": NSE_CHART_HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
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
_nse_calibration = {"done": False, "ok": None, "shift": NSE_TIME_SHIFT_MINUTES, "detail": ""}


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
    """One requests session per worker thread. The session cookies NSE sets on
    its main site are collected once and then sent as an explicit Cookie header
    on every API call (the way NSE's chart page does), because the API lives on
    another host (charting.*) where a cookie jar would not send them."""

    session = getattr(_nse_local, "session", None)

    if session is None or refresh:
        session = requests.Session()
        session.headers.update(NSE_CHART_HEADERS)

        for page in (NSE_HOME_URL + "/", NSE_WARMUP_URL):
            try:
                nse_throttle()
                response = session.get(
                    page, headers=NSE_PAGE_HEADERS, timeout=REQUEST_TIMEOUT,
                    allow_redirects=False,
                )

                if response.status_code in (301, 302, 303, 307, 308):
                    _nse_local.moved = response.headers.get("Location", "?")
            except Exception:
                pass

        _nse_local.cookie_header = "; ".join(
            f"{cookie.name}={cookie.value}" for cookie in session.cookies
        )
        session.cookies.clear()
        _nse_local.session = session

    return session


def nse_get(url, params=None):
    """GET a JSON reply from NSE's chart API (parameters go in the query
    string, session cookies in a Cookie header). Requests are throttled; rate
    limits and blocks are retried after a wait. A redirect is never followed
    (NSE moved hosts in Nov 2026 and a followed redirect loses the cookies): it
    raises at once, naming the new host. Raises RuntimeError with a readable
    reason if every attempt fails."""

    last_error = "NSE request failed"
    refresh = False

    for attempt in range(MAX_RETRIES):

        try:
            nse_throttle()

            with _nse_slots:
                session = nse_session(refresh)
                cookie_header = getattr(_nse_local, "cookie_header", "")
                moved = getattr(_nse_local, "moved", None)

                response = session.get(
                    url, params=params,
                    headers={"Cookie": cookie_header} if cookie_header else None,
                    timeout=REQUEST_TIMEOUT, allow_redirects=False,
                )

            refresh = False
            status = response.status_code

            if status in (301, 302, 303, 307, 308):
                where = response.headers.get("Location", moved or "?")
                raise RuntimeError(
                    f"NSE redirected the request to {where}: NSE has moved hosts - "
                    f"update NSE_WWW_HOST / NSE_CHARTING_HOST in the CONFIGURATION section"
                )

            if status in (401, 403):
                n_cookies = len([c for c in cookie_header.split(";") if c.strip()])
                last_error = (
                    f"NSE refused the request (HTTP {status}, {n_cookies} session cookies); "
                    f"NSE is probably rate-limiting or blocking this network"
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


def nse_search(symbol):
    """Search NSE's chart site for a symbol: (reply, items). All segments are
    searched; the exact-match check in nse_lookup then picks the equity."""

    result = nse_get(NSE_SEARCH_URL, {"segment": "", "symbol": symbol.upper()})

    if isinstance(result, list):
        return result, result

    if isinstance(result, dict) and isinstance(result.get("data"), list):
        return result, result["data"]

    return result, []


def nse_lookup(symbol):
    """(token, chart_symbol, symbol_type) for an equity. Raises NseNoData with a
    reason if NSE has no exact match (a stock is never guessed from a partial
    match) and RuntimeError if NSE's reply is not understood."""

    with _nse_lock:
        if symbol in _nse_tokens and _nse_tokens[symbol]:
            return _nse_tokens[symbol]

        if symbol in _nse_misses:
            raise NseNoData(_nse_misses[symbol])

    result, items = nse_search(symbol)

    if items:
        pass
    elif isinstance(result, dict) and result.get("status") is False:
        detail = f"NSE symbol search said status=false | e.g. {symbol}: {snippet(result)}"

        with _nse_lock:
            _nse_misses[symbol] = detail

        raise NseNoData(detail)
    elif not isinstance(result, (dict, list)):
        raise RuntimeError(
            f"NSE symbol search gave an unexpected reply | e.g. {symbol}: {snippet(result)}"
        )

    if not items:
        detail = f"NSE symbol search returned no results | e.g. {symbol}: {snippet(result)}"

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


def nse_chart_token(scripcode, now=None):
    """The `token` NSE's history endpoint takes: base64("<scripcode>-<DDMMYYYY>")
    with today's IST date, the way NSE's chart page sends it. A bare scripcode is
    answered with {"status": true, "data": []} and no candles."""

    now = time.time() if now is None else now
    ist_date = datetime.datetime.fromtimestamp(
        now + IST_OFFSET_SECONDS, datetime.timezone.utc
    ).strftime("%d%m%Y")

    return base64.b64encode(f"{scripcode}-{ist_date}".encode()).decode()


def nse_fix_day_total(frame):
    """NSE's intraday volume is per minute, except that one row near the end of
    each day (15:28 or 15:29, sometimes elsewhere) carries roughly the WHOLE
    day's volume. When a day's largest row is at least half of all the other
    rows together and over three times any other row, its volume is replaced by
    what is left after subtracting the day's other rows (never below 0) and the
    row is marked Estimated: NSE's day total does not line up exactly with the
    rows. The row is found by size, not position. All rows of the day count
    here, including pre-open ones."""

    frame = frame.reset_index(drop=True)

    for _, index in frame.groupby(frame["minute"].dt.date).groups.items():
        if len(index) < 3:
            continue

        volumes = frame.loc[index, "Volume"].fillna(0.0)
        biggest = volumes.idxmax()
        total = float(volumes[biggest])
        others = volumes.drop(biggest)

        if total >= 0.5 * float(others.sum()) and total > 3.0 * float(others.max()):
            frame.loc[biggest, "Volume"] = max(total - float(others.sum()), 0.0)
            frame.loc[biggest, "Estimated"] = True

    return frame


def nse_candles_to_frame(data, shift=0):
    """NSE 1-minute rows -> a frame (Datetime index = the minute the candle
    STARTS, in IST; Open/High/Low/Close/Volume/Estimated) that clean_candle_data
    understands. Every stamp is floored to its minute, exact repeats are
    dropped, other rows of the same minute are merged, rows from 15:30 on are
    dropped and the day-total row is corrected (nse_fix_day_total). `shift`
    (minutes) is a manual correction (NSE_TIME_SHIFT_MINUTES). Raises
    ValueError when the times cannot be read."""

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

    def in_session(values):
        minute_of_day = values.dt.hour * 60 + values.dt.minute
        return ((minute_of_day >= 9 * 60) & (minute_of_day <= 15 * 60 + 35)).mean()

    # NSE writes India clock time as if it were UTC. If the stamps only look
    # like trading hours after +05:30, they are real UTC.
    if in_session(stamp) < 0.5 and in_session(stamp + pd.Timedelta(hours=5, minutes=30)) >= 0.9:
        stamp = stamp + pd.Timedelta(hours=5, minutes=30)
    elif in_session(stamp) < 0.9:
        raise ValueError(
            f"NSE candle times do not fall inside trading hours | e.g. raw times "
            f"{raw.head(3).tolist()} ... {raw.tail(1).tolist()}, unit {unit}, "
            f"first {stamp.iloc[0]}, last {stamp.iloc[-1]}"
        )

    frame = pd.DataFrame({
        "minute": stamp.dt.floor("min"),
        "Open": pd.to_numeric(df["open"], errors="coerce"),
        "High": pd.to_numeric(df["high"], errors="coerce"),
        "Low": pd.to_numeric(df["low"], errors="coerce"),
        "Close": pd.to_numeric(df["close"], errors="coerce"),
        "Volume": pd.to_numeric(df["volume"], errors="coerce"),
    })

    # One bar per minute: exact repeats dropped, any other rows of the same
    # minute folded together (first open, highest high, lowest low, last close,
    # summed volume).
    frame = frame.sort_values("minute", kind="stable").drop_duplicates()
    frame = (
        frame.groupby("minute", sort=True)
        .agg(Open=("Open", "first"), High=("High", "max"), Low=("Low", "min"),
             Close=("Close", "last"), Volume=("Volume", "sum"))
        .reset_index()
    )

    # Trades after the close are not part of the day total.
    minute_of_day = frame["minute"].dt.hour * 60 + frame["minute"].dt.minute
    frame = frame[minute_of_day < MARKET_CLOSE_MINUTES].copy()

    if frame.empty:
        return None

    frame["Estimated"] = False
    frame = nse_fix_day_total(frame)

    index = pd.DatetimeIndex(
        frame["minute"] + pd.Timedelta(minutes=shift)
    ).tz_localize("Asia/Kolkata")
    index.name = "Datetime"

    out = frame[["Open", "High", "Low", "Close", "Volume", "Estimated"]].copy()
    out.index = index

    return out


def nse_extract_candles(result):
    """NSE's history reply -> (candles, rest). candles is a list of dicts with
    time/open/high/low/close/volume (empty when the reply holds none); rest is
    the reply's other fields (e.g. {"status": true}). Understands a list of
    candles, {"data": [...]}, and the column format {"t": [...], "o": [...], ...}."""

    keys = ("t", "o", "h", "l", "c", "v")
    rest, data = {}, None

    if isinstance(result, list):
        data = result
    elif isinstance(result, dict):
        data = result.get("data")
        rest = {k: v for k, v in result.items() if k != "data"}

        if data is None and all(isinstance(result.get(k), list) for k in keys):
            data = {k: result[k] for k in keys}
            rest = {k: v for k, v in result.items() if k not in keys}
    else:
        raise RuntimeError(f"NSE history gave an unexpected reply: {snippet(result)}")

    if isinstance(data, dict) and all(isinstance(data.get(k), list) for k in keys):
        data = [
            {"time": t, "open": o, "high": h, "low": l, "close": c, "volume": v}
            for t, o, h, l, c, v in zip(*(data[k] for k in keys))
        ]
    elif isinstance(data, list) and data and isinstance(data[0], (list, tuple)) and len(data[0]) >= 6:
        names = ("time", "open", "high", "low", "close", "volume")
        data = [dict(zip(names, row[:6])) for row in data]

    return (data if isinstance(data, list) else []), rest


def nse_raw_minutes(symbol, shift):
    """NSE's 1-minute candles for one stock as an uncleaned frame (see
    nse_candles_to_frame). Raises NseNoData (NSE answered but has nothing for
    this stock) or RuntimeError / ValueError (a real error)."""

    token, chart_symbol, symbol_type = nse_lookup(symbol)

    now_shifted = int(time.time()) + IST_OFFSET_SECONDS
    # History is cut by whole sessions: start at an IST midnight.
    first_day = ((now_shifted - NSE_HISTORY_DAYS * 86400) // 86400) * 86400

    result = nse_get(NSE_HISTORY_URL, {
        "chartType": "I",
        "fromDate": first_day,
        "symbol": chart_symbol,
        "symbolType": symbol_type,
        "timeInterval": 60,                 # seconds: one-minute candles
        "toDate": now_shifted,
        "token": nse_chart_token(token),
    })

    data, rest = nse_extract_candles(result)

    if not data or rest.get("status") is False:
        raise NseNoData(
            f"NSE history returned no candles | e.g. {symbol}: {snippet(rest)}"
        )

    frame = nse_candles_to_frame(data, shift)

    if frame is None:
        raise NseNoData(f"NSE history had no usable timestamps | e.g. {symbol}")

    return frame


def nse_fetch_minutes(symbol, shift):
    """1-minute candles for one stock from NSE: (rows, info). rows is a cleaned
    DataFrame (start-labelled minutes), or None with info "NO_DATA: <reason>"
    (NSE answered but has nothing for this stock) or an error message. Every
    failure carries a specific reason."""

    try:
        frame = nse_raw_minutes(symbol, shift)
        cleaned = clean_candle_data(frame)

        if cleaned is None or cleaned.empty:
            index = frame.index
            minute_of_day = index.hour * 60 + index.minute
            outside = int(((minute_of_day < 555) | (minute_of_day > 929)).sum())
            zero_volume = int((frame["Volume"].fillna(0) <= 0).sum())

            # Nothing traded at all in the whole window: no data, not an error.
            if zero_volume == len(frame):
                raise NseNoData(f"no traded candles in the last {NSE_HISTORY_DAYS} days | e.g. {symbol}")

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


def day_structure(rows):
    """[(date, candle_count, first_hm, last_hm), ...] per session, oldest first."""

    return [
        (date, len(group), int(group["hm"].min()), int(group["hm"].max()))
        for date, group in rows.groupby("date", sort=True)
    ]


def nse_preflight():
    """One-time check, before a scan, that NSE's candle times line up with the
    real session: on a liquid reference stock a full session (300+ candles)
    must start at 09:15 and end at 15:28 or 15:29. Returns True when verified. When it
    cannot be verified the scan must not run: there is no other source to fall
    back to, and a time offset would silently read the wrong candles."""

    if _nse_calibration["done"]:
        return bool(_nse_calibration["ok"])

    _nse_calibration["done"] = True
    context = get_run_context()
    shift = _nse_calibration["shift"]
    problems = []

    print()
    print("Checking NSE candle times against the 09:15-15:29 session shape...")

    for symbol in NSE_REFERENCE_SYMBOLS:
        rows, info = nse_fetch_minutes(symbol, shift)

        if rows is None:
            problems.append(f"{symbol}: {info}"[:220])
            continue

        full = [
            d for d in day_structure(rows)
            if d[1] >= 300 and (d[0] != context["today"] or context["today_complete"])
        ]

        if not full:
            problems.append(f"{symbol}: no complete session in the data returned")
            continue

        for date, count, first, last in full[-2:][::-1]:
            # The very last minute can be missing: NSE's day-total row sits at
            # 15:28 or 15:29 and is dropped when its corrected volume is 0.
            if first == 915 and last in (1528, 1529):
                _nse_calibration.update(
                    ok=True,
                    detail=(
                        f"NSE candle times verified on {symbol} ({date}: {count} candles, "
                        f"{hm_text(first)} to {hm_text(last)}; manual shift {shift:+d} min)"
                    ),
                )
                print("NSE CHECK:", _nse_calibration["detail"])
                return True

        date, count, first, last = full[-1]
        problems.append(
            f"{symbol}: {date} runs {hm_text(first)}-{hm_text(last)} "
            f"({count} candles) instead of 09:15-15:29"
        )

    detail = "; ".join(problems)[:600]

    _nse_calibration.update(
        ok=False,
        detail="NSE candle times could not be verified: " + detail,
    )

    print()
    print("NSE CHECK FAILED - the scan will not run.")
    print("  " + detail)
    print("  Run  python scanner.py --nse-debug RELIANCE  and see what NSE sends back.")
    print("  If the session is shifted by a constant number of minutes, set")
    print("  NSE_TIME_SHIFT_MINUTES in the CONFIGURATION section and run again.")

    return False


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
    in between stop the main scan (the retry rounds pick those stocks up
    again). The scan also stops if NSE has served nothing at all after
    NSE_GIVE_UP_AFTER stocks."""

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
                        f"- pausing the main scan; the remaining stocks are retried later."
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
                f"- stopping the main scan."
            )

    if message:
        print()
        print("NOTE:", message)


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

    today_complete is True on a weekday once the market has closed and NSE
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


def window_minutes(start_hm, minutes):
    """Start times of the 1-minute candles inside the candle that starts at
    start_hm, e.g. (1524, 3) -> [1524, 1525, 1526]."""

    start = (start_hm // 100) * 60 + start_hm % 100

    return [((start + k) // 60) * 100 + (start + k) % 60 for k in range(minutes)]


def resample_from_1m(rows, minutes):
    """Build N-minute candles from 1-minute rows (NSE's own 15-minute candles
    are not on the 09:15 grid the strategy needs, and there is no 3-minute feed).

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


def evaluate_rows(rows):
    """15m / 3m / 1m closing-candle strategy, read on the previous day
    (P = the latest completed session). Entry day = the next trading day.
    `rows` is the cleaned 1-minute frame from NSE; the 3m and 15m candles are
    built from the previous day's 1-minute candles.

    1) 15m 15:00 volume is higher than 15m 15:15 volume.
    2) 3m 15:24 volume is higher than 3m 15:27 volume.
    3) 3m 15:24 and 3m 15:27 candles have the same trend.
    4) 1m 15:28 volume is higher than 1m 15:29 volume.
    5) 1m 15:28 and 1m 15:29 candles have the same trend.
    6) 15m 15:00, 3m 15:24 and 1m 15:28 candles all have the same trend.
    7) 15m 09:15, 3m 09:15 and 1m 09:15 candles have that same trend too.
    (The 15m timeframe does not require its last two candles to be opposite.)
    """

    if rows is None or rows.empty:
        return {"status": "NO_DATA"}

    # Which session is "previous" is decided once per run (see
    # build_run_context): today's session counts only once it is complete.
    ctx = get_run_context()
    entry = ctx["entry"]
    excluded = set() if ctx["today_complete"] else {ctx["today"]}
    dates = sorted(set(rows["date"].unique()) - excluded)

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
    day_rows = rows[rows["date"] == previous_day]

    by_tf = {
        "1m": day_map(day_rows),
        "3m": day_map(resample_from_1m(day_rows, 3)),
        "15m": day_map(resample_from_1m(day_rows, 15)),
    }

    # 1-minute rows whose volume NSE's day-total correction had to estimate.
    estimated_minutes = set()

    if "est" in day_rows.columns:
        estimated_minutes = {int(h) for h in day_rows.loc[day_rows["est"].astype(bool), "hm"]}

    minutes_in = {"15m": 15, "3m": 3, "1m": 1}

    candles, missing = {}, []

    for tf, hm in SLOTS:
        key = f"{tf}_{hm:04d}"
        bar = by_tf.get(tf, {}).get(previous_day, {}).get(hm)

        if bar is None:
            candles[key] = None
            missing.append(slot_label(key))
        else:
            o, h, l, c, v = bar
            candles[key] = {
                "open": o, "close": c, "volume": int(round(v)),
                "est": bool(estimated_minutes & set(window_minutes(hm, minutes_in[tf]))),
            }

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

    t15_1500, t15_1515 = trend("15m_1500"), trend("15m_1515")
    t3_24, t3_27 = trend("3m_1524"), trend("3m_1527")
    t1_28, t1_29 = trend("1m_1528"), trend("1m_1529")
    t15_0915, t3_0915, t1_0915 = trend("15m_0915"), trend("3m_0915"), trend("1m_0915")

    v15_1500, v15_1515 = volume("15m_1500"), volume("15m_1515")
    v3_24, v3_27 = volume("3m_1524"), volume("3m_1527")
    v1_28, v1_29 = volume("1m_1528"), volume("1m_1529")

    def is_est(key):
        return bool(candles[key] and candles[key].get("est"))

    def higher(first, second, estimated=False):
        # Both candles must exist: a missing candle can only block a match.
        if first is None or second is None:
            return False

        # Optionally refuse a volume comparison that rests on an estimate.
        if estimated and ESTIMATED_VOLUME_BLOCKS_SIGNAL:
            return False

        return first > second

    # 15m: volume only (its last two candles may have the same or opposite
    # trends). 3m and 1m: volume AND the last two candles must have the same
    # trend (a flat or missing candle has trend 0, so it is never "the same").
    vol_15m = higher(v15_1500, v15_1515, is_est("15m_1500") or is_est("15m_1515"))
    vol_3m = higher(v3_24, v3_27, is_est("3m_1524") or is_est("3m_1527"))
    vol_1m = higher(v1_28, v1_29, is_est("1m_1528") or is_est("1m_1529"))

    volume_keys = ("15m_1500", "15m_1515", "3m_1524", "3m_1527", "1m_1528", "1m_1529")
    trend_3m = t3_24 != 0 and t3_24 == t3_27
    trend_1m = t1_28 != 0 and t1_28 == t1_29

    cond_15m = vol_15m
    cond_3m = vol_3m and trend_3m
    cond_1m = vol_1m and trend_1m

    # Second-last candles of all three timeframes share one (non-flat) trend.
    cond_align = t15_1500 != 0 and t15_1500 == t3_24 == t1_28

    # The morning (09:15) candles of all three timeframes have that same trend.
    cond_morning = cond_align and t15_0915 == t3_0915 == t1_0915 == t15_1500

    cond = cond_15m and cond_3m and cond_1m and cond_align and cond_morning

    # Direction anchors on the alignment trend: when cond is True the 15m 15:00,
    # 3m 15:24 and 1m 15:28 candles all share it.
    side = t15_1500 * DIRECTION_RULES[DIRECTION_RULE]
    direction = "LONG" if side == 1 else "SHORT" if side == -1 else None

    one_day = by_tf["1m"].get(previous_day, {})

    return {
        "status": "PASS" if cond else "FAIL",
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
        "trend_3m": trend_3m,
        "vol_1m": vol_1m,
        "trend_1m": trend_1m,
        "missing": missing,
        "candles": candles,
        "estimated": [slot_label(k) for k in SLOT_KEYS if candles[k] and candles[k].get("est")],
        "vol_estimated": any(is_est(k) for k in volume_keys),
        "tail_1m": sorted(h for h in one_day if h >= 1520),
        "details": {
            "t15_1500": t15_1500, "t15_1515": t15_1515,
            "t3_24": t3_24, "t3_27": t3_27,
            "t1_28": t1_28, "t1_29": t1_29,
            "t15_0915": t15_0915, "t3_0915": t3_0915, "t1_0915": t1_0915,
        },
    }


# =============================================================================
# SCAN ONE SYMBOL
# =============================================================================

def scan_one_symbol(symbol):

    # Small random delay so the workers do not hit NSE in one burst.
    time.sleep(random.uniform(0.05, 0.25))

    try:
        nse_wait_if_paused()

        if _nse_state["blocked"]:
            return {
                "symbol": symbol,
                "status": "ERROR",
                "data_status": "ERROR",
                "error": "NSE paused for this run after repeated refusals (retried later)",
                "source": "NSE",
            }

        rows, info = nse_fetch_minutes(symbol, _nse_calibration["shift"])

        if rows is None:
            nse_record("soft" if is_no_data(info) else "hard")
            nse_note_failure(info)

            if is_no_data(info):
                return {
                    "symbol": symbol,
                    "status": "NO_DATA",
                    "data_status": "NO_DATA",
                    "source": "NSE",
                }

            return {
                "symbol": symbol,
                "status": "ERROR",
                "data_status": "ERROR",
                "error": info,
                "source": "NSE",
            }

        nse_record("ok")

        result = evaluate_rows(rows)
        result["symbol"] = symbol
        result["data_status"] = info
        result["source"] = "NSE"

        return result

    except Exception as e:
        return {
            "symbol": symbol,
            "status": "ERROR",
            "data_status": "ERROR",
            "error": f"{type(e).__name__}: {e}"[:240],
            "source": "NSE",
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


def retry_failed(results):
    """Retry stocks that errored (rate limits, network trouble), waiting first
    and using fewer workers. No stock may be lost to a technical failure.
    Returns the updated results."""

    by_symbol = {r["symbol"]: r for r in results}

    for round_number in range(1, RETRY_ROUNDS + 1):
        errored = sorted(s_ for s_, r in by_symbol.items() if r.get("status") == "ERROR")

        if not errored:
            break

        if _nse_state["successes"] == 0:
            print()
            print("NSE served nothing in this run, so the errored stocks are not retried.")
            break

        wait = RETRY_WAIT_SECONDS[min(round_number - 1, len(RETRY_WAIT_SECONDS) - 1)]

        print()
        print("=" * 70)
        print(
            f"RETRY {round_number}/{RETRY_ROUNDS}: {len(errored)} stocks errored. "
            f"Waiting {wait}s, then trying again with {RETRY_WORKERS} workers."
        )
        print("=" * 70)

        time.sleep(wait)

        # NSE gets a fresh start after the wait.
        with _nse_lock:
            _nse_state.update(
                failures=0, pause_until=0.0, cooldowns=0,
                cooldown_s=NSE_COOLDOWN_SECONDS, blocked=False,
            )

        recovered = 0

        with ThreadPoolExecutor(max_workers=RETRY_WORKERS) as executor:
            for sym, result in zip(errored, executor.map(scan_one_symbol, errored)):
                if result.get("status") != "ERROR":
                    recovered += 1

                by_symbol[sym] = result

        print(f"Retry {round_number}: {recovered} stocks recovered.")

    return sorted(by_symbol.values(), key=lambda r: r.get("symbol", ""))


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
    """Warn about broad data-quality problems: many symbols missing one of the
    9 required candles. Returns text or None."""

    evaluated = [r for r in results if r.get("status") in ("PASS", "FAIL")]

    if not evaluated:
        return None

    complete = sum(1 for r in evaluated if not r.get("missing"))
    share = complete / len(evaluated)

    if share < SESSION_COMPLETE_SHARE:
        return (
            f"Only {share:.0%} of scanned symbols have all nine required candles "
            f"(09:15 on 15m, 3m and 1m; 15m 15:00 and 15:15; 3m 15:24 and 15:27; "
            f"1m 15:28 and 15:29) on the previous day. "
            f"The data may be unusually incomplete - treat matches with "
            f"extra caution and verify on a chart."
            + (
                " NSE can lag a few minutes after the close; if you just ran "
                "this, try again shortly."
                if get_run_context()["today_complete"] else ""
            )
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
    "PASS": "PASS", "FAIL": "FAIL", "INCOMPLETE": "INCOMPLETE",
    "STALE": "STALE", "NO_DATA": "NO DATA", "ERROR": "ERROR",
}

STATUS_RANK = {
    "PASS": 0, "FAIL": 1, "INCOMPLETE": 2, "STALE": 3, "ERROR": 4, "NO_DATA": 5,
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
.tag {
    display: inline-block; margin-left: 8px; padding: 2px 6px; border-radius: 4px;
    font-size: 11px; font-weight: bold;
}
.tag.warn { background: #fff3cd; color: #6b5200; }
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
        + (" (est.)" if candle.get("est") else "")
    )


def part_badge(value):
    """Small PASS/FAIL text for one part (trend or volume) of a condition."""

    if value is None:
        return "-"

    return "PASS" if value else "FAIL"


def final_signals(results):
    """Every signal to show: each stock that passes all seven rules on NSE data."""

    signals = [
        {
            "symbol": r["symbol"],
            "direction": r["direction"],
            "previous_day": r.get("previous_day"),
            "vol_estimated": bool(r.get("vol_estimated")),
        }
        for r in results if r.get("status") == "PASS"
    ]

    signals.sort(key=lambda x: (x["direction"] != "LONG", x["symbol"]))

    return signals


def build_funnel(results):
    """Where stocks drop out, step by step. Returns [(label, count), ...];
    each line counts only the stocks that survived every line above it. It
    separates data problems (download, missing candles) from the strategy
    simply being strict."""

    got_data = [r for r in results if r.get("status") not in ("NO_DATA", "ERROR")]
    evaluated = [r for r in results if "cond_15m" in r]
    full = [r for r in evaluated if not r.get("missing")]
    step_1 = [r for r in full if r.get("vol_15m")]
    step_2 = [r for r in step_1 if r.get("vol_3m")]
    step_3 = [r for r in step_2 if r.get("trend_3m")]
    step_4 = [r for r in step_3 if r.get("vol_1m")]
    step_5 = [r for r in step_4 if r.get("trend_1m")]
    step_6 = [r for r in step_5 if r.get("cond_align")]
    step_7 = [r for r in step_6 if r.get("cond_morning")]
    step_8 = [r for r in step_7 if r.get("status") != "STALE"]

    return [
        ("Stocks scanned", len(results)),
        ("Data downloaded (no error / no-data)", len(got_data)),
        ("A completed session found", len(evaluated)),
        ("All nine candles present", len(full)),
        ("15m: 15:00 volume > 15:15 volume", len(step_1)),
        ("3m: 15:24 volume > 15:27 volume", len(step_2)),
        ("3m: 15:24 and 15:27 candles same trend", len(step_3)),
        ("1m: 15:28 volume > 15:29 volume", len(step_4)),
        ("1m: 15:28 and 15:29 candles same trend", len(step_5)),
        ("Same trend: 15m 15:00 = 3m 15:24 = 1m 15:28", len(step_6)),
        ("Morning 09:15 candles (15m, 3m, 1m) share that trend", len(step_7)),
        ("SIGNALS SHOWN (same session as most stocks)", len(step_8)),
    ]


def missing_summary(results):
    """How many stocks lack each of the nine candles.
    Returns (per_slot, evaluated_count)."""

    evaluated = [r for r in results if "cond_15m" in r]
    counts = Counter()

    for r in evaluated:
        for label in r.get("missing") or []:
            counts[label] += 1

    per_slot = [(slot_label(k), counts.get(slot_label(k), 0)) for k in SLOT_KEYS]

    return per_slot, len(evaluated)


def source_health(results):
    """Is NSE working? Returns (lines, level): level is "ok", "partial" or "bad"."""

    served = sum(1 for r in results if r.get("status") not in ("NO_DATA", "ERROR"))
    errors = sum(1 for r in results if r.get("status") == "ERROR")

    lines = [f"NSE served {served} stocks; {errors} could not be read (errors)."]

    if _nse_calibration["detail"]:
        lines.append(_nse_calibration["detail"])

    if _nse_state["pauses"]:
        lines.append(
            f"NSE rate-limited this scan {_nse_state['pauses']} time(s); the scanner "
            f"paused for {_nse_state['paused_seconds']:.0f}s in total and carried on."
        )

    if served == 0:
        level = "bad"
        lines.append(
            "NSE served nothing. Run  python scanner.py --nse-debug RELIANCE  "
            "to see what NSE sends back."
        )
    elif errors == 0:
        level = "ok"
        lines.append("NSE is working: every stock was read.")
    elif errors <= 0.10 * len(results):
        level = "ok"
        lines.append("NSE is working: only a few stocks could not be read.")
    else:
        level = "partial"
        lines.append("NSE is only partly working: many stocks could not be read.")

    with _nse_lock:
        reasons = _nse_errors.most_common(5)
        examples = dict(_nse_examples)

    if reasons:
        lines.append("Why NSE had nothing for some stocks (top reasons):")

        for message, count in reasons:
            lines.append(f"{count} x {message}")

            if message in examples:
                lines.append(f"      e.g. {examples[message]}")

    return lines, level


def failed_rules(result):
    """Names of the rules a result fails (plus data problems)."""

    if "cond_15m" not in result:
        return ["no usable data"]

    checks = (
        ("vol_15m", "15m volume (15:00 > 15:15)"),
        ("vol_3m", "3m volume (15:24 > 15:27)"),
        ("trend_3m", "3m trend (15:24 and 15:27 same)"),
        ("vol_1m", "1m volume (15:28 > 15:29)"),
        ("trend_1m", "1m trend (15:28 and 15:29 same)"),
        ("cond_align", "alignment (15m 15:00 = 3m 15:24 = 1m 15:28)"),
        ("cond_morning", "morning 09:15 candles"),
    )

    failed = [label for key, label in checks if not result.get(key)]

    if result.get("missing"):
        failed.append("missing candles: " + ", ".join(result["missing"]))

    return failed


def closest_misses(results, limit=25):
    """When signals are few (or none): stocks that failed exactly ONE condition
    (every candle present). Returns [(symbol, failed)]."""

    found = []

    for r in results:
        if r.get("status") != "FAIL" or "cond_15m" not in r or r.get("missing"):
            continue

        failed = failed_rules(r)

        if len(failed) == 1:
            found.append((r["symbol"], failed[0]))

    return sorted(found)[:limit]


def funnel_console(results):

    lines, previous = [], None

    for label, count in build_funnel(results):
        lost = f"  (-{previous - count})" if previous is not None and previous > count else ""
        lines.append(f"  {count:>5}  {label}{lost}")
        previous = count

    per_slot, total = missing_summary(results)

    lines.append("")
    lines.append(f"  Stocks missing each candle (of {total} with a session):")

    for label, count in per_slot:
        lines.append(f"  {count:>5}  {label}")

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

    per_slot, total = missing_summary(results)

    missing_body = "".join(
        f'<tr><td>{esc(label)}</td><td>{count}</td></tr>' for label, count in per_slot
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

    return (
        '<div class="match-box"><h2>Where stocks dropped out</h2>'
        '<div class="table-wrap"><table class="funnel"><tbody>'
        f'{body}</tbody></table></div>'
        f'<h2 style="margin-top:22px">Stocks missing each candle (of {total} with a session)</h2>'
        '<div class="table-wrap"><table class="funnel"><tbody>'
        f'{missing_body}</tbody></table></div>'
        f'{closest_html}</div>'
    )


def unscanned_lines(results, limit=40):
    """Stocks whose result cannot be trusted, by name: technical ERRORs and
    STALE sessions."""

    lines = []

    for status, meaning in (
        ("ERROR", "could NOT be scanned (technical failure) and may hide a signal"),
        ("STALE", "were read on a different session than most stocks"),
    ):
        items = sorted(r["symbol"] for r in results if r.get("status") == status)

        if items:
            shown = ", ".join(items[:limit])
            more = f" ... and {len(items) - limit} more" if len(items) > limit else ""
            lines.append(f"{len(items)} stocks {meaning}: {shown}{more}")

    return lines


def unscanned_html(results):
    lines = unscanned_lines(results)

    if not lines:
        return ""

    return '<div class="warn">' + "<br>".join(esc(line) for line in lines) + "</div>"


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
            + f'trend {part_badge(r.get("trend_3m"))} / '
            f'volume {part_badge(r.get("vol_3m"))}'
        )
        note_1m = (
            candle_text(candles.get("1m_1528"), "15:28") + "<br>"
            + candle_text(candles.get("1m_1529"), "15:29") + "<br>"
            + f'trend {part_badge(r.get("trend_1m"))} / '
            f'volume {part_badge(r.get("vol_1m"))}'
        )
        note_align = (
            f'15m 15:00 {trend_name(details["t15_1500"])} / '
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
        "OK": "OK", "NO_DATA": "No data", "ERROR": "Error",
    }.get(r.get("data_status"), r.get("data_status"))

    data_text = cell(data_label) + " (NSE)"

    if r.get("missing"):
        data_text += (
            "<br><small>missing: "
            + ", ".join(esc(m) for m in r["missing"]) + "</small>"
        )

    if r.get("estimated"):
        data_text += (
            "<br><small>volume estimated: "
            + ", ".join(esc(m) for m in r["estimated"]) + "</small>"
        )

    if r.get("error"):
        data_text += f"<br><small>{esc(r['error'][:120])}</small>"

    result_label = STATUS_LABEL.get(status, status)

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
<td class="{status_class}">{esc(result_label)}</td>
<td>{data_text}</td>
</tr>
"""


# =============================================================================
# GENERATE HTML
# =============================================================================

def generate_html_report(results, elapsed, universe_source, run_note=None):

    def count(name):
        return sum(1 for r in results if r.get("status") == name)

    matches = final_signals(results)

    longs = sum(1 for r in matches if r.get("direction") == "LONG")
    shorts = len(matches) - longs

    def most_common(field):
        c = Counter(r[field] for r in results if r.get(field)).most_common(1)
        return c[0][0] if c else None

    previous_day = most_common("previous_day")
    entry_day = most_common("entry_day") or get_run_context()["entry"]

    scan_time = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d %H:%M:%S IST")

    health_lines, health_level = source_health(results)
    health_html = (
        f'<div class="{"note" if health_level == "ok" else "warn"}">'
        + "<br>".join(esc(line) for line in health_lines)
        + "</div>"
    )

    warning = session_warning(results)
    warning_html = (
        (f'<div class="note">{esc(run_note)}</div>' if run_note else "")
        + f'<div class="note">{esc(NSE_FEED_NOTE)}</div>'
        + health_html
        + unscanned_html(results)
        + (f'<div class="warn">{esc(warning)}</div>' if warning else "")
    )

    if matches:
        match_html = "".join(
            '<div class="match">'
            f'<span class="direction {"long" if sig["direction"] == "LONG" else "short"}">'
            f'{esc(sig["direction"])}</span>'
            f'<b>{esc(sig["symbol"])}</b>'
            f'<span class="date">Previous day: {esc(sig.get("previous_day", ""))}</span>'
            + (
                '<span class="tag warn" title="A closing-candle volume is NSE\'s '
                'day-total row, corrected to an estimate">volume estimated</span>'
                if sig.get("vol_estimated") else ""
            )
            + '</div>'
            for sig in matches
        )
    else:
        match_html = '<div class="none">No stocks matched.</div>'

    passing = {sig["symbol"] for sig in matches}

    sorted_results = sorted(
        results,
        key=lambda x: (
            0 if x.get("symbol") in passing else 1 + STATUS_RANK.get(x.get("status"), 9),
            x.get("symbol", ""),
        ),
    )

    table_rows = "".join(build_table_row(r) for r in sorted_results)

    stats = [
        (len(results), "Stocks scanned"),
        (len(matches), "Matches"),
        (longs, "Long"),
        (shorts, "Short"),
        (count("FAIL"), "No match"),
        (count("INCOMPLETE"), "Incomplete"),
        (count("STALE"), "Stale"),
        (count("NO_DATA"), "No data"),
        (count("ERROR"), "Errors"),
        (f"{elapsed:.1f}s", "Scan time"),
    ]

    stats_html = "".join(
        f'<div class="stat"><b>{value}</b>{label}</div>' for value, label in stats
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
Data: NSE charting feed only - 1-minute candles, with 3-minute and 15-minute candles built on the 09:15 grid<br>
Rules: {esc(STRATEGY_RULES)}<br>
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
<th>3m condition<br><small>15:24 &amp; 15:27 same trend; 15:24 vol &gt; 15:27 vol</small></th>
<th>1m condition<br><small>15:28 &amp; 15:29 same trend; 15:28 vol &gt; 15:29 vol</small></th>
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
3) 3m trend: the 15:24 and 15:27 candles have the same trend.<br>
4) 1m volume: the 15:28 candle's volume is higher than the 15:29 candle's.<br>
5) 1m trend: the 15:28 and 15:29 candles have the same trend.<br>
6) The 15m 15:00, 3m 15:24 and 1m 15:28 candles all have the same trend.<br>
7) The morning candles (09:15) of the 15m, 3m and 1m timeframes have that same trend too.<br>
The 15m timeframe has no opposite-trend rule: its last two candles may have the same or
opposite trends.<br>
Trend = candle colour (close vs open); a flat candle has no trend, so it is never
the same as another.<br>
{esc(direction_text)}<br>
<b>Entry:</b> the 09:15 open on entry day. <b>Exit:</b> {esc(EXIT_TIME_LABEL)}.<br><br>
<b>Data notes:</b> all candles come from NSE's charting feed; no other source is used. NSE is
downloaded as 1-minute candles, and the 3-minute and 15-minute candles are built from them on a
09:15 grid (open = first, close = last, volume = sum). <b>{esc(NSE_FEED_NOTE)}</b> Prices are rounded to 2 decimals;
zero-volume and corrupt candles are dropped and count as missing, which can only block a
match. INCOMPLETE means no completed session (or none of the nine candles) came back. STALE
means the previous day differs from most other symbols. NO DATA / ERROR mean NSE had nothing
for the stock or could not be reached; those stocks are never filled in from another source.
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

    for key in SLOT_KEYS:
        c = candles.get(key)
        label = slot_label(key)

        if c is None:
            print(f"  {label:<10} missing")
            continue

        d = candle_direction(c["open"], c["close"])
        print(
            f"  {label:<10} {trend_name(d):<5} open {c['open']:.2f}  "
            f"close {c['close']:.2f}  vol {c['volume']:,}"
        )

    if "cond_15m" in r:
        print(f"  15m 15:00 vol > 15:15 vol  : {r['vol_15m']}")
        print(f"  3m  15:24 vol > 15:27 vol  : {r['vol_3m']}")
        print(f"  3m  15:24 and 15:27 same trend: {r['trend_3m']}")
        print(f"  1m  15:28 vol > 15:29 vol  : {r['vol_1m']}")
        print(f"  1m  15:28 and 15:29 same trend: {r['trend_1m']}")
        print(f"  15m 15:00 = 3m 15:24 = 1m 15:28 trend: {r['cond_align']}")
        print(f"  09:15 candles (15m, 3m, 1m) share that trend: {r['cond_morning']}")

    if r.get("direction"):
        print("  Direction:", r["direction"])


def audit_symbols(symbols):

    if not symbols:
        print("Usage: python scanner.py --check SYMBOL [SYMBOL ...]")
        return

    for symbol in symbols:
        print_audit(scan_one_symbol(symbol.strip().upper()))


# =============================================================================
# NSE DEBUG   (python scanner.py --nse-debug RELIANCE)
# =============================================================================

def nse_debug(symbols):
    """Print NSE's raw replies for one stock, and the shape of each session
    after reading, so a problem can be diagnosed. Paste the output when asking
    for help."""

    symbol = (symbols[0] if symbols else "RELIANCE").strip().upper()
    session = nse_session(refresh=True)
    cookie_header = getattr(_nse_local, "cookie_header", "")
    headers = {"Cookie": cookie_header} if cookie_header else None

    print("=" * 78)
    print(f"NSE DEBUG for {symbol}")
    print("=" * 78)
    print(f"1) hosts: www {NSE_WWW_HOST}, charting {NSE_CHARTING_HOST}")
    print(f"   session cookies collected: {[c.split('=')[0].strip() for c in cookie_header.split(';') if c.strip()]}")

    moved = getattr(_nse_local, "moved", None)

    if moved:
        print(f"   !! the main site answered with a redirect to {moved}: NSE has moved hosts again.")

    try:
        reply = session.get(
            NSE_SEARCH_URL, params={"segment": "", "symbol": symbol},
            headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=False,
        )
    except Exception as e:
        print(f"2) symbol search failed: {type(e).__name__}: {e}")
        return

    print(f"2) symbol search: HTTP {reply.status_code}, content-type {reply.headers.get('content-type')}")

    if reply.status_code in (301, 302, 303, 307, 308):
        print(f"   -> redirected to {reply.headers.get('Location')}: update NSE_WWW_HOST / NSE_CHARTING_HOST.")
        return

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

    now_shifted = int(time.time()) + IST_OFFSET_SECONDS
    params = {
        "chartType": "I",
        "fromDate": ((now_shifted - NSE_HISTORY_DAYS * 86400) // 86400) * 86400,
        "symbol": match["symbol"],
        "symbolType": match.get("type") or "Equity",
        "timeInterval": 60,
        "toDate": now_shifted,
        "token": nse_chart_token(str(match["scripcode"])),
    }

    print(f"3) history request: {params}")

    try:
        reply = session.get(
            NSE_HISTORY_URL, params=params, headers=headers,
            timeout=REQUEST_TIMEOUT, allow_redirects=False,
        )
        print(f"   HTTP {reply.status_code}, body {snippet(reply.text, 300)}")
        body = reply.json()
        data, rest = nse_extract_candles(body)
    except Exception as e:
        print(f"   history failed: {type(e).__name__}: {e}")
        return

    print(f"   {len(data)} candles, other fields {snippet(rest, 160)}")

    if data:
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
            print(f"   stamps read as clock time; last day {last_day}: {len(day_times)} candles")
            print(f"   first 6: {labels[:6]}")
            print(f"   last 6 : {labels[-6:]}")

    print("4) shape of each session after reading (09:15 first, 15:28/15:29 last; "
          "15:16-15:19 absent in NSE's feed):")

    shift = _nse_calibration["shift"]

    try:
        raw = nse_raw_minutes(symbol, shift)
        flagged = raw.index[raw["Estimated"]]
        rows = clean_candle_data(raw)

        if rows is None:
            print("   every candle was dropped by cleaning")
            return

        print(f"   manual shift NSE_TIME_SHIFT_MINUTES = {shift:+d}")

        for date, count, first, last in day_structure(rows):
            verdict = "OK" if (first == 915 and last in (1528, 1529) and count >= 300) else "check"
            print(f"   {date}: {count:>3} candles, {hm_text(first)} to {hm_text(last)}  [{verdict}]")

        for stamp in flagged:
            print(f"   day-total row corrected to an estimate: {stamp:%Y-%m-%d %H:%M}")
    except Exception as e:
        print(f"   could not read: {type(e).__name__}: {e}")


def main():

    if len(sys.argv) > 1 and sys.argv[1] == "--nse-debug":
        nse_debug(sys.argv[2:])
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--check":
        load_nse_token_cache()

        if not nse_preflight():
            sys.exit(1)

        audit_symbols(sys.argv[2:])
        save_nse_token_cache()
        return

    print()
    print("=" * 70)
    print("NSE EOD MOMENTUM SCANNER  (NSE data only)")
    print("=" * 70)

    if DIRECTION_RULE not in DIRECTION_RULES:
        print(f"ERROR: DIRECTION_RULE must be one of {list(DIRECTION_RULES)}")
        sys.exit(1)

    load_nse_token_cache()

    # Fix the session being read for the whole run.
    context = get_run_context()

    if context["note"]:
        print()
        print("NOTE:", context["note"])

    # There is no other source to fall back to, so the scan only runs when NSE's
    # candle times are verified.
    if not nse_preflight():
        save_nse_token_cache()
        sys.exit(1)

    universe, source = get_stock_universe()

    print()
    print("=" * 70)
    print(f"FINAL STOCK UNIVERSE: {len(universe)}")
    print(f"SOURCE: {source}")
    print("=" * 70)

    results, elapsed = scan_all_symbols(universe)
    save_nse_token_cache()

    # No stock may be lost to a technical failure: retry the errored ones.
    results = retry_failed(results)
    save_nse_token_cache()

    reference_date = mark_stale(results)

    matches = final_signals(results)

    print()
    print("=" * 70)
    print("SCAN COMPLETE")
    print("=" * 70)
    print(f"Universe    : {len(universe)}")
    print(f"Previous day: {reference_date}   Entry day: {context['entry']}")
    print(f"Rules       : {STRATEGY_RULES}")
    print(f"Matches     : {len(matches)}")
    print(f"Time        : {elapsed:.1f} seconds")
    print()

    if matches:
        print("MATCHES (NSE data):")
        for sig in matches:
            print(
                f"  {sig['symbol']:<15}"
                f"{sig['direction']:<7}"
                f"{sig['previous_day']}"
            )
    else:
        print("NO MATCHES.")

    unscanned = unscanned_lines(results)

    if unscanned:
        print()
        print("STOCKS WITHOUT A RELIABLE RESULT:")
        for line in unscanned:
            print("  " + line)

    print()

    for name in ("PASS", "FAIL", "INCOMPLETE", "STALE", "NO_DATA", "ERROR"):
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
