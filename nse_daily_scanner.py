# =============================================================================
# NSE DAILY MOMENTUM SCANNER - 15-MIN / 5-MIN / 3-MIN / 1-MIN, NATIVE CANDLES
# =============================================================================
#
# EVERY timeframe is read DIRECTLY from TradingView's own chart feed: the 15m,
# 5m, 3m and 1m candles are all TradingView's native candles, requested over one
# websocket connection per stock (four chart sessions). NO timeframe is ever
# calculated, rebuilt or resampled from another timeframe, and there is no other
# data source: Yahoo has no 3-minute feed and NSE's own feed does not stamp its
# candles on the 09:15 grid, so neither can supply the same candles.
#
# TradingView has no free public data API. This uses the same websocket feed its
# own web charts use (no API key, no login). It is unofficial, is against
# TradingView's terms of use, and may be rate-limited, blocked or changed without
# notice.
# Universe: every NSE series (see UNIVERSE_MODE) -> Nifty lists -> built-in list.
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
#   15-minute : ... 15:00, 15:15       (15:15 is the last, 15:00 the second last;
#               NSE closes at 15:30, so the last 15m candle is 15:15-15:29)
#   5-minute : ... 15:20, 15:25       (15:25 is the last, 15:20 the second last)
#   3-minute  : ... 15:24, 15:27       (15:27 is the last, 15:24 the second last)
#               3-minute buckets are anchored to 09:15 (09:15, 09:18, ... 15:27),
#               the same grid TradingView and broker charts use.
#   1-minute  : ... 15:28, 15:29       (15:29 is the last, 15:28 the second last)
#
# ALL of these must hold:
#   1) 5m volume: the 15:20 candle's volume is HIGHER than the 15:25 candle's.
#   2) 3m volume:  the 15:24 candle's volume is HIGHER than the 15:27 candle's.
#   3) 3m trend:   the last two 3m candles (15:24, 15:27) have the SAME trend.
#   4) 1m volume:  the 15:28 candle's volume is HIGHER than the 15:29 candle's.
#   5) 1m trend:   the last two 1m candles (15:28, 15:29) have the SAME trend.
#   6) The second-last candles of all four timeframes (15m 15:00, 5m 15:20,
#      3m 15:24, 1m 15:28) have the SAME trend (the "aligned trend").
#   7) The morning candles (09:15) of all four timeframes (15m, 5m, 3m, 1m
#      09:15) have that SAME aligned trend too.
#   8) 15m volume: the 15:00 candle's volume is HIGHER than the 15:15 candle's.
#
# The 5m and 15m timeframes have no opposite-trend rule: their last two candles may have
# the same or opposite trends; only their volume order matters (and the
# second-last candle's trend, for the alignment in 6). The 3m and 1m
# timeframes also require their last two candles to share a trend (rules 3, 5).
#
# DATA SOURCE: TradingView only, native candles only (see above). A stock that
# TradingView cannot serve is reported as NO DATA or ERROR (errors are retried);
# it is never filled in from another source or from another timeframe.
#
# "Trend" = candle colour (close > open is Up, close < open is Down). A flat
# candle (close == open) has no trend, so it can never be "the same" as another.
#
# DIRECTION: the strategy does not say which way to trade, so it is set by
# DIRECTION_RULE below (default: trade WITH the aligned second-last candles'
# trend, i.e. the 5m 15:20 / 3m 15:24 / 1m 15:28 trend: Up = LONG, Down = SHORT).
# TRADE:     Entry = the 09:15 OPEN of the entry day, Exit = EXIT_TIME_LABEL.
#
# A candle with no trades is absent from the data. By default a missing
# candle can only BLOCK a match, never create one. Optionally
# MISSING_LAST_CANDLE_IS_ZERO_VOLUME treats a missing 5m 15:25 candle as
# volume 0 (see CONFIGURATION).
#
# DATA-ACCURACY SAFEGUARDS
#   * Before a scan starts, the candle TIMES are checked on liquid stocks: every
#     timeframe's candles must start on the 09:15 grid (15m ... 15:15, 5m ...
#     15:25, 3m ... 15:27, 1m ... 15:29) and the session's first and last candles
#     must be the expected ones. If not, the scan STOPS instead of reading the
#     wrong candles.
#   * Prices are rounded to 2 decimals, so float noise can never turn a flat
#     candle into an Up/Down one.
#   * Zero-volume candles and corrupt candles (price <= 0, close/open outside
#     the high-low range) are dropped and treated as MISSING: they can only
#     block a match, never create one.
#   * Every row in the report shows each candle's open -> close and volume, so
#     any signal can be checked against a chart in seconds.
#   * Single-stock audit:  python scanner.py --check RELIANCE TCS
#   * Feed test:           python scanner.py --tv-debug RELIANCE
#
# INSTALL:
#   python -m pip install pandas requests websocket-client
#
# =============================================================================

import html
import json
import re
import string
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

# websocket-client is required: TradingView is the only data source.
try:
    import websocket
except ImportError:
    websocket = None


# =============================================================================
# CONFIGURATION
# =============================================================================

# Worker threads. TradingView connections are limited separately
# (TV_MAX_CONNECTIONS, TV_MIN_INTERVAL), so more workers than that do not make the
# scan faster; they only wait their turn.
MAX_WORKERS = 8

# Attempts per request (only rate limits / network errors are retried).
MAX_RETRIES = 3



REQUEST_TIMEOUT = 20

# If fewer than this share of scanned symbols have all twelve required candles,
# the report shows a "data looks incomplete" warning. Kept low because
# illiquid stocks often have a no-trade candle; when the feed is genuinely
# broken the share is close to 0%.
SESSION_COMPLETE_SHARE = 0.20



# A candle with no trades is absent from the data. For the volume rules
# ("second-last volume > last volume"), a LAST candle with no trades has volume
# 0, so:
#   False -> a missing last candle BLOCKS the match (default, strictest)
#   True  -> a missing last candle counts as volume 0: the rule passes whenever
#            the second-last candle traded. This can only apply to the 5m
#            15:25 candle: the 3m 15:27 and 1m 15:29 candles need a trend (the
#            3m / 1m same-trend rules), and the second-last candles need one
#            too (alignment).
#   NOTE: because the 3m 15:27 and 1m 15:29 candles must exist and lie INSIDE
#   the 5m 15:25 candle, whenever the 5m 15:25 candle is missing they are
#   missing too - so this switch no longer changes any result.
MISSING_LAST_CANDLE_IS_ZERO_VOLUME = False

# A short description of the rules, shown at the top of every report and in the
# console so a result can always be tied to the rule set that produced it.
# (Keep it in step with the rules above if they ever change.)
STRATEGY_RULES = (
    "15m volume | 5m volume | 3m volume + same trend | 1m volume + same trend | "
    "aligned second-last trend (15m/5m/3m/1m) | morning (09:15) trend (15m/5m/3m/1m)"
)

# No stock may be lost to a technical failure. After the main scan, stocks that
# ERRORED are retried (waiting first, with fewer workers).
RETRY_ROUNDS = 2
RETRY_WAIT_SECONDS = (30, 60)
RETRY_WORKERS = 3










# If TradingView fails this many requests in a row, it is PAUSED: every worker
# waits TV_COOLDOWN_SECONDS and then carries on. Each further pause without a
# success in between doubles the wait (up to TV_COOLDOWN_MAX). After
# TV_MAX_COOLDOWNS such pauses in a row, TradingView is given up for the rest of
# the main scan (the stocks left are then listed as errors and retried at the end).
TV_REFUSALS_BEFORE_COOLDOWN = 4
TV_COOLDOWN_SECONDS = 45
TV_COOLDOWN_MAX = 600
TV_MAX_COOLDOWNS = 4

# If TradingView has served NOTHING usable after this many stocks, stop trying it
# for the rest of the main scan (it is blocked from this network / the feed changed).
TV_GIVE_UP_AFTER = 40

# Which way to trade when a match fires. The second-last candles (5m 15:20,
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

# After the 15:30 close, the data feed needs a few minutes to publish the final
# candles. Today's session is only treated as complete this many minutes after
# the close (default 15:50); before that the scanner still reads yesterday.
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
# which list was actually used. A bigger universe takes longer (each stock needs
# a few requests) but the stock -> NSE token lookups are cached between runs.
#
UNIVERSE_MODE = "ALL_NSE"

# With UNIVERSE_MODE = "ALL_NSE":
#   INCLUDE_NON_EQ_SERIES = False -> EQ series only (normal rolling settlement)
#   INCLUDE_NON_EQ_SERIES = True  -> EVERY series on NSE's official equity list:
#       EQ plus BE / BZ (trade-to-trade: delivery only, no same-day exit),
#       SM / ST (SME) and any other series the list carries.
#   INCLUDE_SME_LIST = True       -> also add NSE Emerge's separate SME list.
# Each signal shows its series, because a trade-to-trade or SME stock cannot be
# entered at 09:15 and exited at 15:27 like a normal stock.
INCLUDE_NON_EQ_SERIES = True
INCLUDE_SME_LIST = True

SME_EQUITY_URLS = [
    "https://nsearchives.nseindia.com/emerge/corporates/content/SME_EQUITY_L.csv",
]

# symbol -> NSE series, filled while the universe loads.
SERIES_OF = {}


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

# The twelve candles the strategy reads on the previous day: (timeframe, start).
# 15m, 5m and 1m come straight from the data source (3m too on TradingView);
# for other sources 3m is built from the 1m candles.
SLOTS = [
    ("15m", 915), ("5m", 915), ("3m", 915), ("1m", 915),
    ("15m", 1500), ("15m", 1515),
    ("5m", 1520), ("5m", 1525),
    ("3m", 1524), ("3m", 1527),
    ("1m", 1528), ("1m", 1529),
]

SLOT_KEYS = [f"{tf}_{hm:04d}" for tf, hm in SLOTS]



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

# Old NSE symbols that are now listed under a different symbol. Only matters for
# the built-in fallback list (the official CSVs already use current symbols); the
# new symbol is tried on TradingView if the old one is unknown. Edit as needed.
SYMBOL_ALIASES = {
    "ZOMATO": "ETERNAL",
    "L&TFH": "LTF",
    "TATAMOTORS": "TMPV",
}




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
    """Every stock on NSE's official equity list (EQ only, or every series -
    see INCLUDE_NON_EQ_SERIES)."""

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

            series_values = df[series_column].astype(str).str.strip().str.upper()

            if not INCLUDE_NON_EQ_SERIES:
                df = df[series_values == "EQ"]
                series_values = series_values[series_values == "EQ"]

            symbols = normalize_symbols(df[symbol_column].dropna().tolist())

            if len(symbols) < 300:
                raise ValueError(f"Only {len(symbols)} symbols returned")

            for sym, ser in zip(df[symbol_column].tolist(), series_values.tolist()):
                SERIES_OF[str(sym).strip().upper()] = ser

            by_series = Counter(series_values.tolist())
            shown = ", ".join(f"{k} {v}" for k, v in by_series.most_common(8))
            print(f"NSE official equity list loaded: {len(symbols)} symbols ({shown})")
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


def load_sme_list():
    """NSE Emerge (SME) symbols from NSE's separate SME list, or [] if it
    cannot be loaded (the scan then simply goes ahead without them)."""

    print()
    print("Attempting to load NSE SME list...")

    session = requests.Session()
    session.headers.update({**NSE_HEADERS, "Referer": "https://www.nseindia.com/"})

    try:
        session.get("https://www.nseindia.com/", timeout=15)
    except Exception:
        pass

    for url in SME_EQUITY_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()

            df = pd.read_csv(StringIO(response.text))
            column = next((c for c in df.columns if "symbol" in str(c).lower()), None)

            if column is None:
                raise ValueError("Symbol column not found")

            symbols = normalize_symbols(df[column].dropna().tolist())

            if not symbols:
                raise ValueError("empty list")

            for sym in symbols:
                SERIES_OF.setdefault(sym, "SM")

            print(f"NSE SME list loaded: {len(symbols)} symbols")
            return symbols

        except Exception as e:
            print(f"NSE SME list unavailable ({url.rsplit('/', 1)[-1]}): {e}")

    return []


def series_note(symbol):
    """A short warning for stocks that are not normal EQ-series stocks, or ''."""

    series = SERIES_OF.get(symbol)

    if series in (None, "EQ"):
        return ""

    if series in ("BE", "BZ", "BT"):
        return f"{series}: trade-to-trade, delivery only - no same-day exit"

    if series in ("SM", "ST"):
        return f"{series}: SME, lot-size trading, often illiquid"

    return f"series {series}"


def get_stock_universe():

    print()
    print("=" * 70)
    print("LOADING NSE STOCK UNIVERSE")
    print(f"Requested mode: {UNIVERSE_MODE}")
    print("=" * 70)

    # Tried in order; the first list that loads is used.
    attempts = []

    if UNIVERSE_MODE == "ALL_NSE":
        attempts.append((
            "NSE official equity list (every series)" if INCLUDE_NON_EQ_SERIES
            else "NSE official equity list (all EQ)",
            load_nse_equity_list,
        ))

    if UNIVERSE_MODE in ("ALL_NSE", "TOTAL_MARKET"):
        attempts.append(("Nifty Total Market (Nifty 500 + Microcap 250)", load_total_market))

    attempts.append(("Nifty 500 official CSV", load_nifty500))
    attempts.append(("NSE official equity list", load_nse_equity_list))

    for label, loader in attempts:

        symbols = loader()

        if symbols:
            if (
                UNIVERSE_MODE == "ALL_NSE" and INCLUDE_NON_EQ_SERIES and INCLUDE_SME_LIST
                and label.startswith("NSE official equity list (every series)")
            ):
                extra = load_sme_list()
                before = len(symbols)
                symbols = list(dict.fromkeys(symbols + extra))

                if len(symbols) > before:
                    label += f" + SME list (+{len(symbols) - before})"

            return symbols, label

    print()
    print(f"Using built-in fallback universe: {len(FALLBACK_UNIVERSE)} symbols")

    return FALLBACK_UNIVERSE, "Built-in fallback"


# =============================================================================
# CLEAN CANDLE DATA
# =============================================================================

def clean_candles(df):

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

    # Prices can carry float noise (e.g. 2850.800048828125). NSE ticks are
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
# TRADINGVIEW STATE   (rate-limit handling and feed health)
# =============================================================================

_tv_lock = threading.Lock()
_tv_state = {
    "failures": 0, "blocked": False, "attempts": 0, "successes": 0,
    "pause_until": 0.0, "cooldowns": 0, "cooldown_s": TV_COOLDOWN_SECONDS,
    "pauses": 0, "paused_seconds": 0.0,
}
_tv_errors = Counter()
_tv_examples = {}
_tv_calibration = {"done": False, "ok": None, "detail": ""}


class TvNoData(Exception):
    """TradingView answered, but has no usable data for this stock."""


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




























def tv_note_failure(message):
    """Remember why NSE failed for a stock (shown in the source-health report).
    A message may carry an example after ' | e.g. '; the first example for each
    reason is kept."""

    text = str(message)

    if text.startswith("NO_DATA"):
        text = "no data: " + (text[len("NO_DATA"):].lstrip(": ") or "(no detail)")

    reason, _, example = text.partition(" | e.g. ")
    reason = reason[:140]

    with _tv_lock:
        _tv_errors[reason] += 1

        if example and reason not in _tv_examples:
            _tv_examples[reason] = example[:260]


def tv_wait_if_paused():
    """Block while NSE is cooling down after refusals (every worker waits)."""

    while True:
        with _tv_lock:
            remaining = _tv_state["pause_until"] - time.time()
            blocked = _tv_state["blocked"]

        if blocked or remaining <= 0:
            return

        time.sleep(min(remaining, 5.0))


def tv_record(outcome):
    """Rate-limit handling. outcome: "ok"; "soft" (NSE answered but had no data
    for that stock); "hard" (an error, e.g. HTTP 403). TV_REFUSALS_BEFORE_COOLDOWN
    hard failures in a row pause NSE for a while; repeated pauses with no success
    in between give NSE up for the run. NSE is also given up if it has served
    nothing at all after TV_GIVE_UP_AFTER stocks."""

    message = None

    with _tv_lock:
        state = _tv_state
        state["attempts"] += 1

        if outcome == "ok":
            state["successes"] += 1
            state["failures"] = 0
            state["cooldowns"] = 0
            state["cooldown_s"] = TV_COOLDOWN_SECONDS
            return

        if state["blocked"]:
            return

        if outcome == "hard" and time.time() >= state["pause_until"]:
            state["failures"] += 1

            if state["failures"] >= TV_REFUSALS_BEFORE_COOLDOWN:
                state["failures"] = 0
                state["cooldowns"] += 1

                if state["cooldowns"] > TV_MAX_COOLDOWNS:
                    state["blocked"] = True
                    message = (
                        f"TradingView kept refusing requests after {TV_MAX_COOLDOWNS} pauses "
                        f"- not using TradingView for the rest of this run."
                    )
                else:
                    pause = state["cooldown_s"]
                    state["cooldown_s"] = min(pause * 2, TV_COOLDOWN_MAX)
                    state["pause_until"] = time.time() + pause
                    state["pauses"] += 1
                    state["paused_seconds"] += pause
                    message = (
                        f"TradingView refused {TV_REFUSALS_BEFORE_COOLDOWN} requests in a row "
                        f"(rate limit) - pausing TradingView for {pause:.0f}s, then carrying on."
                    )

        if (
            message is None
            and state["successes"] == 0
            and state["attempts"] >= TV_GIVE_UP_AFTER
        ):
            state["blocked"] = True
            message = (
                f"TradingView gave nothing usable for the first {state['attempts']} stocks "
                f"- not using TradingView for the rest of this run."
            )

    if message:
        print()
        print("NOTE:", message)


# =============================================================================
# TRADINGVIEW CHART FEED   (free, no API key, no login)
# =============================================================================
#
# TradingView has no free public data API. Its own web charts get their candles
# over a websocket, and community libraries (tvdatafeed and forks) speak the same
# protocol without logging in; this is a small client for it:
#   1. connect to wss://data.tradingview.com/socket.io/websocket
#   2. messages are framed as  ~m~<length>~m~<json>  with json {"m": name, "p": params}
#   3. send set_auth_token("unauthorized_user_token"), create a chart session,
#      resolve_symbol("NSE:RELIANCE") and create_series(<interval>, <bars>)
#      (No quote session is used: its quote_add_symbols message is now rejected
#      by TradingView with "invalid_parameters", and the candles do not need it.)
#   4. read timescale_update messages (bars as [time, open, high, low, close,
#      volume], time = the bar's OPEN time in UTC epoch seconds) until
#      series_completed; answer heartbeats (~h~N) by echoing them.
# It is unofficial, may break or be blocked, and is not covered by TradingView's
# terms of use - use is at your own risk. The 15-minute, 5-minute, 3-minute and
# 1-minute bars are all TradingView's own native bars (they start on the 09:15
# session grid); they are requested over ONE connection per stock, as four chart
# sessions. Every failure carries a specific reason.

TV_WS_URL = "wss://data.tradingview.com/socket.io/websocket"
TV_ORIGIN = "https://data.tradingview.com"

# 1-minute bars requested per stock (about 8 sessions; TradingView allows 5000).
TV_BARS = 3000

# The timeframes read from TradingView are its OWN native candles (not built
# from 1-minute ones). TradingView's name for each interval (minutes) and the
# number of bars requested per timeframe (about 8 sessions each):
TV_TIMEFRAMES = ("15m", "5m", "3m", "1m")
TV_INTERVALS = {"15m": "15", "5m": "5", "3m": "3", "1m": "1"}
TV_BARS_BY_TF = {"15m": 300, "5m": 700, "3m": 1100, "1m": TV_BARS}

# True  -> the three timeframes of a stock are requested over ONE websocket
#          connection (three chart sessions);
# False -> one connection per timeframe (slower; use it if the one-connection
#          mode ever misbehaves).
TV_ONE_CONNECTION_PER_STOCK = True

# At most this many connections at once, and at least this many seconds between
# connection attempts (all threads together).
TV_MAX_CONNECTIONS = 3
TV_MIN_INTERVAL = 0.5

# Seconds to wait for each reply, and for one whole stock.
TV_RECV_TIMEOUT = 15
TV_TOTAL_TIMEOUT = 40

_tv_slots = threading.BoundedSemaphore(TV_MAX_CONNECTIONS)
_tv_rate_lock = threading.Lock()
_tv_next = [0.0]


def tv_throttle():
    """Space TradingView connections at least TV_MIN_INTERVAL seconds apart,
    across all worker threads."""

    with _tv_rate_lock:
        now = time.monotonic()
        wait = _tv_next[0] - now
        _tv_next[0] = max(now, _tv_next[0]) + TV_MIN_INTERVAL

    if wait > 0:
        time.sleep(wait)


def tv_pack(function, params):
    """One websocket message in TradingView's framing."""

    payload = json.dumps({"m": function, "p": params}, separators=(",", ":"))

    return f"~m~{len(payload)}~m~{payload}"


def tv_unpack(raw):
    """The payloads inside one websocket frame (a frame can hold several
    messages: ~m~<length>~m~<payload>~m~<length>~m~<payload>...)."""

    payloads, i = [], 0

    while True:
        j = raw.find("~m~", i)

        if j < 0:
            break

        k = raw.find("~m~", j + 3)

        if k < 0:
            break

        try:
            length = int(raw[j + 3:k])
        except ValueError:
            break

        payloads.append(raw[k + 3:k + 3 + length])
        i = k + 3 + length

    return payloads


def tv_session(prefix):
    return prefix + "".join(random.choice(string.ascii_lowercase) for _ in range(12))


def tv_symbol_candidates(symbol):
    """TradingView tickers to try for an NSE symbol: punctuation becomes an
    underscore on TradingView (M&M -> M_M, BAJAJ-AUTO -> BAJAJ_AUTO)."""

    upper = symbol.upper()
    mapped = re.sub(r"[^A-Z0-9]", "_", upper)
    names = [mapped, upper]

    if upper in SYMBOL_ALIASES:
        names.append(SYMBOL_ALIASES[upper])

    return [f"NSE:{name}" for name in dict.fromkeys(names)]


def tv_fetch_series(tv_symbol, wanted):
    """Download several series for one TradingView ticker over ONE websocket
    connection (one chart session per series). wanted = {name: (interval,
    n_bars)}, interval = minutes as text ("1", "3", "5"). Returns {name: [[time,
    open, high, low, close, volume], ...]} (time = bar open, UTC epoch seconds),
    oldest first. Raises TvNoData if TradingView has no such symbol / no bars,
    and RuntimeError (with a readable reason) for connection problems."""

    if websocket is None:
        raise RuntimeError(
            "the websocket-client package is not installed (pip install websocket-client)"
        )

    tv_throttle()

    with _tv_slots:
        try:
            ws = websocket.create_connection(
                TV_WS_URL, origin=TV_ORIGIN, timeout=TV_RECV_TIMEOUT
            )
        except Exception as e:
            raise RuntimeError(f"TradingView connection failed: {type(e).__name__}: {e}"[:200])

        try:
            def send(function, params):
                ws.send(tv_pack(function, params))

            # Only chart sessions are needed for candles. (The quote session
            # used by older clients - quote_create_session / quote_add_symbols -
            # is now rejected with "invalid_parameters", so it is not used.)
            send("set_auth_token", ["unauthorized_user_token"])

            owners = {}     # chart session id -> series name

            for name, (interval, n_bars) in wanted.items():
                chart = tv_session("cs_")
                owners[chart] = name

                send("chart_create_session", [chart, ""])
                send("switch_timezone", [chart, "exchange"])
                send("resolve_symbol", [
                    chart, "symbol_1",
                    "=" + json.dumps(
                        {"symbol": tv_symbol, "adjustment": "splits", "session": "regular"},
                        separators=(",", ":"),
                    ),
                ])
                send("create_series", [chart, "s1", "s1", "symbol_1", interval, n_bars])

            bars = {name: {} for name in wanted}
            pending = set(owners)
            deadline = time.time() + TV_TOTAL_TIMEOUT

            while pending:
                if time.time() > deadline:
                    raise RuntimeError(
                        f"TradingView did not finish within {TV_TOTAL_TIMEOUT}s "
                        f"(still waiting for {sorted(owners[c] for c in pending)})"
                    )

                try:
                    raw = ws.recv()
                except Exception as e:
                    raise RuntimeError(
                        f"TradingView stopped answering ({type(e).__name__})"
                    )

                if not raw:
                    raise RuntimeError("TradingView closed the connection")

                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", "replace")

                for payload in tv_unpack(raw):

                    if payload.startswith("~h~"):
                        ws.send(f"~m~{len(payload)}~m~{payload}")
                        continue

                    try:
                        message = json.loads(payload)
                    except ValueError:
                        continue

                    if not isinstance(message, dict):
                        continue

                    kind = message.get("m")
                    params = message.get("p")
                    session = (
                        params[0]
                        if isinstance(params, list) and params and isinstance(params[0], str)
                        else None
                    )
                    name = owners.get(session)

                    if kind == "timescale_update" and name is not None:
                        try:
                            series = params[1]["s1"]["s"]
                        except (KeyError, IndexError, TypeError):
                            continue

                        for item in series:
                            values = item.get("v") if isinstance(item, dict) else None

                            if values and len(values) >= 5:
                                bars[name][item.get("i", len(bars[name]))] = values

                    elif kind == "symbol_error":
                        raise TvNoData(
                            f"TradingView does not know the symbol {tv_symbol} "
                            f"| e.g. {snippet(params)}"
                        )

                    elif kind == "series_error":
                        raise TvNoData(
                            f"TradingView could not build the series for {tv_symbol} "
                            f"| e.g. {snippet(params)}"
                        )

                    elif kind in ("critical_error", "protocol_error"):
                        raise RuntimeError(f"TradingView {kind}: {snippet(params)}")

                    elif kind == "series_completed" and name is not None:
                        pending.discard(session)

        finally:
            try:
                ws.close()
            except Exception:
                pass

    result = {}

    for name, (interval, _) in wanted.items():
        if not bars[name]:
            raise TvNoData(
                f"TradingView returned no {interval}-minute bars for {tv_symbol} "
                f"| e.g. {tv_symbol}"
            )

        result[name] = sorted((list(v) for v in bars[name].values()), key=lambda v: v[0])

    return result


def tv_fetch_bars(tv_symbol, n_bars, interval="1"):
    """One series for one TradingView ticker: a list of [time, open, high, low,
    close, volume], oldest first (see tv_fetch_series)."""

    return tv_fetch_series(tv_symbol, {"bars": (interval, n_bars)})["bars"]


def tv_fetch_timeframes(tv_symbol):
    """TradingView's own 5m, 3m and 1m bars for one ticker: {timeframe: bars}."""

    wanted = {tf: (TV_INTERVALS[tf], TV_BARS_BY_TF[tf]) for tf in TV_TIMEFRAMES}

    if TV_ONE_CONNECTION_PER_STOCK:
        return tv_fetch_series(tv_symbol, wanted)

    result = {}

    for tf, spec in wanted.items():
        result.update(tv_fetch_series(tv_symbol, {tf: spec}))

    return result


def tv_bars_to_frame(bars, shift=0):
    """TradingView bars -> a frame (Datetime index = the minute the bar STARTS,
    in IST; Open/High/Low/Close/Volume) for clean_candles. `shift` (minutes) is
    always 0: candle times are verified, never corrected."""

    rows = [list(v[:6]) + [0.0] * (6 - len(v[:6])) for v in bars]
    df = pd.DataFrame(rows, columns=["time", "Open", "High", "Low", "Close", "Volume"])

    stamp = pd.to_datetime(pd.to_numeric(df["time"], errors="coerce"), unit="s", utc=True)
    keep = stamp.notna()
    df = df.loc[keep].reset_index(drop=True)
    stamp = stamp.loc[keep].reset_index(drop=True)

    if df.empty:
        return None

    index = pd.DatetimeIndex(stamp).tz_convert("Asia/Kolkata") + pd.Timedelta(minutes=shift)
    index.name = "Datetime"

    out = df[["Open", "High", "Low", "Close", "Volume"]].apply(pd.to_numeric, errors="coerce")
    out.index = index

    return out








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

    today_complete is True on a weekday once the market has closed and the feed
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
    """'5m_1520' -> '5m 15:20' (used in missing notes)."""

    tf, hm = key.split("_")

    return f"{tf} {hm_text(int(hm))}"










def evaluate_rows(rows_by_tf, session_day=None):
    """15m / 5m / 3m / 1m closing-candle strategy, read on the previous day
    (P = the latest completed session). Entry day = the next trading day.

    0) 15m 15:00 volume is higher than 15m 15:15 volume.
    1) 5m 15:20 volume is higher than 5m 15:25 volume.
    2) 3m 15:24 volume is higher than 3m 15:27 volume.
    3) 3m 15:24 and 3m 15:27 candles have the same trend.
    4) 1m 15:28 volume is higher than 1m 15:29 volume.
    5) 1m 15:28 and 1m 15:29 candles have the same trend.
    6) 15m 15:00, 5m 15:20, 3m 15:24 and 1m 15:28 candles all have the same trend.
    7) 15m, 5m, 3m and 1m 09:15 candles have that same trend too.
    (The 5m and 15m timeframes do not require their last two candles to be opposite.)

    Every timeframe is read directly from the data source; none is calculated
    from another timeframe.

    session_day: read this session instead of the latest one (used to check the
    same session on a second data source).
    """

    if not rows_by_tf:
        return {"status": "NO_DATA"}

    # Native candles of every timeframe (15m, 5m, 3m, 1m), exactly as the source
    # provides them.
    by_tf = {tf: day_map(rows) for tf, rows in rows_by_tf.items()}

    # Which session is "previous" is decided once per run (see
    # build_run_context): today's session counts only once it is complete.
    ctx = get_run_context()
    entry = ctx["entry"]
    excluded = set() if ctx["today_complete"] else {ctx["today"]}
    dates = sorted({d for days in by_tf.values() for d in days} - excluded)

    if session_day is not None:
        if session_day not in {d for days in by_tf.values() for d in days}:
            return {
                "status": "INCOMPLETE",
                "date": session_day,
                "previous_day": session_day,
                "entry_day": entry,
                "missing": [slot_label(k) for k in SLOT_KEYS],
                "note": "this data source has no data for that session",
            }

        previous_day = session_day
    elif not dates:
        return {
            "status": "INCOMPLETE",
            "date": None,
            "previous_day": None,
            "entry_day": entry,
            "missing": [slot_label(k) for k in SLOT_KEYS],
            "note": "no completed trading day of data before today",
        }
    else:
        previous_day = dates[-1]

    # A timeframe the source did not provide is simply MISSING (it is never
    # rebuilt from another timeframe), which can only block a match.

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

    t5_20, t5_25 = trend("5m_1520"), trend("5m_1525")
    t15_00, t15_15 = trend("15m_1500"), trend("15m_1515")
    t3_24, t3_27 = trend("3m_1524"), trend("3m_1527")
    t1_28, t1_29 = trend("1m_1528"), trend("1m_1529")
    t5_0915, t15_0915 = trend("5m_0915"), trend("15m_0915")
    t3_0915, t1_0915 = trend("3m_0915"), trend("1m_0915")

    v5_20, v5_25 = volume("5m_1520"), volume("5m_1525")
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

    # 15m and 5m: volume only (their last two candles may have the same or
    # opposite trends). 3m and 1m: volume AND the last two candles must have the
    # same trend (a flat or missing candle has trend 0, so it is never "the same").
    vol_15m = higher(v15_00, v15_15)
    vol_5m = higher(v5_20, v5_25)
    vol_3m = higher(v3_24, v3_27)
    vol_1m = higher(v1_28, v1_29)
    trend_3m = t3_24 != 0 and t3_24 == t3_27
    trend_1m = t1_28 != 0 and t1_28 == t1_29

    cond_15m = vol_15m
    cond_5m = vol_5m
    cond_3m = vol_3m and trend_3m
    cond_1m = vol_1m and trend_1m

    # Second-last candles of all four timeframes share one (non-flat) trend.
    cond_align = t5_20 != 0 and t5_20 == t15_00 == t3_24 == t1_28

    # The morning (09:15) candles of all four timeframes have that same trend.
    cond_morning = cond_align and t5_0915 == t15_0915 == t3_0915 == t1_0915 == t5_20

    cond = cond_15m and cond_5m and cond_3m and cond_1m and cond_align and cond_morning

    one_day = by_tf.get("1m", {}).get(previous_day, {})

    # Direction anchors on the alignment trend: when cond is True the 15m 15:00,
    # 5m 15:20, 3m 15:24 and 1m 15:28 candles all share it.
    side = t5_20 * DIRECTION_RULES[DIRECTION_RULE]
    direction = "LONG" if side == 1 else "SHORT" if side == -1 else None

    status = "PASS" if cond else "FAIL"

    return {
        "status": status,
        "date": previous_day,
        "previous_day": previous_day,
        "entry_day": entry,
        "direction": direction if cond else None,
        "cond": cond,
        "cond_15m": cond_15m,
        "cond_5m": cond_5m,
        "cond_3m": cond_3m,
        "cond_1m": cond_1m,
        "cond_align": cond_align,
        "cond_morning": cond_morning,
        "vol_15m": vol_15m,
        "vol_5m": vol_5m,
        "vol_3m": vol_3m,
        "trend_3m": trend_3m,
        "vol_1m": vol_1m,
        "trend_1m": trend_1m,
        "missing": missing,
        "candles": candles,
        "tail_1m": sorted(h for h in one_day if h >= 1520),
        "details": {
            "t15_00": t15_00, "t15_15": t15_15,
            "t5_20": t5_20, "t5_25": t5_25,
            "t3_24": t3_24, "t3_27": t3_27,
            "t1_28": t1_28, "t1_29": t1_29,
            "t15_0915": t15_0915, "t5_0915": t5_0915,
            "t3_0915": t3_0915, "t1_0915": t1_0915,
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
    """Warn about broad data-quality problems: very few symbols having all twelve
    required candles. Returns text or None."""

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
            f"Only {share:.0%} of scanned symbols have all twelve required candles "
            f"(09:15 on 15m, 5m, 3m and 1m; 15m 15:00 and 15:15; 5m 15:20 and 15:25; 3m 15:24 and 15:27; "
            f"1m 15:28 and 15:29) on the previous day. "
            f"The data may be unusually incomplete - treat matches with "
            f"extra caution and verify on TradingView."
            + (
                " The data feed can lag a few minutes after the close; if you just ran "
                "this, try again shortly."
                if get_run_context()["today_complete"] else ""
            )
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
.tag {
    display: inline-block; margin-left: 8px; padding: 2px 6px; border-radius: 4px;
    font-size: 11px; font-weight: bold;
}
.tag.ok { background: #dff5e5; color: #08752f; }
.tag.warn { background: #fff3cd; color: #6b5200; }
.tag.none { background: #ededed; color: #555; }
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
    """'15:20 Up (100.00->101.00) vol 1,234' for the report; numbers only."""

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


# The only "last" candle whose volume alone is used (every other last candle
# needs a trend too): with MISSING_LAST_CANDLE_IS_ZERO_VOLUME it may be missing.
LAST_CANDLES = {"5m 15:25", "15m 15:15"}




def missing_summary(results):
    """How many stocks lack each of the twelve candles, and how many lack ONLY a
    last candle (5m 15:25) (the ones MISSING_LAST_CANDLE_IS_ZERO_VOLUME would
    recover). Returns (per_slot, only_last, evaluated_count)."""

    evaluated = [r for r in results if "cond_5m" in r]
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




def near_miss_list(results):
    """Stocks that failed ONLY because their last 5m (15:25) candle is missing.
    With MISSING_LAST_CANDLE_IS_ZERO_VOLUME = True they would be signals.
    (In practice the 3m 15:27 and 1m 15:29 candles, which lie inside it, are then
    missing too, so this list is normally empty.) Returns [(symbol, direction)]."""

    if MISSING_LAST_CANDLE_IS_ZERO_VOLUME:
        return []

    found = []

    for r in results:
        missing = set(r.get("missing") or [])

        if r.get("status") != "FAIL" or not missing or not missing <= LAST_CANDLES:
            continue

        c = r.get("candles") or {}

        ok_5m = r.get("vol_5m") or (c.get("5m_1525") is None and c.get("5m_1520") is not None)
        ok_15m = r.get("vol_15m") or (c.get("15m_1515") is None and c.get("15m_1500") is not None)

        if ok_5m and ok_15m and r.get("cond_3m") and r.get("cond_1m") and r.get("cond_align") and r.get("cond_morning"):
            side = r["details"]["t5_20"] * DIRECTION_RULES[DIRECTION_RULE]
            found.append((r["symbol"], "LONG" if side == 1 else "SHORT"))

    return sorted(found)




def rule_failure_counts(results):
    """For every rule: how many stocks fail it ON ITS OWN (every candle present,
    no data conflict), whatever the other rules say. Unlike the funnel this does
    not depend on the order of the rules, so it shows which rule really is the
    bottleneck. Returns ([(rule, count)], stocks_considered)."""

    allowed_missing = LAST_CANDLES if MISSING_LAST_CANDLE_IS_ZERO_VOLUME else set()
    pool = [
        r for r in results
        if r.get("status") in ("PASS", "FAIL") and "cond_5m" in r
        and set(r.get("missing") or []) <= allowed_missing
    ]

    return [
        (label, sum(1 for r in pool if not r.get(key))) for key, label in RULE_NAMES
    ], len(pool)


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
        f"  {last_only:>5}  missing ONLY a last candle (5m 15:25) "
        f"(MISSING_LAST_CANDLE_IS_ZERO_VOLUME = {MISSING_LAST_CANDLE_IS_ZERO_VOLUME})"
    )

    failing, pool = rule_failure_counts(results)

    lines.append("")
    lines.append(f"  Stocks failing each rule on its own (of {pool} with every candle present):")

    for label, count in failing:
        lines.append(f"  {count:>5}  {label}")

    if pool:
        strictest = max(failing, key=lambda item: item[1])
        lines.append(f"  Strictest rule today: {strictest[0]} ({strictest[1]} of {pool} fail it)")

    near = near_miss_list(results)

    if near:
        lines.append("")
        lines.append(
            f"  {len(near)} stock(s) fail ONLY because a last candle (5m 15:25) had no trades."
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

    failing, pool = rule_failure_counts(results)
    failing_body = "".join(
        f'<tr><td>{esc(label)}</td><td>{count}</td></tr>' for label, count in failing
    )
    failing_html = (
        f'<h2 style="margin-top:22px">Stocks failing each rule on its own (of {pool} with every candle present)</h2>'
        '<p class="none">Independent of the order of the rules: the biggest number is the '
        'rule that removes the most stocks today.</p>'
        '<div class="table-wrap"><table class="funnel"><tbody>'
        f'{failing_body}</tbody></table></div>'
    ) if pool else ""

    closest = closest_misses(results)
    closest_html = ""

    if closest:
        names = ", ".join(f"{esc(sym)} [{esc(why)}]" for sym, why in closest)
        closest_html = (
            '<h2 style="margin-top:22px">Watchlist: closest misses (fail exactly one rule)</h2>'
            '<p class="none">NOT signals - a watchlist only: each of these passed every rule except '
            f'the one in brackets.</p><p>{names}</p>'
        )

    near = near_miss_list(results)

    near_html = ""

    if near:
        names = ", ".join(f"{esc(sym)} ({esc(d)})" for sym, d in near)
        near_html = (
            f'<h2 style="margin-top:22px">Filtered only by a missing last candle ({len(near)})</h2>'
            '<p class="none">Every other condition holds; the last 5m (15:25), 3m (15:27) and/or '
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
        f'{failing_html}{near_html}{closest_html}</div>'
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
        note_5m = (
            candle_text(candles.get("5m_1520"), "15:20") + "<br>"
            + candle_text(candles.get("5m_1525"), "15:25") + "<br>"
            + f'volume {part_badge(r.get("vol_5m"))}'
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
            f'15m 15:00 {trend_name(details["t15_00"])} / '
            f'5m 15:20 {trend_name(details["t5_20"])} / '
            f'3m 15:24 {trend_name(details["t3_24"])} / '
            f'1m 15:28 {trend_name(details["t1_28"])}'
        )
        note_morning = (
            candle_text(candles.get("15m_0915"), "15m 09:15") + "<br>"
            + candle_text(candles.get("5m_0915"), "5m 09:15") + "<br>"
            + candle_text(candles.get("3m_0915"), "3m 09:15") + "<br>"
            + candle_text(candles.get("1m_0915"), "1m 09:15")
        )
    else:
        note_15m = note_5m = note_3m = note_1m = note_align = note_morning = ""

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

    direction_value = r.get("direction")
    result_label = STATUS_LABEL.get(status, status)

    return f"""
<tr>
<td class="symbol">{esc(r.get('symbol', ''))}</td>
<td>{esc(r['previous_day']) if r.get('previous_day') else DASH}</td>
<td>{cell(direction_value)}</td>
<td>{badge(r.get('cond_15m'))}<br><small>{note_15m}</small></td>
<td>{badge(r.get('cond_5m'))}<br><small>{note_5m}</small></td>
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
        or "TradingView"
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
            + series_tag(sig["symbol"])
            + '</div>'
            for sig in matches
        )
    else:
        match_html = '<div class="none">No stocks matched.</div>'

    sorted_results = sorted(
        results,
        key=lambda x: (0 if x.get("status") == "PASS" else 1 + STATUS_RANK.get(x.get("status"), 9), x.get("symbol", "")),
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

    zero_note = (
        " A last candle (5m 15:25) with no trades counts as volume 0."
        if MISSING_LAST_CANDLE_IS_ZERO_VOLUME else ""
    )

    if DIRECTION_RULES[DIRECTION_RULE] == 1:
        direction_text = (
            "Direction follows the alignment trend (the second-last candles: "
            "5m 15:20 / 3m 15:24 / 1m 15:28): "
            "Up is LONG, Down is SHORT."
        )
    else:
        direction_text = (
            "Direction fades the alignment trend (the second-last candles: "
            "5m 15:20 / 3m 15:24 / 1m 15:28): "
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
Data: {esc(source_text)} - 15-minute, 5-minute, 3-minute and 1-minute candles on the 09:15 grid<br>
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
<th>5m condition<br><small>15:20 vol &gt; 15:25 vol</small></th>
<th>3m condition<br><small>15:24 &amp; 15:27 same trend; 15:24 vol &gt; 15:27 vol</small></th>
<th>1m condition<br><small>15:28 &amp; 15:29 same trend; 15:28 vol &gt; 15:29 vol</small></th>
<th>Alignment<br><small>15m 15:00 = 5m 15:20 = 3m 15:24 = 1m 15:28 trend</small></th>
<th>Morning candles<br><small>09:15 candles (15m, 5m, 3m, 1m) = that same trend</small></th>
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
<b>STRATEGY (15-minute, 5-minute, 3-minute and 1-minute candles)</b><br>
Read on the previous day, the latest completed session. Candles are labelled by start
time: the last candles are 15m 15:15 (second last 15:00), 5m 15:25 (second last 15:20),
3m 15:27 (second last 15:24) and 1m 15:29 (second last 15:28). A stock matches only when ALL of these hold:<br>
1) 5m volume: the 15:20 candle's volume is higher than the 15:25 candle's.<br>
2) 3m volume: the 15:24 candle's volume is higher than the 15:27 candle's.<br>
3) 3m trend: the 15:24 and 15:27 candles have the same trend.<br>
4) 1m volume: the 15:28 candle's volume is higher than the 15:29 candle's.<br>
5) 1m trend: the 15:28 and 15:29 candles have the same trend.<br>
6) The 15m 15:00, 5m 15:20, 3m 15:24 and 1m 15:28 candles all have the same trend.<br>
7) The morning candles (09:15) of the 15m, 5m, 3m and 1m timeframes have that same trend too.<br>
8) 15m volume: the 15:00 candle's volume is higher than the 15:15 candle's.<br>
The 5m and 15m timeframes have no opposite-trend rule: their last two candles may have the
same or opposite trends.<br>
Trend = candle colour (close vs open); a flat candle has no trend, so it is never
the same as another.<br>
<b>Data source:</b> TradingView only. All four timeframes (15m, 5m, 3m, 1m) are TradingView's own
native candles; none is calculated from another timeframe.<br>
{esc(direction_text)}<br>
<b>Entry:</b> the 09:15 open on entry day. <b>Exit:</b> {esc(EXIT_TIME_LABEL)}.<br><br>
<b>Data notes:</b> prices are rounded to 2 decimals; zero-volume and corrupt candles are dropped and
count as missing, which can only block a match.{zero_note} INCOMPLETE means no completed session (or none of
the twelve candles) came back. STALE means the previous day differs from most other symbols.
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
        line = (
            f"  {label:<10} {trend_name(d):<5} open {c['open']:.2f}  "
            f"close {c['close']:.2f}  vol {c['volume']:,}"
        )

        print(line)

    if "cond_5m" in r:
        print(f"  15m 15:00 vol > 15:15 vol : {r['vol_15m']}")
        print(f"  5m 15:20 vol > 15:25 vol  : {r['vol_5m']}")
        print(f"  3m  15:24 vol > 15:27 vol  : {r['vol_3m']}")
        print(f"  3m  15:24 and 15:27 same trend: {r['trend_3m']}")
        print(f"  1m  15:28 vol > 15:29 vol  : {r['vol_1m']}")
        print(f"  1m  15:28 and 15:29 same trend: {r['trend_1m']}")
        print(f"  15m 15:00 = 5m 15:20 = 3m 15:24 = 1m 15:28 trend: {r['cond_align']}")
        print(f"  09:15 candles (15m, 5m, 3m, 1m) share that trend: {r['cond_morning']}")

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
# RULES, RETRY AND RESULTS
# =============================================================================

RULE_NAMES = (
    ("vol_15m", "15m volume (15:00 > 15:15)"),
    ("vol_5m", "5m volume (15:20 > 15:25)"),
    ("vol_3m", "3m volume (15:24 > 15:27)"),
    ("trend_3m", "3m trend (15:24 and 15:27 same)"),
    ("vol_1m", "1m volume (15:28 > 15:29)"),
    ("trend_1m", "1m trend (15:28 and 15:29 same)"),
    ("cond_align", "alignment (15m 15:00 = 5m 15:20 = 3m 15:24 = 1m 15:28)"),
    ("cond_morning", "morning 09:15 candles (15m, 5m, 3m, 1m)"),
)


def failed_rules(result):
    """Names of the rules a result fails (plus data problems)."""

    if "cond_5m" not in result:
        return ["no usable data"]

    failed = [label for key, label in RULE_NAMES if not result.get(key)]

    allowed = LAST_CANDLES if MISSING_LAST_CANDLE_IS_ZERO_VOLUME else set()
    missing = [m for m in result.get("missing") or [] if m not in allowed]

    if missing:
        failed.append("missing candles: " + ", ".join(missing))

    if result.get("conflicts"):
        failed.append("5m/1m data conflict")

    return failed


























def series_tag(symbol):
    """HTML tag for a stock that is not a normal EQ-series stock."""

    note = series_note(symbol)

    return f'<span class="tag warn">{esc(note)}</span>' if note else ""






def unscanned_report(results, limit=40):
    """Stocks whose result cannot be trusted, by name: technical ERRORs,
    DATA CONFLICTs and STALE sessions."""

    def pick(status):
        return sorted(
            (r["symbol"], (r.get("error") or "")[:110])
            for r in results if r.get("status") == status
        )

    return {
        "ERROR": pick("ERROR"),
        "CONFLICT": pick("CONFLICT"),
        "STALE": pick("STALE"),
        "limit": limit,
    }


def unscanned_lines(results):
    report = unscanned_report(results)
    limit = report["limit"]
    lines = []

    for key, meaning in (
        ("ERROR", "could NOT be scanned (technical failure) and may hide a signal"),
        ("CONFLICT", "have a 5m/1m DATA CONFLICT and were blocked"),
        ("STALE", "were read on a different session than most stocks"),
    ):
        items = report[key]

        if items:
            shown = ", ".join(sym for sym, _ in items[:limit])
            more = f" ... and {len(items) - limit} more" if len(items) > limit else ""
            lines.append(f"{len(items)} stocks {meaning}: {shown}{more}")

    return lines


def unscanned_html(results):
    lines = unscanned_lines(results)

    if not lines:
        return ""

    return '<div class="warn">' + "<br>".join(esc(line) for line in lines) + "</div>"


# =============================================================================
# TRADINGVIEW DEBUG   (python scanner.py --tv-debug RELIANCE)
# =============================================================================





# =============================================================================
# TRADINGVIEW NATIVE CANDLES FOR ONE STOCK
# =============================================================================

TV_MINUTES = {"15m": 15, "5m": 5, "3m": 3, "1m": 1}

# Liquid stocks used (in order) for the one-time check of TradingView's candle
# times before a scan.
TV_REFERENCE_SYMBOLS = ("RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "SBIN")


def tv_fetch_all(symbol):
    """The 15m, 5m, 3m and 1m candles of one stock, all TradingView's own native
    candles: (rows_by_timeframe, info). rows_by_timeframe maps each timeframe to
    a cleaned DataFrame (start-labelled candles, IST). If it is None, info is
    "NO_DATA: <reason>" (TradingView has nothing usable for this stock) or an
    error message. Nothing is calculated from another timeframe."""

    try:
        series, note = None, ""

        for candidate in tv_symbol_candidates(symbol):
            try:
                series = tv_fetch_timeframes(candidate)
                break
            except TvNoData as e:
                note = str(e)

        if series is None:
            raise TvNoData(note or f"TradingView has no data for {symbol}")

        rows_by_tf = {}

        for tf in TV_TIMEFRAMES:
            frame = tv_bars_to_frame(series[tf], 0)
            cleaned = clean_candles(frame) if frame is not None else None

            if cleaned is None or cleaned.empty:
                raise TvNoData(
                    f"every TradingView {tf} candle was dropped by cleaning "
                    f"(zero volume or corrupt) | e.g. {symbol}: {len(series[tf])} bars"
                )

            rows_by_tf[tf] = cleaned

        return rows_by_tf, "OK"

    except TvNoData as e:
        return None, f"NO_DATA: {e}"[:300]

    except Exception as e:
        return None, f"{type(e).__name__}: {e}"[:300]


def tv_grid(minutes):
    """Start times (HHMM) of every N-minute candle of one NSE session, on the
    09:15 grid."""

    grid, t = set(), MARKET_OPEN_MINUTES

    while t < MARKET_CLOSE_MINUTES:
        grid.add((t // 60) * 100 + t % 60)
        t += minutes

    return grid


def tv_check_grid(rows_by_tf):
    """Check, on the latest full session, that every native timeframe's candles
    start on the 09:15 grid and that the first and last candles of the session are
    the expected ones (15m ... 15:15, 5m ... 15:25, 3m ... 15:27, 1m ... 15:29).
    This only looks at candle TIMES; nothing is calculated. Returns (ok, detail)."""

    one_minute = rows_by_tf.get("1m")

    if one_minute is None or one_minute.empty:
        return False, "no 1-minute candles"

    counts = one_minute.groupby("date").size()
    full_days = sorted(d for d, n in counts.items() if n >= 300)

    if not full_days:
        return False, "no full trading session in the data"

    day = full_days[-1]
    parts, problems = [], []

    for tf in TV_TIMEFRAMES:
        rows = rows_by_tf.get(tf)
        starts = set() if rows is None else {int(h) for h in rows.loc[rows["date"] == day, "hm"]}

        if not starts:
            problems.append(f"{tf}: no candles on {day}")
            continue

        grid = tv_grid(TV_MINUTES[tf])
        off_grid = sorted(starts - grid)

        parts.append(f"{tf}: {len(starts)} candles {hm_text(min(starts))}-{hm_text(max(starts))}")

        if off_grid:
            problems.append(f"{tf}: {len(off_grid)} candles off the 09:15 grid (e.g. {hm_text(off_grid[0])})")

        if min(starts) != hm_text_to_int(MARKET_OPEN_MINUTES) or max(starts) != max(grid):
            problems.append(
                f"{tf}: first/last candle {hm_text(min(starts))}/{hm_text(max(starts))}, "
                f"expected {hm_text(hm_text_to_int(MARKET_OPEN_MINUTES))}/{hm_text(max(grid))}"
            )

    return not problems, "; ".join([f"session {day}"] + parts + problems)


def hm_text_to_int(minutes_after_midnight):
    """Minutes after midnight -> HHMM as an integer (555 -> 915)."""

    return (minutes_after_midnight // 60) * 100 + minutes_after_midnight % 60


def tv_preflight():
    """One-time check, before a scan, that TradingView's candles are on the 09:15
    grid (see tv_check_grid) for a liquid stock. Returns True if the scan may go
    ahead. There is no other source to fall back to, so on failure the scan stops
    instead of reading the wrong candles."""

    if _tv_calibration["done"]:
        return bool(_tv_calibration["ok"])

    _tv_calibration["done"] = True
    problems = []

    for symbol in TV_REFERENCE_SYMBOLS:
        rows, info = tv_fetch_all(symbol)

        if rows is None:
            problems.append(f"{symbol}: {info}"[:220])
            continue

        ok, detail = tv_check_grid(rows)

        if ok:
            _tv_calibration.update(
                ok=True,
                detail=f"TradingView candle times verified on the 09:15 grid ({symbol}) - {detail}",
            )
            print()
            print("TRADINGVIEW CHECK:", _tv_calibration["detail"])
            return True

        problems.append(f"{symbol}: {detail}"[:260])

    detail = "; ".join(problems)[:600]
    _tv_calibration.update(ok=False, detail="TradingView candles could not be verified: " + detail)

    print()
    print("TRADINGVIEW CHECK FAILED - the scan cannot continue.")
    print("  " + detail)

    return False


def fetch_symbol_rows(symbol):
    """Returns (rows_by_timeframe, info, source) for one stock: all four
    timeframes straight from TradingView. If rows_by_timeframe is None, info is
    "NO_DATA" or an error message."""

    # Small random delay so the workers do not hit the source in one burst.
    time.sleep(random.uniform(0.05, 0.25))

    tv_wait_if_paused()

    if _tv_state["blocked"]:
        return None, "TradingView skipped (given up after repeated failures)", "TradingView"

    rows, info = tv_fetch_all(symbol)

    if rows is not None:
        tv_record("ok")
        return rows, "OK", "TradingView"

    tv_record("soft" if is_no_data(info) else "hard")
    tv_note_failure(info)

    return None, ("NO_DATA" if is_no_data(info) else info), "TradingView"


def retry_failed(results):
    """Retry stocks that ERRORED (waiting first, with fewer workers). A stock
    with no data is not retried. Returns the updated results."""

    by_symbol = {r["symbol"]: r for r in results}

    for round_number in range(1, RETRY_ROUNDS + 1):
        errored = sorted(sym for sym, r in by_symbol.items() if r.get("status") == "ERROR")

        if not errored:
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

        # TradingView gets a fresh start after the wait.
        with _tv_lock:
            _tv_state.update(
                failures=0, pause_until=0.0, cooldowns=0,
                cooldown_s=TV_COOLDOWN_SECONDS, blocked=False,
            )

        recovered = 0

        with ThreadPoolExecutor(max_workers=RETRY_WORKERS) as executor:
            for sym, result in zip(errored, executor.map(scan_one_symbol, errored)):
                if result.get("status") != "ERROR":
                    recovered += 1

                by_symbol[sym] = result

        print(f"Retry {round_number}: {recovered} stocks recovered.")

    return sorted(by_symbol.values(), key=lambda r: r.get("symbol", ""))


def final_signals(results):
    """Every signal to show: a stock that passes every rule."""

    signals = [
        {
            "symbol": r["symbol"],
            "direction": r["direction"],
            "previous_day": r.get("previous_day"),
        }
        for r in results if r.get("status") == "PASS"
    ]
    signals.sort(key=lambda x: (x["direction"] != "LONG", x["symbol"]))

    return signals


def source_health(results):
    """Is TradingView serving the stocks? Returns (lines, level): level is
    "ok", "partial" or "bad"."""

    counts = Counter(r.get("status") for r in results)
    served = sum(1 for r in results if r.get("status") not in ("NO_DATA", "ERROR"))
    total = len(results)

    lines = [
        f"TradingView served {served} of {total} stocks "
        f"({counts['NO_DATA']} with no data, {counts['ERROR']} errors)."
    ]

    if _tv_calibration["detail"]:
        lines.append(_tv_calibration["detail"])

    if _tv_state["pauses"]:
        lines.append(
            f"TradingView refused requests {_tv_state['pauses']} time(s); the scanner paused "
            f"for {_tv_state['paused_seconds']:.0f}s in total and carried on."
        )

    if _tv_state["blocked"]:
        lines.append("TradingView was given up for part of the run after repeated failures.")

    if served == 0:
        level = "bad"
        lines.append(
            "TradingView served nothing. Run  python scanner.py --tv-debug RELIANCE  "
            "to see what it sends back."
        )
    elif counts["ERROR"] > 0.05 * total:
        level = "partial"
        lines.append("Many stocks failed with errors: TradingView is only partly working from this network.")
    else:
        level = "ok"

    with _tv_lock:
        reasons = _tv_errors.most_common(5)
        examples = dict(_tv_examples)

    if reasons:
        lines.append("Why TradingView failed for those stocks (top reasons):")

        for message, count in reasons:
            lines.append(f"{count} x {message}")

            if message in examples:
                lines.append(f"      e.g. {examples[message]}")

    return lines, level


def build_funnel(results):
    """Where stocks drop out, step by step. Returns [(label, count), ...]; each
    line counts only the stocks that survived every line above it. It separates
    data problems (download, missing candles) from the strategy simply being
    strict."""

    got_data = [r for r in results if r.get("status") not in ("NO_DATA", "ERROR")]
    evaluated = [r for r in results if "cond_5m" in r]
    allowed_missing = (
        LAST_CANDLES if MISSING_LAST_CANDLE_IS_ZERO_VOLUME else set()
    )
    full = [r for r in evaluated if set(r.get("missing") or []) <= allowed_missing]
    step_1 = [r for r in full if r.get("vol_5m")]
    step_1b = [r for r in step_1 if r.get("vol_15m")]
    step_2 = [r for r in step_1b if r.get("vol_3m")]
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
        (
            "All twelve candles present"
            + (" (last candles may be missing)" if allowed_missing else ""),
            len(full),
        ),
        ("5m: 15:20 volume > 15:25 volume", len(step_1)),
        ("15m: 15:00 volume > 15:15 volume", len(step_1b)),
        ("3m: 15:24 volume > 15:27 volume", len(step_2)),
        ("3m: 15:24 and 15:27 candles same trend", len(step_3)),
        ("1m: 15:28 volume > 15:29 volume", len(step_4)),
        ("1m: 15:28 and 15:29 candles same trend", len(step_5)),
        ("Same trend: 15m 15:00 = 5m 15:20 = 3m 15:24 = 1m 15:28", len(step_6)),
        ("Morning 09:15 candles (15m, 5m, 3m, 1m) share that trend", len(step_7)),
        ("Same session as most stocks", len(step_8)),
        ("SIGNALS SHOWN", len(final_signals(results))),
    ]


def closest_misses(results, limit=60):
    """When signals are few (or none): stocks that failed exactly ONE rule (every
    candle present). Returns [(symbol, failed_rule)]."""

    allowed_missing = (
        LAST_CANDLES if MISSING_LAST_CANDLE_IS_ZERO_VOLUME else set()
    )
    found = []

    for r in results:
        if r.get("status") != "FAIL" or "cond_5m" not in r:
            continue

        if not set(r.get("missing") or []) <= allowed_missing:
            continue

        failed = [
            name for name, ok in (
                ("15m volume", r.get("vol_15m")),
                ("5m volume", r.get("vol_5m")),
                ("3m volume", r.get("vol_3m")),
                ("3m trend", r.get("trend_3m")),
                ("1m volume", r.get("vol_1m")),
                ("1m trend", r.get("trend_1m")),
                ("alignment", r.get("cond_align")),
                ("morning candles", r.get("cond_morning")),
            ) if not ok
        ]

        if len(failed) == 1:
            found.append((r["symbol"], failed[0]))

    return sorted(found)[:limit]


def require_websocket():
    if websocket is None:
        print()
        print("ERROR: the websocket-client package is not installed, but TradingView needs it.")
        print("Install with: pip install websocket-client")
        print()
        sys.exit(1)


# =============================================================================
# TRADINGVIEW DEBUG   (python scanner.py --tv-debug RELIANCE)
# =============================================================================

def tv_debug(symbols):
    """Show what TradingView sends for one stock: the native candles of every
    timeframe, whether they sit on the 09:15 grid, and the twelve candles the
    strategy reads. Paste the output when asking for help."""

    symbol = (symbols[0] if symbols else "RELIANCE").strip().upper()

    print("=" * 78)
    print(f"TRADINGVIEW DEBUG for {symbol}")
    print("=" * 78)

    if websocket is None:
        print("The websocket-client package is not installed (pip install websocket-client).")
        return

    series = None

    for candidate in tv_symbol_candidates(symbol):
        print(f"1) {candidate}: requesting the native 15m, 5m, 3m and 1m bars ...")

        try:
            series = tv_fetch_timeframes(candidate)
            break
        except TvNoData as e:
            print("   no data:", e)
        except Exception as e:
            print(f"   failed: {type(e).__name__}: {e}")
            return

    if series is None:
        return

    for tf in TV_TIMEFRAMES:
        bars = series[tf]
        first = pd.to_datetime(bars[0][0], unit="s", utc=True).tz_convert("Asia/Kolkata")
        last = pd.to_datetime(bars[-1][0], unit="s", utc=True).tz_convert("Asia/Kolkata")
        print(f"   {tf:>3}: {len(bars):>5} bars, {first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M} IST")

    rows_by_tf = {}

    for tf in TV_TIMEFRAMES:
        frame = tv_bars_to_frame(series[tf], 0)
        cleaned = clean_candles(frame) if frame is not None else None

        if cleaned is None or cleaned.empty:
            print(f"   {tf}: every candle was dropped by cleaning")
            return

        rows_by_tf[tf] = cleaned

    ok, detail = tv_check_grid(rows_by_tf)
    print("2) candle times on the 09:15 grid:", "OK" if ok else "PROBLEM")
    print("   " + detail)

    result = evaluate_rows(rows_by_tf)
    result.update(symbol=symbol, source="TradingView", data_status="OK")
    print("3) the twelve candles the strategy reads (compare them with your chart):")
    print_audit(result)


def main():

    if len(sys.argv) > 1 and sys.argv[1] == "--check":
        require_websocket()
        audit_symbols(sys.argv[2:])
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--tv-debug":
        tv_debug(sys.argv[2:])
        return

    print()
    print("=" * 70)
    print("NSE EOD MOMENTUM SCANNER (TradingView native candles)")
    print("=" * 70)

    require_websocket()

    if DIRECTION_RULE not in DIRECTION_RULES:
        print(f"ERROR: DIRECTION_RULE must be one of {list(DIRECTION_RULES)}")
        sys.exit(1)

    print()
    print("Data source: TradingView - native 15m, 5m, 3m and 1m candles (nothing is calculated from another timeframe)")

    if not tv_preflight():
        print()
        print("STOPPED: TradingView's candles could not be verified, and there is no other source.")
        print("Run  python scanner.py --tv-debug RELIANCE  and check the output.")
        print()
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

    # No stock may be lost to a technical failure: retry the ones that errored.
    results = retry_failed(results)

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
        print("MATCHES:")
        for sig in matches:
            note = series_note(sig["symbol"])
            print(
                f"  {sig['symbol']:<15}"
                f"{sig['direction']:<7}"
                f"{sig['previous_day']}"
                + (f"   ({note})" if note else "")
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
