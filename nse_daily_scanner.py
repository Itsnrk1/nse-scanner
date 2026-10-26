# =============================================================================
# NSE DAILY MOMENTUM SCANNER - 15-MIN / 3-MIN / 1-MIN CLOSING-CANDLE VERSION
# =============================================================================
#
# Downloads Yahoo Finance NATIVE 15-minute and 1-minute candles (two downloads
# per symbol). Yahoo has no 3-minute feed, so the 3-minute candles are built
# from the 1-minute candles.
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
#   1) 15m trend:  the last two candles (15:00, 15:15) are in OPPOSITE trends
#      (one Up, the other Down).
#   2) 15m volume: the 15:00 candle's volume is HIGHER than the 15:15 candle's.
#   3) 3m volume:  the 15:24 candle's volume is HIGHER than the 15:27 candle's.
#   4) 1m volume:  the 15:28 candle's volume is HIGHER than the 15:29 candle's.
#   5) The second-last candles of all three timeframes (15m 15:00, 3m 15:24,
#      1m 15:28) have the SAME trend.
#
# The 3m and 1m timeframes have NO opposite-trend rule: their last two candles
# may have the same or opposite trends. Only their volume order matters (and
# their second-last candle's trend, for the alignment in 5).
#
# "Trend" = candle colour (close > open is Up, close < open is Down). A flat
# candle (close == open) has no trend: it is never "opposite" and never "same".
#
# DIRECTION: the strategy does not say which way to trade, so it is set by
# DIRECTION_RULE below (default: trade WITH the aligned second-last candles'
# trend, i.e. the 15m 15:00 / 3m 15:24 / 1m 15:28 trend: Up = LONG, Down = SHORT).
# TRADE:     Entry = the 09:15 OPEN of the entry day, Exit = EXIT_TIME_LABEL.
#
# A candle with no trades is absent from Yahoo's data. A missing candle can
# therefore only BLOCK a match, never create one.
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

# If fewer than this share of scanned symbols have all six required candles,
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
# REQUIRED CANDLES
# =============================================================================

# The six candles the strategy reads on the previous day: (timeframe, start).
# 15m and 1m come straight from Yahoo; 3m is built from the 1m candles.
SLOTS = [
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


def fetch_symbol_rows(symbol):
    """Returns (rows_by_timeframe, info): native 15m and 1m data, each straight
    from Yahoo's own feed for that timeframe. If rows_by_timeframe is None,
    info is "NO_DATA" or an error message. Otherwise info is "OK" or
    "FALLBACK" (a longer window was needed for at least one timeframe)."""

    # Small random delay so the workers do not hit Yahoo in one burst.
    time.sleep(random.uniform(0.05, 0.25))

    rows_by_tf, infos, errors = {}, [], []

    for tf in TIMEFRAMES:
        rows, info = fetch_interval(symbol, tf)

        if rows is None:
            errors.append(info)
        else:
            rows_by_tf[tf] = rows
            infos.append(info)

    if len(rows_by_tf) == len(TIMEFRAMES):
        return rows_by_tf, ("FALLBACK" if "FALLBACK" in infos else "OK")

    real_errors = [e for e in errors if e != "NO_DATA"]

    return None, (real_errors[0] if real_errors else "NO_DATA")


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
                "The market has just closed and Yahoo's closing candles are not "
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


def resample_to_3m(rows):
    """Build 3-minute candles from 1-minute rows (Yahoo has no 3-minute feed).

    Buckets are anchored to the 09:15 open (09:15, 09:18, ... 15:24, 15:27), so
    the last two candles of the session are 15:24 and 15:27. Within a bucket:
    open = first, high = max, low = min, close = last, volume = sum. A bucket
    exists if at least one of its 1-minute candles traded."""

    if rows is None or rows.empty:
        return rows

    work = rows.sort_values(["date", "hm"]).copy()

    minutes = (work["hm"] // 100) * 60 + (work["hm"] % 100)
    start = MARKET_OPEN_MINUTES + ((minutes - MARKET_OPEN_MINUTES) // 3) * 3
    work["hm3"] = (start // 60) * 100 + (start % 60)

    return (
        work.groupby(["date", "hm3"], sort=True)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )
        .reset_index()
        .rename(columns={"hm3": "hm"})
    )


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

    1) 15m 15:00 and 15:15 candles are in opposite trends.
    2) 15m 15:00 volume is higher than 15m 15:15 volume.
    3) 3m 15:24 volume is higher than 3m 15:27 volume.
    4) 1m 15:28 volume is higher than 1m 15:29 volume.
    5) 15m 15:00, 3m 15:24 and 1m 15:28 candles all have the same trend.

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

    v15_00, v15_15 = volume("15m_1500"), volume("15m_1515")
    v3_24, v3_27 = volume("3m_1524"), volume("3m_1527")
    v1_28, v1_29 = volume("1m_1528"), volume("1m_1529")

    def higher(first, second):
        # Both candles must exist; a missing candle can only block a match.
        return first is not None and second is not None and first > second

    # 15m: opposite trends (flat or missing = trend 0, never opposite) + volume.
    trend_15m = t15_00 != 0 and t15_00 == -t15_15
    vol_15m = higher(v15_00, v15_15)

    # 3m and 1m: volume only. Their two candles may have the same or opposite
    # trends.
    vol_3m = higher(v3_24, v3_27)
    vol_1m = higher(v1_28, v1_29)

    cond_15m = trend_15m and vol_15m
    cond_3m = vol_3m
    cond_1m = vol_1m

    # Second-last candles of all three timeframes share one (non-flat) trend.
    cond_align = t15_00 != 0 and t15_00 == t3_24 == t1_28

    cond = cond_15m and cond_3m and cond_1m and cond_align

    # Cross-check the two Yahoo feeds against each other (15m vs the 1m
    # candles inside it).
    one_day = by_tf.get("1m", {}).get(previous_day, {})
    derived, conflicts = {}, []

    for hm in (1500, 1515):
        key = f"15m_{hm:04d}"
        rebuilt = derive_15m_from_1m(one_day, hm)
        derived[key] = rebuilt
        native = candles[key]

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
        "trend_15m": trend_15m,
        "vol_15m": vol_15m,
        "vol_3m": vol_3m,
        "vol_1m": vol_1m,
        "missing": missing,
        "conflicts": conflicts,
        "candles": candles,
        "derived": derived,
        "details": {
            "t15_00": t15_00, "t15_15": t15_15,
            "t3_24": t3_24, "t3_27": t3_27,
            "t1_28": t1_28, "t1_29": t1_29,
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
                return {"symbol": symbol, "status": "NO_DATA", "data_status": "NO_DATA"}

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
    of the 6 required candles, or many 15m/1m conflicts. Returns text or None."""

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
            f"Only {share:.0%} of scanned symbols have all six required candles "
            f"(15m 15:00 and 15:15; 3m 15:24 and 15:27; 1m 15:28 and 15:29) on "
            f"the previous day. "
            f"Yahoo's data may be unusually incomplete - treat matches with "
            f"extra caution and verify on TradingView."
            + (
                " Yahoo can lag a few minutes after the close; if you just ran "
                "this, try again shortly."
                if get_run_context()["today_complete"] else ""
            )
        )

    conflicts = sum(1 for r in evaluated if r.get("status") == "CONFLICT")

    if conflicts / len(evaluated) > CONFLICT_WARN_SHARE:
        messages.append(
            f"{conflicts} of {len(evaluated)} symbols have a 15m/1m data conflict. "
            f"Yahoo's feeds are inconsistent with each other today; those "
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


def build_funnel(results):
    """Where stocks drop out, step by step. Returns [(label, count), ...];
    each line counts only the stocks that survived every line above it, so the
    last line is the number of signals. It separates data problems (download,
    missing candles, conflicts) from the strategy simply being strict."""

    got_data = [r for r in results if r.get("status") not in ("NO_DATA", "ERROR")]
    evaluated = [r for r in results if "cond_15m" in r]
    full = [r for r in evaluated if not r.get("missing")]
    step_1 = [r for r in full if r.get("trend_15m")]
    step_2 = [r for r in step_1 if r.get("vol_15m")]
    step_3 = [r for r in step_2 if r.get("vol_3m")]
    step_4 = [r for r in step_3 if r.get("vol_1m")]
    step_5 = [r for r in step_4 if r.get("cond_align")]
    step_6 = [r for r in step_5 if r.get("status") != "CONFLICT"]
    step_7 = [r for r in step_6 if r.get("status") != "STALE"]

    return [
        ("Stocks scanned", len(results)),
        ("Data downloaded (no error / no-data)", len(got_data)),
        ("A completed session found", len(evaluated)),
        ("All six candles present", len(full)),
        ("15m: 15:00 and 15:15 opposite trends", len(step_1)),
        ("15m: 15:00 volume > 15:15 volume", len(step_2)),
        ("3m: 15:24 volume > 15:27 volume", len(step_3)),
        ("1m: 15:28 volume > 15:29 volume", len(step_4)),
        ("Same trend: 15m 15:00 = 3m 15:24 = 1m 15:28", len(step_5)),
        ("No 15m/1m data conflict", len(step_6)),
        ("Same session as most stocks (SIGNALS)", len(step_7)),
    ]


def funnel_console(results):

    lines, previous = [], None

    for label, count in build_funnel(results):
        lost = f"  (-{previous - count})" if previous is not None and previous > count else ""
        lines.append(f"  {count:>5}  {label}{lost}")
        previous = count

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

    return (
        '<div class="match-box"><h2>Where stocks dropped out</h2>'
        '<div class="table-wrap"><table class="funnel"><tbody>'
        f'{body}</tbody></table></div></div>'
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
            + f'trend {part_badge(r.get("trend_15m"))} / '
            f'volume {part_badge(r.get("vol_15m"))}'
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
    else:
        note_15m = note_3m = note_1m = note_align = ""

    data_label = {
        "OK": "OK", "FALLBACK": "Fallback window", "NO_DATA": "No data", "ERROR": "Error",
    }.get(r.get("data_status"), r.get("data_status"))

    data_text = cell(data_label)

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

    warning = session_warning(results)
    warning_html = (
        (f'<div class="note">{esc(run_note)}</div>' if run_note else "")
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
Yahoo Finance 15-minute and 1-minute data (3-minute candles built from 1-minute)<br>
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
<th>15m condition<br><small>15:00 &amp; 15:15 opposite trends; 15:00 vol &gt; 15:15 vol</small></th>
<th>3m condition<br><small>15:24 vol &gt; 15:27 vol</small></th>
<th>1m condition<br><small>15:28 vol &gt; 15:29 vol</small></th>
<th>Alignment<br><small>15m 15:00 = 3m 15:24 = 1m 15:28 trend</small></th>
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
1) 15m trend: the 15:00 and 15:15 candles are in opposite trends (one Up, one Down).<br>
2) 15m volume: the 15:00 candle's volume is higher than the 15:15 candle's.<br>
3) 3m volume: the 15:24 candle's volume is higher than the 15:27 candle's.<br>
4) 1m volume: the 15:28 candle's volume is higher than the 15:29 candle's.<br>
5) The 15m 15:00, 3m 15:24 and 1m 15:28 candles all have the same trend.<br>
The 3m and 1m timeframes have no opposite-trend rule: their last two candles may have the
same or opposite trends.<br>
Trend = candle colour (close vs open); a flat candle has no trend, so it is never
opposite or the same.
{esc(direction_text)}<br>
<b>Entry:</b> the 09:15 open on entry day. <b>Exit:</b> {esc(EXIT_TIME_LABEL)}.<br><br>
<b>Data notes:</b> 15-minute and 1-minute candles come straight from Yahoo's own feeds;
Yahoo has no 3-minute feed, so 3-minute candles are built from the 1-minute candles on a
09:15 grid (open = first, close = last, volume = sum). Prices are rounded to 2 decimals;
zero-volume and corrupt candles are dropped and count as missing, which can only block a
match. DATA CONFLICT means a native 15m candle's trend, or which 15m candle has more
volume, disagrees with the same candles rebuilt from the 1m candles inside them, so the
symbol is blocked rather than trusted. INCOMPLETE means no completed session (or none of
the six candles) came back. STALE means the previous day differs from most other symbols.
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
          f"   (data: {r.get('data_status')})")

    if r.get("error"):
        print("  error:", r["error"])

    if not r.get("previous_day"):
        print("  no completed session found")
        return

    print(f"  Previous day: {r['previous_day']}   Entry day: {r.get('entry_day')}")

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
        print(f"  15m opposite trends        : {r['trend_15m']}")
        print(f"  15m 15:00 vol > 15:15 vol  : {r['vol_15m']}")
        print(f"  3m  15:24 vol > 15:27 vol  : {r['vol_3m']}")
        print(f"  1m  15:28 vol > 15:29 vol  : {r['vol_1m']}")
        print(f"  15m 15:00 = 3m 15:24 = 1m 15:28 trend: {r['cond_align']}")

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


def main():

    if len(sys.argv) > 1 and sys.argv[1] == "--check":
        audit_symbols(sys.argv[2:])
        return

    print()
    print("=" * 70)
    print("NSE EOD MOMENTUM SCANNER")
    print("=" * 70)
    print()
    print("yfinance version:", getattr(yf, "__version__", "unknown"))

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
