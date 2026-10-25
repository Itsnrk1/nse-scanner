# =============================================================================
# NSE DAILY MOMENTUM SCANNER - 15-MIN / 5-MIN OPPOSITE-TREND VERSION
# =============================================================================
#
# Uses Yahoo Finance NATIVE 15-minute and 5-minute candles (two downloads per
# symbol, no resampling).
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
#   5-minute  : ... 15:20, 15:25       (15:25 is the last, 15:20 the second last)
#
# ALL of these must hold:
#   1) 15m trend:  the last two candles (15:00, 15:15) are in OPPOSITE trends
#      (one Up, the other Down).
#   2) 15m volume: the 15:00 candle's volume is HIGHER than the 15:15 candle's.
#   3) 5m trend:   the last two candles (15:20, 15:25) are in OPPOSITE trends.
#   4) 5m volume:  the 15:20 candle's volume is HIGHER than the 15:25 candle's.
#   5) The 15m 15:00 candle and the 5m 15:20 candle have the SAME trend.
#
# "Trend" = candle colour (close > open is Up, close < open is Down). A flat
# candle (close == open) has no trend: it is never "opposite" and never "same".
#
# Because of (1), (3) and (5), the 15m 15:15 and 5m 15:25 candles also share
# one trend (the opposite of the 15:00 / 15:20 trend).
#
# DIRECTION: the strategy does not say which way to trade, so it is set by
# DIRECTION_RULE below (default: trade WITH the final candles' trend).
# TRADE:     Entry = the 09:15 OPEN of the entry day, Exit = EXIT_TIME_LABEL.
#
# A candle with no trades is absent from Yahoo's data and has no trend. A
# missing candle can therefore only BLOCK a match, never create one.
#
# DATA-ACCURACY SAFEGUARDS
#   * Prices are rounded to 2 decimals, so Yahoo's float noise can never turn a
#     flat candle into an Up/Down one.
#   * Zero-volume candles and corrupt candles (price <= 0, close/open outside
#     the high-low range) are dropped and treated as MISSING.
#   * Cross-check: the native 15m 15:00 and 15:15 candles are rebuilt from the
#     5m candles inside them (volume = sum of the 5m volumes). If the two Yahoo
#     feeds disagree on either candle's trend, or on which of the two 15m
#     candles has more volume, the symbol is marked DATA CONFLICT and never
#     produces a signal.
#   * Every row in the report shows each candle's open -> close, so any signal
#     can be checked against a chart in seconds.
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

# Yahoo throttles heavy parallel use. Each symbol now needs TWO downloads
# (15m and 5m). If you see many rate-limit ERRORs, lower this to 4-6.
MAX_WORKERS = 8

# Attempts per request (only rate limits / network errors are retried).
MAX_RETRIES = 3

# Native candles. Yahoo keeps about 60 days of 5-minute and 15-minute history.
INITIAL_PERIOD = {"15m": "5d", "5m": "5d"}

FALLBACK_PERIOD = {"15m": "1mo", "5m": "1mo"}

REQUEST_TIMEOUT = 20

# If fewer than this share of scanned symbols have all four required candles,
# the report shows a "data looks incomplete" warning. Kept low because
# illiquid stocks often have a no-trade candle; when Yahoo's data is genuinely
# broken the share is close to 0%.
SESSION_COMPLETE_SHARE = 0.20

# Cross-check the native 15m candles against the 5m candles inside them (see
# header). Keep True for accuracy; set False only to diagnose.
CROSS_CHECK_FEEDS = True

# If more than this share of scanned symbols show a 15m/5m conflict, the report
# warns that Yahoo's data is inconsistent today.
CONFLICT_WARN_SHARE = 0.10

# Which way to trade when a match fires. The final candles (15m 15:15 and
# 5m 15:25) always share one trend, and the earlier candles (15m 15:00 and
# 5m 15:20) share the opposite one, so there are only two choices:
#   "FOLLOW_LAST" -> trade WITH the final candles' trend (default; Up = LONG)
#   "FADE_LAST"   -> trade AGAINST it, i.e. with the 15:00 / 15:20 trend
DIRECTION_RULES = {
    "FOLLOW_LAST": 1,
    "FADE_LAST": -1,
}

DIRECTION_RULE = "FOLLOW_LAST"

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

# The four candles the strategy reads on the previous day: (timeframe, start).
SLOTS = [("15m", 1500), ("15m", 1515), ("5m", 1520), ("5m", 1525)]

SLOT_KEYS = [f"{tf}_{hm:04d}" for tf, hm in SLOTS]

# Timeframes downloaded for every symbol.
TIMEFRAMES = ("15m", "5m")


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

    # Regular NSE session only (09:15 - 15:29; the last candle starts at 15:25 or 15:15).
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
    """Returns (rows_by_timeframe, info): native 15m and 5m data, each straight
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


def window_hms(start_hm, minutes=15, step=5):
    """Start times of the step-minute candles inside the candle that starts at
    start_hm, e.g. 1515 -> [1515, 1520, 1525]."""

    start = (start_hm // 100) * 60 + start_hm % 100

    return [
        ((start + k) // 60) * 100 + (start + k) % 60
        for k in range(0, minutes, step)
    ]


def derive_15m_from_5m(day_5m, start_hm):
    """Rebuild the 15-minute candle starting at start_hm from the 5-minute
    candles inside it: open = first 5m open, close = last 5m close, volume =
    sum of the 5m volumes. A 5m
    candle exists only if it traded, so this equals Yahoo's real 15m candle.
    Returns None if no 5m candle exists inside the window."""

    bars = [day_5m[h] for h in window_hms(start_hm) if h in day_5m]

    if not bars:
        return None

    return {
        "open": bars[0][0],
        "close": bars[-1][3],
        "volume": int(round(sum(b[4] for b in bars))),
    }


def evaluate_rows(rows_by_tf):
    """15-minute / 5-minute opposite-trend strategy, read on the previous day
    (P = the latest completed session). Entry day = the next trading day.

    1) 15m 15:00 and 15:15 candles are in opposite trends, and the 15:00
       volume is higher than the 15:15 volume.
    2) 5m 15:20 and 15:25 candles are in opposite trends, and the 15:20
       volume is higher than the 15:25 volume.
    3) 15m 15:00 and 5m 15:20 candles have the same trend.

    Before a signal is allowed, the 15m candles are cross-checked against the
    5m candles inside them; a disagreement gives status CONFLICT, not a signal.
    """

    if not rows_by_tf:
        return {"status": "NO_DATA"}

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

    t15_00, t15_15 = trend("15m_1500"), trend("15m_1515")
    t5_20, t5_25 = trend("5m_1520"), trend("5m_1525")

    def volume(key):
        c = candles[key]
        return c["volume"] if c else None

    v15_00, v15_15 = volume("15m_1500"), volume("15m_1515")
    v5_20, v5_25 = volume("5m_1520"), volume("5m_1525")

    # Trend: a flat or missing candle (trend 0) is never "opposite" or "same".
    # Volume: both candles must exist. Either way a missing candle can only
    # block a match, never create one.
    trend_15m = t15_00 != 0 and t15_00 == -t15_15
    trend_5m = t5_20 != 0 and t5_20 == -t5_25

    vol_15m = v15_00 is not None and v15_15 is not None and v15_00 > v15_15
    vol_5m = v5_20 is not None and v5_25 is not None and v5_20 > v5_25

    cond_15m = trend_15m and vol_15m
    cond_5m = trend_5m and vol_5m
    cond_align = t15_00 != 0 and t15_00 == t5_20

    cond = cond_15m and cond_5m and cond_align

    # Cross-check the two Yahoo feeds against each other.
    five_day = by_tf.get("5m", {}).get(previous_day, {})
    derived, conflicts = {}, []

    for hm in (1500, 1515):
        key = f"15m_{hm:04d}"
        rebuilt = derive_15m_from_5m(five_day, hm)
        derived[key] = rebuilt
        native = candles[key]

        if not CROSS_CHECK_FEEDS or native is None:
            continue

        if rebuilt is None:
            conflicts.append(
                f"15m {hm_text(hm)} exists but the 5m feed has no candle inside it"
            )
            continue

        native_trend = candle_direction(native["open"], native["close"])
        rebuilt_trend = candle_direction(rebuilt["open"], rebuilt["close"])

        if native_trend != rebuilt_trend:
            conflicts.append(
                f"15m {hm_text(hm)} is {trend_name(native_trend)} in the 15m feed "
                f"but {trend_name(rebuilt_trend)} when rebuilt from 5m candles"
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
                f"15m feed but {rebuilt_higher} when rebuilt from 5m candles"
            )

    # When cond is True, 15m 15:15 and 5m 15:25 share one trend.
    side = t15_15 * DIRECTION_RULES[DIRECTION_RULE]
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
        "cond_5m": cond_5m,
        "cond_align": cond_align,
        "trend_15m": trend_15m,
        "vol_15m": vol_15m,
        "trend_5m": trend_5m,
        "vol_5m": vol_5m,
        "missing": missing,
        "conflicts": conflicts,
        "candles": candles,
        "derived": derived,
        "details": {
            "t15_00": t15_00, "t15_15": t15_15,
            "t5_20": t5_20, "t5_25": t5_25,
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
    of the 4 required candles, or many 15m/5m conflicts. Returns text or None."""

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
            f"Only {share:.0%} of scanned symbols have all four required candles "
            f"(15m 15:00 and 15:15; 5m 15:20 and 15:25) on the previous day. "
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
            f"{conflicts} of {len(evaluated)} symbols have a 15m/5m data conflict. "
            f"Yahoo's feeds are inconsistent with each other today; those "
            f"symbols were blocked from signalling."
        )

    return " ".join(messages) or None


# =============================================================================
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
        note_5m = (
            candle_text(candles.get("5m_1520"), "15:20") + "<br>"
            + candle_text(candles.get("5m_1525"), "15:25") + "<br>"
            + f'trend {part_badge(r.get("trend_5m"))} / '
            f'volume {part_badge(r.get("vol_5m"))}'
        )
        note_align = (
            f'15m 15:00 {trend_name(details["t15_00"])} vs '
            f'5m 15:20 {trend_name(details["t5_20"])}'
        )
    else:
        note_15m = note_5m = note_align = ""

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
<td>{badge(r.get('cond_5m'))}<br><small>{note_5m}</small></td>
<td>{badge(r.get('cond_align'))}<br><small>{note_align}</small></td>
<td class="{status_class}">{esc(STATUS_LABEL.get(status, status))}</td>
<td>{data_text}</td>
</tr>
"""


# =============================================================================
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
            "Direction follows the final candles (15m 15:15 / 5m 15:25): "
            "Up is LONG, Down is SHORT."
        )
    else:
        direction_text = (
            "Direction fades the final candles (15m 15:15 / 5m 15:25): "
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
Yahoo Finance 15-minute and 5-minute data<br>
Previous day: {esc(fmt_day(previous_day))}<br>
Entry: {esc(fmt_day(entry_day))} at the 09:15 open &nbsp;|&nbsp; Exit: {esc(EXIT_TIME_LABEL)}
</div>

{warning_html}

<div class="stats">{stats_html}</div>

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
<th>5m condition<br><small>15:20 &amp; 15:25 opposite trends; 15:20 vol &gt; 15:25 vol</small></th>
<th>Alignment<br><small>15m 15:00 trend = 5m 15:20 trend</small></th>
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
<b>STRATEGY (15-minute and 5-minute candles)</b><br>
Read on the previous day, the latest completed session. Candles are labelled by start
time: the last 15-minute candle is 15:15 (second last 15:00); the last 5-minute candle
is 15:25 (second last 15:20). A stock matches only when ALL of these hold:<br>
1) 15m trend: the 15:00 and 15:15 candles are in opposite trends (one Up, one Down).<br>
2) 15m volume: the 15:00 candle's volume is higher than the 15:15 candle's.<br>
3) 5m trend: the 15:20 and 15:25 candles are in opposite trends.<br>
4) 5m volume: the 15:20 candle's volume is higher than the 15:25 candle's.<br>
5) The 15m 15:00 candle and the 5m 15:20 candle have the same trend.<br>
Trend = candle colour (close vs open); a flat candle has no trend, so it is never
opposite or the same.
{esc(direction_text)}<br>
<b>Entry:</b> the 09:15 open on entry day. <b>Exit:</b> {esc(EXIT_TIME_LABEL)}.<br><br>
<b>Data notes:</b> Candles come straight from Yahoo's 15-minute and 5-minute feeds.
Prices are rounded to 2 decimals; zero-volume and corrupt candles are dropped and count as
missing, which can only block a match. DATA CONFLICT means a native 15m candle's trend, or which 15m candle has more volume,
disagrees with the same candles rebuilt from the 5m candles inside them, so the symbol is
blocked rather than trusted. INCOMPLETE means no completed session (or none of the four
candles) came back. STALE means the previous day differs from most other symbols. Each
candle is shown as open&rarr;close so a signal can be checked against a chart.
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
                f"   | rebuilt from 5m: {trend_name(rd)} "
                f"({rb['open']:.2f} -> {rb['close']:.2f}) vol {rb['volume']:,}"
            )

        print(line)

    if "cond_15m" in r:
        print(f"  15m opposite trends : {r['trend_15m']}")
        print(f"  15m 15:00 vol > 15:15: {r['vol_15m']}")
        print(f"  5m opposite trends  : {r['trend_5m']}")
        print(f"  5m 15:20 vol > 15:25 : {r['vol_5m']}")
        print(f"  15:00 == 5m 15:20   : {r['cond_align']}")

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
