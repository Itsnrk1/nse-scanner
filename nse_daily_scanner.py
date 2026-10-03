# =============================================================================
# NSE DAILY MOMENTUM SCANNER - FIXED VERSION
# =============================================================================
#
# Uses Yahoo Finance NATIVE 5-minute candles (no aggregation from 1-minute
# bars). No Parquet, no nselib.
# Universe: Nifty 500 CSV -> NSE equity list -> built-in fallback.
# Output: index.html
#
# =============================================================================
# STRATEGY  (5-minute timeframe)
# =============================================================================
#
# "Previous day"  (P) = the most recent COMPLETED trading session (today is
#                       never used - today is the entry day).
# "Day before previous day" (B) = the completed session right before P.
# "Next day"      = entry day = today.
#
# NSE 5-minute candles are labelled by their start time: 09:15 ... 15:25.
# 15:25 is the LAST candle of the session, 15:20 the second-to-last.
#
# 1) On P:  the 15:20 candle's trend is DIFFERENT from the 15:25 candle's
#    trend, AND at least 2 of the candles 15:05, 15:10, 15:15 share the
#    15:20 candle's trend.
#
# 2) On B:  the 15:20 candle's trend MATCHES P's 15:20 trend, AND B's 15:20
#    volume is greater than B's 15:25 volume.
#
# DIRECTION: set by DIRECTION_RULE below (default: follow the 15:20 trend).
# TRADE:     Entry = today's 09:15 OPEN, Exit = EXIT_TIME_LABEL.
#
# A candle with no trades is simply absent from Yahoo's data. It is treated
# as "no trend" (so it can never satisfy a trend condition). The one case
# where that could create a false signal is B's 15:25 candle (its volume
# would read as zero), so that case is flagged "Verify".
#
# INSTALL:
#   python -m pip install yfinance pandas requests curl_cffi
#
# =============================================================================

import html
import json
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

# Native 5-minute candles. Yahoo keeps about 60 days of 5-minute history,
# so a month-long fallback is safely inside the limit.
INTERVAL = "5m"

INITIAL_PERIOD = "5d"

FALLBACK_PERIOD = "1mo"

REQUEST_TIMEOUT = 20

# If fewer than this share of scanned symbols have all 7 required 5-minute
# candles, the report shows a "data looks incomplete" warning. Kept low
# because illiquid stocks often have a no-trade candle; when Yahoo's data
# is genuinely broken the share is close to 0%.
SESSION_COMPLETE_SHARE = 0.20

# Which way to trade when a signal fires. The strategy text defines the
# conditions but not the direction, so this is a one-line switch:
#   "FOLLOW_1520" -> trade in the direction of the 15:20 trend (default)
#   "FOLLOW_1525" -> trade in the direction of the 15:25 trend, i.e. against
#                    the 15:20 trend (the signal requires them to differ)
DIRECTION_RULE = "FOLLOW_1520"

# Exit time shown in the report (the strategy text did not change this).
EXIT_TIME_LABEL = "15:27"


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
# REQUIRED 5-MINUTE CANDLES
# =============================================================================

# The seven native 5-minute candles the strategy reads.
#   P = previous day (latest completed session)
#   B = day before previous day
# Slot keys are "<day><hhmm>", e.g. "P1520" = previous day's 15:20 candle.
SLOTS = [
    ("B", 1520), ("B", 1525),
    ("P", 1505), ("P", 1510), ("P", 1515), ("P", 1520), ("P", 1525),
]

SLOT_KEYS = [f"{day}{hm:04d}" for day, hm in SLOTS]


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
                interval=INTERVAL,
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


def fetch_symbol_rows(symbol):
    """Returns (rows, info). If rows is None, info is "NO_DATA" or an
    error message. Otherwise info is "OK" or "FALLBACK"."""

    # Small random delay so the workers do not hit Yahoo in one burst.
    time.sleep(random.uniform(0.05, 0.25))

    rows, error = yahoo_download(symbol, INITIAL_PERIOD)

    # The strategy reads two completed sessions, so a short window (long
    # weekend / holidays) gets one longer retry. A rate-limit error does
    # not: a longer request would only be throttled again.
    if rows is not None and completed_day_count(rows) >= 2:
        return rows, "OK"

    if rows is not None or error == "NO_DATA":
        rows2, error2 = yahoo_download(symbol, FALLBACK_PERIOD)

        if rows2 is not None:
            return rows2, "FALLBACK"

        if rows is not None:
            return rows, "OK"   # evaluate_rows will report INCOMPLETE

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


def slot_label(key):
    """'P1520' -> 'P 15:20' (used in missing/verify notes)."""

    return f"{key[0]} {hm_text(int(key[1:]))}"


def evaluate_rows(rows):
    """Native 5-minute strategy.

    P = previous day (latest completed session), B = the session before it.
    Entry day = today.

    1) On P: the 15:20 candle's trend differs from the 15:25 candle's, and
       at least 2 of the candles 15:05, 15:10, 15:15 share 15:20's trend.
    2) On B: the 15:20 trend matches P's 15:20 trend, and B's 15:20 volume
       is greater than B's 15:25 volume.
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

    # "Today" is the real calendar date and is never a candidate: it is the
    # entry day. Using the real date (not "latest date in the data") keeps
    # the mapping identical whether the scan runs before the open or
    # mid-session.
    today = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")
    dates = sorted(d for d in by_date if d != today)

    if len(dates) < 2:
        return {
            "status": "INCOMPLETE",
            "date": dates[-1] if dates else None,
            "previous_day": dates[-1] if dates else None,
            "day_before": None,
            "entry_day": today,
            "missing": [slot_label(k) for k in SLOT_KEYS],
            "note": "fewer than 2 completed trading days of data before today",
        }

    previous_day = dates[-1]       # P
    day_before = dates[-2]         # B
    day_data = {"P": by_date[previous_day], "B": by_date[day_before]}

    # ---------------------------------------------------------------------
    # THE SEVEN CANDLES
    # ---------------------------------------------------------------------

    candles = {}
    missing = []

    for day, hm in SLOTS:
        key = f"{day}{hm:04d}"
        bar = day_data[day].get(hm)

        if bar is None:
            candles[key] = None
            missing.append(slot_label(key))
        else:
            o, h, l, c, v = bar
            candles[key] = {"open": o, "high": h, "low": l, "close": c, "volume": int(round(v))}

    if len(missing) == len(SLOTS):
        return {
            "status": "INCOMPLETE",
            "date": previous_day,
            "previous_day": previous_day,
            "day_before": day_before,
            "entry_day": today,
            "missing": missing,
        }

    def trend(key):
        c = candles.get(key)
        return candle_direction(c["open"], c["close"]) if c else 0

    def volume(key):
        c = candles.get(key)
        return c["volume"] if c else 0

    d = {key: trend(key) for key in SLOT_KEYS}

    p1520, p1525 = d["P1520"], d["P1525"]

    # CONDITION 1a (P): 15:20 and 15:25 trends differ (both must be defined).
    cond1a = p1520 != 0 and p1525 != 0 and p1520 != p1525

    # CONDITION 1b (P): at least 2 of 15:05 / 15:10 / 15:15 share 15:20's trend.
    earlier = ["P1505", "P1510", "P1515"]
    matches = sum(1 for k in earlier if p1520 != 0 and d[k] == p1520)
    cond1b = matches >= 2

    # CONDITION 2a (B): B's 15:20 trend matches P's 15:20 trend.
    cond2a = p1520 != 0 and d["B1520"] == p1520

    # CONDITION 2b (B): B's 15:20 volume is greater than B's 15:25 volume.
    v_b1520, v_b1525 = volume("B1520"), volume("B1525")
    cond2b = candles["B1520"] is not None and v_b1520 > v_b1525

    cond1 = cond1a and cond1b
    cond2 = cond2a and cond2b
    passed = cond1 and cond2

    # DIRECTION
    anchor = p1520 if DIRECTION_RULE == "FOLLOW_1520" else p1525
    direction = "LONG" if anchor == 1 else "SHORT" if anchor == -1 else None

    # A missing B 15:25 candle makes its volume read as zero, which can turn
    # cond2b into a false pass. That is the only missing-data case that can
    # manufacture a signal, so it is the one flagged for manual verification.
    verify_notes = []
    if candles["B1525"] is None:
        verify_notes.append("B 15:25 has no data, so its volume was read as zero")

    # ---- coordinates for the 3D overview in the report ----
    # stage: how many conditions the stock cleared IN ORDER (1a, 1b, 2a, 2b).
    #        Where it stops is where it "fell out of the funnel".
    # score: how many of the four it passed in total.
    # x:     day-before volume ratio on a log scale. Right of centre means
    #        B's 15:20 volume beat its 15:25 volume (condition 2b).
    # y:     previous-day reversal: how far the 15:25 candle moved AGAINST
    #        the 15:20 trend. Above centre means the trends differ (1a).
    stage = 0
    if cond1a:
        stage = 1
        if cond1b:
            stage = 2
            if cond2a:
                stage = 3
                if cond2b:
                    stage = 4

    score = sum([cond1a, cond1b, cond2a, cond2b])

    ratio = (v_b1520 / v_b1525) if v_b1525 > 0 else (8.0 if v_b1520 > 0 else 0.125)
    vol_log2 = max(-3.0, min(3.0, math.log2(max(ratio, 0.125))))

    c25 = candles["P1525"]
    if c25 and c25["open"] and p1520 != 0:
        reversal = -p1520 * ((c25["close"] / c25["open"] - 1) * 100)
    else:
        reversal = 0.0
    reversal = max(-0.8, min(0.8, reversal))

    return {
        "status": "PASS" if passed else "FAIL",
        "date": previous_day,
        "previous_day": previous_day,
        "day_before": day_before,
        "entry_day": today,
        "direction": direction if passed else None,
        "raw_direction": direction,
        "cond1": cond1,
        "cond1a": cond1a,
        "cond1b": cond1b,
        "cond1b_matches": matches,
        "cond2": cond2,
        "cond2a": cond2a,
        "cond2b": cond2b,
        "missing": missing,
        "verify_notes": verify_notes,
        "needs_verify": bool(verify_notes),
        "scene": {
            "stage": stage,
            "score": score,
            "x": round(vol_log2 / 3.0, 3),
            "y": round(reversal / 0.8, 3),
        },
        "ohlc": {k: v for k, v in candles.items()},
        "details": {
            **{f"d{k}": d[k] for k in SLOT_KEYS},
            **{f"{k}_vol": volume(k) for k in SLOT_KEYS},
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
    """The strategy reads TWO sessions (previous day and the day before).
    A symbol whose pair of sessions differs from the pair most symbols used
    (a suspended stock, a missing day) cannot be compared fairly, so it is
    marked STALE instead of producing a signal. Returns the reference
    previous day."""

    evaluated = [r for r in results if r.get("status") in ("PASS", "FAIL")]
    pairs = [(r.get("previous_day"), r.get("day_before")) for r in evaluated
             if r.get("previous_day") and r.get("day_before")]

    if not pairs:
        return None

    reference = Counter(pairs).most_common(1)[0][0]

    for r in evaluated:
        if (r.get("previous_day"), r.get("day_before")) != reference:
            r["status"] = "STALE"
            r["direction"] = None

    return reference[0]


def session_warning(results):
    """Warn when a large share of symbols are missing some of the 7 required
    5-minute candles - a sign of a broad Yahoo data-quality issue, not just
    isolated thin trading."""

    evaluated = [r for r in results if r.get("status") in ("PASS", "FAIL")]

    if not evaluated:
        return None

    complete = sum(1 for r in evaluated if not r.get("missing"))
    share = complete / len(evaluated)

    if share < SESSION_COMPLETE_SHARE:
        return (
            f"Only {share:.0%} of scanned symbols have all 7 required "
            f"5-minute candles across the previous day and the day before. "
            f"Yahoo's data may be unusually incomplete - treat matches with "
            f"extra caution and verify on TradingView."
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


def arrow(d):
    return {1: "\u25b2", -1: "\u25bc"}.get(d, "\u2013")  # up, down, dash


def trend_class(d):
    return {1: "up", -1: "down"}.get(d, "flat")


def slot_time(key):
    """'P1520' -> '15:20'."""

    return hm_text(int(key[1:]))


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


# The four candles whose trend/volume the strategy actually compares.
KEY_SLOTS = ("B1520", "B1525", "P1520", "P1525")


def build_candlestick_svg(ohlc, size="large", volumes=None):
    """Real OHLC candlesticks for the seven 5-minute candles the strategy
    reads: the day before previous (15:20, 15:25) then the previous day
    (15:05 to 15:25). The large version also draws volume bars (the day-
    before 15:20 vs 15:25 volumes are what condition 2 compares), highlights
    both 15:20 anchor candles, and captions each day."""

    large = size == "large"

    if large:
        width, height, pad_x = 340, 186, 12
        c_top, c_h = 18, 72
        v_base, v_h = 132, 26
        label_y, bracket_y, day_y = 152, 160, 174
    else:
        width, height, pad_x = 150, 34, 4
        c_top, c_h = 4, 26
        v_base = v_h = label_y = bracket_y = day_y = 0

    ohlc = ohlc or {}
    n = len(SLOT_KEYS)
    slot_w = (width - 2 * pad_x) / n
    body_w = max(2.2, slot_w * (0.40 if large else 0.46))

    present = [ohlc[k] for k in SLOT_KEYS if ohlc.get(k)]

    attrs = (
        f'class="candles-svg candles-{size}" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" preserveAspectRatio="xMidYMid meet" role="img"'
    )

    if not present:
        return f'<svg {attrs} aria-label="No candle data"></svg>'

    hi = max(c["high"] for c in present)
    lo = min(c["low"] for c in present)
    if hi == lo:
        hi, lo = hi + 0.5, lo - 0.5
    span = hi - lo

    def y(price):
        return c_top + (hi - price) / span * c_h

    def cx_of(i):
        return pad_x + slot_w * i + slot_w / 2

    parts = []

    # Divider between the two days.
    div_x = pad_x + slot_w * 2
    parts.append(
        f'<line x1="{div_x:.1f}" y1="{3 if large else 2}" x2="{div_x:.1f}" '
        f'y2="{(bracket_y - 4) if large else height - 2}" stroke="var(--line-strong)" '
        f'stroke-width="1" stroke-dasharray="2,3"/>'
    )

    if large:
        # Highlight bands behind the two 15:20 anchor candles.
        for k in ("B1520", "P1520"):
            ax = pad_x + slot_w * SLOT_KEYS.index(k)
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
    for i, key in enumerate(SLOT_KEYS):
        cx = cx_of(i)
        c = ohlc.get(key)

        if not c:
            ym = c_top + c_h / 2
            parts.append(
                f'<line x1="{cx - body_w/2:.1f}" y1="{ym:.1f}" x2="{cx + body_w/2:.1f}" '
                f'y2="{ym:.1f}" stroke="var(--text-faint)" stroke-width="1.5" '
                f'stroke-dasharray="1.5,2"/>'
            )
            continue

        color = "var(--long)" if c["close"] >= c["open"] else "var(--short)"
        body_top = min(y(c["open"]), y(c["close"]))
        body_h = max(1.6, abs(y(c["open"]) - y(c["close"])))

        parts.append(
            f'<line x1="{cx:.1f}" y1="{y(c["high"]):.1f}" x2="{cx:.1f}" y2="{y(c["low"]):.1f}" '
            f'stroke="{color}" stroke-width="1.3" stroke-linecap="round"/>'
            f'<rect x="{cx - body_w/2:.1f}" y="{body_top:.1f}" width="{body_w:.1f}" '
            f'height="{body_h:.1f}" fill="{color}" rx="1.5"/>'
        )

    # --- volume bars, time labels, day captions (large only) ---
    if large:
        vols = {k: (volumes or {}).get(k) for k in SLOT_KEYS}
        vmax = max([v for v in vols.values() if v] or [0])

        parts.append(
            f'<line x1="{pad_x}" y1="{v_base}" x2="{width - pad_x}" y2="{v_base}" '
            f'stroke="var(--line-strong)" stroke-width="1"/>'
            f'<text x="{pad_x + 1}" y="{v_base - v_h - 4}" class="axis-label">volume</text>'
        )

        for i, key in enumerate(SLOT_KEYS):
            cx = cx_of(i)
            v = vols.get(key)
            c = ohlc.get(key)

            if v and vmax > 0:
                bar_h = max(1.5, v / vmax * v_h)
                color = "var(--long)" if (not c or c["close"] >= c["open"]) else "var(--short)"
                emphasised = key in ("B1520", "B1525")
                parts.append(
                    f'<rect x="{cx - body_w/2:.1f}" y="{v_base - bar_h:.1f}" '
                    f'width="{body_w:.1f}" height="{bar_h:.1f}" rx="1.5" fill="{color}" '
                    f'fill-opacity="{0.95 if emphasised else 0.38}"/>'
                )

            strong = " axis-strong" if key in KEY_SLOTS else ""
            parts.append(
                f'<text x="{cx:.1f}" y="{label_y}" text-anchor="middle" '
                f'class="axis-label{strong}">{slot_time(key)}</text>'
            )

        # Day brackets + captions.
        for first, last, caption in ((0, 1, "Day before"), (2, 6, "Previous day")):
            x1 = pad_x + slot_w * first + 4
            x2 = pad_x + slot_w * (last + 1) - 4
            parts.append(
                f'<path d="M{x1:.1f} {bracket_y - 3} V{bracket_y} H{x2:.1f} V{bracket_y - 3}" '
                f'fill="none" stroke="var(--line-strong)" stroke-width="1"/>'
                f'<text x="{(x1 + x2) / 2:.1f}" y="{day_y}" text-anchor="middle" '
                f'class="axis-label axis-day">{caption}</text>'
            )

    return f'<svg {attrs} aria-label="Seven 5-minute candles across two sessions">' + "".join(parts) + '</svg>'


CSS = """
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

:root {
  --bg: #07061B;
  --panel: rgba(18, 16, 42, 0.62);
  --line: rgba(190, 180, 255, 0.09);
  --line-strong: rgba(190, 180, 255, 0.17);
  --border: rgba(190, 180, 255, 0.17);
  --border-soft: rgba(190, 180, 255, 0.09);
  --text: #EEEDFB;
  --text-dim: #A7A4CC;
  --text-faint: #6E6C98;
  --long: #3DDC97;
  --long-soft: rgba(61, 220, 151, 0.14);
  --short: #FF6B7F;
  --short-soft: rgba(255, 107, 127, 0.14);
  --caution: #FFB257;
  --caution-soft: rgba(255, 178, 87, 0.14);
  --accent: #A99BFF;
  --accent-soft: rgba(169, 155, 255, 0.15);
  --sans: 'Sora', ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif;
  --mono: 'IBM Plex Mono', ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
}

* { box-sizing: border-box; }

html {
  background: var(--bg);
  scroll-behavior: smooth;
  scrollbar-color: rgba(169, 155, 255, 0.35) transparent;
}

body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: var(--sans);
  font-size: 15px;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}

::selection { background: rgba(169, 155, 255, 0.35); }
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
a { color: inherit; }

/* ---------- atmosphere ---------- */

.aurora, .grid-bg { position: fixed; inset: 0; pointer-events: none; z-index: 0; }
.aurora { overflow: hidden; }

.aurora i { position: absolute; border-radius: 50%; filter: blur(80px); opacity: 0.65; }

.aurora .a {
  width: 700px; height: 700px; left: -180px; top: -240px;
  background: radial-gradient(circle, rgba(124, 92, 255, 0.34), transparent 65%);
  animation: drift-a 28s ease-in-out infinite alternate;
}
.aurora .b {
  width: 620px; height: 620px; right: -180px; top: 20%;
  background: radial-gradient(circle, rgba(214, 92, 200, 0.16), transparent 65%);
  animation: drift-b 34s ease-in-out infinite alternate;
}
.aurora .c {
  width: 820px; height: 520px; left: 22%; bottom: -300px;
  background: radial-gradient(circle, rgba(255, 150, 60, 0.20), transparent 65%);
  animation: drift-a 40s ease-in-out infinite alternate-reverse;
}

@keyframes drift-a { to { transform: translate3d(90px, 60px, 0) scale(1.1); } }
@keyframes drift-b { to { transform: translate3d(-80px, 90px, 0) scale(1.08); } }

.grid-bg {
  background-image:
    linear-gradient(rgba(190, 180, 255, 0.025) 1px, transparent 1px),
    linear-gradient(90deg, rgba(190, 180, 255, 0.025) 1px, transparent 1px);
  background-size: 52px 52px;
  -webkit-mask-image: radial-gradient(ellipse at 50% 12%, #000 10%, transparent 70%);
  mask-image: radial-gradient(ellipse at 50% 12%, #000 10%, transparent 70%);
}

.cursor-glow {
  position: fixed; left: 0; top: 0;
  width: 640px; height: 640px;
  margin: -320px 0 0 -320px;
  border-radius: 50%;
  pointer-events: none;
  z-index: 2;
  background: radial-gradient(circle, rgba(169, 155, 255, 0.10), transparent 62%);
  mix-blend-mode: screen;
  opacity: 0;
  transition: opacity 0.6s;
}
.cursor-glow.on { opacity: 1; }

/* ---------- top bar ---------- */

.topbar {
  position: fixed;
  top: 0; left: 0; right: 0;
  z-index: 50;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 18px clamp(18px, 4vw, 48px);
  border-bottom: 1px solid transparent;
  transition: background 0.3s, border-color 0.3s;
}

.topbar.scrolled {
  background: rgba(7, 6, 27, 0.68);
  -webkit-backdrop-filter: blur(16px);
  backdrop-filter: blur(16px);
  border-bottom-color: var(--line);
}

.brand { display: flex; align-items: center; gap: 12px; font-weight: 600; letter-spacing: -0.01em; }
.brand small { display: block; color: var(--text-faint); font-weight: 400; font-size: 0.72rem; }

.topmeta {
  display: flex; align-items: center; gap: 16px;
  color: var(--text-dim);
  font-family: var(--mono);
  font-size: 0.76rem;
}

.live {
  display: inline-flex; align-items: center; gap: 9px;
  padding: 6px 13px;
  border-radius: 999px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.03);
}

.live i {
  width: 7px; height: 7px; border-radius: 50%;
  background: var(--long);
  box-shadow: 0 0 10px 2px rgba(61, 220, 151, 0.7);
  animation: pulse 2.4s ease-in-out infinite;
}

@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.35; } }

/* ---------- cinematic stage ---------- */

.stage { position: relative; z-index: 1; height: 100vh; height: 100svh; }
.js .stage { height: 360vh; }

.stage-pin { position: sticky; top: 0; height: 100vh; height: 100svh; overflow: hidden; }

.horizon {
  position: absolute; inset: 0;
  background:
    radial-gradient(ellipse 85% 42% at 50% 112%, rgba(255, 170, 80, 0.34), transparent 70%),
    radial-gradient(ellipse 70% 50% at 50% -6%, rgba(124, 92, 255, 0.24), transparent 72%);
}

#scene { position: absolute; inset: 0; width: 100%; height: 100%; display: block; }

.caps {
  position: absolute;
  left: clamp(22px, 6vw, 96px);
  bottom: clamp(60px, 13vh, 150px);
  width: min(700px, 88vw);
  height: 360px;
  pointer-events: none;
}

.cap { position: absolute; left: 0; right: 0; bottom: 0; opacity: 0; visibility: hidden; will-change: opacity, transform; }
.cap:first-child { opacity: 1; visibility: visible; }

.cap h1, .cap h2 {
  margin: 0;
  font-size: clamp(2.5rem, 6.3vw, 5.6rem);
  line-height: 1;
  font-weight: 700;
  letter-spacing: -0.045em;
  text-shadow: 0 6px 40px rgba(7, 6, 27, 0.9);
}

.cap p {
  margin: 20px 0 0;
  max-width: 50ch;
  color: var(--text-dim);
  font-size: clamp(0.95rem, 1.25vw, 1.1rem);
  line-height: 1.7;
  text-shadow: 0 2px 20px rgba(7, 6, 27, 0.95);
}

.hl { color: #FFD3A0; }

.w {
  display: inline-block;
  margin-right: 0.2em;
  opacity: 0;
  transform: translateY(0.55em);
  filter: blur(10px);
  animation: wordIn 1s cubic-bezier(0.2, 0.8, 0.2, 1) forwards;
  animation-delay: calc(0.25s + var(--i) * 0.09s);
}
@keyframes wordIn { to { opacity: 1; transform: none; filter: none; } }

.ticker { display: flex; align-items: baseline; gap: 18px; margin-top: 24px; }
.tk-num {
  font-family: var(--mono);
  font-size: clamp(2.4rem, 5vw, 4.1rem);
  font-weight: 600;
  letter-spacing: -0.03em;
  color: #fff;
  text-shadow: 0 0 40px rgba(169, 155, 255, 0.6);
  min-width: 3.4ch;
}
.tk-label { color: var(--text-dim); font-size: 0.95rem; max-width: 26ch; line-height: 1.35; }

.cta {
  display: inline-flex; align-items: center; gap: 10px;
  margin-top: 26px;
  padding: 13px 22px;
  border-radius: 999px;
  border: 1px solid rgba(169, 155, 255, 0.6);
  background: rgba(169, 155, 255, 0.14);
  color: #fff;
  font-size: 0.88rem;
  text-decoration: none;
  pointer-events: auto;
  transition: background 0.2s, box-shadow 0.2s, transform 0.2s;
}
.cta:hover { background: rgba(169, 155, 255, 0.26); box-shadow: 0 0 30px -4px var(--accent); transform: translateY(-1px); }

.rail {
  position: absolute;
  right: clamp(14px, 2.6vw, 36px);
  top: 50%;
  transform: translateY(-50%);
  display: grid; gap: 20px;
  z-index: 3;
}
.rail button {
  all: unset;
  cursor: pointer;
  display: flex; align-items: center; justify-content: flex-end; gap: 12px;
  color: var(--text-faint);
  font-size: 0.72rem;
  transition: color 0.25s;
}
.rail button i {
  width: 9px; height: 9px; border-radius: 50%;
  border: 1.5px solid currentColor;
  transition: all 0.3s;
}
.rail button:hover { color: var(--text); }
.rail button.on { color: #fff; }
.rail button.on i { background: var(--accent); border-color: var(--accent); box-shadow: 0 0 0 5px var(--accent-soft), 0 0 16px var(--accent); }
.rail button:focus-visible { outline: 2px solid var(--accent); outline-offset: 4px; border-radius: 6px; }

.scroll-hint {
  position: absolute; left: 50%; bottom: 20px;
  transform: translateX(-50%);
  display: flex; flex-direction: column; align-items: center; gap: 8px;
  color: var(--text-faint);
  font-size: 0.72rem;
  letter-spacing: 0.04em;
  pointer-events: none;
}
.scroll-hint i {
  width: 1px; height: 36px;
  background: linear-gradient(var(--accent), transparent);
  transform-origin: top;
  animation: drip 2s ease-in-out infinite;
}
@keyframes drip { 0% { transform: scaleY(0); } 55% { transform: scaleY(1); opacity: 1; } 100% { transform: scaleY(1); opacity: 0; } }

.tip {
  position: absolute; left: 0; top: 0;
  z-index: 4;
  pointer-events: none;
  opacity: 0;
  padding: 10px 14px;
  border-radius: 12px;
  background: rgba(12, 10, 34, 0.94);
  border: 1px solid var(--line-strong);
  box-shadow: 0 18px 40px -16px rgba(0, 0, 0, 0.85);
  font-size: 0.76rem;
  line-height: 1.55;
  color: var(--text-dim);
  white-space: nowrap;
  transition: opacity 0.12s;
}
.tip b { display: block; font-family: var(--mono); font-size: 0.9rem; color: #fff; }
.tip .tg { color: var(--long); }
.tip .ts { color: var(--short); }
.tip .tn { color: var(--caution); }

[id] { scroll-margin-top: 90px; }

.wrap { position: relative; z-index: 1; max-width: 1360px; margin: 0 auto; padding: 36px 32px 80px; }

.warn {
  background: var(--caution-soft);
  border: 1px solid rgba(255, 178, 87, 0.4);
  color: #FFD9A8;
  border-radius: 14px;
  padding: 13px 18px;
  margin-bottom: 22px;
  font-size: 0.84rem;
  line-height: 1.6;
}

/* scroll reveal */
.js .reveal {
  opacity: 0;
  transform: translateY(30px);
  transition: opacity 0.9s cubic-bezier(0.2, 0.8, 0.2, 1), transform 0.9s cubic-bezier(0.2, 0.8, 0.2, 1);
}
.js .reveal.in { opacity: 1; transform: none; }

/* ---------- briefing band ---------- */

.briefing {
  display: grid;
  grid-template-columns: minmax(0, 1.15fr) minmax(0, 1fr);
  gap: 34px 48px;
  padding: 38px 40px 34px;
}

.briefing h2 { margin: 0 0 12px; font-size: clamp(1.5rem, 2.6vw, 2.2rem); line-height: 1.1; letter-spacing: -0.03em; }
.hero-sub { margin: 0; color: var(--text-dim); font-size: 0.95rem; line-height: 1.75; max-width: 62ch; }
.hero-sub b { color: var(--text); font-weight: 500; }

.briefing .timeline { grid-column: 1 / -1; }

.timeline { list-style: none; margin: 0; padding: 0; display: grid; grid-template-columns: repeat(4, 1fr); }
.tl-step { position: relative; padding-top: 26px; }
.tl-step::before { content: ''; position: absolute; top: 5px; left: 16px; right: 0; height: 2px; background: var(--line-strong); }
.tl-step:last-child::before { display: none; }
.tl-step.done::before { background: var(--text-faint); }
.tl-step.active::before { background: linear-gradient(90deg, var(--accent), var(--line-strong)); }
.tl-dot { position: absolute; top: 0; left: 0; width: 12px; height: 12px; border-radius: 50%; border: 2px solid var(--text-faint); background: var(--bg); }
.tl-step.done .tl-dot { background: var(--text-faint); }
.tl-step.active .tl-dot { border-color: var(--accent); background: var(--accent); box-shadow: 0 0 0 5px var(--accent-soft), 0 0 18px 2px rgba(169, 155, 255, 0.7); }
.tl-label { color: var(--text-faint); font-size: 0.72rem; }
.tl-date { font-weight: 600; font-size: 0.92rem; margin-top: 2px; }
.tl-sub { font-family: var(--mono); color: var(--text-dim); font-size: 0.74rem; margin-top: 2px; }

.kpis { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; align-self: start; }
.kpi { padding: 16px 18px; border-radius: 16px; border: 1px solid var(--line); background: rgba(255, 255, 255, 0.025); }
.kpi .n { font-family: var(--mono); font-size: 1.7rem; font-weight: 600; letter-spacing: -0.02em; }
.kpi .l { color: var(--text-faint); font-size: 0.74rem; margin-top: 2px; }
.kpi.long .n { color: var(--long); }
.kpi.short .n { color: var(--short); }

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
.axis-day { fill: var(--text-faint); font-size: 9px; letter-spacing: 0.02em; }

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
.chip:hover { border-color: rgba(169, 155, 255, 0.6); color: var(--text); }
.chip.active { background: var(--accent-soft); border-color: rgba(169, 155, 255, 0.6); color: var(--text); }

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
tbody tr:hover { background: rgba(169, 155, 255, 0.04); }
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
  border: 1px solid rgba(169, 155, 255, 0.5);
  background: var(--accent-soft);
  color: var(--text);
  font: inherit;
  font-size: 0.78rem;
  transition: background 0.15s, box-shadow 0.15s;
}
.btn:hover { background: rgba(169, 155, 255, 0.22); box-shadow: 0 0 18px -4px var(--accent); }

/* ---------- footer ---------- */

.foot-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 18px; }
.foot-card { padding: 24px 26px; color: var(--text-dim); font-size: 0.82rem; line-height: 1.75; }
.foot-card h2 { margin: 0 0 8px; font-size: 0.92rem; font-weight: 600; color: var(--text); }
.foot-card ol { margin: 8px 0; padding-left: 20px; }
.foot-card li { margin-bottom: 4px; }



/* ---------- responsive ---------- */

@media (max-width: 980px) {
  .briefing { grid-template-columns: 1fr; padding: 28px 24px; }
  .rail button span { display: none; }
}

@media (max-width: 640px) {
  .wrap { padding: 28px 16px 56px; }
  .topbar { padding: 12px 16px; }
  .topmeta > span:last-child { display: none; }
  .brand small { display: none; }
  .caps { bottom: 90px; height: 330px; }
  .timeline { grid-template-columns: 1fr 1fr; row-gap: 22px; }
  .tl-step::before { display: none; }
  .cards { grid-template-columns: 1fr; }
  .table-head { padding: 16px; }
  .search input { width: 100%; }
  .search { width: 100%; }
  .tk-label { font-size: 0.82rem; }
}

@media (pointer: coarse) { .cursor-glow { display: none; } }

@media (prefers-reduced-motion: reduce) {
  html { scroll-behavior: auto; }
  .aurora i, .live i, .seg, .match-card, .scroll-hint i, .w { animation: none !important; }
  .w { opacity: 1; transform: none; filter: none; }
  .js .reveal { opacity: 1; transform: none; transition: none; }
}
"""


SCRIPT = """<script>
(function () {
  'use strict';

  var reduce = !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
  var coarse = !!(window.matchMedia && window.matchMedia('(pointer: coarse)').matches);

  function clamp(v, a, b) { return v < a ? a : v > b ? b : v; }
  function lerp(a, b, t) { return a + (b - a) * t; }
  function smooth(a, b, v) { var t = clamp((v - a) / (b - a), 0, 1); return t * t * (3 - 2 * t); }

  /* ---------- top bar ---------- */
  var topbar = document.getElementById('topbar');
  function onScroll() { if (topbar) topbar.classList.toggle('scrolled', window.scrollY > 40); }
  window.addEventListener('scroll', onScroll, { passive: true });
  onScroll();

  /* ---------- reveal on scroll ---------- */
  var reveals = document.querySelectorAll('.reveal');
  if ('IntersectionObserver' in window) {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); }
      });
    }, { threshold: 0.1 });
    reveals.forEach(function (el) { io.observe(el); });
  } else {
    reveals.forEach(function (el) { el.classList.add('in'); });
  }

  /* ---------- cursor glow ---------- */
  var glow = document.getElementById('cg');
  if (glow && !coarse && !reduce) {
    var gx = window.innerWidth / 2, gy = window.innerHeight / 2, tx = gx, ty = gy;
    window.addEventListener('mousemove', function (e) { tx = e.clientX; ty = e.clientY; glow.classList.add('on'); }, { passive: true });
    (function glowLoop() {
      gx += (tx - gx) * 0.12; gy += (ty - gy) * 0.12;
      glow.style.transform = 'translate3d(' + gx.toFixed(1) + 'px,' + gy.toFixed(1) + 'px,0)';
      requestAnimationFrame(glowLoop);
    })();
  }

  /* ---------- count-up numbers ---------- */
  if (!reduce) {
    document.querySelectorAll('[data-count]').forEach(function (el) {
      var target = parseFloat(el.getAttribute('data-count'));
      var dec = parseInt(el.getAttribute('data-dec') || '0', 10);
      var suffix = el.getAttribute('data-suffix') || '';
      var started = false;
      function run() {
        if (started) return; started = true;
        var start = null, dur = 1100;
        (function step(ts) {
          if (start === null) start = ts;
          var p = Math.min(1, (ts - start) / dur);
          var v = target * (1 - Math.pow(1 - p, 3));
          el.textContent = (dec ? v.toFixed(dec) : Math.round(v).toLocaleString('en-US')) + suffix;
          if (p < 1) requestAnimationFrame(step);
        })(performance.now());
      }
      if ('IntersectionObserver' in window) {
        var o = new IntersectionObserver(function (es) { if (es[0].isIntersecting) { run(); o.disconnect(); } }, { threshold: 0.4 });
        o.observe(el);
      } else { run(); }
    });
  }

  /* ---------- 3D tilt + glare on signal cards ---------- */
  if (!reduce && !coarse) {
    document.querySelectorAll('.match-card').forEach(function (card) {
      card.addEventListener('mousemove', function (e) {
        var r = card.getBoundingClientRect();
        var px = (e.clientX - r.left) / r.width, py = (e.clientY - r.top) / r.height;
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

  /* ---------- searchable, filterable, paged table ---------- */
  var table = document.getElementById('scan-table');
  if (table) {
    var rows = Array.prototype.slice.call(table.tBodies[0].rows);
    var PAGE = 50, state = { f: 'all', q: '', limit: PAGE };
    var more = document.getElementById('more'), label = document.getElementById('shown-label');
    var empty = document.getElementById('empty-row'), input = document.getElementById('q');
    var chips = Array.prototype.slice.call(document.querySelectorAll('.chip'));

    function apply() {
      var matched = 0, shown = 0;
      rows.forEach(function (r) {
        var ok = (state.f === 'all' || r.getAttribute('data-status') === state.f) &&
                 (!state.q || r.getAttribute('data-symbol').indexOf(state.q) !== -1);
        if (ok) { matched++; if (shown < state.limit) { r.hidden = false; shown++; } else { r.hidden = true; } }
        else { r.hidden = true; }
      });
      label.textContent = 'Showing ' + shown + ' of ' + matched;
      more.style.display = shown < matched ? '' : 'none';
      empty.style.display = matched === 0 ? 'block' : 'none';
    }
    chips.forEach(function (chip) {
      chip.addEventListener('click', function () {
        state.f = chip.getAttribute('data-f'); state.limit = PAGE;
        chips.forEach(function (c) { c.classList.toggle('active', c === chip); });
        apply();
      });
    });
    input.addEventListener('input', function () { state.q = input.value.trim().toLowerCase(); state.limit = PAGE; apply(); });
    more.addEventListener('click', function () { state.limit += PAGE; apply(); });
    apply();
  }

  /* =====================================================================
     THE OBSERVATORY: a scroll-driven 3D scene drawn on a 2D canvas.
     Every evaluated stock is a point. Three layouts are blended by scroll:
       1  universe  x = day-before volume ratio, y = previous-day reversal,
                    depth = how many conditions the stock passed
       2  funnel    one ring per condition; the camera flies through
       3  signals   the survivors, in front, everything else fades to dust
     ===================================================================== */
  (function initScene() {
    var dataEl = document.getElementById('scene-data');
    var canvas = document.getElementById('scene');
    var stage = document.getElementById('stage');
    if (!dataEl || !canvas || !stage || !canvas.getContext) return;

    var pin = stage.querySelector('.stage-pin');
    var tip = document.getElementById('tip');
    var D = JSON.parse(dataEl.textContent);
    var rows = D.rows, N = rows.length, counts = D.counts, nSig = 0;
    var ctx = canvas.getContext('2d');
    var caps = stage.querySelectorAll('.cap');
    var tkNum = document.getElementById('tk-num'), tkLabel = document.getElementById('tk-label');
    var railBtns = stage.querySelectorAll('.rail button');
    var hint = stage.querySelector('.scroll-hint');

    var STAGE_LABEL = [
      'stocks evaluated',
      'left after 15:20 and 15:25 differ',
      'left after 2+ of 3 earlier candles agree',
      'left after the day-before trend matches',
      'left after the day-before volume check'
    ];
    var FAIL_LABEL = [
      '15:20 and 15:25 did not differ',
      'fewer than 2 earlier candles agreed',
      'day-before trend did not match',
      'day-before volume was not higher'
    ];

    function hash(str) {
      var h = 2166136261;
      for (var i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 16777619); }
      return h >>> 0;
    }
    function rng(seed) {
      var a = seed >>> 0;
      return function () {
        a = (a + 0x6D2B79F5) >>> 0;
        var t = a;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
      };
    }
    function sh(v) { return (v < 0 ? -1 : 1) * Math.pow(Math.abs(v), 0.72); }

    /* ---- build the three layouts once ---- */
    var L1 = new Float32Array(N * 3), L2 = new Float32Array(N * 3), L3 = new Float32Array(N * 3);
    var S0 = new Float32Array(N * 3), DLY = new Float32Array(N), SZ = new Float32Array(N);
    var SC = new Uint8Array(N), STG = new Uint8Array(N), DIR = new Int8Array(N);
    var R_ST = [2.0, 1.62, 1.24, 0.86, 0.48], Z_ST = [2.2, 0.85, -0.5, -1.85, -3.2];
    var sigIdx = [];

    for (var i = 0; i < N; i++) {
      var r = rows[i], rnd = rng(hash(r[0]));
      var stg = r[1], sc = r[2], o = i * 3;
      STG[i] = stg; SC[i] = sc; DIR[i] = r[5];
      if (stg === 4) sigIdx.push(i);

      L1[o] = sh(r[3]) * 1.55 + (rnd() - 0.5) * 0.07;
      L1[o + 1] = sh(r[4]) * 0.95 + (rnd() - 0.5) * 0.07;
      L1[o + 2] = (sc - 2) * 0.55 + (rnd() - 0.5) * 0.36;

      var ang = rnd() * 6.2832;
      var rr = R_ST[stg] * (rnd() < 0.25 ? Math.sqrt(rnd()) : 0.82 + 0.18 * rnd());
      L2[o] = Math.cos(ang) * rr;
      L2[o + 1] = Math.sin(ang) * rr;
      L2[o + 2] = Z_ST[stg] + (rnd() - 0.5) * 0.4;

      S0[o] = (rnd() - 0.5) * 13;
      S0[o + 1] = (rnd() - 0.5) * 8;
      S0[o + 2] = (rnd() - 0.5) * 13;
      DLY[i] = rnd();
      SZ[i] = 0.8 + rnd() * 0.9;
    }

    nSig = sigIdx.length;
    sigIdx.forEach(function (idx, k) {
      var o = idx * 3, a = (k / nSig) * 6.2832 - 1.5708;
      L2[o] = Math.cos(a) * 0.48; L2[o + 1] = Math.sin(a) * 0.48; L2[o + 2] = -3.2;
      var perRow = Math.min(nSig, 8), row = Math.floor(k / perRow), col = k % perRow;
      var inRow = Math.min(perRow, nSig - row * perRow);
      var spacing = Math.min(0.95, 3.4 / Math.max(inRow, 1));
      L3[o] = (col - (inRow - 1) / 2) * spacing;
      L3[o + 1] = 0.12 - row * 0.72 + Math.sin(col * 0.9) * 0.06;
      L3[o + 2] = -3.4;
    });
    for (var j = 0; j < N; j++) {
      if (STG[j] === 4) continue;
      var q = j * 3;
      L3[q] = L2[q] * 2.3; L3[q + 1] = L2[q + 1] * 2.3; L3[q + 2] = L2[q + 2];
    }

    /* ---- glow sprites ---- */
    function sprite(rgb) {
      var c = document.createElement('canvas'); c.width = c.height = 64;
      var g = c.getContext('2d'), gr = g.createRadialGradient(32, 32, 0, 32, 32, 32);
      gr.addColorStop(0, 'rgba(255,255,255,1)');
      gr.addColorStop(0.16, 'rgba(' + rgb + ',0.95)');
      gr.addColorStop(0.5, 'rgba(' + rgb + ',0.26)');
      gr.addColorStop(1, 'rgba(' + rgb + ',0)');
      g.fillStyle = gr; g.fillRect(0, 0, 64, 64);
      return c;
    }
    var SPR = [sprite('118,124,214'), sprite('140,146,236'), sprite('178,162,246'),
               sprite('255,178,87'), sprite('61,220,151'), sprite('255,107,127')];
    var KIND_ALPHA = [0.5, 0.62, 0.74, 0.95, 1, 1];
    var RGB = ['118,124,214', '140,146,236', '178,162,246', '255,178,87', '61,220,151', '255,107,127'];
    function kindOf(i) { return SC[i] === 4 ? (DIR[i] === -1 ? 5 : 4) : SC[i]; }

    var stars = [], sr = rng(99);
    for (var s = 0; s < 150; s++) stars.push([sr(), sr(), 0.4 + sr() * 1.1, sr() * 6.28, 0.35 + sr() * 0.65]);

    /* ---- sizing ---- */
    var W = 0, H = 0, DPR = 1, F = 1;
    function resize() {
      var rc = pin.getBoundingClientRect();
      DPR = Math.min(window.devicePixelRatio || 1, 2);
      W = rc.width; H = rc.height;
      canvas.width = Math.round(W * DPR); canvas.height = Math.round(H * DPR);
      ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
      F = Math.min(W, H * 1.2) * 0.84;
    }
    resize();
    var rt; window.addEventListener('resize', function () { clearTimeout(rt); rt = setTimeout(resize, 120); });

    /* ---- input ---- */
    var mouse = { x: 0.5, y: 0.5, px: -999, py: -999, inside: false }, look = { x: 0, y: 0 };
    if (!coarse) {
      pin.addEventListener('mousemove', function (e) {
        var rc = pin.getBoundingClientRect();
        mouse.px = e.clientX - rc.left; mouse.py = e.clientY - rc.top;
        mouse.x = mouse.px / rc.width; mouse.y = mouse.py / rc.height; mouse.inside = true;
      });
      pin.addEventListener('mouseleave', function () { mouse.inside = false; });
    }

    function stageTop() { return window.scrollY + stage.getBoundingClientRect().top; }
    function scrollToP(pp) {
      var total = stage.offsetHeight - H;
      window.scrollTo({ top: stageTop() + total * pp, behavior: reduce ? 'auto' : 'smooth' });
    }
    railBtns.forEach(function (b) { b.addEventListener('click', function () { scrollToP(parseFloat(b.getAttribute('data-p'))); }); });

    /* ---- projection ---- */
    var PX = 0, PY = 0, PZ = 1;
    var ca = 1, sa = 0, camY = 0, camZ = 4.2, cpi = 1, spi = 0, cly = 1, sly = 0;
    function proj(x, y, z) {
      var px = x * ca + z * sa, pz = -x * sa + z * ca;
      var cx = px, cy = y - camY, cz = camZ - pz;
      var y2 = cy * cpi + cz * spi, z2 = -cy * spi + cz * cpi;
      var x3 = cx * cly + z2 * sly, z3 = -cx * sly + z2 * cly;
      PZ = z3;
      PX = W / 2 + F * x3 / z3;
      PY = H / 2 - F * y2 / z3;
    }
    function line3(x1, y1, z1, x2, y2, z2) {
      proj(x1, y1, z1); var ax = PX, ay = PY, az = PZ;
      proj(x2, y2, z2);
      if (az < 0.4 || PZ < 0.4) return;
      ctx.moveTo(ax, ay); ctx.lineTo(PX, PY);
    }

    var SX = new Float32Array(N), SY = new Float32Array(N), SA = new Float32Array(N), SR = new Float32Array(N);
    var p = 0, pT = 0, last = performance.now(), t0 = last, tkVal = counts[0], hoverIdx = -1, lastStageShown = -1;

    function setCap(el, enter, exit) {
      var a = enter * exit;
      el.style.opacity = a.toFixed(3);
      el.style.transform = 'translateY(' + (((1 - enter) * 28) - ((1 - exit) * 28)).toFixed(1) + 'px)';
      el.style.visibility = a < 0.02 ? 'hidden' : 'visible';
    }

    function frame(now) {
      requestAnimationFrame(frame);
      if (document.hidden) return;
      var rect = stage.getBoundingClientRect();
      if (rect.bottom < -60 || rect.top > window.innerHeight + 60) return;

      var dt = Math.min(0.05, (now - last) / 1000); last = now;
      var t = (now - t0) / 1000;
      var total = rect.height - H;
      pT = total > 0 ? clamp(-rect.top / total, 0, 1) : 0;
      p = reduce ? pT : p + (pT - p) * (1 - Math.exp(-dt * 5.5));

      var w1 = smooth(0.20, 0.36, p), w2 = smooth(0.70, 0.84, p);
      camZ = lerp(4.2, -0.8, smooth(0.34, 0.70, p));
      camY = 0.45 * (1 - w1);

      if (!coarse && !reduce) { look.x += ((mouse.x - 0.5) - look.x) * 0.05; look.y += ((mouse.y - 0.5) - look.y) * 0.05; }
      var yo = reduce ? 0.3 : (0.3 + Math.sin(t * 0.07) * 0.3 + look.x * 0.9) * (1 - w1);
      ca = Math.cos(yo); sa = Math.sin(yo);
      var pitch = 0.107 * (1 - w1) + look.y * 0.12, yaw = look.x * 0.16;
      cpi = Math.cos(pitch); spi = Math.sin(pitch); cly = Math.cos(yaw); sly = Math.sin(yaw);

      ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
      ctx.clearRect(0, 0, W, H);

      /* far stars */
      ctx.globalCompositeOperation = 'source-over';
      for (var s = 0; s < stars.length; s++) {
        var st = stars[s], tw = reduce ? 1 : 0.6 + 0.4 * Math.sin(t * st[4] * 1.6 + st[3]);
        ctx.globalAlpha = 0.5 * tw * st[4];
        ctx.fillStyle = '#DCD6FF';
        var sx = st[0] * W - look.x * 26 * st[2], sy = st[1] * H - look.y * 16 * st[2];
        ctx.fillRect(sx, sy, st[2], st[2]);
      }

      /* floor grid, target zone and axis hints: only in the universe view */
      var uv = 1 - w1;
      if (uv > 0.02) {
        ctx.globalAlpha = 0.20 * uv; ctx.strokeStyle = 'rgb(169,155,255)'; ctx.lineWidth = 1;
        ctx.beginPath();
        for (var g = -6; g <= 6; g++) {
          line3(g * 0.32, -1.2, -1.9, g * 0.32, -1.2, 1.9);
          line3(-1.9, -1.2, g * 0.32, 1.9, -1.2, g * 0.32);
        }
        ctx.stroke();

        proj(0, 0, 1.15); var q0x = PX, q0y = PY, q0z = PZ;
        proj(1.6, 0, 1.15); var q1x = PX, q1y = PY;
        proj(1.6, 1, 1.15); var q2x = PX, q2y = PY;
        proj(0, 1, 1.15); var q3x = PX, q3y = PY;
        if (q0z > 0.4) {
          ctx.globalAlpha = 0.07 * uv; ctx.fillStyle = 'rgb(255,178,87)';
          ctx.beginPath(); ctx.moveTo(q0x, q0y); ctx.lineTo(q1x, q1y); ctx.lineTo(q2x, q2y); ctx.lineTo(q3x, q3y); ctx.closePath(); ctx.fill();
          ctx.globalAlpha = 0.5 * uv; ctx.strokeStyle = 'rgb(255,178,87)'; ctx.setLineDash([5, 5]); ctx.stroke(); ctx.setLineDash([]);
          ctx.font = '500 11px "IBM Plex Mono", monospace'; ctx.fillStyle = 'rgb(255,200,140)';
          ctx.globalAlpha = 0.75 * uv; ctx.fillText('target zone', q3x + 8, q3y + 16);
        }
        ctx.font = '500 11px "IBM Plex Mono", monospace'; ctx.fillStyle = 'rgb(190,182,255)'; ctx.globalAlpha = 0.5 * uv;
        proj(0.1, -1.2, 1.95); ctx.fillText('day-before volume ratio \\u2192', PX, PY + 14);
        proj(-1.75, 0.15, 1.15); ctx.fillText('previous-day reversal \\u2191', PX - 10, PY);
      }

      /* ---- points ---- */
      ctx.globalCompositeOperation = 'lighter';
      var intro = !reduce, ign = reduce ? 1 : smooth(2.2, 3.2, t);
      var wallClock = t;

      for (var i = 0; i < N; i++) {
        var o = i * 3;
        var ip = intro ? clamp((wallClock - 0.1 - DLY[i] * 1.2) / 1.4, 0, 1) : 1;
        if (ip <= 0) { SA[i] = 0; continue; }
        ip = 1 - Math.pow(1 - ip, 3);

        var x = lerp(lerp(L1[o], L2[o], w1), L3[o], w2);
        var y = lerp(lerp(L1[o + 1], L2[o + 1], w1), L3[o + 1], w2);
        var z = lerp(lerp(L1[o + 2], L2[o + 2], w1), L3[o + 2], w2);
        if (ip < 1) { x = lerp(S0[o], x, ip); y = lerp(S0[o + 1], y, ip); z = lerp(S0[o + 2], z, ip); }

        proj(x, y, z);
        if (PZ < 0.35) { SA[i] = 0; continue; }

        var isSig = STG[i] === 4, kind = kindOf(i);
        var sc = F / PZ;
        var rad = (isSig ? 0.05 : 0.0125 * SZ[i] + (SC[i] === 3 ? 0.007 : 0)) * sc;
        rad = clamp(rad, 0.7, isSig ? 46 : 15);
        var df = clamp(1.3 - (PZ - 2.5) / 9, 0.12, 1);
        var dust = isSig ? 1 : 1 - 0.9 * w2;
        var a = KIND_ALPHA[kind] * df * ip * dust;

        SX[i] = PX; SY[i] = PY; SA[i] = a; SR[i] = rad;
        ctx.globalAlpha = clamp(a, 0, 1);
        ctx.drawImage(SPR[kind], PX - rad * 2.2, PY - rad * 2.2, rad * 4.4, rad * 4.4);
      }

      /* stems from the best points down to the floor (universe view) */
      if (uv > 0.02) {
        ctx.globalCompositeOperation = 'lighter'; ctx.lineWidth = 1;
        for (var k = 0; k < N; k++) {
          if (SC[k] < 3 || SA[k] < 0.05) continue;
          var ko = k * 3; var kx = L1[ko], ky = L1[ko + 1], kz = L1[ko + 2];
          proj(kx, -1.2, kz); var fx = PX, fy = PY, fz = PZ;
          if (fz < 0.4) continue;
          ctx.globalAlpha = (SC[k] === 4 ? 0.28 : 0.12) * uv * SA[k];
          ctx.strokeStyle = 'rgb(' + RGB[kindOf(k)] + ')';
          ctx.beginPath(); ctx.moveTo(SX[k], SY[k]); ctx.lineTo(fx, fy); ctx.stroke();
        }
      }

      /* pulse rings + labels for signals */
      ctx.lineWidth = 1.2;
      var labelA = clamp((1 - w1) + w2, 0, 1);
      var showAll = nSig <= 12;
      for (var m = 0; m < sigIdx.length; m++) {
        var si = sigIdx[m];
        if (SA[si] < 0.05) continue;
        var kd = kindOf(si), col = 'rgb(' + RGB[kd] + ')';
        var ph = reduce ? 0.4 : ((t * 0.55 + m * 0.21) % 1);
        ctx.globalCompositeOperation = 'lighter';
        ctx.globalAlpha = (1 - ph) * 0.55 * ign * SA[si];
        ctx.strokeStyle = col;
        ctx.beginPath(); ctx.arc(SX[si], SY[si], SR[si] * (1.2 + ph * 3.0), 0, 6.2832); ctx.stroke();

        if ((showAll || hoverIdx === si) && labelA > 0.05) {
          ctx.globalCompositeOperation = 'source-over';
          ctx.globalAlpha = labelA * clamp(ign, 0, 1);
          ctx.font = '600 12px "IBM Plex Mono", monospace';
          ctx.fillStyle = col;
          ctx.shadowColor = 'rgba(7,6,27,0.95)'; ctx.shadowBlur = 8;
          ctx.fillText(rows[si][0], SX[si] + SR[si] * 1.5 + 6, SY[si] - SR[si] * 1.1 - 4);
          ctx.shadowBlur = 0;
        }
      }
      ctx.globalCompositeOperation = 'source-over';
      ctx.globalAlpha = 1;

      /* ---- hover ---- */
      var best = -1, bd = 1e9;
      if (mouse.inside && !coarse) {
        for (var h = 0; h < N; h++) {
          if (SA[h] < 0.2) continue;
          var dx = SX[h] - mouse.px, dy = SY[h] - mouse.py;
          var lim = Math.max(11, SR[h] * 2.0), dd = (dx * dx + dy * dy) * (STG[h] === 4 ? 0.4 : 1);
          if (dd < lim * lim && dd < bd) { bd = dd; best = h; }
        }
      }
      if (best !== hoverIdx) {
        hoverIdx = best;
        if (best >= 0) {
          var rw = rows[best], line;
          if (rw[1] === 4) {
            line = '<span class="' + (rw[5] === -1 ? 'ts' : 'tg') + '">Signal, ' + (rw[5] === -1 ? 'short' : 'long') + '</span><br>Click to open its card';
          } else {
            line = 'Passed ' + rw[2] + ' of 4 conditions<br><span class="tn">Stopped at: ' + FAIL_LABEL[rw[1]] + '</span>';
          }
          tip.innerHTML = '<b></b>' + line;
          tip.firstChild.textContent = rw[0];
          tip.style.opacity = 1;
        } else { tip.style.opacity = 0; }
        canvas.style.cursor = (best >= 0 && rows[best][1] === 4) ? 'pointer' : 'default';
      }
      if (best >= 0) {
        var tx = clamp(SX[best] + 18, 8, W - tip.offsetWidth - 8), ty = clamp(SY[best] + 18, 70, H - tip.offsetHeight - 8);
        tip.style.transform = 'translate(' + tx.toFixed(0) + 'px,' + ty.toFixed(0) + 'px)';
      }

      /* ---- captions, ticker, rail ---- */
      setCap(caps[0], 1, 1 - smooth(0.12, 0.22, p));
      setCap(caps[1], smooth(0.27, 0.35, p), 1 - smooth(0.66, 0.74, p));
      setCap(caps[2], smooth(0.76, 0.86, p), 1);

      var u = clamp((p - 0.34) / 0.36, 0, 1);
      var shown = Math.min(4, Math.floor(u * 5 - 1e-6));
      if (shown < 0) shown = 0;
      tkVal += (counts[shown] - tkVal) * (1 - Math.exp(-dt * 9));
      tkNum.textContent = Math.round(tkVal).toLocaleString('en-US');
      if (shown !== lastStageShown) { tkLabel.textContent = STAGE_LABEL[shown]; lastStageShown = shown; }

      var act = p < 0.27 ? 0 : (p < 0.75 ? 1 : 2);
      railBtns.forEach(function (b, bi) { b.classList.toggle('on', bi === act); });
      if (hint) hint.style.opacity = (1 - smooth(0, 0.04, p)).toFixed(2);
    }
    requestAnimationFrame(frame);

    canvas.addEventListener('click', function () {
      if (hoverIdx >= 0 && rows[hoverIdx][1] === 4) {
        var card = document.getElementById('sig-' + rows[hoverIdx][0]);
        if (card) card.scrollIntoView({ behavior: reduce ? 'auto' : 'smooth', block: 'center' });
      }
    });
  })();
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
        chart = '<span class="dim">&ndash;</span>'

    def chk(value, detail):
        if value is None:
            return '<span class="chk na">&ndash;</span>'
        glyph = "&#10003;" if value else "&#10005;"
        cls = "yes" if value else "no"
        extra = f'<span class="sub">{detail}</span>' if detail else ""
        return f'<span class="chk {cls}">{glyph}</span>{extra}'

    if details:
        p20, p25 = details["dP1520"], details["dP1525"]
        b20 = details["dB1520"]
        detail_1a = f'{trend_name(p20)} vs {trend_name(p25)}'
        detail_1b = f'{r.get("cond1b_matches", 0)} of 3 match'
        detail_2a = f'B {trend_name(b20)} / P {trend_name(p20)}'
        detail_2b = f'{details["B1520_vol"]:,.0f} vs {details["B1525_vol"]:,.0f}'
    else:
        detail_1a = detail_1b = detail_2a = detail_2b = ""

    direction = r.get("direction")
    if direction:
        dir_html = (
            f'<span class="dir {"long" if direction == "LONG" else "short"}">'
            f'{arrow(1 if direction == "LONG" else -1)} {esc(direction)}</span>'
        )
    else:
        dir_html = '<span class="dim">&ndash;</span>'

    verify_html = ""
    if r.get("needs_verify"):
        verify_html = '<span class="verify-tag">Verify: B 15:25 missing</span>'

    data_label = {
        "OK": "OK", "FALLBACK": "Fallback (1mo)", "NO_DATA": "No data", "ERROR": "Error",
    }.get(r.get("data_status"), r.get("data_status"))

    data_bits = [cell(data_label)]

    if r.get("missing"):
        data_bits.append(
            '<span class="sub">missing: '
            + ", ".join(esc(m) for m in r["missing"]) + "</span>"
        )
    if r.get("error"):
        data_bits.append(f'<span class="sub">{esc(r["error"][:120])}</span>')

    prev = r.get("previous_day")

    return f"""
<tr data-status="{key}" data-symbol="{esc(symbol.lower())}">
<td class="symbol">{esc(symbol)}</td>
<td class="mono dim">{esc(fmt_day(prev, weekday=False)) if prev else "&ndash;"}</td>
<td>{dir_html}</td>
<td>{chart}</td>
<td>{chk(r.get("cond1a"), detail_1a)}</td>
<td>{chk(r.get("cond1b"), detail_1b)}</td>
<td>{chk(r.get("cond2a"), detail_2a)}</td>
<td>{chk(r.get("cond2b"), detail_2b)}</td>
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

    first_open = (ohlc.get("P1505") or {}).get("open")
    last_close = (ohlc.get("P1525") or {}).get("close")

    price_html = ""
    if first_open and last_close:
        chg = (last_close / first_open - 1) * 100
        price_html = (
            '<div class="mc-price">'
            f'<span class="mc-last">&#8377;{last_close:,.2f}</span>'
            f'<span class="mc-chg {"up" if chg >= 0 else "down"}">{chg:+.2f}%</span>'
            '<span class="mc-note">previous day, 15:05 to close</span></div>'
        )

    volumes = {k: d.get(f"{k}_vol") for k in SLOT_KEYS}
    chart = build_candlestick_svg(ohlc, size="large", volumes=volumes)

    v20 = d.get("B1520_vol", 0) or 0
    v25 = d.get("B1525_vol", 0) or 0

    checks = [
        ("Previous day: 15:20 and 15:25 trends differ",
         f'{trend_name(d.get("dP1520"))} vs {trend_name(d.get("dP1525"))}'),
        ("Previous day: 2+ of 15:05, 15:10, 15:15 match 15:20",
         f'{r.get("cond1b_matches", 0)} of 3'),
        ("Day before: 15:20 trend matches previous day's",
         f'{trend_name(d.get("dB1520"))} / {trend_name(d.get("dP1520"))}'),
        ("Day before: 15:20 volume above 15:25",
         f"{v20:,.0f} vs {v25:,.0f}"),
    ]
    checks_html = "".join(
        f'<li><span class="ck">&#10003;</span><span>{esc(t)}</span>'
        f'<span class="ck-val">{esc(v)}</span></li>'
        for t, v in checks
    )

    verify = ""
    if r.get("needs_verify"):
        verify = (
            '<div class="mc-verify">Verify on TradingView: the day-before 15:25 candle '
            'has no data here, so its volume was read as zero.</div>'
        )

    return f"""
<article class="match-card {cls}" id="sig-{esc(r["symbol"])}">
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
    <div><span class="k">Exit</span><span class="v">{esc(EXIT_TIME_LABEL)}</span></div>
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
    n = len(matches)

    def most_common(field):
        c = Counter(r[field] for r in results if r.get(field)).most_common(1)
        return c[0][0] if c else None

    previous_day = most_common("previous_day")
    day_before = most_common("day_before")
    entry_day = most_common("entry_day") or pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")

    scan_time = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%d %b %Y, %H:%M IST")

    warning = session_warning(results)
    warning_html = f'<div class="warn">{esc(warning)}</div>' if warning else ""

    # ---- data for the 3D scene ----

    scene_rows = []
    for r in results:
        sc = r.get("scene")
        if r.get("status") in ("PASS", "FAIL") and sc:
            d = 1 if r.get("direction") == "LONG" else -1 if r.get("direction") == "SHORT" else 0
            scene_rows.append([str(r["symbol"]), sc["stage"], sc["score"], sc["x"], sc["y"], d])

    stage_counts = [sum(1 for row in scene_rows if row[1] >= k) for k in range(5)]
    ev = stage_counts[0]

    scene_json = json.dumps(
        {"rows": scene_rows, "counts": stage_counts, "signals": n},
        separators=(",", ":"),
    ).replace("</", "<\\/")

    # ---- stage captions ----

    def words(parts, start=0, hl=False):
        return "".join(
            f'<span class="w{" hl" if hl else ""}" style="--i:{start + i}">{esc(w)}</span>'
            for i, w in enumerate(parts)
        )

    cap0 = f"""
    <div class="cap">
      <h1>{words([f"{ev:,}", "stocks."])}<br>{words(["One", "closing", "bell."], start=2, hl=True)}</h1>
      <p>Every point is a stock, placed by what happened in the last 25 minutes of two
      sessions. The higher it sits in the glow, the more of the four conditions it passed.</p>
    </div>"""

    cap1 = """
    <div class="cap">
      <h2>Four conditions.<br><span class="hl">Most fall away.</span></h2>
      <p>Keep scrolling to fly through the funnel. Each ring is one condition, and every stock
      stops at the first one it fails.</p>
      <div class="ticker"><span class="tk-num" id="tk-num">0</span><span class="tk-label" id="tk-label">stocks evaluated</span></div>
    </div>"""

    if n:
        cap2 = f"""
    <div class="cap">
      <h2><span class="hl">{n}</span> remain.</h2>
      <p>{longs} long, {shorts} short. Entry at the {esc(fmt_day(entry_day))} 09:15 open,
      exit at {esc(EXIT_TIME_LABEL)}.</p>
      <a class="cta" href="#signals">See the signals</a>
    </div>"""
    else:
        cap2 = f"""
    <div class="cap">
      <h2>Nothing <span class="hl">survived.</span></h2>
      <p>No stock passed all four conditions on the {esc(fmt_day(previous_day))} and
      {esc(fmt_day(day_before))} sessions. That is a normal outcome for a strategy this selective.</p>
      <a class="cta" href="#scan">See the full scan</a>
    </div>"""

    # ---- briefing band ----

    if n:
        brief_title = f'{n} signal{"s" if n != 1 else ""} for {esc(fmt_day(entry_day))}'
        split = f' {longs} long, {shorts} short.'
    else:
        brief_title = f'No signals for {esc(fmt_day(entry_day))}'
        split = ""

    hero_sub = (
        f'Scanned <b>{total:,}</b> symbols from {esc(universe_source)} against the '
        f'<b>{esc(fmt_day(previous_day))}</b> and <b>{esc(fmt_day(day_before))}</b> sessions.{split} '
        f'Trades enter at the <b>{esc(fmt_day(entry_day))}</b> 09:15 open and exit at '
        f'{esc(EXIT_TIME_LABEL)}.'
    )

    timeline = f"""
<ol class="timeline">
  <li class="tl-step done"><span class="tl-dot"></span>
    <div class="tl-label">Day before previous</div><div class="tl-date">{esc(fmt_day(day_before))}</div>
    <div class="tl-sub">trend and volume</div></li>
  <li class="tl-step done"><span class="tl-dot"></span>
    <div class="tl-label">Previous day</div><div class="tl-date">{esc(fmt_day(previous_day))}</div>
    <div class="tl-sub">reversal at the close</div></li>
  <li class="tl-step active"><span class="tl-dot"></span>
    <div class="tl-label">Entry</div><div class="tl-date">{esc(fmt_day(entry_day))}</div>
    <div class="tl-sub">09:15 open</div></li>
  <li class="tl-step"><span class="tl-dot"></span>
    <div class="tl-label">Exit</div><div class="tl-date">{esc(fmt_day(entry_day))}</div>
    <div class="tl-sub">{esc(EXIT_TIME_LABEL)}</div></li>
</ol>"""

    kpis = f"""
<div class="kpis">
  <div class="kpi"><div class="n" data-count="{total}">{total:,}</div><div class="l">Scanned</div></div>
  <div class="kpi"><div class="n" data-count="{elapsed:.1f}" data-dec="1" data-suffix="s">{elapsed:.1f}s</div><div class="l">Scan time</div></div>
  <div class="kpi long"><div class="n" data-count="{longs}">{longs}</div><div class="l">Long</div></div>
  <div class="kpi short"><div class="n" data-count="{shorts}">{shorts}</div><div class="l">Short</div></div>
</div>"""

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
  {total:,} symbols were checked against the {esc(fmt_day(previous_day))} and
  {esc(fmt_day(day_before))} sessions and none passed all four conditions.
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
        '<circle cx="16" cy="16" r="13" fill="none" stroke="#A99BFF" stroke-opacity=".6" stroke-width="1.5"/>'
        '<circle cx="16" cy="16" r="7" fill="none" stroke="#FFB257" stroke-opacity=".5" stroke-width="1.5"/>'
        '<path d="M16 16 L26.5 8.5" stroke="#A99BFF" stroke-width="2.2" stroke-linecap="round"/>'
        '<circle cx="16" cy="16" r="2.4" fill="#FFB257"/></svg>'
    )

    aria = f"Three-dimensional overview of {ev:,} stocks, of which {n} are signals"

    document = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#07061B">
<title>NSE Momentum Scanner</title>
<script>document.documentElement.classList.add('js')</script>
<style>{CSS}</style>
</head>
<body>
<div class="aurora"><i class="a"></i><i class="b"></i><i class="c"></i></div>
<div class="grid-bg"></div>
<div class="cursor-glow" id="cg"></div>

<header class="topbar" id="topbar">
  <div class="brand">{logo}<div>NSE Momentum Scanner<small>5-minute closing-window strategy</small></div></div>
  <div class="topmeta"><span class="live"><i></i>Scan complete</span><span>{esc(scan_time)}</span></div>
</header>

<section class="stage" id="stage" aria-label="Scan overview">
  <div class="stage-pin">
    <div class="horizon"></div>
    <canvas id="scene" role="img" aria-label="{esc(aria)}"></canvas>
    <div class="tip" id="tip"></div>
    <div class="caps">{cap0}{cap1}{cap2}
    </div>
    <nav class="rail" aria-label="Overview sections">
      <button type="button" data-p="0.05"><span>Universe</span><i></i></button>
      <button type="button" data-p="0.52"><span>Funnel</span><i></i></button>
      <button type="button" data-p="0.93"><span>Signals</span><i></i></button>
    </nav>
    <div class="scroll-hint"><span>Scroll</span><i></i></div>
  </div>
</section>
<script type="application/json" id="scene-data">{scene_json}</script>

<main class="wrap">

{warning_html}

<section class="glass briefing reveal">
  <div><h2>{brief_title}</h2><p class="hero-sub">{hero_sub}</p></div>
  {kpis}
  {timeline}
</section>

<section class="glass panel reveal">
  <div class="sec-head"><h2 class="sec-title">Scan breakdown</h2><span class="sec-note">{total:,} symbols</span></div>
  <div class="dist-bar">{segs}</div>
  <div class="legend">{legend}</div>
</section>

<section id="signals" class="reveal">
  <div class="sec-head"><h2 class="sec-title">Signals</h2><span class="sec-note">{n} matched</span></div>
  {signals_html}
</section>

<section class="glass table-panel reveal" id="scan">
  <div class="table-head">
    <div class="search">{search_icon}<input id="q" type="search" placeholder="Search symbol" autocomplete="off" aria-label="Search symbol"></div>
    <div class="filters">{"".join(chips)}</div>
  </div>
  <div class="table-scroll">
  <table id="scan-table">
    <thead><tr>
      <th>Symbol</th><th>Previous day</th><th>Direction</th><th>Candles</th>
      <th>P: 15:20 vs 15:25</th><th>P: 2+ of 3 match</th>
      <th>B: 15:20 = P 15:20</th><th>B: vol 15:20 &gt; 15:25</th>
      <th>Result</th><th>Data</th>
    </tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
  <div class="empty-row" id="empty-row">No symbols match this search.</div>
  </div>
  <div class="table-foot"><span id="shown-label"></span><button class="btn" id="more" type="button">Show 50 more</button></div>
</section>

<div class="foot-grid reveal">
  <div class="glass foot-card">
    <h2>How the signal works</h2>
    All candles are native 5-minute candles. P is the previous day (the latest completed
    session) and B is the day before it. 15:25 is the last candle of a session and 15:20
    the second to last.
    <ol>
      <li>On P, the 15:20 candle's trend differs from the 15:25 candle's, and at least two
      of the 15:05, 15:10 and 15:15 candles share the 15:20 candle's trend.</li>
      <li>On B, the 15:20 candle's trend matches P's 15:20 trend, and B's 15:20 volume is
      higher than B's 15:25 volume.</li>
    </ol>
    Direction follows the 15:20 trend: up is long, down is short. Entry is the 09:15 open
    on entry day; exit is {esc(EXIT_TIME_LABEL)} the same day.
  </div>
  <div class="glass foot-card">
    <h2>Reading the 3D overview</h2>
    Left to right is the day-before volume ratio (15:20 against 15:25) and bottom to top is
    how hard the previous day's last candle reversed against the 15:20 trend. The top-right
    quadrant is where both of those conditions hold. Depth shows how many of the four
    conditions a stock passed, so signals sit at the front. Stocks with no usable data are
    left out of the picture and counted in the scan breakdown.
  </div>
  <div class="glass foot-card">
    <h2>Data notes</h2>
    Candles come straight from Yahoo's 5-minute feed with no aggregation. A candle with no
    trades is absent from Yahoo's data and is read as having no trend, so it can never
    satisfy a trend condition. The one case that could create a false signal is a missing
    day-before 15:25 candle, because its volume then reads as zero; those signals carry a
    "Verify" tag, so check them on TradingView. Incomplete means fewer than two completed
    sessions came back; stale means the two sessions differ from most symbols.
  </div>
</div>

</main>
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
    print(f"Previous day: {reference_date}")
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
