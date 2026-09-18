# =============================================================================
# NSE DAILY MOMENTUM SCANNER â€” FIXED / FULL VERSION
# =============================================================================
#
# This is the MAIN LIVE SCANNER.
# It does NOT use Parquet files.
#
# DATA:
#   Yahoo Finance 1-minute data through yfinance 1.7.0
#
# UNIVERSE:
#   1) Nifty 500 official constituent CSV
#   2) NSE official equity list fallback
#   3) Built-in Nifty 50 + Nifty Next 150 fallback
#
# EXACT STRATEGY:
#
# 1) 3-MIN:
#       15:24 and 15:27 can be SAME or OPPOSITE.
#       15:24 volume > 15:27 volume.
#
# 2) 3-MIN MORNING:
#       09:15 and 09:18 must be SAME trend.
#       There is NO connection to 15:24 / 15:27.
#
# 3) 1-MIN:
#       15:28 and 15:29 must be OPPOSITE trends.
#       15:28 volume > 15:29 volume.
#
# 4) 1-MIN / 3-MIN:
#       1-min 15:28 trend = 3-min 15:24 trend.
#
# FINAL DIRECTION:
#       1-min 15:28 trend.
#
# TRADE:
#       Entry = next trading day 09:15 open
#       Exit  = 15:27
#
# =============================================================================
# INSTALL
# =============================================================================
#
# pip install -U "yfinance==1.7.0" pandas requests curl_cffi
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

try:
    import yfinance as yf
except ImportError:
    print("Missing yfinance.")
    print('Run: pip install -U "yfinance==1.7.0" pandas requests curl_cffi')
    sys.exit(1)


# =============================================================================
# CONFIG
# =============================================================================

MAX_WORKERS = 8
MAX_RETRIES = 2

INITIAL_PERIOD = "3d"
FALLBACK_PERIOD = "7d"
REQUEST_TIMEOUT = 20

NIFTY500_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"

NSE_EQUITY_URL = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"

NEEDED_HM = {
    915, 916, 917,
    918, 919, 920,
    1524, 1525, 1526,
    1527, 1528, 1529,
}


# =============================================================================
# FALLBACK UNIVERSE
# =============================================================================

NIFTY_50 = [
    "RELIANCE","TCS","HDFCBANK","ICICIBANK","INFY","HINDUNILVR","ITC",
    "SBIN","BHARTIARTL","KOTAKBANK","LT","AXISBANK","BAJFINANCE",
    "ASIANPAINT","MARUTI","HCLTECH","SUNPHARMA","TITAN","ULTRACEMCO",
    "NESTLEIND","WIPRO","ADANIENT","ONGC","NTPC","POWERGRID","M&M",
    "JSWSTEEL","TATASTEEL","TATAMOTORS","COALINDIA","BAJAJFINSV","TECHM",
    "INDUSINDBK","HDFCLIFE","SBILIFE","GRASIM","DRREDDY","DIVISLAB",
    "EICHERMOT","BRITANNIA","CIPLA","APOLLOHOSP","HEROMOTOCO","BPCL",
    "TATACONSUM","ADANIPORTS","HINDALCO","BAJAJ-AUTO","SHRIRAMFIN","LTIM",
    "UPL"
]

NIFTY_NEXT_150 = [
    "ABB","ADANIENSOL","ADANIGREEN","ADANIPOWER","AMBUJACEM","DMART",
    "BANKBARODA","BERGEPAINT","BEL","BOSCHLTD","CANBK","CHOLAFIN","COLPAL",
    "DABUR","DLF","GAIL","GODREJCP","HAVELLS","HAL","ICICIGI","ICICIPRULI",
    "IOC","IRCTC","IRFC","JINDALSTEL","JIOFIN","LICI","LODHA","LUPIN",
    "MARICO","MOTHERSON","MRF","NAUKRI","NHPC","PIDILITIND","PFC","PNB",
    "RECLTD","SIEMENS","SRF","TATAPOWER","TORNTPHARM","TVSMOTOR","UNIONBANK",
    "VBL","VEDL","ZOMATO","ZYDUSLIFE","PAYTM","POLICYBZR","PERSISTENT",
    "COFORGE","MPHASIS","OBEROIRLTY","PIIND","ASHOKLEY","AUROPHARMA",
    "BANDHANBNK","BATAINDIA","BHARATFORG","BHEL","CGPOWER","CONCOR",
    "CUMMINSIND","DEEPAKNTR","DIXON","ESCORTS","EXIDEIND","FEDERALBNK",
    "GLAND","GMRAIRPORT","GODREJPROP","GUJGASLTD","HDFCAMC","HINDPETRO",
    "IDEA","IDFCFIRSTB","IGL","INDHOTEL","INDIGO","INDUSTOWER","IPCALAB",
    "JSWENERGY","JUBLFOOD","KALYANKJIL","L&TFH","LALPATHLAB","LAURUSLABS",
    "LTTS","M&MFIN","MANKIND","MAXHEALTH","METROPOLIS","MFSL","MUTHOOTFIN",
    "NATIONALUM","NAVINFLUOR","NMDC","OFSS","PAGEIND","PATANJALI","PETRONET",
    "PHOENIXLTD","POLYCAB","PRESTIGE","RAMCOCEM","RVNL","SAIL","SBICARD",
    "SCHAEFFLER","SHREECEM","SJVN","SOLARINDS","SONACOMS","STARHEALTH",
    "SUNDARMFIN","SUPREMEIND","SUZLON","SYNGENE","TATACHEM","TATACOMM",
    "TATAELXSI","THERMAX","TIINDIA","TORNTPOWER","TRENT","TRIDENT","UBL",
    "UCOBANK","VOLTAS","WHIRLPOOL","YESBANK","ZEEL","ABCAPITAL","ABFRL",
    "ALKEM","APLAPOLLO","APOLLOTYRE","ASTRAL","AUBANK","BALKRISIND",
    "BANKINDIA","BSOFT","CANFINHOME","CENTRALBK","CROMPTON","CYIENT",
    "DALBHARAT","DELHIVERY","DEVYANI","EMAMILTD","GICRE","GLENMARK","GNFC",
    "GODIGIT","GRANULES","GRSE","HFCL","HONAUT"
]

FALLBACK_UNIVERSE = list(dict.fromkeys(NIFTY_50 + NIFTY_NEXT_150))


# =============================================================================
# UNIVERSE LOADERS
# =============================================================================

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0 Safari/537.36"
    ),
    "Accept": "text/csv,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.niftyindices.com/",
}


def normalize_symbols(values):
    symbols = []
    for value in values:
        s = str(value).strip().upper()
        if not s or s == "NAN":
            continue
        # Yahoo/NSE symbol cleanup for names that are not valid ordinary EQ symbols.
        symbols.append(s)
    return list(dict.fromkeys(symbols))


def load_nifty500():
    """Load the current Nifty 500 constituent list."""
    try:
        session = requests.Session()
        session.headers.update(NSE_HEADERS)

        # Warm the domain first; Nifty Indices sometimes rejects a direct CSV request.
        try:
            session.get("https://www.niftyindices.com/", timeout=15)
        except Exception:
            pass

        r = session.get(NIFTY500_URL, timeout=20)
        r.raise_for_status()

        df = pd.read_csv(StringIO(r.text))
        symbol_col = next(
            (c for c in df.columns if str(c).strip().lower() == "symbol"),
            None
        )

        if symbol_col is None:
            raise ValueError(f"Symbol column not found. Columns={list(df.columns)}")

        symbols = normalize_symbols(df[symbol_col].dropna().tolist())

        if len(symbols) < 450:
            raise ValueError(f"Nifty 500 CSV returned only {len(symbols)} symbols")

        return symbols

    except Exception as e:
        print(f"Nifty 500 CSV unavailable: {e}")
        return None


def load_nse_equity_list():
    """Fallback: NSE official equity security list."""
    try:
        session = requests.Session()
        session.headers.update({
            **NSE_HEADERS,
            "Referer": "https://www.nseindia.com/",
        })

        session.get("https://www.nseindia.com/", timeout=15)

        r = session.get(NSE_EQUITY_URL, timeout=20)
        r.raise_for_status()

        df = pd.read_csv(StringIO(r.text))

        symbol_col = next(
            (c for c in df.columns if "symbol" in str(c).lower()),
            None
        )
        series_col = next(
            (c for c in df.columns if "series" in str(c).lower()),
            None
        )

        if symbol_col is None:
            raise ValueError("NSE Symbol column not found")

        if series_col is not None:
            df = df[
                df[series_col].astype(str).str.strip().str.upper() == "EQ"
            ]

        symbols = normalize_symbols(df[symbol_col].dropna().tolist())

        if len(symbols) < 300:
            raise ValueError(f"NSE list returned only {len(symbols)} EQ symbols")

        return symbols

    except Exception as e:
        print(f"NSE official equity list unavailable: {e}")
        return None


def get_stock_universe():
    print("Loading current NSE universe...")

    symbols = load_nifty500()
    if symbols:
        print(f"Universe source: Nifty 500 official CSV ({len(symbols)} symbols)")
        return symbols, "Nifty 500 official CSV"

    symbols = load_nse_equity_list()
    if symbols:
        print(f"Universe source: NSE official equity list ({len(symbols)} symbols)")
        return symbols, "NSE official equity list"

    print(
        f"Universe source: built-in fallback "
        f"({len(FALLBACK_UNIVERSE)} symbols)"
    )
    return FALLBACK_UNIVERSE, "Built-in fallback"


# =============================================================================
# YAHOO DATA
# =============================================================================

def clean_yahoo_data(df):
    if df is None or df.empty:
        return None

    df = df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        # For a single ticker, use the first level.
        df.columns = [c[0] for c in df.columns]

    df = df.reset_index()

    timestamp_col = None
    for candidate in ("Datetime", "Date", "datetime", "date"):
        if candidate in df.columns:
            timestamp_col = candidate
            break

    if timestamp_col is None:
        return None

    ts = pd.to_datetime(df[timestamp_col], errors="coerce")
    valid = ts.notna()
    if not valid.any():
        return None

    df = df.loc[valid].copy()
    ts = ts.loc[valid]

    if ts.dt.tz is not None:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    else:
        ts = ts.dt.tz_localize("Asia/Kolkata")

    required = ["Open", "Close", "Volume"]
    if not all(c in df.columns for c in required):
        return None

    out = pd.DataFrame({
        "date": ts.dt.strftime("%Y-%m-%d").values,
        "hm": (ts.dt.hour * 100 + ts.dt.minute).values,
        "open": pd.to_numeric(df["Open"], errors="coerce").values,
        "close": pd.to_numeric(df["Close"], errors="coerce").values,
        "volume": pd.to_numeric(df["Volume"], errors="coerce").values,
    })

    out = out[out["hm"].between(915, 1529)]
    out = out.dropna(subset=["open", "close", "volume"])
    out = out.drop_duplicates(["date", "hm"], keep="last")

    return out if not out.empty else None


def find_complete_days(rows):
    if rows is None or rows.empty:
        return []

    complete = []
    for date, group in rows.groupby("date"):
        available = set(group["hm"].astype(int))
        if NEEDED_HM.issubset(available):
            complete.append(date)

    return sorted(complete)


def yahoo_download(symbol, period):
    """
    One Yahoo request.

    yfinance 1.7.0 has current cookie/crumb handling and fallback logic.
    We deliberately keep retries low so a dead/delisted symbol cannot stall
    the entire scanner.
    """
    ticker = f"{symbol}.NS"

    for attempt in range(MAX_RETRIES):
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

            cleaned = clean_yahoo_data(df)

            if cleaned is not None and not cleaned.empty:
                return cleaned, None

            # Empty data is normally a symbol/data availability problem.
            return None, "NO_DATA"

        except Exception as e:
            message = str(e).replace("\n", " ")
            if attempt < MAX_RETRIES - 1:
                time.sleep((1.5 ** attempt) + random.uniform(0.1, 0.5))
            else:
                return None, message[:240]

    return None, "NO_DATA"


def fetch_symbol_rows(symbol):
    # First request: fast.
    rows, err = yahoo_download(symbol, INITIAL_PERIOD)

    if rows is not None and find_complete_days(rows):
        return rows, "OK"

    # Only one fallback request, not four more retries.
    rows2, err2 = yahoo_download(symbol, FALLBACK_PERIOD)

    if rows2 is not None and find_complete_days(rows2):
        return rows2, "FALLBACK"

    if rows2 is not None:
        return rows2, "INCOMPLETE"

    return None, "NO_DATA"


# =============================================================================
# STRATEGY
# =============================================================================

def candle_direction(open_price, close_price):
    if close_price > open_price:
        return 1
    if close_price < open_price:
        return -1
    return 0


def evaluate_rows(rows):
    if rows is None or rows.empty:
        return {"status": "NO_DATA"}

    by_date = {}

    for _, row in rows.iterrows():
        by_date.setdefault(row["date"], {})[int(row["hm"])] = row

    complete_dates = []

    for date, minute_map in by_date.items():
        if all(hm in minute_map for hm in NEEDED_HM):
            complete_dates.append(date)

    if not complete_dates:
        return {"status": "INCOMPLETE"}

    signal_date = max(complete_dates)
    m = by_date[signal_date]

    def aggregate_3m(minutes):
        candles = [m[x] for x in minutes]
        return {
            "open": float(candles[0]["open"]),
            "close": float(candles[-1]["close"]),
            "volume": sum(float(x["volume"]) for x in candles),
        }

    c0915 = aggregate_3m([915, 916, 917])
    c0918 = aggregate_3m([918, 919, 920])
    c1524 = aggregate_3m([1524, 1525, 1526])
    c1527 = aggregate_3m([1527, 1528, 1529])

    d0915 = candle_direction(c0915["open"], c0915["close"])
    d0918 = candle_direction(c0918["open"], c0918["close"])
    d1524 = candle_direction(c1524["open"], c1524["close"])
    d1527 = candle_direction(c1527["open"], c1527["close"])

    d1528 = candle_direction(
        float(m[1528]["open"]),
        float(m[1528]["close"]),
    )
    d1529 = candle_direction(
        float(m[1529]["open"]),
        float(m[1529]["close"]),
    )

    v1524 = c1524["volume"]
    v1527 = c1527["volume"]
    v1528 = float(m[1528]["volume"])
    v1529 = float(m[1529]["volume"])

    # EXACT CURRENT CONDITIONS
    cond1 = (
        d1524 != 0
        and v1524 > v1527
    )

    # Morning 3m candles only need to match each other.
    # They are intentionally NOT compared with d1524/d1527.
    cond2 = (
        d0915 != 0
        and d0918 != 0
        and d0915 == d0918
    )

    cond3 = (
        d1528 != 0
        and d1529 != 0
        and d1528 != d1529
        and v1528 > v1529
    )

    cond4 = (
        d1528 != 0
        and d1524 != 0
        and d1528 == d1524
    )

    passed = cond1 and cond2 and cond3 and cond4

    direction = (
        "LONG" if d1528 == 1
        else "SHORT" if d1528 == -1
        else None
    )

    return {
        "status": "PASS" if passed else "FAIL",
        "date": signal_date,
        "direction": direction if passed else None,
        "raw_direction": direction,
        "cond1": cond1,
        "cond2": cond2,
        "cond3": cond3,
        "cond4": cond4,
        "details": {
            "d0915": d0915,
            "d0918": d0918,
            "d1524": d1524,
            "d1527": d1527,
            "d1528": d1528,
            "d1529": d1529,
            "15:24_vol": v1524,
            "15:27_vol": v1527,
            "15:28_vol": v1528,
            "15:29_vol": v1529,
        },
    }


# =============================================================================
# SCAN
# =============================================================================

def scan_one_symbol(symbol):
    try:
        rows, data_status = fetch_symbol_rows(symbol)

        if rows is None:
            return {
                "symbol": symbol,
                "status": "NO_DATA",
                "data_status": data_status,
            }

        result = evaluate_rows(rows)
        result["symbol"] = symbol
        result["data_status"] = data_status
        return result

    except Exception as e:
        return {
            "symbol": symbol,
            "status": "ERROR",
            "data_status": "ERROR",
            "error": str(e)[:240],
        }


def scan_all_symbols(symbols):
    total = len(symbols)
    results = []
    completed = 0
    start = time.time()

    print()
    print("=" * 70)
    print(f"Scanning {total} symbols with {MAX_WORKERS} workers")
    print("=" * 70)

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
                    f"[{completed}/{total}] "
                    f"{symbol:<15} MATCH "
                    f"{result.get('direction')}"
                )
            elif completed % 10 == 0 or completed == total:
                print(f"Progress: {completed}/{total}")

    results.sort(key=lambda x: x.get("symbol", ""))
    return results, time.time() - start


# =============================================================================
# HTML
# =============================================================================

def badge(value):
    if value is True:
        return '<span class="badge pass">PASS</span>'
    if value is False:
        return '<span class="badge fail">FAIL</span>'
    return "â€”"


def esc(value):
    return html.escape(str(value))


def generate_html_report(results, elapsed, universe_source):
    matches = [r for r in results if r.get("status") == "PASS"]
    fails = [r for r in results if r.get("status") == "FAIL"]
    incomplete = [r for r in results if r.get("status") == "INCOMPLETE"]
    no_data = [r for r in results if r.get("status") == "NO_DATA"]
    errors = [r for r in results if r.get("status") == "ERROR"]

    if matches:
        match_html = "".join(
            f"""
            <div class="match">
                <span class="direction {'long' if r['direction']=='LONG' else 'short'}">
                    {esc(r['direction'])}
                </span>
                <b>{esc(r['symbol'])}</b>
                <span class="date">Signal day: {esc(r.get('date', 'â€”'))}</span>
            </div>
            """
            for r in matches
        )
    else:
        match_html = '<div class="none">No stocks matched today.</div>'

    rows_html = []

    for r in sorted(
        results,
        key=lambda x: (x.get("status") != "PASS", x.get("symbol", "")),
    ):
        d = r.get("details", {})
        status = r.get("status", "UNKNOWN")
        status_class = (
            "pass" if status == "PASS"
            else "fail" if status == "FAIL"
            else "skip"
        )

        rows_html.append(
            f"""
            <tr>
                <td class="symbol">{esc(r.get('symbol', ''))}</td>
                <td>{esc(r.get('date', 'â€”'))}</td>
                <td>{esc(r.get('direction') or 'â€”')}</td>
                <td>
                    {badge(r.get('cond1'))}<br>
                    <small>
                    15:24 {d.get('15:24_vol', 0):,.0f}
                    &gt;
                    15:27 {d.get('15:27_vol', 0):,.0f}
                    </small>
                </td>
                <td>{badge(r.get('cond2'))}</td>
                <td>
                    {badge(r.get('cond3'))}<br>
                    <small>
                    15:28 {d.get('15:28_vol', 0):,.0f}
                    &gt;
                    15:29 {d.get('15:29_vol', 0):,.0f}
                    </small>
                </td>
                <td>{badge(r.get('cond4'))}</td>
                <td class="{status_class}">{esc(status)}</td>
                <td>{esc(r.get('data_status', 'â€”'))}</td>
            </tr>
            """
        )

    scan_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_doc = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>NSE Daily Momentum Scanner</title>
<style>
body {{
    font-family: Arial, sans-serif;
    background: #f5f5f5;
    color: #222;
    margin: 0;
    padding: 25px;
}}
.container {{ max-width: 1500px; margin: auto; }}
h1 {{ margin-bottom: 5px; }}
.subtitle {{ color: #777; }}
.stats {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin: 20px 0;
}}
.stat {{
    background: white;
    padding: 15px 20px;
    border-radius: 8px;
    border: 1px solid #ddd;
}}
.stat b {{ font-size: 22px; display: block; }}
.match-box {{
    background: white;
    border: 1px solid #ddd;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 20px;
}}
.match {{
    padding: 9px 0;
    border-bottom: 1px solid #eee;
}}
.direction {{
    display: inline-block;
    padding: 3px 7px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: bold;
    margin-right: 8px;
}}
.long {{ background: #dff5e5; color: #08752f; }}
.short {{ background: #f8dddd; color: #a52222; }}
.date {{ color: #777; margin-left: 8px; }}
table {{
    width: 100%;
    border-collapse: collapse;
    background: white;
    font-size: 13px;
}}
th {{
    background: #ededed;
    padding: 10px;
    text-align: left;
    position: sticky;
    top: 0;
}}
td {{
    padding: 9px 10px;
    border-top: 1px solid #eee;
}}
.symbol {{ font-weight: bold; }}
small {{ color: #777; font-size: 10px; }}
.badge {{
    display: inline-block;
    padding: 2px 6px;
    border-radius: 4px;
    font-size: 10px;
    font-weight: bold;
}}
.badge.pass {{ background: #dff5e5; color: #08752f; }}
.badge.fail {{ background: #f8dddd; color: #a52222; }}
.pass {{ color: #08752f; font-weight: bold; }}
.fail {{ color: #999; }}
.skip {{ color: #b07000; }}
.none {{ color: #777; }}
.footer {{
    margin-top: 25px;
    color: #777;
    font-size: 12px;
    line-height: 1.7;
}}
</style>
</head>
<body>
<div class="container">

<h1>NSE Daily Momentum Scanner</h1>

<div class="subtitle">
Generated: {esc(scan_time)}<br>
Universe: {esc(universe_source)}<br>
Yahoo Finance 1-minute data
</div>

<div class="stats">
<div class="stat"><b>{len(results)}</b>Stocks scanned</div>
<div class="stat"><b>{len(matches)}</b>Matches</div>
<div class="stat"><b>{len(fails)}</b>No match</div>
<div class="stat"><b>{len(incomplete)}</b>Incomplete</div>
<div class="stat"><b>{len(no_data)}</b>No data</div>
<div class="stat"><b>{len(errors)}</b>Errors</div>
<div class="stat"><b>{elapsed:.1f}s</b>Scan time</div>
</div>

<div class="match-box">
<h2>Matches</h2>
{match_html}
</div>

<table>
<thead>
<tr>
<th>Symbol</th>
<th>Signal Day</th>
<th>Direction</th>
<th>3M 15:24 Volume</th>
<th>3M Morning</th>
<th>1M 15:28 / 15:29</th>
<th>1M / 3M Match</th>
<th>Result</th>
<th>Data</th>
</tr>
</thead>
<tbody>
{''.join(rows_html)}
</tbody>
</table>

<div class="footer">
<b>Strategy:</b><br>
1. 3-min 15:24 volume &gt; 15:27 volume. 15:24 and 15:27 trend relationship is ignored.<br>
2. 3-min 09:15 and 09:18 must be the same trend. They are NOT compared with 15:24 or 15:27.<br>
3. 1-min 15:28 and 15:29 must be opposite trends, with 15:28 volume &gt; 15:29 volume.<br>
4. 1-min 15:28 trend must equal 3-min 15:24 trend.<br>
5. Final direction = 1-min 15:28 trend.<br>
<b>Entry:</b> next trading day 09:15 open.<br>
<b>Exit:</b> 15:27.
</div>

</div>
</body>
</html>
"""

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html_doc)


# =============================================================================
# MAIN
# =============================================================================

def main():
    print()
    print("=" * 70)
    print("NSE EOD MOMENTUM SCANNER")
    print("=" * 70)

    print(f"yfinance version: {getattr(yf, '__version__', 'unknown')}")

    universe, source = get_stock_universe()

    print(f"Final universe: {len(universe)} symbols")
    print(f"Universe source: {source}")

    start = time.time()
    results, elapsed = scan_all_symbols(universe)

    matches = [r for r in results if r.get("status") == "PASS"]

    print()
    print("=" * 70)
    print("SCAN COMPLETE")
    print("=" * 70)
    print(f"Universe   : {len(universe)}")
    print(f"Matches    : {len(matches)}")
    print(f"Time       : {elapsed:.1f} seconds")
    print()

    if matches:
        print("MATCHES:")
        for r in matches:
            print(
                f"  {r['symbol']:<15} "
                f"{r['direction']:<6} "
                f"{r['date']}"
            )
    else:
        print("NO MATCHES.")

    print()
    print(f"PASS       : {sum(r.get('status') == 'PASS' for r in results)}")
    print(f"FAIL       : {sum(r.get('status') == 'FAIL' for r in results)}")
    print(f"INCOMPLETE : {sum(r.get('status') == 'INCOMPLETE' for r in results)}")
    print(f"NO DATA    : {sum(r.get('status') == 'NO_DATA' for r in results)}")
    print(f"ERROR      : {sum(r.get('status') == 'ERROR' for r in results)}")

    generate_html_report(results, elapsed, source)

    print()
    print("HTML report: index.html")
    print("=" * 70)


if __name__ == "__main__":
    main()
