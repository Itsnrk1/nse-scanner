#!/usr/bin/env python3
"""
NSE EOD Momentum Scanner

Strategy
--------
3-minute:
  - 15:24 volume > 15:27 volume
  - 09:15 trend = 09:18 trend
  - NO restriction between the trends of 15:24 and 15:27

1-minute:
  - 15:28 trend != 15:29 trend
  - 15:28 volume > 15:29 volume
  - 15:28 trend = 3-minute 15:24 trend

Direction:
  - 1m 15:28 bullish -> LONG
  - 1m 15:28 bearish -> SHORT

Trade reference:
  - Next trading day 09:15 OPEN
  - Exit 15:27

This is the main Yahoo Finance scanner. It does NOT use Parquet files.

Install:
    pip install yfinance pandas requests nselib

Run:
    python nse_eod_momentum_scanner.py

The scanner also creates:
    index.html
"""

import html
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import yfinance as yf

IST = "Asia/Kolkata"
MAX_WORKERS = 12
MAX_RETRIES = 4
INITIAL_PERIOD = "3d"
FALLBACK_PERIOD = "7d"
REQUEST_TIMEOUT = 20
OUTPUT_HTML = "index.html"

SESSION_START = "09:15"
SESSION_END = "15:29"

NEEDED_HM = {
    "09:15", "09:16", "09:17",
    "09:18", "09:19", "09:20",
    "15:24", "15:25", "15:26",
    "15:27", "15:28", "15:29",
}

NIFTY_50 = [
    "ADANIENT","ADANIPORTS","APOLLOHOSP","ASIANPAINT","AXISBANK",
    "BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BEL","BHARTIARTL",
    "CIPLA","COALINDIA","DRREDDY","EICHERMOT","ETERNAL","GRASIM",
    "HCLTECH","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO",
    "HINDUNILVR","ICICIBANK","INDUSINDBK","INFY","ITC","JIOFIN",
    "JSWSTEEL","KOTAKBANK","LT","M&M","MARUTI","MAXHEALTH",
    "NESTLEIND","NTPC","ONGC","POWERGRID","RELIANCE","SBILIFE",
    "SBIN","SHRIRAMFIN","SUNPHARMA","TATACONSUM","TATAMOTORS",
    "TATASTEEL","TCS","TECHM","TITAN","TRENT","ULTRACEMCO"
]

NIFTY_NEXT_150 = [
    "ABB","ACC","ABCAPITAL","ABFRL","ADANIENSOL","ADANIGREEN",
    "ADANIPOWER","ALKEM","AMBUJACEM","APARINDS","ASHOKLEY",
    "ASTRAL","AUROPHARMA","BANKBARODA","BANKINDIA","BANDHANBNK",
    "BATAINDIA","BDL","BERGEPAINT","BHARATFORG","BHEL","BIOCON",
    "BLUESTARCO","BOSCHLTD","BPCL","BRITANNIA","CANBK","CDSL",
    "CGPOWER","CHAMBLFERT","CHOLAFIN","COLPAL","CONCOR",
    "COROMANDEL","CUMMINSIND","DABUR","DALBHARAT","DEEPAKNTR",
    "DELHIVERY","DIVISLAB","DIXON","DLF","DMART","EXIDEIND",
    "FEDERALBNK","FORTIS","GAIL","GLENMARK","GMRINFRA","GODREJCP",
    "GODREJPROP","HAL","HAVELLS","HINDPETRO","HUDCO","ICICIGI",
    "IDFCFIRSTB","IEX","IGL","INDHOTEL","INDIANB","INDIGO",
    "INDUSTOWER","INOXWIND","IRCTC","IREDA","IRFC","JINDALSTEL",
    "JUBLFOOD","JSWENERGY","KALYANKJIL","KAYNES","KEI","KPITTECH",
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LUPIN","MANAPPURAM",
    "MANKIND","MARICO","MCX","MFSL","MGL","MOTHERSON","MPHASIS",
    "MRF","MUTHOOTFIN","NATIONALUM","NAUKRI","NBCC","NCC","NHPC",
    "NMDC","NLCINDIA","OBEROIRLTY","OFSS","OIL","OLAELEC","PAGEIND",
    "PAYTM","PEL","PERSISTENT","PETRONET","PFC","PHOENIXLTD",
    "PIDILITIND","PIIND","PNB","POLYCAB","POONAWALLA","PRESTIGE",
    "RBLBANK","RECLTD","RVNL","SAIL","SAMMAANCAP","SCHAEFFLER",
    "SHREECEM","SIEMENS","SOLARINDS","SONACOMS","SRF","STARHEALTH",
    "SUPREMEIND","SUZLON","SYNGENE","TATACHEM","TATAELXSI",
    "TATATECH","THERMAX","TIINDIA","TORNTPHARM","TORNTPOWER",
    "TVSMOTOR","UNOMINDA","UPL","VEDL","VOLTAS","WAAREEENER",
    "YESBANK","ZEEL"
]


def get_static_universe():
    return sorted(set(NIFTY_50 + NIFTY_NEXT_150))


def get_live_nifty500_universe():
    try:
        from nselib import capital_market
        df = capital_market.nifty500_equity_list()
        if df is None or df.empty:
            return None

        column = next(
            (c for c in ("Symbol", "SYMBOL", "symbol") if c in df.columns),
            None
        )
        if column is None:
            return None

        symbols = (
            df[column].astype(str).str.strip().str.upper().tolist()
        )
        symbols = [s for s in symbols if s and s not in {"NAN", "NONE"}]
        return sorted(set(symbols)) if symbols else None
    except Exception:
        return None


def build_universe():
    live = get_live_nifty500_universe()
    if live:
        print(f"Using live NIFTY 500 universe: {len(live)} symbols")
        return live

    fallback = get_static_universe()
    print(f"Using fallback universe: {len(fallback)} symbols")
    return fallback


def clean_yahoo_data(raw):
    if raw is None or raw.empty:
        return pd.DataFrame()

    df = raw.copy()

    if isinstance(df.columns, pd.MultiIndex):
        flattened = []
        for col in df.columns:
            found = None
            for part in col:
                p = str(part).lower()
                if p in {"open", "high", "low", "close", "volume"}:
                    found = p
                    break
            flattened.append(found if found else "_".join(map(str, col)))
        df.columns = flattened

    df.columns = [str(c).strip().lower() for c in df.columns]

    required = ["open", "high", "low", "close", "volume"]
    if any(c not in df.columns for c in required):
        return pd.DataFrame()

    df = df[required].copy()

    idx = pd.to_datetime(df.index, errors="coerce")
    if getattr(idx, "tz", None) is None:
        idx = idx.tz_localize("UTC")
    idx = idx.tz_convert(IST)

    df.index = idx
    df.index.name = "timestamp"
    df = df[~df.index.duplicated(keep="last")]

    for col in required:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])
    return df.between_time(SESSION_START, SESSION_END)


def find_complete_days(df):
    if df.empty:
        return []

    complete = []
    for d in sorted(set(df.index.date), reverse=True):
        day = df[df.index.date == d]
        if NEEDED_HM.issubset(set(day.index.strftime("%H:%M"))):
            complete.append(d)
    return complete


def download_symbol(symbol):
    ticker = f"{symbol}.NS"

    for period in (INITIAL_PERIOD, FALLBACK_PERIOD):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                raw = yf.download(
                    ticker,
                    period=period,
                    interval="1m",
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                    timeout=REQUEST_TIMEOUT,
                )
                df = clean_yahoo_data(raw)
                if not df.empty:
                    return df
            except Exception as exc:
                if attempt == MAX_RETRIES:
                    print(f"[ERROR] {symbol}: {exc}")
            time.sleep(min(1.5 * attempt, 5))

    return pd.DataFrame()


def candle_direction(open_price, close_price):
    if close_price > open_price:
        return 1
    if close_price < open_price:
        return -1
    return 0


def direction_text(direction):
    return {
        1: "BULLISH",
        -1: "BEARISH",
        0: "DOJI",
    }[direction]


def get_minute_candle(day_df, hhmm):
    rows = day_df[day_df.index.strftime("%H:%M") == hhmm]
    if rows.empty:
        return None

    row = rows.iloc[-1]
    o = float(row["open"])
    c = float(row["close"])

    return {
        "open": o,
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": c,
        "volume": float(row["volume"]),
        "direction": candle_direction(o, c),
    }


def aggregate_3m(day_df, start_hm):
    hour = int(start_hm[:2])
    minute = int(start_hm[3:])
    base = hour * 60 + minute
    wanted = {base, base + 1, base + 2}

    mask = day_df.index.map(
        lambda x: x.hour * 60 + x.minute in wanted
    )
    subset = day_df.loc[mask].sort_index()

    if len(subset) != 3:
        return None

    o = float(subset.iloc[0]["open"])
    h = float(subset["high"].max())
    l = float(subset["low"].min())
    c = float(subset.iloc[-1]["close"])
    v = float(subset["volume"].sum())

    return {
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v,
        "direction": candle_direction(o, c),
    }


def evaluate_rows(symbol, df):
    if df.empty:
        return {
            "symbol": symbol,
            "status": "NO DATA",
            "match": False,
            "reason": "No Yahoo Finance 1-minute data",
        }

    complete_days = find_complete_days(df)
    if not complete_days:
        return {
            "symbol": symbol,
            "status": "INCOMPLETE",
            "match": False,
            "reason": "Required strategy minutes unavailable",
        }

    signal_date = complete_days[0]
    day_df = df[df.index.date == signal_date].copy()

    c0915 = aggregate_3m(day_df, "09:15")
    c0918 = aggregate_3m(day_df, "09:18")
    c1524 = aggregate_3m(day_df, "15:24")
    c1527 = aggregate_3m(day_df, "15:27")
    c1528 = get_minute_candle(day_df, "15:28")
    c1529 = get_minute_candle(day_df, "15:29")

    candles = [c0915, c0918, c1524, c1527, c1528, c1529]
    if any(c is None for c in candles):
        return {
            "symbol": symbol,
            "status": "INCOMPLETE",
            "match": False,
            "signal_date": str(signal_date),
            "reason": "Could not build all required candles",
        }

    d0915 = c0915["direction"]
    d0918 = c0918["direction"]
    d1524 = c1524["direction"]
    d1527 = c1527["direction"]
    d1528 = c1528["direction"]
    d1529 = c1529["direction"]

    # --------------------------------------------------------
    # FINAL STRATEGY
    # --------------------------------------------------------

    # 1) 3m 15:24 volume > 3m 15:27 volume.
    #    IMPORTANT: no trend relationship between 15:24/15:27.
    cond1 = c1524["volume"] > c1527["volume"]

    # 2) 3m 09:15 and 09:18 same non-doji trend.
    cond2 = (
        d0915 != 0
        and d0918 != 0
        and d0915 == d0918
    )

    # 3) 1m 15:28 and 15:29 opposite non-doji trends,
    #    with 15:28 volume greater than 15:29.
    cond3 = (
        d1528 != 0
        and d1529 != 0
        and d1528 != d1529
        and c1528["volume"] > c1529["volume"]
    )

    # 4) 1m 15:28 trend = 3m 15:24 trend.
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
        "symbol": symbol,
        "status": "MATCH" if passed else "NO MATCH",
        "match": passed,
        "signal_date": str(signal_date),
        "direction": direction,

        "d0915": direction_text(d0915),
        "d0918": direction_text(d0918),
        "d1524": direction_text(d1524),
        "d1527": direction_text(d1527),
        "d1528": direction_text(d1528),
        "d1529": direction_text(d1529),

        "v1524": c1524["volume"],
        "v1527": c1527["volume"],
        "v1528": c1528["volume"],
        "v1529": c1529["volume"],

        "cond1": cond1,
        "cond2": cond2,
        "cond3": cond3,
        "cond4": cond4,
    }


def scan_one_symbol(symbol):
    return evaluate_rows(symbol, download_symbol(symbol))


def scan_all_symbols(symbols):
    results = []
    total = len(symbols)
    completed = 0

    print(f"Scanning {total} symbols...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scan_one_symbol, s): s
            for s in symbols
        }

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "symbol": symbol,
                    "status": "ERROR",
                    "match": False,
                    "reason": str(exc),
                }

            results.append(result)
            completed += 1

            if completed % 10 == 0 or completed == total:
                print(f"Progress: {completed}/{total}")

    return sorted(results, key=lambda x: x.get("symbol", ""))


def direction_badge(text_value):
    if text_value == "BULLISH":
        return '<span class="bullish">BULLISH</span>'
    if text_value == "BEARISH":
        return '<span class="bearish">BEARISH</span>'
    return '<span class="neutral">DOJI</span>'


def fmt_volume(value):
    try:
        return f"{float(value):,.0f}"
    except Exception:
        return "-"


def generate_html_report(results, universe_size):
    matches = [r for r in results if r.get("match")]

    dates = sorted({
        r.get("signal_date")
        for r in results
        if r.get("signal_date")
    })
    signal_date = dates[-1] if dates else "N/A"

    generated_at = pd.Timestamp.now(tz=IST).strftime(
        "%Y-%m-%d %H:%M:%S %Z"
    )

    errors = sum(r.get("status") == "ERROR" for r in results)
    incomplete = sum(r.get("status") == "INCOMPLETE" for r in results)

    body = []

    for r in matches:
        direction = r.get("direction", "-")
        cls = "long" if direction == "LONG" else "short"

        body.append(f"""
<tr>
<td><strong>{html.escape(r["symbol"])}</strong></td>
<td>{html.escape(r.get("signal_date", "-"))}</td>
<td><span class="{cls}">{html.escape(direction)}</span></td>
<td>{direction_badge(r["d0915"])}</td>
<td>{direction_badge(r["d0918"])}</td>
<td>{direction_badge(r["d1524"])}</td>
<td>{direction_badge(r["d1527"])}</td>
<td>{direction_badge(r["d1528"])}</td>
<td>{direction_badge(r["d1529"])}</td>
<td>{fmt_volume(r["v1524"])}</td>
<td>{fmt_volume(r["v1527"])}</td>
<td>{fmt_volume(r["v1528"])}</td>
<td>{fmt_volume(r["v1529"])}</td>
</tr>
""")

    if not body:
        body_html = """
<tr>
<td colspan="13" class="empty">
No stocks matched the strategy on the latest complete day.
</td>
</tr>
"""
    else:
        body_html = "
".join(body)

    document = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NSE EOD Momentum Scanner</title>
<style>
body {{
    margin: 0;
    padding: 24px;
    background: #f4f6f8;
    color: #17202a;
    font-family: Arial, Helvetica, sans-serif;
}}
.container {{ max-width: 1800px; margin: auto; }}
h1 {{ margin-bottom: 6px; }}
.subtitle {{ color: #667085; margin-bottom: 22px; }}
.cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
    margin-bottom: 24px;
}}
.card {{
    background: white;
    padding: 18px;
    border-radius: 12px;
    box-shadow: 0 1px 4px rgba(0,0,0,.08);
}}
.label {{ font-size: 13px; color: #667085; }}
.value {{ font-size: 28px; font-weight: 700; margin-top: 5px; }}
.strategy, .table-wrap {{
    background: white;
    border-radius: 12px;
    padding: 18px;
    margin-bottom: 24px;
    box-shadow: 0 1px 4px rgba(0,0,0,.08);
}}
.strategy li {{ margin: 8px 0; }}
.table-wrap {{ overflow-x: auto; padding: 12px; }}
table {{
    width: 100%;
    border-collapse: collapse;
    min-width: 1250px;
}}
th, td {{
    padding: 10px 9px;
    border-bottom: 1px solid #eaecf0;
    text-align: center;
    white-space: nowrap;
}}
th {{ background: #f9fafb; font-size: 12px; }}
td {{ font-size: 13px; }}
.bullish, .long {{
    color: #067647;
    font-weight: 800;
}}
.bearish, .short {{
    color: #b42318;
    font-weight: 800;
}}
.neutral {{
    color: #667085;
    font-weight: 700;
}}
.long, .short {{
    display: inline-block;
    padding: 5px 10px;
    border-radius: 999px;
}}
.long {{ background: #dcfae6; }}
.short {{ background: #fee4e2; }}
.empty {{ padding: 40px; color: #667085; }}
.footer {{ color: #667085; font-size: 12px; }}
</style>
</head>
<body>
<div class="container">

<h1>NSE EOD Momentum Scanner</h1>

<div class="subtitle">
Latest complete signal day:
<strong>{html.escape(signal_date)}</strong>
&nbsp;|&nbsp;
Generated:
<strong>{html.escape(generated_at)}</strong>
</div>

<div class="cards">
<div class="card"><div class="label">Universe</div>
<div class="value">{universe_size}</div></div>

<div class="card"><div class="label">Scanned</div>
<div class="value">{len(results)}</div></div>

<div class="card"><div class="label">Matches</div>
<div class="value">{len(matches)}</div></div>

<div class="card"><div class="label">Incomplete</div>
<div class="value">{incomplete}</div></div>

<div class="card"><div class="label">Errors</div>
<div class="value">{errors}</div></div>
</div>

<div class="strategy">
<h2>Current Strategy</h2>
<ol>
<li>3M 15:24 volume &gt; 3M 15:27 volume.</li>
<li>3M 09:15 trend = 3M 09:18 trend.</li>
<li>1M 15:28 trend != 1M 15:29 trend.</li>
<li>1M 15:28 volume &gt; 1M 15:29 volume.</li>
<li>1M 15:28 trend = 3M 15:24 trend.</li>
<li>
3M 15:24 and 15:27 trend relationship is unrestricted:
they may be the same or opposite.
</li>
<li>
Direction: 1M 15:28 bullish = LONG;
1M 15:28 bearish = SHORT.
</li>
<li>Trade reference: next trading day 09:15 open â†’ 15:27 exit.</li>
</ol>
</div>

<div class="table-wrap">
<table>
<thead>
<tr>
<th>Symbol</th>
<th>Signal Day</th>
<th>Direction</th>
<th>3M 09:15</th>
<th>3M 09:18</th>
<th>3M 15:24</th>
<th>3M 15:27</th>
<th>1M 15:28</th>
<th>1M 15:29</th>
<th>3M 15:24 Vol</th>
<th>3M 15:27 Vol</th>
<th>1M 15:28 Vol</th>
<th>1M 15:29 Vol</th>
</tr>
</thead>
<tbody>
{body_html}
</tbody>
</table>
</div>

<div class="footer">
Data source: Yahoo Finance 1-minute data.
3-minute candles are constructed from 1-minute OHLCV data.
</div>

</div>
</body>
</html>
"""

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(document)

    return len(matches), signal_date


def main():
    started = time.time()

    print("=" * 70)
    print("NSE EOD MOMENTUM SCANNER")
    print("=" * 70)

    universe = build_universe()
    results = scan_all_symbols(universe)

    matches, signal_date = generate_html_report(
        results,
        len(universe)
    )

    elapsed = time.time() - started

    print("\n" + "=" * 70)
    print("SCAN COMPLETE")
    print("=" * 70)
    print(f"Signal day : {signal_date}")
    print(f"Universe   : {len(universe)}")
    print(f"Matches    : {matches}")
    print(f"HTML       : {OUTPUT_HTML}")
    print(f"Time       : {elapsed:.1f} seconds")

    print("\nMATCHES:")
    for result in results:
        if result.get("match"):
            print(
                f"  {result['symbol']:15s} "
                f"{result['direction']:5s} "
                f"{result['signal_date']}"
            )

    print("=" * 70)


if __name__ == "__main__":
    main()
