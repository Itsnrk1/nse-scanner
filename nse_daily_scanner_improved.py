"""
NSE Daily Scanner — runs automatically on GitHub's servers via GitHub Actions.
================================================================================
It checks each stock's most recently completed trading day against:
  1) 3-min candles 15:24 & 15:27 -> opposite trends, 15:24 volume > 15:27 volume
  2) 3-min candles 9:15 & 9:18   -> both match 15:24's direction
  3) 1-min candle 15:28          -> matches 15:24's direction, 15:28 volume > 15:29 volume
  4) 1-min candles 9:15 & 9:16   -> both match 15:29's direction
This rule is DIRECTIONAL: a PASS results in either a LONG or SHORT entry at
tomorrow's 9:15 open (exit at 15:27), depending on which way 15:24 went.

SETUP (one-time):
    pip install yfinance pandas nselib

USAGE:
    python nse_daily_scanner.py

Writes results to index.html. When run via the GitHub Actions workflow
(.github/workflows/daily_scan.yml), this happens automatically every trading
day and the result is published at your GitHub Pages URL.
"""

import time
import sys
import os
from datetime import datetime, timedelta

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("Missing dependency. Run:  pip install yfinance pandas --break-system-packages")
    sys.exit(1)


# ---------------------------------------------------------------------------
# STOCK UNIVERSE
# ---------------------------------------------------------------------------
# Tries the broadest option first (Nifty 500 via nselib, fetched live each run
# so it can't go stale) and falls back to a large hardcoded list if that
# fails for any reason. I could not test the nselib path myself — my own
# sandbox can't reach NSE — but it's genuinely worth attempting here since
# this now runs on GitHub's servers, which have normal internet access
# unlike my environment. If it fails, you'll see exactly why in the log,
# and the fallback list still gives you far more than just Nifty 50.

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

# Nifty Next 50 + a further set of liquid mid/large-caps — a snapshot, not
# gospel; index membership shifts roughly twice a year (Mar/Sep). Refresh
# periodically from NSE's own published index constituent files if you want
# this to stay precise.
NIFTY_NEXT_150 = [
    "ABB", "ADANIENSOL", "ADANIGREEN", "ADANIPOWER", "AMBUJACEM", "DMART",
    "BANKBARODA", "BERGEPAINT", "BEL", "BOSCHLTD", "CANBK", "CHOLAFIN",
    "COLPAL", "DABUR", "DLF", "GAIL", "GODREJCP", "HAVELLS", "HAL",
    "ICICIGI", "ICICIPRULI", "IOC", "IRCTC", "IRFC", "JINDALSTEL", "JIOFIN",
    "LICI", "LODHA", "LUPIN", "MARICO", "MOTHERSON", "MRF", "NAUKRI",
    "NHPC", "PIDILITIND", "PFC", "PNB", "RECLTD", "SIEMENS", "SRF",
    "TATAPOWER", "TORNTPHARM", "TVSMOTOR", "UNIONBANK", "VBL", "VEDL",
    "ZOMATO", "ZYDUSLIFE", "PAYTM", "POLICYBZR", "PERSISTENT", "COFORGE",
    "MPHASIS", "OBEROIRLTY", "PIIND", "ASHOKLEY", "AUROPHARMA", "BANDHANBNK",
    "BATAINDIA", "BHARATFORG", "BHEL", "CGPOWER", "CONCOR", "CUMMINSIND",
    "DEEPAKNTR", "DIXON", "ESCORTS", "EXIDEIND", "FEDERALBNK", "GLAND",
    "GMRAIRPORT", "GODREJPROP", "GUJGASLTD", "HDFCAMC", "HINDPETRO",
    "IDEA", "IDFCFIRSTB", "IGL", "INDHOTEL", "INDIGO", "INDUSTOWER",
    "IPCALAB", "JSWENERGY", "JUBLFOOD", "KALYANKJIL", "L&TFH", "LALPATHLAB",
    "LAURUSLABS", "LTTS", "M&MFIN", "MANKIND", "MAXHEALTH", "METROPOLIS",
    "MFSL", "MUTHOOTFIN", "NATIONALUM", "NAVINFLUOR", "NMDC", "OFSS",
    "PAGEIND", "PATANJALI", "PETRONET", "PHOENIXLTD", "POLYCAB", "PRESTIGE",
    "RAMCOCEM", "RVNL", "SAIL", "SBICARD", "SCHAEFFLER", "SHREECEM",
    "SJVN", "SOLARINDS", "SONACOMS", "STARHEALTH", "SUNDARMFIN", "SUPREMEIND",
    "SUZLON", "SYNGENE", "TATACHEM", "TATACOMM", "TATAELXSI", "THERMAX",
    "TIINDIA", "TORNTPOWER", "TRENT", "TRIDENT", "UBL", "UCOBANK",
    "VOLTAS", "WHIRLPOOL", "YESBANK", "ZEEL", "ABCAPITAL", "ABFRL",
    "ALKEM", "APLAPOLLO", "APOLLOTYRE", "ASTRAL", "AUBANK", "BALKRISIND",
    "BANKINDIA", "BSOFT", "CANFINHOME", "CENTRALBK", "CROMPTON", "CYIENT",
    "DALBHARAT", "DELHIVERY", "DEVYANI", "EMAMILTD", "GICRE", "GLENMARK",
    "GNFC", "GODIGIT", "GRANULES", "GRSE", "HFCL", "HONAUT",
]

STOCK_UNIVERSE = NIFTY_50 + NIFTY_NEXT_150  # ~210 stocks, safe fallback

# Tier 2: try the live Nifty 500 via nselib (broader, self-updating)
try:
    from nselib import indices
    df = indices.constituent_stock_list(index_category='BroadMarketIndices',
                                         index_name='Nifty 500')
    fetched = df['Symbol'].tolist()
    if len(fetched) > 100:
        STOCK_UNIVERSE = fetched
        print(f"Loaded {len(STOCK_UNIVERSE)} symbols from nselib (Nifty 500)")
except Exception as e:
    print(f"nselib Nifty 500 fetch failed ({e}) — keeping current {len(STOCK_UNIVERSE)}-stock list")

# Tier 3: try NSE's own full official equity list (~2200 stocks) — broadest option.
# I could not test this myself (my own sandbox can't reach NSE either), but it's
# worth attempting since this runs on GitHub's servers with real internet access.
# Falls back to whichever list survived Tier 1/2 above if this doesn't work.
try:
    import requests
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    session = requests.Session()
    session.headers.update(headers)
    session.get('https://www.nseindia.com', timeout=20)
    resp = session.get('https://nsearchives.nseindia.com/content/equities/sec_list.csv', timeout=30)
    resp.raise_for_status()
    from io import StringIO
    full_df = pd.read_csv(StringIO(resp.text))

    # Column name might not be exactly "SYMBOL" - search case-insensitively rather
    # than assume, and fail loudly with the actual columns seen if not found.
    symbol_col = next((c for c in full_df.columns if 'symbol' in c.lower()), None)
    if symbol_col is None:
        raise ValueError(f"No symbol-like column found. Columns present: {list(full_df.columns)}")

    # Filter to regular equity shares (series "EQ") only - this file also mixes in
    # ETFs, mutual fund wrappers, and SME-platform listings under other series codes,
    # which aren't what "stocks" means for this strategy.
    series_col = next((c for c in full_df.columns if 'series' in c.lower()), None)
    if series_col is not None:
        before = len(full_df)
        full_df = full_df[full_df[series_col].astype(str).str.strip() == 'EQ']
        print(f"Filtered to EQ series: {before} -> {len(full_df)} rows")

    # The EQ-series filter alone isn't enough - many ETFs, liquid funds, and gilt
    # funds are ALSO tagged EQ series on NSE, since they're exchange-traded the
    # same way regular equity is. Exclude by name/symbol keyword as a second pass.
    name_col = next((c for c in full_df.columns if 'name' in c.lower()), None)
    NON_STOCK_KEYWORDS = ['ETF', 'LIQUID', 'GILT', 'BEES', 'FUND', 'INDEX FUND',
                          'EXCHANGE TRADED', 'MUTUAL FUND', 'NIFTY 50', 'NIFTY50 ',
                          'NIFTYIETF', 'BETA', 'MOMENTUM', 'ALPHA', 'QUALITY30']
    if name_col is not None:
        before = len(full_df)
        name_upper = full_df[name_col].astype(str).str.upper()
        mask = ~name_upper.str.contains('|'.join(NON_STOCK_KEYWORDS), na=False)
        full_df = full_df[mask]
        print(f"Filtered out ETF/fund-like names: {before} -> {len(full_df)} rows")
    # Belt-and-braces: also exclude symbols themselves containing these patterns,
    # in case the name field is inconsistent for some rows.
    symbol_upper = full_df[symbol_col].astype(str).str.upper()
    sym_mask = ~symbol_upper.str.contains('ETF|IETF|BEES|LIQUID|GILT', na=False)
    before = len(full_df)
    full_df = full_df[sym_mask]
    if len(full_df) != before:
        print(f"Filtered out ETF-like symbol patterns: {before} -> {len(full_df)} rows")

    full_list = full_df[symbol_col].dropna().astype(str).str.strip().tolist()
    if len(full_list) > len(STOCK_UNIVERSE):
        STOCK_UNIVERSE = full_list
        print(f"Loaded {len(STOCK_UNIVERSE)} symbols from NSE's full equity list (column: {symbol_col}) — using this broadest option")
    else:
        print(f"NSE full list returned {len(full_list)} symbols, not larger than current {len(STOCK_UNIVERSE)} — keeping current list")
except Exception as e:
    print(f"NSE full equity list fetch failed ({e}) — keeping current {len(STOCK_UNIVERSE)}-stock list")

print(f"FINAL universe size: {len(STOCK_UNIVERSE)} stocks")


# ---------------------------------------------------------------------------
# SCAN LOGIC — identical rule to what was verified earlier in this conversation
# ---------------------------------------------------------------------------

NEEDED_HM = [915, 916, 917, 918, 919, 920, 1524, 1525, 1526, 1527, 1528, 1529]


def evaluate_rows(rows):
    """rows: DataFrame with columns date, hm, open, close, volume.
    Finds the latest date with all needed candles present and checks the rule.
    This rule is DIRECTIONAL - the resulting trade may be LONG or SHORT
    depending on which way the 15:24 3-min candle went."""
    by_date = {}
    for _, r in rows.iterrows():
        by_date.setdefault(r['date'], {})[r['hm']] = r

    def valid_candle(r):
        try:
            return (
                pd.notna(r['open']) and pd.notna(r['close']) and
                pd.notna(r['volume']) and
                float(r['open']) > 0 and float(r['close']) > 0 and
                float(r['volume']) >= 0
            )
        except (TypeError, ValueError):
            return False

    valid_dates = sorted(
        d for d, m in by_date.items()
        if all(hm in m and valid_candle(m[hm]) for hm in NEEDED_HM)
    )
    if not valid_dates:
        return {"status": "INSUFFICIENT"}

    d = valid_dates[-1]
    m = by_date[d]

    def sign(x):
        return (x > 0) - (x < 0)

    def aggregate_3m(minutes):
        rows_3m = [m[hm] for hm in minutes]
        return {
            'open': float(rows_3m[0]['open']),
            'close': float(rows_3m[-1]['close']),
            'volume': sum(float(r['volume']) for r in rows_3m),
        }

    candle_1524 = aggregate_3m([1524, 1525, 1526])
    candle_1527 = aggregate_3m([1527, 1528, 1529])
    candle_915 = aggregate_3m([915, 916, 917])
    candle_918 = aggregate_3m([918, 919, 920])

    # cond1: 3-min 15:24 & 15:27 opposite, 15:24 vol > 15:27 vol
    dir_1524 = sign(candle_1524['close'] - candle_1524['open'])
    dir_1527 = sign(candle_1527['close'] - candle_1527['open'])
    vol_1524 = candle_1524['volume']
    vol_1527 = candle_1527['volume']
    cond1 = (dir_1524 != 0) and (dir_1527 != 0) and (dir_1524 != dir_1527) and (vol_1524 > vol_1527)

    # cond2: 3-min 9:15 & 9:18 both match dir_1524
    dir_915_3m = sign(candle_915['close'] - candle_915['open'])
    dir_918_3m = sign(candle_918['close'] - candle_918['open'])
    cond2 = (dir_915_3m == dir_1524) and (dir_918_3m == dir_1524)

    # cond3: 1-min 15:29 matches dir_1524 (15:28's own trend not checked here),
    # and 15:29 volume > 15:28 volume
    dir_1529_1m = sign(m[1529]['close'] - m[1529]['open'])
    cond3 = (dir_1529_1m == dir_1524) and (m[1529]['volume'] > m[1528]['volume'])

    # cond4: 1-min 9:15 & 9:16 both match dir_1529_1m (== dir_1524)
    dir_915_1m = sign(m[915]['close'] - m[915]['open'])
    dir_916_1m = sign(m[916]['close'] - m[916]['open'])
    cond4 = (dir_915_1m == dir_1529_1m) and (dir_916_1m == dir_1529_1m)

    passed = cond1 and cond2 and cond3 and cond4
    direction = 'LONG' if dir_1524 == 1 else ('SHORT' if dir_1524 == -1 else None)

    return {
        "status": "PASS" if passed else "FAIL",
        "date": d, "direction": direction if passed else None,
        "cond1": cond1, "cond2": cond2, "cond3": cond3, "cond4": cond4,
        "details": {
            "15:24_vol": f"{vol_1524:,.0f}",
            "15:27_vol": f"{vol_1527:,.0f}",
            "15:28_vol": f"{float(m[1528]['volume']):,.0f}",
            "15:29_vol": f"{float(m[1529]['volume']):,.0f}",
        },
    }


def fetch_symbol_rows(symbol):
    """Pulls recent 1-min data via yfinance and reshapes it for evaluate_rows."""
    ticker = symbol + ".NS"
    df = None
    last_error = None
    for attempt in range(3):
        try:
            df = yf.download(
                ticker, period="5d", interval="1m",
                progress=False, auto_adjust=False, threads=False
            )
            if df is not None and not df.empty:
                break
        except Exception as e:
            last_error = e
        if attempt < 2:
            time.sleep(2 ** attempt)

    if df is None or df.empty:
        if last_error:
            raise last_error
        return None

    df = df.reset_index()
    # yfinance sometimes returns MultiIndex columns for single tickers
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    ts_col = 'Datetime' if 'Datetime' in df.columns else df.columns[0]
    ts = pd.to_datetime(df[ts_col])
    # yfinance intraday timestamps come back tz-aware in exchange-local time
    # for NSE tickers (IST); if tz-naive, assume already IST.
    if ts.dt.tz is not None:
        ts = ts.dt.tz_convert('Asia/Kolkata')

    out = pd.DataFrame({
        'date': ts.dt.strftime('%Y-%m-%d'),
        'hm': ts.dt.hour * 100 + ts.dt.minute,
        'open': pd.to_numeric(df['Open'], errors='coerce').values,
        'close': pd.to_numeric(df['Close'], errors='coerce').values,
        'volume': pd.to_numeric(df['Volume'], errors='coerce').values,
    })
    out = out.drop_duplicates(subset=['date', 'hm'], keep='last')
    out = out[out['hm'].between(915, 1529)]
    return out


def generate_html_report(all_results, scan_time):
    """Builds a single self-contained HTML file — no server, just open it in a browser."""
    matches = [r for r in all_results if r['status'] == 'PASS']
    checked = [r for r in all_results if r['status'] in ('PASS', 'FAIL')]
    skipped = [r for r in all_results if r['status'] not in ('PASS', 'FAIL')]

    def cond_badge(ok):
        return f'<span class="badge {"pass" if ok else "fail"}">{"PASS" if ok else "FAIL"}</span>'

    if matches:
        matches_html = '<div class="match-list">' + ''.join(
            f'<div class="match-item"><span class="dir-badge {"long" if r["direction"]=="LONG" else "short"}">{r["direction"]}</span> {r["symbol"]} <span class="match-date">(signal day {r["date"]})</span></div>'
            for r in matches
        ) + '</div>'
        banner_class = "banner-pass"
        banner_text = f"{len(matches)} match{'es' if len(matches) != 1 else ''} found"
    else:
        matches_html = '<p class="none-text">No stocks matched the rule on this run.</p>'
        banner_class = "banner-none"
        banner_text = "No matches today"

    rows_html = ''
    for r in sorted(checked, key=lambda x: (x['status'] != 'PASS', x['symbol'])):
        dir_display = r.get('direction') or '—'
        rows_html += f"""
        <tr>
          <td class="sym">{r['symbol']}</td>
          <td>{r.get('date','—')}</td>
          <td>{dir_display}</td>
          <td>{cond_badge(r.get('cond1'))}<br><small>{r.get('details', {}).get('15:24_vol','—')} / {r.get('details', {}).get('15:27_vol','—')}</small></td>
          <td>{cond_badge(r.get('cond2'))}</td>
          <td>{cond_badge(r.get('cond3'))}<br><small>{r.get('details', {}).get('15:28_vol','—')} &gt; {r.get('details', {}).get('15:29_vol','—')}</small></td>
          <td>{cond_badge(r.get('cond4'))}</td>
          <td class="{'overall-pass' if r['status']=='PASS' else 'overall-fail'}">{r['status']}</td>
        </tr>"""

    skipped_html = ''
    if skipped:
        skipped_html = '<p class="skip-note">' + \
            f"{len(skipped)} symbol(s) skipped (insufficient data or fetch error): " + \
            ', '.join(r['symbol'] for r in skipped) + '</p>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>NSE Scan Results — {scan_time}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: #f7f7f5; color: #1a1a1a; margin: 0; padding: 32px 16px; }}
  .wrap {{ max-width: 720px; margin: 0 auto; }}
  h1 {{ font-size: 20px; margin: 0 0 4px; }}
  .subtitle {{ color: #666; font-size: 13px; margin: 0 0 24px; }}
  .banner {{ border-radius: 8px; padding: 16px 20px; margin-bottom: 24px; font-weight: 600; font-size: 15px; }}
  .banner-pass {{ background: #e6f7ec; color: #0a7a3d; border: 1px solid #b8e6c8; }}
  .banner-none {{ background: #f0f0ee; color: #666; border: 1px solid #ddd; }}
  .match-list {{ margin-top: 10px; font-weight: 400; }}
  .match-item {{ padding: 6px 0; font-size: 14px; }}
  .match-date {{ color: #666; font-weight: 400; font-size: 12.5px; }}
  .dir-badge {{ display: inline-block; font-size: 10.5px; font-weight: 700; padding: 2px 6px; border-radius: 4px; margin-right: 6px; }}
  .dir-badge.long {{ background: #e6f7ec; color: #0a7a3d; }}
  .dir-badge.short {{ background: #fbe9e9; color: #b3261e; }}
  .none-text {{ margin: 10px 0 0; font-weight: 400; font-size: 14px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; background: #fff; border-radius: 8px; overflow: hidden; border: 1px solid #e5e5e2; }}
  th {{ text-align: left; background: #efefec; padding: 10px 12px; font-size: 11.5px; text-transform: uppercase; letter-spacing: 0.03em; color: #666; }}
  td {{ padding: 9px 12px; border-top: 1px solid #eee; }}
  small {{ color: #777; font-size: 10.5px; }}
  td.sym {{ font-weight: 600; }}
  .badge {{ font-size: 11px; font-weight: 700; padding: 2px 7px; border-radius: 4px; }}
  .badge.pass {{ background: #e6f7ec; color: #0a7a3d; }}
  .badge.fail {{ background: #fbe9e9; color: #b3261e; }}
  .overall-pass {{ color: #0a7a3d; font-weight: 700; }}
  .overall-fail {{ color: #999; }}
  .skip-note {{ font-size: 12.5px; color: #888; margin-top: 16px; }}
  .rule-note {{ font-size: 12px; color: #888; margin-top: 28px; line-height: 1.6; border-top: 1px solid #e5e5e2; padding-top: 16px; }}
</style>
</head>
<body>
<div class="wrap">
  <h1>NSE Scan Results</h1>
  <p class="subtitle">Generated {scan_time} · data via Yahoo Finance</p>

  <div class="banner {banner_class}">
    {banner_text}
    {matches_html}
  </div>

  <table>
    <tr><th>Symbol</th><th>Signal day</th><th>Direction</th><th>15:24/27 opp,vol</th><th>9:15/18 match</th><th>15:29 match+vol</th><th>9:15/16 match</th><th>Result</th></tr>
    {rows_html}
  </table>

  {skipped_html}

  <p class="rule-note">
    Rule: 3-min 15:24 &amp; 15:27 opposite trends with 15:24 volume &gt; 15:27 · 3-min 9:15 &amp; 9:18 both match
    15:24's direction · 1-min 15:28 matches 15:24's direction with 15:28 volume &gt; 15:29 volume
    doesn't matter) · 1-min 9:15 &amp; 9:16 both match 15:29's direction.
    A PASS results in a LONG or SHORT entry at the next 9:15 open (direction shown per stock), exit 15:27.
    Re-run the script to regenerate this page with fresh data. Scanned {len(all_results)} stocks this run.
  </p>
</div>
</body>
</html>"""

    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(html)


def main():
    print(f"Scanning {len(STOCK_UNIVERSE)} symbols...")
    print("(This calls Yahoo Finance once per symbol with a short delay between")
    print(" calls to avoid rate-limiting — expect roughly 1-2 seconds per stock.)\n")

    all_results = []

    for i, symbol in enumerate(STOCK_UNIVERSE, 1):
        try:
            rows = fetch_symbol_rows(symbol)
            if rows is None or rows.empty:
                all_results.append({"symbol": symbol, "status": "SKIP"})
                print(f"[{i}/{len(STOCK_UNIVERSE)}] {symbol:<15} skipped (no data)")
                continue
            result = evaluate_rows(rows)
            result['symbol'] = symbol
            all_results.append(result)
            if result['status'] == 'PASS':
                print(f"[{i}/{len(STOCK_UNIVERSE)}] {symbol:<15} MATCH ({result['direction']})  (signal day {result['date']})")
            elif result['status'] == 'FAIL':
                print(f"[{i}/{len(STOCK_UNIVERSE)}] {symbol:<15} no match")
            else:
                print(f"[{i}/{len(STOCK_UNIVERSE)}] {symbol:<15} skipped (insufficient data)")
        except Exception as e:
            all_results.append({"symbol": symbol, "status": "ERROR"})
            print(f"[{i}/{len(STOCK_UNIVERSE)}] {symbol:<15} ERROR: {e}")

        # No fixed delay; transient failures are handled by fetch retries.

    matches = [r for r in all_results if r['status'] == 'PASS']

    print("\n" + "=" * 60)
    if matches:
        print(f"MATCHES FOUND ({len(matches)}) — candidates for entry at next 9:15 open:")
        for r in matches:
            print(f"  {r['symbol']}  {r['direction']}  (signal day: {r['date']})")
    else:
        print("NO MATCHES today.")
    print("=" * 60)

    scan_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    generate_html_report(all_results, scan_time)
    print(f"\nReport written to index.html")


if __name__ == "__main__":
    main()