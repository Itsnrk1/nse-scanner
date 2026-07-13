"""
NSE Daily Scanner — runs automatically on GitHub's servers via GitHub Actions.
================================================================================
It checks each stock's most recently completed trading day against:
  1) 1-min candles 9:15 & 9:16   -> both RED
  2) 3-min candles 9:15 & 9:18   -> both RED
  3) 3-min candles 15:24 & 15:27 -> both GREEN, 15:27 volume > 15:24 volume
  4) 1-min candles 15:28 & 15:29 -> both GREEN, 15:28 volume > 15:29 volume
A stock that passes all four is a candidate for a LONG entry at tomorrow's
9:15 open (exit at 15:27), per your rule.

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

STOCK_UNIVERSE = NIFTY_50 + NIFTY_NEXT_150  # ~200 stocks by default

try:
    from nselib import indices
    df = indices.constituent_stock_list(index_category='BroadMarketIndices',
                                         index_name='Nifty 500')
    fetched = df['Symbol'].tolist()
    if len(fetched) > 100:  # sanity check before trusting it over the fallback
        STOCK_UNIVERSE = fetched
        print(f"Loaded {len(STOCK_UNIVERSE)} symbols live from nselib (Nifty 500)")
    else:
        print(f"nselib returned only {len(fetched)} symbols — using the ~200-stock fallback list instead")
except Exception as e:
    print(f"nselib fetch failed ({e}) — using the ~200-stock fallback list instead")


# ---------------------------------------------------------------------------
# SCAN LOGIC — identical rule to what was verified earlier in this conversation
# ---------------------------------------------------------------------------

NEEDED_HM = [915, 916, 917, 918, 919, 920, 1524, 1525, 1526, 1527, 1528, 1529]


def evaluate_rows(rows):
    """rows: DataFrame with columns date, hm, open, close, volume.
    Finds the latest date with all needed candles present and checks the rule."""
    by_date = {}
    for _, r in rows.iterrows():
        by_date.setdefault(r['date'], {})[r['hm']] = r

    valid_dates = sorted(
        d for d, m in by_date.items() if all(hm in m for hm in NEEDED_HM)
    )
    if not valid_dates:
        return {"status": "INSUFFICIENT"}

    d = valid_dates[-1]
    m = by_date[d]

    cond_a = (m[917]['close'] < m[915]['open']) and (m[920]['close'] < m[918]['open'])

    vol_1524 = m[1524]['volume'] + m[1525]['volume'] + m[1526]['volume']
    vol_1527 = m[1527]['volume'] + m[1528]['volume'] + m[1529]['volume']
    cond_b = (m[1526]['close'] > m[1524]['open']) and \
             (m[1529]['close'] > m[1527]['open']) and \
             (vol_1527 > vol_1524)

    cond_c = (m[1528]['close'] > m[1528]['open']) and \
             (m[1529]['close'] > m[1529]['open']) and \
             (m[1528]['volume'] > m[1529]['volume'])

    cond_d = (m[915]['close'] < m[915]['open']) and (m[916]['close'] < m[916]['open'])

    passed = cond_a and cond_b and cond_c and cond_d
    return {"status": "PASS" if passed else "FAIL",
            "date": d, "condA": cond_a, "condB": cond_b, "condC": cond_c, "condD": cond_d}


def fetch_symbol_rows(symbol):
    """Pulls recent 1-min data via yfinance and reshapes it for evaluate_rows."""
    ticker = symbol + ".NS"
    df = yf.download(ticker, period="5d", interval="1m",
                      progress=False, auto_adjust=False)
    if df.empty:
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
        'open': df['Open'].values,
        'close': df['Close'].values,
        'volume': df['Volume'].values,
    })
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
            f'<div class="match-item">▲ {r["symbol"]} <span class="match-date">(signal day {r["date"]})</span></div>'
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
        rows_html += f"""
        <tr>
          <td class="sym">{r['symbol']}</td>
          <td>{r.get('date','—')}</td>
          <td>{cond_badge(r.get('condD'))}</td>
          <td>{cond_badge(r.get('condA'))}</td>
          <td>{cond_badge(r.get('condB'))}</td>
          <td>{cond_badge(r.get('condC'))}</td>
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
  .none-text {{ margin: 10px 0 0; font-weight: 400; font-size: 14px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; background: #fff; border-radius: 8px; overflow: hidden; border: 1px solid #e5e5e2; }}
  th {{ text-align: left; background: #efefec; padding: 10px 12px; font-size: 11.5px; text-transform: uppercase; letter-spacing: 0.03em; color: #666; }}
  td {{ padding: 9px 12px; border-top: 1px solid #eee; }}
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
    <tr><th>Symbol</th><th>Signal day</th><th>9:15/9:16 red (1m)</th><th>9:15/9:18 red (3m)</th><th>15:24/15:27 vol-up</th><th>15:28/15:29 vol-down</th><th>Result</th></tr>
    {rows_html}
  </table>

  {skipped_html}

  <p class="rule-note">
    Rule: 1-min 9:15 &amp; 9:16 both red · 3-min 9:15 &amp; 9:18 both red · 3-min 15:24 &amp; 15:27 both green with
    15:27 volume &gt; 15:24 · 1-min 15:28 &amp; 15:29 both green with 15:28 volume &gt; 15:29. A PASS is a
    candidate for a long entry at the next 9:15 open, exit 15:27. Re-run the script to regenerate this page
    with fresh data. Scanned {len(all_results)} stocks this run.
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
                print(f"[{i}/{len(STOCK_UNIVERSE)}] {symbol:<15} MATCH  (signal day {result['date']})")
            elif result['status'] == 'FAIL':
                print(f"[{i}/{len(STOCK_UNIVERSE)}] {symbol:<15} no match")
            else:
                print(f"[{i}/{len(STOCK_UNIVERSE)}] {symbol:<15} skipped (insufficient data)")
        except Exception as e:
            all_results.append({"symbol": symbol, "status": "ERROR"})
            print(f"[{i}/{len(STOCK_UNIVERSE)}] {symbol:<15} ERROR: {e}")

        time.sleep(1.2)  # be a reasonable citizen toward Yahoo's servers

    matches = [r for r in all_results if r['status'] == 'PASS']

    print("\n" + "=" * 60)
    if matches:
        print(f"MATCHES FOUND ({len(matches)}) — candidates for LONG entry at next 9:15 open:")
        for r in matches:
            print(f"  {r['symbol']}  (signal day: {r['date']})")
    else:
        print("NO MATCHES today.")
    print("=" * 60)

    scan_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    generate_html_report(all_results, scan_time)
    print(f"\nReport written to index.html")


if __name__ == "__main__":
    main()
