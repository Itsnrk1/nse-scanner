# =============================================================================
# NSE DAILY SCANNER — FAST VERSION
# =============================================================================
# Strategy logic:
#
# 1) 3-min 15:24 and 15:27 must be SAME TREND
#    15:27 volume > 15:24 volume
#
# 2) 3-min 9:15 and 9:18 must BOTH match 15:27 direction
#
# 3) 1-min 15:28 must match 3-min 15:27 direction
#    15:28 volume > 15:29 volume
#
# 4) 1-min 9:15 and 9:16 must BOTH match 1-min 15:28 direction
#
# 5) Final LONG/SHORT direction is determined by 3-min 15:27
#
# Entry: Next trading day at 9:15 open
# Exit: 15:27
#
# Scanner uses parallel workers for faster scanning.
# =============================================================================


# =============================================================================
# SETUP
# =============================================================================

# pip install yfinance pandas nselib requests

import time
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd


try:
    import yfinance as yf

except ImportError:

    print(
        "Missing dependency. Run: "
        "pip install yfinance pandas --break-system-packages"
    )

    sys.exit(1)


# =============================================================================
# STOCK UNIVERSE
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


STOCK_UNIVERSE = NIFTY_50 + NIFTY_NEXT_150


# =============================================================================
# TRY LIVE NIFTY 500 VIA NSELIB
# =============================================================================

try:

    from nselib import indices

    df = indices.constituent_stock_list(
        index_category="BroadMarketIndices",
        index_name="Nifty 500"
    )

    fetched = df["Symbol"].tolist()

    if len(fetched) > 100:

        STOCK_UNIVERSE = fetched

        print(
            f"Loaded {len(STOCK_UNIVERSE)} symbols from "
            f"nselib (Nifty 500)"
        )

except Exception as e:

    print(
        f"nselib Nifty 500 fetch failed ({e}) — "
        f"keeping current {len(STOCK_UNIVERSE)}-stock list"
    )


# =============================================================================
# TRY NSE FULL OFFICIAL EQUITY LIST
# =============================================================================

try:

    import requests

    headers = {
        "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36",

        "Accept":
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    session = requests.Session()

    session.headers.update(headers)

    session.get(
        "https://www.nseindia.com",
        timeout=20
    )

    resp = session.get(
        "https://nsearchives.nseindia.com/content/equities/sec_list.csv",
        timeout=30
    )

    resp.raise_for_status()

    from io import StringIO

    full_df = pd.read_csv(
        StringIO(resp.text)
    )

    symbol_col = next(
        (
            c
            for c in full_df.columns
            if "symbol" in c.lower()
        ),
        None
    )

    if symbol_col is None:

        raise ValueError(
            f"No symbol-like column found. "
            f"Columns present: {list(full_df.columns)}"
        )


    series_col = next(
        (
            c
            for c in full_df.columns
            if "series" in c.lower()
        ),
        None
    )


    if series_col is not None:

        before = len(full_df)

        full_df = full_df[
            full_df[series_col]
            .astype(str)
            .str.strip()
            == "EQ"
        ]

        print(
            f"Filtered to EQ series: "
            f"{before} -> {len(full_df)} rows"
        )


    name_col = next(
        (
            c
            for c in full_df.columns
            if "name" in c.lower()
        ),
        None
    )


    NON_STOCK_KEYWORDS = [

        "ETF",
        "LIQUID",
        "GILT",
        "BEES",
        "FUND",
        "INDEX FUND",
        "EXCHANGE TRADED",
        "MUTUAL FUND",
        "NIFTY 50",
        "NIFTY50 ",
        "NIFTYIETF",
        "BETA",
        "MOMENTUM",
        "ALPHA",
        "QUALITY30",

    ]


    if name_col is not None:

        before = len(full_df)

        name_upper = (
            full_df[name_col]
            .astype(str)
            .str.upper()
        )

        mask = ~name_upper.str.contains(
            "|".join(NON_STOCK_KEYWORDS),
            na=False
        )

        full_df = full_df[mask]

        print(
            f"Filtered out ETF/fund-like names: "
            f"{before} -> {len(full_df)} rows"
        )


    symbol_upper = (
        full_df[symbol_col]
        .astype(str)
        .str.upper()
    )

    sym_mask = ~symbol_upper.str.contains(
        "ETF|IETF|BEES|LIQUID|GILT",
        na=False
    )

    before = len(full_df)

    full_df = full_df[sym_mask]

    if len(full_df) != before:

        print(
            f"Filtered out ETF-like symbol patterns: "
            f"{before} -> {len(full_df)} rows"
        )


    full_list = (
        full_df[symbol_col]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )


    if len(full_list) > len(STOCK_UNIVERSE):

        STOCK_UNIVERSE = full_list

        print(
            f"Loaded {len(STOCK_UNIVERSE)} symbols from "
            f"NSE's full equity list "
            f"(column: {symbol_col}) — "
            f"using this broadest option"
        )

    else:

        print(
            f"NSE full list returned {len(full_list)} symbols, "
            f"not larger than current {len(STOCK_UNIVERSE)} — "
            f"keeping current list"
        )


except Exception as e:

    print(
        f"NSE full equity list fetch failed ({e}) — "
        f"keeping current {len(STOCK_UNIVERSE)}-stock list"
    )


print(
    f"FINAL universe size: "
    f"{len(STOCK_UNIVERSE)} stocks"
)


# =============================================================================
# SCAN LOGIC
# =============================================================================

NEEDED_HM = [

    915,
    916,
    917,
    918,
    919,
    920,

    1524,
    1525,
    1526,
    1527,
    1528,
    1529,

]


def evaluate_rows(rows):

    """
    Evaluate the latest completed trading day.

    FINAL CONDITIONS:

    1. 3-min 15:24 and 15:27 SAME TREND.
       15:27 volume > 15:24 volume.

    2. 3-min 9:15 and 9:18 BOTH match
       3-min 15:27 direction.

    3. 1-min 15:28 matches
       3-min 15:27 direction.
       15:28 volume > 15:29 volume.

    4. 1-min 9:15 and 9:16 BOTH match
       1-min 15:28 direction.

    5. Final LONG/SHORT direction comes
       from 3-min 15:27.
    """


    # -------------------------------------------------------------------------
    # GROUP DATA BY DATE
    # -------------------------------------------------------------------------

    by_date = {}


    for _, r in rows.iterrows():

        by_date.setdefault(
            r["date"],
            {}
        )[r["hm"]] = r


    # -------------------------------------------------------------------------
    # VALIDATE CANDLES
    # -------------------------------------------------------------------------

    def valid_candle(r):

        try:

            return (

                pd.notna(r["open"])

                and

                pd.notna(r["close"])

                and

                pd.notna(r["volume"])

                and

                float(r["open"]) > 0

                and

                float(r["close"]) > 0

                and

                float(r["volume"]) >= 0

            )

        except (
            TypeError,
            ValueError
        ):

            return False


    # -------------------------------------------------------------------------
    # FIND LATEST DATE WITH ALL REQUIRED CANDLES
    # -------------------------------------------------------------------------

    valid_dates = sorted(

        d

        for d, m in by_date.items()

        if all(

            hm in m
            and
            valid_candle(m[hm])

            for hm in NEEDED_HM

        )

    )


    if not valid_dates:

        return {
            "status": "INSUFFICIENT"
        }


    d = valid_dates[-1]

    m = by_date[d]


    # -------------------------------------------------------------------------
    # DIRECTION FUNCTION
    # -------------------------------------------------------------------------

    def sign(x):

        return (
            x > 0
        ) - (
            x < 0
        )


    # -------------------------------------------------------------------------
    # BUILD 3-MIN CANDLES
    # -------------------------------------------------------------------------

    def aggregate_3m(minutes):

        rows_3m = [
            m[hm]
            for hm in minutes
        ]


        return {

            "open":
                float(
                    rows_3m[0]["open"]
                ),

            "close":
                float(
                    rows_3m[-1]["close"]
                ),

            "volume":
                sum(
                    float(r["volume"])
                    for r in rows_3m
                ),

        }


    candle_1524 = aggregate_3m(
        [
            1524,
            1525,
            1526
        ]
    )


    candle_1527 = aggregate_3m(
        [
            1527,
            1528,
            1529
        ]
    )


    candle_915 = aggregate_3m(
        [
            915,
            916,
            917
        ]
    )


    candle_918 = aggregate_3m(
        [
            918,
            919,
            920
        ]
    )


    # =========================================================================
    # DIRECTIONS
    # =========================================================================

    dir_1524 = sign(

        candle_1524["close"]
        -
        candle_1524["open"]

    )


    dir_1527 = sign(

        candle_1527["close"]
        -
        candle_1527["open"]

    )


    # =========================================================================
    # CONDITION 1
    #
    # 3-MIN 15:24 AND 15:27 MUST BE SAME TREND
    #
    # 15:27 VOLUME MUST BE GREATER THAN 15:24
    # =========================================================================

    vol_1524 = (
        candle_1524["volume"]
    )

    vol_1527 = (
        candle_1527["volume"]
    )


    cond1 = (

        dir_1524 != 0

        and

        dir_1527 != 0

        and

        dir_1527 == dir_1524

        and

        vol_1527 > vol_1524

    )


    # =========================================================================
    # CONDITION 2
    #
    # 3-MIN 9:15 AND 9:18 MUST BOTH
    # MATCH 15:27 DIRECTION
    # =========================================================================

    dir_915_3m = sign(

        candle_915["close"]
        -
        candle_915["open"]

    )


    dir_918_3m = sign(

        candle_918["close"]
        -
        candle_918["open"]

    )


    cond2 = (

        dir_1527 != 0

        and

        dir_915_3m == dir_1527

        and

        dir_918_3m == dir_1527

    )


    # =========================================================================
    # CONDITION 3
    #
    # 1-MIN 15:28 MUST MATCH 3-MIN 15:27
    #
    # 15:28 VOLUME > 15:29 VOLUME
    # =========================================================================

    dir_1528_1m = sign(

        m[1528]["close"]
        -
        m[1528]["open"]

    )


    dir_1529_1m = sign(

        m[1529]["close"]
        -
        m[1529]["open"]

    )


    cond3 = (

        dir_1528_1m != 0

        and

        dir_1528_1m == dir_1527

        and

        m[1528]["volume"]
        >
        m[1529]["volume"]

    )


    # =========================================================================
    # CONDITION 4
    #
    # 1-MIN 9:15 AND 9:16 MUST BOTH
    # MATCH 1-MIN 15:28
    # =========================================================================

    dir_915_1m = sign(

        m[915]["close"]
        -
        m[915]["open"]

    )


    dir_916_1m = sign(

        m[916]["close"]
        -
        m[916]["open"]

    )


    cond4 = (

        dir_1528_1m != 0

        and

        dir_915_1m == dir_1528_1m

        and

        dir_916_1m == dir_1528_1m

    )


    # =========================================================================
    # FINAL RESULT
    # =========================================================================

    passed = (

        cond1

        and

        cond2

        and

        cond3

        and

        cond4

    )


    # =========================================================================
    # FINAL DIRECTION
    #
    # DIRECTION IS DETERMINED BY 3-MIN 15:27
    # =========================================================================

    direction = (

        "LONG"

        if dir_1527 == 1

        else

        (

            "SHORT"

            if dir_1527 == -1

            else None

        )

    )


    return {

        "status":
            "PASS"
            if passed
            else
            "FAIL",

        "date":
            d,

        "direction":
            direction
            if passed
            else
            None,

        "cond1":
            cond1,

        "cond2":
            cond2,

        "cond3":
            cond3,

        "cond4":
            cond4,

        "details": {

            "15:24_vol":
                f"{vol_1524:,.0f}",

            "15:27_vol":
                f"{vol_1527:,.0f}",

            "15:28_vol":
                f"{float(m[1528]['volume']):,.0f}",

            "15:29_vol":
                f"{float(m[1529]['volume']):,.0f}",

        },

    }


# =============================================================================
# FETCH DATA
# =============================================================================

def fetch_symbol_rows(symbol):

    ticker = (
        symbol
        +
        ".NS"
    )


    df = None

    last_error = None


    # -------------------------------------------------------------------------
    # RETRY UP TO 3 TIMES
    # -------------------------------------------------------------------------

    for attempt in range(3):

        try:

            df = yf.download(

                ticker,

                period="5d",

                interval="1m",

                progress=False,

                auto_adjust=False,

                threads=False

            )


            if (
                df is not None
                and
                not df.empty
            ):

                break


        except Exception as e:

            last_error = e


        if attempt < 2:

            time.sleep(
                2 ** attempt
            )


    # -------------------------------------------------------------------------
    # NO DATA
    # -------------------------------------------------------------------------

    if (
        df is None
        or
        df.empty
    ):

        if last_error:

            raise last_error

        return None


    # -------------------------------------------------------------------------
    # RESET INDEX
    # -------------------------------------------------------------------------

    df = df.reset_index()


    # -------------------------------------------------------------------------
    # HANDLE MULTIINDEX
    # -------------------------------------------------------------------------

    if isinstance(
        df.columns,
        pd.MultiIndex
    ):

        df.columns = [
            c[0]
            for c in df.columns
        ]


    # -------------------------------------------------------------------------
    # FIND TIMESTAMP COLUMN
    # -------------------------------------------------------------------------

    ts_col = (

        "Datetime"

        if "Datetime"
        in df.columns

        else

        df.columns[0]

    )


    ts = pd.to_datetime(
        df[ts_col]
    )


    # -------------------------------------------------------------------------
    # CONVERT TO INDIA TIME
    # -------------------------------------------------------------------------

    if ts.dt.tz is not None:

        ts = ts.dt.tz_convert(
            "Asia/Kolkata"
        )


    # -------------------------------------------------------------------------
    # CREATE CLEAN DATAFRAME
    # -------------------------------------------------------------------------

    out = pd.DataFrame({

        "date":
            ts.dt.strftime(
                "%Y-%m-%d"
            ),

        "hm":
            (
                ts.dt.hour
                *
                100
                +
                ts.dt.minute
            ),

        "open":
            pd.to_numeric(
                df["Open"],
                errors="coerce"
            ).values,

        "close":
            pd.to_numeric(
                df["Close"],
                errors="coerce"
            ).values,

        "volume":
            pd.to_numeric(
                df["Volume"],
                errors="coerce"
            ).values,

    })


    # -------------------------------------------------------------------------
    # REMOVE DUPLICATES
    # -------------------------------------------------------------------------

    out = out.drop_duplicates(

        subset=[
            "date",
            "hm"
        ],

        keep="last"

    )


    # -------------------------------------------------------------------------
    # KEEP MARKET HOURS
    # -------------------------------------------------------------------------

    out = out[
        out["hm"].between(
            915,
            1529
        )
    ]


    return out


# =============================================================================
# FAST PARALLEL SCANNER
# =============================================================================

# Number of stocks fetched simultaneously.
#
# 10 is a reasonable starting point.
#
# If Yahoo starts rate-limiting:
#     reduce to 6 or 8.
#
# If everything is stable:
#     you can test 12.
#
MAX_WORKERS = 10


def scan_one_symbol(symbol):

    try:

        rows = fetch_symbol_rows(
            symbol
        )


        if (
            rows is None
            or
            rows.empty
        ):

            return {

                "symbol":
                    symbol,

                "status":
                    "SKIP"

            }


        result = evaluate_rows(
            rows
        )


        result["symbol"] = (
            symbol
        )


        return result


    except Exception as e:

        return {

            "symbol":
                symbol,

            "status":
                "ERROR",

            "error":
                str(e)

        }


# =============================================================================
# SCAN ALL STOCKS IN PARALLEL
# =============================================================================

def scan_all_symbols(symbols):

    all_results = []

    total = len(
        symbols
    )

    completed = 0


    print(

        f"Scanning {total} symbols "
        f"with {MAX_WORKERS} "
        f"parallel workers...\n"

    )


    with ThreadPoolExecutor(

        max_workers=MAX_WORKERS

    ) as executor:


        future_to_symbol = {

            executor.submit(
                scan_one_symbol,
                symbol
            ):
                symbol

            for symbol in symbols

        }


        # ---------------------------------------------------------------------
        # PROCESS RESULTS AS THEY FINISH
        # ---------------------------------------------------------------------

        for future in as_completed(
            future_to_symbol
        ):

            symbol = (
                future_to_symbol[future]
            )


            try:

                result = (
                    future.result()
                )


            except Exception as e:

                result = {

                    "symbol":
                        symbol,

                    "status":
                        "ERROR",

                    "error":
                        str(e)

                }


            all_results.append(
                result
            )


            completed += 1


            # -----------------------------------------------------------------
            # MATCH
            # -----------------------------------------------------------------

            if result["status"] == "PASS":

                print(

                    f"[{completed}/{total}] "
                    f"{symbol:<15} "
                    f"MATCH "
                    f"({result['direction']}) "
                    f"(signal day "
                    f"{result['date']})"

                )


            # -----------------------------------------------------------------
            # ERROR
            # -----------------------------------------------------------------

            elif result["status"] == "ERROR":

                print(

                    f"[{completed}/{total}] "
                    f"{symbol:<15} "
                    f"ERROR: "
                    f"{result.get('error', 'unknown error')}"

                )


            # -----------------------------------------------------------------
            # SKIP
            # -----------------------------------------------------------------

            elif result["status"] == "SKIP":

                print(

                    f"[{completed}/{total}] "
                    f"{symbol:<15} "
                    f"skipped (no data)"

                )


            # -----------------------------------------------------------------
            # INSUFFICIENT DATA
            # -----------------------------------------------------------------

            elif result["status"] == "INSUFFICIENT":

                print(

                    f"[{completed}/{total}] "
                    f"{symbol:<15} "
                    f"skipped "
                    f"(insufficient data)"

                )


            # -----------------------------------------------------------------
            # FAIL
            # -----------------------------------------------------------------

            else:

                print(

                    f"[{completed}/{total}] "
                    f"{symbol:<15} "
                    f"no match"

                )


    # -------------------------------------------------------------------------
    # SORT RESULTS
    # -------------------------------------------------------------------------

    all_results.sort(

        key=lambda x:
            x.get(
                "symbol",
                ""
            )

    )


    return all_results


# =============================================================================
# HTML REPORT
# =============================================================================

def generate_html_report(
    all_results,
    scan_time
):

    matches = [

        r

        for r in all_results

        if r["status"] == "PASS"

    ]


    checked = [

        r

        for r in all_results

        if r["status"]
        in (
            "PASS",
            "FAIL"
        )

    ]


    skipped = [

        r

        for r in all_results

        if r["status"]
        not in (
            "PASS",
            "FAIL"
        )

    ]


    def cond_badge(ok):

        return (

            f'<span class="badge '
            f'{"pass" if ok else "fail"}">'
            f'{"PASS" if ok else "FAIL"}'
            f'</span>'

        )


    # =========================================================================
    # MATCH BANNER
    # =========================================================================

    if matches:

        matches_html = (

            '<div class="match-list">'

            +

            "".join(

                f'<div class="match-item">'

                f'<span class="dir-badge '
                f'{"long" if r["direction"] == "LONG" else "short"}">'

                f'{r["direction"]}'

                f'</span> '

                f'{r["symbol"]} '

                f'<span class="match-date">'

                f'(signal day {r["date"]})'

                f'</span>'

                f'</div>'

                for r in matches

            )

            +

            "</div>"

        )


        banner_class = (
            "banner-pass"
        )


        banner_text = (

            f"{len(matches)} match"

            f"{'es' if len(matches) != 1 else ''} found"

        )


    else:

        matches_html = (

            '<p class="none-text">'

            "No stocks matched the rule on this run."

            "</p>"

        )


        banner_class = (
            "banner-none"
        )


        banner_text = (
            "No matches today"
        )


    # =========================================================================
    # TABLE
    # =========================================================================

    rows_html = ""


    for r in sorted(

        checked,

        key=lambda x: (

            x["status"] != "PASS",

            x["symbol"]

        )

    ):


        dir_display = (

            r.get(
                "direction"
            )

            or

            "—"

        )


        rows_html += f"""

        <tr>

          <td class="sym">
            {r['symbol']}
          </td>

          <td>
            {r.get('date', '—')}
          </td>

          <td>
            {dir_display}
          </td>

          <td>
            {cond_badge(r.get('cond1'))}<br>

            <small>

              15:24:
              {r.get('details', {}).get('15:24_vol', '—')}

              &nbsp; / &nbsp;

              15:27:
              {r.get('details', {}).get('15:27_vol', '—')}

            </small>

          </td>

          <td>
            {cond_badge(r.get('cond2'))}
          </td>

          <td>
            {cond_badge(r.get('cond3'))}<br>

            <small>

              15:28:
              {r.get('details', {}).get('15:28_vol', '—')}

              &gt;

              15:29:
              {r.get('details', {}).get('15:29_vol', '—')}

            </small>

          </td>

          <td>
            {cond_badge(r.get('cond4'))}
          </td>

          <td class="{
              'overall-pass'
              if r['status'] == 'PASS'
              else 'overall-fail'
          }">

            {r['status']}

          </td>

        </tr>

        """


    # =========================================================================
    # SKIPPED
    # =========================================================================

    skipped_html = ""


    if skipped:

        skipped_html = (

            '<p class="skip-note">'

            +

            f"{len(skipped)} symbol(s) skipped "
            f"(insufficient data or fetch error): "

            +

            ", ".join(

                r["symbol"]

                for r in skipped

            )

            +

            "</p>"

        )


    # =========================================================================
    # HTML
    # =========================================================================

    html = f"""<!DOCTYPE html>

<html lang="en">

<head>

<meta charset="UTF-8">

<title>
NSE Scan Results — {scan_time}
</title>


<style>

body {{

    font-family:
        -apple-system,
        Segoe UI,
        Roboto,
        Arial,
        sans-serif;

    background:
        #f7f7f5;

    color:
        #1a1a1a;

    margin:
        0;

    padding:
        32px 16px;

}}


.wrap {{

    max-width:
        720px;

    margin:
        0 auto;

}}


h1 {{

    font-size:
        20px;

    margin:
        0 0 4px;

}}


.subtitle {{

    color:
        #666;

    font-size:
        13px;

    margin:
        0 0 24px;

}}


.banner {{

    border-radius:
        8px;

    padding:
        16px 20px;

    margin-bottom:
        24px;

    font-weight:
        600;

    font-size:
        15px;

}}


.banner-pass {{

    background:
        #e6f7ec;

    color:
        #0a7a3d;

    border:
        1px solid #b8e6c8;

}}


.banner-none {{

    background:
        #f0f0ee;

    color:
        #666;

    border:
        1px solid #ddd;

}}


.match-list {{

    margin-top:
        10px;

    font-weight:
        400;

}}


.match-item {{

    padding:
        6px 0;

    font-size:
        14px;

}}


.match-date {{

    color:
        #666;

    font-weight:
        400;

    font-size:
        12.5px;

}}


.dir-badge {{

    display:
        inline-block;

    font-size:
        10.5px;

    font-weight:
        700;

    padding:
        2px 6px;

    border-radius:
        4px;

    margin-right:
        6px;

}}


.dir-badge.long {{

    background:
        #e6f7ec;

    color:
        #0a7a3d;

}}


.dir-badge.short {{

    background:
        #fbe9e9;

    color:
        #b3261e;

}}


.none-text {{

    margin:
        10px 0 0;

    font-weight:
        400;

    font-size:
        14px;

}}


table {{

    width:
        100%;

    border-collapse:
        collapse;

    font-size:
        13px;

    background:
        #fff;

    border-radius:
        8px;

    overflow:
        hidden;

    border:
        1px solid #e5e5e2;

}}


th {{

    text-align:
        left;

    background:
        #efefec;

    padding:
        10px 12px;

    font-size:
        11.5px;

    text-transform:
        uppercase;

    letter-spacing:
        0.03em;

    color:
        #666;

}}


td {{

    padding:
        9px 12px;

    border-top:
        1px solid #eee;

}}


small {{

    color:
        #777;

    font-size:
        10.5px;

}}


td.sym {{

    font-weight:
        600;

}}


.badge {{

    font-size:
        11px;

    font-weight:
        700;

    padding:
        2px 7px;

    border-radius:
        4px;

}}


.badge.pass {{

    background:
        #e6f7ec;

    color:
        #0a7a3d;

}}


.badge.fail {{

    background:
        #fbe9e9;

    color:
        #b3261e;

}}


.overall-pass {{

    color:
        #0a7a3d;

    font-weight:
        700;

}}


.overall-fail {{

    color:
        #999;

}}


.skip-note {{

    font-size:
        12.5px;

    color:
        #888;

    margin-top:
        16px;

}}


.rule-note {{

    font-size:
        12px;

    color:
        #888;

    margin-top:
        28px;

    line-height:
        1.6;

    border-top:
        1px solid #e5e5e2;

    padding-top:
        16px;

}}

</style>

</head>


<body>


<div class="wrap">


<h1>
NSE Scan Results
</h1>


<p class="subtitle">

Generated {scan_time}
· data via Yahoo Finance

</p>


<div class="banner {banner_class}">

{banner_text}

{matches_html}

</div>


<table>


<tr>

<th>
Symbol
</th>

<th>
Signal day
</th>

<th>
Direction
</th>

<th>
15:24/15:27
</th>

<th>
3-min morning
</th>

<th>
15:28 match + vol
</th>

<th>
1-min morning
</th>

<th>
Result
</th>

</tr>


{rows_html}


</table>


{skipped_html}


<p class="rule-note">


<b>
Current scanner rule:
</b>


<br><br>


3-min 15:24 and 15:27
must be in the same trend.


<br>


15:27 volume must be greater
than 15:24 volume.


<br><br>


3-min 9:15 and 9:18
must both match 15:27 direction.


<br><br>


1-min 15:28 must match
3-min 15:27 direction.


<br>


1-min 15:28 volume must be
greater than 15:29 volume.


<br><br>


1-min 9:15 and 9:16
must both match 15:28 direction.


<br><br>


<b>
Final direction:
</b>

3-min 15:27.


<br><br>


<b>
Entry:
</b>

Next trading day at 9:15 open.


<br>


<b>
Exit:
</b>

15:27.


<br><br>


<b>
Parallel workers:
</b>

{MAX_WORKERS}


<br><br>


Scanned
{len(all_results)}
stocks this run.


</p>


</div>


</body>


</html>
"""


    # =========================================================================
    # WRITE HTML
    # =========================================================================

    with open(

        "index.html",

        "w",

        encoding="utf-8"

    ) as f:

        f.write(
            html
        )


# =============================================================================
# MAIN
# =============================================================================

def main():

    # -------------------------------------------------------------------------
    # SCAN
    # -------------------------------------------------------------------------

    all_results = scan_all_symbols(
        STOCK_UNIVERSE
    )


    # -------------------------------------------------------------------------
    # FIND MATCHES
    # -------------------------------------------------------------------------

    matches = [

        r

        for r in all_results

        if r["status"] == "PASS"

    ]


    print(
        "\n"
        +
        "=" * 60
    )


    # -------------------------------------------------------------------------
    # PRINT MATCHES
    # -------------------------------------------------------------------------

    if matches:

        print(

            f"MATCHES FOUND "
            f"({len(matches)}) "
            f"— candidates for entry "
            f"at next 9:15 open:"

        )


        for r in matches:

            print(

                f"  {r['symbol']}  "
                f"{r['direction']}  "
                f"(signal day: "
                f"{r['date']})"

            )


    else:

        print(
            "NO MATCHES today."
        )


    print(
        "=" * 60
    )


    # -------------------------------------------------------------------------
    # GENERATE REPORT
    # -------------------------------------------------------------------------

    scan_time = (
        datetime.now()
        .strftime(
            "%Y-%m-%d %H:%M"
        )
    )


    generate_html_report(

        all_results,

        scan_time

    )


    print(
        "\nReport written to index.html"
    )


# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":

    main()
