from datetime import datetime, timedelta
from openchart import NSEData

print("=" * 60)
print("OPENCHART NSE 1-MINUTE DATA TEST")
print("=" * 60)

try:
    # ---------------------------------------------------------
    # 1. Initialize OpenChart
    # ---------------------------------------------------------
    print("\n1. Initializing OpenChart...")

    nse = NSEData()

    print("   OpenChart initialized successfully.")

    # ---------------------------------------------------------
    # 2. Set date range
    # ---------------------------------------------------------
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5)

    print("\n2. Requesting RELIANCE NSE 1-minute data...")
    print(f"   From: {start_date}")
    print(f"   To:   {end_date}")

    # ---------------------------------------------------------
    # 3. Fetch 1-minute NSE data
    # ---------------------------------------------------------
    data = nse.historical(
        "RELIANCE-EQ",
        "EQ",
        start_date,
        end_date,
        "1m"
    )

    # ---------------------------------------------------------
    # 4. Check whether data was returned
    # ---------------------------------------------------------
    if data is None or data.empty:
        print("\n" + "=" * 60)
        print("❌ TEST FAILED")
        print("=" * 60)
        print("OpenChart returned no data.")
        raise SystemExit(1)

    print("\n" + "=" * 60)
    print("✅ DATA RECEIVED")
    print("=" * 60)

    print(f"Number of candles received: {len(data)}")

    print("\nColumns received:")
    print(list(data.columns))

    print("\nLast 20 candles:")
    print(data.tail(20).to_string())

    # ---------------------------------------------------------
    # 5. Make sure timestamp is datetime
    # ---------------------------------------------------------
    data.index = data.index.to_series().apply(
        lambda x: x.to_pydatetime()
        if hasattr(x, "to_pydatetime")
        else x
    )

    # ---------------------------------------------------------
    # 6. Find the latest trading day in the returned data
    # ---------------------------------------------------------
    latest_date = data.index[-1].date()

    print("\n" + "=" * 60)
    print("LATEST TRADING DAY")
    print("=" * 60)
    print(latest_date)

    day_data = data[
        data.index.map(lambda x: x.date() == latest_date)
    ]

    # ---------------------------------------------------------
    # 7. Required candles for your strategy
    # ---------------------------------------------------------
    required_times = [
        "09:15",
        "09:16",
        "09:17",
        "09:18",
        "09:19",
        "09:20",
        "15:24",
        "15:25",
        "15:26",
        "15:27",
        "15:28",
        "15:29",
    ]

    print("\n" + "=" * 60)
    print("CHECKING REQUIRED 1-MINUTE CANDLES")
    print("=" * 60)

    found = 0

    for required_time in required_times:

        matches = day_data[
            day_data.index.map(
                lambda x: x.strftime("%H:%M") == required_time
            )
        ]

        if not matches.empty:

            print(f"\n✅ {required_time} FOUND")

            print(
                matches[
                    ["Open", "High", "Low", "Close", "Volume"]
                ].to_string()
            )

            found += 1

        else:

            print(f"\n❌ {required_time} NOT FOUND")

    # ---------------------------------------------------------
    # 8. Final result
    # ---------------------------------------------------------
    print("\n" + "=" * 60)
    print("FINAL TEST RESULT")
    print("=" * 60)

    print(
        f"Required candles found: "
        f"{found}/{len(required_times)}"
    )

    if found == len(required_times):

        print("\n🎉 TEST SUCCESSFUL")

        print(
            "\nOpenChart successfully returned all "
            "1-minute candles required by your strategy."
        )

        print(
            "\nThis includes the important candles:"
        )

        print("09:15")
        print("09:16")
        print("09:17")
        print("09:18")
        print("09:19")
        print("09:20")
        print("15:24")
        print("15:25")
        print("15:26")
        print("15:27")
        print("15:28")
        print("15:29")

    else:

        print("\n⚠️ TEST INCOMPLETE")

        print(
            "\nOpenChart returned data, but one or more "
            "required timestamps were missing."
        )

        print(
            "\nDo NOT replace Yahoo Finance in your "
            "main scanner yet."
        )

except Exception as e:

    print("\n" + "=" * 60)
    print("❌ TEST FAILED")
    print("=" * 60)

    print("\nError type:")
    print(type(e).__name__)

    print("\nError message:")
    print(str(e))

    raise
