from datetime import datetime, timedelta
from openchart import NSEData

print("=" * 60)
print("OPENCHART NSE DATA TEST")
print("=" * 60)

try:
    print("\n1. Initializing OpenChart...")

    nse = NSEData()

    print("2. Downloading NSE master data...")
    nse.download()

    print("3. Requesting RELIANCE 1-minute data...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=5)

    data = nse.historical(
        symbol="RELIANCE",
        exchange="NSE",
        start=start_date,
        end=end_date,
        interval="1m"
    )

    if data is None or data.empty:
        print("\n❌ TEST FAILED")
        print("No data was returned.")
        raise SystemExit(1)

    print("\n✅ DATA RECEIVED")
    print(f"Rows received: {len(data)}")

    print("\nColumns:")
    print(list(data.columns))

    print("\nLast 20 candles:")
    print(data.tail(20).to_string())

    print("\n" + "=" * 60)
    print("CHECKING REQUIRED TIMES")
    print("=" * 60)

    # Make sure the index is datetime
    data.index = data.index.astype("datetime64[ns]")

    latest_date = data.index[-1].date()

    day_data = data[
        data.index.date == latest_date
    ]

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

    found = 0

    for required_time in required_times:

        matches = day_data[
            day_data.index.strftime("%H:%M") == required_time
        ]

        if not matches.empty:

            print(f"✅ {required_time} FOUND")

            print(
                matches[
                    ["Open", "High", "Low", "Close", "Volume"]
                ].to_string()
            )

            found += 1

        else:

            print(f"❌ {required_time} NOT FOUND")

    print("\n" + "=" * 60)

    print(
        f"Found {found} of "
        f"{len(required_times)} required candles."
    )

    if found == len(required_times):

        print("\n🎉 TEST SUCCESSFUL")
        print(
            "OpenChart is returning all the 1-minute "
            "candles required by your strategy."
        )

    else:

        print("\n⚠️ TEST INCOMPLETE")
        print(
            "OpenChart returned data, but not all "
            "required timestamps were available."
        )

except Exception as e:

    print("\n❌ TEST FAILED")
    print("\nError:")
    print(type(e).__name__, str(e))

    raise
