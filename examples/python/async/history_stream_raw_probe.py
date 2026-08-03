import asyncio

from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync


async def print_list(name, values, limit):
    print(f"\n== {name} ==")
    print(f"count: {len(values)}")

    for index, value in enumerate(values[:limit], start=1):
        print(f"{index}: {value}")


async def print_stream(name, stream, limit, timeout_seconds=20):
    print(f"\n== {name} ==")

    for index in range(1, limit + 1):
        try:
            value = await asyncio.wait_for(anext(stream), timeout=timeout_seconds)
        except TimeoutError:
            print(f"timed out after {timeout_seconds}s waiting for next value")
            return

        print(f"{index}: {value}")


# Main part of the code
async def main(ssid: str):
    # The api automatically detects if the 'ssid' is for real or demo account
    async with PocketOptionAsync(ssid) as api:
        await asyncio.sleep(3)

        asset = "EURUSD_otc"
        period = 5
        limit = 5

        points = await api.history_points(asset, period)
        await print_list("history_points", points, limit)

        candles = await api.history_ohlc(asset, period)
        await print_list("history_ohlc", candles, limit)

        history_candle_stream = await api.subscribe_with_history_mode(asset, period, "ohlc")
        await print_stream("subscribe_with_history_mode ohlc", history_candle_stream, limit)
        del history_candle_stream

        history_point_stream = await api.subscribe_with_history_mode(asset, period, "points")
        await print_stream("subscribe_with_history_mode points", history_point_stream, limit)
        del history_point_stream

        point_stream = await api.subscribe_points(asset)
        await print_stream("subscribe_points", point_stream, limit)


if __name__ == "__main__":
    ssid = input("Please enter your ssid: ")
    asyncio.run(main(ssid))
