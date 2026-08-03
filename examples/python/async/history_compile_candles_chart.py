import asyncio

import pandas as pd
from lightweight_charts import Chart

from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync


def merge_candles(history_candles, compiled_candles):
    candles = {
        candle["timestamp"]: candle
        for candle in compiled_candles + history_candles
    }

    return [candles[timestamp] for timestamp in sorted(candles)]


def chart_data(candles):
    df = pd.DataFrame(candles)
    df["time"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    return df[["time", "open", "high", "low", "close"]].copy().reset_index(drop=True)


async def get_recent_history(ssid: str) -> list[dict]:
    # The api automatically detects if the 'ssid' is for real or demo account
    async with PocketOptionAsync(ssid) as api:
        await asyncio.sleep(3)

        asset = "EURUSD_otc"
        period = 60
        lookback_period = period * 30

        print(f"Fetching recent history for {asset}...")
        history_candles = await api.history(asset, period)
        compiled_candles = await api.compile_candles(asset, period, lookback_period)
        candles = merge_candles(history_candles, compiled_candles)

        print(f"History candles: {len(history_candles)}")
        print(f"Compiled candles: {len(compiled_candles)}")
        print(f"Merged candles: {len(candles)}")

        return candles


if __name__ == "__main__":
    ssid = input("Please enter your ssid: ")
    candles = asyncio.run(get_recent_history(ssid))

    if candles:
        chart = Chart(width=1400, height=800)
        chart.legend(visible=True)
        chart.time_scale(time_visible=True, seconds_visible=False, min_bar_spacing=0.5)
        chart.set(chart_data(candles))
        chart.show(block=True)
    else:
        print("No candles retrieved.")
