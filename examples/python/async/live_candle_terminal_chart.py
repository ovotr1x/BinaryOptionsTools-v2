"""Live candle terminal: historical backfill + gap-free live streaming.

Fetches the last N hours of closed candles for an asset, then hands off to a
raw tick subscription to keep building candles live — including the one that
was already forming while the historical fetch was in flight.

Why subscribe before fetching history:
    get_candles() only ever returns *closed* candles, so the candle for "right
    now" is never included in it. If you subscribed only after the historical
    fetch returned, any ticks that arrived during that round-trip (and the
    partially-formed current candle) would be lost, leaving a hole between
    the last historical candle and the first live one. Subscribing first and
    buffering ticks until history arrives removes that race entirely.

Usage:
    python live_candle_terminal.py [--asset EURUSD_otc] [--period 60] [--hours 2] [--rows 30]

Prompts for your PocketOption SSID at startup. Set POCKET_OPTION_SSID in the
environment to skip the prompt (useful for scripting/automation).
"""

import argparse
import asyncio
import math
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

import pandas as pd
from lightweight_charts import Chart
from rich.console import Console

from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

console = Console()
PLATFORM_TIME_OFFSET_SECONDS = 7_200
ADVANCED_HISTORY_CANDLES = 120
MIN_VALID_UNIX_TIMESTAMP = 1_000_000_000


def bucket_start(timestamp: int, period: int) -> int:
    return (timestamp // period) * period


def extract_time(candle: Dict) -> Optional[int]:
    """Historical candles have used both 'timestamp' and 'time' keys across
    versions of this library; accept either."""
    raw_value = candle.get("timestamp", candle.get("time"))
    if raw_value is None:
        return None

    try:
        value = int(float(raw_value))
    except (TypeError, ValueError):
        return None

    if value > 10_000_000_000:
        value //= 1000

    value -= PLATFORM_TIME_OFFSET_SECONDS
    return value if value >= MIN_VALID_UNIX_TIMESTAMP else None

def merge_candles(*candle_groups: List[Dict]) -> List[Dict]:
    candles: Dict[int, Dict] = {}
    for group in candle_groups:
        for candle in group:
            timestamp = extract_time(candle)
            if timestamp is not None:
                candles[timestamp] = candle

    return [candles[timestamp] for timestamp in sorted(candles)]



class CandleFeed:
    """Rolling window of closed candles plus one live forming candle."""

    def __init__(self, period: int, max_rows: int):
        self.period = period
        self.max_rows = max_rows
        self.candles: Deque[Dict] = deque(maxlen=max_rows)
        self.forming: Optional[Dict] = None
        self.ticks_seen = 0
        self.last_tick_at: Optional[datetime] = None

    def last_closed_end(self) -> int:
        if not self.candles:
            return 0
        return self.candles[-1]["time"] + self.period

    def seed_history(self, history: List[Dict], oldest_time: int) -> None:
        cutoff = bucket_start(int(time.time()), self.period)
        normalized = [
            (timestamp, candle)
            for candle in history
            if (timestamp := extract_time(candle)) is not None
            and oldest_time <= timestamp < cutoff
        ]
        for timestamp, c in sorted(normalized, key=lambda item: item[0])[-self.max_rows:]:
            self.candles.append(
                {
                    "time": timestamp,
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                }
            )

    def _open_forming(self, timestamp: int, price: float) -> None:
        start = bucket_start(timestamp, self.period)
        self.forming = {"time": start, "open": price, "high": price, "low": price, "close": price}

    def ingest_tick(self, timestamp: int, price: float) -> None:
        if not math.isfinite(price) or price <= 0:
            return

        self.ticks_seen += 1
        self.last_tick_at = datetime.now(timezone.utc)

        if self.forming is None:
            self._open_forming(timestamp, price)
            return

        start = bucket_start(timestamp, self.period)
        if start == self.forming["time"]:
            self.forming["high"] = max(self.forming["high"], price)
            self.forming["low"] = min(self.forming["low"], price)
            self.forming["close"] = price
        elif start > self.forming["time"]:
            self.candles.append(dict(self.forming))
            self._open_forming(timestamp, price)
        # start < forming["time"]: stale/out-of-order tick, ignore

    def replay_backlog(self, buffered_ticks: List[Tuple[int, float]]) -> None:
        """Feed ticks collected while the historical fetch was in flight.
        Anything older than the last historical candle is dropped so it
        doesn't duplicate/rewrite already-closed history.
        """
        cutoff = self.last_closed_end()
        for ts, price in sorted(buffered_ticks):
            if ts < cutoff:
                continue
            self.ingest_tick(ts, price)


def chart_data(feed: CandleFeed) -> pd.DataFrame:
    candles = list(feed.candles)
    if feed.forming is not None:
        candles.append(feed.forming)

    frame = pd.DataFrame(candles)
    if frame.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close"])

    frame["time"] = pd.to_datetime(frame["time"], unit="s", utc=True)
    return frame[["time", "open", "high", "low", "close"]].reset_index(drop=True)


async def run(asset: str, period: int, hours: float, max_rows: int) -> None:
    ssid = os.getenv("POCKET_OPTION_SSID") or console.input("Please enter your SSID: ")
    if not ssid:
        console.print("[red]No SSID provided.[/red]")
        return

    client = PocketOptionAsync(ssid)
    await asyncio.sleep(3)
    await client.wait_for_assets()
    console.print(f"[cyan]Connected ({'demo' if client.is_demo() else 'real'} account).[/cyan]")

    feed = CandleFeed(period, max_rows)

    # 1. Subscribe to raw ticks FIRST and buffer them, so nothing is lost while
    #    the historical fetch below is in flight.
    tick_buffer: List[Tuple[int, float]] = []
    buffering = True
    stream = await client.subscribe_symbol(asset)

    async def tick_reader() -> None:
        nonlocal buffering
        async for tick in stream:
            raw_timestamp = tick.get("timestamp", tick.get("time"))
            raw_price = tick.get("close", tick.get("price"))
            if raw_timestamp is None or raw_price is None:
                continue

            try:
                ts = extract_time(tick)
                price = float(raw_price)
            except (TypeError, ValueError):
                continue
            if ts is None:
                continue

            if not math.isfinite(price) or price <= 0:
                continue

            if buffering:
                tick_buffer.append((ts, price))
            else:
                feed.ingest_tick(ts, price)

    reader_task = asyncio.create_task(tick_reader())
    chart = None

    try:
        # 2. Use the oldest recent-history candle as the bulk-history boundary.
        #    The shared boundary candle is fetched by both sources and deduped.
        offset_seconds = int(hours * 3600)
        recent_candles = await client.history(asset, period)
        recent_timestamps = [
            timestamp
            for candle in recent_candles
            if (timestamp := extract_time(candle)) is not None
        ]
        advanced_end_time = (
            min(recent_timestamps)
            if recent_timestamps
            else int(time.time())
        )
        advanced_offset_seconds = period * ADVANCED_HISTORY_CANDLES
        advanced_candles = await client.get_candles_advanced(
            asset,
            period,
            advanced_offset_seconds,
            advanced_end_time + PLATFORM_TIME_OFFSET_SECONDS,
        )
        compiled_candles = await client.compile_candles(
            asset,
            period,
            offset_seconds,
        )
        history = merge_candles(
            compiled_candles,
            recent_candles,
            advanced_candles,
        )
        oldest_time = advanced_end_time - advanced_offset_seconds - period
        feed.seed_history(history, oldest_time)

        # 3. Replay whatever ticks piled up during the fetch, then switch the
        #    reader over to live ingestion directly. No gap.
        feed.replay_backlog(tick_buffer)
        buffering = False

        chart = Chart(width=1400, height=800)
        chart.legend(visible=True)
        chart.watermark(f"{asset} — {period}s candles")
        chart.time_scale(
            time_visible=True,
            seconds_visible=period < 60,
            min_bar_spacing=0.5,
        )
        initial_data = chart_data(feed)
        if not initial_data.empty:
            chart.set(initial_data)
        async def update_chart() -> None:
            while chart.is_alive:
                await asyncio.sleep(0.25)
                data = chart_data(feed)
                if not data.empty:
                    chart.update(data.iloc[-1])

        console.print(f"[green]Loaded {len(feed.candles)} closed candles, streaming live...[/green]")
        await asyncio.gather(chart.show_async(), update_chart())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        reader_task.cancel()
        try:
            await reader_task
        except asyncio.CancelledError:
            pass
        if chart is not None:
            chart.exit()
        await client.unsubscribe(asset)
        await client.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live candle terminal with gap-free history-to-stream handoff")
    parser.add_argument("--asset", default="EURUSD_otc")
    parser.add_argument("--period", type=int, default=60, help="Candle length in seconds")
    parser.add_argument("--hours", type=float, default=2.0, help="Hours of history to backfill")
    parser.add_argument("--rows", type=int, default=150, help="Max closed candles to display")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.asset, args.period, args.hours, args.rows))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()