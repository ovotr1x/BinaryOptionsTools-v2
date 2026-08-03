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
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

from rich.console import Console
from rich.live import Live
from rich.table import Table

from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

console = Console()
PLATFORM_TIME_OFFSET_SECONDS = 7_200


def bucket_start(timestamp: int, period: int) -> int:
    return (timestamp // period) * period


def extract_time(candle: Dict) -> int:
    """Historical candles have used both 'timestamp' and 'time' keys across
    versions of this library; accept either."""
    value = int(float(candle.get("timestamp", candle.get("time", 0))))
    if value > 10_000_000_000:
        value //= 1000
    return value - PLATFORM_TIME_OFFSET_SECONDS

def merge_candles(*candle_groups: List[Dict]) -> List[Dict]:
    candles: Dict[int, Dict] = {}
    for group in candle_groups:
        for candle in group:
            candles[extract_time(candle)] = candle

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

    def seed_history(self, history: List[Dict]) -> None:
        cutoff = bucket_start(int(time.time()), self.period)
        ordered = sorted(
            (c for c in history if extract_time(c) < cutoff),
            key=extract_time,
        )
        for c in ordered[-self.max_rows:]:
            self.candles.append(
                {
                    "time": extract_time(c),
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


def render(feed: CandleFeed, asset: str, period: int, hours: float) -> Table:
    table = Table(title=f"{asset} — {period}s candles (last {hours}h + live)")
    table.add_column("Time (UTC)")
    table.add_column("Open", justify="right")
    table.add_column("High", justify="right")
    table.add_column("Low", justify="right")
    table.add_column("Close", justify="right")
    table.add_column("Status")

    for c in feed.candles:
        t = datetime.fromtimestamp(c["time"], tz=timezone.utc).strftime("%H:%M:%S")
        table.add_row(t, f"{c['open']:.5f}", f"{c['high']:.5f}", f"{c['low']:.5f}", f"{c['close']:.5f}", "closed")

    if feed.forming:
        c = feed.forming
        t = datetime.fromtimestamp(c["time"], tz=timezone.utc).strftime("%H:%M:%S")
        color = "green" if c["close"] >= c["open"] else "red"
        table.add_row(
            t,
            f"{c['open']:.5f}",
            f"{c['high']:.5f}",
            f"{c['low']:.5f}",
            f"[{color}]{c['close']:.5f}[/{color}]",
            "[yellow]forming[/yellow]",
        )

    footer = f"ticks seen: {feed.ticks_seen}"
    if feed.last_tick_at:
        age = (datetime.now(timezone.utc) - feed.last_tick_at).total_seconds()
        footer += f" | last tick {age:.1f}s ago"
    table.caption = footer
    return table


async def run(asset: str, period: int, hours: float, max_rows: int) -> None:
    ssid = os.getenv("POCKET_OPTION_SSID") or console.input("Please enter your SSID: ")
    if not ssid:
        console.print("[red]No SSID provided.[/red]")
        return

    client = PocketOptionAsync(ssid)
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
            ts = extract_time(tick)
            price = float(tick.get("close", tick.get("price", 0.0)))
            if buffering:
                tick_buffer.append((ts, price))
            else:
                feed.ingest_tick(ts, price)

    reader_task = asyncio.create_task(tick_reader())

    try:
        # 2. Fetch bulk historical candles and recent candle sources, merge them
        #    by normalized timestamp. Later sources win overlaps.
        offset_seconds = int(hours * 3600)
        platform_time = int(time.time()) + PLATFORM_TIME_OFFSET_SECONDS
        advanced_candles = await client.get_candles_advanced(
            asset,
            period,
            offset_seconds,
            platform_time,
        )
        recent_candles = await client.history(asset, period)
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
        feed.seed_history(history)

        # 3. Replay whatever ticks piled up during the fetch, then switch the
        #    reader over to live ingestion directly. No gap.
        feed.replay_backlog(tick_buffer)
        buffering = False

        console.print(f"[green]Loaded {len(feed.candles)} closed candles, streaming live...[/green]")

        with Live(render(feed, asset, period, hours), refresh_per_second=4, console=console) as live:
            while True:
                await asyncio.sleep(0.25)
                live.update(render(feed, asset, period, hours))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        reader_task.cancel()
        try:
            await reader_task
        except asyncio.CancelledError:
            pass
        await client.unsubscribe(asset)
        await client.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live candle terminal with gap-free history-to-stream handoff")
    parser.add_argument("--asset", default="EURUSD_otc")
    parser.add_argument("--period", type=int, default=60, help="Candle length in seconds")
    parser.add_argument("--hours", type=float, default=2.0, help="Hours of history to backfill")
    parser.add_argument("--rows", type=int, default=30, help="Max closed candles to display")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.asset, args.period, args.hours, args.rows))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()