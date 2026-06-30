"""
CRYPTO MONITOR PRO — Open Interest Scanner
MUHIM: Real-time OI tracking, rapid surge detection, speed calculation
"""
import asyncio
from datetime import datetime
from collections import defaultdict
from loguru import logger

from config.settings import settings
from core.models import OIData, OIEvent, Exchange
from core.state_manager import state_manager


class OIScanner:
    """
    Open Interest real-time scanner.

    Tracks:
    - OI increase start time
    - OI decrease start time
    - Rapid OI surge (10 seconds window)
    - Speed (change % per second)
    - Duration

    Algorithm:
    1. On each OI update → compare with N-second-ago baseline
    2. If change > threshold → detect direction (increase/decrease)
    3. Calculate speed = change_pct / elapsed_seconds
    4. Detect rapid (10s window change >= oi_rapid_threshold)
    5. Emit OIEvent
    """

    def __init__(self, event_callback):
        self.event_callback = event_callback
        self._oi_snapshots: dict[str, list] = defaultdict(list)   # {key: [{oi, ts}]}
        self._event_starts: dict[str, dict] = {}                  # {key: {ts, oi, direction}}
        self._running = False
        # ✅ Reconnect/data-gap flood fix uchun: baseline 60s ko'zlangan,
        # lekin pollingda tabiiy jitter bo'lishi mumkin — shuning uchun
        # ozgina zahira (90s) qoldirilgan. Bundan eski baseline ishonchsiz
        # deb hisoblanadi va signal berilmaydi (qoidalar/threshold o'zgarmagan).
        self.MAX_BASELINE_AGE_SECONDS = 90

    async def start(self):
        self._running = True
        logger.info("✅ OI Scanner started")

    async def process_oi_update(self, oi_data: OIData):
        """Called every time OI data is received (every ~10s from REST poll)"""
        if not self._running:
            return

        exchange = oi_data.exchange.value
        symbol = oi_data.symbol
        key = f"{exchange}:{symbol}"
        now = datetime.utcnow()
        ts = now.timestamp()
        current_oi = oi_data.open_interest_usdt

        # Skip if OI is 0 (probably bad data)
        if current_oi <= 0:
            return

        # Store snapshot
        self._oi_snapshots[key].append({"oi": current_oi, "ts": ts})

        # Keep only last 120 snapshots (20 minutes at 10s interval)
        if len(self._oi_snapshots[key]) > 120:
            self._oi_snapshots[key] = self._oi_snapshots[key][-120:]

        snapshots = self._oi_snapshots[key]

        # Need at least a few data points
        if len(snapshots) < 3:
            return

        # ── Rapid detection (10-second window) ──
        rapid_result = self._check_rapid(snapshots, ts)

        # ── Standard change detection (60-second window) ──
        standard_result = self._check_standard_change(snapshots, ts)

        # Emit event if significant
        if standard_result:
            change_pct, old_ts, speed = standard_result
            abs_change = abs(change_pct)

            if abs_change >= settings.oi_spike_threshold:
                # Check cooldown
                if await state_manager.is_event_sent(exchange, symbol, "oi_change"):
                    # Update duration if event is ongoing
                    return

                # Determine start time
                if key in self._event_starts:
                    start = self._event_starts[key]
                    if start["direction"] == ("up" if change_pct > 0 else "down"):
                        start_time = datetime.fromtimestamp(start["ts"])
                        duration = int(ts - start["ts"])
                    else:
                        # Direction reversed
                        self._event_starts[key] = {
                            "ts": ts, "oi": current_oi,
                            "direction": "up" if change_pct > 0 else "down"
                        }
                        start_time = now
                        duration = 0
                else:
                    self._event_starts[key] = {
                        "ts": old_ts, "oi": current_oi,
                        "direction": "up" if change_pct > 0 else "down"
                    }
                    start_time = datetime.fromtimestamp(old_ts)
                    duration = int(ts - old_ts)

                is_rapid = rapid_result is not None and abs(rapid_result) >= settings.oi_rapid_threshold

                event = OIEvent(
                    symbol=symbol,
                    exchange=oi_data.exchange,
                    change_pct=change_pct,
                    oi_usdt=current_oi,
                    start_time=start_time,
                    speed_per_second=speed,
                    duration_seconds=duration,
                    is_rapid=is_rapid
                )

                # cooldown: 300s = 5 daqiqa (spam oldini olish)
                await state_manager.mark_event_sent(exchange, symbol, "oi_change", cooldown=300)
                await state_manager.increment_stat("oi_events")

                direction_str = "📈 OI artdi" if change_pct > 0 else "📉 OI kamaydi"
                logger.info(
                    f"{direction_str}: {symbol} {change_pct:+.2f}% | "
                    f"${current_oi:,.0f} | Speed: {speed:+.4f}%/s"
                    f"{' [RAPID]' if is_rapid else ''}"
                )
                await self.event_callback(event)
            else:
                # Below threshold - reset event start if direction changed
                if key in self._event_starts:
                    del self._event_starts[key]

    def _check_rapid(self, snapshots: list, now_ts: float) -> float | None:
        """Check OI change in last 10 seconds"""
        cutoff = now_ts - settings.oi_rapid_seconds
        recent = [s for s in snapshots if s["ts"] >= cutoff]
        if len(recent) < 2:
            return None

        oldest = recent[0]["oi"]
        newest = recent[-1]["oi"]
        if oldest <= 0:
            return None

        return ((newest - oldest) / oldest) * 100

    def _check_standard_change(self, snapshots: list, now_ts: float) -> tuple | None:
        """
        Check OI change vs 60 seconds ago.
        Returns (change_pct, baseline_ts, speed_per_second) or None

        ─── BUG TO'G'IRLANDI (reconnect/data-gap flood) ───
        Eski kod: agar 60s oldingi snapshot topilmasa, ro'yxatdagi ENG ESKI
        nuqta (snapshots[0], 20 daqiqagacha eski bo'lishi mumkin) baseline
        qilib olinardi. Network uzilib qayta ulanganda yoki REST so'rovlar
        vaqtincha kechikkanda, yangi ma'lumot shu eski (masalan 20 daqiqa
        oldingi) qiymat bilan solishtirilib, oddiy sekin drift ham
        oi_spike_threshold'dan oshib ketardi — natijada deyarli barcha
        symbol uchun bir vaqtda soxta "NOTICE" signal ketardi.

        Tuzatish: tanlangan baseline (qaysi yo'l bilan topilgan bo'lishidan
        qat'i nazar) MAX_BASELINE_AGE_SECONDS dan eski bo'lsa, bu ma'lumot
        ishonchsiz (gap/reconnect ta'sirida) deb hisoblanadi va signal
        berilmaydi — keyingi sof ma'lumot kelguncha kutiladi.
        oi_spike_threshold, cooldown, boshqa qoidalar o'zgarmagan.
        """
        current_oi = snapshots[-1]["oi"]
        cutoff = now_ts - 60

        # Find snapshot closest to 60s ago
        baseline = None
        for snap in snapshots:
            if snap["ts"] <= cutoff:
                baseline = snap
            else:
                break

        if not baseline or baseline["oi"] <= 0:
            # Use oldest available
            if len(snapshots) >= 3:
                baseline = snapshots[0]
            else:
                return None

        # ✅ Staleness guard — reconnect/data-gap flood fix
        if (now_ts - baseline["ts"]) > self.MAX_BASELINE_AGE_SECONDS:
            return None

        change_pct = ((current_oi - baseline["oi"]) / baseline["oi"]) * 100
        elapsed = now_ts - baseline["ts"]
        speed = change_pct / elapsed if elapsed > 0 else 0

        return change_pct, baseline["ts"], speed

    async def stop(self):
        self._running = False
