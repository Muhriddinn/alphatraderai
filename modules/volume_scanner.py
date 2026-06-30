"""
CRYPTO MONITOR PRO — Volume Scanner (Yangilangan)

Yangiliklar:
- 5m, 15m, 1h, 4h timeframe qo'shildi
- Kunlik klines dan real baseline (30m o'rtacha emas)
- Trend aniqlash (o'sayaptimi yoki tushayaptimi)
- Composite signal: hajm + trend birgalikda
"""
import asyncio
from datetime import datetime
from collections import defaultdict
from loguru import logger
from config.settings import settings
from core.models import VolumeEvent, TradeData, Exchange
from core.state_manager import state_manager


class VolumeScanner:
    def __init__(self, event_callback):
        self.event_callback = event_callback
        self._volume_windows: dict[str, list] = defaultdict(list)
        self._spike_starts: dict[str, datetime] = {}
        self._daily_baselines: dict[str, float] = {}   # symbol -> kunlik 1m o'rtacha
        self._running = False

    async def start(self):
        self._running = True
        asyncio.create_task(self._cleanup_loop())
        asyncio.create_task(self._refresh_daily_baselines())
        logger.info("✅ Volume Scanner started (multi-timeframe)")

    async def bootstrap(self, symbols: list[str]):
        """
        Bot ishga tushgan zahoti Binance'dan tarixiy 1m klines yuklab,
        5m/15m/1h/4h hajm va kunlik baseline'ni DARHOL to'ldiradi.
        Shu tufayli birinchi soniyadan boshlab signal to'liq keladi —
        2-5 daqiqa kutish shart bo'lmaydi.
        """
        import aiohttp
        logger.info(f"📊 Volume bootstrap boshlandi ({len(symbols)} symbol)...")
        loaded = 0
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    # 1) Oxirgi 4 soatlik 1m klines — 5m/15m/1h/4h hajmni darhol to'ldirish
                    url = "https://fapi.binance.com/fapi/v1/klines"
                    params = {"symbol": symbol, "interval": "1m", "limit": 240}
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as r:
                        if r.status == 200:
                            klines = await r.json()
                            key = f"binance:{symbol}"
                            now_ts = datetime.utcnow().timestamp()
                            # Har bir 1m sham -> "ts" sifatida sham yopilish vaqti, "usdt" = shu daqiqadagi hajm
                            for k in klines:
                                close_ts = float(k[6]) / 1000  # close time (ms -> s)
                                quote_vol = float(k[7])
                                if quote_vol > 0:
                                    self._volume_windows[key].append({
                                        "usdt": quote_vol,
                                        "ts": close_ts,
                                    })

                    # 2) Oxirgi 7 kunlik klines -> kunlik baseline (1m o'rtacha)
                    params2 = {"symbol": symbol, "interval": "1d", "limit": 7}
                    async with session.get(url, params=params2, timeout=aiohttp.ClientTimeout(total=5)) as r2:
                        if r2.status == 200:
                            daily = await r2.json()
                            daily_vols = [float(k[7]) for k in daily if float(k[7]) > 0]
                            if daily_vols:
                                avg_daily = sum(daily_vols) / len(daily_vols)
                                self._daily_baselines[symbol] = avg_daily / 1440
                                loaded += 1
                except Exception as e:
                    logger.debug(f"Volume bootstrap xato {symbol}: {e}")
                await asyncio.sleep(0.05)  # rate-limit uchun

        logger.info(f"✅ Volume bootstrap tugadi: {loaded}/{len(symbols)} symbol")

    async def process_trade(self, trade: TradeData):
        if not self._running:
            return
        key = f"{trade.exchange.value}:{trade.symbol}"
        now = datetime.utcnow()
        self._volume_windows[key].append({
            "usdt": trade.usdt_value,
            "ts": now.timestamp()
        })
        if len(self._volume_windows[key]) % 10 == 0:
            await self._check_spike(trade.exchange, trade.symbol, now)

    async def _check_spike(self, exchange: Exchange, symbol: str, now: datetime):
        key = f"{exchange.value}:{symbol}"
        window = self._volume_windows[key]
        ts_now = now.timestamp()

        # Har bir timeframe uchun hajm
        vol_5m  = sum(v["usdt"] for v in window if v["ts"] >= ts_now - 300)
        vol_15m = sum(v["usdt"] for v in window if v["ts"] >= ts_now - 900)
        vol_1h  = sum(v["usdt"] for v in window if v["ts"] >= ts_now - 3600)
        vol_4h  = sum(v["usdt"] for v in window if v["ts"] >= ts_now - 14400)

        if vol_5m < 1000:
            return

        # Baseline: kunlik o'rtacha 1m hajm
        # Agar kunlik baseline bor bo'lsa — ishlatamiz
        # Yo'q bo'lsa — state_manager dagi 30m baseline ga qaytamiz
        daily_baseline = self._daily_baselines.get(symbol)
        if daily_baseline and daily_baseline > 0:
            baseline_5m  = daily_baseline * 5
            baseline_15m = daily_baseline * 15
            baseline_1h  = daily_baseline * 60
            baseline_4h  = daily_baseline * 240
        else:
            # Eski baseline (30m o'rtacha) fallback
            old_baseline = await state_manager.get_volume_baseline(exchange.value, symbol)
            if not old_baseline or old_baseline <= 0:
                await state_manager.update_volume_baseline(exchange.value, symbol, vol_5m / 5)
                return
            baseline_5m  = old_baseline * 5
            baseline_15m = old_baseline * 15
            baseline_1h  = old_baseline * 60
            baseline_4h  = old_baseline * 240

        # Spike foizlari
        spike_5m  = ((vol_5m  - baseline_5m)  / baseline_5m)  * 100 if baseline_5m  > 0 else 0
        spike_15m = ((vol_15m - baseline_15m) / baseline_15m) * 100 if baseline_15m > 0 else 0
        spike_1h  = ((vol_1h  - baseline_1h)  / baseline_1h)  * 100 if baseline_1h  > 0 else 0
        spike_4h  = ((vol_4h  - baseline_4h)  / baseline_4h)  * 100 if baseline_4h  > 0 else 0

        # Trend: 5m spike > 15m spike > 1h spike = o'sayapti
        trend = "up" if spike_5m >= spike_15m >= 0 else "down" if spike_5m < 0 else "neutral"

        logger.debug(
            f"📊 Volume: {symbol} | "
            f"5m: {vol_5m:,.0f}$ ({spike_5m:+.1f}%) | "
            f"15m: {vol_15m:,.0f}$ ({spike_15m:+.1f}%) | "
            f"1h: {vol_1h:,.0f}$ ({spike_1h:+.1f}%)"
        )

        # Signal: 5m da kamida threshold oshgan bo'lishi kerak
        if spike_5m >= settings.volume_spike_threshold:
            if await state_manager.is_event_sent(exchange.value, symbol, "volume_spike"):
                return

            if key not in self._spike_starts:
                self._spike_starts[key] = now

            start_time = self._spike_starts[key]
            duration = int((now - start_time).total_seconds())
            is_whale = vol_5m >= settings.volume_whale_min_usdt * 5

            event = VolumeEvent(
                symbol=symbol,
                exchange=exchange,
                spike_pct=spike_5m,
                volume_usdt=vol_5m,
                start_time=start_time,
                duration_seconds=duration,
                is_whale=is_whale
            )

            # Qo'shimcha timeframe ma'lumotlarini extra ga saqlash
            event.extra_volumes = {
                "vol_5m": vol_5m,
                "vol_15m": vol_15m,
                "vol_1h": vol_1h,
                "vol_4h": vol_4h,
                "spike_5m": spike_5m,
                "spike_15m": spike_15m,
                "spike_1h": spike_1h,
                "spike_4h": spike_4h,
                "trend": trend,
                "daily_baseline_1m": daily_baseline or (baseline_5m / 5),
            }

            await state_manager.mark_event_sent(
                exchange.value, symbol, "volume_spike", cooldown=120
            )
            await state_manager.increment_stat("volume_events")

            logger.info(
                f"📊 Volume spike: {symbol} "
                f"+{spike_5m:.1f}% (5m) | "
                f"+{spike_15m:.1f}% (15m) | "
                f"trend: {trend}"
            )
            await self.event_callback(event)

        else:
            if key in self._spike_starts:
                del self._spike_starts[key]
            # Eski baseline yangilash (faqat kunlik baseline yo'q bo'lganda)
            if not daily_baseline:
                old_baseline = await state_manager.get_volume_baseline(exchange.value, symbol)
                if old_baseline:
                    new_baseline = old_baseline * 0.95 + (vol_5m / 5) * 0.05
                    await state_manager.update_volume_baseline(exchange.value, symbol, new_baseline)

    async def _refresh_daily_baselines(self):
        """
        Har 1 soatda kunlik klines dan baseline yangilaydi.
        Binance /fapi/v1/klines?interval=1d&limit=7
        Oxirgi 7 kunlik o'rtacha hajm / 1440 = 1m o'rtacha
        """
        import aiohttp
        while self._running:
            try:
                symbols = list(self._volume_windows.keys())
                if not symbols:
                    await asyncio.sleep(60)
                    continue

                logger.info(f"📊 Kunlik baseline yangilanmoqda ({len(symbols)} symbol)...")
                loaded = 0

                async with aiohttp.ClientSession() as session:
                    for key in symbols:
                        try:
                            symbol = key.split(":")[-1]
                            url = "https://fapi.binance.com/fapi/v1/klines"
                            params = {"symbol": symbol, "interval": "1d", "limit": 7}
                            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                                if resp.status == 200:
                                    klines = await resp.json()
                                    if klines:
                                        # k[7] = quote asset volume (USDT)
                                        daily_vols = [float(k[7]) for k in klines if float(k[7]) > 0]
                                        if daily_vols:
                                            avg_daily = sum(daily_vols) / len(daily_vols)
                                            # 1 kunlik hajm / 1440 daqiqa = 1m o'rtacha
                                            self._daily_baselines[symbol] = avg_daily / 1440
                                            loaded += 1
                        except Exception as e:
                            logger.debug(f"Daily baseline error {key}: {e}")
                        await asyncio.sleep(0.05)  # rate limit

                logger.info(f"✅ Kunlik baseline: {loaded} symbol yangilandi")

            except Exception as e:
                logger.error(f"Daily baseline refresh error: {e}")

            # Har 1 soatda yangilash
            await asyncio.sleep(3600)

    def get_daily_baseline(self, symbol: str) -> float:
        """Tashqi modullar uchun: kunlik 1m o'rtacha hajm"""
        return self._daily_baselines.get(symbol, 0)

    def get_current_volumes(self, symbol: str) -> dict | None:
        """
        Har doim (spike bo'lmasa ham) hozirgi multi-timeframe hajmni qaytaradi.
        AlertEngine har bir signalga hajm blokini qo'shishi uchun ishlatiladi.
        """
        # key formati "{exchange}:{symbol}" — exchange nomini bilmasdan qidiramiz
        window = None
        for key, w in self._volume_windows.items():
            if key.endswith(f":{symbol}"):
                window = w
                break
        if not window:
            return None

        ts_now = datetime.utcnow().timestamp()
        vol_1m  = sum(v["usdt"] for v in window if v["ts"] >= ts_now - 60)
        vol_5m  = sum(v["usdt"] for v in window if v["ts"] >= ts_now - 300)
        vol_15m = sum(v["usdt"] for v in window if v["ts"] >= ts_now - 900)
        vol_1h  = sum(v["usdt"] for v in window if v["ts"] >= ts_now - 3600)
        vol_4h  = sum(v["usdt"] for v in window if v["ts"] >= ts_now - 14400)

        daily_baseline = self._daily_baselines.get(symbol, 0)
        if daily_baseline > 0:
            baseline_1m = daily_baseline
            baseline_5m, baseline_15m = daily_baseline * 5, daily_baseline * 15
            baseline_1h, baseline_4h = daily_baseline * 60, daily_baseline * 240
        else:
            baseline_1m = baseline_5m = baseline_15m = baseline_1h = baseline_4h = 0

        def spike(vol, base):
            return ((vol - base) / base) * 100 if base > 0 else 0

        spike_1m = spike(vol_1m, baseline_1m)
        spike_5m, spike_15m = spike(vol_5m, baseline_5m), spike(vol_15m, baseline_15m)
        spike_1h, spike_4h = spike(vol_1h, baseline_1h), spike(vol_4h, baseline_4h)
        trend = "up" if spike_5m >= spike_15m >= 0 else "down" if spike_5m < 0 else "neutral"

        return {
            "vol_1m": vol_1m, "vol_5m": vol_5m, "vol_15m": vol_15m, "vol_1h": vol_1h, "vol_4h": vol_4h,
            "spike_1m": spike_1m, "spike_5m": spike_5m, "spike_15m": spike_15m,
            "spike_1h": spike_1h, "spike_4h": spike_4h,
            "trend": trend,
            "daily_baseline_1m": daily_baseline or (baseline_5m / 5 if baseline_5m else 0),
        }

    def get_volume_data(self, symbol: str) -> dict:
        """Extra data uchun hajm ma'lumotlarini qaytaradi"""
        key = f"binance:{symbol}"
        window = self._volume_windows.get(key, [])
        if not window:
            return {}
        now = datetime.utcnow().timestamp()
        return {
            "vol_5m": sum(v["usdt"] for v in window if v["ts"] >= now - 300),
            "vol_15m": sum(v["usdt"] for v in window if v["ts"] >= now - 900),
            "vol_1h": sum(v["usdt"] for v in window if v["ts"] >= now - 3600),
            "recent_volume_usdt": sum(v["usdt"] for v in window if v["ts"] >= now - 60),
        }

    async def _cleanup_loop(self):
        while self._running:
            now = datetime.utcnow().timestamp()
            cutoff = now - 14400  # 4 soatdan eski o'chiriladi
            for key in list(self._volume_windows.keys()):
                self._volume_windows[key] = [
                    v for v in self._volume_windows[key]
                    if v["ts"] >= cutoff
                ]
            await asyncio.sleep(300)

    async def stop(self):
        self._running = False