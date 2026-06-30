"""
Volume Profile — Binance klines API dan real hajm taqsimoti
POC (Point of Control), VAH (Value Area High), VAL (Value Area Low)
Multi-timeframe qo'llab-quvvatlaydi
"""
import asyncio
import time
import aiohttp
from loguru import logger


class VolumeProfile:
    """Volume Profile — POC, VAH, VAL hisoblash"""

    TOP_SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "AVAXUSDT",
    ]

    TIMEFRAMES = {
        "5m": {"interval": "5m", "limit": 100, "label": "5 daqiqa"},
        "15m": {"interval": "15m", "limit": 96, "label": "15 daqiqa"},
        "1h": {"interval": "1h", "limit": 72, "label": "1 soat"},
        "4h": {"interval": "4h", "limit": 42, "label": "4 soat"},
        "1d": {"interval": "1d", "limit": 30, "label": "1 kun"},
    }

    def __init__(self, update_interval: int = 300):
        self.update_interval = update_interval
        self.data = {}
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("🟢 Volume Profile moduli ishga tushdi")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("🔴 Volume Profile to'xtatildi")

    async def _loop(self):
        while self._running:
            try:
                await self._fetch_all()
            except Exception as e:
                logger.debug(f"Volume Profile xato: {e}")
            await asyncio.sleep(self.update_interval)

    async def _fetch_all(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_symbol(session, sym, "15m", 96) for sym in self.TOP_SYMBOLS]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def get_profile(self, symbol: str, timeframe: str = "15m") -> dict:
        """Bitta coin uchun Volume Profile olish"""
        if timeframe not in self.TIMEFRAMES:
            timeframe = "15m"

        tf = self.TIMEFRAMES[timeframe]
        cache_key = f"{symbol}_{timeframe}"

        if cache_key in self.data:
            cached = self.data[cache_key]
            if time.time() - cached.get("timestamp", 0) < self.update_interval:
                return cached

        async with aiohttp.ClientSession() as session:
            result = await self._fetch_symbol(session, symbol, tf["interval"], tf["limit"])
            return result

    async def _fetch_symbol(self, session, symbol: str, interval: str = "15m", limit: int = 96):
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    if data and len(data) > 10:
                        result = self._calculate_profile(symbol, data, interval)
                        return result
        except Exception as e:
            logger.debug(f"Volume Profile {symbol} {interval} xato: {e}")
        return None

    def _calculate_profile(self, symbol: str, klines: list, interval: str = "15m"):
        volume_at_price = {}
        for k in klines:
            high = float(k[2])
            low = float(k[3])
            vol = float(k[5])
            mid = (high + low) / 2
            if mid not in volume_at_price:
                volume_at_price[mid] = 0
            volume_at_price[mid] += vol

        if not volume_at_price:
            return None

        sorted_prices = sorted(volume_at_price.items(), key=lambda x: x[1], reverse=True)
        poc = sorted_prices[0][0]

        total_vol = sum(v for _, v in sorted_prices)
        target_vol = total_vol * 0.7
        va_prices = []
        accumulated = 0
        for price, vol in sorted(volume_at_price.items(), key=lambda x: abs(x[0] - poc)):
            va_prices.append(price)
            accumulated += vol
            if accumulated >= target_vol:
                break

        vah = max(va_prices) if va_prices else poc * 1.01
        val = min(va_prices) if va_prices else poc * 0.99
        current = float(klines[-1][4])

        if current > vah:
            zone = "📈 Yuqorida (VAH dan yuqori)"
            zone_short = "yuqorida"
            dist_pct = ((current - vah) / vah) * 100
        elif current < val:
            zone = "📉 Pastda (VAL dan past)"
            zone_short = "pastda"
            dist_pct = ((current - val) / val) * 100
        else:
            zone = "➡️ Value Area ichida"
            zone_short = "ichida"
            dist_pct = 0

        poc_dist = ((current - poc) / poc) * 100 if poc > 0 else 0

        result = {
            "poc": poc,
            "vah": vah,
            "val": val,
            "current": current,
            "zone": zone,
            "zone_short": zone_short,
            "dist_pct": dist_pct,
            "poc_dist": poc_dist,
            "total_volume": total_vol,
            "interval": interval,
            "timestamp": time.time(),
        }

        cache_key = f"{symbol}_{interval}"
        self.data[cache_key] = result
        return result

    async def get_multi_tf(self, symbol: str) -> dict:
        """Har qanday coin uchun multi-timeframe Volume Profile"""
        results = {}
        async with aiohttp.ClientSession() as session:
            tasks = []
            for tf_key, tf_info in self.TIMEFRAMES.items():
                tasks.append(self._fetch_symbol(session, symbol, tf_info["interval"], tf_info["limit"]))
            profiles = await asyncio.gather(*tasks, return_exceptions=True)

            for tf_key, profile in zip(self.TIMEFRAMES.keys(), profiles):
                if isinstance(profile, dict) and profile:
                    results[tf_key] = profile
        return results

    def format_symbol_text(self, symbol: str, data: dict) -> str:
        """Bitta coin uchun formatlangan matn"""
        if not data:
            return f"⚠️ {symbol} Volume Profile mavjud emas"

        poc = data.get("poc", 0)
        vah = data.get("vah", 0)
        val = data.get("val", 0)
        current = data.get("current", 0)
        zone = data.get("zone", "")
        poc_dist = data.get("poc_dist", 0)
        interval = data.get("interval", "15m")

        tf_label = self.TIMEFRAMES.get(interval, {}).get("label", interval)

        lines = [
            f"📊 <b>VOLUME PROFILE — {symbol}</b>",
            f"⏰ Interval: {tf_label}",
            "",
            f"💵 Joriy: {fmt_price(current)}$",
            f"🎯 POC: {fmt_price(poc)}$ ({poc_dist:+.2f}%)",
            f"📈 VAH: {fmt_price(vah)}$",
            f"📉 VAL: {fmt_price(val)}$",
            f"📍 {zone}",
        ]

        if vah > val and vah > 0:
            range_ = vah - val
            if current > vah:
                pos = 1.0
            elif current < val:
                pos = 0.0
            else:
                pos = (current - val) / range_ if range_ > 0 else 0.5
            poc_pos = (poc - val) / range_ if range_ > 0 else 0.5
            bar_len = 20
            pos_idx = int(pos * (bar_len - 1))
            poc_idx = int(poc_pos * (bar_len - 1))
            bar_list = ["░"] * bar_len
            bar_list[poc_idx] = "🎯"
            bar_list[pos_idx] = "📍"
            lines.append(f"  [{''.join(bar_list)}]")

        return "\n".join(lines)

    def format_multi_tf_text(self, symbol: str, profiles: dict) -> str:
        """Multi-timeframe formatlangan matn"""
        if not profiles:
            return f"⚠️ {symbol} Multi-TF Profile mavjud emas"

        lines = [
            f"📊 <b>MULTI-TF VOLUME PROFILE — {symbol}</b>",
            "",
        ]

        for tf_key, tf_info in self.TIMEFRAMES.items():
            d = profiles.get(tf_key)
            if not d:
                continue

            poc = d.get("poc", 0)
            poc_dist = d.get("poc_dist", 0)
            zone_short = d.get("zone_short", "")

            ico = "✅" if abs(poc_dist) < 1 else "⚠️" if abs(poc_dist) < 3 else "🔴"
            lines.append(
                f"  {ico} <b>{tf_info['label']}:</b> POC {fmt_price(poc)}$ ({poc_dist:+.1f}%) — {zone_short}"
            )

        return "\n".join(lines)

    def format_text(self) -> str:
        if not self.data:
            return "⚠️ Volume Profile mavjud emas"

        lines = [
            "📊 <b>VOLUME PROFILE</b>",
            "📊 15m interval, so'nggi 24 soat",
            "",
        ]

        for key, d in self.data.items():
            if "_" in key:
                sym = key.rsplit("_", 1)[0]
            else:
                sym = key

            poc = d["poc"]
            vah = d["vah"]
            val = d["val"]
            current = d["current"]
            zone = d["zone"]

            poc_dist = d.get("poc_dist", 0)

            lines.append(f"📌 <b>{sym}</b>")
            lines.append(f"  💵 Joriy: {fmt_price(current)}$")
            lines.append(f"  🎯 POC:  {fmt_price(poc)}$ ({poc_dist:+.2f}%)")
            lines.append(f"  📈 VAH:  {fmt_price(vah)}$")
            lines.append(f"  📉 VAL:  {fmt_price(val)}$")
            lines.append(f"  📍 Zona: {zone}")

            if vah > val and vah > 0:
                range_ = vah - val
                if current > vah:
                    pos = 1.0
                elif current < val:
                    pos = 0.0
                else:
                    pos = (current - val) / range_ if range_ > 0 else 0.5
                poc_pos = (poc - val) / range_ if range_ > 0 else 0.5
                bar_len = 20
                pos_idx = int(pos * (bar_len - 1))
                poc_idx = int(poc_pos * (bar_len - 1))
                bar_list = ["░"] * bar_len
                bar_list[poc_idx] = "🎯"
                bar_list[pos_idx] = "📍"
                lines.append(f"  [{''.join(bar_list)}]")

            lines.append("")

        return "\n".join(lines)


def fmt_price(p: float) -> str:
    if p >= 10000:
        return f"{p:,.0f}"
    elif p >= 100:
        return f"{p:,.2f}"
    elif p >= 1:
        return f"{p:.4f}"
    elif p > 0:
        return f"{p:.6f}"
    return "—"


volume_profile = VolumeProfile()
