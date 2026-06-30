"""
CRYPTO MONITOR PRO — Digest Scheduler (Yangi modul)

Nima qiladi:
- Har 1 soatda: soatlik hisobot (top o'sgan/tushgan + noodatiy)
- Har 4 soatda: 4 soatlik hisobot (kuchliroq, batafsilroq)
- Faqat e'tiborga loyiq coinlarni ko'rsatadi
- Binance klines REST API ishlatadi (bepul)
"""
import asyncio
from datetime import datetime, timezone
from loguru import logger
import aiohttp


# Digest uchun minimum o'zgarish chegaralari
H1_MIN_PRICE_CHANGE = 2.0    # 1h da kamida 2% o'zgargan
H1_MIN_VOLUME_RATIO = 1.5    # O'rtachadan 1.5x ko'p hajm
H4_MIN_PRICE_CHANGE = 4.0    # 4h da kamida 4% o'zgargan
H4_MIN_VOLUME_RATIO = 2.0    # O'rtachadan 2x ko'p hajm
TOP_N = 5                    # Har ro'yxatda nechta coin


def _fmt_usdt(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v/1_000_000_000:.2f}B$"
    elif v >= 1_000_000:
        return f"{v/1_000_000:.1f}M$"
    elif v >= 1_000:
        return f"{v/1_000:.0f}K$"
    return f"{v:.0f}$"


class DigestScheduler:
    def __init__(self, broadcast_callback, symbols: list[str] = None):
        """
        broadcast_callback: async func(message: str) — barcha userlarga yuboradi
        symbols: kuzatiladigan symbollar ro'yxati
        """
        self.broadcast_callback = broadcast_callback
        self.symbols = symbols or []
        self._running = False

    def update_symbols(self, symbols: list[str]):
        self.symbols = symbols

    async def start(self):
        self._running = True
        asyncio.create_task(self._h1_loop())
        asyncio.create_task(self._h4_loop())
        logger.info("✅ Digest Scheduler started (H1 + H4)")

    async def _h1_loop(self):
        """Har soat boshida (XX:00) ishga tushadi"""
        while self._running:
            now = datetime.now(timezone.utc)
            # Keyingi soat boshiga qancha vaqt qoldi
            minutes_left = 60 - now.minute
            seconds_left = minutes_left * 60 - now.second
            await asyncio.sleep(seconds_left)

            if not self._running:
                break

            try:
                await self._send_h1_digest()
            except Exception as e:
                logger.error(f"H1 digest xato: {e}")

    async def _h4_loop(self):
        """Har 4 soatda (00:00, 04:00, 08:00, 12:00, 16:00, 20:00) ishga tushadi"""
        while self._running:
            now = datetime.now(timezone.utc)
            current_hour = now.hour
            next_4h = ((current_hour // 4) + 1) * 4
            hours_left = next_4h - current_hour
            seconds_left = hours_left * 3600 - now.minute * 60 - now.second
            await asyncio.sleep(seconds_left)

            if not self._running:
                break

            try:
                await self._send_h4_digest()
            except Exception as e:
                logger.error(f"H4 digest xato: {e}")

    async def _fetch_coin_data(self, session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> dict | None:
        """Bir coin uchun klines ma'lumotini olish"""
        try:
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {"symbol": symbol, "interval": interval, "limit": limit}
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return None
                klines = await resp.json()
                if not klines or len(klines) < 2:
                    return None

                # Oxirgi tugagan sham (hozirgi hali ochiq)
                last = klines[-2]
                current = klines[-1]

                open_price  = float(last[1])
                close_price = float(last[4])
                vol_usdt    = float(last[7])  # quote volume (USDT)

                # O'rtacha hajm (oldingi shamlar)
                prev_vols = [float(k[7]) for k in klines[:-1] if float(k[7]) > 0]
                avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1

                price_change_pct = ((close_price - open_price) / open_price) * 100 if open_price > 0 else 0
                vol_ratio = vol_usdt / avg_vol if avg_vol > 0 else 1

                # OI o'zgarishi
                oi_change_pct = None
                try:
                    oi_url = "https://fapi.binance.com/futures/data/openInterestHist"
                    oi_params = {"symbol": symbol, "period": interval, "limit": 2}
                    async with session.get(oi_url, params=oi_params, timeout=aiohttp.ClientTimeout(total=3)) as oi_resp:
                        if oi_resp.status == 200:
                            oi_data = await oi_resp.json()
                            if len(oi_data) >= 2:
                                oi_now = float(oi_data[-1]["sumOpenInterestValue"])
                                oi_prev = float(oi_data[-2]["sumOpenInterestValue"])
                                if oi_prev > 0:
                                    oi_change_pct = ((oi_now - oi_prev) / oi_prev) * 100
                except Exception:
                    pass

                return {
                    "symbol": symbol,
                    "price_change_pct": price_change_pct,
                    "vol_usdt": vol_usdt,
                    "vol_ratio": vol_ratio,
                    "avg_vol": avg_vol,
                    "close_price": close_price,
                    "oi_change_pct": oi_change_pct,
                }

        except Exception as e:
            logger.debug(f"Coin data fetch error {symbol}: {e}")
            return None

    async def _send_h1_digest(self):
        """Soatlik hisobot"""
        if not self.symbols:
            return

        logger.info("📊 H1 digest tayyorlanmoqda...")
        now = datetime.now(timezone.utc)
        results = []

        async with aiohttp.ClientSession() as session:
            # Batch: 50 ta 50 ta
            for i in range(0, min(len(self.symbols), 300), 50):
                batch = self.symbols[i:i+50]
                tasks = [self._fetch_coin_data(session, s, "1h", 10) for s in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in batch_results:
                    if isinstance(r, dict) and r:
                        results.append(r)
                await asyncio.sleep(0.5)

        if not results:
            return

        # Faqat e'tiborga loyiqlarini filter
        notable = [
            r for r in results
            if abs(r["price_change_pct"]) >= H1_MIN_PRICE_CHANGE
            or r["vol_ratio"] >= H1_MIN_VOLUME_RATIO
        ]

        gainers = sorted([r for r in notable if r["price_change_pct"] > 0],
                        key=lambda x: x["price_change_pct"], reverse=True)[:TOP_N]
        losers  = sorted([r for r in notable if r["price_change_pct"] < 0],
                        key=lambda x: x["price_change_pct"])[:TOP_N]

        if not gainers and not losers:
            logger.info("H1 digest: e'tiborga loyiq coin yo'q")
            return

        hour_str = now.strftime("%H:00")
        lines = [
            f"━━━━━━━━━━━━━━━━━━",
            f"⏰ <b>SOATLIK HISOBOT — {hour_str} UTC</b>",
            f"━━━━━━━━━━━━━━━━━━",
            "",
        ]

        if gainers:
            lines.append("📈 <b>Ko'tarilganlar:</b>")
            medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
            for i, r in enumerate(gainers):
                medal = medals[i] if i < len(medals) else "▪️"
                lines.append(f"{medal} <b>#{r['symbol']}</b>  <code>{r['price_change_pct']:+.2f}%</code>")
                vol_line = f"   ┣ Hajm: {_fmt_usdt(r['vol_usdt'])}"
                if r["vol_ratio"] >= 1.5:
                    vol_line += f"  <b>▲ {r['vol_ratio']:.1f}x</b>"
                lines.append(vol_line)
                if r["oi_change_pct"] is not None and abs(r["oi_change_pct"]) >= 1.0:
                    oi_ico = "▲" if r["oi_change_pct"] > 0 else "▼"
                    lines.append(f"   ┗ OI: {r['oi_change_pct']:+.1f}% {oi_ico}")
                else:
                    lines.append(f"   ┗ Narx: {r['close_price']:,.4f}" if r["close_price"] < 1 else f"   ┗ Narx: {r['close_price']:,.2f}$")
            lines.append("")

        if losers:
            lines.append("📉 <b>Tushganlar:</b>")
            for r in losers:
                lines.append(f"▪️ <b>#{r['symbol']}</b>  <code>{r['price_change_pct']:+.2f}%</code>")
                if r["vol_ratio"] >= 1.5:
                    lines.append(f"   ┗ Hajm: {_fmt_usdt(r['vol_usdt'])}  ▲ {r['vol_ratio']:.1f}x")
            lines.append("")

        lines += [
            f"📊 {len(self.symbols)} coin kuzatildi | {len(notable)} ta e'tiborga loyiq",
            f"━━━━━━━━━━━━━━━━━━",
        ]

        message = "\n".join(lines)
        await self.broadcast_callback(message)
        logger.info(f"✅ H1 digest yuborildi: {len(gainers)} gainer, {len(losers)} loser")

    async def _send_h4_digest(self):
        """4 soatlik hisobot"""
        if not self.symbols:
            return

        logger.info("📊 H4 digest tayyorlanmoqda...")
        now = datetime.now(timezone.utc)
        results = []

        async with aiohttp.ClientSession() as session:
            for i in range(0, min(len(self.symbols), 300), 50):
                batch = self.symbols[i:i+50]
                tasks = [self._fetch_coin_data(session, s, "4h", 10) for s in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in batch_results:
                    if isinstance(r, dict) and r:
                        results.append(r)
                await asyncio.sleep(0.5)

        if not results:
            return

        notable = [
            r for r in results
            if abs(r["price_change_pct"]) >= H4_MIN_PRICE_CHANGE
            or r["vol_ratio"] >= H4_MIN_VOLUME_RATIO
        ]

        gainers = sorted([r for r in notable if r["price_change_pct"] > 0],
                        key=lambda x: x["price_change_pct"], reverse=True)[:TOP_N]
        losers  = sorted([r for r in notable if r["price_change_pct"] < 0],
                        key=lambda x: x["price_change_pct"])[:TOP_N]

        if not gainers and not losers:
            return

        hour_str = now.strftime("%H:00")
        lines = [
            f"━━━━━━━━━━━━━━━━━━",
            f"📊 <b>4 SOATLIK HISOBOT — {hour_str} UTC</b>",
            f"━━━━━━━━━━━━━━━━━━",
            "",
        ]

        if gainers:
            lines.append("🏆 <b>H4 yetakchilar:</b>")
            medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
            for i, r in enumerate(gainers):
                medal = medals[i] if i < len(medals) else "▪️"
                lines.append(f"\n{medal} <b>#{r['symbol']}</b>")
                lines.append(f"   ┣ Narx:  <code>{r['price_change_pct']:+.2f}%</code>")
                lines.append(f"   ┣ Hajm:  {_fmt_usdt(r['vol_usdt'])}  <b>▲ {r['vol_ratio']:.1f}x</b>")
                if r["oi_change_pct"] is not None:
                    oi_ico = "▲" if r["oi_change_pct"] > 0 else "▼"
                    lines.append(f"   ┗ OI:    {r['oi_change_pct']:+.1f}% {oi_ico}")
            lines.append("")

        if losers:
            lines.append("📉 <b>H4 tushganlar:</b>")
            for r in losers:
                lines.append(f"▪️ <b>#{r['symbol']}</b>  <code>{r['price_change_pct']:+.2f}%</code>")
                if r["oi_change_pct"] is not None and r["oi_change_pct"] < -1:
                    lines.append(f"   ┗ OI: {r['oi_change_pct']:+.1f}% ▼")
            lines.append("")

        lines += [
            f"📊 {len(self.symbols)} coin kuzatildi | {len(notable)} ta e'tiborga loyiq",
            f"━━━━━━━━━━━━━━━━━━",
        ]

        message = "\n".join(lines)
        await self.broadcast_callback(message)
        logger.info(f"✅ H4 digest yuborildi: {len(gainers)} gainer, {len(losers)} loser")

    async def stop(self):
        self._running = False