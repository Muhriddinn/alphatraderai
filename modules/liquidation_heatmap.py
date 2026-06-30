"""
Liquidation Heatmap — AggTrade Large Trades as Proxy
WS likvidatsiya stream ishlamaydi + REST API o'chirilgan.
Shuning uchun katta aggTrade larni likvidatsiya proxy sifatida ishlatamiz.
"""
import time
import asyncio
import aiohttp
from collections import defaultdict
from datetime import datetime
from loguru import logger

BINANCE_SPOT = "https://api.binance.com"
BINANCE_FUTURES = "https://fapi.binance.com"


class LiquidationHeatmap:
    """
    Liquidation Heatmap — Large aggTrades as liq proxy.
    Har bir katta trade ($5K+) likvidatsiya bo'lishi mumkin.
    """

    TOP_SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
        "AVAXUSDT", "TRXUSDT", "NEARUSDT", "MATICUSDT", "UNIUSDT",
        "ATOMUSDT", "FILUSDT", "APTUSDT", "ARBUSDT", "OPUSDT",
    ]

    def __init__(self):
        self.data = {}
        self._running = False

    async def start(self):
        self._running = True
        logger.info("Liquidation Heatmap started (AggTrade proxy)")

    async def stop(self):
        self._running = False

    def build_from_aggregator(self, liq_aggregator):
        """
        LiqAggregator dan real ma'lumotlarni olib heatmap quradi.
        """
        self.data = {}
        for symbol in self.TOP_SYMBOLS:
            info = liq_aggregator.get_total_24h(symbol)
            clusters = liq_aggregator.get_clusters(symbol, min_usdt=5_000)

            if info["count"] == 0:
                continue

            total_long = info["long"]
            total_short = info["short"]
            total = info["total"]

            zones = []
            for c in clusters:
                zones.append({
                    "price": c["price"],
                    "side": "LONG_LIQ" if c["side"] == "long_liq" else "SHORT_LIQ",
                    "usdt": c["total_usdt"],
                    "count": c["count"],
                })

            self.data[symbol] = {
                "zones": zones[:10],
                "total_long": total_long,
                "total_short": total_short,
                "count": info["count"],
                "timestamp": time.time(),
            }

        return len(self.data)

    async def fetch_large_trades(self, extra_symbols=None):
        """
        Binance Futures REST dan katta aggTrade larni olish.
        extra_symbols: qo'shimcha symbol lar (masalan, /liqmap MANTAUSDT uchun)
        """
        symbols = list(self.TOP_SYMBOLS)
        is_single = False
        if extra_symbols:
            for s in extra_symbols:
                if s not in symbols:
                    symbols.append(s)
                    is_single = True

        min_usdt = 1000 if is_single else 5000
        results = {}
        connector = aiohttp.TCPConnector(limit=5, force_close=True)
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                for symbol in symbols:
                    if not self._running:
                        break

                    try:
                        url = f"{BINANCE_FUTURES}/fapi/v1/aggTrades"
                        params = {"symbol": symbol, "limit": 100}
                        headers = {"User-Agent": "Mozilla/5.0"}

                        async with session.get(
                            url, params=params, headers=headers,
                            timeout=aiohttp.ClientTimeout(total=8)
                        ) as resp:
                            if resp.status != 200:
                                await asyncio.sleep(0.3)
                                continue

                            trades = await resp.json()

                        for t in trades:
                            price = float(t.get("p", 0))
                            qty = float(t.get("q", 0))
                            usdt = price * qty
                            is_buyer_maker = t.get("m", False)

                            if usdt < min_usdt:
                                continue

                            if symbol not in results:
                                results[symbol] = {
                                    "long": 0.0,
                                    "short": 0.0,
                                    "count": 0,
                                    "zones": defaultdict(lambda: {"long": 0.0, "short": 0.0, "count": 0}),
                                }

                            results[symbol]["count"] += 1

                            bucket = round(price * 20) / 20

                            if is_buyer_maker:
                                results[symbol]["long"] += usdt
                                results[symbol]["zones"][bucket]["long"] += usdt
                                results[symbol]["zones"][bucket]["count"] += 1
                            else:
                                results[symbol]["short"] += usdt
                                results[symbol]["zones"][bucket]["short"] += usdt
                                results[symbol]["zones"][bucket]["count"] += 1

                        await asyncio.sleep(0.3)

                    except Exception as e:
                        logger.debug(f"Liq heatmap {symbol} error: {e}")
                        await asyncio.sleep(0.5)

        except Exception as e:
            logger.warning(f"Liq heatmap fetch error: {e}")

        total_trades = sum(d["count"] for d in results.values())
        logger.info(f"/liqmap aggTrades: {total_trades} large trades, {len(results)} symbols")
        return results

    async def build_rest_fallback(self, extra_symbols=None):
        """
        AggTrade large trades dan heatmap qurish.
        """
        rest_data = await self.fetch_large_trades(extra_symbols=extra_symbols)

        if not rest_data:
            return len(self.data)

        for symbol, d in rest_data.items():
            if symbol in self.data:
                continue

            total_long = d["long"]
            total_short = d["short"]
            total = total_long + total_short
            if total == 0:
                continue

            zones = []
            for price_level, zdata in d["zones"].items():
                if zdata["long"] > 0:
                    zones.append({
                        "price": price_level,
                        "side": "LONG_LIQ",
                        "usdt": zdata["long"],
                        "count": zdata["count"],
                    })
                if zdata["short"] > 0:
                    zones.append({
                        "price": price_level,
                        "side": "SHORT_LIQ",
                        "usdt": zdata["short"],
                        "count": zdata["count"],
                    })

            zones.sort(key=lambda x: x["usdt"], reverse=True)

            self.data[symbol] = {
                "zones": zones[:10],
                "total_long": total_long,
                "total_short": total_short,
                "count": d["count"],
                "timestamp": time.time(),
                "source": "AggTrade",
            }

        return len(self.data)

    def format_single(self, symbol: str) -> str:
        symbol = symbol.upper()
        d = self.data.get(symbol)
        if not d:
            return (
                f"⚠️ <b>{symbol}</b> uchun ma'lumot topilmadi.\n\n"
                "Bir oz kutib qayta urinib ko'ring."
            )

        tl = d["total_long"]
        ts_ = d["total_short"]
        total = tl + ts_
        count = d["count"]

        if tl > ts_ * 1.5:
            emoji = "🔴"
            bias = "LONG likvidatsiya bo'ldi"
        elif ts_ > tl * 1.5:
            emoji = "🟢"
            bias = "SHORT likvidatsiya bo'ldi"
        else:
            emoji = "⚪"
            bias = "Balans"

        lines = [
            f"🔥 <b>{symbol} LIQUIDATION MAP</b>",
            f"⏰ Yangilangan: {datetime.utcnow().strftime('%H:%M UTC')}",
            "",
            f"{emoji} <b>{symbol}</b>  ({count} ta katta trade)",
            f"  💰 ${total:,.0f}  |  {bias}",
            f"  🔴 LONG: ${tl:,.0f}  🟢 SHORT: ${ts_:,.0f}",
        ]

        if total > 0:
            long_pct = int(tl / total * 10)
            short_pct = 10 - long_pct
            bar = "█" * long_pct + "░" * short_pct
            lines.append(f"  [{bar}]")

        zones = d.get("zones", [])
        if zones:
            lines.append("")
            lines.append("💥 <b>Likvidatsiya zonalari:</b>")
            for z in zones[:5]:
                side_emoji = "🔴" if z["side"] == "LONG_LIQ" else "🟢"
                lines.append(
                    f"  {side_emoji} ${z['usdt']:,.0f} @ {z['price']:.4f}  ({z['count']}x)"
                )

        lines.append("")
        if tl > ts_:
            lines.append("💡 LONG liq ko'p → Narx tushishi mumkin")
        elif ts_ > tl:
            lines.append("💡 SHORT liq ko'p → Narx ko'tarilishi mumkin")
        else:
            lines.append("💡 Balans — yaqin orada katta harakat kutish mumkin")

        return "\n".join(lines)

    def format_text(self) -> str:
        if not self.data:
            return (
                "⚠️ <b>Liquidation Heatmap</b>\n\n"
                "Hali yetarli ma'lumot to'planmadi.\n"
                "Katta trade'lar ($5K+) kuzatilmoqda...\n"
                "bir oz kutib qayta urinib ko'ring."
            )

        lines = [
            "🔥 <b>LIQUIDATION HEATMAP</b>",
            "📊 Manba: AggTrade katta trade'lar (Likvidatsiya proxy)",
            f"⏰ Yangilangan: {datetime.utcnow().strftime('%H:%M UTC')}",
            "",
        ]

        sorted_data = sorted(
            self.data.items(),
            key=lambda x: x[1]["total_long"] + x[1]["total_short"],
            reverse=True,
        )

        total_all_long = 0
        total_all_short = 0

        for sym, d in sorted_data:
            tl = d["total_long"]
            ts_ = d["total_short"]
            total = tl + ts_
            count = d["count"]
            total_all_long += tl
            total_all_short += ts_

            if tl > ts_ * 1.5:
                emoji = "🔴"
                bias = "LONG likvidatsiya bo'ldi"
            elif ts_ > tl * 1.5:
                emoji = "🟢"
                bias = "SHORT likvidatsiya bo'ldi"
            else:
                emoji = "⚪"
                bias = "Balans"

            if total > 0:
                long_pct = int(tl / total * 10)
                short_pct = 10 - long_pct
            else:
                long_pct = 5
                short_pct = 5
            bar = "█" * long_pct + "░" * short_pct

            source_tag = " [AggTrade]" if d.get("source") == "AggTrade" else ""
            lines.append(f"{emoji} <b>{sym}</b>  ({count} ta){source_tag}")
            lines.append(f"  💰 ${total:,.0f}  |  {bias}")
            lines.append(f"  🔴 LONG: ${tl:,.0f}  🟢 SHORT: ${ts_:,.0f}")
            lines.append(f"  [{bar}]")

            top_3 = d["zones"][:3]
            if top_3:
                lines.append("  💥 Katta zonalar:")
                for z in top_3:
                    side_emoji = "🔴" if z["side"] == "LONG_LIQ" else "🟢"
                    lines.append(
                        f"    {side_emoji} ${z['usdt']:,.0f} @ {z['price']:.4f}  ({z['count']}x)"
                    )
            lines.append("")

        grand_total = total_all_long + total_all_short
        if grand_total > 0:
            lines.append("─" * 30)
            lines.append(f"📊 <b>JAMI:</b> ${grand_total:,.0f}")
            lines.append(f"  🔴 LONG liq: ${total_all_long:,.0f} ({total_all_long/grand_total*100:.0f}%)")
            lines.append(f"  🟢 SHORT liq: ${total_all_short:,.0f} ({total_all_short/grand_total*100:.0f}%)")
            lines.append("")
            lines.append("💡 LONG liq ko'p = narx tushishi mumkin")
            lines.append("💡 SHORT liq ko'p = narx ko'tarilishi mumkin")

        return "\n".join(lines)


liquidation_heatmap = LiquidationHeatmap()
