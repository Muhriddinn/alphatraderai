"""
ALPHATRADERAI — Alert Engine v3
Consolidated alerts: 1 message per coin per signal window
Real liq clusters from WebSocket aggregator
Strategy/SL/TP with real liquidation zones as reference
CEXTrack-style activity alerts
"""
import asyncio
import random
import time
from datetime import datetime
from loguru import logger
import aiohttp

from config.settings import settings
from core.models import (
    MarketAlert, AlertLevel, Exchange, MarketType,
    VolumeEvent, OIEvent, LiquidationEvent, WhaleEvent,
    FundingEvent, OrderBookEvent
)
from core.state_manager import state_manager


# API Cache — 120 soniya TTL (kamroq REST call)
_api_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 120  # soniya (oldin 60 edi)

# Rate limiter — API call limit
from modules.cvd_tracker import cvd_tracker


def fmt(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v/1_000_000_000:.2f}B$"
    elif v >= 1_000_000:
        return f"{v/1_000_000:.1f}M$"
    elif v >= 1_000:
        return f"{v/1_000:.0f}K$"
    return f"{v:.0f}$"


class AlertEngine:
    AGGREGATE_WINDOW = 10  # seconds

    def __init__(self, alert_callback):
        self.alert_callback = alert_callback
        self._running = False
        self._buffer: dict[str, dict] = {}

        # Connected from main.py — for adding volume/OB context
        self.volume_scanner = None
        self.ob_tracker = None

        # Connected from main.py — for real liq clusters
        self.liq_aggregator = None

        # Funding history
        self._funding_history: dict[str, list] = {}

        # OI history
        self._oi_history: dict[str, list] = {}

    async def start(self):
        self._running = True
        logger.info("✅ Alert Engine v3 started")

    # ─────────────────────────────────────────
    # HELPER FUNCTIONS
    # ─────────────────────────────────────────

    async def _get_price(self, exchange: str, symbol: str) -> float:
        ticker = await state_manager.get_ticker(exchange, symbol)
        if ticker and ticker.get("price", 0) > 0:
            return ticker["price"]
        try:
            url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        d = await r.json()
                        price = float(d.get("price", 0))
                        if price > 0:
                            await state_manager.set_ticker(exchange, symbol, {"price": price, "ts": 0})
                            return price
        except Exception:
            pass
        return 0.0

    async def _get_extra_data(self, exchange: str, symbol: str) -> dict:
        """
        Extra data — CACHE + EXISTING TRACKERS dan.
        REST API faqat juda kam ishlatiladi (cache TTK 120s).
        """
        cache_key = f"{exchange}:{symbol}"
        if cache_key in _api_cache:
            cached_time, cached_data = _api_cache[cache_key]
            if time.time() - cached_time < CACHE_TTL:
                return cached_data

        extra = {
            "funding_rate": None, "oi_usdt": None,
            "price_change_24h": 0, "volume_24h": 0,
            "recent_volume_usdt": 0,
            "whale_buy_volume": 0, "whale_sell_volume": 0,
            "whale_token_qty": 0, "whale_pct_24h": 0,
            "whale_liquidity_usdt": 0, "is_whale_size": False,
            "vol_5m": 0, "vol_15m": 0, "vol_1h": 0, "vol_4h": 0,
            "price_change_1m": 0, "price_change_5m": 0,
            "price_change_1h": 0, "price_change_4h": 0,
            "ob_buy_walls": [], "ob_sell_walls": [], "ob_imbalance": 1.0,
            "cvd_1m": 0, "cvd_5m": 0, "cvd_direction": "neutral",
            "last_whale_usdt": 0, "last_whale_side": "", "last_whale_ago": 0, "last_whale_qty": 0,
            "liq_est_long": 0, "liq_est_short": 0,
            "liq_real_total": 0, "liq_real_long": 0, "liq_real_short": 0, "liq_real_count": 0,
            "taker_ratio": 0,
            "price_change_15m": 0,
        }

        price = await self._get_price(exchange, symbol)

        # ─── EXISTING TRACKERS DAN OLISH (REST kerak EMAS) ──────
        # price_tracker dan narx o'zgarishlari
        from modules.price_tracker import price_tracker
        pc = price_tracker.get_price_changes(symbol)
        if pc["current"] > 0:
            extra["price_change_1m"] = pc.get("change_1m", 0)
            extra["price_change_5m"] = pc.get("change_5m", 0)
            extra["price_change_15m"] = pc.get("change_15m", 0)
            extra["price_change_1h"] = pc.get("change_1h", 0)
            extra["price_change_4h"] = pc.get("change_4h", 0)

        # cvd_tracker dan CVD
        from modules.cvd_tracker import cvd_tracker
        cvd_data = cvd_tracker.get_cvd_data(symbol)
        if cvd_data:
            extra["cvd_1m"] = cvd_data.get("cvd_1m", 0)
            extra["cvd_5m"] = cvd_data.get("cvd_5m", 0)
            extra["cvd_15m"] = cvd_data.get("cvd_15m", 0)
            extra["cvd_direction"] = cvd_data.get("cvd_direction", "neutral")
            extra["cvd_trend"] = cvd_data.get("cvd_trend", "flat")

        # volume_scanner dan hajm + anomaly
        if hasattr(self, 'volume_scanner') and self.volume_scanner:
            vol_data = self.volume_scanner.get_volume_data(symbol)
            if vol_data:
                extra["vol_5m"] = vol_data.get("vol_5m", 0)
                extra["vol_15m"] = vol_data.get("vol_15m", 0)
                extra["vol_1h"] = vol_data.get("vol_1h", 0)
                extra["recent_volume_usdt"] = vol_data.get("recent_volume_usdt", 0)
            # Volume anomaly — spike ma'lumotlari
            current_vols = self.volume_scanner.get_current_volumes(symbol)
            if current_vols:
                extra["spike_1m"] = current_vols.get("spike_1m", 0)
                extra["spike_5m"] = current_vols.get("spike_5m", 0)
                extra["spike_15m"] = current_vols.get("spike_15m", 0)
                extra["spike_1h"] = current_vols.get("spike_1h", 0)
                extra["vol_baseline_1m"] = current_vols.get("daily_baseline_1m", 0)
                extra["vol_trend"] = current_vols.get("trend", "neutral")

        # ob_tracker dan orderbook
        if hasattr(self, 'ob_tracker') and self.ob_tracker:
            ob_data = self.ob_tracker.get_walls_with_price(symbol, price) if price > 0 else None
            if ob_data:
                extra["ob_buy_walls"] = ob_data.get("buy_walls", [])
                extra["ob_sell_walls"] = ob_data.get("sell_walls", [])
                extra["ob_imbalance"] = ob_data.get("imbalance_ratio", 1.0)

        # ─── OB REST FALLBACK — trackerda data bo'lmasa ─────────
        if not extra["ob_buy_walls"] and not extra["ob_sell_walls"] and price > 0:
            try:
                async with aiohttp.ClientSession() as session_ob:
                    url_ob = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=20"
                    async with session_ob.get(url_ob, timeout=aiohttp.ClientTimeout(total=5)) as rob:
                        if rob.status == 200:
                            dob = await rob.json()
                            bids = dob.get("bids", [])
                            asks = dob.get("asks", [])
                            WALL_USDT = 5_000
                            ob_buy_all = [{"price": float(b[0]), "usdt": float(b[0]) * float(b[1]), "dist_pct": (float(b[0]) - price) / price * 100} for b in bids[:10] if float(b[0]) * float(b[1]) >= WALL_USDT]
                            ob_sell_all = [{"price": float(a[0]), "usdt": float(a[0]) * float(a[1]), "dist_pct": (float(a[0]) - price) / price * 100} for a in asks[:10] if float(a[0]) * float(a[1]) >= WALL_USDT]
                            # Agar hech narsa topilmasa — eng katta 3 ta devorni olish
                            if not ob_buy_all and bids:
                                ob_buy_all = [{"price": float(b[0]), "usdt": float(b[0]) * float(b[1]), "dist_pct": (float(b[0]) - price) / price * 100} for b in bids[:3] if float(b[0]) * float(b[1]) > 0]
                            if not ob_sell_all and asks:
                                ob_sell_all = [{"price": float(a[0]), "usdt": float(a[0]) * float(a[1]), "dist_pct": (float(a[0]) - price) / price * 100} for a in asks[:3] if float(a[0]) * float(a[1]) > 0]
                            extra["ob_buy_walls"] = ob_buy_all[:3]
                            extra["ob_sell_walls"] = ob_sell_all[:3]
                            tb = sum(w["usdt"] for w in ob_buy_all)
                            ts = sum(w["usdt"] for w in ob_sell_all)
                            extra["ob_imbalance"] = round(tb / ts, 2) if ts > 0 else 1.0
                            logger.debug(f"OB REST fallback {symbol}: {len(ob_buy_all)} buy, {len(ob_sell_all)} sell walls")
            except Exception as e:
                logger.warning(f"OB REST fallback error {symbol}: {e}")

        # ─── FAQAT 1 TA REST CALL — 24h ticker + funding + OI ────
        try:
            async with aiohttp.ClientSession() as session:
                # Bittada 3 ta ma'lumot olish (batch)
                url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        d = await r.json()
                        extra["price_change_24h"] = float(d.get("priceChangePercent", 0))
                        extra["volume_24h"] = float(d.get("quoteVolume", 0))
                        extra["vol_4h"] = float(d.get("volume", 0))

                # Kline — faqat 5m (qolganlari price_tracker da)
                url_k = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=5m&limit=2"
                async with session.get(url_k, timeout=aiohttp.ClientTimeout(total=5)) as rk:
                    if rk.status == 200:
                        dk = await rk.json()
                        if dk and len(dk) >= 2:
                            extra["vol_5m"] = float(dk[0][7])

        except Exception as e:
            logger.debug(f"Extra data REST error {symbol}: {e}")

        # ─── LIQ — faqat real data (soxta taxmin EMAS) ──────
        # liq_est_long/short endi qo'llanilmaydi — haqiqiy klasterlar ishlatiladi

        # Agar OB data bo'sh bo'lsa — qisqa cache (keyingi signalda REST fallback ishlasin)
        has_ob = bool(extra.get("ob_buy_walls") or extra.get("ob_sell_walls"))
        cache_ttl = CACHE_TTL if has_ob else 10
        _api_cache[cache_key] = (time.time() - (CACHE_TTL - cache_ttl), extra)
        return extra

    # ─────────────────────────────────────────
    # STRATEGY CALCULATION
    # ─────────────────────────────────────────

    def _calculate_strategy(
        self, symbol: str, price: float, events: dict, extra: dict, clusters: list
    ) -> tuple[str, float, float, float, str]:
        """
        Calculate strategy direction, entry, SL, TP based on events and real liq clusters.
        Returns (direction, entry, sl, tp, reason)
        """
        if price <= 0:
            return "", 0, 0, 0, ""

        reasons = []
        bullish_score = 0
        bearish_score = 0

        # Whale direction
        whale_ev = events.get("whale")
        if whale_ev:
            if whale_ev.direction.value == "buy":
                bullish_score += 3
                reasons.append("Whale BUY")
            else:
                bearish_score += 3
                reasons.append("Whale SELL")
        elif extra.get("whale_liquidity_usdt", 0) > 0:
            buy_v = extra.get("whale_buy_volume", 0)
            sell_v = extra.get("whale_sell_volume", 0)
            if buy_v > sell_v:
                bullish_score += 2
                reasons.append("Whale bias BUY")
            elif sell_v > buy_v:
                bearish_score += 2
                reasons.append("Whale bias SELL")

        # OI — to'g'rilangan strategiya
        oi_ev = events.get("oi")
        if oi_ev:
            price_chg = extra.get("price_change_5m", 0)
            if oi_ev.change_pct > 1:
                # OI up + price up = yangi pozitsiyalar ochilmoqda → LONG
                if price_chg > 0.3:
                    bullish_score += 2
                    reasons.append(f"OI +{oi_ev.change_pct:.1f}% (yangi LONG pozitsiyalar)")
                # OI up + price down = short squeeze ehtimoli → SHORT
                elif price_chg < -0.3:
                    bearish_score += 2
                    reasons.append(f"OI +{oi_ev.change_pct:.1f}% (short squeeze ehtimoli)")
                else:
                    # OI oshdi, narx o'zgarmagan → kutilmoqda
                    bullish_score += 1
                    reasons.append(f"OI +{oi_ev.change_pct:.1f}% (kutilmoqda)")
            elif oi_ev.change_pct < -1:
                # OI down + price down = pozitsiyalar yopilmoqda → SHORT
                if price_chg < -0.3:
                    bearish_score += 2
                    reasons.append(f"OI {oi_ev.change_pct:.1f}% (pozitsiyalar yopilmoqda)")
                # OI down + price up = long squeeze ehtimoli → LONG
                elif price_chg > 0.3:
                    bullish_score += 2
                    reasons.append(f"OI {oi_ev.change_pct:.1f}% (long squeeze ehtimoli)")
                else:
                    bullish_score += 1
                    reasons.append(f"OI {oi_ev.change_pct:.1f}% (pozitsiyalar kamaymoqda)")

        # Funding
        fr = extra.get("funding_rate")
        if fr is not None:
            if fr > 0.01:
                bearish_score += 2
                reasons.append(f"Funding {fr:+.4f}% (overleveraged LONG)")
            elif fr < -0.01:
                bullish_score += 2
                reasons.append(f"Funding {fr:+.4f}% (overleveraged SHORT)")

        # CVD
        cvd_5m = extra.get("cvd_5m", 0)
        if cvd_5m > 50_000:
            bullish_score += 1
            reasons.append("CVD bullish")
        elif cvd_5m < -50_000:
            bearish_score += 1
            reasons.append("CVD bearish")

        # Volume surge
        vol_5m = extra.get("vol_5m", 0)
        vol_1h = extra.get("vol_1h", 0)
        if vol_5m > 0 and vol_1h > 0:
            vol_ratio = vol_5m / (vol_1h / 12)
            if vol_ratio > 2:
                price_chg = extra.get("price_change_5m", 0)
                if price_chg > 0.5:
                    bullish_score += 1
                elif price_chg < -0.5:
                    bearish_score += 1

        # Determine direction
        if bullish_score > bearish_score:
            direction = "LONG"
        elif bearish_score > bullish_score:
            direction = "SHORT"
        else:
            direction = "NEUTRAL"  # Default: signal yuborilmaydi

        # Find nearest real liq cluster for SL/TP reference
        sl_price = 0.0
        tp_price = 0.0

        if clusters:
            if direction == "LONG":
                # SL: nearest LONG liq cluster below price
                long_clusters = [c for c in clusters if c["side"] == "long_liq" and c["price"] < price]
                if long_clusters:
                    sl_price = long_clusters[0]["price"]
                    reasons.append(f"SL @ real liq cluster ({fmt(sl_price)}$)")

                # TP: nearest SHORT liq cluster above price
                short_clusters = [c for c in clusters if c["side"] == "short_liq" and c["price"] > price]
                if short_clusters:
                    tp_price = short_clusters[0]["price"]
                    reasons.append(f"TP @ real liq cluster ({fmt(tp_price)}$)")
            else:
                # SL: nearest SHORT liq cluster above price
                short_clusters = [c for c in clusters if c["side"] == "short_liq" and c["price"] > price]
                if short_clusters:
                    sl_price = short_clusters[0]["price"]
                    reasons.append(f"SL @ real liq cluster ({fmt(sl_price)}$)")

                # TP: nearest LONG liq cluster below price
                long_clusters = [c for c in clusters if c["side"] == "long_liq" and c["price"] < price]
                if long_clusters:
                    tp_price = long_clusters[0]["price"]
                    reasons.append(f"TP @ real liq cluster ({fmt(tp_price)}$)")

        # Fallback SL/TP if no real clusters
        if sl_price == 0:
            if direction == "LONG":
                sl_price = price * 0.97  # -3%
            else:
                sl_price = price * 1.03  # +3%

        if tp_price == 0:
            if direction == "LONG":
                tp_price = price * 1.05  # +5%
            else:
                tp_price = price * 0.95  # -5%

        reason_str = " | ".join(reasons) if reasons else "Composite signal"
        return direction, price, sl_price, tp_price, reason_str

    # ─────────────────────────────────────────
    # CEXTRACK-STYLE ACTIVITY
    # ─────────────────────────────────────────

    def _build_activity_text(
        self, symbol: str, events: dict, extra: dict
    ) -> str:
        """Build CEXTrack-style activity text"""
        parts = []

        whale_ev = events.get("whale")
        if whale_ev:
            dir_str = "BUY" if whale_ev.direction.value == "buy" else "SELL"
            parts.append(f"🐋 Whale {dir_str}: {fmt(whale_ev.volume_usdt)}")
        elif extra.get("whale_liquidity_usdt", 0) > 0:
            parts.append(f"🐋 Whale: {fmt(extra['whale_liquidity_usdt'])}")

        oi_ev = events.get("oi")
        if oi_ev:
            parts.append(f"📈 OI: {oi_ev.change_pct:+.1f}%")

        liq_ev = events.get("liq")
        if liq_ev:
            total = liq_ev.long_liq_usdt + liq_ev.short_liq_usdt
            parts.append(f"💥 Liq: {fmt(total)}")

        vol_ev = events.get("volume")
        if vol_ev:
            parts.append(f"💹 Vol: +{vol_ev.spike_pct:.0f}%")

        fr_ev = events.get("funding")
        if fr_ev:
            parts.append(f"💰 FR: {fr_ev.funding_rate:+.4f}%")

        return " | ".join(parts) if parts else ""

    # ─────────────────────────────────────────
    # BUFFER AND FLUSH
    # ─────────────────────────────────────────

    async def _add_event(self, symbol: str, exchange: Exchange, event_type: str, event):
        if symbol not in self._buffer:
            self._buffer[symbol] = {
                "events": {},
                "exchange": exchange,
                "timer": None,
                "first_event_time": datetime.utcnow(),
            }
        buf = self._buffer[symbol]
        buf["events"][event_type] = event
        buf["exchange"] = exchange

        if buf["timer"] and not buf["timer"].done():
            buf["timer"].cancel()
        buf["timer"] = asyncio.create_task(self._fire_after_window(symbol))

    async def _fire_after_window(self, symbol: str):
        await asyncio.sleep(self.AGGREGATE_WINDOW)
        await self._flush(symbol)

    async def _flush(self, symbol: str):
        if symbol not in self._buffer:
            return
        buf = self._buffer.pop(symbol)
        events = buf["events"]
        exchange = buf["exchange"]
        if not events or not self._running:
            return

        price = await self._get_price(exchange.value, symbol)
        extra = await self._get_extra_data(exchange.value, symbol)

        # Last whale fallback — formatter extra fieldlariga moslab
        if "whale" not in events:
            last_whale = await state_manager.get_last_whale(exchange.value, symbol)
            if last_whale:
                extra["last_whale_usdt"] = last_whale.get("usdt", 0)
                extra["last_whale_side"] = last_whale.get("direction", "")
                extra["last_whale_ago"] = int(time.time() - last_whale.get("ts", 0))
                extra["last_whale_qty"] = last_whale.get("qty", 0)

        # Volume scanner fallback
        vol_event = events.get("volume")
        if vol_event is None and self.volume_scanner is not None:
            cur_vols = self.volume_scanner.get_current_volumes(symbol)
            if cur_vols:
                vol_event = VolumeEvent(
                    symbol=symbol,
                    exchange=exchange,
                    spike_pct=cur_vols["spike_5m"],
                    volume_usdt=cur_vols["vol_5m"],
                    start_time=datetime.utcnow(),
                    is_whale=cur_vols["vol_5m"] >= 500_000,
                )
                vol_event.extra_volumes = cur_vols
        elif vol_event is not None and not getattr(vol_event, "extra_volumes", None) and self.volume_scanner is not None:
            cur_vols = self.volume_scanner.get_current_volumes(symbol)
            if cur_vols:
                vol_event.extra_volumes = cur_vols

        # OB tracker fallback
        ob_event = events.get("orderbook")
        if ob_event is None and self.ob_tracker is not None and price > 0:
            walls = self.ob_tracker.get_walls_with_price(symbol, price)
            if walls:
                ob_event = OrderBookEvent(
                    symbol=symbol,
                    exchange=exchange,
                    buy_wall_usdt=sum(w["usdt"] for w in walls["buy_walls"]) or 0,
                    sell_wall_usdt=sum(w["usdt"] for w in walls["sell_walls"]) or 0,
                    buy_wall_price=walls["buy_walls"][0]["price"] if walls["buy_walls"] else 0,
                    sell_wall_price=walls["sell_walls"][0]["price"] if walls["sell_walls"] else 0,
                    buy_wall_distance_pct=walls["buy_walls"][0]["dist_pct"] if walls["buy_walls"] else 0,
                    sell_wall_distance_pct=walls["sell_walls"][0]["dist_pct"] if walls["sell_walls"] else 0,
                    imbalance_ratio=walls["imbalance_ratio"],
                    current_price=price,
                )
                ob_event.extra_walls = {"buy_walls": walls["buy_walls"], "sell_walls": walls["sell_walls"]}

        cvd_data = cvd_tracker.get_cvd_data(symbol)

        # Get real liq clusters from aggregator
        liq_clusters = []
        if self.liq_aggregator is not None:
            liq_clusters = self.liq_aggregator.get_clusters(symbol, min_usdt=5_000)

        score = sum(getattr(e, "score", 10) for e in events.values())

        if score >= 40:
            level = AlertLevel.EXTREME
        elif score >= 20 or len(events) >= 3:
            level = AlertLevel.STRONG
        elif len(events) >= 2:
            level = AlertLevel.STRONG
        else:
            level = AlertLevel.NOTICE

        # Calculate strategy
        direction, entry, sl, tp, reason = self._calculate_strategy(
            symbol, price, events, extra, liq_clusters
        )

        # CEXTrack-style activity text
        activity_text = self._build_activity_text(symbol, events, extra)

        alert = MarketAlert(
            symbol=symbol,
            exchange=exchange,
            market_type=MarketType.FUTURES,
            current_price=price,
            level=level,
            total_score=score,
            timestamp=datetime.utcnow(),
            volume_event=vol_event,
            oi_event=events.get("oi"),
            liq_event=events.get("liq"),
            whale_event=events.get("whale"),
            funding_event=events.get("funding"),
            orderbook_event=ob_event,
            strategy_direction=direction,
            strategy_entry=entry,
            strategy_sl=sl,
            strategy_tp=tp,
            strategy_reason=reason,
            liq_clusters=liq_clusters,
            activity_text=activity_text,
        )
        alert.extra = extra
        alert.cvd_data = cvd_data

        event_names = " + ".join(k.upper() for k in events)
        logger.info(f"🚨 SIGNAL: {symbol} | {level.value.upper()} | {event_names} | {direction} Entry={entry:.4f} SL={sl:.4f} TP={tp:.4f}")
        await state_manager.increment_stat("alerts_sent")

        await self.alert_callback(alert)

    # ─────────────────────────────────────────
    # SCANNER CALLBACKS
    # ─────────────────────────────────────────

    async def on_volume_event(self, event: VolumeEvent):
        whale = " 🐋" if event.is_whale else ""
        logger.info(f"📊 Volume{whale}: {event.symbol} +{event.spike_pct:.0f}% | ${event.volume_usdt:,.0f}")
        await self._add_event(event.symbol, event.exchange, "volume", event)

    async def on_oi_event(self, event: OIEvent):
        direction = "📈" if event.change_pct > 0 else "📉"
        logger.info(f"{direction} OI: {event.symbol} {event.change_pct:+.2f}%")
        await self._add_event(event.symbol, event.exchange, "oi", event)

    async def on_liq_event(self, event: LiquidationEvent):
        total = event.long_liq_usdt + event.short_liq_usdt
        logger.info(f"💥 Liq: {event.symbol} | ${total:,.0f}")
        await self._add_event(event.symbol, event.exchange, "liq", event)

    async def on_whale_event(self, event: WhaleEvent):
        logger.info(f"🐋 Whale: {event.symbol} {event.direction.value} ${event.volume_usdt:,.0f}")
        await state_manager.set_last_whale(
            event.exchange.value, event.symbol,
            event.direction.value, event.volume_usdt,
            time.time()
        )

        # DARHOL whale signal yuborish (CEXTrack style)
        await self._send_whale_alert(event)

        await self._add_event(event.symbol, event.exchange, "whale", event)

    async def _send_whale_alert(self, event: WhaleEvent):
        try:
            from bot.telegram_bot import bot
            from db.models import AsyncSessionFactory, User
            from sqlalchemy import select

            extra = getattr(event, "extra", {}) or {}
            vol_24h = extra.get("volume_24h", 0) or 0
            vol_pct = (event.volume_usdt / vol_24h * 100) if vol_24h > 0 else 0

            if event.direction and event.direction.value == "neutral":
                emoji = "🤔"
                dir_word = "activity"
            elif hasattr(event, "buy_volume") and event.buy_volume > event.sell_volume:
                emoji = "💰"
                dir_word = "buying"
            elif hasattr(event, "sell_volume") and event.sell_volume > event.buy_volume:
                emoji = "💸"
                dir_word = "selling"
            else:
                emoji = "🤔"
                dir_word = "activity"

            exchange_name = "Binance Futures" if "binance" in str(event.exchange).lower() else str(event.exchange).replace("Exchange.", "")
            symbol_short = event.symbol.replace('USDT', '')

            elapsed = getattr(event, "duration_seconds", 0) or 0
            if elapsed < 60:
                time_str = f"{int(elapsed)} sec"
            elif elapsed < 3600:
                time_str = f"{int(elapsed/60)} min"
            else:
                time_str = f"{int(elapsed/3600)} h"

            text = (
                f"🎰 #{symbol_short} {dir_word} {emoji} "
                f"{fmt(event.volume_usdt)} in {time_str} ({vol_pct:.0f}%) on {exchange_name}\n"
                f"P: {event.price_change_pct:+.2f}%\n"
                f"Vol 24h: {fmt(vol_24h) if vol_24h > 0 else '—'}"
            )

            async with AsyncSessionFactory() as db:
                result = await db.execute(
                    select(User.telegram_id).where(User.is_active == True)
                )
                user_ids = [row[0] for row in result.fetchall()]

            from telegram.constants import ParseMode
            tasks = [
                bot.app.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
                for uid in user_ids
            ]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                logger.info(f"🐋 Whale signal yuborildi: {event.symbol} → {len(user_ids)} foydalanuvchi")
        except Exception as e:
            logger.error(f"Whale alert xatolik: {e}")

    async def on_funding_event(self, event: FundingEvent):
        logger.info(f"💰 Funding: {event.symbol} {event.funding_rate:+.4f}%")
        await self._add_event(event.symbol, event.exchange, "funding", event)

    async def on_orderbook_event(self, event: OrderBookEvent):
        logger.info(f"📚 OB: {event.symbol} ratio={event.imbalance_ratio:.2f}x")
        await self._add_event(event.symbol, event.exchange, "orderbook", event)

    async def stop(self):
        self._running = False
        for symbol in list(self._buffer.keys()):
            if self._buffer[symbol]["timer"]:
                self._buffer[symbol]["timer"].cancel()
            await self._flush(symbol)
