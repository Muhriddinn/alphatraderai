"""
Pre-Signal Detector — Narx o'zgarishidan OLDIN signal beradi
Indicator emas — to'plangan ma'lumotlarni tahlil qiladi:
1. OI oshyapti + narx turgan = Accumulation → Narx ko'tarilishi mumkin
2. Volume oshyapti + narx turgan = Loading → Tez orada harakat
3. OB imbalance = Buy bosimi oshyapti → LONG signal
4. CVD divergence = Smart money harakati → Reversal mumkin
"""
import asyncio
import time
from collections import defaultdict
from loguru import logger


class PreSignalDetector:
    """
    Narx o'zgarishidan 5-15 daqiqa OLDIN signal beradi.
    Bitta indicator emas — BARCHASI BIRGELIKDA ishlashi kerak.
    """

    def __init__(self):
        self._running = False
        self._signals: list[dict] = []
        self._history: dict[str, list[dict]] = defaultdict(list)
        self._cooldown: dict[str, float] = {}

    async def start(self):
        self._running = True
        asyncio.create_task(self._scan_loop())
        logger.info("🔮 Pre-Signal Detector started")

    async def stop(self):
        self._running = False

    async def _scan_loop(self):
        """Har 30 soniyada barcha activelarni tekshiradi"""
        await asyncio.sleep(120)  # Bot o'rnashsin
        while self._running:
            try:
                await self._scan_all()
            except Exception as e:
                logger.debug(f"PreSignal scan error: {e}")
            await asyncio.sleep(30)

    async def _scan_all(self):
        """Barcha activelarni tekshirish"""
        from core.state_manager import state_manager
        from modules.price_tracker import price_tracker
        from modules.cvd_tracker import cvd_tracker

        symbols = list(await state_manager.get_symbols("binance", "futures"))
        now = time.time()

        for symbol in symbols[:200]:
            try:
                score, reasons, direction = await self._evaluate(symbol, price_tracker, cvd_tracker)
                if score >= 1 and direction != "NEUTRAL":
                    cooldown_key = f"{symbol}:{direction}"
                    last = self._cooldown.get(cooldown_key, 0)
                    if now - last < 600:  # 10 daqiqa cooldown
                        continue
                    self._cooldown[cooldown_key] = now

                    signal = {
                        "symbol": symbol,
                        "direction": direction,
                        "score": score,
                        "reasons": reasons,
                        "timestamp": now,
                    }
                    self._signals.insert(0, signal)
                    self._signals = self._signals[:50]  # Oxirgi 50 ta

                    emoji = "🟢" if direction == "LONG" else "🔴"
                    logger.info(
                        f"🔮 PRE-SIGNAL: {emoji} {symbol} {direction} "
                        f"(Score: {score}/5) | {', '.join(reasons)}"
                    )
            except Exception:
                pass

    async def _evaluate(self, symbol, price_tracker, cvd_tracker) -> tuple[int, list[str], str]:
        """Bitta symbol uchun baholash — score 0-5"""
        score = 0
        reasons = []
        direction_votes = []

        pc = price_tracker.get_price_changes(symbol)
        price = pc.get("current", 0)
        if price <= 0:
            return 0, [], "NEUTRAL"

        change_1m = pc.get("change_1m", 0)
        change_5m = pc.get("change_5m", 0)

        # ─── 1. OI ACCUMULATION DETECTION ───
        from core.state_manager import state_manager as sm
        oi_history = await sm.get_oi_history("binance", symbol, 10)
        oi_usdt = oi_history[0].get("oi_usdt", 0) if oi_history else 0
        if oi_usdt > 0:
            # OI yuqori + narx turgan = accumulation
            if abs(change_1m) < 0.3 and oi_usdt > 1_000_000:
                score += 1
                reasons.append("OI steady+high")
                # OI oshyapti — history dan solishtirish
                oi_history = await sm.get_oi_history("binance", symbol, 10)
                if len(oi_history) >= 2:
                    prev_usdt = oi_history[1].get("oi_usdt", 0)
                    if prev_usdt > 0:
                        oi_change = (oi_usdt - prev_usdt) / prev_usdt * 100
                        if oi_change > 1.0:
                            score += 1
                            reasons.append(f"OI +{oi_change:.1f}%")
                            direction_votes.append("LONG")
                        elif oi_change < -1.0:
                            score += 1
                            reasons.append(f"OI {oi_change:.1f}%")
                            direction_votes.append("SHORT")

        # ─── 2. VOLUME SPIKE (real volume from volume_scanner) ───
        try:
            from modules.volume_scanner import volume_scanner
            vol_key = f"binance:{symbol}"
            vol_window = volume_scanner._volume_windows.get(vol_key, [])
            import time as _time
            now_ts = _time.time()
            vol_5m = sum(v["usdt"] for v in vol_window if now_ts - v["ts"] <= 300)
            vol_prev = sum(v["usdt"] for v in vol_window if 300 < now_ts - v["ts"] <= 600)
            if vol_prev > 0 and vol_5m > vol_prev * 1.3 and abs(change_1m) < 0.5:
                score += 1
                reasons.append(f"Vol spike {vol_5m/vol_prev:.1f}x")
                if vol_5m > vol_prev * 2:
                    score += 1
                    reasons.append("Massive vol")
        except Exception:
            pass

        # ─── 3. OB IMBALANCE ───
        from modules.bookmap_engine import bookmap_engine
        ob = bookmap_engine.get_current_ob(symbol)
        if ob:
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            if bids and asks:
                total_bid = sum(b[2] for b in bids)
                total_ask = sum(a[2] for a in asks)
                if total_ask > 0:
                    ratio = total_bid / total_ask
                    if ratio > 2.0:
                        score += 1
                        reasons.append(f"OB bias {ratio:.1f}x BUY")
                        direction_votes.append("LONG")
                    elif ratio < 0.5:
                        score += 1
                        reasons.append(f"OB bias {ratio:.1f}x SELL")
                        direction_votes.append("SHORT")

        # ─── 4. CVD DIVERGENCE ───
        cvd = cvd_tracker.get_cvd_data(symbol)
        if cvd:
            cvd_1m = cvd.get("cvd_1m", 0)
            # CVD ijobiy + narx tushyapti = accumulation (reversal)
            if cvd_1m > 50_000 and change_1m < -0.2:
                score += 1
                reasons.append("CVD bullish divergence")
                direction_votes.append("LONG")
            elif cvd_1m < -50_000 and change_1m > 0.2:
                score += 1
                reasons.append("CVD bearish divergence")
                direction_votes.append("SHORT")

        # ─── 5. FUNDING RATE EXTREME ───
        funding_data, funding_prev = await sm.get_funding("binance", symbol)
        if funding_data:
            rate = funding_data.get("rate", 0)
            if rate < -0.01:
                score += 1
                reasons.append(f"Funding {rate*100:.3f}% SHORT")
                direction_votes.append("LONG")
            elif rate > 0.01:
                score += 1
                reasons.append(f"Funding {rate*100:.3f}% LONG")
                direction_votes.append("SHORT")

        # ─── DIRECTION DECISION ───
        if not direction_votes:
            direction = "NEUTRAL"
        else:
            long_votes = direction_votes.count("LONG")
            short_votes = direction_votes.count("SHORT")
            if long_votes > short_votes:
                direction = "LONG"
            elif short_votes > long_votes:
                direction = "SHORT"
            else:
                direction = "NEUTRAL"

        return score, reasons, direction

    def get_recent_signals(self, count: int = 10) -> list[dict]:
        """Oxirgi pre-signallar"""
        return self._signals[:count]

    def format_text(self) -> str:
        """Pre-signallarni formatlangan matn sifatida qaytaradi"""
        signals = self.get_recent_signals(10)
        if not signals:
            return (
                "🔮 <b>PRE-SIGNAL DETECTOR</b>\n\n"
                "Hali pre-signal aniqlanmadi.\n"
                "Bot ishlagan sari ma'lumot to'planadi.\n"
                "OI + Volume + OB + CVD + Funding convergences kuzatilmoqda..."
            )

        lines = [
            "🔮 <b>PRE-SIGNAL DETECTOR</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "💡 Narx o'zgarishidan OLDIN signal",
            "",
        ]

        for s in signals:
            emoji = "🟢" if s["direction"] == "LONG" else "🔴"
            age = time.time() - s["timestamp"]
            if age < 60:
                age_str = f"{int(age)}s"
            elif age < 3600:
                age_str = f"{int(age/60)}m"
            else:
                age_str = f"{int(age/3600)}h"

            lines.append(f"{emoji} <b>{s['symbol']}</b> → {s['direction']}")
            lines.append(f"  📊 Score: {s['score']}/5 | {age_str} oldin")
            lines.append(f"  💡 {', '.join(s['reasons'])}")
            lines.append("")

        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("📊 Score 3+ = kuchli convergencia")
        lines.append("💡 OI+Volume+OB+CVD birgalikda = ishonchli")

        return "\n".join(lines)


pre_signal_detector = PreSignalDetector()
