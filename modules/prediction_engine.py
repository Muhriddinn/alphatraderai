"""
ALPHATRADERAI — Prediction Engine
Oddiy statistik pattern recognition + ML bashorat.

ISHLAYDI:
1. Data collector dan ma'lumot oladi
2. Patternlarni aniqlaydi (statistical)
3. Ehtimollik hisoblaydi
4. Signal beradi: "Narx 1h da +2% ketishi mumkin — ehtimollik 73%"

MODELLAR:
1. PatternMatch — tarixiy patternlar bilan solishtirish
2. BayesianProbability — shartli ehtimollik
3. SimpleML — sklearn bilan o'rgatilgan model
"""
import asyncio
import time
import aiosqlite
import numpy as np
from datetime import datetime
from collections import defaultdict
from loguru import logger


DB_PATH = "data/market_data.db"


class PatternMatcher:
    """
    Tarixiy patternlarni aniqlaydi.
    Masalan: "OI +0.5% + volume +30% + BTC down → keyingi 1h da +2%"
    """

    def __init__(self):
        self._patterns: dict[str, dict] = {}  # pattern_key -> {count, win_count, avg_outcome}

    async def find_patterns(self, symbol: str) -> list[dict]:
        """Shu symbol uchun patternlarni topadi"""
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("""
                SELECT price_change_1m, price_change_5m, oi_change_pct,
                       volume_spike_pct, funding_rate, cvd_1m, cvd_5m,
                       ob_imbalance_ratio, btc_change_1m, btc_change_5m,
                       outcome_1h
                FROM market_snapshots
                WHERE symbol = ? AND outcome_1h IS NOT NULL
                ORDER BY timestamp DESC LIMIT 1000
            """, (symbol,))
            rows = await cursor.fetchall()

        if not rows:
            return []

        patterns = []
        for row in rows:
            pc1m, pc5m, oi_chg, vol_spike, funding, cvd1m, cvd5m, ob_imb, btc1m, btc5m, outcome = row

            # Pattern yasash
            pattern = self._encode_pattern(pc1m, pc5m, oi_chg, vol_spike, funding, cvd1m, cvd5m, ob_imb, btc1m, btc5m)

            patterns.append({
                "pattern": pattern,
                "outcome_1h": outcome,
            })

        return patterns

    def _encode_pattern(self, pc1m, pc5m, oi_chg, vol_spike, funding, cvd1m, cvd5m, ob_imb, btc1m, btc5m) -> str:
        """Ma'lumotlarni pattern ga aylantiradi"""
        # Har bir qiymatni kategoriyaga aylantirish
        def cat(val, thresholds):
            for i, t in enumerate(thresholds):
                if val < t:
                    return i
            return len(thresholds)

        parts = []
        parts.append(f"pc1m:{cat(pc1m, [-1, -0.5, 0, 0.5, 1])}")
        parts.append(f"pc5m:{cat(pc5m, [-2, -1, 0, 1, 2])}")
        parts.append(f"oi:{cat(oi_chg, [-1, -0.3, 0, 0.3, 1])}")
        parts.append(f"vol:{cat(vol_spike, [-20, 0, 30, 100])}")
        parts.append(f"fr:{cat(funding, [-0.01, 0, 0.01])}")
        parts.append(f"cvd:{cat(cvd1m, [-50000, 0, 50000])}")
        parts.append(f"ob:{cat(ob_imb, [0.8, 1, 1.5])}")
        parts.append(f"btc:{cat(btc1m, [-1, -0.3, 0, 0.3, 1])}")

        return "|".join(parts)

    async def predict(self, symbol: str, current_features: dict) -> dict:
        """Hozirgi holatga asoslangan bashorat"""
        current_pattern = self._encode_pattern(
            current_features.get("price_change_1m", 0),
            current_features.get("price_change_5m", 0),
            current_features.get("oi_change_pct", 0),
            current_features.get("volume_spike_pct", 0),
            current_features.get("funding_rate", 0),
            current_features.get("cvd_1m", 0),
            current_features.get("cvd_5m", 0),
            current_features.get("ob_imbalance_ratio", 1),
            current_features.get("btc_change_1m", 0),
            current_features.get("btc_change_5m", 0),
        )

        patterns = await self.find_patterns(symbol)
        if not patterns:
            return {"prediction": "unknown", "confidence": 0, "pattern": current_pattern}

        # O'xshash patternlarni topish
        matches = [p for p in patterns if p["pattern"] == current_pattern]

        if not matches:
            return {"prediction": "unknown", "confidence": 0, "pattern": current_pattern}

        outcomes = [p["outcome_1h"] for p in matches]
        avg_outcome = sum(outcomes) / len(outcomes)
        positive_count = sum(1 for o in outcomes if o > 0)
        confidence = positive_count / len(outcomes) * 100

        if avg_outcome > 0.5:
            direction = "LONG"
        elif avg_outcome < -0.5:
            direction = "SHORT"
        else:
            direction = "NEUTRAL"

        return {
            "prediction": direction,
            "confidence": round(confidence, 1),
            "avg_outcome_1h": round(avg_outcome, 2),
            "sample_size": len(matches),
            "pattern": current_pattern,
        }


class BayesianPredictor:
    """
    Bayesian ehtimollik hisoblaydi.
    P(A|B) = P(B|A) × P(A) / P(B)

    Masalan:
    A = "Narx 1h da +2% ketadi"
    B = "OI +0.5%"
    P(A|B) = "OI +0.5% bo'lganda, narx +2% ketish ehtimoli"
    """

    def __init__(self):
        self._cache: dict[str, dict] = {}
        self._cache_time = 0

    async def calculate_probabilities(self, symbol: str) -> dict:
        """Shartli ehtimolliklarni hisoblaydi"""
        async with aiosqlite.connect(DB_PATH) as db:
            # Umumiy holatlar soni
            cursor = await db.execute("""
                SELECT COUNT(*) FROM market_snapshots WHERE symbol = ? AND outcome_1h IS NOT NULL
            """, (symbol,))
            total = (await cursor.fetchone())[0]

            if total < 50:
                return {"sufficient_data": False, "total_samples": total}

            results = {}

            # 1. OI change → outcome
            for oi_range, label in [((-100, -0.3), "oi_down"), ((-0.3, 0.3), "oi_neutral"), ((0.3, 100), "oi_up")]:
                cursor = await db.execute("""
                    SELECT outcome_1h FROM market_snapshots
                    WHERE symbol = ? AND oi_change_pct >= ? AND oi_change_pct < ? AND outcome_1h IS NOT NULL
                """, (symbol, oi_range[0], oi_range[1]))
                outcomes = [row[0] for row in await cursor.fetchall()]
                if outcomes:
                    avg = sum(outcomes) / len(outcomes)
                    pos = sum(1 for o in outcomes if o > 0.5) / len(outcomes) * 100
                    results[f"{label}_to_long"] = round(pos, 1)
                    results[f"{label}_avg_outcome"] = round(avg, 2)
                    results[f"{label}_count"] = len(outcomes)

            # 2. Volume spike → outcome
            for vol_range, label in [((-100, 0), "vol_low"), ((0, 50), "vol_normal"), ((50, 1000), "vol_high")]:
                cursor = await db.execute("""
                    SELECT outcome_1h FROM market_snapshots
                    WHERE symbol = ? AND volume_spike_pct >= ? AND volume_spike_pct < ? AND outcome_1h IS NOT NULL
                """, (symbol, vol_range[0], vol_range[1]))
                outcomes = [row[0] for row in await cursor.fetchall()]
                if outcomes:
                    avg = sum(outcomes) / len(outcomes)
                    pos = sum(1 for o in outcomes if o > 0.5) / len(outcomes) * 100
                    results[f"{label}_to_long"] = round(pos, 1)
                    results[f"{label}_avg_outcome"] = round(avg, 2)

            # 3. BTC direction → outcome
            for btc_range, label in [((-100, -0.3), "btc_down"), ((-0.3, 0.3), "btc_flat"), ((0.3, 100), "btc_up")]:
                cursor = await db.execute("""
                    SELECT outcome_1h FROM market_snapshots
                    WHERE symbol = ? AND btc_change_1m >= ? AND btc_change_1m < ? AND outcome_1h IS NOT NULL
                """, (symbol, btc_range[0], btc_range[1]))
                outcomes = [row[0] for row in await cursor.fetchall()]
                if outcomes:
                    avg = sum(outcomes) / len(outcomes)
                    pos = sum(1 for o in outcomes if o > 0.5) / len(outcomes) * 100
                    results[f"{label}_to_long"] = round(pos, 1)
                    results[f"{label}_avg_outcome"] = round(avg, 2)

            # 4. CVD divergence → outcome
            for cvd_range, label in [((-100000, -10000), "cvd_bear"), ((-10000, 10000), "cvd_neutral"), ((10000, 100000), "cvd_bull")]:
                cursor = await db.execute("""
                    SELECT outcome_1h FROM market_snapshots
                    WHERE symbol = ? AND cvd_1m >= ? AND cvd_1m < ? AND outcome_1h IS NOT NULL
                """, (symbol, cvd_range[0], cvd_range[1]))
                outcomes = [row[0] for row in await cursor.fetchall()]
                if outcomes:
                    avg = sum(outcomes) / len(outcomes)
                    pos = sum(1 for o in outcomes if o > 0.5) / len(outcomes) * 100
                    results[f"{label}_to_long"] = round(pos, 1)
                    results[f"{label}_avg_outcome"] = round(avg, 2)

            # 5. Combined: OI up + Volume high + BTC up → ?
            cursor = await db.execute("""
                SELECT outcome_1h FROM market_snapshots
                WHERE symbol = ? AND oi_change_pct > 0.3 AND volume_spike_pct > 30 AND btc_change_1m > 0.1 AND outcome_1h IS NOT NULL
            """, (symbol,))
            outcomes = [row[0] for row in await cursor.fetchall()]
            if outcomes:
                avg = sum(outcomes) / len(outcomes)
                pos = sum(1 for o in outcomes if o > 0.5) / len(outcomes) * 100
                results["combo_bull_to_long"] = round(pos, 1)
                results["combo_bull_avg"] = round(avg, 2)
                results["combo_bull_count"] = len(outcomes)

            # 6. Combined: OI up + Volume high + BTC down → ?
            cursor = await db.execute("""
                SELECT outcome_1h FROM market_snapshots
                WHERE symbol = ? AND oi_change_pct > 0.3 AND volume_spike_pct > 30 AND btc_change_1m < -0.1 AND outcome_1h IS NOT NULL
            """, (symbol,))
            outcomes = [row[0] for row in await cursor.fetchall()]
            if outcomes:
                avg = sum(outcomes) / len(outcomes)
                pos = sum(1 for o in outcomes if o > 0.5) / len(outcomes) * 100
                results["combo_diverge_to_long"] = round(pos, 1)
                results["combo_diverge_avg"] = round(avg, 2)
                results["combo_diverge_count"] = len(outcomes)

            results["sufficient_data"] = True
            results["total_samples"] = total
            return results

    async def predict(self, symbol: str, features: dict) -> dict:
        """Hozirgi holatga asoslangan bashorat"""
        probs = await self.calculate_probabilities(symbol)

        if not probs.get("sufficient_data"):
            return {"prediction": "unknown", "confidence": 0, "reason": "Yetarli data yo'q"}

        # Har bir feature bo'yicha bashorat
        votes = []
        weights = []

        # OI
        oi_chg = features.get("oi_change_pct", 0)
        if oi_chg > 0.3:
            p = probs.get("oi_up_to_long", 50)
            votes.append(1 if p > 50 else -1)
            weights.append(abs(oi_chg) * 2)
        elif oi_chg < -0.3:
            p = probs.get("oi_down_to_long", 50)
            votes.append(1 if p > 50 else -1)
            weights.append(abs(oi_chg) * 2)

        # Volume
        vol_spike = features.get("volume_spike_pct", 0)
        if vol_spike > 30:
            p = probs.get("vol_high_to_long", 50)
            votes.append(1 if p > 50 else -1)
            weights.append(min(vol_spike / 30, 3))

        # BTC
        btc_chg = features.get("btc_change_1m", 0)
        if abs(btc_chg) > 0.1:
            if btc_chg > 0.1:
                p = probs.get("btc_up_to_long", 50)
            else:
                p = probs.get("btc_down_to_long", 50)
            votes.append(1 if p > 50 else -1)
            weights.append(min(abs(btc_chg) * 3, 3))

        # CVD
        cvd = features.get("cvd_1m", 0)
        if abs(cvd) > 10000:
            if cvd > 10000:
                p = probs.get("cvd_bull_to_long", 50)
            else:
                p = probs.get("cvd_bear_to_long", 50)
            votes.append(1 if p > 50 else -1)
            weights.append(min(abs(cvd) / 50000, 2))

        if not votes:
            return {"prediction": "NEUTRAL", "confidence": 0, "reason": "Signal aniqlanmadi"}

        # Weighted vote
        weighted_sum = sum(v * w for v, w in zip(votes, weights))
        total_weight = sum(weights)

        if total_weight > 0:
            confidence = abs(weighted_sum) / total_weight * 100
        else:
            confidence = 0

        if weighted_sum > 0:
            direction = "LONG"
        elif weighted_sum < 0:
            direction = "SHORT"
        else:
            direction = "NEUTRAL"

        return {
            "prediction": direction,
            "confidence": round(confidence, 1),
            "votes": len(votes),
            "weighted_score": round(weighted_sum, 2),
        }


class PredictionEngine:
    """
    Asosiy bashorat tizimi.
    PatternMatcher + BayesianPredictor ni birlashtiradi.
    """

    def __init__(self):
        self.pattern_matcher = PatternMatcher()
        self.bayesian = BayesianPredictor()
        self._running = False
        self._predictions: dict[str, dict] = {}  # symbol -> latest prediction

    async def start(self):
        self._running = True
        asyncio.create_task(self._prediction_loop())
        logger.info("✅ Prediction Engine started")

    async def _prediction_loop(self):
        """Har 60 soniyada bashoratlarni yangilaydi"""
        while self._running:
            try:
                await self._update_predictions()
            except Exception as e:
                logger.debug(f"Prediction error: {e}")
            await asyncio.sleep(60)

    async def _update_predictions(self):
        """Active symbollar uchun bashorat hisoblaydi"""
        from core.state_manager import state_manager
        from modules.price_tracker import price_tracker
        from modules.cvd_tracker import cvd_tracker

        symbols = await state_manager.get_symbols("binance", "futures")
        updated = 0

        for symbol in list(symbols)[:100]:  # Top 100 symbol
            try:
                features = await self._get_features(symbol, price_tracker, cvd_tracker)
                if not features:
                    continue

                # Pattern match
                pattern_result = await self.pattern_matcher.predict(symbol, features)

                # Bayesian
                bayesian_result = await self.bayesian.predict(symbol, features)

                # Kombinatsiya
                combined = self._combine_predictions(pattern_result, bayesian_result)
                combined["symbol"] = symbol
                combined["timestamp"] = time.time()
                combined["features"] = features

                self._predictions[symbol] = combined
                updated += 1

            except Exception as e:
                logger.debug(f"Prediction error {symbol}: {e}")

        if updated > 0:
            logger.info(f"🔮 Prediction: {updated} symbol yangilandi")

    async def _get_features(self, symbol: str, price_tracker, cvd_tracker) -> dict | None:
        """Symbol uchun feature'larni oladi"""
        from core.state_manager import state_manager

        pc = price_tracker.get_price_changes(symbol)
        price = pc.get("current", 0)
        if price <= 0:
            return None

        cvd = cvd_tracker.get_cvd_data(symbol)
        btc_pc = price_tracker.get_price_changes("BTCUSDT")

        oi_history = await state_manager.get_oi_history("binance", symbol, 2)
        oi_change = 0
        if oi_history and len(oi_history) >= 2:
            prev = oi_history[1].get("oi_usdt", 0)
            curr = oi_history[0].get("oi_usdt", 0)
            if prev > 0:
                oi_change = (curr - prev) / prev * 100

        funding_current, funding_prev = await state_manager.get_funding("binance", symbol)
        funding_rate = funding_current.get("rate", 0) if funding_current else 0

        volume_spike = 0.0
        try:
            from modules.volume_scanner import volume_scanner
            vol_key = f"binance:{symbol}"
            vol_window = volume_scanner._volume_windows.get(vol_key, [])
            now_ts = time.time()
            vol_5m = sum(v["usdt"] for v in vol_window if now_ts - v["ts"] <= 300)
            vol_5m_prev = sum(v["usdt"] for v in vol_window if 300 < now_ts - v["ts"] <= 600)
            if vol_5m_prev > 0:
                volume_spike = (vol_5m - vol_5m_prev) / vol_5m_prev * 100
        except Exception:
            pass

        ob_imbalance = 1.0
        try:
            from modules.bookmap_engine import bookmap_engine
            ob = bookmap_engine.get_current_ob(symbol)
            if ob:
                bids = ob.get("bids", [])
                asks = ob.get("asks", [])
                if bids and asks:
                    total_bid = sum(b[2] for b in bids)
                    total_ask = sum(a[2] for a in asks)
                    if total_ask > 0:
                        ob_imbalance = total_bid / total_ask
        except Exception:
            pass

        return {
            "price": price,
            "price_change_1m": pc.get("change_1m", 0),
            "price_change_5m": pc.get("change_5m", 0),
            "price_change_1h": pc.get("change_1h", 0),
            "oi_change_pct": oi_change,
            "volume_spike_pct": volume_spike,
            "funding_rate": funding_rate,
            "cvd_1m": cvd.get("cvd_1m", 0),
            "cvd_5m": cvd.get("cvd_5m", 0),
            "ob_imbalance_ratio": ob_imbalance,
            "btc_change_1m": btc_pc.get("change_1m", 0),
            "btc_change_5m": btc_pc.get("change_5m", 0),
        }

    def _combine_predictions(self, pattern: dict, bayesian: dict) -> dict:
        """Ikkala bashoratni birlashtiradi"""
        p_pred = pattern.get("prediction", "unknown")
        b_pred = bayesian.get("prediction", "unknown")
        p_conf = pattern.get("confidence", 0)
        b_conf = bayesian.get("confidence", 0)

        if p_pred == b_pred and p_pred != "unknown":
            # Ikkalasi ham bir xil — ishonch oshadi
            return {
                "prediction": p_pred,
                "confidence": min((p_conf + b_conf) / 2 + 10, 95),
                "method": "combined",
                "pattern_conf": p_conf,
                "bayesian_conf": b_conf,
            }
        elif p_pred != "unknown" and p_conf > 60:
            return {
                "prediction": p_pred,
                "confidence": p_conf,
                "method": "pattern",
                "pattern_conf": p_conf,
                "bayesian_conf": b_conf,
            }
        elif b_pred != "unknown" and b_conf > 60:
            return {
                "prediction": b_pred,
                "confidence": b_conf,
                "method": "bayesian",
                "pattern_conf": p_conf,
                "bayesian_conf": b_conf,
            }
        else:
            return {
                "prediction": "NEUTRAL",
                "confidence": 0,
                "method": "none",
                "pattern_conf": p_conf,
                "bayesian_conf": b_conf,
            }

    def _heuristic_predict(self, features: dict) -> dict:
        """
        Ikkala algorit ham 'unknown' bo'lganda, real-time feature'lar
        asosida oddiy bashorat. Har bir feature ovoz beradi.
        """
        votes = []
        weights = []

        pc1m = features.get("price_change_1m", 0)
        pc5m = features.get("price_change_5m", 0)
        if pc5m > 0.5:
            votes.append(1); weights.append(1.0)
        elif pc5m < -0.5:
            votes.append(-1); weights.append(1.0)
        elif pc1m > 0.3:
            votes.append(1); weights.append(0.5)
        elif pc1m < -0.3:
            votes.append(-1); weights.append(0.5)

        oi_chg = features.get("oi_change_pct", 0)
        if oi_chg > 1.0:
            votes.append(1); weights.append(1.5)
        elif oi_chg < -1.0:
            votes.append(-1); weights.append(1.5)

        fr = features.get("funding_rate", 0)
        if fr < -0.01:
            votes.append(1); weights.append(1.0)
        elif fr > 0.01:
            votes.append(-1); weights.append(1.0)

        cvd = features.get("cvd_1m", 0)
        if cvd > 50000 and pc1m < -0.2:
            votes.append(1); weights.append(1.5)
        elif cvd < -50000 and pc1m > 0.2:
            votes.append(-1); weights.append(1.5)

        ob = features.get("ob_imbalance_ratio", 1.0)
        if ob > 1.5:
            votes.append(1); weights.append(1.0)
        elif ob < 0.67:
            votes.append(-1); weights.append(1.0)

        btc1m = features.get("btc_change_1m", 0)
        if abs(btc1m) > 0.3:
            votes.append(1 if btc1m > 0 else -1); weights.append(0.8)

        if not votes:
            return {"prediction": "NEUTRAL", "confidence": 0, "method": "heuristic_empty"}

        w_sum = sum(v * w for v, w in zip(votes, weights))
        total_w = sum(weights)
        confidence = min(abs(w_sum) / total_w * 100, 80) if total_w > 0 else 0

        if w_sum > 0:
            direction = "LONG"
        elif w_sum < 0:
            direction = "SHORT"
        else:
            direction = "NEUTRAL"

        return {
            "prediction": direction,
            "confidence": round(confidence, 1),
            "method": "heuristic",
            "votes_used": len(votes),
        }

    async def predict(self, symbol: str) -> dict | None:
        """Bitta symbol uchun bashorat — /predict uchun"""
        from modules.price_tracker import price_tracker
        from modules.cvd_tracker import cvd_tracker

        features = await self._get_features(symbol, price_tracker, cvd_tracker)
        if not features:
            return None

        pattern_result = await self.pattern_matcher.predict(symbol, features)
        bayesian_result = await self.bayesian.predict(symbol, features)
        combined = self._combine_predictions(pattern_result, bayesian_result)

        if combined["method"] == "none" or combined["confidence"] == 0:
            heuristic = self._heuristic_predict(features)
            if heuristic["prediction"] != "NEUTRAL" and heuristic["confidence"] > 0:
                combined = {
                    "prediction": heuristic["prediction"],
                    "confidence": heuristic["confidence"],
                    "method": "heuristic",
                    "pattern_conf": pattern_result.get("confidence", 0),
                    "bayesian_conf": bayesian_result.get("confidence", 0),
                }

        if combined["method"] == "none" or combined["confidence"] == 0:
            btc_result = await self.pattern_matcher.predict("BTCUSDT", features)
            if btc_result.get("prediction") not in ("unknown", "NEUTRAL") and btc_result.get("confidence", 0) > 0:
                combined = {
                    "prediction": btc_result["prediction"],
                    "confidence": max(btc_result["confidence"] * 0.6, 15),
                    "method": "btc_reference",
                    "pattern_conf": btc_result.get("confidence", 0),
                    "bayesian_conf": 0,
                }

        price = features["price"]
        direction = combined["prediction"]
        confidence = combined["confidence"]

        reasons = []
        if combined.get("method") == "heuristic":
            reasons.append("Real-time feature'lar asosida")
        if features.get("oi_change_pct", 0) > 1.0:
            reasons.append(f"OI oshdi +{features['oi_change_pct']:.1f}%")
        elif features.get("oi_change_pct", 0) < -1.0:
            reasons.append(f"OI kamaydi {features['oi_change_pct']:.1f}%")
        if features.get("funding_rate", 0) < -0.01:
            reasons.append("Funding manfiy — SHORT squeeze mumkin")
        elif features.get("funding_rate", 0) > 0.01:
            reasons.append("Funding ijobiy — LONG bosim")
        if features.get("volume_spike_pct", 0) > 30:
            reasons.append(f"Hajm oshdi +{features['volume_spike_pct']:.0f}%")
        ob_val = features.get("ob_imbalance_ratio", 1.0)
        if ob_val > 1.5:
            reasons.append(f"Buy bosimi {ob_val:.1f}x")
        elif ob_val < 0.67:
            reasons.append(f"Sell bosimi {1/ob_val:.1f}x")

        if direction == "LONG":
            target = price * 1.03
            stop_loss = price * 0.98
        elif direction == "SHORT":
            target = price * 0.97
            stop_loss = price * 1.02
        else:
            target = price
            stop_loss = price

        potential_return = (target - price) / price * 100

        algo_name = {
            "pattern": "PatternMatch",
            "bayesian": "Bayesian",
            "combined": "PatternMatch + Bayesian",
            "heuristic": "Real-time Heuristic",
            "btc_reference": "BTC Reference",
            "none": "None",
        }.get(combined.get("method", "none"), combined.get("method", "none"))

        return {
            "direction": direction,
            "confidence": confidence,
            "current_price": price,
            "target_price": target,
            "stop_loss": stop_loss,
            "potential_return": potential_return,
            "algorithm": algo_name,
            "data_points": pattern_result.get("votes", 0) + bayesian_result.get("votes", 0),
            "reasons": reasons,
        }

    def get_prediction(self, symbol: str) -> dict | None:
        """Symbol uchun oxirgi bashoratni qaytaradi"""
        return self._predictions.get(symbol)

    def get_all_predictions(self) -> dict:
        """Barcha bashoratlar"""
        return self._predictions.copy()

    def get_stats(self) -> dict:
        """ML statistikasi — /mlstats uchun"""
        total = len(self._predictions)
        return {
            "total_predictions": total,
            "correct_predictions": 0,
            "accuracy": 0.0,
            "patterns_count": len(self.pattern_matcher._patterns),
            "cached_symbols": total,
        }

    def get_cache_stats(self) -> dict:
        """Cache holati — /mlstats uchun"""
        total = len(self._predictions)
        return {
            "cached": total,
            "patterns": len(self.pattern_matcher._patterns),
            "memory_mb": round(total * 0.001, 2),
        }

    async def get_performance(self) -> dict:
        """Bashoratlar sifatini baholash"""
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("""
                SELECT COUNT(*) FROM signals_log WHERE status != 'active'
            """)
            total = (await cursor.fetchone())[0]

            if total == 0:
                return {"total": 0, "winrate": 0}

            cursor = await db.execute("""
                SELECT COUNT(*) FROM signals_log WHERE hit_tp = 1
            """)
            wins = (await cursor.fetchone())[0]

            cursor = await db.execute("""
                SELECT COUNT(*) FROM signals_log WHERE hit_sl = 1
            """)
            losses = (await cursor.fetchone())[0]

            cursor = await db.execute("""
                SELECT AVG(final_pnl_pct) FROM signals_log WHERE status != 'active'
            """)
            avg_pnl = (await cursor.fetchone())[0] or 0

            return {
                "total": total,
                "wins": wins,
                "losses": losses,
                "winrate": round(wins / total * 100, 1) if total > 0 else 0,
                "avg_pnl": round(avg_pnl, 2),
            }

    async def stop(self):
        self._running = False


# Global instance
prediction_engine = PredictionEngine()
