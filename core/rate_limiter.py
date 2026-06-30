"""
ALPHATRADERAI — Binance Weight-Based Rate Limiter
Binance 2400 weight/minut limitiga moslashtirilgan
"""
import asyncio
import time
from loguru import logger


class WeightRateLimiter:
    def __init__(self, max_weight_per_min: int = 2000):
        self._max = max_weight_per_min
        self._window: list[tuple[float, int]] = []
        self._lock = asyncio.Lock()
        self._total_requests = 0
        self._total_weight = 0
        self._total_waits = 0

    async def acquire(self, weight: int = 1):
        async with self._lock:
            now = time.time()
            cutoff = now - 60
            self._window = [(t, w) for t, w in self._window if t > cutoff]
            current_weight = sum(w for _, w in self._window)

            while current_weight + weight > self._max:
                if self._window:
                    oldest_time, oldest_weight = self._window[0]
                    sleep_time = oldest_time - cutoff + 0.1
                    if sleep_time > 0:
                        self._total_waits += 1
                        if self._total_waits % 50 == 0:
                            logger.warning(
                                f"Rate limiter: {current_weight}/{self._max} weight used, "
                                f"sleeping {sleep_time:.1f}s"
                            )
                        await asyncio.sleep(sleep_time)
                        now = time.time()
                        cutoff = now - 60
                        self._window = [(t, w) for t, w in self._window if t > cutoff]
                        current_weight = sum(w for _, w in self._window)
                else:
                    break

            self._window.append((now, weight))
            self._total_requests += 1
            self._total_weight += weight

    def get_stats(self) -> dict:
        now = time.time()
        cutoff = now - 60
        recent = [(t, w) for t, w in self._window if t > cutoff]
        current_weight = sum(w for _, w in recent)
        return {
            "weight_per_min": current_weight,
            "max_weight_per_min": self._max,
            "requests_per_min": len(recent),
            "total_requests": self._total_requests,
            "total_weight": self._total_weight,
            "total_waits": self._total_waits,
            "utilization_pct": round(current_weight / self._max * 100, 1)
        }


class RetryHandler:
    def __init__(self, max_retries: int = 3, base_delay: float = 5.0, max_delay: float = 120.0):
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay

    async def handle_429(self, response) -> bool:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                wait = float(retry_after)
            except ValueError:
                wait = self._base_delay
        else:
            wait = self._base_delay
        wait = min(wait, self._max_delay)
        logger.warning(f"Rate limited (429). Waiting {wait:.1f}s...")
        await asyncio.sleep(wait)
        return True

    async def handle_418(self) -> bool:
        wait = 120
        logger.error(f"⛔ IP BAN (418). Waiting {wait}s...")
        await asyncio.sleep(wait)
        return True


rate_limiter = WeightRateLimiter(max_weight_per_min=2000)
retry_handler = RetryHandler()
