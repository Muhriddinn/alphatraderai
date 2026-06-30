"""
ALPHATRADERAI — API Rate Limiter
Binance ban dan himoya: max 2000 req/min
"""
import asyncio
import time
from loguru import logger


class RateLimiter:
    def __init__(self, max_per_minute: int = 2000):
        self._max = max_per_minute
        self._window: list[float] = []
        self._lock = asyncio.Lock()
        self._total_requests = 0
        self._total_waits = 0

    async def acquire(self):
        async with self._lock:
            now = time.time()
            cutoff = now - 60
            self._window = [t for t in self._window if t > cutoff]

            if len(self._window) >= self._max:
                sleep_time = self._window[0] - cutoff + 0.1
                if sleep_time > 0:
                    self._total_waits += 1
                    if self._total_waits % 100 == 0:
                        logger.warning(f"Rate limiter: {self._total_waits} waits, {len(self._window)}/min used")
                    await asyncio.sleep(sleep_time)
                    now = time.time()
                    cutoff = now - 60
                    self._window = [t for t in self._window if t > cutoff]

            self._window.append(now)
            self._total_requests += 1

    async def wait_if_needed(self):
        async with self._lock:
            now = time.time()
            cutoff = now - 60
            self._window = [t for t in self._window if t > cutoff]

            if len(self._window) >= self._max * 0.9:
                sleep_time = self._window[0] - cutoff + 0.5
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

    def get_stats(self) -> dict:
        now = time.time()
        cutoff = now - 60
        recent = [t for t in self._window if t > cutoff]
        return {
            "requests_per_min": len(recent),
            "max_per_min": self._max,
            "total_requests": self._total_requests,
            "total_waits": self._total_waits,
            "utilization_pct": round(len(recent) / self._max * 100, 1)
        }


class RetryHandler:
    def __init__(self, max_retries: int = 3, base_delay: float = 2.0, max_delay: float = 60.0):
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

    async def execute_with_retry(self, func, *args, **kwargs):
        for attempt in range(self._max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if hasattr(e, 'status') and e.status == 429:
                    if attempt < self._max_retries:
                        delay = min(self._base_delay * (2 ** attempt), self._max_delay)
                        logger.warning(f"429 on attempt {attempt+1}, retrying in {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue
                raise
        return None


rate_limiter = RateLimiter(max_per_minute=2000)
retry_handler = RetryHandler()
