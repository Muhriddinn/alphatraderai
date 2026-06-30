import asyncio
import aiohttp

async def test():
    async with aiohttp.ClientSession() as s:
        async with s.get("https://fapi.binance.com/fapi/v1/aggTrades?symbol=BTCUSDT&limit=5") as r:
            data = await r.json()
            print("aggTrades type:", type(data))
            print("aggTrades:", str(data)[:300])

        async with s.get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT") as r:
            data = await r.json()
            print("funding type:", type(data))
            print("funding:", data)

asyncio.run(test())
