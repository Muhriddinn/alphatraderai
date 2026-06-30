import asyncio
import websockets
import orjson

async def test():
    try:
        url = "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade"
        async with websockets.connect(url, ping_interval=20, open_timeout=10) as ws:
            print("WS connected!")
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            d = orjson.loads(raw)
            t = d["data"]
            price = float(t["p"])
            qty = float(t["q"])
            usdt = price * qty
            print(f"BTC trade: {price:,.0f}$ x {qty:.4f} = {usdt:,.0f}$")
    except Exception as e:
        print(f"WS ERROR: {e}")

asyncio.run(test())
