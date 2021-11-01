import asyncio, time, json, websockets
import redis


r = redis.Redis(host="localhost", port=6379, db=0)


binance_url = "wss://stream.binance.com:9443/ws"
binance_json = {
        "method": "SUBSCRIBE",
        "params": [
            "xmrusdt@bookTicker",
            "ethusdt@bookTicker",
            "xrpusdt@bookTicker"
        ],
        "id": 100}

hitbtc_url = "wss://api.hitbtc.com/api/3/ws/public"
hitbtc_json = {
    "method": "subscribe", 
    "ch": "orderbook/top/1000ms",
    "params": {"symbols": ["XMRUSDT", "ETHUSDT", "XRPUSDT"]},
    "id": 100
}


async def redis_port(coin_name, ask, bid, ask_vol, bid_vol, id):
    price_dict = {
        "ask": ask,
        "ask_vol": ask_vol,
        "bid": bid,
        "bid_vol": bid_vol,
        "time": str(int(time.time()))
    }
    hash_name = id + "_" + coin_name
    with r.pipeline() as pipe:
        for x, y in price_dict.items():
            pipe.hset(hash_name, x, y)
        pipe.execute()




async def info_call(url, sub_data, id):
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(sub_data))
        confirm = await ws.recv()
        print(confirm)
        async for message in ws:
            message = json.loads(message)
            if id == "binance":
                symbol = message["s"]
                ask = message["a"]
                bid = message["b"]
                ask_vol = message["A"]
                bid_vol = message["B"]
                await redis_port(symbol, ask, bid, ask_vol, bid_vol, id)
            elif id == "hitbtc":
                message = message["data"]
                for x, y in message.items():
                    symbol = x
                    price_dict = y
                ask = price_dict["a"]
                bid = price_dict["b"]
                ask_vol = price_dict["A"]
                bid_vol = price_dict["B"]
                await redis_port(symbol, ask, bid, ask_vol, bid_vol, id)
            else:
                print("Exchange not recognised")



async def client():
    binance_task = asyncio.create_task(info_call(binance_url, binance_json, "binance"))
    hitbtc_task = asyncio.create_task(info_call(hitbtc_url, hitbtc_json, "hitbtc"))
    await asyncio.gather(binance_task, hitbtc_task)

asyncio.run(client())




