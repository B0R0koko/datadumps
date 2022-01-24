import websockets
import asyncio
import asyncpg
import requests
import time
import json
import ssl
import logging

from typing import List, Any


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)


class KucoinSocketClientUtils:
    @classmethod
    def ws_orderbook_parser(cls, response) -> List[Any]:
        changes_asks = response["data"]["changes"]["asks"]
        changes_bids = response["data"]["changes"]["bids"]
        timestamp = [round(time.time(), 2)] * len(changes_asks)
        return [changes_asks, changes_bids]


# Websocket Kucoin Client
class KucoinSocketClient:

    _switch_endpoint = "https://api.kucoin.com/api/v1/bullet-public"

    def __init__(self, subs):
        self.subs = subs
        self._ws_info = self._ask_upgrade()
        self._ws_endpoint = self._get_ws_endpoint()

    # HTTP POST request to the server with prompt to upgrade to WS
    def _ask_upgrade(self):
        ws_info = requests.post(self._switch_endpoint).json()
        logging.info("Sending upgrade request to server")
        return ws_info

    # Kucoin requires user to send prompt request first to get data for
    # initialisation of websockets stream
    def _get_ws_endpoint(self):
        endpoint = self._ws_info["data"]["instanceServers"][0]["endpoint"]
        token = self._ws_info["data"]["token"]
        connect_id = str(int(time.time()) * 1000)
        ws_endpoint = endpoint + f"?token={token}&[connectId={connect_id}]"
        return ws_endpoint

    @property
    def _ping_interval(self):
        return self._ws_info["data"]["instanceServers"][0]["pingInterval"] / 1000

    # To keep TCP connection alive client has to ping server each period of time
    async def _ping_serv(self):
        ping_msg = json.dumps({"id": str(int(time.time() * 1000)), "type": "ping"})
        logging.info("Pinging the server")
        await self._ws.ping(ping_msg)
        self._last_ping = time.time()

    # Subscribe to datastream
    async def _apply_for_stream(self):
        for sub in self.subs:
            subscription = json.dumps(
                {
                    "id": str(int(time.time() * 1000)),
                    "type": "subscribe",
                    "topic": sub,
                    "response": True,
                    "privateChannel": False,
                }
            )
            logging.info(f"Applying for subscription: {sub}")
            await self._ws.send(subscription)

    # Add data to database
    async def _execute_query(self, query, data):
        await self._conn.executemany(query, data)

    # Start stream listening
    async def listen(self, queries, parse_func):

        self._conn = await asyncpg.create_pool("postgresql://127.0.0.1:5432/postgres")

        async with websockets.connect(
            self._ws_endpoint, ssl=ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
        ) as ws:
            self._ws = ws
            # Send hello message to the server to start stream
            await ws.send("Hello World!")
            # Ping server for the first time
            await self._ping_serv()
            # Subscribe to data channels (up to 5 can be tracked)
            await self._apply_for_stream()

            while True:
                if time.time() - self._last_ping > self._ping_interval:
                    await self._ping_serv()
                response = json.loads(await asyncio.wait_for(ws.recv(), 10))
                if response["type"] == "message":
                    data = parse_func(response)
                    for query, data in zip(queries, data):
                        await self._execute_query(query, data)


if __name__ == "__main__":
    client_1 = KucoinSocketClient(["/market/level2:BTC-USDT"])
    client_2 = KucoinSocketClient(["/market/level2:LTC-USDT"])

    queries = [
        "INSERT INTO asks_updates VALUES ($1, $2, $3)",
        "INSERT INTO bids_updates VALUES ($1, $2, $3)",
    ]

    parse_func = KucoinSocketClientUtils.ws_orderbook_parser

    async def main():
        await asyncio.gather(
            client_1.listen(queries, parse_func), client_2.listen(queries, parse_func)
        )

    asyncio.run(main())
