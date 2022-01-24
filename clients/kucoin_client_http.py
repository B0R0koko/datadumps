from clients.http_client import HttpClient, HttpClientUtils

import base64
import hmac
import hashlib
import time
import aiohttp

from typing import List, Any


# Class with variable functions used to create parameters for requests
class KucoinClientUtils:

    _tick_to_sec_map = {
        "1min": 60,
        "3min": 180,
        "5min": 300,
        "15min": 900,
        "30min": 1800,
        "60min": 3600,
    }

    # Generate requests with changing time intervals for parsing klines
    # --Example-- type=1min&symbol=BTC-USDT&startAt=1566703297&endAt=1566789757
    @classmethod
    def get_kline_reqs(
        cls, symbol: str, start: int, end: int, step: str, limit=1500
    ) -> list:
        reqs = []
        req = "GET /api/v1/market/candles?type={}&symbol={}&startAt={}&endAt={}"
        step_sec = cls._tick_to_sec_map[step] * limit
        intervals = HttpClientUtils.generate_intervals(start, end, step_sec)
        reqs = [
            req.format(step, symbol, interval[0], interval[1]) for interval in intervals
        ]
        return reqs

    @classmethod
    def kline_parser(cls, response: aiohttp.ClientResponse) -> List[List[Any]]:
        return [response["data"]]

    @classmethod
    def orderbook_parser(cls, response: dict) -> List[List[List[Any]]]:
        data = response["data"]
        timestamp = data["time"]
        sequence = data["sequence"]  # used to match socket data to snapshot
        asks, bids = data["asks"], data["bids"]
        return [asks, bids]


class KucoinHttpClient(HttpClient):

    _base_url = "https://api.kucoin.com"

    def __init__(
        self, api_key: str, api_secret: str, api_passphrase: str, database: List[Any]
    ):
        super().__init__(api_key, api_secret, api_passphrase, database)
        self.utility_module = KucoinClientUtils

    def _get_headers(self, method: str, endpoint: str) -> dict:
        current_ts = str(int(time.time()) * 1000)
        body = current_ts + method + endpoint
        signiture = base64.b64encode(
            hmac.new(
                self.api_secret.encode("utf-8"),
                body.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        passphrase = base64.b64encode(
            hmac.new(
                self.api_secret.encode("utf-8"),
                self.api_passphrase.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        header = {
            "KC-API-KEY": self.api_key,
            "KC-API-SIGN": signiture,
            "KC-API-TIMESTAMP": current_ts,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2",
        }
        return header

    def parse_klines(self, queries, rps, symbol, start, end, step, limit=1000):
        reqs = self.utility_module.get_kline_reqs(symbol, start, end, step, limit)
        return self.parse_reqs(
            reqs, queries, self.utility_module.kline_parser, rps
        )  # return ready to use coroutine
