from clients.http_client import HttpClient, HttpClientUtils

import aiohttp
import base64
import hmac
import hashlib
import time
import asyncio

from typing import List, Any


class BinanceClientUtils:

    _tick_to_sec_map = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1hr": 3600,
    }

    # Generate reqs with incrementing interval
    # GET /api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=1499040000000&closeTime=1499644799999&limit=1000
    @classmethod
    def get_kline_reqs(cls, symbol, start, end, step, limit):
        req = "GET /api/v3/klines?symbol={}&interval={}&startTime={}000&endTime={}000&limit={}"
        step_sec = cls._tick_to_sec_map[step] * limit
        intervals = HttpClientUtils.generate_intervals(start, end, step_sec)
        reqs = [
            req.format(symbol, step, interval[0], interval[1], limit)
            for interval in intervals
        ]
        return reqs

    @classmethod
    def kline_parser(cls, response: aiohttp.ClientResponse) -> List[List[Any]]:
        # Requesting klines with binance returns list of embedded lists
        return [response]


class BinanceHttpClient(HttpClient):

    _base_url = "https://api.binance.com"

    def __init__(
        self, api_key: str, api_secret: str, api_passphrase: str, database: List[Any]
    ):
        super().__init__(api_key, api_secret, api_passphrase, database)
        self.utility_module = BinanceClientUtils

    def _get_headers(
        self, method, endpoint
    ):  # Using Market Data Endpoints doesnt require signitures
        return {}

    def parse_klines(self, queries, rps, symbol, start, end, step, limit=1000):
        reqs = self.utility_module.get_kline_reqs(symbol, start, end, step, limit)
        return self.parse_reqs(
            reqs, queries, self.utility_module.kline_parser, rps
        )  # return ready to use coroutine
