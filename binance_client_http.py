from http_client import HttpClient

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

    # GET /api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=1499040000000&closeTime=1499644799999&limit=1000
    @classmethod
    def get_kline_reqs(
        cls, symbol: str, startTime: int, endTime: int, interval: str, n_klines_pr=1000
    ):
        kline_reqs = []
        master_interval = endTime - startTime
        small_interval = n_klines_pr * cls._tick_to_sec_map[interval]
        n_reqs = master_interval // small_interval + 1
        for i in range(n_reqs):
            loc_start = startTime + i * small_interval
            loc_end = startTime + (i + 1) * small_interval if i != n_reqs else endTime
            req = (
                f"GET /api/v3/klines?symbol={symbol}&interval={interval}"
                f"&startTime={loc_start}000&endTime={loc_end}000&limit={n_klines_pr}"
            )  # timestamp in ms
            kline_reqs.append(req)
        return kline_reqs

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

    def _get_headers(
        self, method, endpoint
    ):  # Using Market Data Endpoints doesnt require signitures
        return {}
