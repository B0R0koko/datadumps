import http_client

import base64
import hmac
import hashlib
import time

from typing import List, Any


class OkexHttpClient(http_client):

    _base_url = "https://www.okex.com"

    def __init__(
        self, api_key: str, api_secret: str, api_passphrase: str, database: List[Any]
    ):
        super().__init__(api_key, api_secret, api_passphrase, database)

    def _get_headers(self, method, endpoint):
        current_ts = str(int(time.time()) * 1000)
        body = current_ts + method + endpoint

        signiture = base64.b64encode(
            hmac.new(
                body.encode("utf-8"),
                self.api_secret.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        headers = {
            "OK-ACCESS-KEY": self.api_secret,
            "OK-ACCESS-SIGN": signiture,
            "OK-ACCESS-TIMESTAMP": current_ts,
            "OK-ACCESS-PASSPHRASE": self.api_passphrase,
        }

        return headers
