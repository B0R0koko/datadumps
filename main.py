from clients.binance_client_http import BinanceHttpClient
from clients.http_client import Scheduler


client = BinanceHttpClient(
    None, None, None, ["192.168.50.10", "postgres", "pwd", 5432, "crypto_db"]
)

# Create first task
task_1 = client.parse_klines(
    queries=[
        "INSERT INTO klines VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
    ],
    rps=10,
    symbol="GVTBTC",
    start=1586361423,
    end=1587225423,
    step="1m",
)

# Create second task
task_2 = client.parse_klines(
    queries=[
        "INSERT INTO klines VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
    ],
    rps=10,
    symbol="RLCBTC",
    start=1586793620,
    end=1587657620,
    step="1m",
)

# Schedule and run these tasks
Scheduler.schedule_tasks([task_1, task_2])

# Print client to get execution info
print(client)
