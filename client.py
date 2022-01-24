from binance_client_http import BinanceHttpClient, BinanceClientUtils
from http_client import Scheduler


client = BinanceHttpClient(
    None, None, None, ["192.168.50.10", "postgres", "pwd", 5432, "crypto_db"]
)
# Create first task
reqs = BinanceClientUtils.get_kline_reqs(
    "GVTBTC", 1586361423, 1587225423, "1m"
)  # Generate list of urls
parser = BinanceClientUtils.kline_parser  # Define data parser

task_1 = client.parse_reqs(
    reqs=reqs,
    queries=[
        "INSERT INTO klines VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
    ],
    parser=parser,
    rps=5,
)  # create task with all parameters

# Create second task
reqs = BinanceClientUtils.get_kline_reqs("RLCBTC", 1586793620, 1587657620, "1m")

task_2 = client.parse_reqs(
    reqs=reqs,
    queries=[
        "INSERT INTO klines VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
    ],
    parser=parser,
    rps=5,
)

# Schedule and run these tasks

Scheduler.schedule_tasks([task_1, task_2])

# Print client to receive execution info
print(client)
