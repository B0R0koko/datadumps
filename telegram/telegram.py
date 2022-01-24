from telethon.sync import TelegramClient
import re
import pickle

api_id = 15328631
api_hash = "54c75bf2273bf5e85e359b3099ea5e45"

chat = "Big_Pumps_Signals"

client = TelegramClient("crypto_session", api_id, api_hash)
client.start()

messages = client.get_messages(chat, limit=5000, reverse=True)

data = {"coin": [], "timestamp": [], "exchange": []}

check_exchange_list = ["binance", "kucoin", "okex", "mexc", "bitrex", "gate.io"]

for i, message in enumerate(messages):
    msg = str(message.message).lower()
    if "coin is" in msg:
        coin = re.search(r"coin\s{0,1}is\s{0,1}:\s{0,3}(\w{1,5})", msg)[1]
        timestamp = message.date
        exchange = None
        prev_msg = str(messages[i - 1].message).lower()
        for exch in check_exchange_list:
            if exch in prev_msg:
                exchange = exch
                break
        data["coin"].append(coin)
        data["timestamp"].append(timestamp)
        data["exchange"].append(exchange)

with open("data.pickle", "wb") as file:
    pickle.dump(data, file)
