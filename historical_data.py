from kiteconnect import KiteConnect
from kiteconnect import exceptions
import datetime
import time, csv


key='xxxxxxxxxxxxx'
secret='xxxxxxxxxxxxxxxxxx'
access='xxxxxxxxxxxxxxxxxxxxxx'
kite=KiteConnect(key)
data=kite.set_access_token(access)

token_list = ['969473', '969474']  # all you tokens
fromd = datetime.date(2020, 5, 21)
tod = datetime.date(2020, 5, 22)
interval = "minute"


def get_historical_data(token_list):
    count = 0
    for token in token_list:
        count += 1
        data = kite.historical_data(token,fromd,tod,interval)
        with open(f'file{count}.csv', 'a') as csv_file:
            fieldnames = ['date', 'open', 'high', 'low', 'close', 'volume' ]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            for d in data:
                writer.writerow(d)
get_historical_data            


data = [{'date': datetime.datetime(2020, 5, 21, 9, 15, tzinfo=tzoffset(None, 19800)), 'open': 183.4, 'high': 184.9, 'low': 183.4, 'close': 184.05, 'volume': 60161}, {'date': datetime.datetime(2020, 5, 21, 9, 16, tzinfo=tzoffset(None, 19800)), 'open': 184.15, 'high': 185.25, 'low': 184.1, 'close': 185, 'volume': 65010}, {'date': datetime.datetime(2020, 5, 21, 9, 17, tzinfo=tzoffset(None, 19800)), 'open': 185.05, 'high': 185.15, 'low': 184.6, 'close': 185.05, 'volume': 91212}, {'date': datetime.datetime(2020, 5, 21, 9, 18, tzinfo=tzoffset(None, 19800)), 'open': 184.9, 'high': 185.35, 'low': 184.9, 'close': 185.1, 'volume': 37509}, {'date': datetime.datetime(2020, 5, 21, 9, 19, tzinfo=tzoffset(None, 19800)), 'open': 185.1, 'high': 185.3, 'low': 185.05, 'close': 185.2, 'volume': 12888}]
with open(f'file{}.csv', 'a') as csv_file:
    fieldnames = ['date', 'open', 'high', 'low', 'close', 'volume' ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    for d in data:
        writer.writerow(d)
