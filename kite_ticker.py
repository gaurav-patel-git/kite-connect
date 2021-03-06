from kiteconnect import KiteTicker
import logging, json
from helper import get_engine, store_ticks



logging.basicConfig(level=logging.DEBUG)

with open('cred.json') as f:
    cred = json.load(f)
    api_key = cred['api_key']
    access_token = cred['access_token']
    host = cred['host']
    user = cred['user']
    password = cred['password']
    database = cred['database']
    tokens = cred['tokens']

kws=KiteTicker(api_key, access_token)


engine = get_engine(host, user, password, database)

def on_ticks(ws,ticks):
    store_tick=store_ticks(engine, ticks)  # this will store timestamp and last price in _instrument_toekn table in database
    print(ticks)

def on_connect(ws,response):
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL,tokens)


kws.on_ticks=on_ticks
kws.on_connect=on_connect
kws.connect()