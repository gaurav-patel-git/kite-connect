from kiteconnect import KiteTicker
import logging
from helper import get_engine, store_ticks



logging.basicConfig(level=logging.DEBUG)

api_key= "" # your api kei
access_token= ""  # your access_token
tokens=[53703431]  # for now only one instruemnts token

kws=KiteTicker(api_key,access_token)

host = 'localhost'
user = 'root'
password = ''
database = 'ticks'

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