import pandas as pd
from helper import get_connection, store_ticks
from sqlalchemy import create_engine


engine = create_engine(f'mysql+mysqldb://{user}:{password}@{host}/{database}', pool_recycle=3600)
connection = get_connection(host, user, password, database)
cursor = connection.cursor()

def make_candles(instrument_token, duration, cursor):
    table_name = f"_{instrument_token}"
    sqlLastRow = f""" SELECT * FROM {table_name}_{duration} order by id desc limit 1 """
    cursor.execute(sqlLastRow)
    last_row = cursor.fetchall()
    print(last_row)
    last_candle_datetime = last_row[0][1]
    # last_candle_datetime = '2020-07-22 11:02:00'
    sqlInstrumentTicks = f""" SELECT * FROM {table_name} WHERE date_time >= '{last_candle_datetime}' """
    ticks = pd.read_sql(sqlInstrumentTicks, connection, index_col='date_time', parse_dates=True)
    ticks = ticks.drop('id', axis=1)
    candles = ticks.resample(duration).ohlc()
    candles.columns = candles.columns.get_level_values(1)
    candles.to_sql(f"{table_name}_{duration}", engine, if_exists='append', index=True)
    print(candles)
    

make_candles("dataframe", "1min", cursor)




















# candles= pd.read_csv('stockl.csv',parse_dates=True, index_col='date', names=['last_price', 'date'])
# candles = pd.DataFrame(candles)
# candles=candles.resample('1min').ohlc().dropna()
# candles = candles.reset_index()
# print(candles)

