import pymysql
from sqlalchemy import create_engine
import datetime
import pandas as pd

def get_engine(host, user, password, database, timeout=3600):
    try:
        connection_string = f'mysql+mysqldb://{user}:{password}@{host}/{database}'
        engine = create_engine(connection_string, pool_recycle=timeout)
        print("Connected to MySQL Server ")
        return engine
    except:
        print("Error while connecting to MySQL")

def store_ticks(engine, ticks):
    connection = engine.connect()
    instrument_token = ticks['instrument_token']
    timestamp = ticks['timestamp']
    last_price = ticks['last_price']
    sqlInsertQuery = f"""INSERT INTO _{instrument_token} 
                                            (date_time, last_price)
                                            VALUES
                                            ('{timestamp}', {last_price})"""
    sqlCreateTable = f"""CREATE TABLE _{instrument_token} 
                                        (id INT AUTO_INCREMENT PRIMARY KEY,
                                        date_time DATETIME,
                                        last_price FLOAT )"""
    try:
            connection.execute(sqlInsertQuery)
    except:
        try:
            connection.execute(sqlCreateTable)
            connection.execute(sqlInsertQuery)
        except :
            print("Error is some error while storing ticks")
         


host = 'localhost'
user = 'root'
password = 'Gaurav@8966'
database = 'ticks'

engine = get_engine(host, user, password, database)

def make_candles(instrument_token, duration, engine):
    with engine.connect() as connection:
        ticks_table_name = f'_{instrument_token}'
        table_name = f"_{instrument_token}_{duration}"
        sqlTable = f""" SHOW TABLES LIKE '{table_name}' """
        table_result = connection.execute(sqlTable)
        table_exists = table_result.fetchall()
        if not table_exists:
            print('Table dose not exist creating one')
            try:
                sqlCreateTable = f"""CREATE TABLE {table_name} 
                                                (id INT AUTO_INCREMENT PRIMARY KEY,
                                                date_time DATETIME,
                                                open FLOAT,
                                                high FLOAT,
                                                low FLOAT,
                                                close FLOAT )"""
                connection.execute(sqlCreateTable)                                        
                print('Table sucessfully created')
            except:
                print('Problem in creating table for storing candles ')                                        

        sqlInstrumentTicks = f""" SELECT * FROM {ticks_table_name} """
        
        sqlLastRow = f""" SELECT * FROM {table_name} order by id desc limit 1 """
        last_row = connection.execute(sqlLastRow)
        last_row = last_row.fetchall()
        if last_row :
            print(last_row)
            last_candle_datetime = last_row[0][1]
            last_candle_id = last_row[0][0]
            sqlInstrumentTicks = f""" SELECT * FROM {ticks_table_name} WHERE date_time >= '{last_candle_datetime}' """  # getting ticks from last candle datetime
            sqlDelete = f""" DELETE FROM {table_name} WHERE id = {last_candle_id}"""
            connection.execute(sqlDelete)

        ticks = pd.read_sql(sqlInstrumentTicks, engine, index_col='date_time', parse_dates=True)
        ticks = ticks.drop('id', axis=1)  # we don't need ohlc for id column
        candles = ticks.resample(duration).ohlc()
        candles.columns = candles.columns.get_level_values(1)  # flating columns to avoid multiindexing columns

        candles.to_sql(f"{table_name}", engine, if_exists='append', index=True)
        print(candles)
        

make_candles(100, "1min", engine)


# import csv

# with open('stockl.csv') as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=',')
#     tick = {
#         'instrument_token' : '100',
#         'timestamp' : '',
#         'last_price' : ''
#     }
#     for row in csv_reader:
#         if row:
#             tick['last_price'] = row[0]
#             tick['timestamp'] = row[1]
#             store_ticks(engine, tick)


# tick = {
#     'timestamp' : '2019-12-09 17:36:07',
#     'instrument_token' : 200,
#     'last_price' : 202.2
# }

# store_ticks(connection, tick)