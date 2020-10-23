import mysql.connector
from mysql.connector import Error
import datetime

def get_connection(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            )
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            return connection
    
    except Error as e:
        print("Error while connecting to MySQL", e)

def store_ticks(connection, ticks):
    cursor = connection.cursor()
    instrument_token = ticks['instrument_token']
    timestamp = ticks['timestamp']
    last_price = ticks['last_price']
    sqlInsertQuery = f"""INSERT INTO _{instrument_token} 
                                            (timestamp, last_price)
                                            VALUES
                                            ('{timestamp}', {last_price})"""
    print(sqlInsertQuery)
    sqlCreateTable = f"""CREATE TABLE _{instrument_token} (
                                        id INT AUTO_INCREMENT PRIMARY KEY,
                                        timestamp TIMESTAMP,
                                        last_price FLOAT
                                                            )"""
    try:
        cursor.execute(sqlCreateTable)
        cursor.execute(sqlInsertQuery)
        connection.commit()
    except Error as e:
        try:
            cursor.execute(sqlInsertQuery)
            connection.commit()
        except Error as e:
            print("Error is ", e)
         
    
ticks = {
    'instrument_token': 53490439,
    'mode': 'full',
    'volume': 12510,
    'last_price': 4084.20,
    'average_price': 4086.55,
    'last_quantity': 1,
    'buy_quantity': 2356,
    'sell_quantity': 2440,
    'change': 0.46740467404674046,
    'last_trade_time': datetime.datetime(2018, 1, 15, 13, 16, 54),
    'timestamp': datetime.datetime(2018, 1, 15, 13, 16, 56),
    'oi': 21845,
    'oi_day_low': 0,
    'oi_day_high': 0,
    'ohlc': {
        'high': 4093.0,
        'close': 4065.0,
        'open': 4088.0,
        'low': 4080.0
    },
    'tradable': True,
    'depth': {
        'sell': [{
            'price': 4085.0,
            'orders': 1048576,
            'quantity': 43
        }, {
            'price': 4086.0,
            'orders': 2752512,
            'quantity': 134
        }, {
            'price': 4087.0,
            'orders': 1703936,
            'quantity': 133
        }, {
            'price': 4088.0,
            'orders': 1376256,
            'quantity': 70
        }, {
            'price': 4089.0,
            'orders': 1048576,
            'quantity': 46
        }],
        'buy': [{
            'price': 4084.0,
            'orders': 589824,
            'quantity': 53
        }, {
            'price': 4083.0,
            'orders': 1245184,
            'quantity': 145
        }, {
            'price': 4082.0,
            'orders': 1114112,
            'quantity': 63
        }, {
            'price': 4081.0,
            'orders': 1835008,
            'quantity': 69
        }, {
            'price': 4080.0,
            'orders': 2752512,
            'quantity': 89
        }]
    }
}

connection = get_connection('localhost', 'root', 'Gaurav@8966', 'ticks', ticks)