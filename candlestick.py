import json

with open('cred.json') as f:
    cred = json.load(f)
    host = cred['host']
    user = cred['user']
    password = cred['password']
    database = cred['database']

import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host=host, database=database, user=user, password=password)
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("show databases;")
        record = cursor.fetchall()
        for x in record:
            print(x)
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")



# candles= pd.read_csv('stockl.csv',parse_dates=True, index_col='date', names=['last_price', 'date'])
# candles = pd.DataFrame(candles)
# candles=candles.resample('1min').ohlc().dropna()
# candles = candles.reset_index()
# print(candles)

