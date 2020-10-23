import pandas as pd
import time
import os

# set traing time 1min, 2min, 5min, day.....
trading_time = "1min"

# set the type as high or low whatever you want
value_type = "high"

# set the number of last values you want average to be
num_values = 20

while True:
    # os.system('cls')
    data = pd.read_csv('stockl.csv', parse_dates=True, names=['last_price', 'date'], index_col='date')
    data = pd.DataFrame(data)
    candle = data.resample(trading_time).ohlc().dropna()
    specified_last_value = candle['last_price'][value_type].tail(num_values)
    average_value = specified_last_value.mean()
    print(f'Average of the last {num_values} values is {average_value}')
    time.sleep(2)