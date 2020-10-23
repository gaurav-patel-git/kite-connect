
from datetime import datetime

def cur_date_time():
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    return (current_time)

print(cur_date_time())    