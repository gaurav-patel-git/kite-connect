import csv

def insert_ticks(file_name, ticks):
    with open(file_name, mode='a') as csv_file:
        fieldnames = ['last_price', 'date']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        # writer.writeheader()
        for tick in ticks:
            writer.writerow({'last_price':tick['last_price'], 'date':tick['timestamp']})

