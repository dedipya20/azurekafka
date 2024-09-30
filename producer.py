# producer.py
from kafka import KafkaProducer
import json
import time
import pandas as pd

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load data using pandas (replace 'your_file.csv' with the actual file)

file = r"C:\Users\ratna\Downloads\stock_data_new.csv"
df = pd.read_csv(file)
# Sending each row as a message to the Kafka topic 
for index, row in df.iterrows():
    data = {
        'index_name': row['Index'],
        'timestamp': row['Timestamp'],
        'stock_price': row['Price'],
        'trading_volume': row['Volume']
    }
    producer.send('stock_topic', value=data)
    time.sleep(1)

producer.flush()
