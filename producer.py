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


df = pd.read_csv('stock_data_new.csv')
# Sending each row as a message to the Kafka topic 
for index, row in df.iterrows():
    data = {
        'index': row['Index'],
        'date': row['Date'],
        'open': row['Open'],
        'high': row['High'],
	'low' : row['Low'],
        'close' : row['Close'],	
        'adj close' : row['Adj Close'],	
        'volume' : row['Volume'],	
        'closeusd' : row['CloseUSD']
}
    producer.send('stock_topic', value=data)
    time.sleep(1)

producer.flush()
