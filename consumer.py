# consumer.py
from kafka import KafkaConsumer
import mysql.connector
import json

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Deepu@2003",
    database="stock_data_new"
)
cursor = conn.cursor()

# Create a Kafka consumer
consumer = KafkaConsumer(
    'stock_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Process messages and insert them into MySQL
for message in consumer:
    data = message.value
    cursor.execute("""
        INSERT INTO stocks (index_name, timestamp, stock_price, trading_volume)
        VALUES (%s, %s, %s, %s)
    """, (data['index_name'], data['timestamp'], data['stock_price'], data['trading_volume']))
    conn.commit()

cursor.close()
conn.close()
