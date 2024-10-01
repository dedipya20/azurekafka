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

# Create the 'stocks' table if it doesn't already exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stocks (
        index_name VARCHAR(255),
        timestamp VARCHAR(255),
        open DECIMAL(10, 2),
        high DECIMAL(10, 2),
        low DECIMAL(10, 2),
        close DECIMAL(10, 2),
        adj_close DECIMAL(10, 2),
        volume BIGINT,
        closeusd DECIMAL(10, 2)
    );
""")
conn.commit()

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
    
    # Check if all necessary fields are present in the data
    if all(key in data for key in ['index', 'date', 'open', 'high', 'low', 'close', 'adj close', 'volume', 'closeusd']):
        cursor.execute("""
            INSERT INTO stocks (index_name, timestamp, open, high, low, close, adj_close, volume, closeusd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (data['index'], data['date'], data['open'], data['high'], data['low'], data['close'], data['adj close'], data['volume'], data['closeusd']))
        conn.commit()
    else:
        print("Missing required keys in data:", data)

cursor.close()
conn.close()
