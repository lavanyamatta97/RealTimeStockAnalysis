from kafka import KafkaConsumer
import json 
import time

# -----config matching the producer----

consumer = KafkaConsumer(
    'stock_analysis',
    bootstrap_servers=['localhost:9094'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-consumer-group', #define a consumer grp
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Starting kafka consumer.waiting for messages on topic 'customer_info'...")

for message in consumer:
    
    data = message.value
    #print the Received data

    print(f" Value (Deserialized): {data}")

consumer.close()
print("Kafka consumer closed.")
