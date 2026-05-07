from kafka import KafkaConsumer
import json 
import time

# -----config matching the producer----

consumer = KafkaConsumer(
    'stock_analysis',  #topic name
    bootstrap_servers=['localhost:9094'], #host of the kafka cluster
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-consumer-group', #define a consumer grp
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Starting kafka consumer.waiting for messages on topic 'stock analysis'...")

for message in consumer:
    
    data = message.value
    #print the Received data

    print(f" Value (Deserialized): {data}")

consumer.close()

print("Kafka consumer closed.")
