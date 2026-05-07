from kafka import KafkaProducer
import json

topic = "stock_analysis"  #define a variable for the topic name
#this is where python application will store data


def init_producer():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9094', #in compose.yml we have defined 9094 as external listener for local host(python client)docker compose logs kafka --no-log-prefix | tail -50
        value_serializer= lambda v: json.dumps(v).encode('utf-8')
    )
    
    return producer