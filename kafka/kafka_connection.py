from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads


def kafka_producer():
    producer = KafkaProducer(
        value_serializer = lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers = ['localhost:9092']
    )
    return producer

def kafka_consumer():
    consumer = KafkaConsumer(
        'redditChannel',
        #auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
        )

    return consumer