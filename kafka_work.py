from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps

class KafkaWork():
    def __init__(self, topic_name) -> None:
        self.topic_name = topic_name
    
    def create_consumer(self):
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=['172.31.70.21:9092'],
            auto_offset_reset="earilest",
            enable_auto_commit=True,
            group_id='mygroup',
            value_deserializer=lambda x: loads(x.decode('utf-8')) 
        )

    def create_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['172.31.70.21:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
