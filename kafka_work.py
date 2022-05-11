from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps

class KafkaWork():
    def __init__(self, topic_name):
        self.msg = None
        self.topic_name = topic_name
        self.consumer = None
    
    def create_consumer(self):
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=['172.31.70.21:9092'],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=self.topic_name+"_group",
            value_deserializer=lambda x: loads(x.decode('utf-8')) 
        )

    def __repr__(self) -> str:
        return str(self.consumer.topics)

    def create_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['172.31.70.21:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

    def fetch_data(self):
        if self.consumer is None:
            raise ValueError("There is no 'Consumer.")
        else:
            for msg in self.consumer:
                self.msg = msg.value
                if self.msg is not None:
                    print(self.msg)
                else:
                    raise ValueError("'msg' is NULL")
            
        
