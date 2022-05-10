import kafka
from kafka_work import KafkaWork
from time import sleep

kafka_work = KafkaWork('test-topic')
kafka_work.create_producer()

for i in range(1, 1000):
    msg = {f"Hello {i}": f"World! {i}"}
    kafka_work.producer.send(kafka_work.topic_name, value=msg)
    sleep(2)
    