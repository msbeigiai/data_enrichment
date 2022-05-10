import kafka
from kafka_work import KafkaWork


kafka_work = KafkaWork('test-topic')
kafka_work.create_consumer()


print(kafka_work.consumer.topics)

for msg in kafka_work.consumer:
    msg = msg.value
    print(msg)