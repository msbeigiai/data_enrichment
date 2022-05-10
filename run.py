from kafka_work import KafkaWork
from redis_work import RedisWork
from sql_work import SQLWork
import os


kafka_work = KafkaWork('test-topic')
kafka_work.create_consumer()


print(kafka_work.consumer.topics)

# for msg in kafka_work.consumer:
#     msg = msg.value
#     print(msg)

redis_work = RedisWork()
# redis_work.r.set('foo', 'bar')
print(redis_work.value_of_key('foo'))
print(redis_work.check_key('na'))
redis_work.set_key_value("Mohsen", "Sadegh")

sql_work = SQLWork()