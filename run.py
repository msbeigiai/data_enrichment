from kafka_work import KafkaWork
from redis_work import RedisWork
from sql_work import SQLWork
from vars import topics
import os

rtt_consumer = KafkaWork(topic_name=topics["rtt_topic"])
rtt_consumer.create_consumer()
rtt_consumer.fetch_data()

print('*' * 100)

rtst_consumer = KafkaWork(topic_name=topics["rtst_topic"])
rtst_consumer.create_consumer()
rtst_consumer.fetch_data()

# print(kafka_work.consumer.topics)

# # for msg in kafka_work.consumer:
# #     msg = msg.value
# #     print(msg)

# redis_work = RedisWork()
# # redis_work.r.set('foo', 'bar')
# print(redis_work.value_of_key('foo'))
# print(redis_work.check_key('na'))
# redis_work.set_key_value("Mohsen", "Sadegh")

# sql_work = SQLWork()
# sql_work.run_query("select count(*) from retailtransactionsalestrans")
# sql_work.run_query("select count(*) from retailtransactiontable")
