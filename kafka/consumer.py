from kafka import KafkaConsumer
from json import loads

""" 
    rtst = RETAILTRANSACTIOSAILSTRANS
    rtt = RETAILTRANSACTIONTABLE
"""


rtst_consumer = KafkaConsumer(
    'DebeziumTestServer.dbo.RETAILTRANSACTIONSALESTRANS',
    bootstrap_servers=['172.31.70.21:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id='mygroup',
    value_deserializer=lambda x: loads(x.decode('utf-8')) 
)

rtt_consumer = KafkaConsumer(
    'DebeziumTestServer.dbo.RETAILTRANSACTIONTABLE',
    bootstrap_servers=['172.31.70.21:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id='mygroup',
    value_deserializer=lambda x: loads(x.decode('utf-8')) 
)

if rtst_consumer and rtt_consumer:
    print(rtst_consumer.topics)    

else:
    raise ValueError("There is no topic on name 'DebeziumTestServer.dbo.RETAILTRANSACTIONSALESTRANS';")


consumers = {"rtst_consumer": rtst_consumer, "rtt_consumer": rtt_consumer}

values = {}

for key, values in consumers.items():
    for msg in values:
        values[key] = msg.value
        