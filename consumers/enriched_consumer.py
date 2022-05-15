from kafka import KafkaConsumer, KafkaProducer
import json

consumer_rtst = KafkaConsumer(
    'rtst_test_producer01',
    bootstrap_servers=['172.31.70.21:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id='test-producer' + '_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(bootstrap_servers=['172.31.70.21:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('ascii', 'ignore')
                         )

for msg in consumer_rtst:
    msg = msg.value["after"]
    print(msg)
    producer.send('alaki', msg)