from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['172.31.70.21:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('ascii', 'ignore')
                         )


def send_producer(data):
    if producer is not None:
        # data["after"]["ITEMID"] = (data["after"]["ITEMID"]).decode('utf-8')
        # data = data.decode('utf-8')
        # for key, value in data.items():

        producer.send('rtst_test_producer01', data)
        print('*' * 100)
        print(data)
