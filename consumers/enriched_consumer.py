from kafka import KafkaConsumer
from json import loads

consumer_rtst = KafkaConsumer(
    'rtst_test_producer01',
    bootstrap_servers=['172.31.70.21:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id='test-producer' + '_group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

consumer_rtt = KafkaConsumer(
    'rtt_test_producer01',
    bootstrap_servers=['172.31.70.21:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id='rtt_test_producer01' + '_group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)


class AggregatedConsumers:
    def __init__(self):
        self.rtt_message = None
        self.rtst_message = None
        self._consumer_rtst = consumer_rtst
        self._consumer_rtt = consumer_rtt

    def aggregate(self):
        for msg in self._consumer_rtst:
            self.rtst_message = msg.value

        for msg in self._consumer_rtt:
            self.rtt_message = msg.value


ac = AggregatedConsumers()
ac.aggregate()
print(ac.rtt_message)
