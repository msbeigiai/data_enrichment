from kafka import KafkaConsumer
from json import loads, dumps
from vars import topics, sql_conf, tables
import redis
import pyodbc

r = redis.Redis(host="localhost", port=6379, db=0)

consumer = KafkaConsumer(
    topics["rtt_topic"],
    bootstrap_servers=['172.31.70.21:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=topics["rtt_topic"] + '_group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

conn = pyodbc.connect(
    f'DRIVER={sql_conf["driver"]};SERVER=' + sql_conf["server"] + ';DATABASE=' + sql_conf["database"] + \
    ';UID=' + sql_conf["username"] + ';PWD=' + sql_conf["password"])
cursor = conn.cursor()


def check_store_redis(key):
    if r.get(key) is not None:
        value = r.get(key)
        value = value.decode('utf-8')
        return value
    else:
        # Fetch data from sql
        query_store = "select c.NAME " \
                      f"from {tables['rct']} a inner join {tables['ouv']} b " \
                      f"on a.OMOPERATINGUNITID = b.RECID inner join {tables['dpt']} c " \
                      "ON c.PARTYNUMBER = b.PARTYNUMBER where a.STORENUMBER = '%s'" % key
        cursor.execute(query_store)
        value = cursor.fetchone()

        if value is None:
            value = str(key)
            r.set(key, value)
        else:
            value = value[0]
            r.set(key, str(value))
        return value


def define_customer_number(key):
    query_net_price = "select d.NAME " \
                      f"from {tables['ct']} c " \
                      f"inner join {tables['dpt']} d on c.PARTY = d.RECID where " \
                      "c.ACCOUNTNUM = '%s'" % key
    cursor.execute(query_net_price)
    value = cursor.fetchone()
    if value is None:
        value = str(key)
        r.set(key, value)
    else:
        value = value[0]
        r.set(key, str(value))
    return value


def data_enrichment(msg):
    store = msg["payload"]["after"]["STORE"]
    custaccount = msg["payload"]["after"]["CUSTACCOUNT"]
    customer_number = define_customer_number(custaccount)
    store_alias = check_store_redis(store)
    if store_alias:
        msg["payload"]["after"]["STORE"] = store_alias
        msg["payload"]["after"]["CUSTACCOUN"] = customer_number
        return msg


if consumer:
    print(consumer.topics)
else:
    raise ValueError("There is no consumer!")

for message in consumer:
    message = message.value
    enriched_msg = data_enrichment(message)
    enriched_data_producer = enriched_msg["payload"]
    print('*' * 100)
    print(enriched_data_producer)
