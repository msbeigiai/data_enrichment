from kafka import KafkaConsumer, KafkaProducer
from vars import *
import json
import redis
import pyodbc
import kafka_config
import sql_config

data = {}

consumer = KafkaConsumer(
    topics["rtt_topic_2"],
    bootstrap_servers=['172.31.70.22:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=topics["rtt_topic_2"] + '__group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


producer = KafkaProducer(
    bootstrap_servers=['172.31.70.22:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


cursor = sql_config.sql_initialize(sql_conf)

r = redis.Redis(host="localhost", port=6379, db=0)


def rtt_data_fetch(dic):
    list_items = ["TRANSACTIONID", "STORE", "TRANSTIME",
                  "PAYMENTAMOUNT", "CREATEDDATETIME", "CUSTACCOUNT"]
    var = {k: v for k, v in dic.items() if k in [val for val in list_items]}
    return var


def rtt_check_cust_account(key):
    if r.get(key) is not None:
        value = r.get(key)
        value = value.decode('utf-8')
        return value
    else:
        # Fetch data from sql
        query_store = "select d.NAME " \
                      "from CUSTTABLE c " \
                      "inner join DIRPARTYTABLE d on c.PARTY = d.RECID where " \
                      "c.ACCOUNTNUM = '%s'" % key
        cursor.execute(query_store)
        value = cursor.fetchone()

        if value is None:
            value = str(key)
            r.set(key, value)
        else:
            value = value[0]
            r.set(key, str(value))
        return value


def rtt_check_store_redis(key):
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


def rtt_store_fetch(new_data):
    store = new_data["STORE"]
    custaccount = new_data["CUSTACCOUNT"]
    store_alias = rtt_check_store_redis(store)
    custom_number = rtt_check_cust_account(custaccount)
    if store_alias:
        new_data["STORE"] = store_alias
        new_data["CUSTACCOUNT"] = custom_number
    return new_data


def rtst_fetch_namealiases_redis(key):

    name_item = {}

    query_itemid = "select c.ITEMID from RETAILTRANSACTIONTABLE d " \
                   "inner join RETAILTRANSACTIONSALESTRANS c on " \
                   "d.TRANSACTIONID = c.TRANSACTIONID where d.TRANSACTIONID = '%s'" % key
    cursor.execute(query_itemid)
    item_id = [val[0] for val in cursor.fetchall()]

    for item in item_id:
        temp_name = r.get(item)
        if temp_name:
            name_item[item] = temp_name.decode('utf-8')
        else:
            name_item[item] = None

    if None in name_item.values():
        for item in item_id:
            if name_item[item] is None:
                query_transactionid = "select b.NAMEALIAS from RETAILTRANSACTIONTABLE c " \
                                      " inner join RETAILTRANSACTIONSALESTRANS d on " \
                                      "c.TRANSACTIONID = d.TRANSACTIONID " \
                                      "inner join INVENTTABLE b on " \
                                      f"b.ITEMID = {item} where c.TRANSACTIONID = '%s'" % key
                cursor.execute(query_transactionid)
                # name_item[item] = [val[0] for val in cursor.fetchall()]
                name_item[item] = cursor.fetchone()[0]
                r.set(item, str(name_item[item]))

    return [v for v in name_item.values()]


def rtst_fetch_netprice(key):
    query_net_price = "select d.PRICE - d.DISCAMOUNT as NETPRICE from RETAILTRANSACTIONTABLE c " \
                      " inner join RETAILTRANSACTIONSALESTRANS d on " \
                      "c.TRANSACTIONID = d.TRANSACTIONID " \
                      "inner join INVENTTABLE b on " \
                      "b.ITEMID = d.ITEMID where c.TRANSACTIONID = '%s'" % key
    cursor.execute(query_net_price)
    value = cursor.fetchone()
    if value:
        return float(value[0])
    else:
        return ''


def rtst_fetch_data(new_data):
    dict_data = {}
    transaction_id = new_data["TRANSACTIONID"]
    dict_data["ITEMID"] = rtst_fetch_namealiases_redis(transaction_id)
    dict_data["NETPRICE"] = rtst_fetch_netprice(transaction_id)
    return dict_data


def send_producer(ledger_data):
    if producer:
        producer.send('ledger-06', ledger_data)


for msg in consumer:

    if msg is None:
        continue

    msg = msg.value  # ().decode('utf-8')

    msg_cleand = rtt_data_fetch(msg["after"])
    msg_cleand = rtt_store_fetch(msg_cleand)
    msg_rtst = rtst_fetch_data(msg_cleand)

    data["RETAIL_TRANSACTION_TABLE"] = msg_cleand
    data["RETAIL_TRANSACTION_SALES_TRANS"] = msg_rtst

    send_producer(data)

    print(data)
