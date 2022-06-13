from kafka import KafkaConsumer, KafkaProducer
from vars import *
import json
import redis
import sql_config
import time
from pathlib import Path


# final_data = {}

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

r = redis.Redis(host="172.31.70.21", port=6379, db=0)

def rtt_check_store_redis(key):
    if key is not '':
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
    else:
        return "Unknown"

def rtt_check_cust_account(key):
    if key is not '':
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

            if value is not None:
                value = value[0]
                r.set(key, str(value))
                return value
            else:
                return "Unknown"
    else:
        return "Unknown"


def rtt_store_fetch(new_data):
    store = new_data["STORE"]
    custaccount = new_data["CUSTACCOUNT"]
    store_alias = rtt_check_store_redis(store)
    custom_number = rtt_check_cust_account(custaccount)
    if store_alias:
        new_data["STORE"] = store_alias
        new_data["CUSTACCOUNT"] = custom_number
    return new_data


def rtt_store_fetch(new_data):
    store = new_data["STORE"]
    custaccount = new_data["CUSTACCOUNT"]
    store_alias = rtt_check_store_redis(store)
    custom_number = rtt_check_cust_account(custaccount)
    if store_alias:
        new_data["STORE"] = store_alias
        new_data["CUSTACCOUNT"] = custom_number
    return new_data

def rtst_fetch_discount_amount(transaction_id):

    query_net_dicamount = "select d.DISCAMOUNT from RETAILTRANSACTIONTABLE c " \
        "inner join RETAILTRANSACTIONSALESTRANS d on " \
        "c.TRANSACTIONID = d.TRANSACTIONID " \
        "where c.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_net_dicamount)

    return [float(val[0]) for val in cursor.fetchall()]


def rtst_fetch_price(transaction_id):
    query_price = "select d.PRICE from RETAILTRANSACTIONTABLE c " \
        " inner join RETAILTRANSACTIONSALESTRANS d on " \
        "c.TRANSACTIONID = d.TRANSACTIONID " \
        "where c.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_price)

    return [float(price[0]) for price in cursor.fetchall()]


def rtst_fetch_recid(transaction_id):
    query_recid = "select d.RECID from RETAILTRANSACTIONTABLE c " \
        " inner join RETAILTRANSACTIONSALESTRANS d on " \
        "c.TRANSACTIONID = d.TRANSACTIONID " \
        "where c.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_recid)

    return [recid[0] for recid in cursor.fetchall()]


def rtst_fetch_itemid(transaction_id):
    query_itemid = "select c.ITEMID from RETAILTRANSACTIONTABLE d " \
                   "inner join RETAILTRANSACTIONSALESTRANS c on " \
                   "d.TRANSACTIONID = c.TRANSACTIONID where d.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_itemid)

    return [val[0] for val in cursor.fetchall()]


def rtst_fetch_discount_amount(transaction_id):

    query_net_dicamount = "select d.DISCAMOUNT from RETAILTRANSACTIONTABLE c " \
        "inner join RETAILTRANSACTIONSALESTRANS d on " \
        "c.TRANSACTIONID = d.TRANSACTIONID " \
        "where c.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_net_dicamount)

    return [float(val[0]) for val in cursor.fetchall()]


def rtst_fetch_namealiases_redis(key):

    name_item = {}
    temp_list = []

    item_ids = rtst_fetch_itemid(key)

    for item in item_ids:
        temp_name = r.get(item)
        if temp_name:
            if item in name_item.keys():
                name_item[item+" "] = temp_name.decode('utf-8')
                continue
            name_item[item] = temp_name.decode('utf-8')
            

        else:
            name_item[item] = None

    if None in name_item.values():
        for item in item_ids:
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


def rtt_data_fetch(dic):
    list_items = ["TRANSACTIONID", "STORE", "TRANSTIME",
                  "PAYMENTAMOUNT", "CREATEDDATETIME", "CUSTACCOUNT"]
    var = {k: v for k, v in dic.items() if k in [val for val in list_items]}
    return var



def aggregate_data(transaction_id):
    data = {}
    discount_amounts = rtst_fetch_discount_amount(transaction_id)
    prices = rtst_fetch_price(transaction_id)
    net_prices = [price - disc for price,
                  disc in zip(prices, discount_amounts)]
    name_aliases = rtst_fetch_namealiases_redis(transaction_id)
    recids = rtst_fetch_recid(transaction_id)
    item_ids = rtst_fetch_itemid(transaction_id)
    
    data["ItemID"] = item_ids
    data["NameAlias"] = name_aliases
    data["Price"] = prices
    data["DiscountAmount"] = discount_amounts
    data["NetPrice"] = net_prices
    data["RecID"] = recids

    return data


def make_json(data):
    list_items = ["ItemID", "NameAlias", "Price", "DiscountAmount", "NetPrice", "RecID"]
    header_items = ["TRANSACTIONID", "STORE", "TRANSTIME",
                  "PAYMENTAMOUNT", "CREATEDDATETIME", "CUSTACCOUNT"]
    temp_list = []
    file_body = {}
    final_msg = {}

    items_length = len(data["ItemID"])

    
    file_header = {k: v for (k, v) in data.items() if k in [item for item in header_items]}
    # var = {k: v for k, v in dic.items() if k in [val for val in list_items]}

    for i in range(items_length):
        for k, v in data.items():
            if k not in list_items:
                continue
            if k in list_items:
                file_body[k] = v[i]
                continue
                
        final_msg = file_header.copy()
        final_msg.update(file_body)

        temp_list.append(final_msg)

    return temp_list


def send_producer(ledger_data):
    if producer:
        producer.send('ledger-08-13', ledger_data)

def write_to_json(message, file_name):

    base = Path('data')
    path_to_save = base / file_name
    base.mkdir(exist_ok=True)

    with open(path_to_save, "w") as f:
        json.dump(message, f)



for msg in consumer:
    if msg is None:
        continue

    msg = msg.value

    msg_cleaned = rtt_data_fetch(msg["after"])

    msg_cleaned = rtt_store_fetch(msg_cleaned)

    pre_final = aggregate_data(msg_cleaned["TRANSACTIONID"])

    
    final_data = msg_cleaned.copy()

    final_data.update(pre_final)

    data_to_send = make_json(final_data)


    for m in data_to_send:
        send_producer(m)
        write_to_json(m, f"data__{round(time.time()*1000)}.json")

        print(m)