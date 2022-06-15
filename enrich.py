from kafka import KafkaConsumer, KafkaProducer
from typing import List

from vars import *
import json
import redis
import sql_config
import time
from pathlib import Path

# Making a Kafka consumer
consumer = KafkaConsumer(
    topics["rtt_topic_2"],
    bootstrap_servers=['172.31.70.22:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=topics["rtt_topic_2"] + '__group16',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Making a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['172.31.70.22:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Uses 'sql_config' module to make a connection to SQL Server
cursor = sql_config.sql_initialize(sql_conf)

# Make an Instance of Redis to help fetch/ingest from/to Redis
r = redis.Redis(host="172.31.70.21", port=6379, db=0)


def rtt_check_store_redis(key):
    """
    This function jobs is data enrichment/de-normalization.
    This function accepts a key as a parameter which is 'TRANSACTIONID'
    and looks for corresponding 'TRANSACTIONID' store in Redis or main database.
    If corresponding TRANSACTIONID definition exist in Redis it fetches its definition respectively
    else it will search over main database by joining tables.
    Otherwise, it will return 'Unknown' as a value to store in result-set.
    Joining tables just needed for once, thus no need for afterward.
    :param key: 'TRANSACTIONID' with string data type
    :return: a corresponding data according to 'TRANSACTIONID'
    """
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
    """
        This function jobs is data enrichment/de-normalization.
        This function accepts a key as a parameter which is 'TRANSACTIONID'
        and looks for corresponding 'TRANSACTIONID' customer name in Redis or main database.
        If corresponding TRANSACTIONID definition exist in Redis it fetches its definition respectively
        else it will search over main database by joining tables.
        Otherwise, it will return 'Unknown' as a value to store in result-set.
        Joining tables just needed for once, thus no need for afterward.
        :param key: 'TRANSACTIONID' with string data type
        :return: a corresponding data according to 'TRANSACTIONID'
        """
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


# def rtt_store_fetch(new_data):
#     store = new_data["STORE"]
#     custaccount = new_data["CUSTACCOUNT"]
#     store_alias = rtt_check_store_redis(store)
#     custom_number = rtt_check_cust_account(custaccount)
#     if store_alias:
#         new_data["STORE"] = store_alias
#         new_data["CUSTACCOUNT"] = custom_number
#     return new_data


def rtt_store_fetch(new_data):
    """
    This function is first step to data enrichment which is helps
    fetch denormalized data corresponding 'STORE' and 'CUSTACCOUNT'
    columns fetch or/from Redis or main database.
    :param new_data: a dictionary of desired data
    :return: a dictionary with denormalized (enriched) of 'STORE' and 'CUSACCOUNT'
    name.
    """
    store = new_data["STORE"]
    custaccount = new_data["CUSTACCOUNT"]
    store_alias = rtt_check_store_redis(store)
    custom_number = rtt_check_cust_account(custaccount)

    # Add new fetched definition as new keys
    if store_alias:
        new_data["STORE"] = store_alias
        new_data["CUSTACCOUNT"] = custom_number
    return new_data


def rtst_fetch_discount_amount(transaction_id) -> List[float]:
    """
    This function accepts transaction_id and fetches discount amount by
    joining tables in main database.
    :param transaction_id: with string data type.
    :return: a list with fetched data from main database.
    """
    query_net_dicamount = "select d.DISCAMOUNT from RETAILTRANSACTIONTABLE c " \
                          "inner join RETAILTRANSACTIONSALESTRANS d on " \
                          "c.TRANSACTIONID = d.TRANSACTIONID " \
                          "where c.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_net_dicamount)

    return [float(val[0]) for val in cursor.fetchall()]


def rtst_fetch_price(transaction_id) -> List[float]:
    """
    This function fetches price from main database according to
    transaction_id by joining tables and return a list of prices.
    :param transaction_id: with string data type
    :return: list of product prices.
    """
    query_price = "select d.PRICE from RETAILTRANSACTIONTABLE c " \
                  " inner join RETAILTRANSACTIONSALESTRANS d on " \
                  "c.TRANSACTIONID = d.TRANSACTIONID " \
                  "where c.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_price)

    return [float(price[0]) for price in cursor.fetchall()]


def rtst_fetch_recid(transaction_id) -> List[float]:
    """
    This function fetches rec_id from main database according to
    transaction_id by joining tables and return a list of prices.
    :param transaction_id:
    :return: list of product red_ids.
    """
    query_recid = "select d.RECID from RETAILTRANSACTIONTABLE c " \
                  " inner join RETAILTRANSACTIONSALESTRANS d on " \
                  "c.TRANSACTIONID = d.TRANSACTIONID " \
                  "where c.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_recid)

    return [recid[0] for recid in cursor.fetchall()]


def rtst_fetch_itemid(transaction_id):
    """
    This function fetches item_id from main database according to
    transaction_id by joining tables and return a list of prices.
    :param transaction_id:
    :return: list ao product item_ids.
    """
    query_itemid = "select c.ITEMID from RETAILTRANSACTIONTABLE d " \
                   "inner join RETAILTRANSACTIONSALESTRANS c on " \
                   "d.TRANSACTIONID = c.TRANSACTIONID where d.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_itemid)

    return [val[0] for val in cursor.fetchall()]


def rtst_fetch_discount_amount(transaction_id):
    """
    This function fetches discount amount from main database according to
    transaction_id by joining tables and return a list of prices.
    :param transaction_id:
    :return: list ao product discount amount.
    """
    query_net_dicamount = "select d.DISCAMOUNT from RETAILTRANSACTIONTABLE c " \
                          "inner join RETAILTRANSACTIONSALESTRANS d on " \
                          "c.TRANSACTIONID = d.TRANSACTIONID " \
                          "where c.TRANSACTIONID = '%s'" % transaction_id
    cursor.execute(query_net_dicamount)

    return [float(val[0]) for val in cursor.fetchall()]


def rtst_fetch_namealiases_redis(transaction_id) -> List[float]:
    """
    Since, each product name (Name Alias) is needed for data de-normalization process and should
    be located beside item_id in enriched table for OLAP.
    This function helps to search each name_alias according to its corresponding transaction_id.
    Therefore, it determines each name_alias by searching over Redis.
    If it there won't be the name_alias according to its key, SQL query will fetch data from
    main database. Otherwise, Redis returns name_alais.
    Data structures are a bit complicated in this function. It first fetched item_id, then will
    search through name_aliases.
    :param transaction_id:
    :return: List of name_aliases
    """
    name_item = {}

    item_ids = rtst_fetch_itemid(transaction_id)

    for item in item_ids:
        temp_name = r.get(item)
        if temp_name:
            if item in name_item.keys():
                name_item[item + " "] = temp_name.decode('utf-8')
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
                                      f"b.ITEMID = {item} where c.TRANSACTIONID = '%s'" % transaction_id
                cursor.execute(query_transactionid)
                name_item[item] = cursor.fetchone()[0]
                r.set(item, str(name_item[item]))

    return [v for v in name_item.values()]


def rtt_data_fetch(dic):
    """
    This function accepts dict as a input parameter which can call raw data comes directly from Kafka and
    filters columns with those we don't need and keeps those we need.
    Columns we need are: '[TRANSACTIONID, STORE, TRANSTIME,
                  PAYMENTAMOUNT, CREATEDDATETIME, CUSTACCOUNT]'
    :param dic: a dictionary of raw data comes directly from Kafka
    :return: a dictionary contains just filtered columns
    """
    list_items = ["TRANSACTIONID", "STORE", "TRANSTIME",
                  "PAYMENTAMOUNT", "CREATEDDATETIME", "CUSTACCOUNT"]
    var = {k: v for k, v in dic.items() if k in [val for val in list_items]}
    return var


def aggregate_data(transaction_id):
    """
    This function can be assumed as one of the main functions over data enrichment/de-normalization
    process.
    It again accepts transaction_id as parameter and makes several data invocations from
    various functions. Those functions are described in its corresponding
    functions and commented in this function.
    The returned data is a dictionary which each values corresponding to keys are
    in Python list data type.
    :param transaction_id: transaction_id as a string data type.
    :return: a dictionary with enriched/de-normalized data.
    """
    data = {}

    # Fetch discount amount from main database.
    discount_amounts = rtst_fetch_discount_amount(transaction_id)

    # Fetch price from main database.
    prices = rtst_fetch_price(transaction_id)

    # Calculates net price of each item_id (which means each product which has been purchased).
    net_prices = [price - disc for price,
                                   disc in zip(prices, discount_amounts)]

    # Fetches name of each product that has been purchased.
    name_aliases = rtst_fetch_namealiases_redis(transaction_id)

    # Fetches rec_id according to each product has been purchased.
    recids = rtst_fetch_recid(transaction_id)

    # Each product has been identified by its corresponding item_id. Item_id required for
    # searching through tables to fetch corresponding definition e.g. 'product name or name alias'.
    item_ids = rtst_fetch_itemid(transaction_id)

    # Aggregate all fetched data as new keys
    data["ItemID"] = item_ids
    data["NameAlias"] = name_aliases
    data["Price"] = prices
    data["DiscountAmount"] = discount_amounts
    data["NetPrice"] = net_prices
    data["RecID"] = recids

    return data


def make_json(data):
    """
    Nonetheless, the data which is invoked as an input parameter is
    a dictionary with key and a list of values respectively,
    this function made to help Json file format to send cleaned structure data
    to Kafka.
    :param data: Dictionary
    :return: A list of seperated json format values.
    """
    list_items = ["ItemID", "NameAlias", "Price", "DiscountAmount", "NetPrice", "RecID"]
    header_items = ["TRANSACTIONID", "STORE", "TRANSTIME",
                    "PAYMENTAMOUNT", "CREATEDDATETIME", "CUSTACCOUNT"]
    temp_list = []
    file_body = {}

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


# Send cleaned structure data as producer to Kafka
def send_producer(ledger_data):
    if producer:
        print(producer)
        producer.send('ledger-08-16', ledger_data)


# Make .json file format to save in local storage
def write_to_json(message, file_name):
    base = Path('data')
    path_to_save = base / file_name
    base.mkdir(exist_ok=True)

    with open(path_to_save, "w") as f:
        json.dump(message, f)


# Main function for manipulating data
for msg in consumer:
    if msg is None:
        continue

    # Save data coming from Kafka
    msg = msg.value

    # This line of code invokes functions where filters some columns and make data available for certain columns
    msg_cleaned = rtt_data_fetch(msg["after"])

    # This line of code invokes functions where fetch store and custom_account from Redis or main database
    msg_cleaned = rtt_store_fetch(msg_cleaned)

    # This line of code invokes functions where are in charge of fetch various desired columns from main and/or Redis
    # and relocate it into data columns
    pre_final = aggregate_data(msg_cleaned["TRANSACTIONID"])

    # Just copy cleaned data to a variable
    final_data = msg_cleaned.copy()

    # Union all the data ino final_data dictionary
    final_data.update(pre_final)

    # Help to store final data as JSON format
    data_to_send = make_json(final_data)

    # All data which are formatted to Json will send to Kafka and write into Json file format simultaneously
    for m in data_to_send:
        send_producer(m)
        write_to_json(m, f"data__{round(time.time() * 1000)}.json")
        print(m)
