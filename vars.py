import os
from dotenv import load_dotenv
import json

load_dotenv()

tables = {
    "rtt": "RETAILTRANSACTIONTABLE",
    "rtst": "RETAILTRANSACTIONSALESTRANS",
    "rcv": "RETAILCHANNELVIEW",
    "it": "INVENTTABLE",
    "ouv": "OMOPERATINGUNITVIEW",
    "dpt": "DIRPARTYTABLE",
    "rct": "RETAILCHANNELTABLE",
    "ct": "CUSTTABLE",
}

topics = {
    # "rtt_topic": "DebeziumTestServer.dbo.RETAILTRANSACTIONTABLE",
    # "rtt_topic_2": "dbo.RTT",
    "rtt_topic_2": "tdb_server17.dbo.RETAILTRANSACTIONTABLE",
    "rtst_topic": "DebeziumTestServer.dbo.RETAILTRANSACTIONSALESTRANS",
}

sql_conf = {
    "driver": 'ODBC Driver 17 for SQL Server',
    "server": 'tcp:172.31.70.20,1433',
    "database": 'MicrosoftDynamicsAX',
    "username": os.environ.get("DATABASE_USERNAME"),
    "password": os.environ.get("DATABASE_PASS"),
}

kafka_consumer_conf = {
    "topic": topics["rtt_topic_2"],
    "bootstrap_servers": ['172.31.70.22:9092'],
    "auto_offset_reset": "earliest",
    "enable_auto_commit": True,
    "group_id": topics["rtt_topic_2"] + '__group',
}

kafka_producer_conf = {
    'bootstrap_servers': kafka_consumer_conf["bootstrap_servers"],
    "value_serializer": lambda x: json.dumps(x).encode('utf-8')
}
