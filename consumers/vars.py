import os
from dotenv import load_dotenv

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
    "rtt_topic": "DebeziumTestServer.dbo.RETAILTRANSACTIONTABLE",
    "rtt_topic_2": "Debezium.dbo.RETAILTRANSACTIONTABLE",
    "rtst_topic": "DebeziumTestServer.dbo.RETAILTRANSACTIONSALESTRANS",
}

sql_conf = {
    "driver": 'ODBC Driver 17 for SQL Server',
    "server": 'tcp:172.31.70.20,1433' ,
    "database": 'MicrosoftDynamicsAX' ,
    "username": os.environ.get("DATABASE_USERNAME") ,
    "password": os.environ.get("DATABASE_PASS") ,
}
        