import os

tables = {
    "rtt": "RETAILTRANSACTIONTABLE",
    "rtst": "RETAILTRANSACTIONSALESTRANS",
    "rcv": "RETAILCHANNELVIEW",
    "it": "INVENTTABLE"
}

topics = {
    "rtt_topic": "DebeziumTestServer.dbo.RETAILTRANSACTIONTABLE",
    "rtst_topic": "DebeziumTestServer.dbo.RETAILTRANSACTIONSAILESTRANS",
}

sql_conf = {
    "driver": 'ODBC Driver 17 for SQL Server',
    "server": 'tcp:172.31.70.20,1433' ,
    "database": 'MicrosoftDynamicsAX' ,
    "username": os.environ.get("DATABASE_USERNAME") ,
    "password": os.environ.get("DATABASE_PASS") ,
}
        