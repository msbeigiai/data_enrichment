import pyodbc
from vars import sql_conf


class SQLWork():
    def __init__(self) -> None:
        print(sql_conf["username"])
        conn = pyodbc.connect(f'DRIVER={sql_conf["driver"]};SERVER='+sql_conf["server"]+';DATABASE='+sql_conf["database"]+\
        ';UID='+sql_conf["username"]+';PWD='+ sql_conf["password"])
        cursor = conn.cursor()
        if cursor:
            print(cursor)