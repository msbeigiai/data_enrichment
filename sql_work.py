import pyodbc
from vars import sql_conf


class SQLWork():
    def __init__(self) -> None:
        print(sql_conf["username"])
        self.conn = pyodbc.connect(f'DRIVER={sql_conf["driver"]};SERVER='+sql_conf["server"]+';DATABASE='+sql_conf["database"]+\
        ';UID='+sql_conf["username"]+';PWD='+ sql_conf["password"])
        
    def run_query(self, command):
        cursor = self.conn.cursor()
        cursor.execute(command)
        row = cursor.fetchone()
        while row:
            print(row[0])
            row = cursor.fetchone()