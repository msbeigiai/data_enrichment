import pyodbc
from vars import sql_conf


class SQLWork():
    def __init__(self) -> None:
        print(sql_conf["username"])
        conn = pyodbc.connect(f'DRIVER={sql_conf["driver"]};SERVER='+sql_conf["server"]+';DATABASE='+sql_conf["database"]+\
        ';UID='+sql_conf["username"]+';PWD='+ sql_conf["password"])
        self.cursor = conn.cursor()
        if self.cursor:
            print(self.cursor)

    def run_query(self):
        self.cursor.execute("select @@version;")
        row = self.cursor.fetchone()
        while row:
            print(row[0])
            row = self.cursor.fetchone()