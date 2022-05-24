import pyodbc


def sql_initialize(sql_conf):
    conn = pyodbc.connect(
    f'DRIVER={sql_conf["driver"]};SERVER=' + sql_conf["server"] + ';DATABASE=' + sql_conf["database"] + \
    ';UID=' + sql_conf["username"] + ';PWD=' + sql_conf["password"])
    return conn.cursor()