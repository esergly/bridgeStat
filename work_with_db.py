import datetime
import os
import sqlite3

from tqdm import tqdm


def data_set(lst):
    res = []
    node_id = lst.pop(0)
    for i in range(0, len(lst), 5):
        res.append((lst[i], node_id, lst[i + 1], lst[i + 2], lst[i + 3], lst[i + 4]))
    return res


if os.path.exists("counts_db.db"):
    os.remove("counts_db.db")
count = 1
con = sqlite3.connect('counts_db.db')
for each in ["REST", "SOAP", "B50"]:
    with con:
        con.execute("""
            CREATE TABLE {0} (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                node_id INTEGER NOT NULL,
                request_actual INTEGER NOT NULL,
                request_delta INTEGER NOT NULL,
                success_actual INTEGER NOT NULL,
                success_delta INTEGER NOT NULL
            );
        """.format(each))

for _ in range(1, 6):
    REST, SOAP, B50 = [], [], []
    with open("itbr{0}_healthcheck.log".format(_), 'r', encoding='utf-8') as file:
        REST.append(count)
        SOAP.append(count)
        B50.append(count)
        for line in tqdm(file):
            if "Start executing" in line:
                trap = line[41:56] + line[-13:-7]
                _ = datetime.datetime.strptime(trap.strip(' \t\r\n'), '%b %d %H:%M:%S %Y')
                str_time = datetime.datetime.strftime(_, '%Y-%m-%d %H:%M:%S')
                REST.append(str_time)
                SOAP.append(str_time)
                B50.append(str_time)
            elif 'r p="29"' in line or 'r p="30"' in line or 'r p="31"' in line or 'r p="32"' in line:
                REST.append(int(line[26:-5]))
            elif 'r p="33"' in line or 'r p="34"' in line or 'r p="35"' in line or 'r p="36"' in line:
                SOAP.append(int(line[26:-5]))
            elif 'r p="1"' in line or 'r p="2"' in line or 'r p="3"' in line or 'r p="4"' in line:
                B50.append(int(line[25:-5]))
    count += 1
    SQL_REST = 'INSERT INTO REST (' \
               'date, ' \
               'node_id, ' \
               'request_actual,' \
               'request_delta,' \
               'success_actual,' \
               'success_delta) ' \
               'values(?, ?, ?, ?, ?, ?)'
    SQL_SOAP = 'INSERT INTO SOAP (' \
               'date, ' \
               'node_id, ' \
               'request_actual,' \
               'request_delta,' \
               'success_actual,' \
               'success_delta) ' \
               'values(?, ?, ?, ?, ?, ?)'
    SQL_B50 = 'INSERT INTO B50 (' \
              'date, ' \
              'node_id, ' \
              'request_actual,' \
              'request_delta,' \
              'success_actual,' \
              'success_delta) ' \
              'values(?, ?, ?, ?, ?, ?)'

    data_rest = data_set(REST)
    data_soap = data_set(SOAP)
    data_b50 = data_set(B50)

    con = sqlite3.connect('counts_db.db')
    with con:
        con.executemany(SQL_REST, data_rest)
        con.executemany(SQL_SOAP, data_soap)
        con.executemany(SQL_B50, data_b50)
