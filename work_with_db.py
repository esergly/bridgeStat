##!/usr/bin/env python3
"""
docstring
"""

import datetime
import os
import sqlite3
import sys

from tqdm import tqdm


def prepare_db():
    """
    :return:
    """
    if os.path.exists("counts_db.db"):
        try:
            os.remove("counts_db.db")
        except PermissionError:
            print("\U000026A0 The process cannot access the file because it is "
                  "being used by another process \U000026A0")
            sys.exit()
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


def counters_data_set(lst):
    """
    :param lst:
    :return:
    """
    res = []
    node_id = lst.pop(0)
    for i in range(0, len(lst), 5):
        res.append((lst[i], node_id, lst[i + 1], lst[i + 2], lst[i + 3], lst[i + 4]))
    return res


def insert_counters_in_db(lst):
    """
    :param lst:
    :return:
    """
    sql_insert = 'INSERT INTO {0} (' \
                 'date, ' \
                 'node_id, ' \
                 'request_actual,' \
                 'request_delta,' \
                 'success_actual,' \
                 'success_delta) ' \
                 'values(?, ?, ?, ?, ?, ?)'.format(lst.pop(0))
    data = counters_data_set(lst)
    con = sqlite3.connect('counts_db.db')
    with con:
        con.executemany(sql_insert, data)


def main_order():
    """
    :return:
    """
    print("Please wait until working DB will be created! \U000023F3")
    prepare_db()
    for _ in range(1, 6):
        rest, soap, b50 = ["rest"], ["soap"], ["b50"]
        with open(f'itbr{_}_healthcheck.log', 'r', encoding='utf-8') as file:
            for lst in (rest, soap, b50):
                lst.append(_)
            for _item in tqdm(range(os.path.getsize(f'itbr{_}_healthcheck.log') // 1048576)):
                for line in file:
                    if "Start executing" in line:
                        trap = line[41:56] + line[-13:-7]
                        _ = datetime.datetime.strptime(trap.strip(' \t\r\n'), '%b %d %H:%M:%S %Y')
                        str_time = datetime.datetime.strftime(_, '%Y-%m-%d %H:%M')
                        for lst in (rest, soap, b50):
                            lst.append(str_time)
                    elif 'r p="29' in line or 'r p="30' in line \
                            or 'r p="31' in line or 'r p="32' in line:
                        rest.append(int(line[26:-5]))
                    elif 'r p="33' in line or 'r p="34' in line \
                            or 'r p="35' in line or 'r p="36' in line:
                        soap.append(int(line[26:-5]))
                    elif 'r p="1"' in line or 'r p="2"' in line \
                            or 'r p="3"' in line or 'r p="4"' in line:
                        b50.append(int(line[25:-5]))
        for each in (rest, soap, b50):
            insert_counters_in_db(each)
    print("The DB is ready to be using.")


if __name__ == '__main__':
    main_order()

_VERSION_ = "0.1"
