import os
import sys
import numpy as np
import json
import datetime
import mysql.connector as my_conn
import time
from datetime import timedelta

start_time = time.time()

cnxn, cursor = None, None
directory = os.path.dirname(os.path.abspath(__file__))
with open(directory + '/dbconfig.json', 'r', encoding = 'utf-8') as f:
    data = json.load(f)
print(data)

try:
    cnxn = my_conn.connect(user = data["user"], password = data["password"],
                           host = data["host"], database = data["database_host"])
    cnxn.autocommit = True
    print('Успешно подключились к базе')
except Exception as e:
    print('Ошибка подключения, причина :')
    print(e)
else:
    cursor = cnxn.cursor()


def gosql(sql = '', cursor = None):
    er = ['НЕТ СОЕДИНЕНИЯ', 'СОЕДИНЕНИЕ УСТАНОВЛЕНО', 'ОШИБКА SQL']
    if cursor is not None:
        if sql[:6] == "SELECT":
            cursor.execute(sql)
            row0 = cursor.fetchone()
        elif sql[:6] == "select":
            cursor.execute(sql)
            row0 = cursor.fetchall()
        else:
            import logging
            logging.basicConfig()
            # noinspection PyBroadException
            try:
                row0 = cursor.execute(sql).rowcount
            except Exception:
                row0 = er[2]
    else:
        row0 = er[0]
    return row0


def create_table(percent = 0, cursor = None, data_in = None):
    pc = str(percent).split('.')
    if len(pc) == 2:
        table_write = 'price_' + str(percent).split('.')[0] + '_' + str(percent).split('.')[1].ljust(2, "0")
    else:
        table_write = 'price_' + str(percent) + '_00'
    print(table_write)
    create_stmt = "CREATE TABLE IF NOT EXISTS {} (id int NOT NULL, time datetime DEFAULT NULL, " \
                  "open decimal(12,2) DEFAULT NULL, high decimal(12,2) DEFAULT NULL, " \
                  "low decimal(12,2) DEFAULT NULL, close decimal(12,2) DEFAULT NULL," \
                  "PRIMARY KEY (id)," \
                  "KEY `time` (time))".format(table_write)
    try:
        gosql(sql = create_stmt, cursor = cursor)
    except Exception as e:
        print('Ошибка создания таблицы ' + table_write + ', причина: ')
        print(e)
    else:
        print('Таблица ' + table_write + ', существует: ')
        data_in['table_write'] = table_write
    return data_in


def create_table_open(cursor = None, data_in = None):
    table_title = 'opens_table'

    create_stmt = "CREATE TABLE IF NOT EXISTS {} (id int NOT NULL, time datetime DEFAULT NULL, " \
                  "open decimal(12,2) DEFAULT NULL," \
                  "PRIMARY KEY (id)," \
                  "KEY `time` (time))".format(table_title)
    try:
        cursor.execute(create_stmt)
    except Exception as e:
        print('Ошибка создания таблицы ' + table_title + ', причина: ')
        print(e)
    else:
        print('Таблица ' + table_title + ', существует: ')
        data_in['table_write'] = table_title
    return data_in


def truncate_table(data_in = None, cursor = None):
    table_write = data_in['table_write']
    try:
        gosql(sql = "TRUNCATE TABLE {0}".format(table_write), cursor = cursor)
    except Exception as e:
        print('Ошибка обнуления таблицы с результами, причина: ')
        print(e)


def select_data(id = 0, cursor = None, data_in = None):
    table_read = data_in['table_read']
    select_stmt = "SELECT {}, {}, {} FROM {} WHERE {} = {};".format('id', 'time', 'price', table_read, 'id', id)
    str_id = gosql(sql = select_stmt, cursor = cursor)
    return str_id


def select_data_all(id = 1, cursor = None, data_in = None, time_frame = 0):
    table_read = data_in['table_read']
    select_stmt = "select {}, {}, {} from {} where {} between {} and {};" \
        .format('id', 'time', 'price', table_read, 'id', id, int(time_frame))
    print(select_stmt)
    str_id_all = gosql(sql = select_stmt, cursor = cursor)
    return str_id_all


def insert_data(id, time, open, high, low, close, data_in):
    table_write = data_in['table_write']
    insert_stmt = "INSERT INTO {}({}, {}, {}, {}, {}, {}) VALUES ('{}', '{}', '{}', '{}', '{}', '{}')" \
        .format(table_write, 'id', 'time', 'open', 'high', 'low', 'close',
                str(id), str(time), str(open), str(high), str(low), str(close))
    gosql(sql = insert_stmt, cursor = cursor)


def insert_data_opens(id, time, open):
    insert_stmt = "INSERT INTO {}({}, {}, {}) VALUES ('{}', '{}', '{}')" \
        .format("opens_table", 'id', 'time', 'open', str(id), str(time), str(open))
    gosql(sql = insert_stmt, cursor = cursor)


def main_all(id = 1, cursor = None, data_in = None, time_frame = 0):
    truncate_table(data_in = data_in, cursor = cursor)
    sd_all = select_data_all(id = id, cursor = cursor, data_in = data_in, time_frame = time_frame)
    return sd_all


if len(sys.argv) == 3:
    percent = float(sys.argv[1])
    time_frame = int(sys.argv[2])
else:
    percent = 1
    time_frame = 1038

print(time_frame)
data = create_table(percent = percent, cursor = cursor, data_in = data)
# копируем данные исходной таблицы
sd_all = main_all(1, cursor, data, time_frame)

arr_minus = [float(sd_all[0][2]) - float(sd_all[0][2]) * (percent / 100)]
arr_plus = [float(sd_all[0][2])]

# создаём массив с отрицательными уровнями
for i in range(10000):
    arr_minus.insert(0, (round(arr_minus[0] - arr_minus[0] * (percent / 100), 2)))
print("arr_minus created")

# создаём массив с положительными уровнями
for i in range(10000):
    arr_plus.append(round(arr_plus[-1] * ((100 + percent) / 100), 2))
print("arr_plus created")

#  создаём общий массив уровней
main_arr = arr_minus + arr_plus

#  задаём стартовые значения переменным
open_tmp = main_arr[len(arr_minus)]
close_tmp = main_arr[len(arr_minus) + 1]
flag = 1
nmb = 1

opens_arr = []

# переменная для хранения последнего записанного времени
# нужна для проверки, что следующая строка в исходной таблице строго позже предыдущей
check_time = sd_all[0][1]
for i, item in enumerate(sd_all):
    time_index = 0
    if item[1] <= check_time:
        time_tmp = check_time + datetime.timedelta(seconds = 1)
        check_time = time_tmp
    else:
        time_tmp = item[1]
    # если тренд восходящий
    if flag == 1:
        # если вверх на 1 уровень
        if float(item[2]) > close_tmp:
            while float(item[2]) > close_tmp:
                if time_index != 0:
                    time_tmp += datetime.timedelta(seconds = 1)
                check_time = time_tmp
                # записываем в таблицу open и close
                insert_data(nmb, time_tmp, open_tmp, close_tmp, open_tmp, close_tmp, data)
                opens_arr.append([nmb, time_tmp, open_tmp])
                # затем сдвигаем open и close на 1 индекс вправо
                open_tmp = close_tmp
                close_tmp = main_arr[main_arr.index(open_tmp) + 1]
                nmb += 1
                time_index += 1
        # если вниз на 2 уровня, разворот
        elif float(item[2]) < main_arr[main_arr.index(open_tmp) - 2]:
            flag = -1
            while float(item[2]) < main_arr[main_arr.index(open_tmp) - 1]:
                if time_index != 0:
                    time_tmp += datetime.timedelta(seconds = 1)
                check_time = time_tmp
                # open приравниваем к прерыдущему open'у (как в таблице)
                open_tmp = main_arr[main_arr.index(open_tmp) - 1]
                # close приравниваем к уровню на 1 меньше open'а
                close_tmp = main_arr[main_arr.index(open_tmp) - 1]
                insert_data(nmb, time_tmp, open_tmp, open_tmp, close_tmp, close_tmp, data)
                opens_arr.append([nmb, time_tmp, open_tmp])
                nmb += 1
                time_index += 1
    # если тренд нисходящий
    elif flag == -1:
        # если вниз на 1 уровень
        if float(item[2]) < main_arr[main_arr.index(close_tmp) - 1]:
            while float(item[2]) < main_arr[main_arr.index(close_tmp) - 1]:
                if time_index != 0:
                    time_tmp += datetime.timedelta(seconds = 1)
                check_time = time_tmp
                open_tmp = close_tmp
                close_tmp = main_arr[main_arr.index(close_tmp) - 1]
                insert_data(nmb, time_tmp, open_tmp, open_tmp, close_tmp, close_tmp, data)
                opens_arr.append([nmb, time_tmp, open_tmp])
                nmb += 1
                time_index += 1
        # если вверх на 2 уровня, то разворот
        elif float(item[2]) > main_arr[main_arr.index(open_tmp) + 1]:
            flag = 1
            close_tmp = main_arr[main_arr.index(open_tmp) + 1]
            insert_data(nmb, time_tmp, open_tmp, close_tmp, open_tmp, close_tmp, data)
            opens_arr.append([nmb, time_tmp, open_tmp])
            nmb += 1
            time_index += 1
            while float(item[2]) > main_arr[main_arr.index(close_tmp) + 1]:
                if time_index != 0:
                    time_tmp += datetime.timedelta(seconds = 1)
                check_time = time_tmp
                open_tmp = close_tmp
                close_tmp = main_arr[main_arr.index(close_tmp) + 1]
                insert_data(nmb, time_tmp, open_tmp, close_tmp, open_tmp, close_tmp, data)
                opens_arr.append([nmb, time_tmp, open_tmp])
                nmb += 1
                time_index += 1
            open_tmp = close_tmp
            close_tmp = main_arr[main_arr.index(close_tmp) + 1]


data_opens = create_table_open(cursor = cursor, data_in = data)
truncate_table(data_opens, cursor)
for i in opens_arr:
    insert_data_opens(i[0], i[1], i[2])

if cnxn is not None:
    var = cnxn.close

elapsed_time_secs = time.time() - start_time

msg = "Execution took: %s secs (Wall clock time)" % timedelta(seconds=round(elapsed_time_secs))

print(msg)

exit(0)
