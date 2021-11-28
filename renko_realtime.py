import os
import sys
import numpy as np
import json
import datetime
import mysql.connector as my_conn
import time
from datetime import timedelta

start_time = time.time()

# максимальный допустимый разрыв между строками (в минутах)
skip_min = 7200
# переменная с названием таблицы с open'ами
table_title = ""
# частота проверки новых залитых строк (в сек)
time_sleep = 3.0
# стартовый уровень
start_level = 10000
# сколько обрабатываем записей за 1 раз
time_frame = 50000

if len(sys.argv) == 2:
    percent = float(sys.argv[1])


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
        # len = cursor.execute(sql).rowcount
        # print("Количество строк:", len)
    else:
        row0 = er[0]
    return row0


def create_table(percent = 0, cursor = None, data_in = None):
    global table_title_ohlc
    pc = str(percent).split('.')
    if len(pc) == 2:
        table_title_ohlc = 'price_' + str(percent).split('.')[0] + '_' + str(percent).split('.')[1].ljust(2, "0")
    else:
        table_title_ohlc = 'price_' + str(percent) + '_00'
    print(table_title_ohlc)
    create_stmt = "CREATE TABLE IF NOT EXISTS {} (id int NOT NULL AUTO_INCREMENT, time datetime DEFAULT NULL, " \
                  "open decimal(12,2) DEFAULT NULL, high decimal(12,2) DEFAULT NULL, " \
                  "low decimal(12,2) DEFAULT NULL, close decimal(12,2) DEFAULT NULL," \
                  "PRIMARY KEY (id)," \
                  "KEY `time` (time))".format(table_title_ohlc)
    try:
        gosql(sql = create_stmt, cursor = cursor)
    except Exception as e:
        print('Ошибка создания таблицы ' + table_title_ohlc + ', причина: ')
        print(e)
    else:
        print('Таблица ' + table_title_ohlc + ', существует: ')
        data_in['table_title_ohlc'] = table_title_ohlc
    return data_in


def create_table_open(percent, cursor = None, data_in = None):
    global table_title
    pc = str(percent).split('.')
    if len(pc) == 2:
        table_title = 'price_tick_' + pc[0] + '_' + pc[1].ljust(2, "0")
    else:
        table_title = 'price_tick_' + pc[0] + '_00'
    create_stmt = "CREATE TABLE IF NOT EXISTS {} (id int NOT NULL AUTO_INCREMENT, time datetime DEFAULT NULL, " \
                  "price decimal(12,2) DEFAULT NULL," \
                  "PRIMARY KEY (id)," \
                  "KEY `time` (time))".format(table_title)
    try:
        cursor.execute(create_stmt)
    except Exception as e:
        print('Ошибка создания таблицы ' + table_title + ', причина: ')
        print(e)
    else:
        print('Таблица ' + table_title + ', существует: ')
        data_in['table_title'] = table_title
    return data_in


def truncate_table(data_in = None, cursor = None, title = None):
    try:
        gosql(sql = "TRUNCATE TABLE {0}".format(title), cursor = cursor)
    except Exception as e:
        print('Ошибка обнуления таблицы с результами, причина: ')
        print(e)


# def select_data(id = 0, cursor = None, data_in = None):
#     table_read = data_in['table_read']
#     select_stmt = "SELECT {}, {}, {} FROM {} WHERE {} = {};".format('id', 'time', 'price', table_read, 'id', id)
#     str_id = gosql(sql = select_stmt, cursor = cursor)
#     return str_id


def select_data_all(id = 1, cursor = None, data_in = None, time_frame = 0):
    global step, start_index
    table_read = data_in['table_read']
    select_stmt = "select {}, {}, {} from {} where {} between {} and {};" \
        .format('id', 'time', 'price', table_read, 'id', last_id1, int(last_id1 + time_frame))
    print(select_stmt)
    print(f"Текущий step == {step}")
    str_id_all = gosql(sql = select_stmt, cursor = cursor)
    return str_id_all


def insert_data(id, time, open, high, low, close, data_in):
    global table_title_ohlc
    # table_title_ohlc = data_in['table_title_ohlc']
    insert_stmt = "INSERT INTO {}({}, {}, {}, {}, {}, {}) VALUES ('{}', '{}', '{}', '{}', '{}', '{}')" \
        .format(table_title_ohlc, 'id', 'time', 'open', 'high', 'low', 'close',
                str(id), str(time), str(open), str(high), str(low), str(close))
    gosql(sql = insert_stmt, cursor = cursor)


def insert_data_opens(id, time, open):
    insert_stmt = "INSERT INTO {}({}, {}, {}) VALUES ('{}', '{}', '{}')" \
        .format(table_title, 'id', 'time', 'price', str(id), str(time), str(open))
    gosql(sql = insert_stmt, cursor = cursor)


def main_all(id = 1, cursor = None, data_in = None, time_frame = 0):
    global step
    # if step == 1:
    #     truncate_table(data_in = data_in, cursor = cursor)
    sd_all = select_data_all(id = id, cursor = cursor, data_in = data_in, time_frame = time_frame)
    return sd_all


def found_flag(b):
    global flag, nmb, open_tmp, close_tmp, flag_0_i, time_frame
    # open_tmp = main_arr[len(arr_minus)]
    # close_tmp = main_arr[len(arr_minus) + 1]
    # open_tmp = main_arr[main_arr.index(open_tmp) + 1]
    # close_tmp = main_arr[main_arr.index(open_tmp) + 1]
    if open_tmp > close_tmp:
        open_tmp, close_tmp = close_tmp, open_tmp
    flag = 0
    while flag == 0:
        if sd_all[b][2] > close_tmp:
            flag = 1
            open_tmp = close_tmp
            close_tmp = main_arr[main_arr.index(open_tmp) + 1]
        elif sd_all[b][2] < open_tmp:
            flag = -1
            open_tmp = main_arr[main_arr.index(open_tmp) - 1]
            close_tmp = main_arr[main_arr.index(open_tmp) - 1]
        else:
            b += 1
            nmb += 1
        if b > time_frame - 1:
            print("Таблица закончилась")
            exit(0)
    flag_0_i = b


print(time_frame)
# счётчик итераций в количестве time_frame строк
step = 1
# номер строки в таблице, с которой начинаем копировать данные в очередной итерации
start_index = 1
# создаём таблицу с результатами open, high, low, close
data = create_table(percent = percent, cursor = cursor, data_in = data)
# номер текущей строки в price_tick
last_id1 = 1


def main_function():
    global step, arr_minus, arr_plus, main_arr, open_tmp, close_tmp, flag, opens_arr, start_index, last_id1, check_time, nmb
    while step:
        print(f"Итерация {step}")
        # копируем данные исходной таблицы в количестве time_frame строк
        sd_all = main_all(1, cursor, data, time_frame)
        # if sd_all == None:
        #     print("Конец таблицы")
        #     break
        # подготовительные действия перед стартом первой итерации
        if step == 1:
            arr_minus = [start_level - start_level * (percent / 100)]
            arr_plus = [start_level]
            # создаём массив с отрицательными уровнями
            for i in range(10000):
                arr_minus.insert(0, (round(arr_minus[0] - arr_minus[0] * (percent / 100), 2)))

            # создаём массив с положительными уровнями
            for i in range(10000):
                arr_plus.append(round(arr_plus[-1] * ((100 + percent) / 100), 2))

            # создаём общий массив уровней
            main_arr = arr_minus + arr_plus

            # задаём стартовые значения переменным
            # текущий open
            open_tmp = main_arr[len(arr_minus)]
            # текущий close
            close_tmp = main_arr[len(arr_minus) + 1]
            flag = 0
            i = 1
            # находим флаг
            while flag == 0:
                if sd_all[i][2] > close_tmp:
                    flag = 1
                elif sd_all[i][2] < open_tmp:
                    flag = -1
                else:
                    i += 1
            # текущий индекс (номер строки в результирующей таблице)
            nmb = 1
        flag_0_i = 0
        opens_arr = []

        # переменная для хранения последнего записанного времени
        # нужна для проверки, что следующая строка в исходной таблице строго позже предыдущей
        try:
            check_time = sd_all[0][1]
        except:
            print("Конец таблицы")
            break
        for i, item in enumerate(sd_all):
            if item[1] is not None:
                last_id1 = item[0]
            if i < flag_0_i:
                continue
            # переменная показывает, работаем мы сейчас со строкой из таблицы или итерируемся по уровням
            time_flag = 0
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
                        # проверка, что мы перешли к следующей строке, иначе добавляем секунду
                        if time_flag != 0:
                            time_tmp += datetime.timedelta(seconds = 1)
                        # проверка на разрыв по времени
                        if (item[1] - check_time).total_seconds() / 60 > skip_min:
                            found_flag(i)
                            insert_data(nmb, time_tmp, open_tmp, close_tmp, open_tmp, close_tmp, data)
                            opens_arr.append([nmb, time_tmp, open_tmp])
                            check_time = time_tmp
                            continue
                        check_time = time_tmp
                        # записываем в таблицу open и close
                        insert_data(nmb, time_tmp, open_tmp, close_tmp, open_tmp, close_tmp, data)
                        opens_arr.append([nmb, time_tmp, open_tmp])
                        # затем сдвигаем open и close на 1 индекс вправо
                        open_tmp = close_tmp
                        close_tmp = main_arr[main_arr.index(open_tmp) + 1]
                        nmb += 1
                        time_flag = 1
                # если вниз на 2 уровня, разворот
                elif float(item[2]) < main_arr[main_arr.index(open_tmp) - 2]:
                    flag = -1
                    while float(item[2]) < main_arr[main_arr.index(open_tmp) - 1]:
                        if time_flag != 0:
                            time_tmp += datetime.timedelta(seconds = 1)
                        # проверка на разрыв по времени
                        if (item[1] - check_time).total_seconds() / 60 > skip_min:
                            found_flag(i)
                            insert_data(nmb, time_tmp, open_tmp, close_tmp, open_tmp, close_tmp, data)
                            opens_arr.append([nmb, time_tmp, open_tmp])
                            check_time = time_tmp
                            continue
                        check_time = time_tmp
                        # open приравниваем к прерыдущему open'у (как в таблице)
                        open_tmp = main_arr[main_arr.index(open_tmp) - 1]
                        # close приравниваем к уровню на 1 меньше open'а
                        close_tmp = main_arr[main_arr.index(open_tmp) - 1]
                        if time_flag == 0:
                            insert_data(nmb, time_tmp, main_arr[main_arr.index(open_tmp) + 1],
                                        main_arr[main_arr.index(open_tmp) + 1], close_tmp, close_tmp, data)
                            opens_arr.append([nmb, time_tmp, main_arr[main_arr.index(open_tmp) + 1]])
                        else:
                            insert_data(nmb, time_tmp, open_tmp, open_tmp, close_tmp, close_tmp, data)
                            opens_arr.append([nmb, time_tmp, open_tmp])
                        nmb += 1
                        time_flag = 1
            # если тренд нисходящий
            elif flag == -1:
                # если вниз на 1 уровень
                if float(item[2]) < main_arr[main_arr.index(close_tmp) - 1]:
                    while float(item[2]) < main_arr[main_arr.index(close_tmp) - 1]:
                        if time_flag != 0:
                            time_tmp += datetime.timedelta(seconds = 1)
                        # проверка на разрыв по времени
                        if (item[1] - check_time).total_seconds() / 60 > skip_min:
                            found_flag(i)
                            insert_data(nmb, time_tmp, open_tmp, close_tmp, open_tmp, close_tmp, data)
                            opens_arr.append([nmb, time_tmp, open_tmp])
                            check_time = time_tmp
                            continue
                        check_time = time_tmp
                        open_tmp = close_tmp
                        close_tmp = main_arr[main_arr.index(close_tmp) - 1]
                        insert_data(nmb, time_tmp, open_tmp, open_tmp, close_tmp, close_tmp, data)
                        opens_arr.append([nmb, time_tmp, open_tmp])
                        nmb += 1
                        time_flag = 1
                # если вверх на 2 уровня, то разворот
                elif float(item[2]) > main_arr[main_arr.index(open_tmp) + 1]:
                    # проверка на разрыв по времени
                    if (item[1] - check_time).total_seconds() / 60 > skip_min:
                        found_flag(i)
                        insert_data(nmb, time_tmp, open_tmp, close_tmp, open_tmp, close_tmp, data)
                        opens_arr.append([nmb, time_tmp, open_tmp])
                        check_time = time_tmp
                        continue
                    check_time = time_tmp
                    flag = 1
                    close_tmp = main_arr[main_arr.index(open_tmp) + 1]
                    insert_data(nmb, time_tmp, main_arr[main_arr.index(open_tmp) - 1], close_tmp,
                                main_arr[main_arr.index(open_tmp) - 1], close_tmp, data)
                    opens_arr.append([nmb, time_tmp, open_tmp])
                    nmb += 1
                    time_flag = 1
                    while float(item[2]) > main_arr[main_arr.index(close_tmp) + 1]:
                        if time_flag != 0:
                            time_tmp += datetime.timedelta(seconds = 1)
                        open_tmp = close_tmp
                        close_tmp = main_arr[main_arr.index(close_tmp) + 1]
                        insert_data(nmb, time_tmp, open_tmp, close_tmp, open_tmp, close_tmp, data)
                        opens_arr.append([nmb, time_tmp, open_tmp])
                        nmb += 1
                        time_flag = 1
                    open_tmp = close_tmp
                    close_tmp = main_arr[main_arr.index(close_tmp) + 1]
        last_id1 += 1
        step += 1
        print(f"Конец итерации {step}")


main_function()
# last_id = cursor.lastrowid
print(f"last_id1  = {last_id1}")
count = 1
while step:
    time.sleep(time_sleep)
    print(f"Проверка {count}")
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
    try:
        cursor.execute(f"SELECT * FROM {data['table_read']} WHERE id between {last_id1} and {last_id1 + time_frame}")
        prnt = cursor.fetchone()
        if prnt is not None:
            print(prnt)
            print(cursor.fetchall())
            main_function()
            #last_id1 = cursor.lastrowid
            print(f"last_id1 = {last_id1}")
        else:
            print("Новых записей не найдено1")
    except Exception as e:
        print('Ошибка подключения, причина :')
        print(e)
    # except:
    #     print("Новых записей не найдено2")
    count += 1

    # cursor.execute(f"SELECT * FROM {table_title_ohlc} WHERE id = {cursor.lastrowid}")
    # prnt = cursor.fetchone()

# if cnxn is not None:
#    var = cnxn.close

elapsed_time_secs = time.time() - start_time

msg = "Execution took: %s secs (Wall clock time)" % timedelta(seconds = round(elapsed_time_secs))

print(msg)

# exit(0)
