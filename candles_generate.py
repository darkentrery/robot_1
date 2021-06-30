import random
import re
import mysql.connector
import time
import datetime

def insert_candle(date_time):
    
    table_name = 'price_1'

    cnx = mysql.connector.connect(user='tester', password='testerpass',
                                  host='95.142.38.22',
                                  database='robot_1')
    cursor = cnx.cursor()

    insert_stmt = ("INSERT INTO {0} (time, open, high, low, close, aidata_1)"
    "VALUES (%s, %s, %s, %s, %s, %s)".format(table_name))

    data = (date_time.strftime('%Y-%m-%d %H:%M:%S'), 1, 1, 1, 1, int(random.uniform(-110, 110)))
    cursor.execute(insert_stmt, data)

    cnx.commit()
    cnx.close()

def update_candle(date_time):
    
    table_name = 'price_1'

    cnx = mysql.connector.connect(user='tester', password='testerpass',
                                  host='95.142.38.22',
                                  database='robot_1')
    cursor = cnx.cursor()

    insert_stmt = ("UPDATE {0} SET aidata_1 = %s "
    "where MINUTE (time) = %s and HOUR (time) = %s and DAY (time) = %s and MONTH (time) = %s".format(table_name))

    data = (int(random.uniform(-200, 200)), date_time.minute - 1, date_time.hour, date_time.day, date_time.month)
    cursor.execute(insert_stmt, data)

    cnx.commit()
    cnx.close()


table_name = ''
time_frame = 1

printed_minute = datetime.datetime.now().minute


while True:

    cur_minute = datetime.datetime.now().minute
    if (cur_minute % time_frame) == 0 and printed_minute != cur_minute:
        time.sleep(20)
        print(cur_minute) 
        printed_minute = cur_minute
        update_candle(datetime.datetime.now())

    time.sleep(1)