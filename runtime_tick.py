import datetime
import time
import http.client
import json
import mysql.connector

def get_deribit_price():

    launch = {}
    launch['symbol'] = 'BTC-PERPETUAL'
    launch['host'] = 'www.deribit.com'

    try:
        connection = http.client.HTTPSConnection(launch['host'], timeout=7)
        connection.request("GET", "/api/v2/public/get_last_trades_by_instrument?count=1&instrument_name={0}".format(launch['symbol']))
        response = json.loads(connection.getresponse().read().decode())

        connection.close()

        if response.get('result') != None and response['result'].get('trades') != None and len(response['result']['trades']) > 0:
            price = response['result']['trades'][0]['price'] 
            print("deribit price = " + str(price) + ", time = " + str(datetime.datetime.utcnow()))
            return price
        else:
            time.sleep(2)
            print("deribit error" + ", time = " + str(datetime.datetime.utcnow()))
            return None
    except Exception as e:
        time.sleep(2)
        print(e)
        print("deribit exception" + ", time = " + str(datetime.datetime.utcnow()))
        return None

def get_db_connection(user, password, host, database):

    while True:
        try:
            cnx = mysql.connector.connect(user=user, password=password,
                                        host=host,
                                        database=database,
                                        connection_timeout=2)
            cnx.autocommit = True
            break
        except Exception as e:
            time.sleep(2)
            print(e)

    return cnx

def insert_price(price):

    connection = get_db_connection("user", "userpass2", "95.142.38.22", "robot_1")
    cursor = connection.cursor()

    table_result = 'price_tick_1_00'

    try:
        query = (
            "INSERT INTO {0}(time, price)"
            "VALUES (%s, %s)".format(table_result)
        )
        data = (datetime.datetime.utcnow(), price)
    
        cursor.execute(query, data)
        connection.commit()
        connection.close()
    except Exception as e:
        print(e)


while True:
    
    price = get_deribit_price()
    if price == None:
        continue
    insert_price(price)