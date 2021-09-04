import mysql.connector
import json
import os
import ast
import datetime
import time
import http.client
import pika 
import uuid
import keyboard
import ssl
import requests

ssl._create_default_https_context = ssl._create_unverified_context

print('=============================================================================')

directory = os.path.dirname(os.path.abspath(__file__))
with open(directory + '/dbconfig.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
user = data['user']
password = data['password']
host = data['host']
database = data['database']

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

cn_db = get_db_connection(user, password, host, database)
cursor_db = cn_db.cursor()

keys_candle_table = []

def send_signal_rmq(action, side, leverage, uuid, mode, rmq_metadata):

    if mode != 'robot':
        return

    try:
        msg = {}
        msg['action'] = action
        msg['side'] = side
        msg['leverage'] = leverage
        msg['uuid'] = uuid
        msg['time_stamp'] = str(datetime.datetime.utcnow())

        credentials = pika.PlainCredentials(rmq_metadata['user'], rmq_metadata['password'])
        connection = pika.BlockingConnection(pika.ConnectionParameters(rmq_metadata['ip'], rmq_metadata['port'], rmq_metadata['vhost'], credentials))
        channel = connection.channel()
        channel.basic_publish(exchange=rmq_metadata['exchange'],
                        routing_key='',
                        body=json.dumps(msg))
        connection.close()
    except Exception as e:
        print(e)

def get_trading_status():
    
    global cn_db

    try:

        cursor_local = cn_db.cursor(buffered=True)
        query = ("SELECT trading_status FROM launch")
        cursor_local.execute(query)
        result = cursor_local.fetchone()
        for (trading_status) in result:
            return trading_status
        
        return 'on'
    except Exception as e:
        print(e)
        cn_db = get_db_connection(user, password, host, database)
        return get_trading_status()

cnx = get_db_connection(user, password, host, database)
cursor_candles = cnx.cursor()

cnx2 = get_db_connection(user, password, host, database)
cursor = cnx2.cursor()

cn_pos = get_db_connection(user, password, host, database)

def init_launch():
    launch = {}

    query = ("SELECT algorithm, start_time, end_time, timeframe, symbol, mode, trading_status, rmq_metadata, deribit_metadata, telegram_metadata FROM launch")
    cursor.execute(query)
    for (postfix_algorithm, launch['start_time'], launch['end_time'], launch['time_frame'], 
    launch['symbol'], launch['mode'], launch['trading_status'], launch['rmq_metadata'], launch['deribit_metadata'], launch['telegram_metadata']) in cursor:
        launch['algorithm'] = 'algorithm_' + str(postfix_algorithm)
        break

    launch['rmq_metadata'] = json.loads(launch['rmq_metadata'])
    launch['deribit_metadata'] = json.loads(launch['deribit_metadata'])
    launch['telegram_metadata'] = json.loads(launch['telegram_metadata'])

    launch['cur_conditions_group'] = {}
    launch['id_candle'] = 0
    launch['last_price'] = 0
    launch['empty_time_candles'] = 0

    return launch

launch = init_launch()

def db_get_algorithm(launch):
    
    try:
        cursor.execute('SELECT * FROM {0}'.format(launch['algorithm']))
    except Exception as e:
        print('Ошибка получения таблицы с настройками, причина: ')
        print(e)
    rows1 = cursor.fetchall()

    launch['algorithm_data'] = {}

    launch['algorithm_data']['block_order'] = {}
    iter = 0

    launch['algorithm_data']['blocks_data'] = rows1
    for gg in rows1:
        launch['algorithm_data']['block_order'][str(gg[0])] = iter
        iter = iter + 1


if launch['mode'] == 'tester':
    cn_tick = get_db_connection(user, password, host, database)

empty_time_candles = 10

price_table_name = 'price_' + str(launch['time_frame'])

cur_minute = (datetime.datetime.utcnow() - datetime.timedelta(minutes = 2*launch['time_frame'])).replace(second=0).replace(microsecond=0)

keys = []

table_result = data['table_result']
table_result_sum = data['table_result_sum']

if launch['mode'] != 'robot':
    try:
        cursor.execute("TRUNCATE TABLE {0}".format(table_result))
        cursor.execute("TRUNCATE TABLE {0}".format(table_result_sum))
    except Exception as e:
        print('Ошибка получения таблицы с результами, причина: ')
        print(e)



candle = {}
prev_candle = {}
prev_prev_candle = {}
robot_is_stoped = True

# ---------- mode ---------------

def get_cur_time():
    return datetime.datetime.utcnow()

def update_candle(launch):
    launch['id_candle'] = launch['id_candle'] + 1

def set_candle(launch, keys, cursor, price_table_name, candle, prev_candle, prev_prev_candle):

    if launch['mode'] == 'tester':
        get_tick_from_table(launch, candle, 0)
        if candle == {}:
            return
        candle['price'] = float(candle['price'])
        cur_time = candle['time']

    if launch['mode'] == 'robot':
        candle.clear()
        cur_time = get_cur_time()
        price = get_deribit_price(launch)
        if price != None:
            candle['price'] = price
            candle['time'] = cur_time

    prev_candle_time = cur_time - launch['time_frame'] * datetime.timedelta(seconds=60)
    prev_candle_prom = get_indicators(prev_candle_time, price_table_name)
    if prev_candle_prom != None and prev_candle_prom != {}:
        if ((prev_candle == {}) or (prev_candle != {} and prev_candle['time'] != prev_candle_prom['time'])):
            launch['was_close'] = False
            launch['was_open'] = False
            update_candle(launch)
            if launch['mode'] == 'robot':
                print("prev_candle: " + str(prev_candle_prom))
        prev_candle.update(prev_candle_prom)
    elif prev_candle_prom == None:
        prev_candle.clear()
    
    prev_prev_candle_time = cur_time - 2 * launch['time_frame'] * datetime.timedelta(seconds=60)
    prev_prev_candle_prom = get_indicators(prev_prev_candle_time, price_table_name)
    if prev_prev_candle_prom != None and prev_prev_candle_prom != {}:
        if ((prev_prev_candle == {}) or (prev_prev_candle != {} and prev_prev_candle['time'] != prev_prev_candle_prom['time'])):
            if launch['mode'] == 'robot':
                print("prev_prev_candle: " + str(prev_prev_candle_prom))
        prev_prev_candle.update(prev_prev_candle_prom)
    elif prev_prev_candle_prom == None:
        prev_prev_candle.clear()

def select_candle(date_time, table_name):
    
    global cn_db
    global cursor_db
    global keys_candle_table

    try: 

        date_time = date_time.replace(second=0)
        date_time = date_time.replace(microsecond=0)
        insert_stmt = ("select {0} from {1} where time = '{2}'".format("*", table_name, date_time))

        cursor_db.execute(insert_stmt)

        if len(keys_candle_table) == 0:
            keys_name = cursor_db.description
            for row in keys_name:
                keys_candle_table.append(row[0]) 
        
        candle = {}

        for row in cursor_db:
            for ss in keys_candle_table:
                candle[ss] = row[keys_candle_table.index(ss)]

        return candle

    except Exception as e:
        print(e)
        cn_db = get_db_connection(user, password, host, database)
        cursor_db = cn_db.cursor()
        select_candle(date_time, table_name)

def get_indicators(candle_time, table_name):

    global cur_minute

    candle_minute = candle_time.replace(second=0).replace(microsecond=0)

    if (candle_time.minute % launch['time_frame']) == 0 and cur_minute != candle_minute:
        result = select_candle(candle_time, table_name)
        if result != {}:
            cur_minute = candle_time.replace(second=0).replace(microsecond=0)
            return result
        else:
            return None

    return {}    

def get_deribit_price(launch):

    connection = http.client.HTTPSConnection(launch['deribit_metadata']['host'])
    connection.request("GET", "/api/v2/public/get_last_trades_by_instrument?count=1&instrument_name={0}".format(launch['symbol']))
    response = json.loads(connection.getresponse().read().decode())

    connection.close()

    if response.get('result') != None and response['result'].get('trades') != None and len(response['result']['trades']) > 0:
        price = response['result']['trades'][0]['price'] 
        print("deribit price = " + str(price) + ", time = " + str(datetime.datetime.utcnow()))
        return price
    else:
        return None

def get_tick_from_table1(launch, candle, last_id):

    tick_table_name = 'price_' + str(launch['time_frame'])

    if launch.get('ticks') == None:
        launch['ticks'] = {}
        ticks = launch['ticks']
        ticks['last_ohlc'] = 'close'
        ticks['connection'] = cn_db
        ticks['cursor'] = ticks['connection'].cursor(buffered=True)
        query = ("select * from {0} where id > {1} and time BETWEEN %s AND %s".format(tick_table_name, last_id))
        ticks['cursor'].execute(query, (launch['start_time'], launch['end_time']))

        ticks['keys'] = []
        keys_name = ticks['cursor'].description
        for row in keys_name:
            ticks['keys'].append(row[0]) 

    try:
        if launch['ticks']['last_ohlc'] == 'close':
            row = launch['ticks']['cursor'].fetchone()
            launch['ticks']['row'] = row
        else:
            row = launch['ticks']['row']
    except:
        id = launch['ticks']['last_id']
        launch['ticks'] = None
        get_tick_from_table(launch, candle, id)
        return

    if row == None:
        launch['ticks']['connection'].close()
    else:
        launch['ticks']['last_id'] = row[0]

        for ss in launch['ticks']['keys']:
             candle[ss] = row[launch['ticks']['keys'].index(ss)]

        if candle['open'] > candle['close']:
            if launch['ticks']['last_ohlc'] == 'close':
                candle['price'] = candle['open']
                launch['ticks']['last_ohlc'] = 'open'
            elif launch['ticks']['last_ohlc'] == 'open':
                candle['price'] = candle['high']
                launch['ticks']['last_ohlc'] = 'high'
            elif launch['ticks']['last_ohlc'] == 'high':
                candle['price'] = candle['low']
                launch['ticks']['last_ohlc'] = 'low'
            elif launch['ticks']['last_ohlc'] == 'low':
                candle['price'] = candle['close']
                launch['ticks']['last_ohlc'] = 'close'
        else:
            if launch['ticks']['last_ohlc'] == 'close':
                candle['price'] = candle['open']
                launch['ticks']['last_ohlc'] = 'open'
            elif launch['ticks']['last_ohlc'] == 'open':
                candle['price'] = candle['low']
                launch['ticks']['last_ohlc'] = 'low'
            elif launch['ticks']['last_ohlc'] == 'low':
                candle['price'] = candle['high']
                launch['ticks']['last_ohlc'] = 'high'
            elif launch['ticks']['last_ohlc'] == 'high':
                candle['price'] = candle['close']
                launch['ticks']['last_ohlc'] = 'close'

def get_tick_from_table(launch, candle, last_id):

    tick_table_name = 'price_tick'

    if launch.get('ticks') == None:
        launch['ticks'] = {}
        ticks = launch['ticks']
        ticks['connection'] = cn_tick
        ticks['cursor'] = ticks['connection'].cursor()
        query = ("select * from {0} where id > {1} and time BETWEEN %s AND %s".format(tick_table_name, last_id))
        ticks['cursor'].execute(query, (launch['start_time'], launch['end_time']))

        ticks['keys'] = []
        keys_name = ticks['cursor'].description
        for row in keys_name:
            ticks['keys'].append(row[0]) 

    try:
       row = launch['ticks']['cursor'].fetchone()
    except Exception as e:
        print(e)
        id = launch['ticks']['last_id']
        launch['ticks'] = None
        get_tick_from_table(launch, candle, id)
        return

    if row == None:
        launch['ticks']['connection'].close()
        candle.clear()
    else:
        launch['ticks']['last_id'] = row[0]

        for ss in launch['ticks']['keys']:
             candle[ss] = row[launch['ticks']['keys'].index(ss)]



# ---------- constructors ---------------

def get_new_statistics():

    stat = {}
    stat['profit_points'] = 0
    stat['loss_points'] = 0
    stat['loss_sum'] = 0
    stat['profit_sum'] = 0
    stat['percent_position'] = 0
    stat['last_percent_position'] = 0
    stat['percent_positions'] = 0
    stat['percent_series'] = 0

    stat['losses_money'] = 0

    stat['cur_month'] = 0
    stat['month_percent'] = 0
    stat['last_month_percent'] = 0
    
    stat['max_month_percent'] = 0
    stat['rollback_month_percent'] = 0

    return stat

def get_new_order(order):

    if order == None:
        order = {}

    order['open_price_position'] = 0
    order['close_price_position'] = 0

    order['open_time_order'] = 0
    order['open_time_position'] = 0
    order['close_time_position'] = 0
    order['close_time_order'] = 0

    order['trailings'] = {}
    order['uuid'] = str(uuid.uuid4())

    order['leverage'] = 1
    order['price_indent'] = 0
    order['direction'] = ''
    order['order_type'] = ''
    order['state'] = 'start'
    order['path'] = ''

    order['proboi'] = {}

    order['condition_checked_candle'] = None

    return order

def manage_order_tester(order, prev_candle, launch):
    
    if launch['mode'] != 'tester':
        return False
    
    if prev_candle == {}:
        launch['empty_time_candles'] = launch['empty_time_candles'] + 1
        if launch['empty_time_candles'] >= empty_time_candles:
            order = get_new_order(order)
            launch['cur_conditions_group'] = {}
            return True
    else:
        launch['empty_time_candles'] = 0

    return False

def get_new_tick(price, time):
    tick = {}
    tick['price'] = float(price)
    tick['time'] = time

    return tick

def get_proboi_id(block, condition):

    return block['alg_number'] + '_' + condition['number']  + '_' + condition['name']

order = get_new_order(None)
stat = get_new_statistics()

# ---------- conditions -----------------

def check_value_change(condition, block, candle, order, prev_candle, prev_prev_candle, launch):

    if prev_candle == {}:
        return False

    if prev_prev_candle == {}:
        return False    

    indicator = prev_candle.get(condition['name'])
    if indicator == None:
        return False

    last_ind = prev_prev_candle.get(condition['name'])
    if last_ind == None:
        return False

    if condition.get('value') != None:
        ind_oper = condition['value'].split(' ')[0]
        ind_value = float(condition['value'].split(' ')[1])
    else:
        ind_oper = ''
        ind_value = 0

    if condition.get('change'):
        change = condition['change']
    else:
        change = ''

    change_check = False

    if change == 'more_than_previous':
        if indicator > last_ind:
            change_check = True
    elif change == 'less_than_previous':
        if indicator < last_ind:
            change_check = True
    elif change == '':
        change_check = True
    
    if change_check == True:
        if ind_oper == '>=':
            if indicator >= ind_value:
               return True
        elif ind_oper == '<=':
            if indicator <= ind_value:
                return True
        elif ind_oper == '<':
            if indicator < ind_value:
                return True
        elif ind_oper == '>':
            if indicator > ind_value:
                return True
        elif ind_oper == '=':
            if indicator == ind_value:
                return True
        elif ind_oper == '':
            return True
        else:
            return True

    return False

def check_pnl(condition, block, candle, order, launch):
    
    direction = order['direction']

    ind_oper = condition['value'].split(' ')[0]
    ind_value = float(condition['value'].split(' ')[1])
    if direction == 'short':
        pnl = order['open_price_position'] - (((order['open_price_position'] / 100) * ind_value))/float(order['leverage'])
    else:
        pnl = order['open_price_position'] + (((order['open_price_position'] / 100) * ind_value))/float(order['leverage'])

    if candle.get('price') == None:
        return False

    if direction == 'long':
        left_value = candle['price']
        right_value = pnl
    else:
        left_value = pnl
        right_value = candle['price']

    if ind_oper == '>=' and left_value >= right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    elif ind_oper == '<=' and left_value <= right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    elif ind_oper == '=' and left_value == right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    elif ind_oper == '>' and left_value > right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    elif ind_oper == '<' and left_value < right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    else:
        return False

def check_exit_price_by_step(condition, block, candle, order, prev_candle):

    pid = get_proboi_id(block, condition)
    
    side = condition['side']
    check = condition['check']

    try:
        # если уже было открытие в данной свече
        if launch.get('was_open') != None and launch['was_open'] == True:
            return False
        # если уже было закрытие в данной свече
        if launch.get('was_close') != None and launch['was_close'] == True:
            return False
        if check == 'low':
            if float(candle['price']) < float(order['proboi'].get(pid)['proboi']):
                proc = (float(order['proboi'].get(pid)['proboi']) - float(candle['price'])) / (float(order['proboi'].get(pid)['proboi']) / 100)
                print('time=' + str(candle['time']) + ',side=' + str(side) + ', check=' + str(check) + ',proboi=' + str(order['proboi'].get(pid)['proboi']) +  ', name=' + str(condition['name']))
                return proc
        if check == 'close':
            if side == 'high':
                if float(candle['close']) > float(order['proboi'].get(pid)['proboi']):
                    proc = (float(candle['close']) - float(order['proboi'].get(pid)['proboi'])) / (float(order['proboi'].get(pid)['proboi'])/100)
                    print('time=' + str(candle['time']) + ',side=' + str(side) + ', check=' + str(check) +', close=' + str(candle['close']) + ',proboi=' + str(order['proboi'].get(pid)['proboi']) +  ', name=' + str(condition['name']))
                    return proc
            if side == 'low':
                if float(order['proboi'].get(pid)['proboi']) > float(candle['close']):
                    proc = (float(order['proboi'].get(pid)['proboi']) - float(candle['close'])) / (float(order['proboi'].get(pid)['proboi']) / 100)
                    print('time=' + str(candle['time']) + ',side=' + str(side) + ', check=' + str(check) +', close=' + str(candle['close']) + ',proboi=' + str(order['proboi'].get(pid)['proboi']) +  ', name=' + str(condition['name']))
                    return proc
        if check == 'high':
            if float(candle['price']) > float(order['proboi'][pid]['proboi']):
                proc = (float(candle['price']) - float(order['proboi'][pid]['proboi'])) / (float(order['proboi'][pid]['proboi'])/100)
                print('time=' + str(candle['time']) + ',side=' + str(side) + ', check=' + str(check) + ',proboi=' + str(order['proboi'].get(pid)['proboi']) +  ', name=' + str(condition['name']))
                return proc
    except:
        return False

    return False

def check_exit_price_by_steps(condition, block, candle, order, prev_candle):

    pid = get_proboi_id(block, condition)

    order['proboi'].setdefault(pid, {})

    order['proboi'].get(pid).setdefault('status', 0)
    order['proboi'].get(pid).setdefault('step', 0)
    order['proboi'].get(pid).setdefault('exit_price_price', False)
    order['proboi'].get(pid).setdefault('proboi', 0)
    order['proboi'].get(pid).setdefault('old_proboi', 0)
    order['proboi'].get(pid).setdefault('line_proc', 0)

    condition.setdefault('new_breakdown_sum', 1)

    side = condition['side']
    check = condition['check']

    exit_price_percent = float(condition['exit_price_percent'])

    if prev_candle != None:
        old_proboi = order['proboi'].get(pid)['proboi']

        level_name = condition['name'] + '-' + condition['side']
        proboi = prev_candle.get(level_name)
        if proboi == None:
            proboi = 0
        proboi = float(proboi)
        order['proboi'].get(pid)['proboi'] = proboi
        if order['proboi'].get(pid)['status'] == 0:
            order['proboi'].get(pid)['old_proboi'] = order['proboi'].get(pid)['proboi']
    else:
        order['proboi'].get(pid)['proboi'] = 0
        old_proboi = 0

    if order['proboi'].get(pid)['proboi'] == 0:
        return False
    
    if condition.get('new_breakdown_sum') == None:
        new_breakdown_sum = 1
    else:
        new_breakdown_sum = int(condition['new_breakdown_sum'])
        
    if order['proboi'].get(pid)['status'] != 0 and side == 'high' and order['proboi'].get(pid)['proboi'] < old_proboi:
        order['proboi'][pid] = {}
        return False
    if order['proboi'].get(pid)['status'] != 0 and side == 'low' and order['proboi'].get(pid)['proboi'] > old_proboi:
        order['proboi'][pid] = {}
        return False
    
    func_result = False
    price = 0
    result = check_exit_price_by_step(condition, block, candle, order, prev_candle)
    if result:

        order['proboi'].get(pid)['status'] = 1
        order['proboi'].get(pid)['line_proc'] = result + order['proboi'].get(pid)['line_proc']

        order['proboi'].get(pid)['step'] = order['proboi'].get(pid)['step'] + 1
        if order['proboi'].get(pid)['step'] >= new_breakdown_sum and order['proboi'].get(pid)['line_proc'] >= exit_price_percent:
            if order['open_time_position'] != 0:
                if check == 'low':
                    price = float(order['proboi'].get(pid)['proboi']) - ((float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] <= price:
                        order['close_price_position'] = price
                        func_result = True
                if check == 'close':
                    price = float(candle['close'])
                    order['close_price_position'] = price
                    func_result = True
                if check == 'high':
                    price = float(order['proboi'].get(pid)['proboi']) + ((float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] >= price:
                        order['close_price_position'] = price
                        func_result = True
            if order['open_time_position'] == 0:
                if check == 'low':
                    price = float(order['proboi'].get(pid)['proboi']) - ((float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] <= price:
                        order['open_price_position'] = price
                        func_result = True
                if check == 'close':
                    price = float(candle['close'])
                    order['open_price_position'] = price
                    func_result = True
                if check == 'high':
                    price = float(order['proboi'].get(pid)['proboi']) + ((float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] >= price:
                        order['open_price_position'] = price
                        func_result = True
            if func_result:
                order['proboi'][pid] = {}
                return price
            else:
                return False
        else:
            return False

    return False

def check_trailing(condition, block, candle, order, launch):

    direction = order['direction']

    back_percent = float(condition['back_percent'])

    result = False
    
    trailing = order['trailings'].setdefault(str(block['number']), {})

    trailing.setdefault('price', 0)
    trailing.setdefault('max_price', 0)
    trailing.setdefault('min_price', 0)

    price_change = True
    if direction == 'long' and (candle['price'] > trailing['max_price'] or trailing['max_price'] == 0):
        trailing['price'] = candle['price'] - (candle['price'] - order['open_price_position']) * back_percent / 100
        trailing['max_price'] = candle['price']
    elif direction == 'short' and (candle['price'] < trailing['min_price'] or trailing['min_price'] == 0):
        trailing['price'] = candle['price'] + (order['open_price_position'] - candle['price']) * back_percent / 100
        trailing['min_price'] = candle['price']
    else:
        price_change = False

    if price_change:
        print("trailing_price(change)=" + str(trailing['price']) + ", time = " + str(candle['time']) + ", price=" + str(candle['price']) + ", open_price=" + str(order['open_price_position']))            
                
    if trailing['price'] != 0:
        if direction == 'long' and candle['price'] < trailing['price']:
            result = trailing['price']
        elif direction == 'short' and candle['price'] > trailing['price']:
            result = trailing['price'] 

    if result != False:
        print("trailing_price(finish)=" + str(result) + ", time = " + str(candle['time']) + ", price=" + str(candle['price']))

    return result

def check_price(condition, block, candle, order, launch):
    
    direction = order['direction']

    ind_oper = condition['change_percent'].split(' ')[0]
    ind_value = float(condition['change_percent'].split(' ')[1])
    if direction == 'short':
        pnl = order['open_price_position'] - order['open_price_position'] / 100 * ind_value
    else:
        pnl = order['open_price_position'] + order['open_price_position'] / 100 * ind_value

    if candle.get('price') == None:
        return False

    if direction == 'long':
        left_value = candle['price']
        right_value = pnl
    else:
        left_value = pnl
        right_value = candle['price']

    if ind_oper == '>=' and left_value >= right_value:
        result = pnl
    elif ind_oper == '<=' and left_value <= right_value:
        result = pnl
    elif ind_oper == '=' and left_value == right_value:
        result = pnl
    elif ind_oper == '>' and left_value > right_value:
        result = pnl
    elif ind_oper == '<' and left_value < right_value:
        result = pnl
    else:
        result = False

    if result != False:
        print("price(" + direction + ", " + str(ind_value) +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))

    return result 

# ---------- engine -----------------

def get_leverage(order, action, stat):
    
    leverage_start = action.get('leverage_start')
    if leverage_start == None:
        return float(action.get('leverage', 1))

    leverage_max = action.get('leverage_max')
    if leverage_max == None:
        return float(action.get('leverage', 1))

    order['leverage_start'] = float(leverage_start)
    
    if stat['losses_money'] >= 0:
        return float(leverage_start)

    leverage_max = float(action.get('leverage_max'))
    leverage_take_price_percent = float(action.get('leverage_take_price_percent', '1'))

    leverage_take_money = order['open_price_position'] / 100 * float(leverage_take_price_percent) * float(leverage_start)

    leverage_compensation = (-stat['losses_money'] + float(leverage_take_money)) / float(leverage_take_money) * float(leverage_start)

    if leverage_max != None and leverage_max < leverage_compensation:
        leverage_compensation = leverage_max

    return leverage_compensation

def set_block_data(table_row, alg_number, col_number, col_conditions_a, col_activations):

    c_a = ast.literal_eval(table_row[col_conditions_a])
    if c_a.get('conditions') == None:
        conditions = []
    else:
        conditions = c_a['conditions']

    if c_a.get('actions') == None:
        actions = []
    else:
        actions = c_a['actions']

    block_data = {}
    block_data['conditions'] = conditions
    block_data['actions'] = actions
    block_data['number'] = table_row[col_number]
    block_data['activations'] = table_row[col_activations]
    block_data['alg_number'] = alg_number
    return block_data 

def get_activation_blocks(action_block, algorithm_data):

    blocks_data = algorithm_data['blocks_data']
    block_order = algorithm_data['block_order']

    blocks = []
    activation_blocks = []

    if action_block != '0':
        activations = action_block['activations'].split(',')
        for activtation in activations:
            if activtation == '0':
                continue
            else:
                if activtation == '':
                    action_block = '0'
                else:
                    activation_block = {}
                    activation_block['id'] = activtation.split('_')[0]
                    activation_block['direction'] = activtation.split('_')[1]
                    activation_blocks.append(activation_block)

    if action_block == '0':
        for block in blocks_data:
            if '0' in block[5].split(','):
                block_data = set_block_data(block, '1', 0, 4, 5)
                blocks.append(block_data)
            if '0' in block[7].split(','):
                block_data = set_block_data(block, '2', 0, 6, 7)
                blocks.append(block_data)
    else:
        for activation_block in activation_blocks: 

            index = block_order[activation_block['id']]

            if activation_block['direction'] == '1':
                block_data = set_block_data(blocks_data[index], '1', 0, 4, 5)
                blocks.append(block_data)
            else:
                block_data = set_block_data(blocks_data[index], '2', 0, 6, 7)
                blocks.append(block_data)
    
    return blocks

def drop_conditions(blocks, launch):
    launch['cur_conditions_group'] = {}

def check_blocks_condition(blocks, candle, order, prev_candle, prev_prev_candle, launch):

    for block in blocks:
        launch['cur_conditions_group'].setdefault(str(block['number']),[])
        if block_conditions_done(block, candle, order, prev_candle, prev_prev_candle, launch):
            drop_conditions(blocks, launch)
            return block
    
    return None

def set_done_conditions_group(conditions_group):

    for condition in conditions_group:
        condition['done'] = True

def undone_conditions_group(conditions_group):
    for condition in conditions_group:
        condition['done'] = False
        condition['id_candle'] = None

def block_conditions_done(block, candle, order, prev_candle, prev_prev_candle, launch):

    cur_condition_number = None
    cond_done_id_candle = None
    launch['prices'] = []

    cur_conditions_group = launch['cur_conditions_group'][str(block['number'])]

    # если изменилась свеча для текущего намбера, то обнуляем
    if len(cur_conditions_group) > 0 and cur_conditions_group[0]['id_candle'] != launch['id_candle']:
        undone_conditions_group(cur_conditions_group)
        launch['cur_conditions_group'][str(block['number'])] = []

    for condition in block['conditions']:

        condition.setdefault('done', False)

        # пропускаем те условия, которые отработали в данной свече
        if condition.get('id_candle') != None and condition['id_candle'] == launch['id_candle']:
            continue

        # если изменился намбер в цикле, то возвращаем False
        if cur_condition_number != None and condition['number'] != cur_condition_number:
             set_done_conditions_group(cur_conditions_group)
             launch['cur_conditions_group'][str(block['number'])] = []
             return False
        
        # если условие выполнилось, то продолжаем
        if condition['done'] == True:
            continue


        if cond_done_id_candle != None and cond_done_id_candle == launch['id_candle']: # ждем пока не появится новая свеча, чтобы проверить группу с новым намбером
            return False

        if condition['type'] == 'pnl':
            result = check_pnl(condition, block, candle, order, launch)
            if result == False:
                return False
            else:
                launch['prices'].append(result)
                order['close_time_order'] = candle['time']
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'value_change':
            result = check_value_change(condition, block, candle, order, prev_candle, prev_prev_candle, launch)
            if result == False:
                return False
            else:
                order['condition_checked_candle'] = prev_candle
                order['last_condition_type'] = 'history'
                order['close_time_order'] = 0
        elif condition['type'] == 'price':
            result = check_price(condition, block, candle, order, launch)
            if result == False:
                return False
            else:
                launch['prices'].append(result)
                order['close_time_order'] = candle['time']
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'trailing':
            result = check_trailing(condition, block, candle, order, launch)
            if result == False:
                return False
            else:
                launch['prices'].append(result)
                order['close_time_order'] = candle['time']
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'exit_price':
            check = condition['check']
            if check == 'high' or check == 'low':
                result = check_exit_price_by_steps(condition, block, candle, order, prev_candle)
                if result != False:
                    order['last_condition_type'] = 'realtime'
            else:
                result = check_exit_price_by_steps(condition, block, prev_candle, order, prev_prev_candle)
                if result != False:
                    order['last_condition_type'] = 'history'
            if result == False:
                return False
            else:
                launch['prices'].append(result)
                order['close_time_order'] = candle['time']
        else:
            return False

        # если условие выполнилось
        condition['id_candle'] = launch['id_candle']
        cur_condition_number = condition['number']

        launch['cur_conditions_group'][str(block['number'])].append(condition)

        if order['condition_checked_candle'] == None:
            order['condition_checked_candle'] = candle
        
    set_done_conditions_group(cur_conditions_group)
    launch['cur_conditions_group'][str(block['number'])] = []

    if len(launch['prices']) > 0:
        if candle['price'] < launch['last_price']:
            launch['price'] = min(launch['prices'])
        else:
            launch['price'] = max(launch['prices'])
    else:
        launch['price'] = 0
    
    return True

def execute_block_actions(block, candle, order, stat, launch):

    saved_close_time = 0
    saved_close_price = 0

    was_close = False

    for action in block['actions']:

        if action.get('done') and action['done'] == True:
            continue

        if action['order'] == "close":
            if action['direction'] != order['direction']:
                return False   
            if order['close_time_order'] == 0:
                order['close_time_order'] = candle['time']
            result = close_position(order, block, candle, stat, action)
            if result:
                action['done'] = True
                send_signal_rmq('close', order['direction'], order['leverage'], order['uuid'], launch['mode'], launch['rmq_metadata'])
                print('Закрытие позиции: ' + str(stat['percent_position']) + ', ' + str(order['close_time_position']))
                print('-------------------------------------------------------')
                saved_close_time = order['close_time_order']
                saved_close_price = order['close_price_position']
                order = get_new_order(order)
                if order['last_condition_type'] == 'realtime':
                    was_close = True
                if launch['trading_status'] == 'on': 
                    continue
                elif launch['trading_status'] == 'off_after_close':
                    return None    
            else:
                action['done'] = False
                return False
        if action['order'] == "open":
            if order['state'] == 'start':
                
                # если уже было закрытие в данной свече
                if launch.get('was_close') != None and launch['was_close'] == True:
                    return False

                order['order_type'] = action['order_type']
                order['direction'] = action['direction']
                if saved_close_time == 0:
                    order['open_time_order'] = candle['time']
                else:
                    order['open_time_order'] = saved_close_time
                if saved_close_price != 0:
                    order['open_price_position'] = saved_close_price
                order['state'] = 'order_is_opened'
            if order['state'] == 'order_is_opened':
                result = open_position(order, block, candle, stat, action, prev_candle)
                if result:
                    action['done'] = True
                    send_signal_rmq('open', order['direction'], order['leverage'], order['uuid'], launch['mode'], launch['rmq_metadata'])
                    print('Открытие позиции: ' + order['direction'] + ', ' + str(order['leverage']) + ', ' + str(order['open_time_position']))
                    launch['was_open'] = True
                else:
                    action['done'] = False
                    return False
            else:
                return False

    launch['was_close'] = was_close

    return True

def open_position(order, block, candle, stat, action, prev_candle):

    result = False

    if order['direction'] == 'long':
        if order['order_type'] == 'market':
            order['open_time_position'] = order['open_time_order']
            result = True
    elif order['direction'] == 'short':
        if order['order_type'] == 'market':
            order['open_time_position'] = order['open_time_order']
            result = True

    if result == True:
        price_old = prev_candle['close']
        if order['direction'] == 'long':
            price = float(price_old) - (float(price_old) / 100) * float(order['price_indent'])
        elif order['direction'] == 'short':
            price = float(price_old) + (float(price_old) / 100) * float(order['price_indent'])  
        if launch.get('price') != None and launch['price'] != 0:
            order['open_price_position'] = launch['price']
        if order['open_price_position'] == 0:
            order['open_price_position'] = price
        if order['path'] == '':
            pr_str = ''
        else:
            pr_str = ','
        order['path'] = order['path'] + pr_str + str(block['number']) + '_' + block['alg_number']
        order['leverage'] = round(get_leverage(order, action, stat), 2)
        if launch['mode'] == 'robot':
            db_open_position(order)

    launch['price'] = 0
  
    return result

def close_position(order, block, candle, stat, action):
    
    points_position = 0

    if ((order['direction'] == 'long' and order['order_type'] == 'market') or
        (order['direction'] == 'short' and order['order_type'] == 'market')):
        
        # если уже было закрытие в данной свече
        if launch.get('was_close') != None and launch['was_close'] == True:
            order['close_time_order'] = 0
            return False

        if type(block)  == str:
            order['path'] = order['path'] + ', ' + block
        else:
            order['path'] = order['path'] + ', ' + str(block['number']) + '_' + block['alg_number']

        if launch.get('price') != None and launch['price'] != 0:
            order['close_price_position'] = launch['price']
        launch['price'] = 0

        if order['close_price_position'] == 0:
            if order['condition_checked_candle'] == None:
                order['close_price_position'] = float(candle['close'])
            else:
                order['close_price_position'] = float(order['condition_checked_candle']['close'])
        if order['direction'] == 'long':
            if order['close_price_position'] >= order['open_price_position']:
                result_position = 'profit'
                stat['profit_sum'] = stat['profit_sum'] + 1
            else:
                result_position = 'loss'
                stat['loss_sum'] = stat['loss_sum'] + 1
            if order['order_type'] == 'limit':
                points_position = order['close_price_position'] - order['open_price_position']
            else:
                points_position = order['close_price_position'] - order['open_price_position']
        else:
            if order['open_price_position'] >= order['close_price_position']:
                result_position = 'profit'
                stat['profit_sum'] = stat['profit_sum'] + 1
            else:
                result_position = 'loss'
                stat['loss_sum'] = stat['loss_sum'] + 1
            if order['order_type'] == 'limit':
                points_position = order['open_price_position'] - order['close_price_position']
            else:
                points_position = order['open_price_position'] - order['close_price_position']

        rpl = points_position * float(order['leverage'])
        if order.get('leverage_start') != None and order['leverage'] > order['leverage_start'] and points_position >=0:
            rpl_comp = (points_position * float(order['leverage'])) - (points_position * float(order['leverage_start']))
        else:
            rpl_comp = rpl

        if result_position == 'profit':
            stat['profit_points'] = stat['profit_points'] + points_position
            if stat['losses_money'] < 0:
                stat['losses_money'] = stat['losses_money'] + rpl_comp    
        elif result_position == 'loss':
            stat['loss_points'] = stat['loss_points'] + points_position
            stat['losses_money'] = stat['losses_money'] + rpl_comp
        
        if stat['losses_money'] > 0: stat['losses_money'] = 0

        stat['percent_position'] = (points_position / order['open_price_position']) * 100 * float(order['leverage'])
        stat['percent_positions'] = stat['percent_positions'] + stat['percent_position']
        

        price_perecent = points_position / order['open_price_position'] * 100

        if result_position == 'profit': 
            if stat['percent_series'] <= 0:
                stat['percent_series'] = stat['percent_position']
            else:
                stat['percent_series'] = stat['percent_series'] + stat['percent_position']
        elif result_position == 'loss':
            if stat['percent_series'] >= 0:
                stat['percent_series'] = stat['percent_position']
            else:
                stat['percent_series'] = stat['percent_series'] + stat['percent_position']

        if order['order_type'] == 'market':
            order['close_time_position'] = order['close_time_order']

        if order['open_time_position'].month == stat['cur_month']:
            stat['month_percent'] = stat['last_month_percent'] + stat['percent_position']
        else:
            stat['month_percent'] = stat['percent_position']
            stat['cur_month'] = order['open_time_position'].month
            stat['max_month_percent'] = 0
            stat['rollback_month_percent'] = 0

        stat['last_month_percent'] = stat['month_percent']

        if stat['month_percent'] - stat['max_month_percent'] < stat['rollback_month_percent']:
            stat['rollback_month_percent'] = stat['month_percent'] - stat['max_month_percent']

        if stat['month_percent'] > stat['max_month_percent']:
            stat['max_month_percent'] = stat['month_percent']

        if launch['mode'] == 'robot':
            db_close_position(order, result_position, points_position, rpl, price_perecent, stat)
        else:
            db_insert_position(order, result_position, points_position, rpl, price_perecent, stat)

        stat['last_percent_position'] = stat['percent_position']

        return True

    return False

# ---------- database ----------------------

def db_open_position(order):
    
    global cn_db
    global cursor_db

    try:
        insert_stmt = (
            "INSERT INTO {0}(id_position, side, open_type_order, open_time_order, open_price_position, open_time_position, leverage, blocks_id)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(table_result)
        )
        data = (
            order['uuid'], order['direction'], order['order_type'], order['open_time_order'], 
            order['open_price_position'], order['open_time_position'], order['leverage'], order['path'])
    
        cursor_db.execute(insert_stmt, data)
        cn_db.commit()
        send_open_position_telegram(launch, order)
    except Exception as e:
        print(e)
        cn_db = get_db_connection(user, password, host, database)
        cursor_db = cn_db.cursor()
        db_open_position(order)

def db_close_position(order, result_position, points_position, rpl, price_perecent, stat):

    global cn_db
    global cursor_db

    try:
        insert_stmt = (
            "UPDATE {0} SET close_order_type = %s, close_time_order = %s, close_price_position = %s, close_time_position = %s, result_position = %s, points_position = %s, percent_position = %s, percent_series = %s, percent_price_deviation = %s, blocks_id = %s, percent_positions = %s, rpl = %s, losses_money = %s, price_perecent = %s, month_percent = %s, rollback_month_percent = %s"
            " where id_position = %s".format(table_result)
        )
        data = (
            order['order_type'], order['close_time_order'], order['close_price_position'], order['close_time_position'], result_position, points_position, 
            stat['percent_position'], stat['percent_series'], 0, order['path'], stat['percent_positions'], rpl, stat['losses_money'], price_perecent, stat['month_percent'], stat['rollback_month_percent'], order['uuid'])
        cursor_db.execute(insert_stmt, data)
        cn_db.commit()
        send_close_position_telegram(launch, order)
    except Exception as e:
        print(e)
        cn_db = get_db_connection(user, password, host, database)
        cursor_db = cn_db.cursor()
        db_close_position(order, result_position, points_position, rpl, price_perecent)

def db_insert_position(order, result_position, points_position, rpl, price_perecent, stat):

    global cn_pos
    cursor_local = cn_pos.cursor()

    try:
        insert_stmt = (
            "INSERT INTO {0}(id_position, side, open_type_order, open_time_order, open_price_position, open_time_position, leverage, blocks_id, month_percent,"
            "close_order_type, close_time_order, close_price_position, close_time_position , result_position , points_position , percent_position , percent_series , percent_price_deviation , percent_positions , rpl , losses_money , price_perecent, rollback_month_percent) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_result)
        )
        data = (
            order['uuid'], order['direction'], order['order_type'], order['open_time_order'], 
            order['open_price_position'], order['open_time_position'], order['leverage'], order['path'], stat['month_percent'],
            order['order_type'], order['close_time_order'], order['close_price_position'], order['close_time_position'], result_position, points_position, 
            stat['percent_position'], stat['percent_series'], 0, stat['percent_positions'], rpl, stat['losses_money'], price_perecent, stat['rollback_month_percent'])
    
        cursor_local.execute(insert_stmt, data)
        cn_pos.commit()
        cursor_local.close()

    except Exception as e:
        print(e)
        cn_pos = get_db_connection(user, password, host, database)
        db_insert_position(order, result_position, points_position, rpl, price_perecent, cn_pos)

def json_serial(obj):

    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        return obj.isoformat()

def load_with_datetime(pairs, format='%Y-%m-%dT%H:%M:%S'):
    """Load with dates"""
    d = {}
    for k, v in pairs:
        ok = False
        try:
            d[k] = datetime.datetime.strptime(v, format).date()
            ok = True
        except:
            d[k] = v
        if ok == False:
            try:
                d[k] = datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%S.%f').date()
            except:
                d[k] = v
    return d

def db_save_state(launch, stat, order):

    if launch['mode'] != 'robot':
        return False

    global cnx2
    global cursor

    launch_data = json.dumps(launch, default=json_serial)
    stat_data = json.dumps(stat, default=json_serial)
    order_data = json.dumps(order, default=json_serial)

    try:
        update_query = ("UPDATE launch SET launch_data = %s, stat_data = %s, order_data = %s where id = 1")
        data = (launch_data, stat_data, order_data)
        cursor.execute(update_query, data)
        cnx2.commit()
    except Exception as e:
        print(e)

def db_get_state(launch, stat, order):

    if launch['mode'] != 'robot':
        return False

    global cnx2
    global cursor

    launch_data = json.dumps(launch, default=json_serial)
    stat_data = json.dumps(stat, default=json_serial)
    order_data = json.dumps(order, default=json_serial)

    try:
        query = ("SELECT launch_data, stat_data, order_data FROM launch")
        cursor.execute(query)
        for (launch_data, stat_data, order_data) in cursor:
            if launch_data == "" or stat_data == "" or order_data == "" or launch_data == None or stat_data == None or order_data == None:
                return False

        launch_data = json.loads(launch_data, object_pairs_hook=load_with_datetime)
        stat_data = json.loads(stat_data, object_pairs_hook=load_with_datetime)
        order_data = json.loads(order_data, object_pairs_hook=load_with_datetime)

        launch.update(launch_data)
        stat.update(stat_data)
        order.update(order_data)

        return True

    except Exception as e:
        print(e)
        return False

def db_clear_state():

    try:
        update_query = ("UPDATE launch SET launch_data = %s, stat_data = %s, order_data = %s where id = 1")
        data = (None, None, None)
        cursor.execute(update_query, data)
        cnx2.commit()
        print("Контекст очищен")
    except Exception as e:
        print(e)

# ---------- telegram ----------------------

def send_open_position_telegram(launch, order):

    if launch['mode'] != 'robot':
        return

    global cn_db
    global cursor_db

    text = ''
    try:
        query = "select id, leverage from {0} where id_position = '{1}'".format(table_result, order['uuid'])
        cursor_db.execute(query)
        for (id, leverage)  in cursor_db:
            text = 'ID=' + str(id) + ", " + order['direction'] + ", open" + "\n" + str(order['open_price_position'])

        if text != '':
            send_telegram(launch, text)    
    except Exception as e:
        print(e)

def send_close_position_telegram(launch, order):

    if launch['mode'] != 'robot':
        return

    global cn_db
    global cursor_db

    text = ''
    try:
        query = "select id, percent_position, month_percent from {0} where id_position = '{1}'".format(table_result, order['uuid'])
        cursor_db.execute(query)
        for (id, percent_position, month_percent) in cursor_db:
            text = ('id' + str(id) + ", " + order['direction'] + ", close" +
                "\n" + str(order['close_price_position']) + " " + str(percent_position) + "%" +
                "\n" + str(month_percent) + "%"
                )
        if text != '':
            send_telegram(launch, text)    
    except Exception as e:
        print(e)


def send_telegram(launch, text):

    token = launch['telegram_metadata']['token'] # ключ тг бота
    url = "https://api.telegram.org/bot"
    channel_id = launch['telegram_metadata']['channel_id']
    url += token
    method = url + "/sendMessage"

    requests.post(method, data={
        "chat_id": channel_id,
        "text": text}) 


# ---------- main programm -----------------

def init_algo(launch):
    db_get_algorithm(launch)
    launch['strategy_state'] = 'check_blocks_conditions'
    launch['action_block'] = None
    launch['activation_blocks'] = get_activation_blocks('0', launch['algorithm_data'])
    if len(launch['activation_blocks']) == 0:
        raise Exception('There is no first block in startegy')

if db_get_state(launch, stat, order) != True:
    init_algo(launch)

while True: #цикл по тикам

    if launch['mode'] == 'robot':
        try:
            trading_status = get_trading_status()
        except Exception as e:
            print(e)
            continue

        if robot_is_stoped and trading_status == 'on':
            robot_is_stoped = False
            print('Робот запущен')

        robot_must_stop = (trading_status == 'off'
            or (launch['trading_status'] == "off_after_close" and order['open_time_position'] == 0)
            or trading_status == 'off_now_close')

        if robot_must_stop and robot_is_stoped == False:
            robot_is_stoped = True
            print('Робот остановлен')
            if trading_status != 'off':
                db_clear_state()
                launch = init_launch()
                init_algo(launch)

        launch['trading_status'] = trading_status

        if robot_is_stoped:
            continue

    try:
        set_candle(launch, keys, cursor_candles, price_table_name, candle, prev_candle, prev_prev_candle)
    except Exception as e:
        print(e)
        continue

    if manage_order_tester(order, prev_candle, launch):
        launch['strategy_state'] = 'check_blocks_conditions'
        launch['action_block'] = None
        launch['activation_blocks'] = get_activation_blocks('0', launch['algorithm_data'])
        continue

    if candle == {}:
        break

    if order['open_time_position'] != 0 and order['close_time_position'] == 0 and launch['trading_status'] == 'off_now_close':
        order['close_time_position'] = candle['time']
        order['close_time_order'] = candle['time']
        order['close_price_position'] = candle['price']
        if close_position(order, launch['trading_status'], candle, stat, None):
            send_signal_rmq('close', order['direction'], order['leverage'], order['uuid'], launch['mode'], launch['rmq_metadata'])
            print('Закрытие позиции: ' + str(stat['percent_position']) + ', ' + str(order['close_time_position']))
            print('-------------------------------------------------------')
            order = get_new_order(order)
            launch['activation_blocks'] = get_activation_blocks('0', launch['algorithm_data'])
            continue

    while True: #цикл по блокам
        
        # проверка условий активных блоков
        if launch['strategy_state'] == 'check_blocks_conditions':
            launch['action_block'] = check_blocks_condition(launch['activation_blocks'], candle, order, prev_candle, prev_prev_candle, launch)
            if launch['action_block'] != None:
                launch['strategy_state'] = 'execute_block_actions'
                # если в блоке нет текущих действий, то активным блоком назначаем следующий
                if len(launch['action_block']['actions']) == 0:
                    launch['activation_blocks'] = get_activation_blocks(launch['action_block'], launch['algorithm_data'])
                    # назначаем только, если он (блок) один и в нем нет условий
                    if len(launch['activation_blocks']) == 1 and len(launch['activation_blocks'][0]['conditions']) == 0:
                        launch['action_block'] = launch['activation_blocks'][0]
            else:
                break
            
        # исполнение действий блока
        if launch['strategy_state'] == 'execute_block_actions':
            result = execute_block_actions(launch['action_block'], candle, order, stat, launch)
            if result == True:
                launch['activation_blocks'] = get_activation_blocks(launch['action_block'], launch['algorithm_data'])
                launch['strategy_state'] = 'check_blocks_conditions'
                db_save_state(launch, stat, order)
            else:
                break

    launch['last_price'] = candle['price']
        

if cn_db:
    cn_db.close()

if cnx:  
    cnx.close()

if cn_pos:  
    cn_pos.close()

if cnx2:
    cnx2.close()

if launch['mode'] == 'tester' and cn_tick:
    cn_tick.close()