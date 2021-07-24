import mysql.connector
import json
import os
import ast
import datetime
import time
import http.client
import pika 
import uuid

print('=============================================================================')

directory = os.path.dirname(os.path.abspath(__file__))
with open(directory + '/dbconfig.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
user = data['user']
password = data['password']
host = data['host']
database_host = data['database_host']

def get_db_connection(user, password, host, database_host):

    while True:
        try:
            cnx = mysql.connector.connect(user=user, password=password,
                                        host=host,
                                        database=database_host,
                                        connection_timeout=2)
            break
        except Exception as e:
            print(e)

    return cnx

def send_signal_rmq(action, side, leverage, uuid, mode, rmq_metadata):

    if mode == 'tester':
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
    cnx_ts = get_db_connection(user, password, host, database_host)
    cursor_ts = cnx_ts.cursor()
    query = ("SELECT trading_status FROM launch")
    cursor_ts.execute(query)
    for (trading_status) in cursor_ts:
        if trading_status[0] == 'on':
            cnx_ts.close()
            return trading_status[0]
    
    cnx_ts.close()
    return 'off'

cnx = get_db_connection(user, password, host, database_host)
cursor_candles = cnx.cursor()

cnx2 = get_db_connection(user, password, host, database_host)
cursor = cnx2.cursor()

launch = {}

query = ("SELECT algorithm, start_time, end_time, timeframe, symbol, mode, trading_status, rmq_metadata, deribit_metadata FROM launch")
cursor.execute(query)
for (postfix_algorithm, launch['start_time'], launch['end_time'], launch['time_frame'], 
launch['symbol'], launch['mode'], launch['trading_status'], launch['rmq_metadata'], launch['deribit_metadata']) in cursor:
    launch['algorithm'] = 'algorithm_' + str(postfix_algorithm)
    break

rmq_metadata = json.loads(launch['rmq_metadata'])
launch['deribit_metadata'] = json.loads(launch['deribit_metadata'])

price_table_name = 'price_' + str(launch['time_frame'])

cur_minute = (datetime.datetime.utcnow() - datetime.timedelta(minutes = 2*launch['time_frame'])).minute

keys = []
if launch['mode'] == 'tester':
    # ---- таблица свечей
    cursor_candles.execute('SELECT * FROM {0} WHERE time BETWEEN %s AND %s'.format(price_table_name), (launch['start_time'], launch['end_time']))
    keys_name = cursor_candles.description
    for row in keys_name:
        keys.append(row[0])


table_result = data['table_result']
table_result_sum = data['table_result_sum']
try:
    cursor.execute("TRUNCATE TABLE {0}".format(table_result))
    cursor.execute("TRUNCATE TABLE {0}".format(table_result_sum))
except Exception as e:
    print('Ошибка получения таблицы с результами, причина: ')
    print(e)

try:
    cursor.execute('SELECT * FROM {0}'.format(launch['algorithm']))
except Exception as e:
    print('Ошибка получения таблицы с настройками, причина: ')
    print(e)
rows1 = cursor.fetchall()

block_order = {}
iter = 0

blocks_data = rows1
for gg in rows1:
    block_order[str(gg[0])] = iter
    iter = iter + 1

strategy_state = 'check_blocks_conditions'
action_block = None
prev_candle = {}
prev_prev_candle = {}
last_trading_status = 'off'

# ---------- mode ---------------

def get_cur_time():
    return datetime.datetime.utcnow()

def set_candle(launch, keys, cursor, price_table_name, candle):

    global prev_candle
    global prev_prev_candle

    if launch['mode'] == 'tester':
        row = cursor.fetchone()
        if row != None:
            for ss in keys:
                candle[ss] = row[keys.index(ss)]

    else:
        
        if launch['mode'] == 'robot_debug':
            get_tick_from_table(launch, candle, 0)
            if candle == {}:
                return
            candle['price'] = float(candle['price'])
            cur_time = candle['time']
            price = candle['price']
        else:
            cur_time = get_cur_time()
            price = get_deribit_price(launch)
            if price != None:
                candle['price'] = price
                candle['time'] = cur_time

        prev_candle_time = cur_time - launch['time_frame'] * datetime.timedelta(seconds=60)
        prev_candle_prom = get_indicators(prev_candle_time, price_table_name)
        if prev_candle_prom != {}:
            if prev_candle != {} and prev_candle['time'] != prev_candle_prom['time']:
                launch['was_close'] = False
                print("prev_candle: " + str(prev_candle_prom))
            prev_candle = prev_candle_prom
            
        
        prev_prev_candle_time = cur_time - 2 * launch['time_frame'] * datetime.timedelta(seconds=60)
        prev_prev_candle_prom = get_indicators(prev_prev_candle_time, price_table_name)
        if prev_prev_candle_prom != {}:
            if prev_prev_candle != {} and prev_prev_candle['time'] != prev_prev_candle_prom['time']:
                print("prev_prev_candle: " + str(prev_prev_candle_prom))
            prev_prev_candle = prev_prev_candle_prom

        

def select_candle(date_time, table_name):
    
    cnx = get_db_connection(user, password, host, database_host)

    cursor = cnx.cursor()

    insert_stmt = ("select {0} from {1} "
    "where MINUTE(time) = %s and HOUR(time) = %s and DAY(time) = %s and MONTH(time) = %s and YEAR(time) = %s".format("*", table_name))

    data = (date_time.minute, date_time.hour, date_time.day, date_time.month, date_time.year)
    cursor.execute(insert_stmt, data)

    keys = []
    keys_name = cursor.description
    for row in keys_name:
        keys.append(row[0]) 
    
    candle = {}

    isNone = False

    for row in cursor:
        for ss in keys:
            candle[ss] = row[keys.index(ss)]
            #if candle[ss] == None:
            #    isNone = True

    cnx.commit()
    cnx.close()

    #if bool(candle) and not isNone:
    return candle
    #else:
    #    return None

def get_indicators(candle_time, table_name):

    global cur_minute

    if (candle_time.minute % launch['time_frame']) == 0 and cur_minute != candle_time.minute:
        result = select_candle(candle_time, table_name)
        if result != None:
            cur_minute = candle_time.minute
            return result

    time.sleep(1)
    return {}    

def get_deribit_price(launch):

    connection = http.client.HTTPSConnection(launch['deribit_metadata']['host'])
    connection.request("GET", "/api/v2/public/get_last_trades_by_instrument?count=1&instrument_name={0}".format(launch['symbol']))
    response = json.loads(connection.getresponse().read().decode())

    connection.close()

    if response.get('result') != None and response['result'].get('trades') != None and len(response['result']['trades']) > 0:
        price = response['result']['trades'][0]['price'] 
        print("deribit price = " + str(price))
        return price
    else:
        return None

def get_tick_from_table(launch, candle, last_id):

    tick_table_name = 'price_tick'

    if launch.get('ticks') == None:
        launch['ticks'] = {}
        ticks = launch['ticks']
        ticks['connection'] = get_db_connection(user, password, host, database_host)
        ticks['cursor'] = ticks['connection'].cursor()
        query = ("select * from {0} where id > {1}".format(tick_table_name, last_id))
        ticks['cursor'].execute(query)

        ticks['keys'] = []
        keys_name = ticks['cursor'].description
        for row in keys_name:
            ticks['keys'].append(row[0]) 

    try:
        row = launch['ticks']['cursor'].fetchone()
    except:
        id = launch['ticks']['last_id']
        launch.pop(launch['ticks'])
        get_tick_from_table(launch, candle, id)
        return

    launch['ticks']['last_id'] = row[0]
    if row == None:
        launch['ticks']['connection'].close()
    else:
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

    order['trailing_stop'] = 0
    order['uuid'] = str(uuid.uuid4())

    order['leverage'] = 1
    order['price_indent'] = 0
    order['direction'] = ''
    order['order_type'] = ''
    order['state'] = 'start'
    order['path'] = ''
    order['price'] = 0

    order['condition_checked_candle'] = None

    return order

def check_ohlc(candle):

    if (candle.get('open') == None
        or candle.get('close') == None
        or candle.get('high') == None
        or candle.get('low') == None):

        return False

    return True

def get_proboi_postfix(block, condition):

    return '_' + block['alg_number'] + '_' + condition['number']  + '_' + condition['name']

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

    if launch['mode'] != 'tester':
        
        if candle.get('price') == None:
            return False

        if direction == 'long':
            left_value = candle['price']
            right_value = pnl
        else:
            left_value = pnl
            right_value = candle['price']

        if ind_oper == '>=' and left_value >= right_value:
            return candle['price']
        elif ind_oper == '<=' and left_value <= right_value:
            return candle['price']
        elif ind_oper == '=' and left_value == right_value:
            return candle['price']
        elif ind_oper == '>' and left_value > right_value:
            return candle['price']
        elif ind_oper == '<' and left_value < right_value:
            return candle['price']
        else:
            return False
    
    if direction == 'long':
        if ind_oper == '>=':
            if candle['high'] >= pnl:
                return pnl
        if ind_oper == '<=':
            if candle['high'] <= pnl:
                return pnl
        if ind_oper == '=':
            if candle['high'] == pnl:
                return pnl
        if ind_oper == '>':
            if candle['high'] > pnl:
                return pnl
        if ind_oper == '<':
            if candle['high'] < pnl:
                return pnl
        if ind_oper == '>=':
            if candle['low'] >= pnl:
                return pnl
        if ind_oper == '<=':
            if candle['low'] <= pnl:
                return pnl
        if ind_oper == '=':
            if candle['low'] == pnl:
                return pnl
        if ind_oper == '>':
            if candle['low'] > pnl:
                return pnl
        if ind_oper == '<':
            if candle['low'] < pnl:
                return pnl
    else:
        if ind_oper == '>=':
            if pnl >= candle['high']:
                return pnl
        if ind_oper == '<=':
            if pnl <= candle['high']:
                return pnl
        if ind_oper == '=':
            if pnl == candle['high']:
                return pnl
        if ind_oper == '>':
            if pnl > candle['high']:
                return pnl
        if ind_oper == '<':
            if pnl < candle['high']:
                return pnl
        if ind_oper == '>=':
            if pnl >= candle['low']:
                return pnl
        if ind_oper == '<=':
            if pnl <= candle['low']:
                return pnl
        if ind_oper == '=':
            if pnl == candle['low']:
                return pnl
        if ind_oper == '>':
            if pnl > candle['low']:
                return pnl
        if ind_oper == '<':
            if pnl < candle['low']:
                return pnl
    return False

def check_trailing(condition, block, candle, order, prev_candle):

    direction = order['direction']

    value = int(condition['value'])

    if ((order['trailing_stop'] == 0)
        or (direction == 'long' and candle['high'] > prev_candle['high'])
        or (direction == 'short' and candle['low'] < prev_candle['low'])):
            if direction == 'long':
                order['trailing_stop'] = order['open_price_position'] + (float(candle['high']) - order['open_price_position']) * (value / 100)
            elif direction == 'short':
                order['trailing_stop'] = order['open_price_position'] - (order['open_price_position'] - float(candle['low'])) * (value / 100)
    else:
        if direction == 'long':
            if candle['low'] <= order['trailing_stop']:
                return order['trailing_stop']
        elif direction == 'short':
            if candle['high'] >= order['trailing_stop']:
                return order['trailing_stop']

    return 0

def check_exit_price(condition, block, candle, order, prev_candle):

    indicator_name = condition['name']
    side = condition['side']

    try:
        proboi = float(prev_candle[indicator_name + '-' + side])
    except:
        proboi = 0

    proc_value_2 = float(condition['exit_price_percent'])
    check = condition['check']
    try:
        exit_price_price = condition['exit_price_price']
    except:
        exit_price_price = False
    try:
        if check == 'low':
            proc = (float(proboi) - float(candle['low'])) / (float(proboi) / 100)
            value = float(proboi) - ((float(proboi) / 100) * proc_value_2)
        if check == 'close':
            if side == 'high':
                proc = (float(candle['close']) - float(proboi)) / (float(proboi)/100)
            if side == 'low':
                proc = (float(proboi) - float(candle['close'])) / (float(proboi) / 100)
            value = float(candle['close'])
        if check == 'high':
            proc = (float(candle['high']) - float(proboi)) / (float(proboi)/100)
            value = float(proboi) + ((float(proboi) / 100) * proc_value_2)

    except:
        return False
    if proc > proc_value_2:
        if exit_price_price:
            if check == 'low':
                if exit_price_price == 'yes':
                    if float(candle['close']) <= float(proboi):
                        return value
                else:
                    if float(candle['close']) > float(proboi):
                        return value
            if check == 'high':
                if exit_price_price == 'yes':
                    if float(candle['close']) >= float(proboi):
                        return value
                else:
                    if float(candle['close']) < float(proboi):
                        return value

        else:
            return value

    return False

def check_exit_price_by_step(condition, block, candle, order, prev_candle):

    if check_ohlc(candle) == False:
        return False

    pp = get_proboi_postfix(block, condition)
    
    side = condition['side']
    check = condition['check']

    if order['exit_price_price' + pp] == False:
        
        try:
            if check == 'low':
                if float(candle['low']) < float(order['proboi' + pp]):
                    proc = (float(order['old_proboi' + pp]) - float(candle['low' + pp])) / (float(order['old_proboi' + pp]) / 100)
                    return proc
            if check == 'close':

                # если уже было закрытие в данной свече
                if launch.get('was_close') != None and launch['was_close'] == True:
                    return False

                if side == 'high':
                    if float(candle['close']) > float(order['proboi' + pp]):
                        proc = (float(candle['close']) - float(order['proboi' + pp])) / (float(order['proboi' + pp])/100)
                        print('side=' + str(side) + ', check=' + str(check) +', close=' + str(candle['close']) + ',proboi=' + str(order['proboi' + pp]) +  ', name=' + str(condition['name']))
                        return proc
                if side == 'low':
                    if float(order['proboi' + pp]) > float(candle['close']):
                        proc = (float(order['proboi' + pp]) - float(candle['close' + pp])) / (float(order['proboi' + pp]) / 100)
                        print('side=' + str(side) + ', check=' + str(check) +', close=' + str(candle['close']) + ',proboi=' + str(order['proboi' + pp]) +  ', name=' + str(condition['name']))
                        return proc
            if check == 'high':
                if float(candle['high']) > float(order['proboi' + pp]):
                    proc = (float(candle['high']) - float(order['old_proboi' + pp])) / (float(order['old_proboi' + pp])/100)
                    return proc
        except:
            return False
        return False
    else:

        try:
            if check == 'low':
                if float(candle['low']) < float(order['proboi' + pp]):
                    if float(candle['close']) <= float(order['proboi' + pp]):
                        proc = (float(order['old_proboi' + pp]) - float(candle['low'])) / (float(order['old_proboi' + pp]) / 100)
                        return proc
            if check == 'close':
                if side == 'high':
                    if float(candle['close']) > float(order['proboi' + pp]):
                        proc = (float(candle['close']) - float(order['old_proboi' + pp])) / (float(order['old_proboi' + pp])/100)
                        return proc
                if side == 'low':
                    if float(order['proboi' + pp]) > float(candle['close']):
                        proc = (float(order['old_proboi' + pp]) - float(candle['close'])) / (float(order['old_proboi' + pp]) / 100)
                        return proc
            if check == 'high':
                if float(candle['high']) > float(order['proboi' + pp]):
                    if float(candle['close']) >= float(order['proboi' + pp]):
                        proc = (float(candle['high']) - float(order['old_proboi' + pp])) / (float(order['old_proboi' + pp])/100)
                        return proc
        except:
            return False
        return False

def check_exit_price_by_steps(condition, block, candle, order, prev_candle):

    pp = get_proboi_postfix(block, condition)

    if order.get('proboi_status' + pp) == None:
        order['proboi_status' + pp] = 0
    
    if order.get('proboi_step' + pp) == None:
        order['proboi_step' + pp] = 0

    if order.get('exit_price_price' + pp) == None:
        order['exit_price_price' + pp] = False

    if order.get('proboi' + pp) == None:
        order['proboi' + pp] = 0

    if order.get('old_proboi' + pp) == None:
        order['old_proboi' + pp] = 0

    if order.get('proboi_line_proc' + pp) == None:
        order['proboi_line_proc' + pp] = 0

    side = condition['side']
    check = condition['check']
    try:
        exit_price_price_main = condition['exit_price_price']
    except:
        exit_price_price_main = 'no'

    proc_value_2 = float(condition['exit_price_percent'])

    if prev_candle != None:
        old_proboi = order['proboi' + pp]
        proboi = float(prev_candle.get(condition['name'] + '-' + condition['side'], 0))
        order['proboi' + pp] = proboi
        if order['proboi_status' + pp] == 0:
            order['old_proboi' + pp] = order['proboi' + pp]
    else:
        order['proboi' + pp] = 0
        old_proboi = 0
    
    if condition.get('new_breakdown_sum') == None:
        new_breakdown_sum = 1
    else:
        new_breakdown_sum = int(condition['new_breakdown_sum'])
        
    if order['proboi_step' + pp] >= new_breakdown_sum and order['proboi_line_proc' + pp] >= proc_value_2:
        order['proboi_status' + pp] = 0
        order['close_time_order' + pp] = prev_candle['time']
        order['proboi_line_proc' + pp] = 0
        order['proboi_step' + pp] = 0
        order['old_proboi' + pp] = 0
        order['exit_price_price' + pp] = False
        return True
    else:
        if order['proboi_status' + pp] != 0 and side == 'high' and order['proboi' + pp] < old_proboi:
            order['old_proboi' + pp] = 0
            order['proboi_step' + pp] = 0
            order['proboi_line_proc' + pp] = 0
            order['proboi_status' + pp] = 0
            order['exit_price_price' + pp] = False
            return False
        if order['proboi_status' + pp] != 0 and side == 'low' and order['proboi' + pp] > old_proboi:
            order['old_proboi' + pp] = 0
            order['proboi_step' + pp] = 0
            order['proboi_line_proc' + pp] = 0
            order['proboi_status' + pp] = 0
            order['exit_price_price' + pp] = False
            return False
        result = check_exit_price_by_step(condition, block, candle, order, prev_candle)
        if result:

            order['proboi_status' + pp] = 1
            order['proboi_line_proc' + pp] = result

            order['proboi_step' + pp] = order['proboi_step' + pp] + 1
            if order['proboi_step' + pp] + 1 == new_breakdown_sum and exit_price_price_main == 'yes':
                order['exit_price_price' + pp] = True
            if order['proboi_step' + pp] >= new_breakdown_sum:
                if check == 'low':
                    order['close_price_position'] = float(order['proboi' + pp]) - ((float(order['proboi' + pp]) / 100) * proc_value_2)
                if check == 'close':
                    order['close_price_position'] = float(candle['close'])
                if check == 'high':
                    order['close_price_position'] = float(order['proboi' + pp]) + ((float(order['proboi' + pp]) / 100) * proc_value_2)
                    
            return False

    return False

# ---------- engine -----------------

def get_leverage(order, action, stat):
    
    leverage_start = action.get('leverage_start')
    if leverage_start == None:
        return float(action.get('leverage', 1))

    order['leverage_start'] = float(leverage_start)
    
    if stat['losses_money'] >= 0:
        return float(leverage_start)

    leverage_max = float(action.get('leverage_max'))
    leverage_take_price_percent = float(action.get('leverage_take_price_percent', '1'))

    leverage_take_money = order['price'] / 100 * float(leverage_take_price_percent) * float(leverage_start)

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

def get_activation_blocks(action_block, blocks_data, block_order):

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

def check_blocks_condition(blocks, candle, order, prev_candle, prev_prev_candle, launch):

    for block in blocks:
        if block_conditions_done(block, candle, order, prev_candle, prev_prev_candle, launch):
            return block
    
    return None

def set_done_conditions_group(conditions_group):
    for condition in conditions_group:
        condition['done'] = True    

def block_conditions_done(block, candle, order, prev_candle, prev_prev_candle, launch):

    cur_condition_number = None
    cur_conditions_group = []

    for condition in block['conditions']:
        
        if cur_condition_number != None and condition['number'] != cur_condition_number:
            set_done_conditions_group(cur_conditions_group)
            return False

        if condition.get('done') == None:
            condition['done'] = False

        if condition['done']:
            continue
        
        if condition['type'] == 'pnl':
            result = check_pnl(condition, block, candle, order, launch)
            if result == False:
                condition['done'] = False
                return False
            else:
                order['close_time_order'] = candle['time']
                order['close_price_position'] = result
        elif condition['type'] == 'trailing':
            result = check_trailing(condition, block, candle, order, prev_candle)
            if result == 0:
                condition['done'] = False
                return False
            else:
                order['close_time_order'] = candle['time']
                order['close_price_position'] = result
        elif condition['type'] == 'value_change':
            result = check_value_change(condition, block, candle, order, prev_candle, prev_prev_candle, launch)
            if result == False:
                condition['done'] = False
                return False
            else:
                order['condition_checked_candle'] = prev_candle
        elif condition['type'] == 'exit_price' and condition.get('new_breakdown_sum') == None:
            result = check_exit_price(condition, block, candle, order)
            if result == False:
                condition['done'] = False
                return False
            else:
                order['close_time_order'] = candle['time']
                order['close_price_position'] = result
        elif condition['type'] == 'exit_price' and condition.get('new_breakdown_sum') != None:
            if launch['mode'] == 'tester':
                result = check_exit_price_by_steps(condition, block, candle, order, prev_candle)
            else:
                result = check_exit_price_by_steps(condition, block, prev_candle, order, prev_prev_candle)
            if result == False:
                condition['done'] = False
                return False
            else:
                order['close_time_order'] = candle['time']
        else:
            condition['done'] = False
            return False

        cur_condition_number = condition['number']

        cur_conditions_group.append(condition)

        if order['condition_checked_candle'] == None:
            order['condition_checked_candle'] = candle
        
    if len(block['conditions']) == len(cur_conditions_group):
        set_done_conditions_group(cur_conditions_group)
    
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
                send_signal_rmq('close', order['direction'], order['leverage'], order['uuid'], launch['mode'], rmq_metadata)
                print('Закрытие позиции: ' + str(stat['percent_position']) + ', ' + str(order['close_time_position']))
                saved_close_time = order['close_time_order']
                saved_close_price = order['close_price_position']
                order = get_new_order(order)
                was_close = True
                if launch['trading_status'] == 'on': 
                    continue
                else:
                    return False    
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
                    order['price'] = saved_close_price
                order['state'] = 'order_is_opened'
            if order['state'] == 'order_is_opened':
                result = open_position(order, block, candle, stat, action, prev_candle)
                if result:
                    action['done'] = True
                    send_signal_rmq('open', order['direction'], order['leverage'], order['uuid'], launch['mode'], rmq_metadata)
                    print('Открытие позиции: ' + order['direction'] + ', ' + str(order['leverage']) + ', ' + str(order['open_time_position']))
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
        if order['open_price_position'] == 0:
            order['open_price_position'] = price
        if order['price'] == 0:
            order['price'] = price
        order['path'] = order['path'] + str(block['number']) + '_' + block['alg_number']
        order['leverage'] = round(get_leverage(order, action, stat), 2)
        db_open_position(order)
  
    return result

def close_position(order, block, candle, stat, action):
    
    points_position = 0

    if ((order['direction'] == 'long' and order['order_type'] == 'market') or
        (order['direction'] == 'short' and order['order_type'] == 'market')):
        
        # если уже было закрытие в данной свече
        if launch.get('was_close') != None and launch['was_close'] == True:
            order['close_time_order'] = 0
            return False

        order['path'] = order['path'] + ', ' + str(block['number']) + '_' + block['alg_number']

        if order['close_price_position'] == 0:
            if order['condition_checked_candle'] == None:
                order['close_price_position'] = float(candle['close'])
            else:
                order['close_price_position'] = float(order['condition_checked_candle']['close'])
        if order['direction'] == 'long':
            if order['close_price_position'] >= order['price']:
                result_position = 'profit'
                stat['profit_sum'] = stat['profit_sum'] + 1
            else:
                result_position = 'loss'
                stat['loss_sum'] = stat['loss_sum'] + 1
            if order['order_type'] == 'limit':
                points_position = order['close_price_position'] - order['price']
            else:
                points_position = order['close_price_position'] - order['price']
        else:
            if order['price'] >= order['close_price_position']:
                result_position = 'profit'
                stat['profit_sum'] = stat['profit_sum'] + 1
            else:
                result_position = 'loss'
                stat['loss_sum'] = stat['loss_sum'] + 1
            if order['order_type'] == 'limit':
                points_position = order['price'] - order['close_price_position']
            else:
                points_position = order['price'] - order['close_price_position']

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
        stat['last_percent_position'] = stat['percent_position']

        price_perecent = points_position / order['price'] * 100

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
        
        db_close_position(order, result_position, points_position, rpl, price_perecent)

        return True

    return False

# ---------- database ----------------------

def db_open_position(order):

    insert_stmt = (
        "INSERT INTO {0}(id_position, side, open_type_order, open_time_order, open_price_position, open_time_position, leverage, blocks_id)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(table_result)
    )
    data = (
        order['uuid'], order['direction'], order['order_type'], order['open_time_order'], 
        order['open_price_position'], order['open_time_position'], order['leverage'], order['path'])
    try:
        cursor.execute(insert_stmt, data)
        cnx2.commit()
    except Exception as e:
        print(e)

def db_close_position(order, result_position, points_position, rpl, price_perecent):

    insert_stmt = (
        "UPDATE {0} SET close_order_type = %s, close_time_order = %s, close_price_position = %s, close_time_position = %s, result_position = %s, points_position = %s, percent_position = %s, percent_series = %s, percent_price_deviation = %s, blocks_id = %s, percent_positions = %s, rpl = %s, losses_money = %s, price_perecent = %s"
        " where id_position = %s".format(table_result)
    )
    data = (
        order['order_type'], order['close_time_order'], order['close_price_position'], order['close_time_position'], result_position, points_position, 
        stat['percent_position'], stat['percent_series'], 0, order['path'], stat['percent_positions'], rpl, stat['losses_money'], price_perecent, order['uuid'])
    try:
        cursor.execute(insert_stmt, data)
        cnx2.commit()
    except Exception as e:
        print(e)

# ---------- main programm -----------------

activation_blocks = get_activation_blocks('0', blocks_data, block_order)
if len(activation_blocks) == 0:
    raise Exception('There is no first block in startegy')

while True: #цикл по свечам

    launch['trading_status'] = get_trading_status()
    if last_trading_status != 'on' and launch['trading_status'] == 'on':
        print('Робот запущен')

    if (launch['trading_status'] == 'off'
    or (launch['trading_status'] == 'off_after_close' and order['open_time_position'] == 0)):
        if last_trading_status == 'on':
            last_trading_status = launch['trading_status']
            print('Робот остановлен')
        continue

    last_trading_status = launch['trading_status']

    candle = {}
    set_candle(launch, keys, cursor_candles, price_table_name, candle)
    if candle == {}:
        if launch['mode'] != 'robot':
            continue
        else:
            break
            

    while True: #цикл по блокам
        
        # проверка условий активных блоков
        if strategy_state == 'check_blocks_conditions':
            action_block = check_blocks_condition(activation_blocks, candle, order, prev_candle, prev_prev_candle, launch)
            if action_block != None:
                strategy_state = 'execute_block_actions'
                # если в блоке нет текущих действий, то активным блоком назначаем следующий
                if len(action_block['actions']) == 0:
                    activation_blocks = get_activation_blocks(action_block, blocks_data, block_order)
                    # назначаем только, если он (блок) один и в нем нет условий
                    if len(activation_blocks) == 1 and len(activation_blocks[0]['conditions']) == 0:
                        action_block = activation_blocks[0]
            else:
                break
            
        # исполнение действий блока
        if strategy_state == 'execute_block_actions':
            result = execute_block_actions(action_block, candle, order, stat, launch)
            if result == True:
                activation_blocks = get_activation_blocks(action_block, blocks_data, block_order)
                strategy_state = 'check_blocks_conditions'
            else:
                break
    
    if launch['mode'] == 'tester':
        prev_candle = candle 
        prev_prev_candle = prev_candle
    #else:
    #    prev_candle['price'] = candle['price']

cnx.close()

all_orders = stat['profit_sum'] + stat['loss_sum']

if all_orders > 0:

    profit_positions_percent = stat['profit_sum']/(all_orders/100)
    loss_positions_percent = stat['loss_sum']/(all_orders/100)

    if stat['profit_sum'] == 0:
        profit_average_points = 0
    else:    
        profit_average_points = stat['profit_points'] / stat['profit_sum']
    if stat['loss_sum'] == 0:
        loss_average_points = 0
    else:    
        loss_average_points = stat['loss_points'] / stat['loss_sum']

    insert_stmt = ("INSERT INTO {0}(percent_positions, profit_positions_percent, profit_average_points, profit_sum, loss_positions_percent, loss_average_points, loss_sum)"
    "VALUES (%s, %s, %s, %s, %s, %s, %s)".format(table_result_sum))

    data = (int(stat['percent_positions']), int(profit_positions_percent), profit_average_points, stat['profit_sum'], int(loss_positions_percent), loss_average_points, int(stat['loss_sum']))
    cursor.execute(insert_stmt, data)

cnx2.commit()
cnx2.close()