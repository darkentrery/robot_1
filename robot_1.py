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
            time.sleep(5)
            if keyboard.is_pressed('s'):
                print('Скрипт остановлен!')
                break
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
            if trading_status == 'on' or trading_status == 'off_now_close':
                return trading_status
        
        return 'off'
    except Exception as e:
        print(e)
        cn_db = get_db_connection(user, password, host, database)
        return get_trading_status()

cnx = get_db_connection(user, password, host, database)
cursor_candles = cnx.cursor()

cnx2 = get_db_connection(user, password, host, database)
cursor = cnx2.cursor()

cn_pos = get_db_connection(user, password, host, database)

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

table_result = data['table_result']
table_result_sum = data['table_result_sum']

if launch['mode'] != 'robot':
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
candle = {}
prev_candle = {}
prev_prev_candle = {}
last_trading_status = 'off'

cnx2.close()

# ---------- mode ---------------

def get_cur_time():
    return datetime.datetime.utcnow()

def set_candle(launch, keys, cursor, price_table_name, candle, prev_candle, prev_prev_candle):

    if launch['mode'] == 'tester':
        if candle.get('time') != None:
            save_tick_time = candle['time']
            save_candle = candle.copy()
            save_prev_candle = prev_candle.copy()
        else:
            save_tick_time = None
        candle.clear()
        get_tick_from_table(launch, candle, 0)
        if candle == {}:
            return
        candle['price'] = float(candle['price'])
        cur_time = candle['time']
        price = candle['price']
        if save_tick_time != None and save_tick_time != candle['time']:
            prev_prev_candle.update(save_prev_candle)
            prev_candle.update(save_candle)
            launch['was_close'] = False
            launch['was_open'] = False
        

    if launch['mode'] == 'robot':
        
        candle.clear()
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
                launch['was_open'] = False
                if launch['mode'] == 'robot':
                    print("prev_candle: " + str(prev_candle_prom))
            prev_candle = prev_candle_prom
            
        
        prev_prev_candle_time = cur_time - 2 * launch['time_frame'] * datetime.timedelta(seconds=60)
        prev_prev_candle_prom = get_indicators(prev_prev_candle_time, price_table_name)
        if prev_prev_candle_prom != {}:
            if prev_prev_candle != {} and prev_prev_candle['time'] != prev_prev_candle_prom['time']:
                if launch['mode'] == 'robot':
                    print("prev_prev_candle: " + str(prev_prev_candle_prom))
            prev_prev_candle = prev_prev_candle_prom

def select_candle(date_time, table_name):
    
    global cn_db
    global cursor_db
    global keys_candle_table

    try: 

        date_time.replace(second=0)
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

    if (candle_time.minute % launch['time_frame']) == 0 and cur_minute != candle_time.minute:
        result = select_candle(candle_time, table_name)
        if result != None:
            cur_minute = candle_time.minute
            return result

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

    order['proboi'] = {}

    order['condition_checked_candle'] = None

    return order

def get_new_tick(price, time):
    tick = {}
    tick['price'] = float(price)
    tick['time'] = time

    return tick

def check_ohlc(candle):

    if (candle.get('open') == None
        or candle.get('close') == None
        or candle.get('high') == None
        or candle.get('low') == None):

        return False

    return True

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

    if launch['mode'] == 'robot':
        
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
                return proc
        if check == 'close':
            if side == 'high':
                if float(candle['close']) > float(order['proboi'].get(pid)['proboi']):
                    proc = (float(candle['close']) - float(order['proboi'].get(pid)['proboi'])) / (float(order['proboi'].get(pid)['proboi'])/100)
                    print('side=' + str(side) + ', check=' + str(check) +', close=' + str(candle['close']) + ',proboi=' + str(order['proboi'].get(pid)['proboi']) +  ', name=' + str(condition['name']))
                    return proc
            if side == 'low':
                if float(order['proboi'].get(pid)['proboi']) > float(candle['close']):
                    proc = (float(order['proboi'].get(pid)['proboi']) - float(candle['close'])) / (float(order['proboi'].get(pid)['proboi']) / 100)
                    print('side=' + str(side) + ', check=' + str(check) +', close=' + str(candle['close']) + ',proboi=' + str(order['proboi'].get(pid)['proboi']) +  ', name=' + str(condition['name']))
                    return proc
        if check == 'high':
            if float(candle['price']) > float(order['proboi'][pid]['proboi']):
                proc = (float(candle['price']) - float(order['proboi'][pid]['proboi'])) / (float(order['proboi'][pid]['proboi'])/100)
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
        proboi = float(prev_candle.get(condition['name'] + '-' + condition['side'], 0))
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
    result = check_exit_price_by_step(condition, block, candle, order, prev_candle)
    if result:

        order['proboi'].get(pid)['status'] = 1
        order['proboi'].get(pid)['line_proc'] = result + order['proboi'].get(pid)['line_proc']

        order['proboi'].get(pid)['step'] = order['proboi'].get(pid)['step'] + 1
        if order['proboi'].get(pid)['step'] >= new_breakdown_sum and order['proboi'].get(pid)['line_proc'] >= exit_price_percent:
            if order['open_time_position'] != 0:
                if check == 'low':
                    close_price_position = float(order['proboi'].get(pid)['proboi']) - ((float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] <= close_price_position:
                        order['close_price_position'] = close_price_position
                        func_result = True
                if check == 'close':
                    order['close_price_position'] = float(candle['close'])
                    func_result = True
                if check == 'high':
                    close_price_position = float(order['proboi'].get(pid)['proboi']) + ((float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] >= close_price_position:
                        order['close_price_position'] = close_price_position
                        func_result = True
            if order['open_time_position'] == 0:
                if check == 'low':
                    open_price_position = float(order['proboi'].get(pid)['proboi']) - ((float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] <= open_price_position:
                        order['open_price_position'] = open_price_position
                        func_result = True
                if check == 'close':
                    order['open_price_position'] = float(candle['close'])
                    func_result = True
                if check == 'high':
                    open_price_position = float(order['proboi'].get(pid)['proboi']) + ((float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] >= open_price_position:
                        order['open_price_position'] = open_price_position
                        func_result = True
            if func_result:
                order['proboi'][pid] = {}
                return True
            else:
                return False
        else:
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
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'value_change':
            result = check_value_change(condition, block, candle, order, prev_candle, prev_prev_candle, launch)
            if result == False:
                condition['done'] = False
                return False
            else:
                order['condition_checked_candle'] = prev_candle
                order['last_condition_type'] = 'history'
        elif condition['type'] == 'exit_price':
            check = condition['check']
            if check == 'high' or check == 'low':
                result = check_exit_price_by_steps(condition, block, candle, order, prev_candle)
                if result:
                    order['last_condition_type'] = 'realtime'
            else:
                result = check_exit_price_by_steps(condition, block, prev_candle, order, prev_prev_candle)
                if result:
                    order['last_condition_type'] = 'history'
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
                if order['last_condition_type'] == 'realtime':
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
        if order['open_price_position'] == 0:
            order['open_price_position'] = price
        if order['price'] == 0:
            order['price'] = price
        order['path'] = order['path'] + str(block['number']) + '_' + block['alg_number']
        order['leverage'] = round(get_leverage(order, action, stat), 2)
        if launch['mode'] == 'robot':
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

        if type(block)  == str:
            order['path'] = order['path'] + ', ' + block
        else:
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

        if order['open_time_position'].month == stat['cur_month']:
            stat['month_percent'] = stat['last_month_percent'] + stat['percent_position']
        else:
            stat['month_percent'] = stat['percent_position']
            stat['cur_month'] = order['open_time_position'].month
        stat['last_month_percent'] = stat['month_percent']


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
            "UPDATE {0} SET close_order_type = %s, close_time_order = %s, close_price_position = %s, close_time_position = %s, result_position = %s, points_position = %s, percent_position = %s, percent_series = %s, percent_price_deviation = %s, blocks_id = %s, percent_positions = %s, rpl = %s, losses_money = %s, price_perecent = %s, month_percent = %s"
            " where id_position = %s".format(table_result)
        )
        data = (
            order['order_type'], order['close_time_order'], order['close_price_position'], order['close_time_position'], result_position, points_position, 
            stat['percent_position'], stat['percent_series'], 0, order['path'], stat['percent_positions'], rpl, stat['losses_money'], price_perecent, stat['month_percent'], order['uuid'])
        cursor_db.execute(insert_stmt, data)
        cn_db.commit()
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
            "close_order_type, close_time_order, close_price_position, close_time_position , result_position , points_position , percent_position , percent_series , percent_price_deviation , percent_positions , rpl , losses_money , price_perecent) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_result)
        )
        data = (
            order['uuid'], order['direction'], order['order_type'], order['open_time_order'], 
            order['open_price_position'], order['open_time_position'], order['leverage'], order['path'], stat['month_percent'],
            order['order_type'], order['close_time_order'], order['close_price_position'], order['close_time_position'], result_position, points_position, 
            stat['percent_position'], stat['percent_series'], 0, stat['percent_positions'], rpl, stat['losses_money'], price_perecent)
    
        cursor_local.execute(insert_stmt, data)
        cn_pos.commit()
        cursor_local.close()
    except Exception as e:
        print(e)
        cn_pos = get_db_connection(user, password, host, database)
        db_insert_position(order, result_position, points_position, rpl, price_perecent, cn_pos)


# ---------- main programm -----------------

activation_blocks = get_activation_blocks('0', blocks_data, block_order)
if len(activation_blocks) == 0:
    raise Exception('There is no first block in startegy')


while True: #цикл по тикам

    if keyboard.is_pressed('s'):
        print('Скрипт остановлен!')
        break

    if launch['mode'] == 'robot':
        try:
            launch['trading_status'] = get_trading_status()
        except Exception as e:
            print(e)
            continue

        if last_trading_status != 'on' and launch['trading_status'] == 'on':
            print('Робот запущен')

        if (launch['trading_status'] == 'off'
        or (launch['trading_status'].startswith('off') and order['open_time_position'] == 0)):
            if last_trading_status == 'on':
                last_trading_status = launch['trading_status']
                print('Робот остановлен')
            continue

        last_trading_status = launch['trading_status']

    try:
        set_candle(launch, keys, cursor_candles, price_table_name, candle, prev_candle, prev_prev_candle)
    except Exception as e:
        print(e)
        continue


    if candle == {}:
        break

    if order['open_time_position'] != 0 and order['close_time_position'] == 0 and launch['trading_status'] == 'off_now_close':
        order['close_time_position'] = candle['time']
        order['close_time_order'] = candle['time']
        order['close_price_position'] = candle['price']
        if close_position(order, launch['trading_status'], candle, stat, None):
            send_signal_rmq('close', order['direction'], order['leverage'], order['uuid'], launch['mode'], rmq_metadata)
            print('Закрытие позиции: ' + str(stat['percent_position']) + ', ' + str(order['close_time_position']))
            order = get_new_order(order)
            activation_blocks = get_activation_blocks('0', blocks_data, block_order)
            continue

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
        

if cn_db:
    cn_db.close()

if cnx:  
    cnx.close()

if cn_pos:  
    cn_pos.close()

# all_orders = stat['profit_sum'] + stat['loss_sum']

# if all_orders > 0:

#     profit_positions_percent = stat['profit_sum']/(all_orders/100)
#     loss_positions_percent = stat['loss_sum']/(all_orders/100)

#     if stat['profit_sum'] == 0:
#         profit_average_points = 0
#     else:    
#         profit_average_points = stat['profit_points'] / stat['profit_sum']
#     if stat['loss_sum'] == 0:
#         loss_average_points = 0
#     else:    
#         loss_average_points = stat['loss_points'] / stat['loss_sum']

#     insert_stmt = ("INSERT INTO {0}(percent_positions, profit_positions_percent, profit_average_points, profit_sum, loss_positions_percent, loss_average_points, loss_sum)"
#     "VALUES (%s, %s, %s, %s, %s, %s, %s)".format(table_result_sum))

#     data = (int(stat['percent_positions']), int(profit_positions_percent), profit_average_points, stat['profit_sum'], int(loss_positions_percent), loss_average_points, int(stat['loss_sum']))
#     cursor.execute(insert_stmt, data)

# cnx2.commit()
# cnx2.close()