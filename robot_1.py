import mysql.connector
import json
import os
import ast

print('=============================================================================')

directory = os.path.dirname(os.path.abspath(__file__))
with open(directory + '/dbconfig.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
user = data['user']
password = data['password']
host = data['host']
database_host = data['database_host']
try:
    cnx = mysql.connector.connect(user=user, password=password,
                                  host=host,
                                  database=database_host)
    print('Успешно подключились к базе')
except Exception as e:
    print('Ошибка подключения, причина :')
    print(e)
cursor = cnx.cursor()


query = ("SELECT algorithm, start_time, end_time, timeframe FROM launch")
cursor.execute(query)
for (posfix_algorithm, start_time, end_time, timeframe) in cursor:
    algorithm = 'algorithm_' + str(posfix_algorithm)
    break

try:
    cursor.execute('SELECT * FROM {0} WHERE time BETWEEN %s AND %s'.format('price_' + str(timeframe)), (start_time, end_time))
except Exception as e:
    print('Ошибка получения таблицы с ценами, причина: ')
    print(e)
    

rows = cursor.fetchall()
keys_name = cursor.description
keys = []

back_price_1 = []
for row in keys_name:
    keys.append(row[0])
print(len(rows))
for row in rows:
    dict1 = {}
    for ss in keys:
        dict1[ss] = row[keys.index(ss)]
    back_price_1.append(dict1)
print(len(back_price_1))



table_result = data['table_result']
table_result_sum = data['table_result_sum']
try:
    cursor.execute("TRUNCATE TABLE {0}".format(table_result))
    cursor.execute("TRUNCATE TABLE {0}".format(table_result_sum))
except Exception as e:
    print('Ошибка получения таблицы с результами, причина: ')
    print(e)

try:
    cursor.execute('SELECT * FROM {0}'.format(algorithm))
except Exception as e:
    print('Ошибка получения таблицы с настройками, причина: ')
    print(e)
rows1 = cursor.fetchall()

rows2 = []

block_order = {}
iter = 0

blocks_data = rows1
for gg in rows1:
    block_order[str(gg[0])] = iter
    iter = iter + 1
rows1 = rows2

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

    order['leverage'] = 1
    order['price_indent'] = 0
    order['direction'] = ''
    order['order_type'] = ''
    order['state'] = 'start'
    order['path'] = ''
    order['price'] = 0

    order['condition_checked_candle'] = None

    return order

order = get_new_order(None)
stat = get_new_statistics()

# ---------- conditions -----------------

def check_value_change(condition, block, candle, order, prev_candle):

    if prev_candle == None:
        return False

    indicator = prev_candle[condition['name']]

    if back_price_1.index(prev_candle) == 0:
        return False

    try:
        last_ind = back_price_1[back_price_1.index(prev_candle) - 1][condition['name']]
    except:
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

def check_pnl(condition, block, candle, order):
    
    direction = order['direction']

    ind_oper = condition['value'].split(' ')[0]
    ind_value = float(condition['value'].split(' ')[1])
    if direction == 'short':
        pnl = order['open_price_position'] - (((order['open_price_position'] / 100) * ind_value))/float(order['leverage'])
    else:
        pnl = order['open_price_position'] + (((order['open_price_position'] / 100) * ind_value))/float(order['leverage'])

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

def check_exit_price(condition, block, candle, order):

    indicator_name = condition['name']
    side = condition['side']

    try:
        proboi = float(back_price_1[back_price_1.index(candle) - 1][indicator_name + '-' + side])
    except:
        proboi = 0

    proc_value_2 = float(condition['exit_price_percent'])
    check = condition['check']
    try:
        exit_price_price = condition['exit_price_price']
    except:
        exit_price_price = False
    #candle_check = 0
    try:
        if check == 'low':
            proc = (float(proboi) - float(candle['low'])) / (float(proboi) / 100)
            #candle_check = float(candle['low'])
            value = float(proboi) - ((float(proboi) / 100) * proc_value_2)
        if check == 'close':
            if side == 'high':
                proc = (float(candle['close']) - float(proboi)) / (float(proboi)/100)
                #candle_check = float(candle['close'])
            if side == 'low':
                proc = (float(proboi) - float(candle['close'])) / (float(proboi) / 100)
                #candle_check = float(candle['close'])
            value = float(candle['close'])
        if check == 'high':
            proc = (float(candle['high']) - float(proboi)) / (float(proboi)/100)
            #candle_check = float(candle['high'])
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

    side = condition['side']
    check = condition['check']

    if order['exit_price_price'] == False:
        
        try:
            if check == 'low':
                if float(candle['low']) < float(order['proboi']):
                    proc = (float(order['old_proboi']) - float(candle['low'])) / (float(order['old_proboi']) / 100)
                    return proc
            if check == 'close':
                if side == 'high':
                    if float(candle['close']) > float(order['proboi']):
                        proc = (float(candle['close']) - float(order['old_proboi'])) / (float(order['old_proboi'])/100)
                        return proc
                if side == 'low':
                    if float(order['proboi']) > float(candle['close']):
                        proc = (float(order['old_proboi']) - float(candle['close'])) / (float(order['old_proboi']) / 100)
                        return proc
            if check == 'high':
                if float(candle['high']) > float(order['proboi']):
                    proc = (float(candle['high']) - float(order['old_proboi'])) / (float(order['old_proboi'])/100)
                    return proc
        except:
            return False
        return False
    else:

        try:
            if check == 'low':
                if float(candle['low']) < float(order['proboi']):
                    if float(candle['close']) <= float(order['proboi']):
                        proc = (float(order['old_proboi']) - float(candle['low'])) / (float(order['old_proboi']) / 100)
                        return proc
            if check == 'close':
                if side == 'high':
                    if float(candle['close']) > float(order['proboi']):
                        proc = (float(candle['close']) - float(order['old_proboi'])) / (float(order['old_proboi'])/100)
                        return proc
                if side == 'low':
                    if float(order['proboi']) > float(candle['close']):
                        proc = (float(order['old_proboi']) - float(candle['close'])) / (float(order['old_proboi']) / 100)
                        return proc
            if check == 'high':
                if float(candle['high']) > float(order['proboi']):
                    if float(candle['close']) >= float(order['proboi']):
                        proc = (float(candle['high']) - float(order['old_proboi'])) / (float(order['old_proboi'])/100)
                        return proc
        except:
            return False
        return False

def check_exit_price_by_steps(condition, block, candle, order, prev_candle):

    if order.get('proboi_status') == None:
        order['proboi_status'] = 0
    
    if order.get('proboi_step') == None:
        order['proboi_step'] = 0

    if order.get('exit_price_price') == None:
        order['exit_price_price'] = False

    if order.get('proboi') == None:
        order['proboi'] = 0

    if order.get('old_proboi') == None:
        order['old_proboi'] = 0

    if order.get('proboi_line_proc') == None:
        order['proboi_line_proc'] = 0

    side = condition['side']
    check = condition['check']
    try:
        exit_price_price_main = condition['exit_price_price']
    except:
        exit_price_price_main = 'no'

    proc_value_2 = float(condition['exit_price_percent'])
    
    if prev_candle != None:
        old_proboi = order['proboi']
        order['proboi'] = float(prev_candle[condition['name'] + '-' + condition['side']])
        if order['proboi_status'] == 0:
            order['old_proboi'] = order['proboi']
    else:
        order['proboi'] = 0
        old_proboi = 0
    
    if condition.get('new_breakdown_sum') == None:
        new_breakdown_sum = 1
    else:
        new_breakdown_sum = int(condition['new_breakdown_sum'])
        
    if order['proboi_step'] >= new_breakdown_sum and order['proboi_line_proc'] >= proc_value_2:
        order['proboi_status'] = 0
        order['close_time_order'] = prev_candle['time']
        order['proboi_line_proc'] = 0
        order['proboi_step'] = 0
        order['old_proboi'] = 0
        order['exit_price_price'] = False
        return True
    else:
        if order['proboi_status'] != 0 and side == 'high' and order['proboi'] < old_proboi:
            order['old_proboi'] = 0
            order['proboi_step'] = 0
            order['proboi_line_proc'] = 0
            order['proboi_status'] = 0
            order['exit_price_price'] = False
            return False
        if order['proboi_status'] != 0 and side == 'low' and order['proboi'] > old_proboi:
            order['old_proboi'] = 0
            order['proboi_step'] = 0
            order['proboi_line_proc'] = 0
            order['proboi_status'] = 0
            order['exit_price_price'] = False
            return False
        result = check_exit_price_by_step(condition, block, candle, order, prev_candle)
        if result:

            order['proboi_status'] = 1
            order['proboi_line_proc'] = result

            order['proboi_step'] = order['proboi_step'] + 1
            if order['proboi_step'] + 1 == new_breakdown_sum and exit_price_price_main == 'yes':
                order['exit_price_price'] = True
            if order['proboi_step'] >= new_breakdown_sum:
                if check == 'low':
                    order['close_price_position'] = float(order['proboi']) - ((float(order['proboi']) / 100) * proc_value_2)
                if check == 'close':
                    order['close_price_position'] = float(candle['close'])
                if check == 'high':
                    order['close_price_position'] = float(order['proboi']) + ((float(order['proboi']) / 100) * proc_value_2)
                    
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

def check_blocks_condition(blocks, candle, order, prev_candle):

    for block in blocks:
        if block_conditions_done(block, candle, order, prev_candle):
            return block
    
    return None

def set_done_conditions_group(conditions_group):
    for condition in conditions_group:
        condition['done'] = True    

def block_conditions_done(block, candle, order, prev_candle):

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
            result = check_pnl(condition, block, candle, order)
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
            result = check_value_change(condition, block, candle, order, prev_candle)
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
            result = check_exit_price_by_steps(condition, block, candle, order, prev_candle)
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
        #condition['done'] = True

        if order['condition_checked_candle'] == None:
            order['condition_checked_candle'] = candle
        
    if len(block['conditions']) == len(cur_conditions_group):
        set_done_conditions_group(cur_conditions_group)
    
    return True

def execute_block_actions(block, candle, order, stat):

    saved_close_time = 0
    saved_close_price = 0

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
                print('Закрытие позиции: ' + str(order['close_time_position']))
                saved_close_time = order['close_time_order']
                saved_close_price = order['close_price_position']
                order = get_new_order(order)
                candle['was_close'] = True 
                continue
            else:
                action['done'] = False
                return False
        if action['order'] == "open":
            if order['state'] == 'start':
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
                result = open_position(order, block, candle, stat, action)
                if result:
                    action['done'] = True
                    print('Открытие позиции: ' + str(order['open_time_position']))
                else:
                    action['done'] = False
                    return False
            else:
                return False

    return True

def open_position(order, block, candle, stat, action):

    result = False

    if order['direction'] == 'long':
        if candle['low'] <= back_price_1[back_price_1.index(candle) - 1]['close'] and order['order_type'] == 'limit':
            order['open_time_position'] = back_price_1[back_price_1.index(candle) + 1]['time']
            result = True
        elif order['order_type'] == 'market':
            order['open_time_position'] = order['open_time_order']
            result = True
    elif order['direction'] == 'short':
        if candle['high'] >= back_price_1[back_price_1.index(candle) - 1]['close'] and order['order_type'] == 'limit':
            order['open_time_position'] = back_price_1[back_price_1.index(candle) + 1]['time']
            result = True
        if order['order_type'] == 'market':
            order['open_time_position'] = order['open_time_order']
            result = True

    if result == True:
        price_old = back_price_1[back_price_1.index(candle) - 1]['close']
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
  
    return result

def close_position(order, block, candle, stat, action):
    
    points_position = 0

    if ((candle['low'] <= back_price_1[back_price_1.index(candle) - 1]['close'] and order['order_type'] == 'limit' and order['direction'] == 'long' or order['direction'] == 'long' and order['order_type'] == 'market') or
        (candle['high'] >= back_price_1[back_price_1.index(candle) - 1]['close'] and order['order_type'] == 'limit' and order['direction'] == 'short' or order['direction'] == 'short' and order['order_type'] == 'market')):
        
        # если уже было закрытие в данной свече
        if candle.get('was_close') != None and candle['was_close'] == True:
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

        price_precent = points_position / order['price'] * 100

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

        if order['order_type'] == 'limit':
            order['close_time_position'] = back_price_1[back_price_1.index(candle)+1]['time']
        else:
            order['close_time_position'] = order['close_time_order']
        insert_stmt = (
            "INSERT INTO {0}(side, open_type_order, open_time_order, open_price_position, open_time_position, close_order_type, close_time_order, close_price_position, close_time_position, result_position, points_position, percent_position, percent_series, percent_price_deviation, blocks_id, percent_positions, leverage, rpl, losses_money, price_precent)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_result)
        )
        data = (
            order['direction'], order['order_type'], order['open_time_order'], order['open_price_position'], order['open_time_position'], order['order_type'],
            order['close_time_order'], order['close_price_position'], order['close_time_position'], result_position, points_position, stat['percent_position'], 
            stat['percent_series'], 0, order['path'], stat['percent_positions'], order['leverage'], rpl, stat['losses_money'], price_precent)
        try:
            cursor.execute(insert_stmt, data)
            cnx.commit()
        except Exception as e:
            print(e)

        return True

    return False

# ---------- main programm -----------------

activation_blocks = get_activation_blocks('0', blocks_data, block_order)
if len(activation_blocks) == 0:
    raise Exception('There is no first block in startegy')


strategy_state = 'check_blocks_conditions'
action_block = None
prev_candle = None

for candle in back_price_1:
    
    while True:
        
        # проверка условий активных блоков
        if strategy_state == 'check_blocks_conditions':
            action_block = check_blocks_condition(activation_blocks, candle, order, prev_candle)
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
            result = execute_block_actions(action_block, candle, order, stat)
            if result == True:
                activation_blocks = get_activation_blocks(action_block, blocks_data, block_order)
                strategy_state = 'check_blocks_conditions'
            else:
                    break
    
    prev_candle = candle 
 
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

cnx.commit()
cnx.close()