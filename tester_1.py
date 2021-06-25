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
table1 = data['table_price']
algorithm = data['table_algorithm']
try:
    cursor.execute('SELECT * FROM {0}'.format(table1))
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

squeeze = 0
ids = 0
money_deal = 0
percent_deal = 0
equity = 1
start_balance = 1
money_day = 0
percent_day = 0
min_percent_list = []
min_balance_percent = 0
rows2 = []
money_result = 0
profit_percent = 0
profit_sum = 0
loss_percent = 0
loss_sum = 0
id_day = 0

block_order = {}
iter = 0

blocks_data = rows1
for gg in rows1:
    block_order[str(gg[0])] = iter
    iter = iter + 1
rows1 = rows2

def reset_variables():

    global points_deal
    global fee
    global money_deal

    points_deal = 0
    fee = 0
    money_deal = 0

# ---------- conditions -----------------

def check_value_change(condition, block, candle, order, prev_candle):

    if prev_candle == None:
        return False

    indicator = prev_candle[condition['name']]
    try:
        last_ind = back_price_1[back_price_1.index(prev_candle) - 1][condition['name']]
    except:
        return False 

    ind_oper = condition['value'].split(' ')[0]
    ind_value = float(condition['value'].split(' ')[1])
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
        if ind_oper == '<=':
            if indicator <= ind_value:
                return True
        if ind_oper == '<':
            if indicator < ind_value:
                return True
        if ind_oper == '>':
            if indicator > ind_value:
                return True
        if ind_oper == '=':
            if indicator == ind_value:
                return True

    return False

def check_pnl(condition, block, candle, order):
    
    direction = order['direction']

    ind_oper = condition['value'].split(' ')[0]
    ind_value = float(condition['value'].split(' ')[1])
    if direction == 'short':
        pnl = order['open_price_order'] - (((order['open_price_order'] / 100) * ind_value))/float(order['leverage'])
    else:
        pnl = order['open_price_order'] + (((order['open_price_order'] / 100) * ind_value))/float(order['leverage'])

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
    candle_check = 0
    try:
        if check == 'low':
            proc = (float(proboi) - float(candle['low'])) / (float(proboi) / 100)
            candle_check = float(candle['low'])
            value = float(proboi) - ((float(proboi) / 100) * proc_value_2)
        if check == 'close':
            if side == 'high':
                proc = (float(candle['close']) - float(proboi)) / (float(proboi)/100)
                candle_check = float(candle['close'])
                #value = float(proboi) + ((float(proboi) / 100) * proc_value_2)
            if side == 'low':
                proc = (float(proboi) - float(candle['close'])) / (float(proboi) / 100)
                candle_check = float(candle['close'])
                #value = float(proboi) - ((float(proboi) / 100) * proc_value_2)
            value = float(candle['close'])
        if check == 'high':
            proc = (float(candle['high']) - float(proboi)) / (float(proboi)/100)
            candle_check = float(candle['high'])
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
            #print('-------------')
            #print('обнуление')
            #print('time == ' + str(candle['time']))
            order['old_proboi'] = 0
            order['proboi_step'] = 0
            order['proboi_line_proc'] = 0
            order['proboi_status'] = 0
            order['exit_price_price'] = False
            return False
        if order['proboi_status'] != 0 and side == 'low' and order['proboi'] > old_proboi:
            #print('-------------')
            #print('обнуление')
            #print('time == ' + str(candle['time']))
            order['old_proboi'] = 0
            order['proboi_step'] = 0
            order['proboi_line_proc'] = 0
            order['proboi_status'] = 0
            order['exit_price_price'] = False
            return False
        result = check_exit_price_by_step(condition, block, candle, order, prev_candle)
        if result:
            #print('-------------')
            #print('Сработала одна ступень пробоя')
            
            #print('proboi work ' + str(order['proboi']))
            #print('time ' + str(candle['time']))
            #if prev_candle != None:
            #    print('price ' + str(prev_candle[condition['name'] + '-' + condition['side']]))

            #print('proc == ' + str(result))
            #print('proboi stup == ' + str(order['proboi_step']))
            #print('oldproboi work ' + str(order['old_proboi']))

            order['proboi_status'] = 1
            order['proboi_line_proc'] = result

            order['proboi_step'] = order['proboi_step'] + 1
            if order['proboi_step'] + 1 == new_breakdown_sum and exit_price_price_main == 'yes':
                order['exit_price_price'] = True
            if order['proboi_step'] >= new_breakdown_sum:
                if check == 'low':
                    order['close_price_order'] = float(order['proboi']) - ((float(order['proboi']) / 100) * proc_value_2)
                if check == 'close':
                    order['close_price_order'] = float(candle['close'])
                if check == 'high':
                    order['close_price_order'] = float(order['proboi']) + ((float(order['proboi']) / 100) * proc_value_2)
                    
            return False

    return False


# ---------- engine -----------------

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

def block_conditions_done(block, candle, order, prev_candle):

    cur_condition_number = None

    for condition in block['conditions']:
        
        if condition.get('done') == None:
            condition['done'] = False

        if condition['done']:
            continue

        if cur_condition_number != None and condition['number'] != cur_condition_number:
            return False
        
        if condition['type'] == 'pnl':
            result = check_pnl(condition, block, candle, order)
            if result == False:
                condition['done'] = False
                return False
            else:
                order['close_time_order'] = candle['time']
                order['close_price_order'] = result
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
                order['close_price_order'] = result
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

        condition['done'] = True

        if order['condition_checked_candle'] == None:
            order['condition_checked_candle'] = candle
        
    return True

def execute_block_actions(block, candle):

    global order
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
            result = close_position(order, block, candle)
            if result:
                action['done'] = True
                print('Закрытие позиции: ' + str(order['close_time_position']))
                saved_close_time = order['close_time_order']
                saved_close_price = order['close_price_order']
                order = get_new_order()
                candle['was_close'] = True 
                continue
            else:
                action['done'] = False
                return False
        if action['order'] == "open":
            if order['state'] == 'start':
                order['order_type'] = action['order_type']
                order['direction'] = action['direction']
                if  action.get('leverage') != None:
                    order['leverage'] = action['leverage']
                if saved_close_time == 0:
                    order['open_time_order'] = candle['time']
                else:
                    order['open_time_order'] = saved_close_time
                if saved_close_price != 0:
                    order['open_price_order'] = saved_close_price
                    order['price'] = saved_close_price
                order['state'] = 'order_is_opened'
                #return False 
            if order['state'] == 'order_is_opened':
                result = open_position(order, block, candle)
                if result:
                    action['done'] = True
                    print('Открытие позиции: ' + str(order['open_time_position']))
                else:
                    action['done'] = False
                    return False
            else:
                return False

    return True

def get_new_order():

    order = {}
    order['leverage'] = 0

    order['open_price_order'] = 0
    order['close_price_order'] = 0

    order['open_time_order'] = 0
    order['open_time_position'] = 0
    order['close_time_position'] = 0
    order['close_time_order'] = 0

    order['leverage'] = 1
    order['price_indent'] = 0
    order['direction'] = ''
    order['order_type'] = ''
    order['state'] = 'start'
    order['path'] = ''
    order['lot'] = 0
    order['price'] = 0

    order['condition_checked_candle'] = None

    return order

def open_position(order, block, candle):

    if order['direction'] == 'long':
        if candle['low'] <= back_price_1[back_price_1.index(candle) - 1]['close'] and order['order_type'] == 'limit':
            order['open_time_position'] = back_price_1[back_price_1.index(candle) + 1]['time']
            price_old = back_price_1[back_price_1.index(candle) - 1]['close']
            if order['price_indent'] != 0:
                price = float(price_old) - (float(price_old) / 100) * float(order['price_indent'])
            else:
                price = float(price_old)
            if order['open_price_order'] == 0:
                order['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(order['leverage'])
            order['lot'] = int(round(lot, -1))
            if order['price'] == 0:
                order['price'] = price
            order['path'] = order['path'] + str(block['number']) + '_' + block['alg_number']
            return True
        if order['order_type'] == 'market':
            order['open_time_position'] = order['open_time_order']
            price_old = back_price_1[back_price_1.index(candle) - 1]['close']
            if order['price_indent'] != 0:
                price = float(price_old) - (float(price_old) / 100) * float(order['price_indent'])
            else:
                price = float(price_old)
            if order['open_price_order'] == 0:
                order['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(order['leverage'])
            lot = int(round(lot, -1))
            order['lot'] = int(round(lot, -1))
            if order['price'] == 0:
                order['price'] = price
            order['path'] = order['path'] + str(block['number']) + '_' + block['alg_number']
            return True
    if order['direction'] == 'short':
        if candle['high'] >= back_price_1[back_price_1.index(candle) - 1]['close'] and order['order_type'] == 'limit':
            order['open_time_position'] = back_price_1[back_price_1.index(candle) + 1]['time']
            price_old = back_price_1[back_price_1.index(candle) - 1]['close']
            if order['price_indent'] != 0:
                price = float(price_old) + (float(price_old) / 100) * float(order['price_indent'])
            else:
                price = float(price_old)
            if order['open_price_order'] == 0:
                order['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(order['leverage'])
            lot = int(round(lot, -1))
            order['lot'] = int(round(lot, -1))
            if order['price'] == 0:
                order['price'] = price
            order['path'] = order['path'] + str(block['number']) + '_' + block['alg_number']
            return True
        if order['order_type'] == 'market':
            order['open_time_position'] = order['open_time_order']
            price_old = back_price_1[back_price_1.index(candle) - 1]['close']
            if order['price_indent'] != 0:
                price = float(price_old) + (float(price_old) / 100) * float(order['price_indent'])
            else:
                price = float(price_old)
            if order['open_price_order'] == 0:
                order['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(order['leverage'])
            lot = int(round(lot, -1))
            order['lot'] = int(round(lot, -1))
            if order['price'] == 0:
                order['price'] = price
            order['path'] = order['path'] + str(block['number']) + '_' + block['alg_number']
            return True
    return False

def close_position(order, block, candle):
    
    global profit_sum
    global loss_sum 
    global squeeze
    global equity
    global id_day
    global money_day
    global ids
    
    if ((candle['low'] <= back_price_1[back_price_1.index(candle) - 1]['close'] and order['order_type'] == 'limit' and order['direction'] == 'long' or order['direction'] == 'long' and order['order_type'] == 'market') or
        (candle['high'] >= back_price_1[back_price_1.index(candle) - 1]['close'] and order['order_type'] == 'limit' and order['direction'] == 'short' or order['direction'] == 'short' and order['order_type'] == 'market')):
        
        # если уже было закрытие в данной свече
        if candle.get('was_close') != None and candle['was_close'] == True:
            order['close_time_order'] = 0
            return False

        order['path'] = order['path'] + ', ' + str(block['number']) + '_' + block['alg_number']

        if order['close_price_order'] == 0:
            if order['condition_checked_candle'] == None:
                order['close_price_order'] = float(candle['close'])
            else:
                order['close_price_order'] = float(order['condition_checked_candle']['close'])
        if order['direction'] == 'long':
            if order['close_price_order'] >= order['price']:
                res = 'profit'
                profit_sum = profit_sum + 1
            else:
                res = 'loss'
                loss_sum = loss_sum + 1
            if order['order_type'] == 'limit':
                points_deal = order['close_price_order'] - order['price']
            else:
                points_deal = order['close_price_order'] - order['price'] - squeeze
                squeeze = squeeze + squeeze
        else:
            if order['price'] >= order['close_price_order']:
                res = 'profit'
                profit_sum = profit_sum + 1
            else:
                res = 'loss'
                loss_sum = loss_sum + 1
            if order['order_type'] == 'limit':
                points_deal = order['price'] - order['close_price_order']
            else:
                points_deal = order['price'] - order['close_price_order'] - squeeze
                squeeze = squeeze + squeeze
        fee = 0
        money_deal = (points_deal / order['close_price_order']) * (order['lot'] / order['price']) - fee
        money_day = money_day + money_deal
        percent_deal = (money_deal / equity) * 100
        equity = equity + money_deal
        percent_day = (money_day / start_balance) * 100
        min_percent_list.append(percent_day)
        min_balance_percent = min(min_percent_list)
        if order['order_type'] == 'limit':
            order['close_time_position'] = back_price_1[back_price_1.index(candle)+1]['time']
        else:
            order['close_time_position'] = order['close_time_order']
        if str(order['open_time_position']).split(' ')[0].split('-')[2] != str(order['close_time_position']).split(' ')[0].split('-')[2]:
            id_day = id_day - 1
        insert_stmt = (
            "INSERT INTO {0}(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, equity, money_day, percent_day, minimum_equity_percent, minimum_losses_percent, price_deviation, blocks_id)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_result)
        )
        data = (
            id_day, order['direction'], order['lot'], order['order_type'], order['open_time_order'], order['open_price_order'], order['open_time_position'], order['order_type'],
            order['close_time_order'], order['close_price_order'], order['close_time_position'], fee, res, points_deal, money_deal, percent_deal,
            equity, money_day, percent_day, min_balance_percent, 0, 0, order['path'])
        try:
            cursor.execute(insert_stmt, data)
            cnx.commit()
        except Exception as e:
            print(e)

        if str(order['open_time_position']).split(' ')[0].split('-')[2] != str(order['close_time_position']).split(' ')[0].split('-')[2]:
            id_day = id_day + 1
        reset_variables()
        ids = ids + 1
        return True

    return False

# ---------- main programm -----------------

activation_blocks = get_activation_blocks('0', blocks_data, block_order)
if len(activation_blocks) == 0:
    raise Exception('There is no first block in startegy')

strategy_state = 'check_blocks_conditions'
action_block = None
order = get_new_order()
prev_candle = None

for candle in back_price_1:
    
    # настройка первого дня
    if back_price_1.index(candle) == 0:
        id_day = 1
        day = str(candle['time']).split(' ')[0].split('-')[2]
        ids = 1
    else:
        ids = ids + 1
        # настройка дня

    if str(candle['time']).split(' ')[0].split('-')[2] > day:
        day = str(candle['time']).split(' ')[0].split('-')[2]
        money_result = money_result + money_day
        money_day = 0
        percent_day = 0
        min_percent_list.clear()
        min_balance_percent = 0


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
            result = execute_block_actions(action_block, candle)
            if result == True:
                activation_blocks = get_activation_blocks(action_block, blocks_data, block_order)
                strategy_state = 'check_blocks_conditions'
            else:
                break
    
    prev_candle = candle 

 
profitability = (money_result/start_balance) - 1
all_orders = profit_sum + loss_sum

if all_orders > 0:

    profit_percent = profit_sum/(all_orders/100)
    loss_percent = loss_sum/(all_orders/100)
    insert_stmt = ("INSERT INTO {0}(profitability, money_result, profit_percent, profit_average_points, profit_sum, loss_percent, loss_average_points, loss_sum)"
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(table_result_sum))

    data = (profitability, money_result, profit_percent, 0, profit_sum, loss_percent, 0, loss_sum)
    cursor.execute(insert_stmt, data)

cnx.commit()
cnx.close()