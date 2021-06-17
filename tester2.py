import mysql.connector
import json
import datetime
import os
import sys
import ast

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
table2 = data['table_result']
try:
    cursor.execute("TRUNCATE TABLE back_positions")
    cursor.execute("TRUNCATE TABLE back_positions_sum")
except Exception as e:
    print('Ошибка получения таблицы с результами, причина: ')
    print(e)

try:
    cursor.execute('SELECT * FROM front_algorithms')
except Exception as e:
    print('Ошибка получения таблицы с настройками, причина: ')
    print(e)
rows1 = cursor.fetchall()

algo = []
for row in rows1:
    uslovie = row[7]
    algo.append(uslovie)


squeeze = 0  # 30
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
    rows2.append([ast.literal_eval(gg[8]), ast.literal_eval(gg[10])])
    block_order[str(gg[0])] = iter
    iter = iter + 1
rows1 = rows2

# обнуление глобальных переменных
def reset_variables():

    global points_deal
    global res
    global fee
    global money_deal

    points_deal = 0
    res = ''
    fee = 0
    money_deal = 0

def check_indicator(condition, block, candle):

    indicator = candle[condition['name']]
    try:
        last_ind = back_price_1[back_price_1.index(candle) - 1][condition['name']]
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
            if '0' in block[7].split(','):
                block_data = {}
                block_data['direction'] = 'long'
                block_data['conditions'] = ast.literal_eval(block[8])['conditions']
                block_data['actions'] = ast.literal_eval(block[8])['actions']
                block_data['number'] = block[0]
                block_data['activations'] = block[7]
                blocks.append(block_data)
                return blocks
            elif '0' in block[9].split(','):
                block_data = {}
                block_data['direction'] = 'short'
                block_data['conditions'] = ast.literal_eval(block[10])['conditions']
                block_data['actions'] = ast.literal_eval(block[10])['actions']
                block_data['number'] = block[0]
                block_data['activations'] = block[9]
                blocks.append(block_data)
                return blocks
    else:
        for activation_block in activation_blocks: 

            index = block_order[activation_block['id']]

            if activation_block['direction'] == 'long':
                block_data = {}
                block_data['direction'] = 'long'
                block_data['conditions'] = ast.literal_eval(blocks_data[index][8])['conditions']
                block_data['actions'] = ast.literal_eval(blocks_data[index][8])['actions']
                block_data['number'] = blocks_data[index][0]
                block_data['activations'] = blocks_data[index][7]
                blocks.append(block_data)
            else:
                block_data = {}
                block_data['direction'] = 'short'
                block_data['conditions'] = ast.literal_eval(blocks_data[index][10])['conditions']
                block_data['actions'] = ast.literal_eval(blocks_data[index][10])['actions']
                block_data['number'] = blocks_data[index][0]
                block_data['activations'] = blocks_data[index][9]
                blocks.append(block_data)
    
    return blocks

def block_conditions_done(block, candle, order):

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
        elif condition['type'] == 'indicator':
            result = check_indicator(condition, block, candle)
            if result == False:
                condition['done'] = False
                return False
        else:
            condition['done'] = False
            return False

        cur_condition_number = condition['number']

        condition['done'] = True
        
    return True

def check_blocks_condition(blocks, candle, order):

    for block in blocks:
        if block_conditions_done(block, candle, order):
            return block
    
    return None

def check_pnl(condition, block, candle, order):
    
    direction = block['direction']

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

    return order

def open_position(order, block, candle):

    if order['direction'] == 'long':
        if candle['low'] <= back_price_1[back_price_1.index(candle) - 1]['close'] and order['order_type'] == 'limit':
            order['open_time_position'] = back_price_1[back_price_1.index(candle) + 1]['time']
            price_old = back_price_1[back_price_1.index(candle) - 1]['close']
            if order['price_indent'] != 0:
                price = float(price_old) - (float(price_old) / 100) * float(order['price_indent'])
            else:
                price = float(price_old) - (float(price_old) / 100)
            if order['open_price_order'] == 0:
                order['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(order['leverage'])
            order['lot'] = int(round(lot, -1))
            order['price'] = price
            order['path'] = order['path'] + str(block['number']) + '_' + order['direction']
            return True
        if order['order_type'] == 'market':
            order['open_time_position'] = order['open_time_order']
            price_old = back_price_1[back_price_1.index(candle) - 1]['close']
            if order['price_indent'] != 0:
                price = float(price_old) - (float(price_old) / 100) * float(order['price_indent'])
            else:
                price = float(price_old)
            order['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(order['leverage'])
            lot = int(round(lot, -1))
            order['lot'] = int(round(lot, -1))
            order['price'] = price
            order['path'] =order['path'] + str(block['number']) + '_' + order['direction']
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
            order['price'] = price
            order['path'] = order['path'] + str(block['number']) + '_' + order['direction']
            return True
        if order['order_type'] == 'market':
            order['open_time_position'] = order['open_time_order']
            price_old = back_price_1[back_price_1.index(candle) - 1]['close']
            if order['price_indent'] != 0:
                price = float(price_old) + (float(price_old) / 100) * float(order['price_indent'])
            else:
                price = float(price_old)
            order['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(order['leverage'])
            lot = int(round(lot, -1))
            order['lot'] = int(round(lot, -1))
            order['price'] = price
            order['path'] = order['path'] + str(block['number']) + '_' + order['direction']
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
        
        order['path'] = order['path'] + ', ' + str(block['number']) + '_' + order['direction']

        if order['close_price_order'] == 0:
            order['close_price_order'] = float(candle['close'])
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
            "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, equity, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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

def execute_block_actions(block, candle):

    global order

    for action in block['actions']:

        if action.get('done') and action['done'] == True:
            continue

        if action['order'] == "close":
            if order['close_time_order'] == 0:
                order['close_time_order'] = back_price_1[back_price_1.index(candle) + 1]['time']
            result = close_position(order, block, candle)
            if result:
                action['done'] = True
                print('Закрытие позиции')
                order = get_new_order()
            else:
                action['done'] = False
                return False
        if action['order'] == "open":
            if order['state'] == 'start':
                order['order_type'] = action['order_type']
                order['direction'] = action['direction']
                order['leverage'] = action['leverage']
                order['open_time_order'] = back_price_1[back_price_1.index(candle) + 1]['time']
                order['state'] = 'order_is_opened'
                return False 
            elif order['state'] == 'order_is_opened':
                result = open_position(order, block, candle)
                if result:
                    action['done'] = True
                    print('Открытие позиции')
                else:
                    action['done'] = False
                    return False
            else:
                return False

    return True

# ---------- main programm -----------------

activation_blocks = get_activation_blocks('0', blocks_data, block_order)
if len(activation_blocks) == 0:
    raise Exception('There is no first block in startegy')

strategy_state = 'check_blocks_conditions'
action_block = None
order = get_new_order()

for cc in back_price_1:
    
    # настройка первого дня
    if back_price_1.index(cc) == 0:
        id_day = 1
        day = str(cc['time']).split(' ')[0].split('-')[2]
        ids = 1
    else:
        ids = ids + 1
        # настройка дня

    if str(cc['time']).split(' ')[0].split('-')[2] > day:
        day = str(cc['time']).split(' ')[0].split('-')[2]
        money_result = money_result + money_day
        money_day = 0
        percent_day = 0
        min_percent_list.clear()
        min_balance_percent = 0

    # проверка условий активных блоков
    if strategy_state == 'check_blocks_conditions':
        action_block = check_blocks_condition(activation_blocks, cc, order)
        if action_block != None:
            strategy_state = 'execute_block_actions'
    
    # исполнение действий блока
    if strategy_state == 'execute_block_actions':
        result = execute_block_actions(action_block, cc)
        if result == True:
            activation_blocks = get_activation_blocks(action_block, blocks_data, block_order)
            strategy_state = 'check_blocks_conditions'
 
profitability = (money_result/start_balance) - 1
all_orders = profit_sum + loss_sum

if all_orders > 0:

    profit_percent = profit_sum/(all_orders/100)
    loss_percent = loss_sum/(all_orders/100)
    insert_stmt = ("INSERT INTO back_positions_sum(profitability, money_result, profit_percent, profit_sum, loss_percent, loss_sum)"
    "VALUES (%s, %s, %s, %s, %s, %s)")

    data = (profitability, money_result, profit_percent, profit_sum, loss_percent, loss_sum)
    cursor.execute(insert_stmt, data)

cnx.commit()
cnx.close()