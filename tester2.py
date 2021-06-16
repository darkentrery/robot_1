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
# prices = []
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

# переменные и списки
stat = '0'
price = ''
lot = ''
results = []
fee_limit = 0
fee_market = 0  # 0.045
squeeze = 0  # 30
ids = 0
money_deal = 0
percent_deal = 0
close_candle = 0
open_time_order = 0
open_time_position = 0
open_price_position = 0
close_time_order = 0
close_time_position = 0
equity = 1
start_balance = 1
money_day = 0
percent_day = 0
min_percent_list = []
min_balance_percent = 0
rows2 = []
block_id = ''
probitability = 0
money_result = 0
profit_percent = 0
profit_sum = 0
loss_percent = 0
loss_sum = 0
activations = []
block_num = 0
direction = ''
order_type = ''
order_type_1 = 0
order_type_2 = 0
open_price_order = 0
close_price_order = 0
block_order = {}

iter = 0
blocks_data = rows1
for gg in rows1:
    rows2.append([ast.literal_eval(gg[8]), ast.literal_eval(gg[10])])
    activations.append([gg[7], gg[9]])
    block_order[str(gg[0])] = iter
    iter = iter + 1
rows1 = rows2

# обнуление глобальных переменных
def reset_variables():

    global close_candle
    global open_time_order
    global open_time_position
    global open_price_order
    global open_price_position
    global close_time_order
    global close_time_position
    global points_deal
    global res
    global fee
    global money_deal
    global stat
    global block_id
    global block_num

    close_candle = 0
    open_time_order = 0
    open_time_position = 0
    open_price_order = 0##
    open_price_position = 0
    close_time_order = 0
    close_time_position = 0##
    points_deal = 0
    res = ''
    fee = 0
    money_deal = 0
    stat = '0'
    block_id = ''
    stat = '0'
    block_num = 0

# проверка условий блока
def check_block_condition(candle, indicators, direction, db_position, price_close1, price_close2):
    
    global close_candle
    global close_time_order

    if direction == 'long':
        if indicators != None:
            for indicator in indicators:
                last_ind = back_price_1[back_price_1.index(candle) - 1][indicator['name']]
                result_ind = check_indicator(indicator, direction, last_ind, db_position[0])
                if result_ind == False: 
                    return False
            return True 
        elif db_position[0].get('position_condition') and db_position[0]['position_condition'].get('pnl'):    
            pnl = check_pnl(open_price_order, direction, db_position[0], price_close1, price_close2, leverage)
            if pnl:
                close_time_order = candle['time']
                close_candle = pnl
                return True
            else:
                return False
    else:
        if indicators != None:
            for indicator in indicators:
                last_ind = back_price_1[back_price_1.index(candle) - 1][indicator['name']]
                result_ind = check_indicator(indicator, direction, last_ind, db_position[1])
                if result_ind == False: 
                    return False
            return True
        elif db_position[1].get('position_condition') and db_position[1]['position_condition'].get('pnl'):    
            pnl = check_pnl(open_price_order, direction, db_position[1], price_close1, price_close2, leverage)
            if pnl:
                close_time_order = candle['time']
                close_candle = pnl
                return True
            else:
                return False

# закрытие позиции

#-------------------------------------------------------------------------------

def check_indicator(condition, block, candle):

    indicator = candle[condition['name']]
    try:
        last_ind = back_price_1[back_price_1.index(candle) - 1][condition['name']]
    except:
        return False 

    ind_oper = condition['value'].split(' ')[0]
    ind_value = float(condition['value'].split(' ')[1])
    change = condition['change']

    if change == 'more_than_previous':
        if indicator > last_ind:
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
    else:
        if indicator < last_ind:
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

def get_first_block(blocks):

    block_data = {}    

    for block in blocks:
        if '0' in block[7].split(','):
            block_data['direction'] = 'long'
            block_data['conditions'] = block[8]['conditions']
            block_data['actions'] = block[8]['actions']
            block_data['number'] = block[2]
            return block_data
        elif '0' in block[9].split(','):
            block_data['direction'] = 'short'
            block_data['conditions'] = block[10]['conditions']
            block_data['actions'] = block[10]['actions']
            block_data['number'] = block[2]
            return block_data
    
    return None

def check_blocks_condition(blocks, candle):

    for block in blocks:
        block_conditions_done = block_conditions_done(block, candle)
        if block_conditions_done:
            return block
    
    return None

def check_pnl(condition, block, candle):
    
    global open_price_order
    global leverage

    direction = block['direction']

    ind_oper = condition['pnl'].split(' ')[0]
    ind_value = float(condition['pnl'].split(' ')[1])
    if direction == 'short':
        pnl = open_price_order - (((open_price_order / 100) * ind_value))/float(leverage)
    else:
        pnl = open_price_order + (((open_price_order / 100) * ind_value))/float(leverage)

    if direction == 'long':
        if ind_oper== '>=':
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

def block_conditions_done(block, candle):

    cur_condition_number = None

    for condition in block['conditions']:
        
        if condition.get('done') and condition['done']:
            continue

        if cur_condition_number != None and condition['number'] <> cur_condition_number:
            return False

        
        if condition['type'] == 'pnl':
            result = check_pnl(condition, block, candle)
            if result == False:
                condition['done'] = False
                return False
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
        
def get_new_deal():

    deal = {}
    deal['leverage'] = 0
    deal['open_price_order'] = 0
    deal['open_price_position'] = 0
    deal['close_price_position'] = 0
    deal['close_price_order'] = 0
    deal['leverage'] = 1
    deal['price_indent'] = 0
    deal['direction'] = ''
    deal['order_type'] = ''
    deal['state'] = 'start'
    deal['path'] = ''
    deal['lot'] = 0
    deal['price'] = 0

    return deal

def open_position(deal, block, candle):

    global lot
    global price

    if deal['direction'] == 'long':
        if candle['low'] <= back_price_1[back_price_1.index(candle) - 1]['close'] and deal['order_type'] == 'limit':
            deal['open_time_position'] = back_price_1[back_price_1.index(candle) + 1]['time']
            price_old = back_price_1[back_price_1.index(candle) - 1]['close']
            if deal['price_indent'] != 0:
                price = float(price_old) - (float(price_old) / 100) * float(deal['price_indent'])
            else:
                price = float(price_old) - (float(price_old) / 100)
            if deal['open_price_order'] == 0:
                deal['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(deal['leverage'])
            deal['lot'] = int(round(lot, -1))
            deal['price'] = price
            deal['path'] = str(block['number']) + '_' + deal['direction']
            return True
        if deal['order_type'] == 'market':
            deal['open_time_position'] = open_time_order
            price_old = back_price_1[back_price_1.index(candle) - 1]['close']
            if deal['price_indent'] != 0:
                price = float(price_old) - (float(price_old) / 100) * float(deal['price_indent'])
            else:
                price = float(price_old)
            deal['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(deal['leverage'])
            lot = int(round(lot, -1))
            deal['lot'] = int(round(lot, -1))
            deal['price'] = price
            deal['path'] = str(block['number']) + '_' + deal['direction']
            return True
    if deal['direction'] == 'short':
        if candle['high'] >= back_price_1[back_price_1.index(candle) - 1]['close'] and deal['order_type'] == 'limit':
            deal['open_time_position'] = back_price_1[back_price_1.index(cc) + 1]['time']
            price_old = back_price_1[back_price_1.index(cc) - 1]['close']
            if deal['price_indent'] != 0:
                price = float(price_old) + (float(price_old) / 100) * float(deal['price_indent'])
            else:
                price = float(price_old)
            if deal['open_price_order'] == 0:
                deal['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(deal['leverage'])
            lot = int(round(lot, -1))
            deal['lot'] = int(round(lot, -1))
            deal['price'] = price
            deal['path'] = str(block['number']) + '_' + deal['direction']
            return True
        if deal['order_type'] == 'market':
            deal['open_time_position'] = open_time_order
            price_old = back_price_1[back_price_1.index(cc) - 1]['close']
            if deal['price_indent'] != 0:
                price = float(price_old) + (float(price_old) / 100) * float(deal['price_indent'])
            else:
                price = float(price_old)
            deal['open_price_order'] = price
            lot = (float(start_balance) * float(price)) * float(deal['leverage'])
            lot = int(round(lot, -1))
            deal['lot'] = int(round(lot, -1))
            deal['price'] = price
            deal['path'] = str(block['number']) + '_' + deal['direction']
            return True
    return False

def close_position(deal, block, candle):
    
    global direction
    global profit_sum
    global loss_sum 
    global squeeze
    global equity
    global id_day
    global order_type
    global money_day
    global ids
    global close_time_position
    global close_price_order
    
    if ((candle['low'] <= back_price_1[back_price_1.index(deal) - 1]['close'] and deal['order_type'] == 'limit' and deal['direction'] == 'long' or deal['direction'] == 'long' and deal['order_type'] == 'market') or
        (candle['high'] >= back_price_1[back_price_1.index(candle) - 1]['close'] and deal['order_type'] == 'limit' and deal['direction'] == 'short' or deal['direction'] == 'short' and deal['order_type'] == 'market')):
        
        if deal['close_price_order'] == 0:
            deal['close_price_order'] = float(candle['close'])
        if deal['direction'] == 'long':
            if deal['close_price_order'] >= deal['price']:
                res = 'profit'
                profit_sum = profit_sum + 1
            else:
                res = 'loss'
                loss_sum = loss_sum + 1
            if deal['order_type'] == 'limit':
                points_deal = deal['close_price_order'] - deal['price']
            else:
                points_deal = deal['close_price_order'] - deal['price'] - squeeze
                squeeze = squeeze + squeeze
        else:
            if deal['price'] >= deal['close_price_order']:
                res = 'profit'
                profit_sum = profit_sum + 1
            else:
                res = 'loss'
                loss_sum = loss_sum + 1
            if deal['order_type'] == 'limit':
                points_deal = deal['price'] - deal['close_price_order']
            else:
                points_deal = deal['price'] - deal['close_price_order'] - squeeze
                squeeze = squeeze + squeeze
        fee = 0
        money_deal = (points_deal / deal['close_price_order']) * (lot / price) - fee
        money_day = money_day + money_deal
        percent_deal = (money_deal / equity) * 100
        equity = equity + money_deal
        percent_day = (money_day / start_balance) * 100
        min_percent_list.append(percent_day)
        min_balance_percent = min(min_percent_list)
        if deal['order_type'] == 'limit':
            deal['close_time_position'] = back_price_1[back_price_1.index(candle)+1]['time']
        else:
            deal['close_time_position'] = deal['close_time_order']
        if str(deal['open_time_position']).split(' ')[0].split('-')[2] != str(deal['close_time_position']).split(' ')[0].split('-')[2]:
            id_day = id_day - 1
        insert_stmt = (
            "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, equity, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        data = (
            id_day, deal['direction'], deal['lot'], deal['order_type'], deal['open_time_order'], deal['open_price_order'], deal['open_time_position'], order_type_2,
            deal['close_time_order'], deal['close_price_order'], deal['close_time_position'], fee, res, points_deal, money_deal, percent_deal,
            equity, money_day, percent_day, min_balance_percent, 0, 0, deal['path'])
        try:
            cursor.execute(insert_stmt, data)
            cnx.commit()
        except Exception as e:
            print(e)

        if str(deal['open_time_position']).split(' ')[0].split('-')[2] != str(deal['close_time_position']).split(' ')[0].split('-')[2]:
            id_day = id_day + 1
        reset_variables()
        ids = ids + 1
        return True

    return False

def execute_block_actions(block, deal, candle):

    for action in block['actions']:

        if action.get('done') and action['done'] == True:
            continue

        if action['order'] == "close":
            result = close_position(deal, block, candle)
            if result:
                action['done'] == True
                print('Закрытие позиции')
                deal = get_new_deal()
            else:
                action['done'] == False
                return False
        if action['order'] == "open":
            if deal['state'] == 'start':
                deal['order_type'] = action['order_type']
                deal['direction'] = action['direction']
                deal['leverage'] = action['leverage']
                deal['open_time_order'] = back_price_1[back_price_1.index(candle) + 1]['time']
                deal['state'] = 'order_is_opened'
                return 
            if deal['state'] == 'order_is_opened':
                result = open_position(deal, block, candle)
                if result:
                    action['done'] == True
                    print('Открытие позиции')
                else:
                    action['done'] == False
                    return False
            return False
            
    return True

active_blocks = get_first_block(blocks_data)
if active_blocks == None:
    raise Exception('There is no first block in startegy')
else:
    active_blocks = [active_blocks] 
strategy_state = 'check_blocks_conditions'
action_block = None
deal = get_new_deal()

# обход по свечам
for cc in back_price_1:
    
    # проверка условий активных блоков
    if strategy_state == 'check_blocks_conditions':
        action_block = check_blocks_condition(active_blocks, cc)
        if action_block != None:
            strategy_state = 'execute_block_actions'
    # исполнение действий блока
    if strategy_state == 'execute_block_actions':
        result = execute_block_actions(action_block, deal, cc)
        if result == True:
            
        

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
      
 
# рассчет результата
profitability = (money_result/start_balance) - 1
all_orders = profit_sum + loss_sum
profit_percent = profit_sum/(all_orders/100)
loss_percent = loss_sum/(all_orders/100)
insert_stmt = ("INSERT INTO back_positions_sum(profitability, money_result, profit_percent, profit_sum, loss_percent, loss_sum)"
"VALUES (%s, %s, %s, %s, %s, %s)")

data = (profitability, money_result, profit_percent, profit_sum, loss_percent, loss_sum)
cursor.execute(insert_stmt, data)
cnx.commit()
cnx.close()