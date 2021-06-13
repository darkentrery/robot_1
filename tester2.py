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
order = []
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
for gg in rows1:
    rows2.append([ast.literal_eval(gg[8]), ast.literal_eval(gg[10])])
    activations.append([gg[7], gg[9]])
    block_order[str(gg[0])] = iter
    iter = iter + 1
rows1 = rows2

# обнуление глобальных переменных
def reset_variables():

    global order
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

    order = []
    close_candle = 0
    open_time_order = 0
    open_time_position = 0
    #open_price_order = 0
    open_price_position = 0
    close_time_order = 0
    #close_time_position = 0
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

#проверка pnl
def check_pnl(open_price_order, direction, db_position, price_close1, price_close2, leverage):
    
    ind_oper = db_position['position_condition']['pnl'].split(' ')[0]
    ind_value = float(db_position['position_condition']['pnl'].split(' ')[1])
    if direction == 'short':
        pnl = open_price_order - (((open_price_order / 100) * ind_value))/float(leverage)
    else:
        pnl = open_price_order + (((open_price_order / 100) * ind_value))/float(leverage)

    if direction == 'long':
        if ind_oper== '>=':
            if price_close1 >= pnl:
                return pnl
        if ind_oper == '<=':
            if price_close1 <= pnl:
                return pnl
        if ind_oper == '=':
            if price_close1 == pnl:
                return pnl
        if ind_oper == '>':
            if price_close1 > pnl:
                return pnl
        if ind_oper == '<':
            if price_close1 < pnl:
                return pnl
        if ind_oper == '>=':
            if price_close2 >= pnl:
                return pnl
        if ind_oper == '<=':
            if price_close2 <= pnl:
                return pnl
        if ind_oper == '=':
            if price_close2 == pnl:
                return pnl
        if ind_oper == '>':
            if price_close2 > pnl:
                return pnl
        if ind_oper == '<':
            if price_close2 < pnl:
                return pnl
    else:
        if ind_oper == '>=':
            if pnl >= price_close1:
                return pnl
        if ind_oper == '<=':
            if pnl <= price_close1:
                return pnl
        if ind_oper == '=':
            if pnl == price_close1:
                return pnl
        if ind_oper == '>':
            if pnl > price_close1:
                return pnl
        if ind_oper == '<':
            if pnl < price_close1:
                return pnl
        if ind_oper == '>=':
            if pnl >= price_close2:
                return pnl
        if ind_oper == '<=':
            if pnl <= price_close2:
                return pnl
        if ind_oper == '=':
            if pnl == price_close2:
                return pnl
        if ind_oper == '>':
            if pnl > price_close2:
                return pnl
        if ind_oper == '<':
            if pnl < price_close2:
                return pnl
    return False

# проверка идентификатора1 в блоке условий
def check_indicator(indicator_condition, direction, last_ind, db_position):

    indicator = cc[indicator_condition['name']]
    try:
        last_ind = back_price_1[back_price_1.index(cc) - 1][indicator_condition['name']]
    except:
        return False 

    ind_oper = indicator_condition['value'].split(' ')[0]
    ind_value = float(indicator_condition['value'].split(' ')[1])
    change = indicator_condition['change']

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

# открытие ордера
def open_order():

    # глобальные переменные
    global stat
    global indicator1
    global direction
    global open_time_order
    global order_type
    global change
    global open_price_order
    global close_time_position

    if stat != '0':
        return False
    if '0' in activations[block_num][0].split(','):

        if rows1[block_num][0].get('indicators'):
            indicators = rows1[block_num][0]['indicators']
        else:
            indicators = None

        direction = 'long'
        order_type = rows1[block_num][0]['position_action']['order_type']
        if check_block_condition(cc, indicators, direction, rows1[block_num], cc['high'], cc['low']):
            open_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
            return True
    if '0' in activations[block_num][1].split(','):

        if rows1[block_num][1].get('indicators'):
            indicators = rows1[block_num][1]['indicators']
        else:
            indicators = None

        direction = 'short'
        order_type = rows1[block_num][1]['position_action']['order_type']
        if check_block_condition(cc, indicators, direction, rows1[block_num], cc['high'], cc['low']):
            open_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
            return True

    return False

# открытие позиции
def open_position(block_number):
    global direction
    global order_type
    global open_price_order
    global stat
    global order
    global lot
    global block_id
    global price
    global lev
    global leverage
    global open_time_position
    global open_time_order

    if direction == 'long':
        if cc['low'] <= back_price_1[back_price_1.index(cc) - 1]['close'] and order_type == 'limit':
            open_time_position = back_price_1[back_price_1.index(
                cc) + 1]['time']
            # side_1 = rows1[0][0]['position_action']['direction']
            price_old = back_price_1[back_price_1.index(cc) - 1]['close']
            try:
                price_indent = rows1[block_number][0]['position_action']['price_indent']
                price = float(price_old) - (float(price_old) /
                              100) * float(price_indent)
            except:
                price = float(price_old) - (float(price_old) / 100)
            if open_price_order == 0:
                open_price_order = price
            leverage = rows1[block_number][0]['position_action']['leverage']
            lot = (float(start_balance) * float(price)) * float(leverage)
            lot = int(round(lot, -1))
            direction = rows1[block_number][0]['position_action']['direction']
            order_type = rows1[block_number][0]['position_action']['order_type']
            # order_type_1 = order_type
            order.append(direction)
            order.append(price)
            order.append(lot)
            order.append(order_type)
            order.append(cc['time'])
            stat = 'position_' + rows1[block_number][0]['position_action']['order']
            block_id = str(block_number + 1) + '_' + direction
            return True
        if order_type == 'market':
            open_time_position = open_time_order
            price_old = back_price_1[back_price_1.index(cc) - 1]['close']
            try:
                price_indent = rows1[block_number][0]['position_action']['price_indent']
                price = float(price_old) - (float(price_old) /
                              100) * float(price_indent)
            except:
                price = float(price_old)
            open_price_order = price
            leverage = rows1[block_number][0]['position_action']['leverage']
            lot = (float(start_balance) * float(price)) * float(leverage)
            lot = int(round(lot, -1))
            direction = rows1[block_number][0]['position_action']['direction']
            order_type = rows1[block_number][0]['position_action']['order_type']
            order.append(direction)
            order.append(price)
            order.append(lot)
            order.append(order_type)
            order.append(cc['time'])
            stat = 'position_' + rows1[block_number][0]['position_action']['order']
            block_id = str(block_number + 1) + '_' + direction
            return True
    if direction == 'short':
        if cc['high'] >= back_price_1[back_price_1.index(cc) - 1]['close'] and order_type == 'limit':
            open_time_position = back_price_1[back_price_1.index(
                cc) + 1]['time']
            price_old = back_price_1[back_price_1.index(cc) - 1]['close']
            try:
                price_indent = rows1[block_number][1]['position_action']['price_indent']
                price = float(price_old) + (float(price_old) /
                              100) * float(price_indent)
            except:
                price = float(price_old)
            if open_price_order == 0:
                open_price_order = price
            try:
                leverage = rows1[block_number][1]['position_action']['leverage']
            except:
                leverage = 1
            lot = (float(start_balance) * float(price)) * float(leverage)
            lot = int(round(lot, -1))
            direction = rows1[block_number][1]['position_action']['direction']
            order_type = rows1[block_number][1]['position_action']['order_type']
            order.append(direction)
            order.append(price)
            order.append(lot)
            order.append(order_type)
            order.append(cc['time'])
            stat = 'position_' + rows1[block_number][1]['position_action']['order']
            block_id = str(block_number + 1) + '_' + direction
            return True
        if order_type == 'market':
            open_time_position = open_time_order
            price_old = back_price_1[back_price_1.index(cc) - 1]['close']
            try:
                price_indent = rows1[block_number][1]['position_action']['price_indent']
                price = float(price_old) + (float(price_old) /
                              100) * float(price_indent)
            except:
                price = float(price_old) + (float(price_old) / 100)
            open_price_order = price
            try:
                leverage = rows1[block_number][1]['position_action']['leverage']
            except:
                leverage = 1
            lot = (float(start_balance) * float(price)) * float(leverage)
            lot = int(round(lot, -1))
            direction = rows1[block_number][1]['position_action']['direction']
            order_type = rows1[block_number][1]['position_action']['order_type']
            order.append(direction)
            order.append(price)
            order.append(lot)
            order.append(order_type)
            order.append(cc['time'])
            stat = 'position_' + rows1[block_number][1]['position_action']['order']
            block_id = str(block_number + 1) + '_' + direction
            return True
    return False

# попытка смены блока
def change_block_num(block_number):
    
    global direction
    global block_id
    global block_num
    global stat
    global order_type_2
    global close_time_order
    global price_close1
    global price_close2

    if direction == 'long':
        activation_blocks = activations[block_number][0].split(',')
    else:
        activation_blocks = activations[block_number][1].split(',')
    for ac_block in activation_blocks:
        ac_block_parameters = ac_block.split('_')
        if (len(ac_block_parameters)) < 2:
            continue
        ac_block_num = block_order[ac_block_parameters[0]]
        ac_block_direction = ac_block_parameters[1]
        if ac_block_direction == 'long' and direction == 'long':
            if rows1[ac_block_num][0].get('indicators'):
                indicators = rows1[ac_block_num][0]['indicators']
            else:
                indicators = None
            
            order_type_2 = rows1[ac_block_num][0]['position_action']['order_type']
            next_order = rows1[ac_block_num][0]['position_action']['order']
        elif ac_block_direction == 'short' and direction == 'short':
            if rows1[ac_block_num][1].get('indicators'):
                indicators = rows1[ac_block_num][1]['indicators']
            else:
                indicators = None
            order_type_2 = rows1[ac_block_num][1]['position_action']['order_type']
            next_order = rows1[ac_block_num][1]['position_action']['order']
        if check_block_condition(cc, indicators, direction, rows1[ac_block_num], cc['high'], cc['low']): 
            block_num = ac_block_num
            stat = 'position_' + next_order
            if close_time_order == 0:
                close_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
            block_id = block_id + ',' + ac_block_parameters[0] + '_' + direction
            break

# закрытие позиции
def close_position():
    
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
    global close_candle
    
    if cc['low'] <= back_price_1[back_price_1.index(cc) - 1]['close'] and order_type == 'limit' and direction == 'long' or direction == 'long' and order_type == 'market':
        if close_candle == 0:
            close_candle = float(cc['close'])
        close_price_order = close_candle
        order_type_1 = order[3]
        direction = order[0]
        if direction == 'long':
            if close_candle >= order[1]:
                res = 'profit'
                profit_sum = profit_sum + 1
            else:
                res = 'loss'
                loss_sum = loss_sum + 1
            if order_type_1 == 'limit':
                points_deal = close_candle - order[1]
            else:
                points_deal = close_candle - order[1] - squeeze
                squeeze = squeeze + squeeze
        else:
            if order[1] >= close_candle:
                res = 'profit'
                profit_sum = profit_sum + 1
            else:
                res = 'loss'
                loss_sum = loss_sum + 1
            if order_type_1 == 'limit':
                points_deal = order[1] - close_candle
            else:
                points_deal = order[1] - close_candle - squeeze
                squeeze = squeeze + squeeze
        fee = 0
        money_deal = (points_deal / close_candle) * (lot / price) - fee
        money_day = money_day + money_deal
        percent_deal = (money_deal / equity) * 100
        equity = equity + money_deal
        percent_day = (money_day / start_balance) * 100
        min_percent_list.append(percent_day)
        min_balance_percent = min(min_percent_list)
        if order_type == 'limit':
            close_time_position = back_price_1[back_price_1.index(
                cc)+1]['time']
        else:
            close_time_position = close_time_order
        if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[2]:
            id_day = id_day - 1
        insert_stmt = (
            "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, equity, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        data = (
            id_day, order[0], lot, order_type_1, open_time_order, open_price_order, open_time_position, order_type_2,
            close_time_order, close_candle, close_time_position, fee, res, points_deal, money_deal, percent_deal,
            equity, money_day, percent_day, min_balance_percent, 0, 0, block_id)
        try:
            cursor.execute(insert_stmt, data)
            cnx.commit()
        except Exception as e:
            print(e)

        if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[2]:
            id_day = id_day + 1
        reset_variables()
        ids = ids + 1
        return True

    if cc['high'] >= back_price_1[back_price_1.index(cc) - 1]['close'] and order_type == 'limit' and direction == 'short' or direction == 'short' and order_type == 'market':
        if close_candle == 0:
            close_candle = float(cc['close'])
        close_price_order = close_candle
        order_type_1 = order[3]
        direction = order[0]
        if direction == 'long':
            if close_candle >= order[1]:
                res = 'profit'
                profit_sum = profit_sum + 1
            else:
                res = 'loss'
                loss_sum = loss_sum + 1
            if order_type_1 == 'limit':
                points_deal = close_candle - order[1]
            else:
                points_deal = close_candle - order[1] - squeeze
                squeeze = squeeze + squeeze
        else:
            if order[1] >= close_candle:
                res = 'profit'
                profit_sum = profit_sum + 1
            else:
                res = 'loss'
                loss_sum = loss_sum + 1
            if order_type_1 == 'limit':
                points_deal = order[1] - close_candle
            else:
                points_deal = order[1] - close_candle - squeeze
                squeeze = squeeze + squeeze
        fee = 0
        money_deal = (points_deal / close_candle) * (lot / price) - fee
        money_day = money_day + money_deal
        percent_deal = (money_deal / equity) * 100
        equity = equity + money_deal
        percent_day = (money_day / start_balance) * 100
        min_percent_list.append(percent_day)
        min_balance_percent = min(min_percent_list)
        if order_type == 'limit':
            close_time_position = back_price_1[back_price_1.index(cc)+1]['time']
        else:
            close_time_position = close_time_order
        if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[2]:
            id_day = id_day - 1
        insert_stmt = (
            "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, equity, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        data = (
            id_day, order[0], lot, order_type_1, open_time_order, open_price_order, open_time_position, order_type_2,
            close_time_order, close_candle, close_time_position, fee, res, points_deal, money_deal, percent_deal,
            equity, money_day, percent_day, min_balance_percent, 0, 0, block_id)
        cursor.execute(insert_stmt, data)
        cnx.commit()
        if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[2]:
            id_day = id_day + 1
        reset_variables()
        ids = ids + 1
        return True
    return False

# обход по свечам
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

    if stat == 'position_open':
        change_block_num(block_num)
    if stat == 'position_close' and close_position():
        print('Закрытие позиции')
    if open_order():
        print('Открытие ордера')
        stat = 'order_is_opened'
        continue
    if stat == 'order_is_opened' and open_position(block_num):
        print('Открытие позиции')
        continue
      
 
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