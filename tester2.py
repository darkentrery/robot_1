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
#prices = []
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
fee_market = 0#0.045
squeeze = 0#30
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
balance = 1
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
for gg in rows1:
    rows2.append([ast.literal_eval(gg[8]), ast.literal_eval(gg[10])])
    activations.append([gg[7], gg[9]])
rows1 = rows2


# блоки функции
def block_1(indicator, direction, change, last_ind):
    if direction == 'long':
        ind_oper_1 = rows1[0][0]['indicator_1']['value'].split(' ')[0]
        ind_value_1 = float(rows1[0][0]['indicator_1']['value'].split(' ')[1])
        side_1 = rows1[0][0]['position_action']['direction']
    else:
        ind_oper_1 = rows1[0][1]['indicator_1']['value'].split(' ')[0]
        ind_value_1 = float(rows1[0][1]['indicator_1']['value'].split(' ')[1])
        side_1 = rows1[0][1]['position_action']['direction']
    if change == 'more_than_previous':
        if indicator > last_ind:
            if ind_oper_1 == '>=':
                if indicator >= ind_value_1:
                    return True
            if ind_oper_1 == '<=':
                if indicator <= ind_value_1:
                    return True
            if ind_oper_1 == '<':
                if indicator < ind_value_1:
                    return True
            if ind_oper_1 == '>':
                if indicator > ind_value_1:
                    return True
            if ind_oper_1 == '=':
                if indicator == ind_value_1:
                    return True
    else:
        if indicator < last_ind:
            if ind_oper_1 == '>=':
                if indicator >= ind_value_1:
                    return True
            if ind_oper_1 == '<=':
                if indicator <= ind_value_1:
                    return True
            if ind_oper_1 == '<':
                if indicator < ind_value_1:
                    return True
            if ind_oper_1 == '>':
                if indicator > ind_value_1:
                    return True
            if ind_oper_1 == '=':
                if indicator == ind_value_1:
                    return True
    return False

def block_2(indicator, direction, change, last_ind):
    if direction == 'long':
        ind_oper_2 = rows1[1][0]['indicator_1']['value'].split(' ')[0]
        ind_value_2 = float(rows1[1][0]['indicator_1']['value'].split(' ')[1])
    else:
        ind_oper_2 = rows1[1][1]['indicator_1']['value'].split(' ')[0]
        ind_value_2 = float(rows1[1][1]['indicator_1']['value'].split(' ')[1])
    if change == 'more_than_previous':
        if indicator > last_ind:
            if ind_oper_2 == '<=':
                if indicator <= ind_value_2:
                    return True
            if ind_oper_2 == '>=':
                if indicator >= ind_value_2:
                    return True
            if ind_oper_2 == '=':
                if indicator == ind_value_2:
                    return True
            if ind_oper_2 == '<':
                if indicator < ind_value_2:
                    return True
            if ind_oper_2 == '>':
                if indicator > ind_value_2:
                    return True
    else:
        if indicator < last_ind:
            if ind_oper_2 == '<=':
                if indicator <= ind_value_2:
                    return True
            if ind_oper_2 == '>=':
                if indicator >= ind_value_2:
                    return True
            if ind_oper_2 == '=':
                if indicator == ind_value_2:
                    return True
            if ind_oper_2 == '<':
                if indicator < ind_value_2:
                    return True
            if ind_oper_2 == '>':
                if indicator > ind_value_2:
                    return True

    return False

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
        # первый блок активация
    if stat == '0':
        if '0' in activations[0][0].split(','):
            indicator1 = cc['indicator_1' + '_' + rows1[0][0]['indicator_1']['setting']]
            try:
                last_ind = back_price_1[back_price_1.index(cc) - 1][
                    'indicator_1' + '_' + rows1[0][0]['indicator_1']['setting']]
            except:
                continue
            direction = 'long'
            change = rows1[0][0]['indicator_1']['change']
            order_type = rows1[0][0]['position_action']['order_type']
            if block_1(indicator1, direction, change, last_ind):
                print('Открытие ордера')
                open_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
                stat = 'open_1_1'
                cancel_status = rows1[0][0]['position_action']['cancel'].split(',')
                continue
        if '0' in activations[0][1].split(','):
            indicator1 = cc['indicator_1' + '_' + rows1[0][1]['indicator_1']['setting']]
            try:
                last_ind = back_price_1[back_price_1.index(cc) - 1][
                    'indicator_1' + '_' + rows1[0][1]['indicator_1']['setting']]
            except:
                continue
            direction = 'short'
            change = rows1[0][1]['indicator_1']['change']
            order_type = rows1[0][1]['position_action']['order_type']
            if block_1(indicator1, direction, change, last_ind):
                print('Открытие ордера')
                open_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
                stat = 'open_1_1'
                cancel_status = rows1[0][1]['position_action']['cancel'].split(',')
                continue
    # процесс первого открытия
    if stat == 'open_1_1':
        if direction == 'long':
            if cc['low'] <= back_price_1[back_price_1.index(cc) - 1]['close'] and order_type == 'limit':
                open_time_position = back_price_1[back_price_1.index(cc) + 1]['time']
                side_1 = rows1[0][0]['position_action']['direction']
                price_old = back_price_1[back_price_1.index(cc) - 1]['close']
                try:
                    price_indent = rows1[0][0]['position_action']['price_indent']
                    price = float(price_old) - (float(price_old) / 100) * float(price_indent)
                except:
                    price = float(price_old) - (float(price_old) / 100)
                if open_price_order == 0:
                    open_price_order = price
                leverage = rows1[0][0]['position_action']['leverage']
                lot = (float(start_balance) * float(price)) * float(leverage)
                lot = int(round(lot, -1))
                direction = rows1[0][0]['position_action']['direction']
                order_type = rows1[0][0]['position_action']['order_type']
                order_type_1 = order_type
                order.append(direction)
                order.append(price)
                order.append(lot)
                order.append(order_type)
                order.append(cc['time'])
                stat = 'open_2_2'
                block_id = '1'
                print('Открытие позиции')
                continue
            if order_type == 'market':
                open_time_position = open_time_order
                side_1 = rows1[0][0]['position_action']['direction']
                price_old = back_price_1[back_price_1.index(cc) - 1]['close']
                try:
                    price_indent = rows1[0][0]['position_action']['price_indent']
                    price = float(price_old) - (float(price_old) / 100) * float(price_indent)
                except:
                    price = float(price_old)
                if open_price_order == 0:
                    open_price_order = price
                leverage = rows1[0][0]['position_action']['leverage']
                lot = (float(start_balance) * float(price)) * float(leverage)
                lot = int(round(lot, -1))
                direction = rows1[0][0]['position_action']['direction']
                order_type = rows1[0][0]['position_action']['order_type']
                order_type_1 = order_type
                order.append(direction)
                order.append(price)
                order.append(lot)
                order.append(order_type)
                order.append(cc['time'])
                stat = 'open_2_2'
                block_id = '1'
                print('Открытие позиции')
                continue
        if direction == 'short':
            if cc['high'] >= back_price_1[back_price_1.index(cc) - 1]['close'] and order_type == 'limit':
                open_time_position = back_price_1[back_price_1.index(cc) + 1]['time']
                side_1 = rows1[0][1]['position_action']['direction']
                price_old = back_price_1[back_price_1.index(cc) - 1]['close']
                try:
                    price_indent = rows1[0][1]['position_action']['price_indent']
                    price = float(price_old) + (float(price_old) / 100) * float(price_indent)
                except:
                    price = float(price_old)
                if open_price_order == 0:
                    open_price_order = price
                try:
                    leverage = rows1[0][1]['position_action']['leverage']
                except:
                    leverage = 1
                lot = (float(start_balance) * float(price)) * float(leverage)
                lot = int(round(lot, -1))
                direction = rows1[0][1]['position_action']['direction']
                order_type = rows1[0][1]['position_action']['order_type']
                order.append(direction)
                order.append(price)
                order.append(lot)
                order.append(order_type)
                order.append(cc['time'])
                stat = 'open_2_2'
                block_id = '1'
                print('Открытие позиции')
                continue
            if order_type == 'market':
                open_time_position = open_time_order
                side_1 = rows1[0][1]['position_action']['direction']
                price_old = back_price_1[back_price_1.index(cc) - 1]['close']
                try:
                    price_indent = rows1[0][1]['position_action']['price_indent']
                    price = float(price_old) + (float(price_old) / 100) * float(price_indent)
                except:
                    price = float(price_old) + (float(price_old) / 100)
                if open_price_order == 0:
                    open_price_order = price
                try:
                    leverage = rows1[0][1]['position_action']['leverage']
                except:
                    leverage = 1
                lot = (float(start_balance) * float(price)) * float(leverage)
                lot = int(round(lot, -1))
                direction = rows1[0][1]['position_action']['direction']
                order_type = rows1[0][1]['position_action']['order_type']
                order.append(direction)
                order.append(price)
                order.append(lot)
                order.append(order_type)
                order.append(cc['time'])
                stat = 'open_2_2'
                block_id = '1'
                print('Открытие позиции')
                continue
    if stat == 'open_2_2':
        if direction == 'long':
            activation1 = activations[block_num][0].split(',')
        else:
            activation1 = activations[block_num][1].split(',')
        for block in activation1:
            if block == '2_long' or block == '2_short':
                if direction == 'long':
                    indicator2 = cc['indicator_1' + '_' + rows1[1][0]['indicator_1']['setting']]
                    order_type_2 = rows1[1][0]['position_action']['order_type']
                    cancel_status = rows1[1][0]['position_action']['cancel'].split(',')
                    try:
                        last_ind = back_price_1[back_price_1.index(cc) - 1][
                            'indicator_1' + '_' + rows1[1][0]['indicator_1']['setting']]
                    except:
                        continue
                    change = rows1[1][0]['indicator_1']['change']
                else:
                    indicator2 = cc['indicator_1' + '_' + rows1[1][1]['indicator_1']['setting']]
                    order_type_2 = rows1[1][1]['position_action']['order_type']
                    cancel_status = rows1[1][1]['position_action']['cancel'].split(',')
                    try:
                        last_ind = back_price_1[back_price_1.index(cc) - 1][
                            'indicator_1' + '_' + rows1[1][1]['indicator_1']['setting']]
                    except:
                        continue
                    change = rows1[1][1]['indicator_1']['change']
                # print(cc['time'])
                if block_2(indicator2, direction, change, last_ind):
                    print('Закрытие')
                    block_num = 1
                    stat = 'close_1'
                    close_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
                    block_id = block_id + ',2'

                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break

    if stat == 'close_1':
        if cc['low'] <= back_price_1[back_price_1.index(cc) - 1]['close'] and order_type == 'limit' and direction == 'long' or direction == 'long' and order_type == 'market':
            close_candle = float(cc['close'])
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
            percent_deal = (money_deal / balance) * 100
            balance = balance + money_deal
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
                "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, balance, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            data = (
                id_day, order[0], lot, order_type_1, open_time_order, open_price_order, open_time_position, order_type_2,
                close_time_order, close_candle, close_time_position, fee, res, points_deal, money_deal, percent_deal,
                balance, money_day, percent_day, min_balance_percent, 0, 0, block_id)
            try:
                cursor.execute(insert_stmt, data)
                cnx.commit()
            except Exception as e:
                print(e)

            if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[2]:
                id_day = id_day + 1
            order = []
            close_candle = 0
            open_time_order = 0
            open_time_position = 0
            open_price_order = 0
            open_price_position = 0
            close_time_order = 0
            close_time_position = 0
            points_deal = 0
            res = ''
            fee = 0
            money_deal = 0
            stat = '0'
            block_id = ''
            ids = ids + 1
            stat = '0'
            block_num = 0
            continue


        if cc['high'] >= back_price_1[back_price_1.index(cc) - 1]['close'] and order_type == 'limit' and direction == 'short' or direction == 'short' and order_type == 'market':
            close_candle = float(cc['close'])
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
            percent_deal = (money_deal / balance) * 100
            balance = balance + money_deal
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
                "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, balance, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            data = (
                id_day, order[0], lot, order_type_1, open_time_order, open_price_order, open_time_position, order_type_2,
                close_time_order, close_candle, close_time_position, fee, res, points_deal, money_deal, percent_deal,
                balance, money_day, percent_day, min_balance_percent, 0, 0, block_id)
            cursor.execute(insert_stmt, data)
            cnx.commit()
            if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[2]:
                id_day = id_day + 1
            order = []
            close_candle = 0
            open_time_order = 0
            open_time_position = 0
            open_price_order = 0
            open_price_position = 0
            close_time_order = 0
            close_time_position = 0
            points_deal = 0
            res = ''
            fee = 0
            money_deal = 0
            stat = '0'
            block_id = ''
            ids = ids + 1
            stat = '0'
            block_num = 0
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