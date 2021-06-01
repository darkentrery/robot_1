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


stat = '0'
price = ''
lot = ''
results = []
last_value = 0
fee_limit = 0
fee_market = 0#0.045
squeeze = 0#30
id_day = 1
proboi = 0
day = 0
proboi = 0
proboi_line_proc = 0
proboi_status = 0
exit_price_price = False
order = []
fee = 0
old_proboi = 0
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
proboi_stup = 0
new_breakdown_sum = 1
proboi_end = 0
exit_price_price = 0
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
stat_10 = [0, 0]
check_stup = 0
price_value = 0
proboi_stat = 0
vh_vih_stat = 0
order_type_1 = 0
order_type_2 = 0
cancel_status = ''
open_price_order = 0
for gg in rows1:
    #try:
    rows2.append([ast.literal_eval(gg[8]), ast.literal_eval(gg[10])])
    activations.append([gg[7], gg[9]])
    #except:
        #continue
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
    #print(change)
    #print(str(indicator) + ' ' + str(ind_oper_2) + ' ' + str(ind_value_2))
    #print('previous ' + str(last_ind))
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

def block_3_1(indicator, dictin, direction, change, last_ind):
    if direction == 'long':
        ind_oper_3 = rows1[4][0]['indicator_1_1']['value'].split(' ')[0]
        ind_value_3 = float(rows1[4][0]['indicator_1_1']['value'].split(' ')[1])
    else:
        ind_oper_3 = rows1[4][1]['indicator_1_1']['value'].split(' ')[0]
        ind_value_3 = float(rows1[4][1]['indicator_1_1']['value'].split(' ')[1])
    if change != 0:
        if change == 'more_than_previous':
            if dictin[indicator] > last_ind:
                if ind_oper_3 == '>=':
                    if dictin[indicator] >= ind_value_3:
                        return True
                if ind_oper_3 == '<=':
                    if dictin[indicator3] <= ind_value_3:
                        return True
                if ind_oper_3 == '=':
                    if dictin[indicator] == ind_value_3:
                        return True
                if ind_oper_3 == '>':
                    if dictin[indicator] > ind_value_3:
                        return True
                if ind_oper_3 == '<':
                    if dictin[indicator] < ind_value_3:
                        return True
        else:
            if dictin[indicator] < last_ind:
                if ind_oper_3 == '>=':
                    if dictin[indicator] >= ind_value_3:
                        return True
                if ind_oper_3 == '<=':
                    if dictin[indicator3] <= ind_value_3:
                        return True
                if ind_oper_3 == '=':
                    if dictin[indicator] == ind_value_3:
                        return True
                if ind_oper_3 == '>':
                    if dictin[indicator] > ind_value_3:
                        return True
                if ind_oper_3 == '<':
                    if dictin[indicator] < ind_value_3:
                        return True
    else:
        if ind_oper_3 == '>=':
            if dictin[indicator] >= ind_value_3:
                return True
        if ind_oper_3 == '<=':
            if dictin[indicator3] <= ind_value_3:
                return True
        if ind_oper_3 == '=':
            if dictin[indicator] == ind_value_3:
                return True
        if ind_oper_3 == '>':
            if dictin[indicator] > ind_value_3:
                return True
        if ind_oper_3 == '<':
            if dictin[indicator] < ind_value_3:
                return True
    return False

def block_3_2(indicator, direction, change, last_ind):
    if direction == 'long':
        ind_oper_4 = rows1[4][0]['indicator_1_2']['value'].split(' ')[0]
        ind_value_4 = float(rows1[4][0]['indicator_1_2']['value'].split(' ')[1])
    else:
        ind_oper_4 = rows1[4][1]['indicator_1_2']['value'].split(' ')[0]
        ind_value_4 = float(rows1[4][1]['indicator_1_2']['value'].split(' ')[1])
    if change != 0:
        if change == 'more_than_previous':
            if indicator > last_ind:
                if ind_oper_4 == '>=':
                    if indicator >= ind_value_4:
                        return True
                if ind_oper_4 == '<=':
                    if indicator <= ind_value_4:
                        return True
                if ind_oper_4 == '=':
                    if indicator == ind_value_4:
                        return True
                if ind_oper_4 == '>':
                    if indicator > ind_value_4:
                        return True
                if ind_oper_4 == '<':
                    if indicator < ind_value_4:
                        return True
        else:
            if indicator < last_ind:
                if ind_oper_4 == '>=':
                    if indicator >= ind_value_4:
                        return True
                if ind_oper_4 == '<=':
                    if indicator <= ind_value_4:
                        return True
                if ind_oper_4 == '=':
                    if indicator == ind_value_4:
                        return True
                if ind_oper_4 == '>':
                    if indicator > ind_value_4:
                        return True
                if ind_oper_4 == '<':
                    if indicator < ind_value_4:
                        return True
    else:
        if ind_oper_4 == '>=':
            if indicator >= ind_value_4:
                return True
        if ind_oper_4 == '<=':
            if indicator <= ind_value_4:
                return True
        if ind_oper_4 == '=':
            if indicator == ind_value_4:
                return True
        if ind_oper_4 == '>':
            if indicator > ind_value_4:
                return True
        if ind_oper_4 == '<':
            if indicator < ind_value_4:
                return True
    return False

def block_4(indicator, direction):
    if direction == 'long':
        ind_oper_5 = rows1[3][0]['indicator_1_1']['value'].split(' ')[0]
        ind_value_5 = float(rows1[3][0]['indicator_1_1']['value'].split(' ')[1])
    else:
        ind_oper_5 = rows1[3][1]['indicator_1_1']['value'].split(' ')[0]
        ind_value_5 = float(rows1[3][1]['indicator_1_1']['value'].split(' ')[1])
    if ind_oper_5 == '>=':
        if indicator >= ind_value_5:
            return True
    if ind_oper_5 == '<=':
        if indicator <= ind_value_5:
            return True
    if ind_oper_5 == '=':
        if indicator == ind_value_5:
            return True
    if ind_oper_5 == '>':
        if indicator > ind_value_5:
            return True
    return False

def block_5(indicator, direction, num_block, change, last_ind):
    if direction == 'long':
        ind_oper_6 = rows1[num_block][0]['indicator_1']['value'].split(' ')[0]
        ind_value_6 = float(rows1[num_block][0]['indicator_1']['value'].split(' ')[1])
    else:
        ind_oper_6 = rows1[num_block][1]['indicator_1']['value'].split(' ')[0]
        ind_value_6 = float(rows1[num_block][1]['indicator_1']['value'].split(' ')[1])
    if change == 'more_than_previous':
        if indicator > last_ind:
            if ind_oper_6 == '>=':
                if indicator >= ind_value_6:
                    return True
            if ind_oper_6 == '<=':
                if indicator <= ind_value_6:
                    return True
            if ind_oper_6 == '=':
                if indicator == ind_value_6:
                    return True
            if ind_oper_6 == '>':
                if indicator > ind_value_6:
                    return True
            if ind_oper_6 == '<':
                if indicator < ind_value_6:
                    return True
    else:
        if indicator < last_ind:
            if ind_oper_6 == '>=':
                if indicator >= ind_value_6:
                    return True
            if ind_oper_6 == '<=':
                if indicator <= ind_value_6:
                    return True
            if ind_oper_6 == '=':
                if indicator == ind_value_6:
                    return True
            if ind_oper_6 == '>':
                if indicator > ind_value_6:
                    return True
            if ind_oper_6 == '<':
                if indicator < ind_value_6:
                    return True
    return False

def block_6(dictin, proboi, direction, num_block):
    if direction == 'long':
        proc_value_2 = float(rows1[num_block][0]['indicator_2']['exit_price_percent'])
        check = rows1[num_block][0]['indicator_2']['check']
        exit_price_price = rows1[num_block][0]['indicator_2']['exit_price_price']
        new_breakdown_sum = int(rows1[num_block][0]['indicator_2']['new_breakdown_sum'])
    else:
        proc_value_2 = float(rows1[num_block][1]['indicator_2']['exit_price_percent'])
        check = rows1[num_block][1]['indicator_2']['check']
        exit_price_price = rows1[num_block][1]['indicator_2']['exit_price_price']
        new_breakdown_sum = int(rows1[num_block][1]['indicator_2']['new_breakdown_sum'])
    proboi_end = new_breakdown_sum
    candle_check = 0
    try:
        if check == 'low':
            proc = (proboi - float(dictin['low'])) / (proboi / 100)
            candle_check = float(dictin['low'])
        if check == 'close':
            proc = (proboi - float(dictin['close'])) / (proboi / 100)
            candle_check = float(dictin['close'])
        if check == 'high':
            proc = (proboi - float(dictin['high'])) / (proboi / 100)
            candle_check = float(dictin['high'])
    except:
        return False
    if proc > proc_value_2:
        return proboi_end
    return False

def block_6_1(dictin, proboi, direction, num_block, side):
    if direction == 'long':
        proc_value_2 = float(rows1[num_block][0]['indicator_2']['exit_price_percent'])
        check = rows1[num_block][0]['indicator_2']['check']
        try:
            exit_price_price = rows1[num_block][0]['indicator_2']['exit_price_price']
        except:
            exit_price_price = False
    else:
        proc_value_2 = float(rows1[num_block][1]['indicator_2']['exit_price_percent'])
        check = rows1[num_block][1]['indicator_2']['check']
        try:
            exit_price_price = rows1[num_block][1]['indicator_2']['exit_price_price']
        except:
            exit_price_price = False
    candle_check = 0
    try:
        if check == 'low':
            proc = (float(proboi) - float(dictin['low'])) / (float(proboi) / 100)
            candle_check = float(dictin['low'])
            value = float(proboi) - ((float(proboi) / 100) * proc_value_2)
        if check == 'close':
            if side == 'high':
                proc = (float(dictin['close']) - float(proboi)) / (float(proboi)/100)
                candle_check = float(dictin['close'])
                value = float(proboi) + ((float(proboi) / 100) * proc_value_2)
            if side == 'low':
                proc = (float(proboi) - float(dictin['close'])) / (float(proboi) / 100)
                candle_check = float(dictin['close'])
                value = float(proboi) - ((float(proboi) / 100) * proc_value_2)
        if check == 'high':
            proc = (float(dictin['high']) - float(proboi)) / (float(proboi)/100)
            candle_check = float(dictin['high'])
            value = float(proboi) + ((float(proboi) / 100) * proc_value_2)

    except:
        return False
    if proc > proc_value_2:
        if exit_price_price:
            if check == 'low':
                if exit_price_price == 'yes':
                    if float(dictin['close']) <= float(proboi):
                        return value
                else:
                    if float(dictin['close']) > float(proboi):
                        return value
            if check == 'high':
                if exit_price_price == 'yes':
                    if float(dictin['close']) >= float(proboi):
                        return value
                else:
                    if float(dictin['close']) < float(proboi):
                        return value

        else:
            return value
    return False

def block_9_1(dictin, old_proboi, proboi, direction, num_block, side, exit_price_price):
    if exit_price_price == False:
        if direction == 'long':
            check = rows1[num_block][0]['indicator_2']['check']
        else:
            check = rows1[num_block][1]['indicator_2']['check']
        try:
            if check == 'low':
                if float(dictin['low']) < float(proboi):
                    proc = (float(old_proboi) - float(dictin['low'])) / (float(old_proboi) / 100)
                    return proc
            if check == 'close':
                if side == 'high':
                    if float(dictin['close']) > float(proboi):
                        proc = (float(dictin['close']) - float(old_proboi)) / (float(old_proboi)/100)
                        return proc
                if side == 'low':
                    if float(proboi) > float(dictin['close']):
                        proc = (float(old_proboi) - float(dictin['close'])) / (float(old_proboi) / 100)
                        return proc
            if check == 'high':
                if float(dictin['high']) > float(proboi):
                    proc = (float(dictin['high']) - float(old_proboi)) / (float(old_proboi)/100)
                    return proc

        except:
            return False
        return False
    else:
        if direction == 'long':
            check = rows1[num_block][0]['indicator_2']['check']
        else:
            check = rows1[num_block][1]['indicator_2']['check']
        try:
            if check == 'low':
                if float(dictin['low']) < float(proboi):
                    if float(dictin['close']) <= float(proboi):
                        proc = (float(old_proboi) - float(dictin['low'])) / (float(old_proboi) / 100)
                        return proc
            if check == 'close':
                if side == 'high':
                    if float(dictin['close']) > float(proboi):
                        proc = (float(dictin['close']) - float(old_proboi)) / (float(old_proboi)/100)
                        return proc
                if side == 'low':
                    if float(proboi) > float(dictin['close']):
                        proc = (float(old_proboi) - float(dictin['close'])) / (float(old_proboi) / 100)
                        return proc
            if check == 'high':
                if float(dictin['high']) > float(proboi):
                    if float(dictin['close']) >= float(proboi):
                        proc = (float(dictin['high']) - float(old_proboi)) / (float(old_proboi)/100)
                        return proc
        except:
            return False

def block_7(dictin, proboi, direction):
    proc = ''
    if direction == 'long':
        proc_value_1 = float(rows1[6][0]['indicator_2']['exit_price_percent'])
        check = rows1[6]['indicator_2'][0]['check']
        exit_price_price = rows1[6][0]['indicator_2']['exit_price_price']
    else:
        proc_value_1 = float(rows1[6][1]['indicator_2']['exit_price_percent'])
        check = rows1[6][1]['indicator_2']['check']
        exit_price_price = rows1[6][1]['indicator_2']['exit_price_price']
    candle_check = 0
    if check == 'low':
        proc = (proboi - float(dictin['low'])) / (proboi / 100)
        candle_check = float(dictin['low'])
    if check == 'close':
        proc = (proboi - float(dictin['close'])) / (proboi / 100)
        candle_check = float(dictin['close'])
    if check == 'high':
        proc = (proboi - float(dictin['high'])) / (proboi / 100)
        candle_check = float(dictin['high'])
    if exit_price_price == 'yes':
        if candle_check <= dictin['indicator_2_' + rows1[6]['indicator_2']['setting'] + '-' + rows1[6]['indicator_2']['side']]:
            if proc > proc_value_1:
                return True
    else:
        if proc > proc_value_1:
            return True
    return False

def block_9(indicator, direction):
    if direction == 'long':
        ind_oper_8 = rows1[8][0]['indicator_1_1']['value'].split(' ')[0]
        ind_value_8 = float(rows1[8][0]['indicator_1_1']['value'].split(' ')[1])
    else:
        ind_oper_8 = rows1[8][1]['indicator_1_1']['value'].split(' ')[0]
        ind_value_8 = float(rows1[8][1]['indicator_1_1']['value'].split(' ')[1])
    if ind_oper_8 == '>=':
        if indicator >= ind_value_8:
            return True
    if ind_oper_8 == '<=':
        if indicator <= ind_value_8:
            return True
    if ind_oper_8 == '=':
        if indicator == ind_value_8:
            return True
    if ind_oper_8 == '>':
        if indicator > ind_value_8:
            return True
    if ind_oper_8 == '<':
        if indicator < ind_value_8:
            return True
    return False

def block_11(open_price_order, direction, num_block):
    if direction == 'short':
        ind_oper_9 = rows1[num_block][1]['position_condition']['pnl'].split(' ')[0]
        ind_value_9 = float(rows1[num_block][1]['position_condition']['pnl'].split(' ')[1])
        pnl = open_price_order - (((open_price_order / 100) * ind_value_9))
    else:
        ind_oper_9 = rows1[num_block][0]['position_condition']['pnl'].split(' ')[0]
        ind_value_9 = float(rows1[num_block][0]['position_condition']['pnl'].split(' ')[1])
        pnl = open_price_order + (((open_price_order / 100) * ind_value_9))
    if ind_oper_9 == '>=':
        if pnl >= ind_value_9:
            return True
    if ind_oper_9 == '<=':
        if pnl <= ind_value_9:
            return True
    if ind_oper_9 == '=':
        if pnl == ind_value_9:
            return True
    if ind_oper_9 == '>':
        if pnl > ind_value_9:
            return True
    if ind_oper_9 == '<':
        if pnl < ind_value_9:
            return True
    return False



for cc in back_price_1:
    if len(order) > 0:
        if order[0] == 'long':
            last_value = float(cc['indicator_1_' + rows1[1][0]['indicator_1']['setting']])
            try:
                proboi = float(cc['indicator_2_' + rows1[5][0]['indicator_2']['setting'] + '-' + rows1[5][0]['indicator_2']['side']])
            except:
                proboi = 0
        else:
            last_value = float(cc['indicator_1_' + rows1[1][1]['indicator_1']['setting']])
            try:
                proboi = float(cc['indicator_2_' + rows1[5][1]['indicator_2']['setting'] + '-' + rows1[5][1]['indicator_2']['side']])
            except:
                proboi = 0

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
                last_ind = back_price_1[back_price_1.index(cc)-1]['indicator_1' + '_' + rows1[0][0]['indicator_1']['setting']]
            except:
                continue
            direction = 'long'
            change = rows1[0][0]['indicator_1']['change']
            order_type = rows1[0][0]['position_action']['order_type']
            if block_1(indicator1, direction, change, last_ind):
                print('Открытие ордера 1')
                open_time_order = back_price_1[back_price_1.index(cc)+1]['time']
                stat = 'open_1_1'
                cancel_status = rows1[0][0]['position_action']['cancel'].split(',')
                continue
        if '0' in activations[0][1].split(','):
            indicator1 = cc['indicator_1' + '_' + rows1[0][1]['indicator_1']['setting']]
            try:
                last_ind = back_price_1[back_price_1.index(cc)-1]['indicator_1' + '_' + rows1[0][1]['indicator_1']['setting']]
            except:
                continue
            direction = 'short'
            change = rows1[0][1]['indicator_1']['change']
            order_type = rows1[0][1]['position_action']['order_type']
            if block_1(indicator1, direction,change, last_ind):
                print('Открытие ордера 1')
                open_time_order = back_price_1[back_price_1.index(cc)+1]['time']
                stat = 'open_1_1'
                cancel_status = rows1[0][1]['position_action']['cancel'].split(',')
                continue
    if stat == 'open_1_1':
        #print(order_type)
        #print(direction)
        if direction == 'long':
            if cc['low'] <= back_price_1[back_price_1.index(cc)-1]['close'] and order_type == 'limit':
                open_time_position = back_price_1[back_price_1.index(cc)+1]['time']
                side_1 = rows1[0][0]['position_action']['direction']
                price_old = back_price_1[back_price_1.index(cc)-1]['close']
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
                print('Открытие позиции 1')
                continue
            if order_type == 'market':
                open_time_position = open_time_order
                side_1 = rows1[0][0]['position_action']['direction']
                price_old = back_price_1[back_price_1.index(cc)-1]['close']
                try:
                    price_indent = rows1[0][0]['position_action']['price_indent']
                    price = float(price_old) - (float(price_old) / 100) * float(price_indent)
                except:
                    price = float(price_old) #- (float(price_old) / 100)
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
                print('Открытие позиции 1')
                continue
        if direction == 'short':
            if cc['high'] >= back_price_1[back_price_1.index(cc) - 1]['close'] and order_type == 'limit':
                open_time_position = back_price_1[back_price_1.index(cc)+1]['time']
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
                print('Открытие позиции 1')
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
                print('Открытие позиции 14')
                continue
        # отмена на открытие
        for block in cancel_status:
            # закрытие
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

                if block_2(indicator2, direction, change, last_ind):
                    print('cancel2')

                    block_num = 1
                    stat = 'close_1'
                    close_time_order = back_price_1[back_price_1.index(cc)+1]['time']#cc['time']
                    block_id = block_id + ',2'
                    break
            # безубыток
            if block == '3_long' or block == '3_short':
                if direction == 'long':
                    order_type_2 = rows1[2][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[2][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 2):
                    block_num = 2
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',3'
                    break
            # стоп
            if block == '4_long' or block == '4_short':
                if direction == 'long':
                    order_type_2 = rows1[3][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[3][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 3):
                    block_num = 3
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',4'
                    break
            # продолжение тренда открытие
            if block == '5_long' or block == '5_short':
                if vh_vih_stat == 0:
                    if direction == 'long':
                        indicator3 = 'indicator_1' + '_' + rows1[4][0]['indicator_1_1']['setting']
                        order_type_2 = rows1[4][0]['position_action_1']['order_type']
                    else:
                        indicator3 = 'indicator_1' + '_' + rows1[4][1]['indicator_1_1']['setting']
                        order_type_2 = rows1[4][1]['position_action_1']['order_type']
                    if block_3_1(indicator3, cc, direction):
                        vh_vih_stat = 1
                        # stat = 'vhod_vihod_1'
                        # block_id = block_id + ',3'
                        # break
                else:
                    if direction == 'long':
                        indicator6 = cc['indicator_1_' + rows1[4][0]['indicator_1_2']['setting']]
                    else:
                        indicator6 = cc['indicator_1_' + rows1[4][1]['indicator_1_2']['setting']]
                    if block_3_2(indicator6, direction):
                        stat = 'close_open_1'
                        block_num = 4
                        block_id = block_id + ',5'
                        close_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
                        vh_vih_stat = 0
            # продолжение тренда закрытие
            if block == '6_long' or block == '6_short':
                if vh_vih_stat == 0:
                    if direction == 'long':
                        indicator3 = 'indicator_1' + '_' + rows1[5][0]['indicator_1_1']['setting']
                        order_type_2 = rows1[5][0]['position_action_1']['order_type']
                    else:
                        indicator3 = 'indicator_1' + '_' + rows1[5][1]['indicator_1_1']['setting']
                        order_type_2 = rows1[5][1]['position_action_1']['order_type']
                    if block_3_1(indicator3, cc, direction):
                        vh_vih_stat = 1
                        # stat = 'vhod_vihod_1'
                        # block_id = block_id + ',3'
                        # break
                else:
                    if direction == 'long':
                        indicator6 = cc['indicator_1_' + rows1[5][0]['indicator_1_2']['setting']]
                    else:
                        indicator6 = cc['indicator_1_' + rows1[5][1]['indicator_1_2']['setting']]
                    if block_3_2(indicator6, direction):
                        stat = 'close_1'
                        block_num = 5
                        block_id = block_id + ',5'
                        close_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
                        vh_vih_stat = 0
            # продолжение тренда безубыток
            if block == '7_long' or block == '7_short':
                if direction == 'long':
                    order_type_2 = rows1[6][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[6][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 6):
                    block_num = 6
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',7'
                    break
            # продолжение тренда стоп
            if block == '8_long' or block == '8_short':
                if direction == 'long':
                    indicator9 = cc['indicator_1_' + rows1[7][0]['indicator_1']['setting']]
                    order_type_2 = rows1[7][0]['position_action']['order_type']
                else:
                    indicator9 = cc['indicator_1_' + rows1[7][1]['indicator_1']['setting']]
                    order_type_2 = rows1[7][1]['position_action']['order_type']
                if block_5(indicator9, direction):
                    block_num = 7
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',7'
                    break
            # пробой уровня 1 ступень
            if block == '10_long' or block == '10_short':
                # if direction == 'long':
                # order_type_2 = rows1[6][0]['position_action']['order_type']
                # else:
                # order_type_2 = rows1[6][1]['position_action']['order_type']
                if block_6(cc, proboi, direction):
                    block_num = 9
                    stat = 'close_open_2'
                    close_candle = float(cc['close'])
                    close_time_order = cc['time']
                    block_id = block_id + '10,11'
                    break
            # пробой уровня вторая ступень
            if block == '9_long' or block == '9_short':
                # if direction == 'long':
                # order_type_2 = rows1[5][0]['position_action']['order_type']
                # else:
                # order_type_2 = rows1[5][1]['position_action']['order_type']
                if proboi_stat == 0:
                    if block_6(cc, proboi, direction):
                        proboi_end = block_6(cc, proboi, direction)
                        proboi_stat = 1
                        # stat = 'proboi_1'
                        proboi_stup = 1
                        # block_id = block_id + ',6'
                        # break
                else:
                    if proboi_stup == proboi_end:
                        if direction == 'long':
                            indicator4 = cc['indicator_2_' + rows1[8][0]['indicator_2']['setting'] + '-' +
                                            rows1[8][0]['indicator_2']['side']]
                            proc_value_2 = float(rows1[8][0]['indicator_2']['exit_price_percent'])
                            check = rows1[8][0]['indicator_2']['check']
                            exit_price_price = rows1[8][0]['indicator_2']['exit_price_price']
                            try:
                                new_breakdown_sum = int(rows1[8][0]['indicator_2']['new_breakdown_sum'])
                            except:
                                new_breakdown_sum = 0
                        else:
                            indicator4 = cc['indicator_2_' + rows1[8][0]['indicator_2']['setting'] + '-' +
                                            rows1[8][1]['indicator_2']['side']]
                            proc_value_2 = float(rows1[8][1]['indicator_2']['exit_price_percent'])
                            check = rows1[8][1]['indicator_2']['check']
                            exit_price_price = rows1[8][1]['indicator_2']['exit_price_price']
                            try:
                                new_breakdown_sum = int(rows1[8][1]['indicator_2']['new_breakdown_sum'])
                            except:
                                new_breakdown_sum = 0

                        candle_check = 0
                        proc = 0
                        if check == 'low':
                            proc = (proboi - float(cc['low'])) / (proboi / 100)
                            candle_check = float(cc['low'])
                        if check == 'close':
                            proc = (proboi - float(cc['close'])) / (proboi / 100)
                            candle_check = float(cc['close'])
                        if check == 'high':
                            proc = (proboi - float(cc['high'])) / (proboi / 100)
                            candle_check = float(cc['high'])
                        if exit_price_price == 'yes':
                            if candle_check <= indicator4:
                                if proc > proc_value_2:
                                    block_num = 8
                                    stat = 'close_open_2'
                                    close_candle = float(cc['close'])
                                    close_time_order = cc['time']
                                    block_id = block_id + ',9,11'
                                    proboi_stat = 0
                                    break  # continue
                        else:
                            if proc > proc_value_2:
                                block_num = 8
                                stat = 'close_open_2'  # 'proboi_2'
                                close_candle = float(cc['close'])
                                close_time_order = cc['time']
                                block_id = block_id + ',9,11'
                                proboi_stat = 0
                                break  # continue
                    else:
                        if direction == 'long':
                            proc_value_2 = float(rows1[8][0]['indicator_2']['exit_price_percent'])
                            check = rows1[5][0]['indicator_2']['check']
                            exit_price_price = rows1[8][0]['indicator_2']['exit_price_price']
                            new_breakdown_sum = int(rows1[8][0]['indicator_2']['new_breakdown_sum'])
                        else:
                            proc_value_2 = float(rows1[8][1]['indicator_2']['exit_price_percent'])
                            check = rows1[8][1]['indicator_2']['check']
                            exit_price_price = rows1[8][1]['indicator_2']['exit_price_price']
                            new_breakdown_sum = int(rows1[8][1]['indicator_2']['new_breakdown_sum'])
                        candle_check = 0
                        if check == 'low':
                            proc = (proboi - float(cc['low'])) / (proboi / 100)
                            candle_check = float(cc['low'])
                        if check == 'close':
                            proc = (proboi - float(cc['close'])) / (proboi / 100)
                            candle_check = float(cc['close'])
                        if check == 'high':
                            proc = (proboi - float(cc['high'])) / (proboi / 100)
                            candle_check = float(cc['high'])
                        if proc > proc_value_2:
                            # stat = 'proboi_1'
                            proboi_stup = proboi_stup + 1
                            # block_id = '1,6'
                            continue
            # пробой уровня закрытие
            if block == '12_long' or block == '12_short':
                if direction == 'long':
                    order_type_2 = rows1[11][0]['position_action']['order_type']
                    check_stup = back_price_1[back_price_1.index(cc) - 1]['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' + rows1[9][0]['indicator_2']['side']]
                    side_1 = rows1[11][0]['indicator_2']['side']
                    if side_1 == 'high':
                        if stat_10[0] == 0 and cc['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' + rows1[9][0]['indicator_2']['side']] >= check_stup:
                            stat_10[0] = 1
                            stat_10[1] = check_stup
                        if stat_10 == 1:
                            price_value = cc['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' +rows1[11][0]['indicator_2']['side']] + (cc['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' + rows1[11][0]['indicator_2']['side']] / 100) * rows1[11][0]['indicator_2']['exit_price_percent']
                            if price_value > check_stup:
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                break  # continue
                    else:
                        if stat_10[0] == 0 and cc['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' + rows1[11][0]['indicator_2']['side']] <= check_stup:
                            stat_10[0] = 1
                            stat_10[1] = check_stup
                        if stat_10 == 1:
                            price_value = cc['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' + rows1[11][0]['indicator_2']['side']] + (cc['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' +
                                                                                       rows1[11][0]['indicator_2'][
                                                                                           'side']] / 100) * \
                                          rows1[11][0]['indicator_2']['exit_price_percent']
                            if price_value < check_stup:
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                break  # continue

                else:
                    order_type_2 = rows1[11][1]['position_action']['order_type']
                    side_1 = rows1[11][0]['indicator_2']['side']
                    check_stup = back_price_1[back_price_1.index(cc) - 1][
                        'indicator_2_' + rows1[11][1]['indicator_2']['setting'] + '-' + rows1[11][1]['indicator_2'][
                            'side']]
                    if side_1 == 'high':
                        if stat_10[0] == 0 and cc[
                            'indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' + rows1[11][0]['indicator_2'][
                                'side']] >= check_stup:
                            stat_10[0] = 1
                            stat_10[1] = check_stup
                        if stat_10 == 1:
                            price_value = cc['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' +
                                             rows1[11][0]['indicator_2']['side']] + (cc['indicator_2_' +
                                                                                       rows1[11][0]['indicator_2'][
                                                                                           'setting'] + '-' +
                                                                                       rows1[11][0]['indicator_2'][
                                                                                           'side']] / 100) * \
                                          rows1[11][0]['indicator_2']['exit_price_percent']
                            if price_value > check_stup:
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                break  # continue
                    else:
                        if stat_10[0] == 0 and cc[
                            'indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' + rows1[11][0]['indicator_2'][
                                'side']] <= check_stup:
                            stat_10[0] = 1
                            stat_10[1] = check_stup
                        if stat_10 == 1:
                            price_value = cc['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' -
                                             rows1[11][0]['indicator_2']['side']] + (cc['indicator_2_' +
                                                                                       rows1[11][0]['indicator_2'][
                                                                                           'setting'] + '-' +
                                                                                       rows1[11][0]['indicator_2'][
                                                                                           'side']] / 100) * \
                                          rows1[11][0]['indicator_2']['exit_price_percent']
                            if price_value < check_stup:
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                break  # continue
            # пробой уровня безубыток
            if block == '13_long' or block == '13_short':
                if direction == 'long':
                    order_type_2 = rows1[12][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[12][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 12):
                    block_num = 12
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',13'
                    break
            if block == '14_long' or block == '14_short':
                if direction == 'long':
                    order_type_2 = rows1[13][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[13][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 13):
                    block_num = 13
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',14'
                    break
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
                #print(cc['time'])
                if block_2(indicator2, direction, change, last_ind):
                    print('Закрытие')
                    block_num = 1
                    stat = 'close_1'
                    close_time_order = back_price_1[back_price_1.index(cc)+1]['time']
                    block_id = block_id + ',2'

                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break
            # безубыток
            if block == '3_long' or block == '3_short':
                if direction == 'long':
                    order_type_2 = rows1[2][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[2][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 2):
                    print('Безубыток')
                    block_num = 2
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',3'

                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break
            # стоп
            if block == '4_long' or block == '4_short':
                if direction == 'long':
                    order_type_2 = rows1[3][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[3][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 3):
                    print('Стоп')
                    block_num = 3
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',4'

                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break
            # продолжение тренда открытие
            if block == '5_long' or block == '5_short':
                if vh_vih_stat == 0:
                    if direction == 'long':
                        indicator3 = 'indicator_1' + '_' + rows1[4][0]['indicator_1_1']['setting']
                        order_type_2 = rows1[4][0]['position_action_1']['order_type']
                        try:
                            last_ind = back_price_1[back_price_1.index(cc) - 1][
                                'indicator_1' + '_' + rows1[4][0]['indicator_1_1']['setting']]
                        except:
                            continue
                        try:
                            change = rows1[4][0]['indicator_1_1']['change']
                        except:
                            change = 0
                    else:
                        indicator3 = 'indicator_1' + '_' + rows1[4][1]['indicator_1_1']['setting']
                        order_type_2 = rows1[4][1]['position_action_1']['order_type']
                        try:
                            last_ind = back_price_1[back_price_1.index(cc) - 1][
                                'indicator_1' + '_' + rows1[4][1]['indicator_1_1']['setting']]
                        except:
                            continue
                        try:
                            change = rows1[4][1]['indicator_1_1']['change']
                        except:
                            change = 0
                    if block_3_1(indicator3, cc, direction, change, last_ind):
                        vh_vih_stat = 1
                        # stat = 'vhod_vihod_1'
                        # block_id = block_id + ',3'
                        # break
                else:
                    if direction == 'long':
                        indicator6 = cc['indicator_1_' + rows1[4][0]['indicator_1_2']['setting']]
                        try:
                            last_ind = back_price_1[back_price_1.index(cc) - 1][
                                'indicator_1' + '_' + rows1[4][0]['indicator_1_2']['setting']]
                        except:
                            continue
                        try:
                            change = rows1[4][0]['indicator_1_1']['change']
                        except:
                            change = 0
                    else:
                        indicator6 = cc['indicator_1_' + rows1[4][1]['indicator_1_2']['setting']]
                        try:
                            last_ind = back_price_1[back_price_1.index(cc) - 1][
                                'indicator_1' + '_' + rows1[4][1]['indicator_1_2']['setting']]
                        except:
                            continue
                        try:
                            change = rows1[4][1]['indicator_1_2']['change']
                        except:
                            change = 0
                    if block_3_2(indicator6, direction, change, last_ind):
                        print('Продолжение тренда открытие')
                        stat = 'close_open_1'
                        block_num = 4
                        block_id = block_id + ',5'
                        close_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
                        vh_vih_stat = 0

                        proboi_line_proc = 0
                        proboi_stup = 0
                        old_proboi = 0
                        exit_price_price = False
            # продолжение тренда закрытие
            if block == '6_long' or block == '6_short':
                if vh_vih_stat == 0:
                    if direction == 'long':
                        indicator3 = 'indicator_1' + '_' + rows1[5][0]['indicator_1_1']['setting']
                        order_type_2 = rows1[5][0]['position_action']['order_type']
                        try:
                            last_ind = back_price_1[back_price_1.index(cc) - 1][
                                'indicator_1' + '_' + rows1[5][0]['indicator_1_1']['setting']]
                        except:
                            continue
                        change = rows1[5][0]['indicator_1_1']['change']
                    else:
                        indicator3 = 'indicator_1' + '_' + rows1[5][1]['indicator_1_1']['setting']
                        order_type_2 = rows1[5][1]['position_action']['order_type']
                        try:
                            last_ind = back_price_1[back_price_1.index(cc) - 1][
                                'indicator_1' + '_' + rows1[5][1]['indicator_1_1']['setting']]
                        except:
                            continue
                        change = rows1[5][1]['indicator_1_1']['change']
                    if block_3_1(indicator3, cc, direction, change, last_ind):
                        print('Продолжение тренда закрытие 1')
                        vh_vih_stat = 1
                        # stat = 'vhod_vihod_1'
                        # block_id = block_id + ',3'
                        # break
                else:
                    if direction == 'long':
                        indicator6 = cc['indicator_1_' + rows1[5][0]['indicator_1_2']['setting']]
                        try:
                            last_ind = back_price_1[back_price_1.index(cc) - 1][
                                'indicator_1' + '_' + rows1[5][0]['indicator_1_2']['setting']]
                        except:
                            continue
                        change = rows1[5][0]['indicator_1_2']['change']
                    else:
                        indicator6 = cc['indicator_1_' + rows1[5][1]['indicator_1_2']['setting']]
                        try:
                            last_ind = back_price_1[back_price_1.index(cc) - 1][
                                'indicator_1' + '_' + rows1[5][1]['indicator_1_2']['setting']]
                        except:
                            continue
                        change = rows1[5][1]['indicator_1_2']['change']
                    if block_3_2(indicator6, direction, change, last_ind):
                        print('Продолжение тренда закрытие 2')
                        stat = 'close_2'
                        block_num = 5
                        block_id = block_id + ',5'
                        close_time_order = back_price_1[back_price_1.index(cc) + 1]['time']
                        vh_vih_stat = 0

                        proboi_line_proc = 0
                        proboi_stup = 0
                        old_proboi = 0
                        exit_price_price = False
            # продолжение тренда безубыток
            if block == '7_long' or block == '7_short':
                if direction == 'long':
                    order_type_2 = rows1[6][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[6][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 6):
                    print('Продолжение тренда безубыток')
                    block_num = 6
                    close_time_order = back_price_1[back_price_1.index(cc)+1]['time']
                    stat = 'close_1'
                    block_id = block_id + ',7'

                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break
            # продолжение тренда стоп
            if block == '8_long' or block == '8_short':
                if direction == 'long':
                    indicator9 = cc['indicator_1_' + rows1[7][0]['indicator_1']['setting']]
                    order_type_2 = rows1[7][0]['position_action']['order_type']
                    try:
                        last_ind = back_price_1[back_price_1.index(cc) - 1][
                            'indicator_1' + '_' + rows1[7][0]['indicator_1']['setting']]
                    except:
                        continue
                    change = rows1[7][0]['indicator_1']['change']
                else:
                    indicator9 = cc['indicator_1_' + rows1[7][1]['indicator_1']['setting']]
                    order_type_2 = rows1[7][1]['position_action']['order_type']
                    try:
                        last_ind = back_price_1[back_price_1.index(cc) - 1][
                            'indicator_1' + '_' + rows1[7][1]['indicator_1']['setting']]
                    except:
                        continue
                    change = rows1[7][1]['indicator_1']['change']
                if block_5(indicator9, direction, 7, change, last_ind):
                    print('Продолжение тренда стоп')
                    block_num = 7
                    close_time_order = back_price_1[back_price_1.index(cc)+1]['time']
                    stat = 'close_2'
                    block_id = block_id + ',8'

                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break
            # пробой уровня 1 ступень(без ступенек)
            if block == '10_long' or block == '10_short':
                if order[0] == 'long':
                    side = rows1[9][0]['indicator_2']['side']

                    try:
                        proboi = float(back_price_1[back_price_1.index(cc)-1]['indicator_2_' + rows1[9][0]['indicator_2']['setting'] + '-' +
                                          rows1[9][0]['indicator_2']['side']])
                    except:
                        proboi = 0
                else:
                    side = rows1[9][1]['indicator_2']['side']
                    try:
                        proboi = float(back_price_1[back_price_1.index(cc)-1]['indicator_2_' + rows1[9][1]['indicator_2']['setting'] + '-' +
                                          rows1[9][1]['indicator_2']['side']])
                    except:
                        proboi = 0
                # if direction == 'long':
                # order_type_2 = rows1[6][0]['position_action']['order_type']
                # else:
                # order_type_2 = rows1[6][1]['position_action']['order_type']
                if block_6_1(cc, proboi, direction, 9, side):
                    print('Пробой без ступенек ступень сработал')
                    block_num = 9
                    stat = 'close_open_2'
                    close_candle = block_6_1(cc, proboi, direction, 9, side)
                    close_time_order = cc['time']
                    block_id = block_id + ',10'

                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break
            # пробой уровня вторая ступень(со ступеньками)
            if block == '9_long' or block == '9_short':

                if order[0] == 'long':
                    side = rows1[8][0]['indicator_2']['side']
                    check = rows1[8][0]['indicator_2']['check']
                    try:
                        exit_price_price_main = rows1[8][0]['indicator_2']['exit_price_price']
                    except:
                        exit_price_price_main = 'no'
                    proc_value_2 = float(rows1[8][0]['indicator_2']['exit_price_percent'])
                    try:
                        proboi = float(back_price_1[back_price_1.index(cc)-1]['indicator_2_' + rows1[8][0]['indicator_2']['setting'] + '-' +
                                          rows1[8][0]['indicator_2']['side']])
                        if proboi_status == 0:
                            old_proboi = proboi
                    except:
                        proboi = 0
                    try:
                        new_breakdown_sum = int(rows1[8][0]['indicator_2']['new_breakdown_sum'])
                    except:
                        new_breakdown_sum = 1
                else:
                    side = rows1[8][1]['indicator_2']['side']
                    check = rows1[8][1]['indicator_2']['check']
                    try:
                        exit_price_price_main = rows1[8][1]['indicator_2']['exit_price_price']
                    except:
                        exit_price_price_main = 'no'
                    proc_value_2 = float(rows1[8][1]['indicator_2']['exit_price_percent'])
                    try:
                        proboi = float(back_price_1[back_price_1.index(cc)-1]['indicator_2_' + rows1[8][1]['indicator_2']['setting'] + '-' +
                                          rows1[8][1]['indicator_2']['side']])
                        if proboi_status == 0:
                            old_proboi = proboi
                    except:
                        proboi = 0
                    try:
                        new_breakdown_sum = int(rows1[8][1]['indicator_2']['new_breakdown_sum'])
                    except:
                        new_breakdown_sum = 1
                if proboi_stup >= new_breakdown_sum and proboi_line_proc >= proc_value_2:
                    print('Пробой со ступеньками сработал')
                    block_num = 8
                    stat = 'close_open_2'

                    proboi_status = 0
                    close_time_order = cc['time']
                    block_id = block_id + ',9'
                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break
                else:
                    if old_proboi != 0 and side == 'high' and proboi < old_proboi:
                        print('обнуление')
                        print('time == ' + str(cc['time']))
                        old_proboi = 0
                        proboi_stup = 0
                        proboi_line_proc = 0
                        proboi_status = 0
                        exit_price_price = False
                        continue
                    if old_proboi != 0 and side == 'low' and proboi > old_proboi:
                        print('обнуление')
                        print('time == ' + str(cc['time']))
                        old_proboi = 0
                        proboi_stup = 0
                        proboi_line_proc = 0
                        proboi_status = 0
                        exit_price_price = False
                        continue
                    if block_9_1(cc, old_proboi, proboi, direction, 8, side, exit_price_price):
                        print('Сработала одна ступень пробоя')
                        print('time == ' + str(cc['time']))
                        proboi_status = 1
                        proboi_line_proc = block_9_1(cc, old_proboi, proboi, direction, 8, side, exit_price_price)
                        print('proc == ' + str(proboi_line_proc))
                        print('proboi stup == ' + str(proboi_stup))
                        proboi_stup = proboi_stup + 1
                        if proboi_stup + 1 == new_breakdown_sum and exit_price_price_main == 'yes':
                            exit_price_price = True
                        if proboi_stup == new_breakdown_sum:
                            if check == 'low':
                                close_candle = float(proboi) - ((float(proboi) / 100) * proc_value_2)
                            if check == 'close':
                                if side == 'high':
                                    close_candle = float(proboi) + ((float(proboi) / 100) * proc_value_2)
                                if side == 'low':
                                    close_candle = float(proboi) - ((float(proboi) / 100) * proc_value_2)
                            if check == 'high':
                                close_candle = float(proboi) + ((float(proboi) / 100) * proc_value_2)
                        print(price)
                        continue


            # пробой уровня закрытие
            if block == '12_long' or block == '12_short':
                order_type_2 = rows1[11][0]['position_action']['order_type']
                check_stup = float(back_price_1[back_price_1.index(cc) - 1]['indicator_2_' + rows1[11][0]['indicator_2']['setting'] + '-' + rows1[11][0]['indicator_2']['side']])
                side_1 = rows1[11][0]['indicator_2']['side']
                check = rows1[11][0]['indicator_2']['check']
                exit_price_percent = rows1[11][0]['indicator_2']['exit_price_percent']
                if side_1 == 'high':
                    if check == 'close':
                        if float(cc['close']) >= check_stup:
                            price_value = (float(cc['close']) - check_stup)/(check_stup/100)
                            if float(price_value) > float(exit_price_percent):
                                print('Пробой закрытие')
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                proboi_line_proc = 0
                                proboi_stup = 0
                                old_proboi = 0
                                exit_price_price = False
                                break  # continue
                    if check == 'high':
                        if float(cc['high']) >= check_stup:
                            price_value = (float(cc['high']) - check_stup)/(check_stup/100)
                            if float(price_value) > float(exit_price_percent):
                                print('Пробой закрытие')
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                proboi_line_proc = 0
                                proboi_stup = 0
                                old_proboi = 0
                                exit_price_price = False
                                break  # continue
                    if check == 'low':
                        if float(cc['low']) >= check_stup:
                            price_value = (float(cc['low']) - check_stup)/(check_stup/100)
                            if float(price_value) > float(exit_price_percent):
                                print('Пробой закрытие')
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                proboi_line_proc = 0
                                proboi_stup = 0
                                old_proboi = 0
                                exit_price_price = False
                                break  # continue
                else:
                    if check == 'close':
                        if float(cc['close']) <= check_stup:
                            price_value = (check_stup - float(cc['close']))/(check_stup/100)
                            if float(price_value) > float(exit_price_percent):
                                print('Пробой закрытие')
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                proboi_line_proc = 0
                                proboi_stup = 0
                                old_proboi = 0
                                exit_price_price = False
                                break  # continue
                    if check == 'high':
                        if float(cc['high']) <= check_stup:
                            price_value = (check_stup - float(cc['high']))/(check_stup/100)
                            if float(price_value) > float(exit_price_percent):
                                print('Пробой закрытие')
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                proboi_line_proc = 0
                                proboi_stup = 0
                                old_proboi = 0
                                exit_price_price = False
                                break  # continue
                    if check == 'low':
                        if float(cc['low']) <= check_stup:
                            price_value = (check_stup - float(cc['low']))/(check_stup/100)
                            if float(price_value) > float(exit_price_percent):
                                print('Пробой закрытие')
                                block_num = 11
                                close_time_order = cc['time']
                                stat = 'close_1'
                                block_id = block_id + ',12'
                                check_stup = 0
                                price_value = 0
                                proboi_line_proc = 0
                                proboi_stup = 0
                                old_proboi = 0
                                exit_price_price = False
                                break  # continue


            # пробой уровня безубыток
            if block == '13_long' or block == '13_short':
                if direction == 'long':
                    order_type_2 = rows1[12][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[12][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 12):
                    print('пробой безубыток')
                    block_num = 12
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',13'
                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break
            # Пробой стоп
            if block == '14_long' or block == '14_short':
                if direction == 'long':
                    order_type_2 = rows1[13][0]['position_action']['order_type']
                else:
                    order_type_2 = rows1[13][1]['position_action']['order_type']

                if block_11(open_price_order, direction, 13):
                    print('Пробой стоп')
                    block_num = 13
                    close_time_order = cc['time']
                    stat = 'close_1'
                    block_id = block_id + ',14'

                    proboi_line_proc = 0
                    proboi_stup = 0
                    old_proboi = 0
                    exit_price_price = False
                    break
    if stat == 'close_2':
        if direction == 'long':
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
                close_time_position = back_price_1[back_price_1.index(cc) + 1]['time']
            else:
                close_time_position = close_time_order

            if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[
                2]:
                id_day = id_day - 1
            insert_stmt = (
                "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, balance, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            data = (
                id_day, order[0], lot, order_type_1, open_time_order, open_price_order, open_time_position,
                order_type_2,
                close_time_order, close_candle, close_time_position, fee, res, points_deal, money_deal, percent_deal,
                balance, money_day, percent_day, min_balance_percent, 0, 0, block_id)
            cursor.execute(insert_stmt, data)
            cnx.commit()
            if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[
                2]:
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
        else:
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
                close_time_position = back_price_1[back_price_1.index(cc) + 1]['time']
            else:
                close_time_position = close_time_order
            # print(close_time_position)
            if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[
                2]:
                id_day = id_day - 1
            insert_stmt = (
                "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, balance, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            data = (
                id_day, order[0], lot, order_type_1, open_time_order, open_price_order, open_time_position,
                order_type_2,
                close_time_order, close_candle, close_time_position, fee, res, points_deal, money_deal, percent_deal,
                balance, money_day, percent_day, min_balance_percent, 0, 0, block_id)
            cursor.execute(insert_stmt, data)
            cnx.commit()
            if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[
                2]:
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
            if '0' in activations[0][0].split(','):
                indicator1 = cc['indicator_1' + '_' + rows1[0][0]['indicator_1']['setting']]
                try:
                    last_ind = back_price_1[back_price_1.index(cc)-1]['indicator_1' + '_' + rows1[0][0]['indicator_1']['setting']]
                except:
                    pass
                    #continue
                direction = 'long'
                change = rows1[0][0]['indicator_1']['change']
                if block_1(indicator1, direction, change, last_ind):
                    print('Открытие ордера 1/2')
                    open_time_order = back_price_1[back_price_1.index(cc)+1]['time']
                    #print(open_time_order)
                    open_price_order = cc['close']
                    stat = 'open_1_1'
                    cancel_status = rows1[0][0]['position_action']['cancel'].split(',')
                    continue
            if '0' in activations[0][1].split(','):
                indicator1 = cc['indicator_1' + '_' + rows1[0][1]['indicator_1']['setting']]
                try:
                    last_ind = back_price_1[back_price_1.index(cc)-1]['indicator_1' + '_' + rows1[0][1]['indicator_1']['setting']]
                except:
                    pass
                    #continue
                direction = 'short'
                change = rows1[0][1]['indicator_1']['change']
                if block_1(indicator1, direction,change, last_ind):
                    print('Открытие ордера 1/2')
                    open_price_order = cc['close']
                    open_time_order = back_price_1[back_price_1.index(cc)+1]['time']
                    #print(open_time_order)
                    stat = 'open_1_1'
                    cancel_status = rows1[0][1]['position_action']['cancel'].split(',')
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
            #print(close_time_position)
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
            if '0' in activations[0][0].split(','):
                indicator1 = cc['indicator_1' + '_' + rows1[0][0]['indicator_1']['setting']]
                try:
                    last_ind = back_price_1[back_price_1.index(cc)-1]['indicator_1' + '_' + rows1[0][0]['indicator_1']['setting']]
                except:
                    pass
                    #continue
                direction = 'long'
                change = rows1[0][0]['indicator_1']['change']
                if block_1(indicator1, direction, change, last_ind):
                    print('Открытие ордера 1/2')
                    open_time_order = back_price_1[back_price_1.index(cc)+1]['time']
                    #print(open_time_order)
                    open_price_order = cc['close']
                    stat = 'open_1_1'
                    cancel_status = rows1[0][0]['position_action']['cancel'].split(',')
                    continue
            if '0' in activations[0][1].split(','):
                indicator1 = cc['indicator_1' + '_' + rows1[0][1]['indicator_1']['setting']]
                try:
                    last_ind = back_price_1[back_price_1.index(cc)-1]['indicator_1' + '_' + rows1[0][1]['indicator_1']['setting']]
                except:
                    pass
                    #continue
                direction = 'short'
                change = rows1[0][1]['indicator_1']['change']
                if block_1(indicator1, direction,change, last_ind):
                    print('Открытие ордера 1/2')
                    open_price_order = cc['close']
                    open_time_order = back_price_1[back_price_1.index(cc)+1]['time']
                    #print(open_time_order)
                    stat = 'open_1_1'
                    cancel_status = rows1[0][1]['position_action']['cancel'].split(',')
                    continue
    if stat == 'close_open_1':
        """if direction == 'long':
            change = rows1[4][0]['indicator_1_2']['change']
            indicator7 = cc['indicator_1_' + rows1[4][0]['indicator_1_2']['setting']]
        else:
            change = rows1[4][1]['indicator_1_2']['change']
            indicator7 = cc['indicator_1_' + rows1[2][1]['indicator_1_2']['setting']]"""
        if direction == 'long':
            try:
                price_indent1 = float(rows1[4][0]['position_action_1']['price_indent'])
            except:
                price_indent1 = 0
            order_type_2 = rows1[4][0]['position_action_1']['order_type']
        else:
            try:
                price_indent1 = float(rows1[4][1]['position_action_1']['price_indent'])
            except:
                price_indent1 = 0
            order_type_2 = rows1[4][1]['position_action_1']['order_type']
        price_old = cc['close']
        if price_indent1 != 0:
            close_candle = float(price_old) + (float(price_old) / 100) * price_indent1
        else:
            close_candle = float(price_old)
        order_type_1 = order[3]
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
        try:
            money_deal = (points_deal / close_candle) * (lot / price) - fee
        except Exception as e:
            print(e)
        money_day = money_day + money_deal
        percent_deal = (money_deal / balance) * 100
        balance = balance + money_deal
        percent_day = (money_day / start_balance) * 100
        min_percent_list.append(percent_day)
        min_balance_percent = min(min_percent_list)
        close_time_position = back_price_1[back_price_1.index(cc)+1]['time']
        if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[2]:
            id_day = id_day - 1
        insert_stmt = (
            "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, balance, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        data = (id_day, order[0], lot, order_type_1, open_time_order, open_price_order, open_time_position, order_type_2,close_time_order, close_candle, close_time_position, fee, res, points_deal, money_deal,percent_deal,balance, money_day, percent_day, min_balance_percent, 0, 0, block_id)
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
        ids = ids + 1
        # open second
        if direction == 'long':
            try:
                price_indent2 = float(rows1[4][0]['position_action_2']['price_indent'])
            except:
                price_indent2 = 0
            try:
                leverage2 = float(rows1[4][0]['position_action_2']['leverage'])
            except:
                leverage2 = 1
            direction = rows1[4][0]['position_action_2']['direction']
            order_type = rows1[4][0]['position_action_2']['order_type']
        else:
            try:
                price_indent2 = float(rows1[4][1]['position_action_2']['price_indent'])
            except:
                price_indent2 = 0
            try:
                leverage2 = float(rows1[4][1]['position_action_2']['leverage'])
            except:
                leverage2 = 1
            direction = rows1[4][1]['position_action_2']['direction']
            order_type = rows1[4][1]['position_action_2']['order_type']
        open_time_order = back_price_1[back_price_1.index(cc)+1]['time']
        open_time_position = back_price_1[back_price_1.index(cc)+1]['time']
        if direction == 'long':
            if price_indent2 != 0:
                price = float(cc['close']) + (float(cc['close']) / 100) * price_indent2
            else:
                price = float(cc['close'])
        else:
            if price_indent2 != 0:
                price = float(cc['close']) - (float(cc['close']) / 100) * price_indent2
            else:
                price = float(cc['close'])
        open_price_order = price
        lot = (start_balance * price) * leverage2
        lot = int(round(lot, -1))
        order.append(direction)
        order.append(price)
        order.append(lot)
        order.append(order_type)
        order.append(cc['time'])
        block_num = 4
        block_id1 = block_id.split(',')
        block_id2 = block_id1.pop(1)
        block_id = ''.join(block_id2)
        stat = 'open_2_2'


    if stat == 'close_open_2':
        stat_10 = [0, 0]
        block_num = 10
        if direction == 'long':
            try:
                price_indent1 = float(rows1[10][0]['position_action_2']['price_indent'])
                close_candle = float(close_candle) + (float(close_candle) / 100) * price_indent1
            except:
                close_candle = close_candle#float(cc['close']) + (float(cc['close']) / 100)
        else:
            try:
                price_indent1 = float(rows1[10][1]['position_action_2']['price_indent'])
                close_candle = float(close_candle) + (float(close_candle) / 100) * price_indent1
            except:
                close_candle = close_candle#float(cc['close']) + (float(cc['close']) / 100)
        order_type_1 = order[3]
        if direction == 'long':
            order_type_2 = rows1[10][0]['position_action_2']['order_type']
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
            order_type_2 = rows1[10][1]['position_action_2']['order_type']
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
        close_time_position = cc['time']
        if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[2]:
            id_day = id_day - 1
        insert_stmt = (
            "INSERT INTO back_positions(id_day, side, quantity, open_type_order, open_time_order, open_price_order, open_time_position, close_order_type, close_time_order, close_price_order, close_time_position, fee, result_deal, points_deal, money_deal, percent_deal, balance, money_day, percent_day, minimum_balance_percent, minimum_losses_percent, price_deviation, blocks_id)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        data = (
            id_day, order[0], lot, order_type_1, open_time_order, open_price_order, open_time_position, order_type_2,
            close_time_order, close_candle, close_time_position, fee, res, points_deal, money_deal,
            percent_deal,
            balance, money_day, percent_day, min_balance_percent, 0, 0, block_id)
        cursor.execute(insert_stmt, data)
        cnx.commit()
        if str(open_time_position).split(' ')[0].split('-')[2] != str(close_time_position).split(' ')[0].split('-')[2]:
            id_day = id_day + 1
        order = []
        close_candle1 = close_candle
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
        ids = ids + 1
        block_id = '11'
        if direction == 'long':
            try:
                price_indent2 = float(rows1[10][0]['position_action_2']['price_indent'])
            except:
                price_indent2 = 0
            leverage2 = rows1[10][0]['position_action_2']['leverage']
            direction = rows1[10][0]['position_action_2']['direction']
        else:
            try:
                price_indent2 = float(rows1[10][1]['position_action_2']['price_indent'])
            except:
                price_indent2 = 0
            leverage2 = rows1[10][1]['position_action_2']['leverage']
            direction = rows1[10][1]['position_action_2']['direction']
        if direction == 'long':
            if price_indent2 != 0:
                price = float(close_candle1) + (float(close_candle1) / 100) * price_indent2
            else:
                price = close_candle1#float(cc['close']) + (float(cc['close']) / 100)
            order_type = rows1[10][0]['position_action_2']['order_type']
        else:
            if price_indent2 != 0:
                price = float(close_candle1) - (float(close_candle1) / 100) * price_indent2
            else:
                price = close_candle1#float(cc['close']) - (float(cc['close']) / 100)
            order_type = rows1[10][1]['position_action_2']['order_type']

        open_time_order = cc['time']
        open_time_position = cc['time']
        open_price_order = price
        lot = (float(start_balance) * float(price)) * float(leverage2)
        lot = int(round(lot, -1))
        order.append(direction)
        order.append(price)
        order.append(lot)
        order.append(order_type)
        order.append(cc['time'])
        stat = 'open_2_2'
        print('open close_open')
        continue



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







