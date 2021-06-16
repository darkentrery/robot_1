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
