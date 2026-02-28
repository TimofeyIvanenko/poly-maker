import poly_data.global_state as global_state
from poly_data.utils import get_sheet_df
import time
import poly_data.global_state as global_state

#sth here seems to be removing the position
def update_positions(avgOnly=False):
    pos_df = global_state.client.get_all_positions()

    for idx, row in pos_df.iterrows():
        asset = str(row['asset'])

        if asset in  global_state.positions:
            position = global_state.positions[asset].copy()
        else:
            position = {'size': 0, 'avgPrice': 0}

        position['avgPrice'] = row['avgPrice']

        if not avgOnly:
            position['size'] = row['size']
        else:
            any_pending = any(
                col in global_state.performing
                and isinstance(global_state.performing[col], set)
                and len(global_state.performing[col]) > 0
                for col in [f"{asset}_sell", f"{asset}_buy"]
            )

            if any_pending:
                pending_info = {col: list(global_state.performing[col]) for col in [f"{asset}_sell", f"{asset}_buy"] if col in global_state.performing and len(global_state.performing[col]) > 0}
                print(f"ALERT: Skipping update for {asset} because there are trades pending: {pending_info}")
            else:
                try:
                    old_size = position['size']
                except:
                    old_size = 0

                if asset in global_state.last_trade_update:
                    if time.time() - global_state.last_trade_update[asset] < 5:
                        print(f"Skipping update for {asset} because last trade update was less than 5 seconds ago")
                    else:
                        if old_size != row['size']:
                            print(f"No trades are pending. Updating position from {old_size} to {row['size']} and avgPrice to {row['avgPrice']} using API")
                        position['size'] = row['size']
                else:
                    if old_size != row['size']:
                        print(f"No trades are pending. Updating position from {old_size} to {row['size']} and avgPrice to {row['avgPrice']} using API")
                    position['size'] = row['size']
    
        global_state.positions[asset] = position

def get_position(token):
    token = str(token)
    if token in global_state.positions:
        return global_state.positions[token]
    else:
        return {'size': 0, 'avgPrice': 0}

def set_position(token, side, size, price, source='websocket'):
    token = str(token)
    size = float(size)
    price = float(price)

    global_state.last_trade_update[token] = time.time()
    
    if side.lower() == 'sell':
        size *= -1

    if token in global_state.positions:
        
        prev_price = global_state.positions[token]['avgPrice']
        prev_size = global_state.positions[token]['size']


        if size > 0:
            if prev_size == 0:
                # Starting a new position
                avgPrice_new = price
            else:
                # Buying more; update average price
                avgPrice_new = (prev_price * prev_size + price * size) / (prev_size + size)
        elif size < 0:
            # Selling; average price remains the same
            avgPrice_new = prev_price
        else:
            # No change in position
            avgPrice_new = prev_price


        global_state.positions[token]['size'] += size
        global_state.positions[token]['avgPrice'] = avgPrice_new
    else:
        global_state.positions[token] = {'size': size, 'avgPrice': price}

    print(f"Updated position from {source}, set to ", global_state.positions[token])

def update_orders():
    all_orders = global_state.client.get_all_orders()

    orders = {}

    if len(all_orders) > 0:
            for token in all_orders['asset_id'].unique():
                
                if token not in orders:
                    orders[str(token)] = {'buy': {'price': 0, 'size': 0}, 'sell': {'price': 0, 'size': 0}}

                curr_orders = all_orders[all_orders['asset_id'] == str(token)]
                
                if len(curr_orders) > 0:
                    sel_orders = {}
                    sel_orders['buy'] = curr_orders[curr_orders['side'] == 'BUY']
                    sel_orders['sell'] = curr_orders[curr_orders['side'] == 'SELL']

                    for type in ['buy', 'sell']:
                        curr = sel_orders[type]

                        if len(curr) > 1:
                            print("Multiple orders found, cancelling")
                            global_state.client.cancel_all_asset(token)
                            orders[str(token)] = {'buy': {'price': 0, 'size': 0}, 'sell': {'price': 0, 'size': 0}}
                        elif len(curr) == 1:
                            orders[str(token)][type]['price'] = float(curr.iloc[0]['price'])
                            orders[str(token)][type]['size'] = float(curr.iloc[0]['original_size'] - curr.iloc[0]['size_matched'])

    global_state.orders = orders

def get_order(token):
    token = str(token)
    if token in global_state.orders:

        if 'buy' not in global_state.orders[token]:
            global_state.orders[token]['buy'] = {'price': 0, 'size': 0}

        if 'sell' not in global_state.orders[token]:
            global_state.orders[token]['sell'] = {'price': 0, 'size': 0}

        return global_state.orders[token]
    else:
        return {'buy': {'price': 0, 'size': 0}, 'sell': {'price': 0, 'size': 0}}
    
def set_order(token, side, size, price):
    token = str(token)
    if token not in global_state.orders:
        global_state.orders[token] = {'buy': {'price': 0, 'size': 0}, 'sell': {'price': 0, 'size': 0}}
    global_state.orders[token][side] = {'price': float(price), 'size': float(size)}
    print("Updated order, set to ", global_state.orders[token])

    

def update_markets():
    received_df, received_params = get_sheet_df()

    if len(received_df) > 0:
        # Ensure multiplier column exists and fill NaN values with empty string
        if 'multiplier' not in received_df.columns:
            received_df['multiplier'] = ''
        else:
            received_df['multiplier'] = received_df['multiplier'].fillna('')

        # Cancel orders and merge positions for markets removed from the sheet
        if global_state.df is not None and global_state.client is not None:
            old_conditions = set(global_state.df['condition_id'].astype(str))
            new_conditions = set(received_df['condition_id'].astype(str))
            removed = old_conditions - new_conditions
            for condition_id in removed:
                print(f"Market {condition_id} removed from sheet — cancelling all orders")
                try:
                    global_state.client.cancel_all_market(condition_id)
                except Exception as e:
                    print(f"Error cancelling orders for removed market {condition_id}: {e}")

                # Auto-merge if both sides have position (risk-free profit)
                try:
                    mkt_row = global_state.df[global_state.df['condition_id'].astype(str) == condition_id]
                    if len(mkt_row) == 0:
                        continue
                    mkt_row = mkt_row.iloc[0]
                    pos_1 = global_state.client.get_position(mkt_row['token1'])[0]
                    pos_2 = global_state.client.get_position(mkt_row['token2'])[0]
                    amount_to_merge = min(pos_1, pos_2)
                    scaled_amt = amount_to_merge / 10**6
                    if scaled_amt > 20:
                        print(f"Auto-merging {scaled_amt} tokens for removed market {condition_id}")
                        global_state.client.merge_positions(amount_to_merge, condition_id, mkt_row['neg_risk'] == 'TRUE')
                        set_position(mkt_row['token1'], 'SELL', scaled_amt, 0, 'merge')
                        set_position(mkt_row['token2'], 'SELL', scaled_amt, 0, 'merge')

                    # Place sell orders for any remaining unmerged positions
                    neg_risk = mkt_row['neg_risk'] == 'TRUE'
                    for token_col in ['token1', 'token2']:
                        token_str = str(mkt_row[token_col])
                        remaining_pos = get_position(token_str)['size']
                        if remaining_pos >= 1:
                            try:
                                book = global_state.client.get_order_book(token_str)
                                bids = sorted(
                                    [(float(b.price), float(b.size)) for b in book.bids],
                                    reverse=True
                                )
                                best_bid = bids[0][0] if bids else None
                                if best_bid and best_bid >= 0.02:
                                    print(f"Auto-selling {remaining_pos} {token_col} at {best_bid} for removed market {condition_id}")
                                    global_state.client.create_order(token_str, 'SELL', best_bid, remaining_pos, neg_risk)
                                    set_order(token_str, 'sell', remaining_pos, best_bid)
                                else:
                                    print(f"WARNING: {token_col} for {condition_id} has {remaining_pos} shares but best_bid={best_bid} — skipping auto-sell")
                            except Exception as e:
                                print(f"Error auto-selling {token_col} for removed market {condition_id}: {e}")
                except Exception as e:
                    print(f"Error during merge for removed market {condition_id}: {e}")

        global_state.df, global_state.params = received_df.copy(), received_params


    for _, row in global_state.df.iterrows():
        for col in ['token1', 'token2']:
            row[col] = str(row[col])

        if row['token1'] not in global_state.all_tokens:
            global_state.all_tokens.append(row['token1'])

        if row['token1'] not in global_state.REVERSE_TOKENS:
            global_state.REVERSE_TOKENS[row['token1']] = row['token2']

        if row['token2'] not in global_state.REVERSE_TOKENS:
            global_state.REVERSE_TOKENS[row['token2']] = row['token1']

        for col2 in [f"{row['token1']}_buy", f"{row['token1']}_sell", f"{row['token2']}_buy", f"{row['token2']}_sell"]:
            if col2 not in global_state.performing:
                global_state.performing[col2] = set()