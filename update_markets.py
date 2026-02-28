import time
import pandas as pd
from data_updater.trading_utils import get_clob_client
from data_updater.google_utils import get_spreadsheet
from data_updater.find_markets import get_sel_df, get_all_markets, get_all_results, get_markets, add_volatility_to_df
from gspread_dataframe import set_with_dataframe
import traceback

# Initialize global variables
spreadsheet = get_spreadsheet()
client = get_clob_client()

wk_all = spreadsheet.worksheet("All Markets")
wk_vol = spreadsheet.worksheet("Volatility Markets")

sel_df = get_sel_df(spreadsheet, "Selected Markets")

def update_sheet(data, worksheet):
    all_values = worksheet.get_all_values()
    existing_num_rows = len(all_values)
    existing_num_cols = len(all_values[0]) if all_values else 0

    num_rows, num_cols = data.shape
    max_rows = max(num_rows, existing_num_rows)
    max_cols = max(num_cols, existing_num_cols)

    # Create a DataFrame with the maximum size and fill it with empty strings
    padded_data = pd.DataFrame('', index=range(max_rows), columns=range(max_cols))

    # Update the padded DataFrame with the original data and its columns
    padded_data.iloc[:num_rows, :num_cols] = data.values
    padded_data.columns = list(data.columns) + [''] * (max_cols - num_cols)

    # Update the sheet with the padded DataFrame, including column headers
    set_with_dataframe(worksheet, padded_data, include_index=False, include_column_header=True, resize=True)

def sort_df(df):
    # Calculate the mean and standard deviation for each column
    mean_gm = df['gm_reward_per_100'].mean()
    std_gm = df['gm_reward_per_100'].std()
    
    mean_volatility = df['volatility_sum'].mean()
    std_volatility = df['volatility_sum'].std()
    
    # Standardize the columns
    df['std_gm_reward_per_100'] = (df['gm_reward_per_100'] - mean_gm) / std_gm
    df['std_volatility_sum'] = (df['volatility_sum'] - mean_volatility) / std_volatility
    
    # Create a composite score (higher is better for rewards, lower is better for volatility)
    df['composite_score'] = (
        df['std_gm_reward_per_100'] -
        df['std_volatility_sum']
    )

    # Sort by the composite score in descending order
    sorted_df = df.sort_values(by='composite_score', ascending=False)

    # Drop the intermediate columns used for calculation
    sorted_df = sorted_df.drop(columns=['std_gm_reward_per_100', 'std_volatility_sum', 'composite_score'])
    
    return sorted_df

REMOVE_REWARD_THRESHOLD = 0.5   # remove from Selected if reward drops below this
ADD_REWARD_THRESHOLD = 1.0      # add to Selected only if reward is above this
ADD_MIN_SIZE_MAX = 50           # add to Selected only if min_size <= this (budget constraint)
ADD_MAX_SPREAD = 0.15           # add to Selected only if current bid-ask spread <= this
ADD_MIN_BID = 0.05              # add to Selected only if best_bid >= this (avoid empty orderbooks)
ADD_MAX_ASK = 0.95              # add to Selected only if best_ask <= this (avoid empty orderbooks)
ADD_MIN_VOLATILITY = 0.5        # add to Selected only if volatility_sum >= this (avoid zero-volume markets)
ADD_MIN_DAILY_RATE = 10         # add to Selected only if rewards_daily_rate >= this (avoid low-pool markets)
TOP_N_MARKETS = 10              # max markets in Selected Markets
DEFAULT_TRADE_SIZE = 50         # fallback if min_size < this
DEFAULT_MAX_SIZE = 100          # 2 rounds of buying per market

def auto_update_selected_markets(new_df, volatility_df, sel_df, spreadsheet):
    """Auto-populate Selected Markets based on reward + volatility criteria.
    - Removes markets whose reward dropped below REMOVE_REWARD_THRESHOLD
    - Adds top candidates not already selected (sorted by composite score)
    - Preserves existing trade_size/max_size for already-selected markets
    """
    wk_sel = spreadsheet.worksheet("Selected Markets")

    # Candidates: low volatility + good reward + tradeable min_size + liquid orderbook + active market
    candidates = volatility_df[
        (volatility_df['volatility_sum'] >= ADD_MIN_VOLATILITY) &
        (volatility_df['gm_reward_per_100'] >= ADD_REWARD_THRESHOLD) &
        (volatility_df['rewards_daily_rate'] >= ADD_MIN_DAILY_RATE) &
        (volatility_df['min_size'] <= ADD_MIN_SIZE_MAX) &
        (volatility_df['min_size'] > 0) &
        (volatility_df['best_ask'] - volatility_df['best_bid'] <= ADD_MAX_SPREAD) &
        (volatility_df['best_bid'] >= ADD_MIN_BID) &
        (volatility_df['best_ask'] <= ADD_MAX_ASK)
    ].copy()

    try:
        candidates = sort_df(candidates).head(TOP_N_MARKETS)
    except Exception:
        # sort_df can fail with too few rows (std=0); fall back to simple sort
        candidates = candidates.sort_values('gm_reward_per_100', ascending=False).head(TOP_N_MARKETS)

    def make_rows(df_subset):
        """Build new_rows with trade_size = max(DEFAULT, min_size) and max_size = 2 * trade_size."""
        rows = df_subset[['question']].copy()
        min_sizes = df_subset['min_size'].values
        trade_sizes = [max(DEFAULT_TRADE_SIZE, int(ms)) for ms in min_sizes]
        rows['trade_size'] = trade_sizes
        rows['max_size'] = [max(DEFAULT_MAX_SIZE, 2 * ts) for ts in trade_sizes]
        rows['param_type'] = 'high'
        return rows

    if len(sel_df) == 0:
        # Cold start: no existing selection, add top candidates with defaults
        new_rows = make_rows(candidates)
        update_sheet(new_rows, wk_sel)
        print(f"Selected Markets (cold start): added {len(new_rows)} markets")
        return new_rows

    # Get current reward and liquidity for already-selected markets
    existing_with_rewards = sel_df.merge(
        new_df[['question', 'gm_reward_per_100', 'best_bid', 'best_ask', 'volatility_sum', 'rewards_daily_rate']], on='question', how='left'
    )
    # Keep markets with reward above threshold AND liquid orderbook
    existing_with_rewards['_spread'] = existing_with_rewards['best_ask'] - existing_with_rewards['best_bid']
    kept = existing_with_rewards[
        (existing_with_rewards['gm_reward_per_100'].fillna(0) >= REMOVE_REWARD_THRESHOLD) &
        (existing_with_rewards['rewards_daily_rate'].fillna(0) >= ADD_MIN_DAILY_RATE) &
        (existing_with_rewards['_spread'].fillna(1) <= ADD_MAX_SPREAD) &
        (existing_with_rewards['best_bid'].fillna(0) >= ADD_MIN_BID) &
        (existing_with_rewards['best_ask'].fillna(1) <= ADD_MAX_ASK) &
        (existing_with_rewards['volatility_sum'].fillna(0) >= ADD_MIN_VOLATILITY)
    ]
    kept = kept.drop(columns=['_spread', 'best_bid', 'best_ask', 'volatility_sum', 'rewards_daily_rate'])

    # If more than TOP_N, trim to best by reward
    if len(kept) > TOP_N_MARKETS:
        kept = kept.sort_values('gm_reward_per_100', ascending=False).head(TOP_N_MARKETS)

    kept = kept.drop(columns=['gm_reward_per_100'])
    removed_count = len(sel_df) - len(kept)

    # Add new candidates not already in kept, up to TOP_N total
    existing_questions = set(kept['question'])
    new_to_add = candidates[~candidates['question'].isin(existing_questions)]
    slots_available = max(0, TOP_N_MARKETS - len(kept))
    new_to_add = new_to_add.head(slots_available)

    new_rows = make_rows(new_to_add)

    combined = pd.concat([kept, new_rows], ignore_index=True).drop_duplicates('question')

    update_sheet(combined, wk_sel)
    print(f"Selected Markets updated: {len(kept)} kept, {len(new_rows)} added, {removed_count} removed")
    return combined


def fetch_and_process_data():
    global spreadsheet, client, wk_all, wk_vol, sel_df
    
    spreadsheet = get_spreadsheet()
    client = get_clob_client()

    wk_all = spreadsheet.worksheet("All Markets")
    wk_vol = spreadsheet.worksheet("Volatility Markets")
    wk_full = spreadsheet.worksheet("Full Markets")

    sel_df = get_sel_df(spreadsheet, "Selected Markets")


    all_df = get_all_markets(client)
    print("Got all Markets")

    all_results = get_all_results(all_df, client)
    print("Got all Results")
    m_data, all_markets = get_markets(all_results, sel_df, maker_reward=0.75)
    print("Got all orderbook")

    print(f'{pd.to_datetime("now")}: Fetched all markets data of length {len(all_markets)}.')
    new_df = add_volatility_to_df(all_markets)
    new_df['volatility_sum'] =  new_df['24_hour'] + new_df['7_day'] + new_df['14_day']
    
    new_df = new_df.sort_values('volatility_sum', ascending=True)
    new_df['volatilty/reward'] = ((new_df['gm_reward_per_100'] / new_df['volatility_sum']).round(2)).astype(str)

    new_df = new_df[['question', 'answer1', 'answer2', 'spread', 'rewards_daily_rate', 'gm_reward_per_100', 'sm_reward_per_100', 'bid_reward_per_100', 'ask_reward_per_100',  'volatility_sum', 'volatilty/reward', 'min_size', '1_hour', '3_hour', '6_hour', '12_hour', '24_hour', '7_day', '30_day',  
                     'best_bid', 'best_ask', 'volatility_price', 'max_spread', 'tick_size',  
                     'neg_risk',  'market_slug', 'token1', 'token2', 'condition_id']]

    
    volatility_df = new_df.copy()
    volatility_df = volatility_df[new_df['volatility_sum'] < 20]
    # volatility_df = sort_df(volatility_df)
    volatility_df = volatility_df.sort_values('gm_reward_per_100', ascending=False)
   
    new_df = new_df.sort_values('gm_reward_per_100', ascending=False)
    

    print(f'{pd.to_datetime("now")}: Fetched select market of length {len(new_df)}.')

    if len(new_df) > 50:
        update_sheet(new_df, wk_all)
        update_sheet(volatility_df, wk_vol)
        update_sheet(m_data, wk_full)
        result = auto_update_selected_markets(new_df, volatility_df, sel_df, spreadsheet)
        if result is not None:
            sel_df = result
    else:
        print(f'{pd.to_datetime("now")}: Not updating sheet because of length {len(new_df)}.')

if __name__ == "__main__":
    while True:
        try:
            fetch_and_process_data()
            time.sleep(60 * 30)  # Sleep for 30 minutes
        except Exception as e:
            traceback.print_exc()
            print(str(e))
