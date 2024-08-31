import pandas as pd
from pathlib import Path
import warnings
from tqdm import tqdm
from zipline.utils.calendar_utils import get_calendar
from joblib import Parallel, delayed

warnings.filterwarnings('ignore')

zipline_root = '~/.zipline'
custom_data_path = Path(zipline_root, 'custom_data')

hist_data_name = "QUOTEMEDIA_PRICES_247f636d651d8ef83d8ca1e756cf5ee4.csv"
ticker_data_name = 'QUOTEMEDIA_TICKERS_6d75499fefd916e54334b292986eafcc.csv'
idx = pd.IndexSlice


def load_prices():
    start_date = '2008-01-10'
    end_date = '2022-12-31'

    df = pd.read_csv(custom_data_path / hist_data_name)
    print(len(df.ticker.unique()))
    df.date = pd.to_datetime(df.date)
    df = df.set_index(['ticker', 'date']).sort_index(level=0)

    nyse_calendar = get_calendar('NYSE')
    trading_days = nyse_calendar.sessions_in_range(start=start_date, end=end_date)
    df = df[df.index.get_level_values('date').isin(trading_days)]

    def reindex_trading_days(df, trading_days):
        def reindex_group(group):
            group = group.droplevel('ticker')
            return group.reindex(trading_days, method='ffill')
        new_df = df.groupby(level='ticker').apply(reindex_group)

        return new_df

    # Apply the reindexing function
    df = reindex_trading_days(df, trading_days)
    df.index.set_names(['ticker', 'date'], inplace=True)

    # Fill the remaining NaN values
    df = df.fillna(method='ffill').fillna(method='bfill')
    print(len(df.index.get_level_values('ticker').unique()))

    return (df.loc[idx[:, start_date:end_date], :]
            .unstack('ticker')
            .sort_index()
            .tz_localize('UTC')
            .stack('ticker')
            .swaplevel())


def load_symbols(tickers):
    df = pd.read_csv(custom_data_path / ticker_data_name)
    return (df[df.ticker.isin(tickers)]
            .reset_index(drop=True)
            .reset_index()
            .rename(columns={'index': 'sid'}))

"""
def create_split_table(data):
    df = pd.DataFrame(columns=['sid', 'effective_date', 'ratio'], data=data).dropna()
    df['ratio'] = pd.to_numeric(df['ratio'], errors='coerce')
    df['sid'] = df['sid'].astype(int)

    with pd.HDFStore(custom_data_path / 'quandl.h5') as store:
        store.put('splits', df, format='t')


def create_dividend_table(data):
    df = pd.DataFrame(columns=['sid', 'amount', 'ex_date'], data=data).dropna()
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df=df[(df.amount>0.001)&(df.amount<1000)]
    df['sid'] = df['sid'].astype(int)
    df['record_date'] = pd.NaT
    df['declared_date'] = pd.NaT
    df['pay_date'] = pd.NaT

    with pd.HDFStore(custom_data_path / 'quandl.h5') as store:
        store.put('dividends', df[['sid', 'ex_date', 'record_date', 'declared_date', 'pay_date', 'amount']], format='t')
"""

if __name__ == '__main__':
    prices = load_prices()
    print(prices.info(null_counts=True))
    tickers = prices.index.unique('ticker')

    symbols = load_symbols(tickers)
    print(symbols.info(null_counts=True))
    print(">>> storing equities")

    symbols.to_hdf(custom_data_path / 'quandl.h5', 'equities', format='t')

    dates = prices.index.unique('date')
    start_date = dates.min()
    end_date = dates.max()

    sid_map = symbols[['ticker', 'sid']].set_index('ticker')['sid'].to_dict()
    temp = prices.reset_index()
    prices['sid'] = temp['ticker'].map(sid_map).values

    print(">>> storing prices")
    # Ensure custom_data_path is defined

    def save_prices_to_hdf(sid, symbol, prices, custom_data_path):
        p = prices.loc[symbol, ['adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_volume']]
        p.index = p.index.values

        # Optimize data types
        p = p.astype({
            'adj_open': 'float32',
            'adj_high': 'float32',
            'adj_low': 'float32',
            'adj_close': 'float32',
            'adj_volume': 'int32'
        })

        # Save to HDF5
        p.to_hdf(custom_data_path / 'quandl.h5', 'prices/{}'.format(sid), format='t')


    # Parallel processing
    for sid, symbol in tqdm(symbols.set_index('sid').ticker.items(),
                            total=len(symbols),
                            bar_format='{l_bar}{bar} | {n_fmt}/{total_fmt}'):
        save_prices_to_hdf(sid, symbol, prices, custom_data_path)


    # Display the HDF5 file structure
    with pd.HDFStore(custom_data_path / 'quandl.h5') as store:
        print(store.info())
