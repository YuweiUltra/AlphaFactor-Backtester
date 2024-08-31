import pandas as pd
from pathlib import Path
import warnings
import numpy as np
from tqdm import tqdm

warnings.filterwarnings('ignore')

zipline_root = '~/.zipline'
custom_data_path = Path(zipline_root, 'custom_data')

hist_data_name = "QUOTEMEDIA_PRICES_247f636d651d8ef83d8ca1e756cf5ee4.csv"
ticker_data_name = 'QUOTEMEDIA_TICKERS_6d75499fefd916e54334b292986eafcc.csv'
idx = pd.IndexSlice


def load_equities():
    return pd.read_hdf(custom_data_path / 'quandl.h5', 'equities')


def ticker_generator():
    """
    Lazily return (sid, ticker) tuple
    """
    return (v for v in load_equities().values)


def data_generator():
    for sid, symbol, exchange_, asset_name in ticker_generator():
        df = pd.read_hdf(custom_data_path / 'quandl.h5', 'prices/{}'.format(sid))
        df.columns = ['open', 'high', 'low', 'close', 'volume']
        start_date = df.index[0]
        end_date = df.index[-1]

        first_traded = start_date.date()
        auto_close_date = end_date + pd.Timedelta(days=1)
        exchange = 'NYSE'

        yield (sid, df), symbol, asset_name, start_date, end_date, first_traded, auto_close_date, exchange


def metadata_frame():
    dtype = [
        ('symbol', 'object'),
        ('asset_name', 'object'),
        ('start_date', 'datetime64[ns]'),
        ('end_date', 'datetime64[ns]'),
        ('first_traded', 'datetime64[ns]'),
        ('auto_close_date', 'datetime64[ns]'),
        ('exchange', 'object')]
    return pd.DataFrame(np.empty(len(load_equities()), dtype=dtype))


def quandl_to_bundle(interval='1d'):
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir
               ):
        metadata = metadata_frame()

        def daily_data_generator():
            return (sid_df for (sid_df, *metadata.iloc[sid_df[0]]) in data_generator())

        daily_bar_writer.write(daily_data_generator(), show_progress=True)
        metadata.dropna(inplace=True)
        exchange = {'exchange': 'NYSE', 'canonical_name': 'NYSE', 'country_code': 'US'}
        exchange_df = pd.DataFrame(exchange, index=[0])
        asset_db_writer.write(equities=metadata, exchanges=exchange_df)

        '''
        Since we used adjusted data here, no need to add splits and dividends
            splits = pd.read_hdf(custom_data_path / 'quandl.h5', 'splits')
            splits['sid'] = splits['sid'].astype(np.int64)
            dividends = pd.read_hdf(custom_data_path / 'quandl.h5', 'dividends')
            dividends['sid'] = dividends['sid'].astype(np.int64)
            adjustment_writer.write(splits=splits,dividends=dividends)
        '''
        adjustment_writer.write()

    return ingest
