import os
import pickle
import json
import pandas as pd
from tqdm import tqdm
import yfinance as yf
from unifier import unifier
from zipline.errors import SymbolNotFound

# Set up environment variables for the unifier API
unifier.user = "qrt"
unifier.token = "wLeSyZXo/amHh1THUE1ZV/loYeK7qutQXOsV674DGiI="
os.environ["UNIFIER_USER"] = unifier.user
os.environ["UNIFIER_TOKEN"] = unifier.token

# Define base file path for truth & deception data
base_truth_deception_filepath = "./D&T/concatenated_data_on_quater"


def get_truth_deception_data(start_year, end_year):
    """
    Load Truth & Deception data for the specified year range using the unifier API.
    The data is stored in ./D&T/ as pickle files.
    """
    concat_dict = dict()

    for curr_year in tqdm(range(start_year, end_year + 1)):
        truth_deception_filepath = base_truth_deception_filepath + f"_{curr_year}.pkl"
        if os.path.exists(truth_deception_filepath):
            # Load existing data if available
            with open(truth_deception_filepath, "rb") as f:
                curr_concat_dict = pickle.load(f)
        else:
            # Fetch new data from the unifier API
            kq_dict, mdna_dict, t_dict = dict(), dict(), dict()

            Quarters = [f"Q{n}" for n in [1, 2, 3, 4]]
            quarters = [f"{quarter}_{curr_year}" for quarter in Quarters]

            for q in quarters:
                # Load 10-K/Q data
                df_10 = unifier.get_dataframe(name="deception_and_truth_10kq_quarterly", key=q)
                df_10["source"] = "10kq"
                kq_dict[q] = df_10

                # Load MD&A data
                df_md = unifier.get_dataframe(name="deception_and_truth_mdna_quarterly", key=q)
                df_md["source"] = "mdna"
                mdna_dict[q] = df_md

                # Load call transcripts data
                df_t = unifier.get_dataframe(name="deception_and_truth_call_transcripts_quarterly", key=q)
                df_t["source"] = "call transcripts"
                t_dict[q] = df_t

            # Concatenate the data for each quarter
            curr_concat_dict = {}
            for key in kq_dict.keys():
                if key in mdna_dict:
                    curr_concat_dict[key] = pd.concat(
                        [kq_dict[key], mdna_dict[key], t_dict[key]], axis=0
                    )

            # Save the concatenated data to a pickle file
            with open(truth_deception_filepath, "wb") as f:
                pickle.dump(curr_concat_dict, f)

        # Update the main dictionary with the current year's data
        for key in curr_concat_dict:
            concat_dict[key] = curr_concat_dict[key]

    return concat_dict


def filter_truth_deception_data(BacktestSetting):
    """
    Filter Truth & Deception data according to the specified BacktestSetting parameters.
    """
    print(">>> Filtering truth and deception data ......")

    # Load the data for the specified fiscal years
    truth_deception_data = get_truth_deception_data(
        BacktestSetting.fiscal_start_year, BacktestSetting.fiscal_end_year
    )

    # Combine data from all quarters into a single DataFrame
    df = pd.concat(list(truth_deception_data.values()), axis=0)
    df = df[df.source == BacktestSetting.source]

    # Extract Fiscal Year and Filing Year from the data
    df['FiscalYear'] = df['fiscalperiod'].apply(lambda x: int(x.split('_')[1]))
    df['FilingYear'] = pd.to_datetime(df['filingdate']).dt.year

    # Step 1: Exclude all records for any Fiscal Year prior to 2008
    if BacktestSetting.source == 'call transcripts':
        df['scorepublishedyear'] = df['date'].apply(lambda x: int(x.split('-')[0]))
    else:
        df['scorepublishedyear'] = df['scorepublisheddate'].apply(lambda x: int(x.split('-')[0]))
    df = df[df['scorepublishedyear'] >= 2008]
    df = df[df['FiscalYear'] >= 2007]

    # Step 2: Drop earnings filing revisions
    df = df[abs(df['FilingYear'] - df['FiscalYear']) <= 1]

    # Step 3: Drop all filings after 2022
    df = df[df[['FiscalYear', 'FilingYear']].min(axis=1) < 2023]

    # Filter by primary index for non-call transcripts sources
    if not BacktestSetting.source == 'call transcripts':
        truth_deception_df = df[(df.primaryindex == BacktestSetting.primaryindex)]
    else:
        truth_deception_df = df

    # Pivot the data to have tickers as columns and dates as the index
    truth_deception_df.date = pd.to_datetime(truth_deception_df.date)
    truth_deception_df = truth_deception_df.pivot_table(
        index="date", columns="ticker", values=[BacktestSetting.score], aggfunc="mean"
    )
    truth_deception_df.columns = truth_deception_df.columns.droplevel(0)

    return truth_deception_df


def load_score(bundle, BacktestSetting):
    """
    Filter scores by tickers that have complete OHLCV data in the specified bundle.
    """
    scores = filter_truth_deception_data(BacktestSetting)
    tickers = scores.columns.unique().tolist()
    print(f'Total number of tickers for {BacktestSetting.primaryindex} is {len(tickers)}')

    found_assets = []
    found_tickers = []
    missing_tickers = []

    # Set the reference date for ticker lookup
    as_of_date = pd.Timestamp(BacktestSetting.start_date)

    for ticker in tickers:
        try:
            asset = bundle.asset_finder.lookup_symbol(ticker, as_of_date=as_of_date)
            found_assets.append(asset)
            found_tickers.append(ticker)
        except SymbolNotFound:
            missing_tickers.append(ticker)
            continue

    if missing_tickers:
        print(f"Missing tickers: {missing_tickers}")
    print(f'Total tickers found: {len(found_tickers)}')

    # Map tickers to asset IDs
    ticker_map = dict(zip(found_tickers, [asset.sid for asset in found_assets]))
    filtered_scores = scores[found_tickers]

    # Rename columns with asset IDs and localize to UTC timezone
    return (filtered_scores.rename(columns=ticker_map).tz_localize('UTC')), found_assets


def save_backtest_setting(settings, directory):
    """
    Save the BacktestSetting configuration to a JSON file.
    """
    settings_dict = {attr: getattr(settings, attr) for attr in dir(settings) if
                     not callable(getattr(settings, attr)) and not attr.startswith("__")}
    with open(os.path.join(directory, 'BacktestSetting.json'), 'w') as f:
        json.dump(settings_dict, f, indent=4)


def get_russell2000_data(start_date, end_date):
    """
    Retrieve Russell 2000 ETF data (IWM) and calculate daily returns.
    Data is stored in ./BenchmarkData/russell2000.csv.
    """
    file_path = './BenchmarkData/russell2000.csv'

    if not os.path.exists(file_path):
        spy = yf.download('IWM', start=start_date, end=end_date, auto_adjust=True)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Create the directory if it doesn't exist
        spy.to_csv(file_path)
    else:
        spy = pd.read_csv(file_path, index_col='Date', parse_dates=True)

    spy['Return'] = spy['Close'].pct_change()
    spy = spy.tz_localize('UTC')
    return spy['Return']


def get_russell1000_data(start_date, end_date):
    """
    Retrieve Russell 1000 ETF data (IWB) and calculate daily returns.
    Data is stored in ./BenchmarkData/russell1000.csv.
    """
    file_path = './BenchmarkData/russell1000.csv'

    if not os.path.exists(file_path):
        spy = yf.download('IWB', start=start_date, end=end_date, auto_adjust=True)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Create the directory if it doesn't exist
        spy.to_csv(file_path)
    else:
        spy = pd.read_csv(file_path, index_col='Date', parse_dates=True)

    spy['Return'] = spy['Close'].pct_change()
    spy = spy.tz_localize('UTC')
    return spy['Return']


def get_sp500_etf_data(start_date, end_date):
    """
    Retrieve S&P 500 ETF data (SPY) and calculate daily returns.
    Data is stored in ./BenchmarkData/sp500.csv.
    """
    file_path = './BenchmarkData/sp500.csv'

    if not os.path.exists(file_path):
        spy = yf.download('SPY', start=start_date, end=end_date, auto_adjust=True)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Create the directory if it doesn't exist
        spy.to_csv(file_path)
    else:
        spy = pd.read_csv(file_path, index_col='Date', parse_dates=True)

    spy['Return'] = spy['Close'].pct_change()
    spy = spy.tz_localize('UTC')
    return spy['Return']