import argparse
import pandas as pd
from tqdm import tqdm
from collections import defaultdict
from datetime import datetime
import os
import logging
import pyfolio as pf

from config import Backtest_Setting
from CustomFactors import ScoreFactor, IsFilingDateFactor
from CustomCommission import PercentageCommissionModel
from utils import save_backtest_setting, load_score

from zipline import run_algorithm
from zipline.finance.order import ORDER_STATUS
from zipline.api import (attach_pipeline,
                         pipeline_output,
                         date_rules,
                         time_rules,
                         record,
                         schedule_function,
                         slippage,
                         set_slippage,
                         set_commission,
                         order_target,
                         order_target_percent,
                         cancel_order,
                         order_target_value)
from zipline.data import bundles
from zipline.utils.run_algo import load_extensions
from zipline.pipeline import Pipeline
from zipline.utils.calendar_utils import get_calendar
from zipline.pipeline.filters import StaticAssets


def initialize(context):
    """
    Called once at the start of the algorithm to set up the initial context.
    """
    global BacktestSetting
    global scores, assets

    # Show progress through trading sessions
    trading_calendar = get_calendar('NYSE')
    trading_sessions = trading_calendar.sessions_in_range(
        pd.Timestamp(BacktestSetting.start_date),
        pd.Timestamp(BacktestSetting.end_date)
    )
    context.dates_processed = tqdm(total=len(trading_sessions))

    # Initialize context attributes
    context.n_longs = BacktestSetting.N_LONGS
    context.n_shorts = BacktestSetting.N_SHORTS
    context.holding_period = defaultdict(int)
    context.longs = 0
    context.shorts = 0
    context.n_to_open = 0
    context.n_to_close = 0
    context.universe = assets

    # Set slippage and commission models
    set_slippage(slippage.FixedSlippage(spread=0.00))
    set_commission(PercentageCommissionModel(cost=0.0005))

    # Adjust scores DataFrame to align with the trading calendar
    scores = scores.shift(BacktestSetting.lag)  # Apply signal lag
    all_sessions = trading_calendar.sessions_in_range(start=scores.index.min().date(), end=scores.index.max().date())
    all_sessions = all_sessions.tz_localize('UTC')
    all_sessions_df = pd.DataFrame(index=all_sessions)
    scores_ = pd.merge(all_sessions_df, scores, left_index=True, right_index=True, how='left')
    ScoreFactor.scores = scores_.reindex(all_sessions).ffill(axis=0, limit=BacktestSetting.lookback).tz_convert('UTC')

    # Create and attach the pipeline with the score factor
    score_factor = ScoreFactor()
    # is_filing_date_factor = IsFilingDateFactor()

    # Exclude specific tickers if necessary
    tickers_to_remove = []  # Add tickers to remove if needed
    context.universe = [ticker for ticker in assets if ticker.symbol not in tickers_to_remove]

    # Create the pipeline
    pipe = Pipeline(
        columns={
            'score': score_factor,
            # 'isFilingDate': is_filing_date_factor,
        },
        screen=StaticAssets(context.universe)
    )
    attach_pipeline(pipe, 'score_pipeline')

    # Schedule rebalancing function
    schedule_function(rebalance,
                      date_rules.month_start(days_offset=BacktestSetting.days_offset),
                      time_rules.market_close())

    # Optionally schedule a function to record variables each day
    # schedule_function(record_vars,
    #                   date_rules.every_day(),
    #                   time_rules.market_close())


def before_trading_start(context, data):
    """
    Called every day before the market opens. Used to update context before trading begins.
    """
    global BacktestSetting
    context.dates_processed.update(1)

    if BacktestSetting.do_log:
        logging.info("-" * 80)
        logging.info(f"PROCESSING DATE: {data.current_dt}")
        logging.info("-" * 20 + "BEFORE TRADING TIME" + "-" * 20)
        logging.info(f"OPENED POSITION NUMS: {len(context.portfolio.positions)}")
        logging.info(f"POSITION TO CLOSE: {context.n_to_close}")
        logging.info(f"POSITION TO OPEN: {context.n_to_open}")


def my_function(context, data):
    """
    Custom function to determine which assets to trade based on the ranking of scores.
    """
    pipeline_data = pipeline_output('score_pipeline')
    latest_scores = pipeline_data['score']
    # is_filing_date = pipeline_data['isFilingDate']

    # Uncomment and modify the code below to apply additional constraints
    # prices = data.history(context.universe, 'price', 1, '1d').iloc[-1]
    # volumes = data.history(context.universe, 'volume', 1, '1d').iloc[-1]
    # opens = data.history(context.universe, 'open', 1, '1d').iloc[-1]
    # eligible_stocks = latest_scores[(volumes * opens >= BacktestSetting.initial_cash) & (prices >= 0.1) & (is_filing_date == 1)]

    eligible_stocks = latest_scores
    ranks = eligible_stocks.rank(pct=True)

    if BacktestSetting.up_cutoff >= BacktestSetting.low_cutoff:
        top_longs = ranks[ranks > BacktestSetting.up_cutoff].nlargest(context.n_longs)
        top_shorts = ranks[ranks < BacktestSetting.low_cutoff].nsmallest(context.n_shorts)
    else:
        # Used in decile backtesting
        top_longs = ranks[(ranks > BacktestSetting.up_cutoff) & (ranks < BacktestSetting.low_cutoff)].nlargest(
            context.n_longs)
        top_shorts = pd.DataFrame()

    assert len(top_longs) <= context.n_longs
    assert len(top_shorts) <= context.n_shorts

    # Store the trades to be executed in the context
    context.trades = {
        asset: (1 if asset in top_longs.index else (-1 if asset in top_shorts.index else 0))
        for asset in context.universe
    }


def rebalance(context, data):
    """
    Rebalance the portfolio based on the latest scores and context settings.
    """
    my_function(context, data)

    global BacktestSetting

    # Cancel open orders that haven't been executed
    for stock, orders in context.blotter.open_orders.items():
        for order in orders:
            if order.status in [ORDER_STATUS.OPEN, ORDER_STATUS.HELD]:
                cancel_order(order.id)

    trades = defaultdict(list)
    if BacktestSetting.ExitMode == 'Rebalance':
        for stock, trade in context.trades.items():
            if trade == 1:
                trades[1].append(stock)
            elif trade == -1 and BacktestSetting.DoShort:
                trades[-1].append(stock)
            elif trade == 0:
                trades[0].append(stock)

        context.longs = len(trades[1])
        context.shorts = len(trades[-1]) if BacktestSetting.DoShort else 0

        # Close all positions not in the current trade plan
        close_count = 0
        for stock in context.portfolio.positions:
            if stock not in trades[1] and stock not in trades[-1]:
                order_target_value(stock, 0)
                close_count += 1

        # Execute orders for long positions
        open_count = 0
        if context.longs > 0:
            target_percent = 1 / context.longs
            safe_pos_cash = 1
            target_value = safe_pos_cash * target_percent * context.portfolio.portfolio_value
            for stock in trades[1]:
                if data.can_trade(stock):
                    open_count += 1
                    order_target_value(stock, int(target_value))

        # Execute orders for short positions if shorting is enabled
        if BacktestSetting.DoShort and context.shorts > 0:
            target_percent = -1 / context.shorts
            safe_pos_cash = 1
            target_value = safe_pos_cash * target_percent * context.portfolio.portfolio_value
            for stock in trades[-1]:
                if data.can_trade(stock):
                    open_count += 1
                    order_target_value(stock, int(target_value))

        context.n_to_open = open_count
        context.n_to_close = close_count

    elif BacktestSetting.ExitMode == 'EventBased':
        """
        Event-based exit logic (not fully implemented).
        """
        for stock in context.universe:
            pos = context.portfolio.positions[stock]
            if pos.amount != 0:  # If a position exists
                context.holding_period[stock] += 1  # Increment holding period
                if context.holding_period[stock] >= BacktestSetting.holdingdays:
                    order_target(stock, 0)  # Close position
                    context.holding_period[stock] = 0  # Reset holding period
            elif context.trades.get(stock) == 1 and context.longs < context.n_longs:
                order_target_percent(stock, 1 / context.n_longs)
                context.holding_period[stock] = 0  # Reset holding period for newly opened position
                context.longs += 1  # Increment longs count

            elif BacktestSetting.DoShort and context.trades.get(stock) == -1 and context.shorts < context.n_shorts:
                order_target_percent(stock, -1 / context.n_shorts)
                context.holding_period[stock] = 0  # Reset holding period for newly opened position
                context.shorts += 1  # Increment shorts count


def record_vars(context, data):
    """
    Record and log key variables at the end of each trading day.
    """
    # Calculate leverage components
    long_exposure = sum(
        [pos.amount * data.current(asset, 'price') for asset, pos in context.portfolio.positions.items() if
         pos.amount > 0])
    short_exposure = sum(
        [abs(pos.amount) * data.current(asset, 'price') for asset, pos in context.portfolio.positions.items() if
         pos.amount < 0])
    total_exposure = long_exposure + short_exposure
    portfolio_value = context.portfolio.portfolio_value

    leverage = total_exposure / portfolio_value if portfolio_value != 0 else 0

    # Record variables for plotting
    record(leverage=leverage,
           longs=context.longs,
           shorts=context.shorts)

    if BacktestSetting.do_log:
        # Log detailed leverage and exposure information
        logging.info("-" * 20 + " AFTER TRADING TIME " + "-" * 20)
        logging.info(f"OPENED POSITION NUM: {len(context.portfolio.positions)}")
        logging.info(f"PORTFOLIO VALUE: {context.portfolio.portfolio_value}")
        logging.info(f"CASH BALANCE: {context.portfolio.cash}")
        logging.info(f"LEVERAGE: {leverage:.2f}")
        logging.info(f"LONG EXPOSURE: {long_exposure:.2f}")
        logging.info(f"SHORT EXPOSURE: {short_exposure:.2f}")
        logging.info(f"TOTAL EXPOSURE: {total_exposure:.2f}")


def run(Setting, scores_, assets_):
    """
    Main function to run the backtest with the provided settings, scores, and assets.
    """
    global BacktestSetting, assets, scores
    BacktestSetting = Setting
    assets = assets_
    scores = scores_

    # Create a directory to save the results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_dir = os.path.join('./plots/temp', timestamp)
    os.makedirs(results_dir, exist_ok=True)
    save_backtest_setting(BacktestSetting, results_dir)

    if BacktestSetting.do_log:
        # Set up logging if enabled
        log_file_path = os.path.join(results_dir, 'record.log')
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename=log_file_path,  # Log file path
            level=logging.INFO,  # Log level (INFO, DEBUG, WARNING, etc.)
            format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
            datefmt='%Y-%m-%d %H:%M:%S'  # Date format
        )

    # Run the algorithm
    start_date = pd.Timestamp(BacktestSetting.start_date)
    end_date = pd.Timestamp(BacktestSetting.end_date)
    results = run_algorithm(start=start_date,
                            end=end_date,
                            initialize=initialize,
                            before_trading_start=before_trading_start,
                            capital_base=BacktestSetting.initial_cash,
                            data_frequency='daily',
                            bundle='quandl_custom_bundle',
                            )

    # Extract performance metrics
    returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(results)

    # Save results to an HDF5 file
    results_file_path = os.path.join(results_dir, 'results.h5')
    with pd.HDFStore(results_file_path) as store:
        store['results'] = results
        store['returns'] = returns
        store['positions'] = positions
        store['transactions'] = transactions

    if BacktestSetting.do_log:
        logging.info(f"Results saved to {results_file_path}")


def parse_args():
    """
    Parse command-line arguments to configure the backtest settings.
    """
    parser = argparse.ArgumentParser(description="Run the backtest with specified settings")

    # Add arguments for each attribute in Backtest_Setting
    parser.add_argument("--start-date", type=str, default="2020-01-10", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2023-10-01", help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--fiscal-start-year", type=int, default=2007, help="Fiscal start year")
    parser.add_argument("--fiscal-end-year", type=int, default=2024, help="Fiscal end year")
    parser.add_argument("--source", type=str, choices=["10kq", "mdna", "call transcripts"], default="mdna",
                        help="Source of the data")  # "2008-01-10" for "call transcripts"
    parser.add_argument("--score", type=str, default="datascore", help="Score used in the backtest")
    parser.add_argument("--primaryindex", type=str, choices=["S&P 500", "Russell 2000", "Russell 1000"],
                        default="S&P 500", help="Primary index (e.g., S&P 500, Russell 2000)")
    parser.add_argument("--exit-mode", type=str, choices=["Rebalance", "EventBased"], default="Rebalance",
                        help="Exit mode")
    parser.add_argument("--lookback", type=int, default=60, help="Lookback period for the score factor")
    parser.add_argument("--holding-days", type=int, default=80, help="Holding period in days")
    parser.add_argument("--lag", type=int, default=0, help="Signal lag days")
    parser.add_argument("--up-cutoff", type=float, default=0.6, help="Upper cutoff for rankings")
    parser.add_argument("--low-cutoff", type=float, default=0.9, help="Lower cutoff for rankings")
    parser.add_argument("--initial-cash", type=float, default=1000000, help="Initial cash for the backtest")
    parser.add_argument("--n-longs", type=int, default=1000, help="Number of long positions")
    parser.add_argument("--n-shorts", type=int, default=1000, help="Number of short positions")
    parser.add_argument("--do-short", action='store_true', help="Enable shorting in the strategy")
    parser.add_argument("--do-log", action='store_true', help="Enable logging during the backtest")
    parser.add_argument("--days-offset", type=int, default=10, help="Offset for the rebalancing schedule")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_args()

    # Create BacktestSetting object with command-line arguments
    BacktestSetting = Backtest_Setting(
        start_date=args.start_date,
        end_date=args.end_date,
        fiscal_start_year=args.fiscal_start_year,
        fiscal_end_year=args.fiscal_end_year,
        source=args.source,
        score=args.score,
        primaryindex=args.primaryindex,
        ExitMode=args.exit_mode,
        lookback=args.lookback,
        holdingdays=args.holding_days,
        lag=args.lag,
        up_cutoff=args.up_cutoff,
        low_cutoff=args.low_cutoff,
        initial_cash=args.initial_cash,
        N_LONGS=args.n_longs,
        N_SHORTS=args.n_shorts,
        DoShort=args.do_short,
        DoLog=args.do_log,
        days_offset=args.days_offset,
    )

    # Load data bundle and score data
    load_extensions(default=True, extensions=[], strict=True, environ=None)
    bundle_data = bundles.load('quandl_custom_bundle')
    scores, assets = load_score(bundle_data, BacktestSetting)

    # Run the backtest
    run(BacktestSetting, scores, assets)