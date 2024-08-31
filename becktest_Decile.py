import argparse
import time
from joblib import Parallel, delayed
from main import run
from config import Backtest_Setting
from zipline.data import bundles
from utils import load_score
from zipline.utils.run_algo import load_extensions


def parse_args():
    """
    Parse command-line arguments to configure the backtest settings.
    """
    parser = argparse.ArgumentParser(description="Run the backtest with specified settings and parameter ranges.")

    # Basic settings
    parser.add_argument("--start-date", type=str, default="2008-01-10", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2022-12-31", help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--fiscal-start-year", type=int, default=2007, help="Fiscal start year")
    parser.add_argument("--fiscal-end-year", type=int, default=2022, help="Fiscal end year")

    # Data source and scoring settings
    parser.add_argument("--source", type=str, choices=["10kq", "mdna", "call transcripts"], default="mdna",
                        help="Source of the data")
    parser.add_argument("--score", type=str, default="datascore", help="Score used in the backtest")
    parser.add_argument("--primaryindex", type=str, choices=["S&P 500", "Russell 2000", "Russell 1000"],
                        default="S&P 500", help="Primary index (e.g., S&P 500, Russell 2000)")

    # Backtest strategy settings
    parser.add_argument("--exit-mode", type=str, choices=["Rebalance", "EventBased"], default="Rebalance",
                        help="Exit mode")
    parser.add_argument("--lookback", type=int, default=60, help="Lookback period for the score factor")
    parser.add_argument("--holding-days", type=int, default=80, help="Holding period in days")
    parser.add_argument("--lag", type=int, default=0, help="Signal lag days")
    parser.add_argument("--up-cutoff", type=float, default=0.5, help="Upper cutoff for rankings")
    parser.add_argument("--low-cutoff", type=float, default=0.5, help="Lower cutoff for rankings")
    parser.add_argument("--initial-cash", type=float, default=1000000, help="Initial cash for the backtest")
    parser.add_argument("--n-longs", type=int, default=1000, help="Number of long positions")
    parser.add_argument("--n-shorts", type=int, default=1000, help="Number of short positions")
    parser.add_argument("--do-short", action='store_true', help="Enable shorting in the strategy")
    parser.add_argument("--do-log", action='store_true', help="Enable logging during the backtest")
    parser.add_argument("--days-offset", type=int, default=10, help="Days offset for rebalancing schedule")

    args = parser.parse_args()
    return args


def create_backtest_settings(args):
    """
    Create a list of Backtest_Setting objects with varying cutoff values.
    """
    settings_list = []

    # Define different cutoff pairs for testing
    cutoffs = [[i / 10, i / 10 + 0.1] for i in range(0, 10)]

    for cutoff_pair in cutoffs:
        setting = Backtest_Setting(
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
            up_cutoff=cutoff_pair[0],
            low_cutoff=cutoff_pair[1],
            lag=args.lag,
            initial_cash=args.initial_cash,
            N_LONGS=args.n_longs,
            N_SHORTS=args.n_shorts,
            DoShort=args.do_short
        )
        settings_list.append(setting)

    return settings_list


def run_single_backtest(BacktestSetting, delay_interval):
    """
    Run a single backtest with a specified delay.
    """
    # Introduce a delay to avoid overwhelming resources
    time.sleep(delay_interval)

    # Load necessary extensions and data bundle
    load_extensions(default=True, extensions=[], strict=True, environ=None)
    bundle_data = bundles.load('quandl_custom_bundle')

    # Load scores and assets based on the backtest settings
    scores, assets = load_score(bundle_data, BacktestSetting)

    # Run the backtest
    run(BacktestSetting, scores, assets)


if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_args()

    # Create a list of parameter sets for backtesting
    param_list = create_backtest_settings(args)

    # Define delay interval between parallel backtests
    delay_interval = 10

    # Run backtests in parallel, introducing a delay for each backtest
    Parallel(n_jobs=-1)(delayed(run_single_backtest)(
        params, delay_interval * i) for i, params in enumerate(param_list))