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
    Parse command-line arguments to configure the backtest settings and parameter ranges.
    """
    parser = argparse.ArgumentParser(description="Run the backtest with specified settings and parameter ranges.")

    # Basic settings
    parser.add_argument("--start-date", type=str, default="2008-01-10", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2022-12-31", help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--fiscal-start-year", type=int, default=2007, help="Fiscal start year")
    parser.add_argument("--fiscal-end-year", type=int, default=2022, help="Fiscal end year")
    parser.add_argument("--source", type=str, choices=["10kq", "mdna", "call transcripts"], default="mdna",
                        help="Source of the data")
    parser.add_argument("--score", type=str, default="datascore", help="Score used in the backtest")
    parser.add_argument("--primaryindex", type=str, choices=["S&P 500", "Russell 2000", "Russell 1000"],
                        default="S&P 500", help="Primary index (e.g., S&P 500, Russell 2000)")
    parser.add_argument("--exit-mode", type=str, choices=["Rebalance", "EventBased"], default="Rebalance",
                        help="Exit mode")
    parser.add_argument("--up-cutoff", type=float, default=0.5, help="Upper cutoff for rankings")
    parser.add_argument("--low-cutoff", type=float, default=0.5, help="Lower cutoff for rankings")
    parser.add_argument("--initial-cash", type=float, default=1000000, help="Initial cash for the backtest")

    # Parameter ranges for backtest variations
    parser.add_argument("--lookback-options", type=int, nargs='+', default=[60],
                        help="Lookback period options (multiple values)")
    parser.add_argument("--holdingdays-options", type=int, nargs='+', default=[80],
                        help="Holding days options (multiple values)")
    parser.add_argument("--lag", type=int, nargs='+', default=[0], help="Signal lag days options (multiple values)")
    parser.add_argument("--days-offset", type=int, nargs='+', default=[0, 5, 10, 15],
                        help="Days offset for rebalancing")
    parser.add_argument("--n-longs-options", type=int, nargs='+', default=[1000],
                        help="Number of long positions options (multiple values)")
    parser.add_argument("--n-shorts-options", type=int, nargs='+', default=[1000],
                        help="Number of short positions options (multiple values)")
    parser.add_argument("--do-short", action='store_true', help="Enable shorting in the strategy")
    parser.add_argument("--do-log", action='store_true', help="Enable logging during the backtest")

    args = parser.parse_args()
    return args


def create_backtest_settings(args):
    """
    Create a list of Backtest_Setting objects with all combinations of provided parameters.
    """
    settings_list = []

    # Extract parameter options from args
    lookback_options = args.lookback_options
    holdingdays_options = args.holdingdays_options
    N_LONGS_options = args.n_longs_options
    N_SHORTS_options = args.n_shorts_options
    lag_options = args.lag
    days_offsets = args.days_offset

    # Create a Backtest_Setting object for each combination of parameters
    for lookback in lookback_options:
        for holdingdays in holdingdays_options:
            for N_LONGS in N_LONGS_options:
                for N_SHORTS in N_SHORTS_options:
                    for lag in lag_options:
                        for days_offset in days_offsets:
                            setting = Backtest_Setting(
                                start_date=args.start_date,
                                end_date=args.end_date,
                                fiscal_start_year=args.fiscal_start_year,
                                fiscal_end_year=args.fiscal_end_year,
                                source=args.source,
                                score=args.score,
                                primaryindex=args.primaryindex,
                                ExitMode=args.exit_mode,
                                lookback=lookback,
                                holdingdays=holdingdays,
                                up_cutoff=args.up_cutoff,
                                low_cutoff=args.low_cutoff,
                                lag=lag,
                                initial_cash=args.initial_cash,
                                N_LONGS=N_LONGS,
                                N_SHORTS=N_SHORTS,
                                DoShort=args.do_short,
                                DoLog=args.do_log,
                                days_offset=days_offset
                            )
                            settings_list.append(setting)

    return settings_list


def run_single_backtest(BacktestSetting, delay_interval):
    """
    Run a single backtest with a specified delay interval to avoid resource contention.
    """
    # Introduce the delay before starting the backtest
    time.sleep(delay_interval)

    # Load necessary extensions and data bundle
    load_extensions(default=True, extensions=[], strict=True, environ=None)
    bundle_data = bundles.load('quandl_custom_bundle')

    # Load scores and assets for the given settings
    scores, assets = load_score(bundle_data, BacktestSetting)

    # Execute the backtest
    run(BacktestSetting, scores, assets)


if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_args()

    # Create a list of backtest settings based on the provided parameter ranges
    param_list = create_backtest_settings(args)

    # Define delay interval between backtests to avoid resource contention
    delay_interval = 10

    # Run the backtests in parallel, introducing a delay for each backtest
    Parallel(n_jobs=-1)(delayed(run_single_backtest)(
        params, delay_interval * i) for i, params in enumerate(param_list))