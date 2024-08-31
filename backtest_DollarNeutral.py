from main import *

def parse_args():
    parser = argparse.ArgumentParser(description="Run the backtest with specified settings")

    # Add arguments for each attribute in Backtest_Setting
    parser.add_argument("--start-date", type=str, default="2008-01-10", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2022-12-31", help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--fiscal-start-year", type=int, default=2007, help="Fiscal start year")
    parser.add_argument("--fiscal-end-year", type=int, default=2022, help="Fiscal end year")
    parser.add_argument("--source", type=str, choices=["10kq", "mdna", "call transcripts"], default="mdna",
                        help="Source of the data")  # "2008-01-10" for "call transcripts"
    parser.add_argument("--score", type=str, default="datascore", help="Score used in the backtest")
    parser.add_argument("--primaryindex", type=str, choices=["S&P 500", "Russell 2000", "Russell 1000"],
                        default="Russell 2000",
                        help="Primary index (e.g., S&P 500, Russell 2000)")
    parser.add_argument("--exit-mode", type=str, choices=["Rebalance", "EventBased"], default="Rebalance",
                        help="Exit mode")
    parser.add_argument("--lookback", type=int, default=60, help="Lookback period for the score factor")
    parser.add_argument("--holding-days", type=int, default=80, help="Holding period in days")
    parser.add_argument("--lag", type=int, default=0, help="signal lag days")
    parser.add_argument("--up-cutoff", type=float, default=0.9, help="Upper cutoff for rankings")
    parser.add_argument("--low-cutoff", type=float, default=1, help="Lower cutoff for rankings")
    parser.add_argument("--short-up-cutoff", type=float, default=0, help="Upper cutoff for rankings")
    parser.add_argument("--short-low-cutoff", type=float, default=0.1, help="Lower cutoff for rankings")
    parser.add_argument("--initial-cash", type=float, default=1000000, help="Initial cash for the backtest")
    parser.add_argument("--n-longs", type=int, default=1000, help="Number of long positions")
    parser.add_argument("--n-shorts", type=int, default=1000, help="Number of short positions")
    parser.add_argument("--do-short", action='store_true', help="Enable shorting in the strategy")
    parser.add_argument("--do-log", action='store_true', help="Enable logging during the backtest")
    parser.add_argument("--days-offset", type=int, default=10, help="Number of short positions")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
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
        short_up_cutoff=args.short_up_cutoff,
        short_low_cutoff=args.short_low_cutoff,
    )

    load_extensions(default=True, extensions=[], strict=True, environ=None)
    bundle_data = bundles.load('quandl_custom_bundle')
    scores, assets = load_score(bundle_data, BacktestSetting)
    run(BacktestSetting, scores, assets)
