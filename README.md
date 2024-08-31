# AlphaFactor-Backtester
### **Summer 2024 Project at UChicago in collaboration with Exponential Tech and Deception &amp; Truth Analysis. This tool backtests fundamental factors using the Zipline-Reloaded framework, enabling robust analysis and evaluation of investment strategies.**

At first, two additional packages, zipline and pyfolio are required to clone
1. clone pyfolio-reloaded for analysing and plotting the backtest results [here is a fetch to output the plots in a entire html file ](https://github.com/YuweiUltra/pyfolio-reloaded)
2. clone zipline-reloaded (for backtest)
3. clone this AlphaFactor-Backtester repo

### project Structure
- **`.zipline/`**: Contains configuration files and datasets used by Zipline.(if there is no .zipline/ in your root path, run this in terminal
'''
zipline ingest
'''
- **`AlphaFactor-Backtester/`**: The main directory where the primary code and project files are located.
- **`pyfolio-reloaded/`**: A custom version or fork of Pyfolio, used for performance analysis and visualization.
- **`zipline-reloaded/`**: A custom version or fork of the Zipline backtesting library.
