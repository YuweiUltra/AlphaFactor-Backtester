# AlphaFactor-Backtester
### **Summer 2024 Project at UChicago in collaboration with Exponential Tech and Deception &amp; Truth Analysis. This tool backtests fundamental factors using the Zipline-Reloaded framework, enabling robust analysis and evaluation of investment strategies.**

---

### Required install
At first, two additional packages, zipline and pyfolio are required to clone
1. clone pyfolio-reloaded for analysing and plotting the backtest results [here is a fetch to output the plots in a entire html file ](https://github.com/YuweiUltra/pyfolio-reloaded)
2. clone zipline-reloaded for backtesting trading [link](https://github.com/YuweiUltra/zipline-reloaded)
3. clone this AlphaFactor-Backtester repo

### project Structure
.\
├── **`.zipline `**:                  # Configuration and data for Zipline\
├── **`AlphaFactor-Backtester`**:      # Main project directory\
├── **`pyfolio-reloaded `**:           # Pyfolio library for analyzing performance and visualization\
└── **`zipline-reloaded  `**:          # Zipline reloaded library for backtesting trading 

if there is no .zipline/ in your root path, run this in terminal
```
zipline ingest
```
