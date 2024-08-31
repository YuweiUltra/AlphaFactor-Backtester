# AlphaFactor-Backtester
### **Summer 2024 Project at UChicago in collaboration with Exponential Tech and Deception &amp; Truth Analysis. This tool backtests fundamental factors using the Zipline-Reloaded framework, enabling robust analysis and evaluation of investment strategies.**

---

### Required Installations
To get started, you need to install two additional packages: zipline-reloaded and pyfolio-reloaded.

1.	Clone pyfolio-reloaded for analyzing and plotting backtest results. You can find it [here](https://github.com/YuweiUltra/pyfolio-reloaded). This version includes functionality to output plots in an HTML file.
2.	Clone zipline-reloaded for backtesting trading strategies. You can find it [here](https://github.com/YuweiUltra/zipline-reloaded).
3.	Clone this AlphaFactor-Backtester repository.

### project Structure
```
.
├── .zipline/                  # Configuration and data for Zipline
├── AlphaFactor-Backtester/    # Main project directory
├── pyfolio-reloaded/          # Pyfolio library for performance analysis and visualization
└── zipline-reloaded/          # Zipline Reloaded library for backtesting trading
```
If .zipline/ does not exist in your root directory, run the following command in your terminal:
```
zipline ingest
```

### Creating a Custom Bundle
Zipline uses bundle data to speed up backtests. Therefore, we need to create and ingest our custom bundle data.

In this project, I used Quandl EOD data downloaded from Nasdaq Data Link. First, I ran a quandl_preprocessing script to store the data in a more readable quandl.h5 file with a unique sid for each ticker. (This step is optional.)

Next, create quandl_custom_bundle.py (the name can vary) and extension.py in the .zipline/ directory.

Your .zipline/ directory should look like this:
```
~/.zipline/
│
├── quandl_custom_bundle.py
└── extension.py
```
Once everything is set up correctly, run the following command to ingest the custom bundle:
```
zipline ingest -b quandl_custom_bundle
```
After successfully ingesting the data, the directory structure will look like this:
```
~/.zipline/
│
├── data
│   └── quandl_custom_bundle
│       └── 2024-08-15T07:00:21.328524
├── quandl_custom_bundle.py
└── extension.py
```

### Starting the Backtest
You can start the backtest by running main.py with terminal arguments for parsing parameters, or you can directly run one of the following scripts:

	•	backtest_DollarNeutral.py
	•	backtest_Parallelize.py
	•	backtest_Decile.py
