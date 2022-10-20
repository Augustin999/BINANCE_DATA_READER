# BINANCE DATA READER

This repository allows to pull historical OHLCV data from the Binance API for the pairs and timeframes defined in the *config.py* file. Simply install the *requirents.txt* file, and run ```python main.py``` in your cmd and let the code works for you. 

Note that:
- The code loads data since the year Binance was created (ie: 2017). Running the command line for the first time will load and save the whole data since that date. Running the same command line will look for existing stored data files and update them, adding the most recent and missing OHLCV candles.
- Depending on the timeframes and the number of pairs provided in the *config.py* file, the code may be running for several minutes (or hours).
- The code is built to create a *data* folder located in the parent directory of the current working directory.