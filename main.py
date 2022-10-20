import pandas as pd
import time
import os

import exchange, utils, config


binance_creation = pd.Timestamp("2017-01-01 00:00:00")



def load_whole_history(pair: str, timeframe: str):
    now = pd.Timestamp(int(round(time.time() * 1000, 0)), unit='ms')
    ohlcv = exchange.most_recent_market_data(
        pair=pair,
        timeframe=timeframe,
        n_candles=int(round((now - binance_creation) / pd.Timedelta(timeframe), 0))
    )
    ohlcv.to_csv(utils.file_path(pair, timeframe), sep=',', index=False)
    del ohlcv
    return


def update_history(pair: str, timeframe: str):
    last_line = utils.read_last_line(utils.file_path(pair, timeframe))
    new_start_time = pd.Timestamp(last_line.split(',')[1])
    now = pd.Timestamp(int(round(time.time() * 1000, 0)), unit='ms')
    ohlcv = exchange.most_recent_market_data(
        pair=pair,
        timeframe=timeframe,
        n_candles=int(round((now-new_start_time) / pd.Timedelta(timeframe), 0))
    )
    if ohlcv.shape[0] == 0:
        return
    for row in ohlcv.values.tolist():
        if pd.Timestamp(row[0]) == new_start_time:
            utils.append_lines(utils.file_path(pair, timeframe), [row])
            new_start_time = pd.Timestamp(row[1])
    return


if __name__ == '__main__':
    if not os.path.exists(utils.data_path()):
        os.mkdir(utils.data_path())
    # Loop through all pairs and timeframes to get historical data
    for pair in config.PAIRS:
        print(pair)
        # Check if pair folder already exists, otherwise create it
        if not os.path.exists(utils.data_path() / pair.upper()):
            os.mkdir(utils.data_path() / pair.upper())
        for timeframe in config.TIMEFRAMES:
            print('\t', timeframe)
            # if data file does NOT already exist, create it
            if not os.path.exists(utils.file_path(pair, timeframe)):
                load_whole_history(pair, timeframe)
            # else, update it
            else:
                update_history(pair, timeframe)

