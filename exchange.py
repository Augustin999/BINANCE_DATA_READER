# BINANCE SPOT MARKET DATA PROVIDER
# ---------------------------------

# GitHub: @Augustin999
# October 2022

import numpy as np
import pandas as pd
import requests
from urllib.parse import urlencode
import time





trading_type = 'spot'
base_url = "https://api.binance.com"


def dispatch_request(http_method: str):
    """ Prepare a request with given http method """
    session = requests.Session()
    session.headers.update({
        'Content-Type': 'application/json;charset=utf-8',
        'X-MBX-APIKEY': ""
    })
    response = {
        'GET': session.get,
        'DELETE': session.delete,
        'PUT': session.put,
        'POST': session.post,
    }.get(http_method, 'GET')
    return response


def send_public_request(url_path: str, payload={}):
    """
    Prepare and send an unsigned request.
    Use this function to obtain public market data
    """
    query_string = urlencode(payload, True)
    url = base_url + url_path
    if query_string:
        url = url + '?' + query_string
    response = dispatch_request('GET')(url=url)
    return response.json()


def server_time():
    """
    Test connectivity to the REST API and return server time.
    """
    url_path = "/api/v3/time"
    params = {}
    response = send_public_request(
        url_path=url_path,
        payload=params
    )
    return response['serverTime']


def current_price(pair: str):
    """
    Latest price for a pair or pairs.
    """
    url_path = "/api/v3/ticker/price"
    params = {'symbol': pair}
    response = send_public_request(
        url_path=url_path,
        payload=params
    )
    return float(response['price'])


def most_recent_market_data(pair: str, timeframe: str, n_candles: int):
    """
    Load the n_candles most recent ohlc candlesticks for the given pair and timeframe.
    """
    ohlc = []
    end_time = 0
    n_c = n_candles
    end_of_history = False
    while (n_c+10 > 0) and (not end_of_history):
        limit = np.min([1000, n_c+10])
        if end_time == 0:
            new_ohlc = klines(
                pair=pair,
                timeframe=timeframe,
                limit=limit
            )
        else:
            new_ohlc = klines(
                pair=pair,
                timeframe=timeframe,
                limit=limit,
                end_time=end_time
            )
        if len(new_ohlc) != 0:
            new_ohlc = pd.DataFrame(
                new_ohlc,
                columns=[
                    'open_time', 'open_price', 'high_price', 
                    'low_price', 'close_price', 'volume',
                    'close_time', 'quote_volume', 'n_trades', 
                    'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
                ]
            )
            ohlc.append(new_ohlc)
            end_time = pd.Timestamp(pd.to_datetime(new_ohlc['close_time'], unit='ms').min())
            end_time = int(end_time.timestamp() * 1000 + 1)
            n_c -= limit
        else:
            end_of_history = True
    ohlc = pd.concat(ohlc, axis=0)
    ohlc.sort_values(by='open_time', inplace=True)
    timedelta = pd.Timedelta(timeframe).seconds if timeframe != '1d' else 24*pd.Timedelta('1h').seconds
    ohlc['close_time'] = ohlc['open_time'] + 1000 * timedelta
    ohlc.drop_duplicates(inplace=True)
    ohlc.index = [i for i in range(ohlc.shape[0])]
    if pd.to_datetime(ohlc['close_time'], unit='ms').max() > pd.Timestamp(int(time.time()), unit='s'):
        ohlc = ohlc.iloc[:-1].copy()
    for col in ohlc.columns:
        ohlc[col] = pd.to_numeric(ohlc[col])
    ohlc['open_time'] = pd.to_datetime(ohlc['open_time'], unit='ms')
    ohlc['close_time'] = pd.to_datetime(ohlc['close_time'], unit='ms')
    ohlc = ohlc.drop('ignore', axis=1)
    ohlc = ohlc.drop('volume', axis=1)
    ohlc = ohlc.drop('n_trades', axis=1)
    ohlc = ohlc.drop('taker_buy_base_volume', axis=1)
    ohlc = ohlc.drop('taker_buy_quote_volume', axis=1)
    ohlc = ohlc[[
        'open_time', 'close_time', 
        'open_price', 'high_price', 
        'low_price', 'close_price', 
        'quote_volume'
    ]].copy()
    ohlc = ohlc.iloc[-n_candles:-1].copy()
    ohlc.index = [i for i in range(ohlc.shape[0])]
    return ohlc


def klines(pair: str, timeframe: str, **kwargs):
    """
    Kline/candlestick bars for a symbol.
    Klines are uniquely identified by their open time.
    """
    url_path = "/api/v3/klines"
    params = {'symbol': pair, 'interval': timeframe}
    for key, value in kwargs.items():
        if key == 'start_time':
            params['startTime'] = int(value)
        if key == 'end_time':
            params['endTime'] = int(value)
        if key == 'limit':
            params['limit'] = np.min([value, 1000])
    response = send_public_request(
        url_path=url_path,
        payload=params
    )
    return response


def get_aggregate_trade_list(pair: str, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
    trades = []
    stime = start_time
    finished = False
    while not finished:
        _trades = aggregate_trade_list(
            pair=pair,
            start_time=int(1000 * stime.timestamp()),
            end_time=int(1000 * (stime + pd.Timedelta('1h')).timestamp())
        )
        for trade in _trades:
            if pd.Timestamp(int(trade['T']), unit='ms') < end_time:
                stime = pd.Timestamp(int(trade['T']), unit='ms')
                trades.append(trade)
            else:
                finished = True
    trades = pd.DataFrame(trades)
    trades.columns = ['aggregate_trade_id', 'price', 'qty', 'first_trade_id', 'last_trade_id', 'time', 'buyer_maker', 'best_price_match']
    trades['time'] = pd.to_numeric(trades['time'])
    trades['time'] = pd.to_datetime(trades['time'], unit='ms')
    trades['aggregate_trade_id'] = pd.to_numeric(trades['aggregate_trade_id'])
    trades['price'] = pd.to_numeric(trades['price'])
    trades['qty'] = pd.to_numeric(trades['qty'])
    trades['first_trade_id'] = pd.to_numeric(trades['first_trade_id'])
    trades['last_trade_id'] = pd.to_numeric(trades['last_trade_id'])
    trades['bid/ask'] = np.where(trades['buyer_maker'] == True, 'bid', 'ask')
    trades.drop_duplicates(inplace=True)
    trades.sort_values(by='time', ascending=False, inplace=True)
    trades.index = [i for i in range(trades.shape[0])]
    return trades


def aggregate_trade_list(pair: str, **kwargs) -> list:
    url_path = "/api/v3/aggTrades"
    params = {'symbol': pair}
    for key, value in kwargs.items():
        if key == 'from_id':
            params['fromId'] = int(value)
        if key == 'start_time':
            params['startTime'] = int(value)
        if key == 'end_time':
            params['endTime'] = int(value)
        if key == 'limit':
            params['limit'] = np.min([value, 1000])
    response = send_public_request(
        url_path=url_path,
        payload=params
    )
    return response
