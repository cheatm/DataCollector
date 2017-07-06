from datetime import datetime, timedelta
from fxdayu_data import sina_tick
import pandas as pd
from itertools import chain


PriceMap = {'high': 'max', 'low': 'min', 'open': 'first', 'close': 'last'}


def is_in_db(collection, date):
    timestamp = datetime.strptime(date, "%Y-%m-%d")
    ft = {'datetime': {'$gt': timestamp.replace(hour=9, minute=30), '$lte': timestamp.replace(hour=15)}}
    return collection.find(ft).count().real == 240


def scan(collection, frame, full=True):
    if full:
        index = frame.index
    else:
        index = frame[frame['status'] != 2].index

    for date in index:
        if is_in_db(collection, date):
            frame.loc[date, 'status'] = 2
    return frame


def do_scan(code, fm, collection, full=True):
    stock = fm.get_stock(code)
    scanned = scan(collection, stock, full)
    fm.save_csv(scanned, fm.stock_path(code))
    return scanned


def write(code, fm, handler, log):
    stock = fm.get_stock(code)
    stock = fm.tick_exist(code, stock)
    fm.save_csv(stock, fm.stock_path(code))

    for date in fm.get_status(stock, 1):
        try:
            candle = get_candle(code, date, fm)
        except Exception as e:
            log.error("{} {} {}".format(code, date, e))
            continue
        handler.inplace(candle, code)
        log.info("{} {} {}".format(code, date, 1))

    for date in fm.get_status(stock, -1):
        candle = get_candle_stopped(code, date, stock.loc[date, 'close'])
        handler.inplace(candle, code)
        log.info("{} {} {}".format(code, date, -1))


def write_db(fm, handler, log):
    for code in fm.find_stocks():
        try:
            write(code, fm, handler, log)
        except Exception as e:
            log.error("{} {}".format(code, e))


def get_candle(code, date, fm):
    tick = fm.read(code, date)

    if code.endswith(".XSHE"):
        return resample(sina_tick.sz_slice(tick), date2index(date))
    elif code.endswith(".XSHG"):
        return resample(sina_tick.sh_slice(tick), date2index(date))


def resample(frame, date_index):
    group = map(timer, frame.index)
    if isinstance(frame, pd.DataFrame):
        grouper = frame.groupby(group)
        candle = pd.DataFrame(grouper['price'].agg(PriceMap), date_index)
        candle['volume'] = grouper['volume'].sum()
        return fill_candle(candle)


def fill_candle(frame):
    if isinstance(frame, pd.DataFrame):
        frame['volume'].fillna(0, inplace=True)
        frame['close'].ffill(inplace=True)
        frame.fillna(
            {"high": frame['close'], 'low': frame['close'], 'open': frame['close']},
            inplace=True
        )
        frame['open'].bfill(inplace=True)
        return frame.fillna(
            {"high": frame['open'], 'low': frame['open'], 'close': frame['open']}
        )


def timer(timestamp):
    return timestamp + timedelta(seconds=(60 - timestamp.second) % 60)


def get_candle_stopped(code, date, price):
    return pd.DataFrame(
        {"close": price, "high": price, 'open': price, 'low': price, "volume": 0},
        date2index(date)
    )


def date2index(date):
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    return pd.Index(
        list(chain(
            pd.DatetimeIndex(freq='1min', start=date.replace(hour=9, minute=31), end=date.replace(hour=11, minute=30)),
            pd.DatetimeIndex(freq='1min', start=date.replace(hour=13, minute=1), end=date.replace(hour=15, minute=0))
        ))
    )


def clean(date, collection):
    date = datetime.strptime(date, '%Y-%m-%d')
    ft = {'datetime': {'$gt': date.replace(hour=11, minute=30),
                       '$lte': date.replace(hour=13)}}
    return date, collection.delete_many(ft).deleted_count


def read_log(path):
    import re
    with open(path) as f:
        return pd.DataFrame(
            re.findall("INFO] index_db (.*?) (.*?) (.*?)\n", f.read(), re.S),
            columns=['code', 'date', 'status']
        )


