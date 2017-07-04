from datetime import datetime
from fxdayu_data import sina_tick
import pandas as pd
from itertools import chain


def is_in_db(collection, date):
    timestamp = datetime.strptime(date, "%Y-%m-%d")
    start = timestamp.replace(hour=9, minute=31)
    end = timestamp.replace(hour=15)
    if doc_exist(collection, datetime=start) and doc_exist(collection, datetime=end):
        return True
    else:
        return False


def doc_exist(collection, **kwargs):
    doc = collection.find_one(kwargs, projection=kwargs.keys())
    if doc is None:
        return False

    for key, value in kwargs.items():
        v = doc.get(key, None)
        if v != value:
            return False
    return True


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
        tick = sina_tick.sz_slice(tick).resample('1min', label='right', closed='right')
    elif code.endswith(".XSHG"):
        tick = sina_tick.sh_slice(tick).resample('1min', label='right', closed='right')

    candle = tick['price'].agg({'high': 'max', 'low': 'min', 'open': 'first', 'close': 'last'}).ffill()
    candle['volume'] = tick['volume'].sum().fillna(0)

    return candle


def get_candle_stopped(code, date, price):
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    index = pd.Index(
        list(chain(
            pd.DatetimeIndex(freq='1min', start=date.replace(hour=9, minute=31), end=date.replace(hour=11, minute=30)),
            pd.DatetimeIndex(freq='1min', start=date.replace(hour=13, minute=1), end=date.replace(hour=15, minute=0))
        ))
    )
    return pd.DataFrame(
        {"close": price, "high": price, 'open': price, 'low': price, "volume": 0},
        index
    )