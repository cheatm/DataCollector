# encoding:utf-8
import pandas as pd
import tushare
from datetime import datetime
import os


def to_sina(code):
    if code.endswith('.XSHE'):
        return 'sz' + code[:-5]
    elif code.endswith('.XSHG'):
        return 'sh' + code[:-5]
    else:
        return code


def to_rq(code):
    if code.startswith('sz'):
        return code[2:] + '.XSHE'
    elif code.startswith('sh'):
        return code[2:] + '.XSHG'
    else:
        return code


def day_data(code, start, end, *args, **kwargs):
    if len(code) > 6:
        code = code[:-5]
    data = tushare.get_k_data(code, start, end, *args, **kwargs).set_index('date')
    data.pop('code')
    data['status'] = 0
    return data


class FileManger(object):

    MONTH = "%Y-%m"
    DAY = "%Y-%m-%d"

    def __init__(self, root, benchmark='000001'):
        self.root = root + '/'
        self.benchmark = benchmark

    @staticmethod
    def save_csv(frame, path):
        try:
            frame.to_csv(path)
        except IOError:
            dirs = path[:path.rfind('/')]
            os.makedirs(dirs)
            frame.to_csv(path)

    def find_stocks(self):
        import json
        return json.load(open(self.root+'stocks.json'))

    # 创建并获取主索引
    def create_benchmark(self, start="2012-06-01", end=datetime.now().strftime("%Y-%m-%d")):
        data = day_data(self.benchmark, start, end, index=True)
        data.to_csv(self.benchmark_path)
        return data

    @property
    def benchmark_path(self):
        return self.root + self.benchmark + '.csv'

    # 获取主索引
    def get_benchmark(self, update=False):
        if update:
            try:
                if os.path.exists(self.benchmark_path):
                    bench = self.get_benchmark()
                    return self.create_benchmark(bench.index[0])
                else:
                    return self.create_benchmark()
            except Exception as e:
                print e
                return self.get_benchmark()
        else:
            try:
                return pd.read_csv(self.benchmark_path, index_col='date')
            except IOError:
                return self.create_benchmark()

    # 获得股票索引地址
    def stock_path(self, code):
        return self.root + code + '/index.csv'

    # 创建并返回股票索引
    def create_stock(self, code, benchmark):
        start, end = benchmark[0], benchmark[-1]
        frame = day_data(code, start, end)
        frame = self.extend(frame, benchmark)
        path = self.stock_path(code)
        self.save_csv(frame, path)
        return frame

    # 获取股票索引
    def get_stock(self, code):
        try:
            return pd.read_csv(self.stock_path(code), index_col='date')
        except IOError:
            return self.create_stock(code, self.get_benchmark().index)

    # 更新股票索引
    def update_stock(self, code, benchmark):
        try:
            stock = self.get_stock(code)
            updated = self.synchronize(code, stock, benchmark)
        except Exception as e:
            return e

        updated.to_csv(self.stock_path(code))
        return updated

    # 将origin的时间与benchmark同步
    def synchronize(self, code, origin, benchmark):
        new = self.fill(code, origin, benchmark)
        extended = self.extend(new, benchmark)
        return extended

    # 更新补齐数据
    @staticmethod
    def fill(code, origin, benchmark):
        start, end = origin.index[-1], benchmark[-1]
        if datetime.strptime(start, "%Y-%m-%d") < datetime.strptime(end, "%Y-%m-%d"):
            new = day_data(code, start, end)
            new = pd.concat((origin, new))
            duplicated = new.index.duplicated()
            return new[-duplicated]
        else:
            return origin

    # 标记缺失的日线
    @staticmethod
    def extend(frame, index):
        extended = pd.DataFrame(frame, index)
        extended['volume'].fillna(0, inplace=True)
        extended['close'].ffill(inplace=True)
        extended['status'].fillna(-1, inplace=True)
        return extended.ffill(axis=1).bfill(axis=1)

    def tick_exist(self, code, frame):
        for date in self.get_status(frame, 0):
            tick_path = self.get_tick_path(code, date)
            if os.path.exists(tick_path):
                frame.loc[date, 'status'] = 1

        return frame

    def get_status(self, frame, status):
        return frame[frame['status']==status].index

    def ensure_tick(self, code):
        try:
            frame = self.get_stock(code)
        except Exception as e:
            return e
        frame = self.tick_exist(code, frame)
        self.save_csv(frame, self.stock_path(code))
        return frame

    def ensure_tick_all(self):
        for code in self.find_stocks():
            self.ensure_tick(code)

    def empty_date(self):
        for code in self.find_stocks():
            try:
                stock = self.get_stock(code)
            except:
                continue
            for date in self.get_status(stock, 0):
                yield {"code": code, 'date': date}

    def get_dir(self, code):
        return self.root + code + '/'

    def get_tick_path(self, code, date):
        return self.get_dir(code) + date + '.xlsx'

    def save(self, frame, code, date):
        file_path = self.get_tick_path(code, date)
        try:
            frame.to_excel(file_path)
        except IOError:
            os.makedirs(self.get_dir(code))
            frame.to_excel(file_path)

    def read(self, code, date, **kwargs):
        file_path = self.get_tick_path(code, date)
        return pd.read_excel(file_path, **kwargs)


