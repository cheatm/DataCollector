# encoding:utf-8
from file_manager import FileManger
import json
from fxdayu_data import MongoHandler
import pandas as pd
import os
import logger

try:
    root = os.environ.get("SINADATA")
    fm = FileManger(root)
    handler = MongoHandler.params(**json.load(open(root+'/mongo.json')))
except:
    root = None
    fm = None
    handler = None


def write_db():
    import index_db

    index_db.write_db(fm, handler, logger.get_time_rotate("WriteDBLog"))


def emend_db(include_2=False):
    import index_db

    log = logger.get_time_rotate("CommandLog")

    db = handler.client[handler.db]
    for code in fm.find_stocks():
        try:
            index_db.do_scan(code, fm, db[code], include_2)
            log.info("EmendDB {} success".format(code))
        except Exception as e:
            log.error("EmendDB {} fail {}".format(code, e))


def ensure_tick():
    log = logger.get_time_rotate("CommandLog")
    for code in fm.find_stocks():
        result = fm.ensure_tick(code)
        if isinstance(result, pd.DataFrame):
            log.info("Ensure {} success".format(code))
        elif isinstance(result, Exception):
            log.error("Ensure {} fail {}".format(code, result))


def update_index(update_bench=True):
    log = logger.get_time_rotate("CommandLog")
    benchmark = fm.get_benchmark(update_bench)
    for code in fm.find_stocks():
        result = fm.update_stock(code, benchmark.index)
        if isinstance(result, pd.DataFrame):
            log.info("UpdateIndex {} success".format(code))
        elif isinstance(result, Exception):
            log.error("UpdateIndex {} fail {}".format(code, result))


def req_tick_data():
    from worker import TornadoWorker, Producer
    import MQconfig
    import req_data

    TornadoWorker.params(
        Producer(req_data.check(), MQconfig.exchange, MQconfig.queue, MQconfig.bind)
    ).start()


def ensure_index(include_2=False):
    ensure_tick()
    emend_db(include_2)


commands = {"ensure_tick": ensure_tick,  # 按tick文件更新索引
            "update_index": update_index,  # 更新索引日期
            "emend_db": emend_db,  # 按数据库更新索引
            "write_db": write_db,  # 根据索引写数据库
            "req_tick_data": req_tick_data,  # 根据索引向MQ提交数据下载任务
            "ensure_index": ensure_index}  # 按tick和数据库更新索引


import click


@click.group(chain=True)
def command():
    pass


@command.command()
def tick():
    """read tick file and update index"""
    ensure_tick()


@command.command()
@click.option("--bench", is_flag=True)
def update(bench):
    """
    update stock index by benchmark
    --bench: update benchmark before update stock index
    """
    update_index(bench)


@command.command()
def require():
    """read index and post DataRequestMessage to MQ"""
    req_tick_data()


@command.command()
def write():
    """read index and write data into db"""
    write_db()


@command.command()
@click.option("--include2", is_flag=True)
def emend(include2):
    """
    read db and update index
    --include2: check all log
    """
    emend_db(include2)

from datetime import datetime


@command.command()
@click.option("--path")
@click.option("--start", default='2012-06-01')
@click.option("--end", default=datetime.now().strftime("%Y-%m-%d"))
@click.option("--stock_index", is_flag=True)
def create(path, start, end, stock_index):
    """create index dir"""
    os.environ['SINADATA'] = path
    os.makedirs(path+'/')
    os.makedirs(path+'/Log/')
    fm = FileManger(root)
    benchmark = fm.create_benchmark(start, end)
    print "create benchmark {}".format(fm.benchmark)

    import json
    codes = json.load(open('stocks.json'))
    json.dump(codes, open('stocks.json', 'w'))
    if stock_index:
        for code in codes:
            try:
                fm.create_stock(code, benchmark.index)
                print "create index {}".format(code)
            except:
                print "create index {} failed".format(code)


if __name__ == '__main__':
    command()