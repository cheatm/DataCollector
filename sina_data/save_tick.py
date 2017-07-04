from datetime import datetime
from fxdayu_data import sina_tick
from tools import retry
import json
from time import sleep
from file_manager import FileManger
import pandas as pd
import MQconfig
from worker import TornadoWorker, Consumer
import logger
import os


manager = FileManger(os.environ['SINADATA'])
log = logger.get_time_rotate("TickDownloadLog")


def sina_code(code):
    if code.endswith(".XSHG"):
        return 'sh'+code[:-5]
    elif code.endswith(".XSHE"):
        return 'sz'+code[:-5]
    else:
        return code


def save_tick(code, date):
    tick = get_tick(code, date)
    if isinstance(tick, pd.DataFrame):
        manager.save(tick[['price', 'volume', 'amount']], code, date)
        log.info("save {} at {} in excel".format(code, date))
    else:
        log.error("no data of {} at {} found".format(code, date))
    sleep(1)


@retry(log=log)
def get_tick(code, date):
    if datetime.strptime(date, "%Y-%m-%d") < datetime.today():
        try:
            data = sina_tick.history_tick(sina_code(code), datetime.strptime(date, "%Y-%m-%d"))
        except sina_tick.SinaBreak:
            log.error("Sina reject request for {} at {}, wait for 180 sec".format(code, date))
            sleep(180)
            return get_tick(code, date)
    else:
        data = sina_tick.get_tick(sina_code(code))
    return data


def callback(ch, method, head, body):
    param = json.loads(body)
    save_tick(**param)

    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    TornadoWorker.params(
        Consumer(callback, MQconfig.queue, MQconfig.consume)
    ).start()

