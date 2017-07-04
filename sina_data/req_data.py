from file_manager import FileManger
import MQconfig
from worker import TornadoWorker, Producer
import logger
import json
import os


root = os.environ.get("SINADATA")
log = logger.get_time_rotate("CheckIndexLog")


def check():
    fm = FileManger(root)
    for param in fm.empty_date():
        log.info("code={code} date={date}".format(**param))
        yield json.dumps(param)


if __name__ == '__main__':

    TornadoWorker.params(
        Producer(check(), MQconfig.exchange, MQconfig.queue, MQconfig.bind)
    ).start()