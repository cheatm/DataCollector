# encoding:utf-8
import json


def retry(count=3, exception=Exception, default=lambda: None, wait=1, log=None):
    def wrapper(function):
        from time import sleep
        def re(*args, **kwargs):
            for i in range(count):
                try:
                    return function(*args, **kwargs)
                except exception as e:
                    if log is None:
                        print "{} {} {}".format(function, args, kwargs)
                        print e
                    else:
                        log.error("%s: %s, %s, %s", e, function, args, kwargs)
                    sleep(wait)
            return default()
        return re
    return wrapper


def callback(handle=json.loads):
    def wrapper(function):
        def call_back(ch, method, properties, body):
            try:
                result = function(**handle(body))
            except Exception as e:
                print(e)
                return
            if result:
                ch.basic_ack(delivery_tag=method.delivery_tag)
        return call_back
    return wrapper