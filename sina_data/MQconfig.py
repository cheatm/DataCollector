EXCHANGE = "SinaStock"
DOWNLOAD = "DownloadTick"
DOWNLOADROUTING = "DownloadTick"


consume = {'queue': DOWNLOAD}


exchange = {"exchange": EXCHANGE,
            "durable": True}


queue = {"queue": DOWNLOAD,
         "durable": True}


bind = {'routing_key': DOWNLOADROUTING}
