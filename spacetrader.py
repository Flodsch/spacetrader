import math
import time
import json
import enum
import logging
import requests
import threading

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s] %(message)s"))
log = logging.getLogger('spacetrader')
log.addHandler(handler)
log.setLevel(logging.INFO)

class Request(enum.Enum):
    GET = 1
    POST = 2

with open('token', 'r') as f:
    token = f.read()
http_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer '+token+''}
http_semaphore = threading.BoundedSemaphore(value=1)

def request(url, type, body=None):
    http_semaphore.acquire()
    while True:
        if type == Request.GET:
            x = requests.get(url, headers = http_headers)
        if type == Request.POST:
            if body:
                x = requests.post(url, headers=http_headers, data=json.dumps(body))
            else:
                x = requests.post(url, headers=http_headers)
        if x.status_code == 429:
            time.sleep(float(json.loads(x.text)["error"]["data"]["retryAfter"]))
        else:
            break
    http_semaphore.release()
    return json.loads(x.text)

if __name__ == "__main__":
    ships = request("https://api.spacetraders.io/v2/my/ships", Request.GET)
    for page in range(2, math.ceil(ships["meta"]["total"] / ships["meta"]["limit"]) + 1):
        [ships["data"].append(ship) for ship in request("https://api.spacetraders.io/v2/my/ships?page={}".format(page), Request.GET)["data"]]
    for ship in ships["data"]:
        pass # log.info(ship["symbol"])
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
