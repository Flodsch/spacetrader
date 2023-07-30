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
    agent = request("https://api.spacetraders.io/v2/my/agent", Request.GET)
    hq_system = "-".join((agent["data"]["headquarters"].split("-")[0], agent["data"]["headquarters"].split("-")[1]))
    waypoints = request("https://api.spacetraders.io/v2/systems/{}/waypoints".format(hq_system), Request.GET)
    for page in range(2, math.ceil(waypoints["meta"]["total"] / waypoints["meta"]["limit"]) + 1):
        [waypoints["data"].append(waypoint) for waypoint in request("https://api.spacetraders.io/v2/systems/{}/waypoints?page={}".format(hq_system, page), Request.GET)["data"]]
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
