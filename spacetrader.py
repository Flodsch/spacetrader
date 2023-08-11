import math
import time
import json
import enum
import sqlite3
import logging
import requests
import jmespath
import threading

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s] %(message)s"))
log = logging.getLogger('spacetrader')
log.addHandler(handler)
log.setLevel(logging.INFO)

class Request(enum.Enum):
    GET = 1
    POST = 2

agent_symbol = None
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
            time.sleep(float(jmespath.search('error.data.retryAfter', json.loads(x.text))))
        else:
            break
    http_semaphore.release()
    return json.loads(x.text)

def function_i(ship_symbol):
    log.info("[{}] function_i".format(ship_symbol))
    time.sleep(5)
    function_switchboard(ship_symbol)

def function_switchboard(ship_symbol):
    with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
        function_name = conn.execute("SELECT function_name FROM ships WHERE ship_symbol = '{}';".format(ship_symbol)).fetchone()[0]
        try:
            func = globals()[function_name]
        except KeyError:
            log.error("Please correct column function_name for '{}'. '{}' does not match a valid function signature.".format(ship_symbol, function_name))
        else:
            func(ship_symbol)

def main():
    global agent_symbol
    agent = request("https://api.spacetraders.io/v2/my/agent", Request.GET)
    agent_symbol = jmespath.search('data.symbol', agent)
    hq_system = "-".join((jmespath.search('data.headquarters', agent).split("-")[0], jmespath.search('data.headquarters', agent).split("-")[1]))
    waypoints = request("https://api.spacetraders.io/v2/systems/{}/waypoints".format(hq_system), Request.GET)
    for page in range(2, math.ceil(waypoints["meta"]["total"] / waypoints["meta"]["limit"]) + 1):
        [waypoints["data"].append(waypoint) for waypoint in request("https://api.spacetraders.io/v2/systems/{}/waypoints?page={}".format(hq_system, page), Request.GET)["data"]]
    ships = request("https://api.spacetraders.io/v2/my/ships", Request.GET)
    for page in range(2, math.ceil(ships["meta"]["total"] / ships["meta"]["limit"]) + 1):
        [ships["data"].append(ship) for ship in request("https://api.spacetraders.io/v2/my/ships?page={}".format(page), Request.GET)["data"]]
    with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS ships(ship_symbol, function_name)")
        for ship in ships["data"]:
            conn.execute("INSERT INTO ships VALUES('{}', 'function_i')".format(ship["symbol"]))
    for ship in ships["data"]:
        threading.Thread(target=function_switchboard, args=(ship["symbol"], ), daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
            conn.execute("DELETE FROM ships;")

if __name__ == "__main__":
    main()
