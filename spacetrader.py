import math
import time
import json
import enum
import sqlite3
import logging
import requests
import datetime
import threading
import collections
from dateutil import parser

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(threadName)-20.20s] [%(levelname)-5.5s] %(message)s"))
log = logging.getLogger('spacetrader')
log.addHandler(handler)
log.setLevel(logging.INFO)

class Request(enum.Enum):
    GET = 1
    POST = 2

with open('token', 'r') as f:
    token = f.read()

http_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer '+token+''}
http_semaphore = threading.BoundedSemaphore(value=2)

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
        log.debug("[{}] {} {} {}".format(type, x.request.url, x.request.headers, x.request.body))
        if x.status_code == 429:
            time.sleep(float(json.loads(x.text)["error"]["data"]["retryAfter"]))
        else:
            break
    http_semaphore.release()
    log.debug(x.text)
    return json.loads(x.text)

def query(sql_string):
    con = sqlite3.connect("survey.db")
    cur = con.cursor()
    res = cur.execute(sql_string).fetchall()
    cur.close()
    con.close()
    return res

def save_survey(survey):
    con = sqlite3.connect("survey.db")
    cur = con.cursor()
    cur.execute('INSERT INTO surveys(survey, rating) VALUES(\'{}\', {})'.format(json.dumps(survey), 40))
    con.commit()
    cur.close()
    con.close()

def read_survey():
    con = sqlite3.connect("survey.db")
    cur = con.cursor()
    res = cur.execute("SELECT * FROM surveys ORDER BY rating DESC LIMIT 1;")
    x = res.fetchone()
    return json.loads(x[1])

"""
survey/extract/sell at asteroid using own survey
"""
def miner_ii(ship_symbol):
    
    def _survey():
        x = request("https://api.spacetraders.io/v2/my/ships/{}/survey".format(ship_symbol), Request.POST)
        try:
            x["data"]
        except KeyError:
            try:
                x["error"]
            except:
                log.error("[{}] {}".format(ship_symbol, x))
            else:
                if x["error"]["code"] == 4000: # Ship action is still on cooldown
                    cooldown_seconds = x["error"]["data"]["cooldown"]["remainingSeconds"]
                    log.info("[{}] Ship is still cooling down for {} seconds.".format(ship_symbol, cooldown_seconds))
                    time.sleep(cooldown_seconds)
                    _survey()
        else:
            for survey in x["data"]["surveys"]:
                log.info("[{}] Successfully surveyed. Adding {}.".format(ship_symbol, survey["signature"]))
                save_survey(survey)
            return x

    def _extract():
        while True:
            survey = read_survey()
            x = request("https://api.spacetraders.io/v2/my/ships/{}/extract".format(ship_symbol), Request.POST, body = {"survey": survey})
            try:
                x["data"]
            except KeyError:
                try:
                    x["error"]
                except:
                    log.error("[{}] {}".format(ship_symbol, x))
                else:
                    if x["error"]["code"] == 4228: # Ship is at maximum capacity
                        log.info("[{}] Ship is at maximum capacity.".format(ship_symbol))
                        return
                    if x["error"]["code"] == 4000: # Ship action is still on cooldown
                        cooldown_seconds = x["error"]["data"]["cooldown"]["remainingSeconds"]
                        log.info("[{}] Ship is still cooling down for {} seconds.".format(ship_symbol, cooldown_seconds))
                        time.sleep(cooldown_seconds)
                    if x["error"]["code"] in [4221, 4224]: # shipSurveyExpirationError || shipSurveyExhaustedError
                        log.info("[{}] Survey expired or exhausted. Deleting {}.".format(ship_symbol, survey["signature"]))
                        con = sqlite3.connect("survey.db")
                        cur = con.cursor()
                        cur.execute("DELETE FROM surveys WHERE survey = \'{}\';".format(json.dumps(survey)))
                        con.commit()
            else:
                log.info("[{}] {}".format(ship_symbol, "Extracting successful: {} x {}.".format(x["data"]["extraction"]["yield"]["units"], x["data"]["extraction"]["yield"]["symbol"])))
                load = int((x["data"]["cargo"]["units"] / x["data"]["cargo"]["capacity"]) * 100)
                if load > 80:
                    log.info("[{}] capacity at: {}%".format(ship_symbol, load))
                    return
                else:
                    cooldown_seconds = x["data"]["cooldown"]["remainingSeconds"]
                    time.sleep(cooldown_seconds)

    def _sell():
        request("https://api.spacetraders.io/v2/my/ships/{}/dock".format(ship_symbol), Request.POST)
        for item in request("https://api.spacetraders.io/v2/my/ships/{}/cargo".format(ship_symbol), Request.GET)["data"]["inventory"]:
            x = request("https://api.spacetraders.io/v2/my/ships/{}/sell".format(ship_symbol), Request.POST, {"symbol": item["symbol"], "units": item["units"]})
            try:
                x["data"]
            except KeyError:
                log.error("[{}] {}".format(ship_symbol, x))
            else:
                log.info("[{}] {}".format(ship_symbol, "Selling successful: {} x {} @ {} cr.".format(item["units"], item["symbol"], x["data"]["transaction"]["totalPrice"])))
    
    request("https://api.spacetraders.io/v2/my/ships/{}/orbit".format(ship_symbol), Request.POST)
    if int(query("SELECT COUNT(*) FROM surveys")[0][0]) < 10:
        _survey()
    _extract()
    _sell()
    upgrader_i(ship_symbol)
    miner_ii(ship_symbol)

"""
extract/sell at asteroid
"""
def miner_i(ship_symbol):
    request("https://api.spacetraders.io/v2/my/ships/{}/orbit".format(ship_symbol), Request.POST) 
    while True:
        x = request("https://api.spacetraders.io/v2/my/ships/{}/extract".format(ship_symbol), Request.POST)
        try:
            x["data"]
        except KeyError:
            try:
                x["error"]
            except:
                pass
            else:
                if x["error"]["code"] == 4228: # Ship is at maximum capacity
                    log.info("[{}] Ship is at maximum capacity.".format(ship_symbol))
                    break
                if x["error"]["code"] == 4000: # Ship action is still on cooldown
                    cooldown_seconds = x["error"]["data"]["cooldown"]["remainingSeconds"]
                    log.info("[{}] Ship is still cooling down for {} seconds.".format(ship_symbol, cooldown_seconds))
                    time.sleep(cooldown_seconds)
        else:
            log.info("[{}] {}".format(ship_symbol, "Extracting successful: {} x {}.".format(x["data"]["extraction"]["yield"]["units"], x["data"]["extraction"]["yield"]["symbol"])))
            load = int((x["data"]["cargo"]["units"] / x["data"]["cargo"]["capacity"]) * 100)
            if load > 80:
                log.info("[{}] capacity at: {}%".format(ship_symbol, load))
                break
            else:
                cooldown_seconds = x["data"]["cooldown"]["remainingSeconds"]
                time.sleep(cooldown_seconds)
    request("https://api.spacetraders.io/v2/my/ships/{}/dock".format(ship_symbol), Request.POST)
    for item in request("https://api.spacetraders.io/v2/my/ships/{}/cargo".format(ship_symbol), Request.GET)["data"]["inventory"]:
        z = request("https://api.spacetraders.io/v2/my/ships/{}/sell".format(ship_symbol), Request.POST, {"symbol": item["symbol"], "units": item["units"]})
        try:
            z["data"]
        except KeyError:
            log.error("[{}] {}".format(ship_symbol, z))
        else:
            log.info("[{}] {}".format(ship_symbol, "Selling successful: {} x {} @ {} cr.".format(item["units"], item["symbol"], z["data"]["transaction"]["totalPrice"])))
    miner_i(ship_symbol)

"""
auto buy ships
"""
def buyer_i(shipyard_symbol, asteroid_field_symbol):
    while True:
        ships = request("https://api.spacetraders.io/v2/my/ships", Request.GET)
        if ships["meta"]["total"] < 100:
            x = request('https://api.spacetraders.io/v2/my/ships', Request.POST, body={"shipType": "SHIP_ORE_HOUND", "waypointSymbol": shipyard_symbol})
            try:
                x["data"]
            except KeyError:
                if x["error"]["code"] == 4216: # insufficient funds
                    log.info("Insufficient funds: {}".format(x["error"]["data"]))
                else:
                    log.error(x)
            else:
                ship = x["data"]["ship"]["symbol"]
                log.info("[{}] Welcome to the fleet.".format(ship))
                request("https://api.spacetraders.io/v2/my/ships/{}/orbit".format(ship), Request.POST)
                b = request("https://api.spacetraders.io/v2/my/ships/{}/navigate".format(ship), Request.POST, body={"waypointSymbol": asteroid_field_symbol})
                time.sleep(300)
                threading.Thread(target=miner_ii, args=(ship,), daemon=True).start()
        time.sleep(900)

def rater_i(system_symbol, asteroid_field_symbol):
    
    def _score(survey, market):
        counter = collections.Counter([x["symbol"] for x in survey["deposits"]])
        total = 0
        for key, value in counter.items():
            total += value * [x for x in market if x["symbol"] == key][0]["sellPrice"]
        # return int(total / len(counter))
        return int(total)

    market_data = request("https://api.spacetraders.io/v2/systems/{}/waypoints/{}/market".format(system_symbol, asteroid_field_symbol), Request.GET)["data"]["tradeGoods"]
    con = sqlite3.connect("survey.db")
    cur = con.cursor()
    res = cur.execute("SELECT * FROM surveys")
    # log.info("Survey pool size: {}".format(len(res.fetchall())))
    for survey in res.fetchall():
        data = json.loads(survey[1])
        remainder = int((parser.parse(data["expiration"]).replace(tzinfo=None) - datetime.datetime.now()).total_seconds())
        if remainder < 300:
            cur.execute("DELETE FROM surveys WHERE number = {};".format(survey[0]))
        else:
            score = _score(data, market_data)
            cur.execute("UPDATE surveys SET rating = {} WHERE number = {}".format(score, survey[0]))
    cur.execute("DELETE FROM surveys WHERE number NOT IN (SELECT number FROM surveys ORDER BY rating DESC LIMIT 10);")
    con.commit()
    time.sleep(60)
    rater_i(system_symbol, asteroid_field_symbol)

def upgrader_i(ship_symbol, shipyard_symbol="X1-YA22-18767C", asteroid_field_symbol="X1-YA22-87615D"):
    x = request("https://api.spacetraders.io/v2/my/ships/{}".format(ship_symbol), Request.GET)
    if x["data"]["frame"]["symbol"] == "FRAME_MINER" and len(x["data"]["mounts"]) == 2: # if free mount:
        agent = request("https://api.spacetraders.io/v2/my/agent", Request.GET)
        market_data = request("https://api.spacetraders.io/v2/systems/{}/waypoints/{}/market".format("X1-YA22", shipyard_symbol), Request.GET)["data"]["tradeGoods"]
        laser_price = [x for x in market_data if x["symbol"] == "MOUNT_MINING_LASER_II"][0]["purchasePrice"]
        if agent["data"]["credits"] > laser_price + 5000: # consider installation fees
            log.info("[{}] Going to upgrade: {}.".format(ship_symbol, "MOUNT_MINING_LASER_II"))
            request("https://api.spacetraders.io/v2/my/ships/{}/orbit".format(ship_symbol), Request.POST)
            x = request("https://api.spacetraders.io/v2/my/ships/{}/navigate".format(ship_symbol), Request.POST, {"waypointSymbol": shipyard_symbol})
            seconds_to_arrival = round((parser.parse(x["data"]["nav"]["route"]["arrival"]).replace(tzinfo=None) - datetime.datetime.utcnow().replace(tzinfo=None)).total_seconds())
            time.sleep(seconds_to_arrival)
            request("https://api.spacetraders.io/v2/my/ships/{}/dock".format(ship_symbol), Request.POST)
            request('https://api.spacetraders.io/v2/my/ships/{}/purchase'.format(ship_symbol), Request.POST, body={"symbol": "MOUNT_MINING_LASER_II", "units": 1})
            request('https://api.spacetraders.io/v2/my/ships/{}/mounts/install'.format(ship_symbol), Request.POST, body={"symbol": "MOUNT_MINING_LASER_II",})
            request("https://api.spacetraders.io/v2/my/ships/{}/orbit".format(ship_symbol), Request.POST)
            x = request("https://api.spacetraders.io/v2/my/ships/{}/navigate".format(ship_symbol), Request.POST, {"waypointSymbol": asteroid_field_symbol})
            seconds_to_arrival = round((parser.parse(x["data"]["nav"]["route"]["arrival"]).replace(tzinfo=None) - datetime.datetime.utcnow().replace(tzinfo=None)).total_seconds())
            time.sleep(seconds_to_arrival)

if __name__ == "__main__":
    agent = request("https://api.spacetraders.io/v2/my/agent", Request.GET)
    log.info("Starting... {}".format(agent))
    system_symbol = "-".join((agent["data"]["headquarters"].split("-")[0], agent["data"]["headquarters"].split("-")[1]))
    waypoints = request("https://api.spacetraders.io/v2/systems/{}/waypoints".format(system_symbol), Request.GET)
    for page in range(2, math.ceil(waypoints["meta"]["total"] / waypoints["meta"]["limit"]) + 1):
        [waypoints["data"].append(waypoint) for waypoint in request("https://api.spacetraders.io/v2/systems/{}/waypoints?page={}".format(system_symbol, page), Request.GET)["data"]]
    shipyard_symbol = None
    for waypoint in waypoints["data"]:
        for trait in waypoint["traits"]:
            if trait["symbol"] == "SHIPYARD":
                shipyard_symbol = waypoint["symbol"]
    asteroid_field_symbol = None
    for waypoint in waypoints["data"]:
        if waypoint["type"] == 'ASTEROID_FIELD':
            asteroid_field_symbol = waypoint["symbol"]
    threading.Thread(target=buyer_i, args=(shipyard_symbol, asteroid_field_symbol, ), daemon=True).start()
    threading.Thread(target=rater_i, args=(system_symbol, asteroid_field_symbol, ), daemon=True).start()
    ships = request("https://api.spacetraders.io/v2/my/ships", Request.GET)
    for page in range(2, math.ceil(ships["meta"]["total"] / ships["meta"]["limit"]) + 1):
        [ships["data"].append(ship) for ship in request("https://api.spacetraders.io/v2/my/ships?page={}".format(page), Request.GET)["data"]]
    for ship in ships["data"]:
        if ship["symbol"] != "FLODSCH-2":
            threading.Thread(target=miner_ii, args=(ship["symbol"], ), daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
