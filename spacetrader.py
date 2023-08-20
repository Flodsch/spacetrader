import math
import time
import json
import enum
import numpy
import sqlite3
import logging
import requests
import datetime
import threading
import collections
import jmespath as jp
from dateutil import parser

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(threadName)-9.9s] [%(levelname)-5.5s] %(message)s"))
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
        if x.status_code == 429:
            try:
                retry_after = float(jp.search('error.data.retryAfter', json.loads(x.text)))
                log.debug("429 http status code. Need to wait {} seconds.".format(retry_after))
                time.sleep(retry_after)
            except:
                pass
        else:
            break
    http_semaphore.release()
    log.debug("[{}] {} {} {}".format(type, x.request.url, x.request.headers, x.request.body))
    log.debug(x.text)
    return json.loads(x.text)

class ShipException(Exception):
    pass

class Ship:
    def __init__(self, symbol) -> None:
        self.symbol = symbol
        req = request("https://api.spacetraders.io/v2/my/ships/{}".format(self.symbol), Request.GET)
        data = jp.search('data', req)
        if data:
            for k, v in data.items():
                setattr(self, k, v)
        log.info("[{}] Ship is online.".format(self.symbol))

    def dock(self):
        req = request("https://api.spacetraders.io/v2/my/ships/{}/dock".format(self.symbol), Request.POST)
        if jp.search('data', req):
            self.nav = jp.search('data', req)["nav"]
            log.info("[{}] Docking successful.".format(self.symbol))
            return req
        if jp.search('error', req):
            raise ShipException(jp.search('error', req))

    def orbit(self):
        req = request("https://api.spacetraders.io/v2/my/ships/{}/orbit".format(self.symbol), Request.POST)
        if jp.search('data', req):
            self.nav = jp.search('data', req)["nav"]
            log.info("[{}] Orbiting successful.".format(self.symbol))
            return req
        if jp.search('error', req):
            raise ShipException(jp.search('error', req))

    def sell(self, item):
        req = request("https://api.spacetraders.io/v2/my/ships/{}/sell".format(self.symbol), Request.POST, {"symbol": item["symbol"], "units": item["units"]})
        if jp.search('data', req):
            self.cargo = jp.search('data.cargo', req)
            log.info("[{}] Selling successful: {} x {} @ {} cr.".format(self.symbol, item["units"], item["symbol"], jp.search("data.transaction.totalPrice", req)))
            return req
        if jp.search('error', req):
            raise ShipException(jp.search('error', req))

    def survey(self):
        req = request("https://api.spacetraders.io/v2/my/ships/{}/survey".format(self.symbol), Request.POST)
        if jp.search('data', req):
            for survey in jp.search('data.surveys', req):
                log.info("[{}] Surveyed: {}.".format(self.symbol, survey["signature"]))
            return req
        if jp.search('error', req):
            raise ShipException(jp.search('error', req))

    def extract(self, survey=None):
        req = request("https://api.spacetraders.io/v2/my/ships/{}/extract".format(self.symbol), Request.POST, body = {"survey": survey}) if survey else request("https://api.spacetraders.io/v2/my/ships/{}/extract".format(self.symbol), Request.POST)
        if jp.search('data', req):
            self.cargo = jp.search('data.cargo', req)
            log.info("[{}] Extracting successful: {} x {}. Using {}.".format(self.symbol, jp.search("data.extraction.yield", req)["units"], jp.search("data.extraction.yield", req)["symbol"], survey["signature"]))
            return req
        if jp.search('error', req):
            raise ShipException(jp.search('error', req))
    
    def navigate(self, waypoint):
        req = request("https://api.spacetraders.io/v2/my/ships/{}/navigate".format(self.symbol), Request.POST, {"waypointSymbol": waypoint})
        if jp.search('data', req):
            return req
        if jp.search('error', req):
            raise ShipException(jp.search('error', req))
    
    def purchase(self, item, units):
        req = request('https://api.spacetraders.io/v2/my/ships/{}/purchase'.format(self.symbol), Request.POST, body={"symbol": item, "units": units})
        if jp.search('data', req):
            return req
        if jp.search('error', req):
            raise ShipException(jp.search('error', req))
    
    def install(self, item):
        req = request('https://api.spacetraders.io/v2/my/ships/{}/mounts/install'.format(self.symbol), Request.POST, body={"symbol": item,})
        if jp.search('data', req):
            return req
        if jp.search('error', req):
            raise ShipException(jp.search('error', req))

def _sell(ship):
    ship.dock()
    for item in ship.cargo["inventory"]:
        ship.sell(item)   
    ship.orbit()

def surveyor_i(ship):
    try:
        req = ship.survey()
    except ShipException as e:
        match e.args[0]["code"]:
            case 4000: # cooldownConflictError
                remaining_seconds = jp.search('data.cooldown.remainingSeconds', e.args[0])
                log.info("[{}] Erroneous survey cooldown: {} seconds.".format(ship.symbol, remaining_seconds))
                time.sleep(remaining_seconds)
    except Exception as e:
        pass
    else:
        with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
            for survey in req["data"]["surveys"]:
                conn.execute('INSERT INTO surveys(survey, rating, signature, deposits, expiration, size) VALUES(\'{}\', {}, \'{}\', \'{}\', \'{}\', \'{}\');'.format(json.dumps(survey), 2, survey["signature"], json.dumps(survey["deposits"]), survey["expiration"], survey["size"]))
        remaining_seconds = int((parser.parse(jp.search('data.cooldown.expiration', req)).replace(tzinfo=None) - datetime.datetime.utcnow()).total_seconds())
        log.info("[{}] Normal survey cooldown: {} seconds.".format(ship.symbol, remaining_seconds))
        time.sleep(remaining_seconds)

def miner_i(ship):
    if ship.nav["status"] == "DOCKED":
        ship.orbit()
    if len(ship.cargo["inventory"]) > 0:
        _sell(ship)
    with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
        survey = conn.execute("SELECT * FROM surveys ORDER BY rating DESC LIMIT 1;").fetchone()
    if survey:
        try:
            req = ship.extract(json.loads(survey[2]))
        except ShipException as e:
            match e.args[0]["code"]:
                case 4000: # cooldownConflictError
                    remaining_seconds = jp.search('data.cooldown.remainingSeconds', e.args[0])
                    log.info("[{}] Erroneous extract cooldown: {} seconds.".format(ship.symbol, remaining_seconds))
                    time.sleep(remaining_seconds)
                case 4221: # shipSurveyExpirationError 
                    log.info("[{}] Survey expired. Deleting {}.".format(ship.symbol, survey[3]))
                    with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
                        conn.execute("DELETE FROM surveys WHERE signature = '{}';".format(survey[3]))
                case 4224: # shipSurveyExhaustedError
                    log.info("[{}] Survey exhausted. Deleting {}.".format(ship.symbol, survey[3]))
                    with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
                        conn.execute("DELETE FROM surveys WHERE signature = '{}';".format(survey[3]))
        except Exception as e:
            log.error(e)
        else:
            _sell(ship)
            remaining_seconds = int((parser.parse(jp.search('data.cooldown.expiration', req)).replace(tzinfo=None) - datetime.datetime.utcnow()).total_seconds())
            log.info("[{}] Normal extract cooldown: {} seconds.".format(ship.symbol, remaining_seconds))
            time.sleep(remaining_seconds)
    else:
        surveyor_i(ship) # survey one round

def standby_i(ship_symbol):
    time.sleep(1)

def function_switchboard(ship):
    while True:
        with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
            function_name = conn.execute("SELECT function_name FROM ships WHERE ship_symbol = '{}';".format(ship.symbol)).fetchone()[0]
            try:
                func = globals()[function_name]
            except KeyError:
                log.error("Please correct column function_name for '{}'. '{}' does not match a valid function signature.".format(ship.symbol, function_name))
            else:
                func(ship)

def rater_i():
    while True:
        with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
            asteroid_field_symbol = conn.execute("SELECT symbol FROM waypoints WHERE type = 'ASTEROID_FIELD';").fetchone()[0]
            system_symbol = "{}-{}".format(asteroid_field_symbol.split("-")[0], asteroid_field_symbol.split("-")[1])
            market_data = request("https://api.spacetraders.io/v2/systems/{}/waypoints/{}/market".format(system_symbol, asteroid_field_symbol), Request.GET)["data"]["tradeGoods"]
            for page in range(2, math.ceil(market_data["meta"]["total"] / market_data["meta"]["limit"]) + 1):
                [market_data["data"].append(ship) for ship in request("https://api.spacetraders.io/v2/systems/{}/waypoints/{}/market?page={}".format(system_symbol, asteroid_field_symbol, page), Request.GET)["data"]]
            goods_to_delete = []
            for good in market_data:
                if good["sellPrice"] < 15:
                    goods_to_delete.append("deposits LIKE '%{}%'".format(good["symbol"]))
            conn.execute('''
                DELETE FROM surveys
                WHERE {} 
                OR expiration <= time('now');
            '''.format(" OR ".join(goods_to_delete)))
            log.info("Purged surveys!")
        time.sleep(60)

def rater_ii():

    def _score(survey, market):
        counter = collections.Counter([x["symbol"] for x in survey["deposits"]])
        arr1 = []
        arr2 = []
        for key, value in counter.items():
            arr1.append(value)
            arr2.append([x for x in market if x["symbol"] == key][0]["sellPrice"])
        return numpy.dot(arr1, arr2) / len(arr1) # return numpy.sum(numpy.divide(arr1, arr2)) # 
    
    while True:
        with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
            asteroid_field_symbol = conn.execute("SELECT symbol FROM waypoints WHERE type = 'ASTEROID_FIELD';").fetchone()[0]
            system_symbol = "{}-{}".format(asteroid_field_symbol.split("-")[0], asteroid_field_symbol.split("-")[1])
            market_data = request("https://api.spacetraders.io/v2/systems/{}/waypoints/{}/market".format(system_symbol, asteroid_field_symbol), Request.GET)
            """
            goods_to_delete = []
            for good in market_data["data"]["tradeGoods"]:
                if good["sellPrice"] < 10: # TODO: make available in DB
                    goods_to_delete.append("deposits LIKE '%{}%'".format(good["symbol"]))
            if goods_to_delete:
                log.info("[rater_ii] {}".format(goods_to_delete))
                conn.execute("DELETE FROM surveys WHERE {};".format(" OR ".join(goods_to_delete)))
            """
            conn.execute("DELETE FROM surveys WHERE expiration <= time('now');")
            for survey in conn.execute("SELECT * FROM surveys;"):
                conn.execute("UPDATE surveys SET rating = {} WHERE signature = '{}';".format(_score(json.loads(survey[2]), market_data["data"]["tradeGoods"]), survey[3]))
            conn.execute("DELETE FROM surveys WHERE number NOT IN (SELECT number FROM surveys ORDER BY rating DESC LIMIT 50);") # TODO make parameter
            log.info("[rater_ii] Rated and purged surveys!")
        time.sleep(120) # TODO: make available in DB

def upgrader_i():
    while True:
        with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
            ships = conn.execute("SELECT ship_symbol FROM ships;").fetchall()
            shipyard_symbol = conn.execute("SELECT symbol FROM waypoints WHERE shipyard = 1;").fetchone()[0]
            asteroid_field_symbol = conn.execute("SELECT symbol FROM waypoints WHERE type = 'ASTEROID_FIELD';").fetchone()[0]
        for ship_symbol in ships:
            ship = Ship(ship_symbol[0])
            req = request("https://api.spacetraders.io/v2/my/ships/{}".format(ship.symbol), Request.GET)
            if req["data"]["frame"]["symbol"] == "FRAME_MINER" and len(req["data"]["mounts"]) == 2: # if free mount:
                agent = request("https://api.spacetraders.io/v2/my/agent", Request.GET)
                system_symbol = "{}-{}".format(shipyard_symbol.split("-")[0], shipyard_symbol.split("-")[1])
                market_data = request("https://api.spacetraders.io/v2/systems/{}/waypoints/{}/market".format(system_symbol, shipyard_symbol), Request.GET)["data"]["tradeGoods"]
                laser_price = [x for x in market_data if x["symbol"] == "MOUNT_MINING_LASER_II"][0]["purchasePrice"]
                if agent["data"]["credits"] > laser_price + 5000: # consider installation fees
                    log.info("[{}] Going to upgrade: {}.".format(ship.symbol, "MOUNT_MINING_LASER_II"))
                    with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
                        conn.execute("UPDATE ships SET function_name = 'standby_i' WHERE ship_symbol = '{}';".format(ship.symbol))
                    ship.orbit()
                    ship.navigate(shipyard_symbol)
                    time.sleep(150)
                    ship.dock()
                    ship.purchase("MOUNT_MINING_LASER_II", 1)
                    ship.install("MOUNT_MINING_LASER_II")
                    ship.orbit()
                    ship.navigate(asteroid_field_symbol)
                    time.sleep(150)
                    with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
                        conn.execute("UPDATE ships SET function_name = 'miner_i' WHERE ship_symbol = '{}';".format(ship.symbol))
        time.sleep(300)

def buyer_i():
    while True:
        with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
            ship_count = conn.execute("SELECT COUNT(*) FROM ships;").fetchone()[0]
            shipyard_symbol = conn.execute("SELECT symbol FROM waypoints WHERE shipyard = 1;").fetchone()[0]
            asteroid_field_symbol = conn.execute("SELECT symbol FROM waypoints WHERE type = 'ASTEROID_FIELD';").fetchone()[0]
        if ship_count < 26: # TODO: put parameter in DB
            x = request('https://api.spacetraders.io/v2/my/ships', Request.POST, body={"shipType": "SHIP_ORE_HOUND", "waypointSymbol": shipyard_symbol})
            try:
                x["data"]
            except KeyError:
                if x["error"]["code"] == 4216: # insufficient funds
                    log.info("[buyer_i] Insufficient funds: {}".format(x["error"]["data"]))
                else:
                    log.error(x)
            else:
                ship = Ship(x["data"]["ship"]["symbol"])
                with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
                    conn.execute("INSERT OR IGNORE INTO ships VALUES('{}', 'miner_i')".format(ship.symbol))
                ship.orbit()
                ship.navigate(asteroid_field_symbol)
                time.sleep(200)
                threading.Thread(target=function_switchboard, args=(ship, ), daemon=True).start()
        time.sleep(300)

def main():
    global agent_symbol
    agent = request("https://api.spacetraders.io/v2/my/agent", Request.GET)
    log.info("Starting... {}".format(agent))
    agent_symbol = jp.search('data.symbol', agent)
    hq_system = "-".join((jp.search('data.headquarters', agent).split("-")[0], jp.search('data.headquarters', agent).split("-")[1]))
    waypoints = request("https://api.spacetraders.io/v2/systems/{}/waypoints".format(hq_system), Request.GET)
    for page in range(2, math.ceil(waypoints["meta"]["total"] / waypoints["meta"]["limit"]) + 1):
        [waypoints["data"].append(waypoint) for waypoint in request("https://api.spacetraders.io/v2/systems/{}/waypoints?page={}".format(hq_system, page), Request.GET)["data"]]
    ships = request("https://api.spacetraders.io/v2/my/ships", Request.GET)
    for page in range(2, math.ceil(ships["meta"]["total"] / ships["meta"]["limit"]) + 1):
        [ships["data"].append(ship) for ship in request("https://api.spacetraders.io/v2/my/ships?page={}".format(page), Request.GET)["data"]]
    with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS "waypoints" (
                "symbol"	TEXT,
                "type"	TEXT,
                "x"	INTEGER,
                "y"	INTEGER,
                "shipyard"	TEXT,
                PRIMARY KEY("symbol")
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS "surveys" (
                "number"	INTEGER,
                "rating"	INTEGER,
                "survey"	TEXT NOT NULL,
                "signature"	TEXT,
                "deposits"	TEXT,
                "expiration"	TEXT,
                "size"	TEXT,
                PRIMARY KEY("number" AUTOINCREMENT)
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS "ships" (
                "ship_symbol"	,
                "function_name"	,
                PRIMARY KEY("ship_symbol")
            )
        ''')
        for ship in ships["data"]:
            conn.execute("INSERT OR IGNORE INTO ships VALUES('{}', 'standby_i')".format(ship["symbol"]))
        for waypoint in waypoints["data"]:
            conn.execute("INSERT OR IGNORE into waypoints(symbol, type, x, y, shipyard) VALUES('{}', '{}', {}, {}, {});".format(waypoint["symbol"], waypoint["type"], waypoint["x"], waypoint["y"], "SHIPYARD" in str(waypoint["traits"])))
    for ship in ships["data"]:
        threading.Thread(target=function_switchboard, args=(Ship(ship["symbol"]), ), daemon=True).start()
    threading.Thread(target=rater_ii, daemon=True).start()
    threading.Thread(target=buyer_i, daemon=True).start()
    # threading.Thread(target=upgrader_i, daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down!")
        with sqlite3.connect("{}.db".format(agent_symbol)) as conn:
            conn.execute("DELETE FROM ships;")

if __name__ == "__main__":
    main()
