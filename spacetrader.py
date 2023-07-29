import time
import json
import enum
import logging
import requests

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
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
