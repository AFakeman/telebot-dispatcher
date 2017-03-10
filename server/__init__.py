from flask import Flask, request
import redis
import random
import json

app = Flask(__name__)
app.config.from_pyfile("config.py")

redis_client = redis.Redis(host=app.config["REDIS_HOST"],
                           port=app.config["REDIS_PORT"],
                           password=app.config["REDIS_PASSWORD"],
                           db=app.config["REDIS_DB"])

redis_workers = "workers"
redis_worker_queue_root = "worker_queue:{0}"
redis_workers_alive_root = "worker_registry:{0}"
redis_ctw_root = "chat_to_worket:{0}"
redis_ctw_expire = 60

def random_string(alphabet, length):
    result = ""
    for i in range(length):
        result += random.choice(alphabet)
    return result


@app.route('/abagofcoffee', methods=['POST', 'GET'])
def process_bot():
    if request.content_type != "application/json":
        print("Bad content type: {0}".format(request.content_type))
        return "Accepted Content-Type: application/json"
    data_utf8 = request.data.decode('UTF-8')
    update = json.loads(data_utf8)
    chat_id = get_chat_id(update)
    update_to_queue(chat_id, request.data)
    return ""


def get_chat_id(update):
    possible_keys = [
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
    ]

    for key in possible_keys:
        if key in update:
            return update[key]["chat"]["id"]

    if "callback_query" in update:
        return update["callback_query"]["message"]["chat"]["id"]

def get_chat_worker(chat_id):
    ctw = redis_ctw_root.format(str(chat_id))

    if not redis_client.exists(ctw):
        new_worker_needed = True
    else:
        worker = redis_client.get(ctw).decode("UTF-8")
        new_worker_needed = not is_alive(worker)

    if new_worker_needed:
        worker = get_new_worker()

    redis_client.setex(ctw, worker, redis_ctw_expire)
    return worker



def get_new_worker():
    while redis_client.scard(redis_workers) != 0:
        candidate = redis_client.srandmember(redis_workers).decode("UTF-8")
        if is_alive(candidate):
            return candidate
        else:
            print("Kicking {0} due to no response".format(candidate))
            redis_client.srem(redis_workers, candidate)
    print("No workers available!")


def is_alive(worker):
    return redis_client.exists(redis_workers_alive_root.format(worker))


def update_to_queue(chat_id, update):
    worker = get_chat_worker(chat_id)
    queue = redis_worker_queue_root.format(worker)
    redis_client.lpush(queue, update)