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

redis_update_queue_root = "update_queue:"


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

    return -1


def update_to_queue(chat_id, update):
    queue = redis_update_queue_root + str(chat_id)
    redis_client.lpush(queue, update)