"""Module with all constants."""

DATA_CONNECTION = "amqp://guest:guest@127.0.0.1/"

MANAGEMENT_BROADCAST_EXCHANGE = "dh2.broadcast"
MANAGEMENT_EXCHANGE = "dh2.management"
MANAGEMENT_QUEUE = "management"

METRIC_LIST = [
    {"text": "met1", "value": 1},
    {"text": "met2", "value": 2},
    {"text": "met2.counter1", "value": 3},
    {"text": "dataDrop", "value": 4},
]
