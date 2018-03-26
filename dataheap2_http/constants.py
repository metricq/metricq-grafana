"""Module with all constants."""

MANAGEMENT_CONNECTION = "amqp://guest:guest@127.0.0.1/"
DATA_CONNECTION = "amqp://guest:guest@127.0.0.1/"

MANAGEMENT_BROADCAST_EXCHANGE = "dh2.broadcast"
MANAGEMENT_EXCHANGE = "dh2.management"
MANAGEMENT_QUEUE = "management"

METRIC_LIST = [
    {"text": "met1"},
    {"text": "met2", "expandable": True},
    {"text": "met2.counter1"}
]
