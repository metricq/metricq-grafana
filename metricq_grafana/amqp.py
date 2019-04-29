import asyncio
import datetime
import functools
import re
import operator

from metricq import get_logger
from .utils import Target

logger = get_logger(__name__)


async def get_history_data(app, request):
    targets = [Target.extract_from_string(x["target"]) for x in request["targets"]]
    start_time = int(datetime.datetime.strptime(request["range"]["from"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
    end_time = int(datetime.datetime.strptime(request["range"]["to"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
    interval_ns = request["intervalMs"] * 10 ** 6
    results = await asyncio.gather(*[target.get_response(app, start_time, end_time, interval_ns) for target in targets])

    return functools.reduce(operator.iconcat, results, [])


async def get_metric_list(app, search_query):
    selector = "^(.+\\.)?{}.*$".format(re.escape(search_query))
    result = await app["history_client"].history_metric_list(selector=selector)
    if result:
        lists = [["{}/{}".format(metric, type) for type in ["min", "max", "avg"]] for metric in result]
        return sorted([x for t in zip(*lists) for x in t])
    return []
