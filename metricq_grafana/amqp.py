import asyncio
import functools
import re
import operator

from metricq import get_logger
from .utils import Target

logger = get_logger(__name__)


async def get_history_data(app, request):
    targets = [Target.extract_from_string(x["target"]) for x in request["targets"]]
    results = await asyncio.gather(*[target.get_response(app, request) for target in targets])

    return functools.reduce(operator.iconcat, results, [])


async def get_metric_list(app, search_query):
    selector = "^(.+\\.)?{}.*$".format(re.escape(search_query))
    result = await app["history_client"].history_metric_list(selector=selector)
    if result:
        lists = [["{}/{}".format(metric, type) for type in ["min", "max", "avg"]] for metric in result]
        return sorted([x for t in zip(*lists) for x in t])
    return []
