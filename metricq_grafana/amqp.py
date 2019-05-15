import asyncio
import datetime
import functools
import re
import operator
import time

from metricq import get_logger
from metricq.types import Timestamp, Timedelta
from .utils import Target

logger = get_logger(__name__)
timer = time.monotonic


async def get_history_data(app, request):
    time_begin = timer()
    targets = [Target.extract_from_string(x["target"]) for x in request["targets"]]
    start_time = Timestamp.from_iso8601(request["range"]["from"])
    end_time = Timestamp.from_iso8601(request["range"]["to"])
    interval = Timedelta(request["intervalMs"] * 10 ** 6)
    results = await asyncio.gather(*[target.get_response(app, start_time, end_time, interval) for target in targets])
    rv = functools.reduce(operator.iconcat, results, [])
    logger.info('get_history_data for {} targets took {} s', len(targets), timer() - time_begin)
    return rv


async def get_metric_list(app, search_query):
    time_begin = timer()
    selector = "^(.+\\.)?{}.*$".format(re.escape(search_query))
    result = await app["history_client"].history_metric_list(selector=selector)
    if result:
        lists = [["{}/{}".format(metric, type) for type in ["min", "max", "avg"]] for metric in result]
        rv = sorted([x for t in zip(*lists) for x in t])
    else:
        rv = []
    logger.info('get_metric_list for {} returned {} metrics and took {} s',
                search_query, len(rv), timer() - time_begin)
    return rv


async def get_counter_list(app, selector):
    time_begin = timer()
    metrics = await app["history_client"].history_metric_metadata(selector=selector)
    result = []
    for metric, metadata in metrics.items():
        result.append([metric, metadata.get("description", "")])
    logger.info('get_counter_list for {} returned {} metrics and took {} s',
                selector, len(result), timer() - time_begin)
    return result


async def get_counter_data(app, metric, start, stop, width):
    time_begin = timer()
    target = Target.extract_from_string(metric, order_time_value=True)
    start_time = Timestamp(start * 10 ** 6)
    end_time = Timestamp(stop * 10 ** 6)
    interval = (end_time - start_time) / width
    results, metadata = await asyncio.gather(
        target.get_response(app, start_time, end_time, interval),
        target.get_metadata(app))
    result = results[0] if len(results) > 0 else {"datapoints": []}

    rv = {
        "description": metadata.get("description", ""),
        "unit": metadata.get("unit", ""),
        "data": result["datapoints"]
    }
    logger.info('get_counter_data for {} returned {} values and took {} s',
                metric, len(rv['data']), timer() - time_begin)
    return rv