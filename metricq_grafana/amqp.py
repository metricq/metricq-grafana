import asyncio
import functools
import operator
import time

from metricq import get_logger
from metricq.types import Timedelta, Timestamp

from .functions import parse_functions
from .target import Target

logger = get_logger(__name__)
timer = time.monotonic


async def get_history_data(app, request):
    time_begin = timer()
    targets = []
    for target_dict in request["targets"]:
        targets.append(
            Target(
                metric=target_dict["metric"],
                name=target_dict.get("name", None),
                functions=list(parse_functions(target_dict)),
                scaling_factor=float(target_dict.get("scaling_factor", "1")),
            )
        )

    start_time = Timestamp.from_iso8601(request["range"]["from"])
    end_time = Timestamp.from_iso8601(request["range"]["to"])
    interval = Timedelta.from_ms(request["intervalMs"])
    results = await asyncio.gather(
        *[
            target.get_response(app, start_time, end_time, interval)
            for target in targets
        ]
    )
    rv = functools.reduce(operator.iconcat, results, [])
    logger.info(
        "get_history_data for {} targets took {} s", len(targets), timer() - time_begin
    )
    return rv


async def get_metric_list(app, search_query):
    time_begin = timer()
    result = await app["history_client"].get_metrics(
        infix=search_query, metadata=False, limit=100, historic=True
    )
    if result:
        rv = sorted(result)
    else:
        rv = []
    logger.info(
        "get_metric_list for {} returned {} metrics and took {} s",
        search_query,
        len(rv),
        timer() - time_begin,
    )
    return rv


async def get_counter_list(app, selector):
    time_begin = timer()
    metrics = await app["history_client"].get_metrics(selector=selector, historic=True)
    result = []
    for metric, metadata in metrics.items():
        result.append([metric, metadata.get("description", "")])
    logger.info(
        "get_counter_list for {} returned {} metrics and took {} s",
        selector,
        len(result),
        timer() - time_begin,
    )
    return result


async def get_counter_data(app, metric, start, stop, width):
    time_begin = timer()
    target = Target(metric, order_time_value=True)
    start_time = Timestamp(start * 10 ** 6)
    end_time = Timestamp(stop * 10 ** 6)
    interval = (end_time - start_time) / width
    results, metadata = await asyncio.gather(
        target.get_response(app, start_time, end_time, interval),
        target.get_metadata(app),
    )
    result = results[0] if len(results) > 0 else {"datapoints": []}

    rv = {
        "description": metadata.get("description", ""),
        "unit": metadata.get("unit", ""),
        "data": result["datapoints"],
    }
    logger.info(
        "get_counter_data for {} returned {} values and took {} s",
        metric,
        len(rv["data"]),
        timer() - time_begin,
    )
    return rv
