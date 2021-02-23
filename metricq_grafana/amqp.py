import asyncio
import functools
import logging
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
        metrics = [target_dict["metric"]]
        if "(" in target_dict["metric"] and ")" in target_dict["metric"]:
            metrics = await app["history_client"].get_metrics(
                metadata=False, historic=True, selector=target_dict["metric"]
            )
        for metric in metrics:
            targets.append(
                Target(
                    metric=metric,
                    name=target_dict.get("name", None),
                    functions=list(parse_functions(target_dict)),
                    scaling_factor=float(target_dict.get("scaling_factor", "1")),
                )
            )

    start_time = Timestamp.from_iso8601(request["range"]["from"])
    end_time = Timestamp.from_iso8601(request["range"]["to"])
    # Grafana gives inconsistent information here:
    #    intervalMs is very coarse grained
    #    maxDataPoints is not really the number of pixels, usually less
    # interval = Timedelta.from_ms(request["intervalMs"])
    interval = ((end_time - start_time) / request["maxDataPoints"]) * 2
    results = await asyncio.gather(
        *[
            target.get_response(app, start_time, end_time, interval)
            for target in targets
        ]
    )
    rv = functools.reduce(operator.iconcat, results, [])
    time_diff = timer() - time_begin
    logger.log(
        logging.DEBUG if time_diff < 1 else logging.INFO,
        "get_history_data for {} targets took {} s",
        len(targets),
        time_diff,
    )

    return rv


async def get_metric_list(app, search_query):
    time_begin = timer()
    get_metrics_args = {"metadata": False, "historic": True}
    if search_query.startswith("/") and search_query.endswith("/"):
        get_metrics_args["selector"] = search_query[1:-1]
    else:
        get_metrics_args["infix"] = search_query
        get_metrics_args["limit"] = 100

    result = await app["history_client"].get_metrics(**get_metrics_args)
    if result:
        rv = sorted(result)
    else:
        rv = []
    time_diff = timer() - time_begin
    logger.log(
        logging.DEBUG if time_diff < 1 else logging.INFO,
        "get_metric_list for {} returned {} metrics and took {} s",
        search_query,
        len(rv),
        time_diff,
    )
    return rv


async def get_metadata(app, metric):
    time_begin = timer()
    result = await app["history_client"].get_metrics(selector=[metric])
    logger.info(
        "get_metadata for {} returned {} metrics and took {} s",
        metric,
        len(result),
        timer() - time_begin,
    )
    if result:
        return result
    else:
        raise KeyError(f"Could not find any metadata for '{metric}'")


async def get_counter_list(app, selector):
    time_begin = timer()
    metrics = await app["history_client"].get_metrics(selector=selector, historic=True)
    result = []
    for metric, metadata in metrics.items():
        result.append([metric, metadata.get("description", "")])
    time_diff = timer() - time_begin
    logger.log(
        logging.DEBUG if time_diff < 1 else logging.INFO,
        "get_counter_list for {} returned {} metrics and took {} s",
        selector,
        len(result),
        time_diff,
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

    datapoints = [
        datapoint for datapoint in result["datapoints"] if start <= datapoint[0] <= stop
    ]

    rv = {
        "description": metadata.get("description", ""),
        "unit": metadata.get("unit", ""),
        "data": datapoints,
    }
    time_diff = timer() - time_begin
    logger.log(
        logging.DEBUG if time_diff < 1 else logging.INFO,
        "get_counter_data for {} returned {} values and took {} s",
        metric,
        len(rv["data"]),
        time_diff,
    )
    return rv
