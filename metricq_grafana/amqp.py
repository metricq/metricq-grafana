import asyncio
import dataclasses
import functools
import logging
import operator
import time

from metricq import get_logger
from metricq.history_client import HistoryResponseType, HistoryRequestType
from metricq.types import Timestamp

from .functions import parse_functions
from .target import Target
from .utils import unpack_metric

logger = get_logger(__name__)
timer = time.monotonic


async def get_history_data(app, request):
    targets = []
    for target_dict in request["targets"]:
        metrics = await unpack_metric(app, target_dict["metric"])

        for metric in metrics:
            targets.append(
                Target(
                    metric=metric,
                    name=target_dict.get("name", None),
                    functions=list(parse_functions(target_dict)),
                    scaling_factor=float(
                        target_dict.get(
                            "scalingFactor", target_dict.get("scaling_factor", "1")
                        )
                    ),
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

    return rv


async def get_analyze_data(app, request):
    targets = []
    for target_dict in request["targets"]:
        targets.extend(await unpack_metric(app, target_dict["metric"]))

    start_time = Timestamp.from_iso8601(request["range"]["from"])
    end_time = Timestamp.from_iso8601(request["range"]["to"])
    results = await asyncio.gather(
        *[get_analyze_response(app, metric, start_time, end_time) for metric in targets]
    )

    return results


async def get_analyze_response(app, metric, start_time, end_time):
    perf_begin_ns = time.perf_counter_ns()
    aggregate = await app["history_client"].history_aggregate(
        metric,
        start_time,
        end_time,
        timeout=30,
    )
    perf_end_ns = time.perf_counter_ns()

    if aggregate is None:
        return None

    return {
        "target": metric,
        "time_measurements": {
            "http": (perf_end_ns - perf_begin_ns) / 1e9,
        },
        "minimum": aggregate.minimum,
        "maximum": aggregate.maximum,
        "sum": aggregate.sum,
        "count": aggregate.count,
        "integral_ns": aggregate.integral_ns,
        "active_time_ns": aggregate.active_time.ns,
        "mean": aggregate.mean,
        "mean_integral": aggregate.mean_integral,
        "mean_sum": aggregate.mean_sum,
    }


async def get_metric_list(app, search_query, metadata=False, limit=None):
    time_begin = timer()
    get_metrics_args = {"metadata": metadata, "historic": True}
    if search_query.startswith("/") and search_query.endswith("/") and len(search_query) > 1:
        get_metrics_args["selector"] = search_query[1:-1]
        if limit:
            get_metrics_args["limit"] = limit
    else:
        get_metrics_args["infix"] = search_query
        if limit:
            get_metrics_args["limit"] = limit
        elif limit is None:
            get_metrics_args["limit"] = 100

    result = await app["history_client"].get_metrics(**get_metrics_args)
    if result:
        if isinstance(result, list):
            rv = sorted(result)
        else:
            rv = result
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
    try:
        result = results[0]
    except IndexError:
        return {
            "data": [],
            "description": "error: not found in database",
            "unit": "",
        }


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

async def get_history_data_hta(app, request):
    metrics = []
    for metric_dict in request["metrics"]:
        metrics.extend(await unpack_metric(app, metric_dict))

    start_time = Timestamp.from_iso8601(request["range"]["from"])
    end_time = Timestamp.from_iso8601(request["range"]["to"])

    interval = ((end_time - start_time) / request["maxDataPoints"]) * 2

    results = await asyncio.gather(
        *[get_hta_response(app, metric, start_time, end_time, interval) for metric in metrics]
    )
    assert len(metrics) == len(results)
    return dict(zip(metrics, results))


async def get_hta_response(app, metric, start_time, end_time, interval):
    perf_begin_ns = time.perf_counter_ns()
    response = await app["history_client"].history_data_request(
        metric,
        start_time,
        end_time,
        interval,
        request_type=HistoryRequestType.FLEX_TIMELINE
    )
    perf_end_ns = time.perf_counter_ns()
    data_array = []
    mode = ""

    if response is None:
        return None

    if response.mode is HistoryResponseType.EMPTY:
        return None
    elif response.mode is HistoryResponseType.AGGREGATES:
        mode = "aggregates"
        for resp in response.aggregates():
            data_array.append({
                "min": resp.minimum,
                "mean": resp.mean,
                "max": resp.maximum,
                "count": resp.count,
                "time": resp.timestamp.posix_ms})
    elif response.mode is HistoryResponseType.VALUES:
        mode = "raw"
        for resp in response.values():
            data_array.append({
                "value": resp.value,
                "time": resp.timestamp.posix_ms})

    return {
        "mode": mode,
        "time_measurements": {
            "db": response.request_duration,
            "http": (perf_end_ns - perf_begin_ns) / 1e9,
        },
        mode: data_array
    }


