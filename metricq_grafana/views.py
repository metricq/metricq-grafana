"""Module for view functions"""

import logging
import time
from asyncio import TimeoutError
from json import JSONDecodeError

from aiohttp import web
from metricq import get_logger

from .amqp import get_counter_data, get_counter_list, get_metadata, get_metric_list

logger = get_logger(__name__)


async def view_with_duration_measure(amqp_function, request):
    try:
        req_json = await request.json()
    except JSONDecodeError:
        raise web.HTTPBadRequest()

    logger.debug("{} request data: {}", amqp_function.__name__, req_json)

    try:
        perf_begin_ns = time.perf_counter_ns()
        perf_begin_process_ns = time.process_time_ns()
        resp = await amqp_function(request.app, req_json)
        perf_end_ns = time.perf_counter_ns()
        perf_end_process_ns = time.process_time_ns()
        perf_diff = (perf_end_ns - perf_begin_ns) / 1e9
        headers = {
            "x-request-duration": str(perf_diff),
            "x-request-duration-cpu": str(
                (perf_end_process_ns - perf_begin_process_ns) / 1e9
            ),
        }

        try:
            metrics_length = len(req_json["targets"])
        except KeyError:
            # Catch the first KeyError and try again, but let the
            # next KeyError raise, so it gets converted to BadRequest
            # in the next try-except
            metrics_length = len(req_json["metrics"])

        logger.log(
            logging.DEBUG if perf_diff < 1 else logging.INFO,
            "{} for {} targets took {} s",
            amqp_function.__name__,
            metrics_length,
            perf_diff,
        )
    except TimeoutError:
        # No one responds means not found
        raise web.HTTPNotFound()
    except ValueError:
        raise web.HTTPBadRequest()
    except KeyError:
        raise web.HTTPBadRequest()
    return web.json_response(resp, headers=headers)


async def search(request: web.Request):
    json_data = await request.json()
    search_query = json_data["target"]
    metadata_requested = json_data.get("metadata", False)
    limit = json_data.get("limit")
    logger.debug("Search query: {}", search_query)
    metric_list = await get_metric_list(
        request.app, search_query, metadata=metadata_requested, limit=limit
    )
    return web.json_response(metric_list)


async def metadata(request):
    metric = (await request.json())["target"]
    logger.debug("Metadata query: {}", metric)

    try:
        return web.json_response(await get_metadata(request.app, metric))
    except KeyError as e:
        raise web.HTTPNotFound() from e


async def legacy_cntr_status(request):
    data = await request.post()
    if "selector" not in data:
        raise web.HTTPBadRequest()
    counter_list = await get_counter_list(request.app, data["selector"])
    return web.Response(text="\n".join([";".join(cntr) for cntr in counter_list]))


async def legacy_counter_data(request):
    data = request.query
    if not all([k in data for k in ["cntr", "start", "stop", "width"]]):
        raise web.HTTPBadRequest()
    counter_data = await get_counter_data(
        request.app,
        data["cntr"],
        int(data["start"]),
        int(data["stop"]),
        int(data["width"]),
    )
    return web.json_response(counter_data)


async def test_connection(request):
    raise web.HTTPOk
