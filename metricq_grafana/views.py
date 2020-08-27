"""Module for view functions"""
import time
from asyncio import TimeoutError

from aiohttp import web
from metricq import get_logger

from .amqp import (
    get_counter_data,
    get_counter_list,
    get_history_data,
    get_metric_list,
    get_metadata,
)

logger = get_logger(__name__)


async def query(request):
    req_json = await request.json()
    logger.debug("Query request data: {}", req_json)
    try:
        perf_begin_ns = time.perf_counter_ns()
        perf_begin_process_ns = time.process_time_ns()
        resp = await get_history_data(request.app, req_json)
        perf_end_ns = time.perf_counter_ns()
        perf_end_process_ns = time.process_time_ns()
        headers = {
            "x-request-duration": str((perf_end_ns - perf_begin_ns) / 1e9),
            "x-request-duration-cpu": str(
                (perf_end_process_ns - perf_begin_process_ns) / 1e9
            ),
        }
    except TimeoutError:
        # No one responds means not found
        raise web.HTTPNotFound()
    return web.json_response(resp, headers=headers)


async def search(request):
    search_query = (await request.json())["target"]
    logger.debug("Search query: {}", search_query)
    metric_list = await get_metric_list(request.app, search_query)
    return web.json_response(metric_list)


async def metadata(request):
    metrics = (await request.json())["target"]
    logger.debug("Metadata query: {}", metrics)
    return web.json_response(await get_metadata(request.app, metrics))


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
