"""Module for view functions"""
from asyncio import futures

from aiohttp import web

from metricq import get_logger

from .amqp import get_counter_data, get_counter_list, get_history_data, get_metric_list

logger = get_logger(__name__)


async def query(request):
    req_json = await request.json()
    logger.debug("Query request data: {}", req_json)
    try:
        resp = await get_history_data(request.app, req_json)
    except futures.TimeoutError:
        raise web.HTTPFound()
    return web.json_response(resp)


async def search(request):
    search_query = (await request.json())["target"]
    logger.debug("Search query: {}", search_query)
    metric_list = await get_metric_list(request.app, search_query)
    return web.json_response(metric_list)


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
