"""Module for view functions"""
from asyncio import futures
from aiohttp import web

from metricq import get_logger

from .amqp import get_history_data, get_metric_list

logger = get_logger(__name__)


async def query(request):
    req_json = await request.json()
    logger.debug("Query request data: {}", req_json)
    try:
        resp = await get_history_data(request.app, req_json)
    except futures.TimeoutError:
        raise web.HTTPFound()
    return web.json_response(
        resp
    )


async def search(request):
    search_query = (await request.json())["target"]
    logger.debug("Search query: {}", search_query)
    metric_list = await get_metric_list(request.app, search_query)
    return web.json_response(metric_list)
