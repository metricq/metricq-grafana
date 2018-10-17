"""Module for view functions"""
from aiohttp import web

from metricq import get_logger

from .amqp import get_history_data, get_metric_list

logger = get_logger(__name__)


async def query(request):
    req_json = await request.json()
    logger.debug("Query request data: {}", req_json)
    resp = await get_history_data(request.app, req_json)
    return web.json_response(
        resp
    )


async def search(request):
    search_query = (await request.json())["target"]
    metric_list = await get_metric_list(request.app)
    logger.debug("Search query: {}", search_query)
    if search_query == "":
        ml = metric_list
    else:
        search_query = search_query.replace("*", "")
        ml = []
        for x in metric_list:
            if x.startswith(search_query):
                ml.append(x)
    return web.json_response(ml)
