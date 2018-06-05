"""Module for view functions"""
from aiohttp import web

from .amqp import get_history_data, get_metric_list
#from .constants import METRIC_LIST


async def query(request):
    req_json = await request.json()
    print(req_json)
    resp = await get_history_data(request.app, req_json)
    return web.json_response(
        resp
    )


async def search(request):
    search_query = (await request.json())["target"]
    metric_list = await get_metric_list(request.app)
    print(search_query)
    if search_query == "":
        ml = metric_list
    else:
        search_query = search_query.replace("*", "")
        ml = []
        for x in metric_list:
            if x.startswith(search_query):
                m = {"text": x}
                m["text"] = m["text"].lstrip(search_query)
                ml.append(m)
        print(ml)
    return web.json_response(ml)
