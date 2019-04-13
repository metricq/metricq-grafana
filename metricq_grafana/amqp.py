import asyncio
import calendar
import datetime
import json
import re
from statistics import mean
import time
import uuid
import math

import aio_pika

from metricq import get_logger
from .utils import Target

logger = get_logger(__name__)


async def get_history_data(app, request):
    targets = [Target.extract_from_string(x["target"]) for x in request["targets"]]
    results = []

    for target in targets:
        pull_description_future = target.pull_description(app)
        start_time = int(datetime.datetime.strptime(request["range"]["from"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
        end_time = int(datetime.datetime.strptime(request["range"]["to"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
        interval_ns = request["intervalMs"] * 10 ** 6
        perf_start_time = time.perf_counter_ns()
        rep = await app['history_client'].history_data_request(target.target, start_time, end_time, interval_ns)
        perf_end_time = time.perf_counter_ns()
        time_delta_ns = (perf_end_time - perf_start_time)
        time_delta = time_delta_ns / (10 ** 9)
        app['last_perf_list'].insert(0, time_delta)
        if 'last_perf_log' not in app or app['last_perf_log'] < datetime.datetime.now() - datetime.timedelta(seconds=10):
            app['last_perf_list'] = app['last_perf_list'][:100]
            max_value = datetime.timedelta(seconds=max(app['last_perf_list']))
            min_value = datetime.timedelta(seconds=min(app['last_perf_list']))
            avg_value = datetime.timedelta(seconds=mean(app['last_perf_list']))
            logger.info('Last 100 metricq data reponse times: min {}, max {}, avg {}', min_value, max_value, avg_value)
            app['last_perf_log'] = datetime.datetime.now()

        await pull_description_future
        results.extend(target.convert_response(rep, time_delta_ns))

    return results


async def get_metric_list(app, search_query):
    selector = "^(.+\\.)?{}.*$".format(re.escape(search_query))
    result = await app["history_client"].history_metric_list(selector=selector)
    if result:
        lists = [["{}/{}".format(metric, type) for type in ["min", "max", "avg"]] for metric in result]
        return sorted([x for t in zip(*lists) for x in t])
    return []
