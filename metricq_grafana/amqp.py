import asyncio
import calendar
import datetime
import json
import re
from statistics import mean
import time
import uuid

import aio_pika

from metricq import get_logger

logger = get_logger(__name__)

async def get_history_data(app, request):
    targets = [x["target"] for x in request["targets"]]
    results = []

    for target in targets:
        target_split = target.split("/")
        if len(target_split) > 1:
            target_metric = "/".join(target_split[:-1])
            target_types = [target_split[-1]]
        else:
            target_metric = "/".join(target_split[:-1])
            target_types = ["avg"]
        template_var_match = re.fullmatch(r"\((?P<multitype>((min|max|avg)\|?)+)\)", target_types[0])
        if template_var_match:
            target_types = template_var_match.group("multitype").split("|")
        start_time = int(datetime.datetime.strptime(request["range"]["from"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
        end_time = int(datetime.datetime.strptime(request["range"]["to"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
        interval_ns = request["intervalMs"] * 10 ** 6
        perf_start_time = time.perf_counter()
        rep = await app['history_client'].history_data_request(target_metric, start_time, end_time, interval_ns)
        perf_end_time = time.perf_counter()
        time_delta = (perf_end_time - perf_start_time)
        app['last_perf_list'].insert(0, time_delta)
        if 'last_perf_log' not in app or app['last_perf_log'] < datetime.datetime.now() - datetime.timedelta(seconds=10):
            app['last_perf_list'] = app['last_perf_list'][:100]
            max_value = datetime.timedelta(seconds=max(app['last_perf_list']))
            min_value = datetime.timedelta(seconds=min(app['last_perf_list']))
            avg_value = datetime.timedelta(seconds=mean(app['last_perf_list']))
            logger.info('Last 100 metricq data reponse times: min {}, max {}, avg {}', min_value, max_value, avg_value)
            app['last_perf_log'] = datetime.datetime.now()

        for target_type in target_types:
            rep_dict = {"target": "{}/{}".format(target_metric, target_type), "datapoints": [] }
            last_timed = 0

            if target_type == "min":
                zipped_tv = zip(rep.time_delta, rep.value_min)
            elif target_type == "max":
                zipped_tv = zip(rep.time_delta, rep.value_max)
            else:
                zipped_tv = zip(rep.time_delta, rep.value_avg)

            for timed, value in zipped_tv:
                dp = rep_dict["datapoints"]
                last_timed += timed
                dp.append((value , (last_timed) / (10 ** 6) ))
                rep_dict["datapoints"] = dp

            results.append(rep_dict)

    return results

async def get_metric_list(app, search_query):
    selector = "^(.+\\.)?{}.*$".format(re.escape(search_query))
    result = await app["history_client"].history_metric_list(selector=selector)
    if result:
        lists = [["{}/{}".format(metric, type) for type in ["min", "max", "avg"]] for metric in result]
        return sorted([x for t in zip(*lists) for x in t])
    return []
