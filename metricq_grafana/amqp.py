import asyncio
import calendar
import datetime
import json
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
            target_type = target_split[-1]
        else:
            target_metric = "/".join(target_split[:-1])
            target_type = "avg"
        start_time = int(datetime.datetime.strptime(request["range"]["from"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
        end_time = int(datetime.datetime.strptime(request["range"]["to"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
        interval_ns = request["intervalMs"] * 10 ** 6
        perf_start_time = time.perf_counter()
        rep = await app['history_client'].history_data_request(target_metric, start_time, end_time, interval_ns)
        perf_end_time = time.perf_counter()
        time_delta = (perf_end_time - perf_start_time)
        if 'last_perf_log' not in app or app['last_perf_log'] < datetime.datetime.now() - datetime.timedelta(seconds=10):
            logger.info('current metricq data reponse time: {}', datetime.timedelta(seconds=time_delta))
            app['last_perf_log'] = datetime.datetime.now()

        rep_dict = {"target": target, "datapoints": [] }
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

async def get_metric_list(app):
    result = await app["history_client"].history_metric_list()
    if result:
        lists = [["{}/{}".format(metric, type) for type in ["min", "max", "avg"]] for metric in result]
        return sorted([x for t in zip(*lists) for x in t])
    return []
