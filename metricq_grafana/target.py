import asyncio
import time
from string import Template

from metricq import get_logger
from metricq.history_client import HistoryResponse

from .functions import AvgFunction
from .utils import sanitize_number


logger = get_logger(__name__)


class Target:
    """
     Contains metric, name, aggregates, sma config
    """

    def __init__(self, metric, name=None, functions=None, order_time_value=False):
        self.metric = metric
        self.name = name if name else "$metric/$function"
        self.functions = functions if functions else [AvgFunction()]
        self.order_time_value = order_time_value

    async def get_metadata(self, app):
        result = await app["history_client"].history_metric_metadata(
            selector=[self.name]
        )
        return result.get(self.name, None)

    async def get_response(self, app, start_time, end_time, interval):
        metadata = None
        if self.name:
            ((data, time_delta_ns), metadata) = await asyncio.gather(
                self._get_data(app, start_time, end_time, interval),
                self.get_metadata(app),
            )
        else:
            data, time_delta_ns = await self._get_data(
                app, start_time, end_time, interval
            )

        if data is None or time_delta_ns is None:
            return []
        return self._convert_response(data, time_delta_ns, metadata)

    async def _get_data(self, app, start_time, end_time, interval):
        perf_begin_ns = time.perf_counter_ns()
        extension = self._additional_interval / 2
        start_time -= extension
        end_time += extension
        data = await app["history_client"].history_data_request(
            self.metric, start_time, end_time, interval, timeout=30
        )
        perf_end_ns = time.perf_counter_ns()
        return data, (perf_end_ns - perf_begin_ns) / 1e9

    def _get_aliased_target(self, function, metadata) -> str:
        if metadata is None:
            metadata = {}
        metadata["function"] = str(function)
        metadata["metric"] = self.metric

        return Template(self.name).safe_substitute(**metadata)

    def _convert_response(self, response: HistoryResponse, time_measurement, metadata):
        response_aggregates = list(response.aggregates(convert=True))

        return [
            {
                "target": self._get_aliased_target(function, metadata),
                "time_measurements": {
                    "db": response.request_duration,
                    "http": time_measurement,
                },
                "datapoints": [
                    data for data in self._transform_data(function, response_aggregates)
                ],
            }
            for function in self.functions
        ]

    def _transform_data(self, function, response_aggregates):
        for timestamp, value in function.transform_data(response_aggregates):
            if not self.order_time_value:
                yield sanitize_number(value), timestamp.posix_ms
            else:
                yield timestamp.posix_ms, sanitize_number(value)

    @property
    def _additional_interval(self):
        return max(function.interval for function in self.functions)
