import asyncio
import time
from string import Template

from metricq import get_logger
from metricq.history_client import (
    HistoryRequestType,
    HistoryResponse,
    HistoryResponseType,
)

from .functions import AggregateFunction, AvgFunction, RawFunction
from .utils import sanitize_number

logger = get_logger(__name__)


class Target:
    """
     Contains metric, name, aggregates, sma config
    """

    def __init__(
        self,
        metric,
        name=None,
        functions=None,
        order_time_value=False,
        scaling_factor=1,
    ):
        self.metric = metric
        self.name = name if name else "$metric/$function"

        self.functions = functions if functions else [AvgFunction()]
        self.functions.sort()

        self.order_time_value = order_time_value
        self.scaling_factor = scaling_factor

    async def get_metadata(self, app):
        result = await app["history_client"].history_metric_metadata(
            selector=[self.metric]
        )
        return result.get(self.metric, {})

    async def get_response(self, app, start_time, end_time, interval):
        ((data, time_delta_ns), metadata) = await asyncio.gather(
            self._get_data(app, start_time, end_time, interval), self._get_metadata(app)
        )

        if data is None or time_delta_ns is None:
            return []
        return self._convert_response(data, time_delta_ns, metadata)

    @property
    def metadata_required(self):
        keys = [
            key
            for key in self._pattern_keys(self.name)
            if key not in ["metric", "function"]
        ]
        return len(keys) > 0

    async def _get_metadata(self, app):
        if not self.metadata_required:
            return {}

        return await self.get_metadata(app)

    async def _get_data(self, app, start_time, end_time, interval):
        perf_begin_ns = time.perf_counter_ns()
        extension = self._additional_interval / 2
        start_time -= extension
        end_time += extension
        data = await app["history_client"].history_data_request(
            self.metric,
            start_time,
            end_time,
            interval,
            timeout=30,
            request_type=HistoryRequestType.FLEX_TIMELINE,
        )
        perf_end_ns = time.perf_counter_ns()
        return data, (perf_end_ns - perf_begin_ns) / 1e9

    def _get_aliased_target(self, function, metadata) -> str:
        metadata["function"] = str(function)
        metadata["metric"] = self.metric

        return Template(self.name).safe_substitute(**metadata)

    def _convert_response(self, response: HistoryResponse, time_measurement, metadata):
        # TODO find a way to cache the response.aggregates again...
        if response.mode == HistoryResponseType.VALUES:
            # Drop all aggregates and add raw values
            has_aggregate = any(
                [isinstance(f, AggregateFunction) for f in self.functions]
            )
            if has_aggregate:
                self.functions = [
                    f for f in self.functions if not isinstance(f, AggregateFunction)
                ] + [RawFunction()]

        return [
            {
                "target": self._get_aliased_target(function, metadata),
                "time_measurements": {
                    "db": response.request_duration,
                    "http": time_measurement,
                },
                "datapoints": [
                    data for data in self._transform_data(function, response)
                ],
            }
            for function in self.functions
        ]

    def _transform_data(self, function, response):
        for timestamp, value in function.transform_data(response):
            value = value * self.scaling_factor
            if self.order_time_value:
                yield timestamp.posix_ms, sanitize_number(value)
            else:
                yield sanitize_number(value), timestamp.posix_ms

    @property
    def _additional_interval(self):
        return max(function.interval for function in self.functions)

    @staticmethod
    def _pattern_keys(pattern):
        return [s[1] or s[2] for s in Template.pattern.findall(pattern) if s[1] or s[2]]
