import asyncio
import math
import re
import time

from metricq import get_logger
from metricq.history_client import HistoryResponse
from metricq.types import Timedelta, Timestamp

logger = get_logger(__name__)


def sanitize_number(value):
    """ Convert NaN and Inf to None - because JSON is dumb """
    if math.isfinite(value):
        return value
    return None


class Target:

    # Target types
    ALIAS = "alias"
    METRIC = "metric"
    DESC = "description"
    METRICDESC = "metricAndDescription"

    def __init__(self):
        self.target = ""
        self.alias_type = None
        self.alias_value = ""
        self.aggregation_types = ["avg"]
        self.response = None
        self.time_delta_ns = None
        self.metadata = None
        self.order_time_value = False
        self.moving_average_interval = None
        self.start_time: Timestamp = None
        self.end_time: Timestamp = None

    @classmethod
    def extract_from_string(cls, target_string: str, order_time_value: bool = False):
        target = cls()
        target_string = target._extract_alias(target_string)
        target_split = target_string.split("/")
        if len(target_split) > 1:
            target.target = "/".join(target_split[:-1])
            target.aggregation_types = [target_split[-1]]
        else:
            target.target = target_string
        template_var_match = re.fullmatch(
            r"\((?P<multitype>((min|max|avg)\|?)+)\)", target.aggregation_types[0]
        )
        if template_var_match:
            target.aggregation_types = template_var_match.group("multitype").split("|")
        target.order_time_value = order_time_value
        return target

    def _extract_alias(self, target_string) -> str:
        extracted_target_string = target_string
        prefix_length = None
        suffix_length = 0
        if target_string.startswith("alias("):
            self.alias_type = Target.ALIAS
            self.alias_value = ",".join(target_string.split(",")[1:])[:-1]
            prefix_length = len("alias(")
            suffix_length = (
                len(self.alias_value) + 2
            )  # remove alias string, closing bracket and separation comma
        elif target_string.startswith("aliasByMetric("):
            self.alias_type = Target.METRIC
            prefix_length = len("aliasByMetric(")
            suffix_length = 1
        elif target_string.startswith("aliasByDescription("):
            self.alias_type = Target.DESC
            self.alias_value = "No description found"
            prefix_length = len("aliasByDescription(")
            suffix_length = 1
        elif target_string.startswith("aliasByMetricAndDescription("):
            self.alias_type = Target.METRICDESC
            self.alias_value = "No description found"
            prefix_length = len("aliasByMetricAndDescription(")
            suffix_length = 1
        elif target_string.startswith("movingAverageWithAlias("):
            self.alias_type = Target.ALIAS
            self.alias_value = ",".join(target_string.split(",")[1:-1])
            self.moving_average_interval = int(target_string.split(",")[-1][:-1])
            prefix_length = len("movingAverageWithAlias(")
            suffix_length = (
                len(",".join(target_string.split(",")[1:])[:-1]) + 2
            )  # remove alias string, separation comma, moving average window, closing bracket and separation comma
        if prefix_length:
            extracted_target_string = target_string[prefix_length:-suffix_length]
        return extracted_target_string

    def get_target_as_regex(self):
        return "^{}$".format(re.escape(self.target))

    def convert_response(self, response: HistoryResponse, time_measurement):
        results = []
        for target_type in self.aggregation_types:
            rep_dict = {
                "target": (self.get_aliased_target(aggregation_type=target_type)),
                "datapoints": [],
                "time_measurements": {
                    "db": response.request_duration,
                    "http": str(time_measurement),
                },
            }

            moving_interval_start = 0
            normal_interval_count = 0
            dp = rep_dict["datapoints"]
            for timeaggregate in response.aggregates(convert=True):
                if timeaggregate.timestamp < self.start_time:
                    moving_interval_start += 1
                if (
                    timeaggregate.timestamp >= self.start_time
                    and timeaggregate.timestamp <= self.end_time
                ):
                    normal_interval_count += 1
                if target_type == "min":
                    value = timeaggregate.minimum
                elif target_type == "max":
                    value = timeaggregate.maximum
                else:
                    value = timeaggregate.mean

                if timeaggregate.count == 0 and not self.moving_average_interval:
                    # Don't insert empty intervals with no data to avoid confusing visualization
                    # However, this would complicate moving average computation, so we retain them in this case
                    continue

                if not self.order_time_value:
                    dp.append(
                        (sanitize_number(value), timeaggregate.timestamp.posix_ms)
                    )
                else:
                    dp.append(
                        (timeaggregate.timestamp.posix_ms, sanitize_number(value))
                    )

            rep_dict["datapoints"] = dp

            if self.moving_average_interval:
                logger.debug(
                    f"Moving average_start {moving_interval_start}, normal count {normal_interval_count}"
                )
                moving_interval_size_half = moving_interval_start
                avg_datapoints = []
                sum = 0
                if not self.order_time_value:
                    value_index = 0
                else:
                    value_index = 1

                last_timed = 0
                for i in range(
                    0, len(rep_dict["datapoints"]) - moving_interval_size_half
                ):
                    timestamp = dp[i][1 - value_index]
                    if timestamp <= last_timed:
                        logger.warning(
                            f"Current timestamp ({i}, {timestamp}) is <= last timestamp ({last_timed})"
                        )
                    last_timed = timestamp
                    sum += dp[i + moving_interval_size_half][value_index]
                    if i >= moving_interval_size_half:
                        sum -= dp[i - moving_interval_size_half][value_index]
                    if (
                        timestamp >= self.start_time.posix_ms
                        and timestamp <= self.end_time.posix_ms
                    ):
                        avg = sum / (moving_interval_size_half * 2 + 1)
                        if not self.order_time_value:
                            avg_datapoints.append((sanitize_number(avg), timestamp))
                        else:
                            avg_datapoints.append((timestamp, sanitize_number(avg)))
                logger.debug(f"Avg {len(avg_datapoints)}")
                last_timed = self.start_time.posix_ms
                for i, (_, timed) in enumerate(avg_datapoints):
                    if timed <= last_timed:
                        logger.warning(f"Current timestamp ({i}) is <= last timestamp")
                    last_timed = timed
                rep_dict["datapoints"] = avg_datapoints

            results.append(rep_dict)

        return results

    def get_aliased_target(self, aggregation_type=None) -> str:
        if not self.alias_type:
            if aggregation_type:
                return "{}/{}".format(self.target, aggregation_type)
            else:
                return self.target
        if self.alias_type == Target.ALIAS or self.alias_type == Target.DESC:
            return self.alias_value
        elif self.alias_type == Target.METRIC:
            if aggregation_type:
                return "{}/{}".format(self.target.replace(".", "/"), aggregation_type)
            else:
                return self.target.replace(".", "/")
        elif self.alias_type == Target.METRICDESC:
            if aggregation_type:
                return "{}/{} ({})".format(
                    self.target.replace(".", "/"), aggregation_type, self.alias_value
                )
            else:
                return "{} ({})".format(self.target.replace(".", "/"), self.alias_value)

    async def pull_description(self, app):
        if self.alias_type not in (Target.DESC, Target.METRICDESC):
            return
        metadata = await self.get_metadata(app)
        if self.target not in metadata:
            return
        if "description" in metadata[self.target]:
            self.alias_value = metadata[self.target]["description"]
        return

    async def pull_data(self, app, start_time, end_time, interval):
        perf_start_time = time.perf_counter_ns()
        before_start_interval = Timedelta(0)
        after_end_interval = Timedelta(0)
        self.start_time = start_time
        self.end_time = end_time
        if self.moving_average_interval:
            before_start_interval = Timedelta(
                (self.moving_average_interval * 10 ** 6) // 2
            )
            after_end_interval = Timedelta(
                (self.moving_average_interval * 10 ** 6) - before_start_interval
            )
        self.response = await app["history_client"].history_data_request(
            self.target,
            start_time - before_start_interval,
            end_time + after_end_interval,
            interval,
            timeout=5,
        )
        perf_end_time = time.perf_counter_ns()
        self.time_delta_ns = perf_end_time - perf_start_time

    async def get_response(self, app, start_time, end_time, interval):
        await asyncio.wait(
            [
                self.pull_data(app, start_time, end_time, interval),
                self.pull_description(app),
            ]
        )

        if self.response is None or self.time_delta_ns is None:
            return []
        return self.convert_response(self.response, self.time_delta_ns)

    async def pull_metadata(self, app):
        result = await app["history_client"].history_metric_metadata(
            selector=self.get_target_as_regex()
        )
        self.metadata = result.get(self.target, None)

    async def get_metadata(self, app):
        if self.metadata:
            return self.metadata

        await self.pull_metadata(app)
        return self.metadata
