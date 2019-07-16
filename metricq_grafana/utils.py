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
    ALIAS = "custom"
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
        if target.moving_average_interval:
            target.aggregation_types = ["sma"]
        target.order_time_value = order_time_value
        return target

    @classmethod
    def extract_from_dict(cls, target_dict: dict, order_time_value: bool = False):
        target = cls()
        target.target = target_dict["target_metric"]
        target.alias_type = target_dict.get("alias_type", None)
        target.alias_value = target_dict.get("alias_value", "")
        target.aggregation_types = target_dict.get("aggregates", ["avg"])
        # TODO properly check if we need the interval or not
        try:
            target.moving_average_interval = Timedelta.from_string(
                target_dict.get("sma_window", "0h")
            )
        except TypeError:
            target.moving_average_interval = Timedelta(0)

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
            self.moving_average_interval = Timedelta.from_string(
                target_string.split(",")[-1][:-1]
            )
            prefix_length = len("movingAverageWithAlias(")
            suffix_length = (
                len(",".join(target_string.split(",")[1:])[:-1]) + 2
            )  # remove alias string, separation comma, moving average window, closing bracket and separation comma
        if prefix_length:
            extracted_target_string = target_string[prefix_length:-suffix_length]
        return extracted_target_string

    def convert_response(self, response: HistoryResponse, time_measurement):
        results = []
        response_aggregates = response.aggregates(convert=True)
        for target_type in self.aggregation_types:
            rep_dict = {
                "target": (self.get_aliased_target(aggregation_type=target_type)),
                "time_measurements": {
                    "db": response.request_duration,
                    "http": str(time_measurement),
                },
                "datapoints": [],
            }

            response_aggregates = list(response_aggregates)
            if len(response_aggregates) == 0:
                results.append(rep_dict)
                continue

            dp = []

            if target_type == "sma":
                ma_integral = 0
                ma_active_time = 0
                ma_begin_index = 1
                ma_begin_time = response_aggregates[0].timestamp
                ma_end_index = 1
                ma_end_time = response_aggregates[0].timestamp

                # we assume LAST semantic, but this should not matter for equidistant intervals
                interval_durations = [
                    current_ta.timestamp - previous_ta.timestamp
                    for previous_ta, current_ta in zip(
                        response_aggregates, response_aggregates[1:]
                    )
                ]
                # We can't handle this now...
                assert min(interval_durations) > Timedelta(0)
                interval_durations = [Timedelta(0)] + interval_durations

                for timeaggregate, current_interval_duration in zip(
                    response_aggregates, interval_durations
                ):
                    # The moving average window is symmetric around the current *interval* - not the current point
                    # How much time is covered by the current interval width and how much is on both sides "outside"
                    assert current_interval_duration >= Timedelta(0)
                    outside_duration = (
                        self.moving_average_interval - current_interval_duration
                    )
                    # If the current interval is wider than the target moving average window, just use the current one
                    outside_duration = max(Timedelta(0), outside_duration)
                    seek_begin_time = (
                        timeaggregate.timestamp
                        - current_interval_duration
                        - outside_duration / 2
                    )
                    seek_end_time = timeaggregate.timestamp + outside_duration / 2

                    # TODO unify this code
                    # Move left part of the window
                    while ma_begin_time < seek_begin_time:
                        next_step_time = min(
                            response_aggregates[ma_begin_index].timestamp,
                            seek_begin_time,
                        )
                        step_duration = next_step_time - ma_begin_time
                        # scale can be 0 (everything is nop),
                        # 1 (full interval needs to be removed), or something in between
                        scale = step_duration.ns / interval_durations[ma_begin_index].ns
                        ma_active_time -= (
                            response_aggregates[ma_begin_index].active_time * scale
                        )
                        ma_integral -= (
                            response_aggregates[ma_begin_index].integral * scale
                        )

                        ma_begin_time = next_step_time
                        assert (
                            ma_begin_time
                            <= response_aggregates[ma_begin_index].timestamp
                        )
                        if (
                            ma_begin_time
                            == response_aggregates[ma_begin_index].timestamp
                        ):
                            # Need to move to the next interval
                            ma_begin_index += 1

                    # Move right part of the window
                    while ma_end_time < seek_end_time and ma_end_index < len(
                        response_aggregates
                    ):
                        next_step_time = min(
                            response_aggregates[ma_end_index].timestamp, seek_end_time
                        )
                        step_duration = next_step_time - ma_end_time
                        # scale can be 0 (everything is nop),
                        # 1 (full interval needs to be removed), or something in between
                        scale = step_duration.ns / interval_durations[ma_end_index].ns
                        ma_active_time += (
                            response_aggregates[ma_end_index].active_time * scale
                        )
                        ma_integral += (
                            response_aggregates[ma_end_index].integral * scale
                        )

                        ma_end_time = next_step_time
                        assert (
                            ma_end_time <= response_aggregates[ma_end_index].timestamp
                        )
                        if ma_end_time == response_aggregates[ma_end_index].timestamp:
                            # Need to move to the next interval
                            ma_end_index += 1

                    if seek_begin_time != ma_begin_time or seek_end_time != ma_end_time:
                        # Interval window not complete
                        continue
                    if ma_active_time == 0:
                        continue
                    value = ma_integral / ma_active_time

                    if not self.order_time_value:
                        dp.append((sanitize_number(value), timestamp.posix_ms))
                    else:
                        dp.append((timestamp.posix_ms, sanitize_number(value)))

            if target_type != "sma":
                for timeaggregate in response_aggregates:
                    timestamp = timeaggregate.timestamp
                    if target_type == "min":
                        value = timeaggregate.minimum
                    elif target_type == "max":
                        value = timeaggregate.maximum
                    elif target_type == "count":
                        value = timeaggregate.count
                    else:
                        value = timeaggregate.mean

                    if timeaggregate.count == 0:
                        # Don't insert empty intervals with no data to avoid confusing visualization
                        # However, this would complicate moving average computation, so we retain them in this case
                        continue

                    if not self.order_time_value:
                        dp.append((sanitize_number(value), timestamp.posix_ms))
                    else:
                        dp.append((timestamp.posix_ms, sanitize_number(value)))

            rep_dict["datapoints"] = dp
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
        self.start_time = start_time
        self.end_time = end_time
        if self.moving_average_interval:
            start_time -= self.moving_average_interval / 2
            end_time += self.moving_average_interval / 2
        self.response = await app["history_client"].history_data_request(
            self.target, start_time, end_time, interval, timeout=5
        )
        perf_end_time = time.perf_counter_ns()
        self.time_delta_ns = perf_end_time - perf_start_time

    async def get_response(self, app, start_time, end_time, interval):
        await asyncio.gather(
            self.pull_data(app, start_time, end_time, interval),
            self.pull_description(app),
        )

        if self.response is None or self.time_delta_ns is None:
            return []
        return self.convert_response(self.response, self.time_delta_ns)

    async def pull_metadata(self, app):
        result = await app["history_client"].history_metric_metadata(
            selector=[self.target]
        )
        self.metadata = result.get(self.target, None)

    async def get_metadata(self, app):
        if self.metadata:
            return self.metadata

        await self.pull_metadata(app)
        return self.metadata
