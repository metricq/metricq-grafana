from abc import ABC, abstractmethod

from metricq.history_client import HistoryResponse, HistoryResponseType
from metricq.types import Timedelta


def parse_functions(target_dict):
    for function in target_dict.get("functions", ["avg"]):
        if function == "avg":
            yield AvgFunction()
        elif function == "min":
            yield MinFunction()
        elif function == "max":
            yield MaxFunction()
        elif function == "count":
            yield CountFunction()
        elif function == "sma":
            try:
                yield MovingAverageFunction(
                    Timedelta.from_string(
                        target_dict.get("smaWindow", target_dict.get("sma_window"))
                    )
                )
            except (TypeError, KeyError):
                pass
        # Cannot instantiate RawFunction - it automatically replaces the aggregates when zooming in
        else:
            raise KeyError(f"Unknown function '{function}' requested")


class Function(ABC):
    def __init__(self):
        self.interval = Timedelta(0)

    def __lt__(self, other):
        return self._order < other._order

    @property
    @abstractmethod
    def _order(self):
        pass

    @abstractmethod
    def transform_data(self, response):
        pass


class AggregateFunction(Function, ABC):
    pass


class AvgFunction(AggregateFunction):
    def __str__(self):
        return "avg"

    @property
    def _order(self):
        return 2

    def transform_data(self, response):
        for timeaggregate in response.aggregates():
            if timeaggregate.count != 0:
                yield timeaggregate.timestamp, timeaggregate.mean
            else:
                yield timeaggregate.timestamp, None


class MinFunction(AggregateFunction):
    def __str__(self):
        return "min"

    @property
    def _order(self):
        return 3

    def transform_data(self, response):
        for timeaggregate in response.aggregates():
            if timeaggregate.count != 0:
                yield timeaggregate.timestamp, timeaggregate.minimum
            else:
                yield timeaggregate.timestamp, None


class MaxFunction(AggregateFunction):
    def __str__(self):
        return "max"

    @property
    def _order(self):
        return 1

    def transform_data(self, response):
        for timeaggregate in response.aggregates():
            if timeaggregate.count != 0:
                yield timeaggregate.timestamp, timeaggregate.maximum
            else:
                yield timeaggregate.timestamp, None


class CountFunction(AggregateFunction):
    def __str__(self):
        return "count"

    @property
    def _order(self):
        return 0

    def transform_data(self, response):
        for timeaggregate in response.aggregates():
            if timeaggregate.count != 0:
                yield timeaggregate.timestamp, timeaggregate.count
            else:
                yield timeaggregate.timestamp, None


class RawFunction(Function):
    def __str__(self):
        return "raw"

    @property
    def _order(self):
        return 2

    def transform_data(self, response: HistoryResponse):
        for tv in response.values(convert=False):
            yield tv.timestamp, tv.value


class MovingAverageFunction(Function):
    def __init__(self, interval):
        self.interval = interval

    def __str__(self):
        return "sma"

    @property
    def _order(self):
        return 4

    def _calculate_interval_durations(self, response_aggregates):
        # we assume LAST semantic, but this should not matter for equidistant intervals
        yield Timedelta(0)

        for previous_ta, current_ta in zip(
            response_aggregates, response_aggregates[1:]
        ):
            duration = current_ta.timestamp - previous_ta.timestamp
            # We need strong monotony. DB-HTA guarantees it currently
            assert duration > Timedelta(0)
            yield duration

    def transform_data(self, response):
        if len(response) == 0:
            return []

        response_aggregates = list(response.aggregates(convert=True))

        ma_integral_ns = 0
        ma_active_time = Timedelta(0)
        ma_begin_index = 1
        ma_begin_time = response_aggregates[0].timestamp
        ma_end_index = 1
        ma_end_time = response_aggregates[0].timestamp

        interval_durations = list(
            self._calculate_interval_durations(response_aggregates)
        )

        for timeaggregate, current_interval_duration in zip(
            response_aggregates, interval_durations
        ):
            # The moving average window is symmetric around the current *interval* - not the current point

            # How much time is covered by the current interval width and how much is on both sides "outside"
            assert current_interval_duration >= Timedelta(0)
            outside_duration = self.interval - current_interval_duration
            # If the current interval is wider than the target moving average window, just use the current one
            outside_duration = max(Timedelta(0), outside_duration)
            seek_begin_time = (
                timeaggregate.timestamp
                - current_interval_duration
                - outside_duration / 2
            )
            seek_end_time = timeaggregate.timestamp + outside_duration / 2

            # TODO unify these two loops
            # Move left part of the window
            while ma_begin_time < seek_begin_time:
                next_step_time = min(
                    response_aggregates[ma_begin_index].timestamp, seek_begin_time
                )
                step_duration = next_step_time - ma_begin_time
                # scale can be 0 (everything is nop),
                # 1 (full interval needs to be removed), or something in between
                scale = step_duration.ns / interval_durations[ma_begin_index].ns
                ma_active_time -= (
                    response_aggregates[ma_begin_index].active_time * scale
                )
                ma_integral_ns -= (
                    response_aggregates[ma_begin_index].integral_ns * scale
                )

                ma_begin_time = next_step_time
                assert ma_begin_time <= response_aggregates[ma_begin_index].timestamp
                if ma_begin_time == response_aggregates[ma_begin_index].timestamp:
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
                ma_active_time += response_aggregates[ma_end_index].active_time * scale
                ma_integral_ns += response_aggregates[ma_end_index].integral_ns * scale

                ma_end_time = next_step_time
                assert ma_end_time <= response_aggregates[ma_end_index].timestamp
                if ma_end_time == response_aggregates[ma_end_index].timestamp:
                    # Need to move to the next interval
                    ma_end_index += 1

            if seek_begin_time != ma_begin_time or seek_end_time != ma_end_time:
                # Interval window not complete
                continue
            if ma_active_time == 0:
                continue

            yield timeaggregate.timestamp, ma_integral_ns / ma_active_time.ns
