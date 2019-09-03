from abc import ABC, abstractmethod

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
                    Timedelta.from_string(target_dict.get("sma_window"))
                )
            except (TypeError, KeyError):
                pass
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
    def transform_data(self, response_aggregates):
        pass


class AvgFunction(Function):
    def __str__(self):
        return "avg"

    @property
    def _order(self):
        return 2

    def transform_data(self, response_aggregates):
        for timeaggregate in response_aggregates:
            if timeaggregate.count != 0:
                yield timeaggregate.timestamp, timeaggregate.mean


class MinFunction(Function):
    def __str__(self):
        return "min"

    @property
    def _order(self):
        return 3

    def transform_data(self, response_aggregates):
        for timeaggregate in response_aggregates:
            if timeaggregate.count != 0:
                yield timeaggregate.timestamp, timeaggregate.minimum


class MaxFunction(Function):
    def __str__(self):
        return "max"

    @property
    def _order(self):
        return 1

    def transform_data(self, response_aggregates):
        for timeaggregate in response_aggregates:
            if timeaggregate.count != 0:
                yield timeaggregate.timestamp, timeaggregate.maximum


class CountFunction(Function):
    def __str__(self):
        return "count"

    @property
    def _order(self):
        return 0

    def transform_data(self, response_aggregates):
        for timeaggregate in response_aggregates:
            if timeaggregate.count != 0:
                yield timeaggregate.timestamp, timeaggregate.count


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

    def transform_data(self, response_aggregates):
        if len(response_aggregates) == 0:
            return []

        ma_integral = 0
        ma_active_time = 0
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
                ma_integral -= response_aggregates[ma_begin_index].integral * scale

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
                ma_integral += response_aggregates[ma_end_index].integral * scale

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

            yield timeaggregate.timestamp, ma_integral / ma_active_time
