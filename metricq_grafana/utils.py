import asyncio
import math
import re
import time


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

    @classmethod
    def extract_from_string(cls, target_string: str, order_time_value: bool=False):
        target = cls()
        target_string = target._extract_alias(target_string)
        target_split = target_string.split("/")
        if len(target_split) > 1:
            target.target = "/".join(target_split[:-1])
            target.aggregation_types = [target_split[-1]]
        else:
            target.target = target_string
        template_var_match = re.fullmatch(r"\((?P<multitype>((min|max|avg)\|?)+)\)", target.aggregation_types[0])
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
            suffix_length = len(self.alias_value) + 2  # remove alias string, closing bracket and separation comma
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
        if prefix_length:
            extracted_target_string = target_string[prefix_length:-suffix_length]
        return extracted_target_string

    def get_target_as_regex(self):
        return "^{}$".format(re.escape(self.target))

    def convert_response(self, response, time_measurement):
        results = []
        for target_type in self.aggregation_types:
            rep_dict = {
                "target": (self.get_aliased_target(aggregation_type=target_type)),
                "datapoints": [],
                "time_measurements": {"db": response.request_duration, "http": str(time_measurement)}
            }
            last_timed = 0

            if target_type == "min":
                zipped_tv = zip(response.time_delta, response.value_min)
            elif target_type == "max":
                zipped_tv = zip(response.time_delta, response.value_max)
            else:
                zipped_tv = zip(response.time_delta, response.value_avg)

            for timed, value in zipped_tv:
                dp = rep_dict["datapoints"]
                last_timed += timed
                if not self.order_time_value:
                    dp.append((sanitize_number(value), (last_timed / (10 ** 6))))
                else:
                    dp.append((last_timed / (10 ** 6), sanitize_number(value)))
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
                return "{}/{} ({})".format(self.target.replace(".", "/"), aggregation_type, self.alias_value)
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

    async def pull_data(self, app, start_time_ns, end_time_ns, interval_ns):
        perf_start_time = time.perf_counter_ns()
        self.response = await app['history_client'].history_data_request(self.target, start_time_ns, end_time_ns, interval_ns, timeout=5)
        perf_end_time = time.perf_counter_ns()
        self.time_delta_ns = (perf_end_time - perf_start_time)

    async def get_response(self, app, start_time_ns, end_time_ns, interval_ns):
        await asyncio.wait([self.pull_data(app, start_time_ns, end_time_ns, interval_ns), self.pull_description(app)])

        if self.response is None or self.time_delta_ns is None:
            return []
        return self.convert_response(self.response, self.time_delta_ns)

    async def pull_metadata(self, app):
        result = await app["history_client"].history_metric_metadata(selector=self.get_target_as_regex())
        self.metadata = result.get(self.target, None)

    async def get_metadata(self, app):
        if self.metadata:
            return self.metadata

        await self.pull_metadata(app)
        return self.metadata
