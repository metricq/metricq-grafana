import math


def sanitize_number(value):
    """Convert NaN and Inf to None - because JSON is dumb"""
    if value is not None and math.isfinite(value):
        return value
    return None


async def unpack_metric(app, metric):
    # guess if this is a pattern (regex) to expand by sending it to the manager
    if "(" in metric and ")" in metric:
        metrics = await app["history_client"].get_metrics(
            metadata=False, historic=True, selector=metric
        )
        return metrics.keys()
    return [metric]
