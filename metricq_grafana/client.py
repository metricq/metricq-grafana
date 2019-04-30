from aiocache import cached, SimpleMemoryCache

from metricq import HistoryClient


class Client(HistoryClient):
    @cached(ttl=10 * 60, cache=SimpleMemoryCache, noself=True)
    async def history_metric_metadata(self, selector=None):
        return await super().history_metric_metadata(selector=selector)
