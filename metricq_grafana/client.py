from typing import Optional, Sequence, Union

from aiocache import SimpleMemoryCache, cached
from metricq import HistoryClient, JsonDict, get_logger

logger = get_logger(__name__)


class Client(HistoryClient):
    @cached(ttl=10 * 60, cache=SimpleMemoryCache, noself=True)
    async def get_metrics(
        self,
        selector: Union[str, Sequence[str], None] = None,
        metadata: bool = True,
        historic: Optional[bool] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> dict[str, JsonDict]:
        return await super().get_metrics(
            selector=selector,
            metadata=metadata,
            historic=historic,
            timeout=timeout,
            **kwargs,
        )
