import asyncio

import scorep.user
import uvloop
from metricq_grafana import runserver_cmd
from scorep.trace import global_trace

asyncio.get_event_loop().close()


class MySuperEventLoop(asyncio.unix_events._UnixSelectorEventLoop):
    def create_task(self, coro):
        global_trace.unregister()
        task = super().create_task(coro)
        global_trace.register()
        return task

    def create_future(self):
        global_trace.unregister()
        task = super().create_future()
        global_trace.register()
        return task

    def call_soon(self, callback, *args, context=None):
        global_trace.unregister()
        result = super().call_soon(callback, *args, context=context)
        global_trace.register()
        return result

    def call_later(self, delay, callback, *args, context=None):
        global_trace.unregister()
        result = super().call_later(delay, callback, *args, context=context)
        global_trace.register()
        return result


class MySuperEventLoopPolicy(asyncio.events.BaseDefaultEventLoopPolicy):
    def _loop_factory(self):
        return MySuperEventLoop()


# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
asyncio.set_event_loop_policy(MySuperEventLoopPolicy())

runserver_cmd()
