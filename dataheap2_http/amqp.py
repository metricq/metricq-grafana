import asyncio
import calendar
import datetime
import json
import aio_pika
from .history_pb2 import HistoryRequest, HistoryResponse

from .constants import MANAGEMENT_BROADCAST_EXCHANGE, DATA_CONNECTION


async def listen_to_amqp_management(app):
    token = app['token']
    try:
        connection = await aio_pika.connect(
            app['management_url'], loop=app.loop
        )

        async with connection:
            channel = await connection.channel()
            app['management_channel'] = channel

            queue = await channel.declare_queue(
                "client-{}-rpc".format(token)
            )

            management_broadcast_exchange = await channel.declare_exchange(MANAGEMENT_BROADCAST_EXCHANGE, passive=True)

            await queue.bind(management_broadcast_exchange, "")

            async for message in queue:
                with message.process():
                    print(message.body)

    except asyncio.CancelledError:
        del app['management_channel']
    except aio_pika.exceptions.ChannelClosed:
        pass


async def get_history_data(app, request):
    targets = [x["target"] for x in request["targets"]]
    results = []

    try:
        connection = await aio_pika.connect(
            DATA_CONNECTION, loop=app.loop
        )

        async with connection:
            for target in targets:
                req = HistoryRequest()
                req.start_time = int(datetime.datetime.strptime(request["range"]["from"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
                req.end_time = int(datetime.datetime.strptime(request["range"]["to"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp() * (10 ** 9))
                req.interval_ns = request["intervalMs"] * 10 ** 6
                rep = await get_single_history_data(connection, target, req)

                rep_dict = {"target": rep.metric, "datapoints": [] }
                last_timed = 0
                for timed, value in zip(rep.time_delta, rep.value_avg):
                    dp = rep_dict["datapoints"]
                    last_timed += timed
                    dp.append((value , (last_timed) / (10 ** 6) ))
                    rep_dict["datapoints"] = dp
                results.append(rep_dict)

        await connection.close()
    except asyncio.CancelledError:
        pass
    except aio_pika.exceptions.ChannelClosed:
        pass

    return results


async def get_single_history_data(connection, target, request):
    try:
        # Creating channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue(auto_delete=True)

        exchange = await channel.declare_exchange('dh2.history', passive=True)

        # Send request
        await exchange.publish(
            aio_pika.Message(
                request.SerializeToString(),
                reply_to=queue.name
            ),
            target
        )

        incoming_message = await queue.get(fail=False)
        while not incoming_message:
            incoming_message = await queue.get(fail=False)

        # Confirm message
        incoming_message.ack()

        await queue.delete()
        await channel.close()
        del channel

        resp = HistoryResponse()
        resp.ParseFromString(incoming_message.body)
        return resp
    except aio_pika.exceptions.ChannelClosed:
        pass
    except asyncio.CancelledError:
        pass
