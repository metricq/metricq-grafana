import asyncio
import calendar
import datetime
import json
import uuid
import aio_pika
from .history_pb2 import HistoryRequest, HistoryResponse

from .constants import MANAGEMENT_BROADCAST_EXCHANGE, DATA_CONNECTION, MANAGEMENT_EXCHANGE


async def listen_to_amqp_management(app):
    token = app['token']
    try:
        connection = await aio_pika.connect(
            app['management_url'], loop=app.loop, ssl=app["management_url"].startswith("amqps"),
        )

        async with connection:
            channel = await connection.channel()
            app['management_channel'] = channel

            queue = await channel.declare_queue(
                "client-{}-rpc".format(token)
            )
            app["rpc_futures"] = {}
            app["management_queue"] = queue

            management_exchange = await channel.declare_exchange(MANAGEMENT_EXCHANGE, passive=True)
            app['management_exchange'] = management_exchange

            management_broadcast_exchange = await channel.declare_exchange(MANAGEMENT_BROADCAST_EXCHANGE, passive=True)

            await queue.bind(management_broadcast_exchange, "")

            async for message in queue:
                with message.process():
                    body = message.body.decode()
                    from_token = message.app_id
                    correlation_id = message.correlation_id.decode()

                    app.logger.info('received message from {}, correlation id: {}, reply_to: {}\n{}',
                        from_token, correlation_id, message.reply_to, body)
                    arguments = json.loads(body)
                    arguments['from_token'] = from_token

                    if correlation_id in app["rpc_futures"]:
                        future = app["rpc_futures"][correlation_id]
                        future.set_result(arguments)


    except asyncio.CancelledError:
        del app["management_queue"]
        del app['management_exchange']
        del app['management_channel']
    except aio_pika.exceptions.ChannelClosed:
        pass


async def get_history_data(app, request):
    targets = [x["target"] for x in request["targets"]]
    results = []

    try:
        connection = await aio_pika.connect(
            DATA_CONNECTION, loop=app.loop, ssl=app["management_url"].startswith("amqps"),
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


async def get_metric_list(app):
    token = app["token"]
    try:
        if "management_exchange" not in app or "management_queue" not in app:
            return []
        correlation_id = 'dh2-rpc-py-{}-{}'.format(token, uuid.uuid4().hex)
        request = {
            "function": "history.get_metric_list",
        }

        body = json.dumps(request).encode()

        future = asyncio.Future(loop=app.loop)
        app["rpc_futures"][correlation_id] = future
        await app["management_exchange"].publish(
            aio_pika.Message(
                body,
                correlation_id=correlation_id,
                app_id=token,
                reply_to=app["management_queue"].name,
                content_type='application/json'
            ),
            routing_key="http.get_metric_list"
        )
        done, _ = await asyncio.wait([future], loop=app.loop)
        result = future.result()
        if "metric_list" in result:
            return result["metric_list"]
        return []
    except aio_pika.exceptions.ChannelClosed:
        pass
    except asyncio.CancelledError:
        pass
    return []
