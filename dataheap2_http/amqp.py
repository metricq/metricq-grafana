import asyncio
import json
import aio_pika

from .constants import MANAGEMENT_BROADCAST_EXCHANGE, MANAGEMENT_CONNECTION, DATA_CONNECTION


async def listen_to_amqp_management(app):
    token = app['token']
    try:
        connection = await aio_pika.connect(
            MANAGEMENT_CONNECTION, loop=app.loop
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


async def get_history_data(app, request):
    targets = [x["target"] for x in request["targets"]]
    results = []

    try:
        connection = await aio_pika.connect(
            DATA_CONNECTION, loop=app.loop
        )

        for target in targets:
            req = {
                "target": target,
                "range": request["range"],
                "intervalMs": request["intervalMs"],
                "maxDataPoints": request["maxDataPoints"]
            }
            results.append(await get_single_history_data(connection, req))

        await connection.close()
    except asyncio.CancelledError:
        pass

    return results


async def get_single_history_data(connection, request):
    try:
        async with connection:
            # Creating channel
            channel = await connection.channel()

            # Declaring queue
            queue = await channel.declare_queue(auto_delete=True)

            exchange = await channel.declare_exchange('historyExchange', passive=True)

            # Send request
            await exchange.publish(
                aio_pika.Message(
                    bytes(json.dumps(request), 'utf-8'),
                    content_type='application/json',
                    reply_to=queue.name
                ),
                request["target"]
            )

            incoming_message = await queue.get(fail=False)
            while not incoming_message:
                incoming_message = await queue.get(fail=False)

            # Confirm message
            incoming_message.ack()

            await queue.delete()
            await channel.close()

            return json.loads(incoming_message.body.decode('utf-8'))
    except aio_pika.exceptions.ChannelClosed:
        pass
    except asyncio.CancelledError:
        pass
