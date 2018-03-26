"""Main module for running http server"""
import asyncio
from aiohttp import web
import aiohttp_cors
import aio_pika

from .amqp import listen_to_amqp_management
from .routes import setup_routes


async def start_background_tasks(app):
    app['rabbitmq_listener'] = app.loop.create_task(listen_to_amqp_management(app))


async def cleanup_background_tasks(app):
    app['rabbitmq_listener'].cancel()
    await app['rabbitmq_listener']


app = web.Application()
app.on_startup.append(start_background_tasks)
app.on_cleanup.append(cleanup_background_tasks)

cors = aiohttp_cors.setup(app, defaults={
        # Allow all to read all CORS-enabled resources from
        # http://client.example.org.
        "http://localhost:3000": aiohttp_cors.ResourceOptions(
            allow_headers=("Content-Type", ),
        ),
    })

setup_routes(app, cors)

if __name__ == "__main__":

    web.run_app(app, port=4000)
