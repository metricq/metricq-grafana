"""Main module for running http server"""
import traceback

import click
import click_completion

import asyncio
from aiohttp import web
import aiohttp_cors
import aio_pika

from metricq.history_client import HistoryClient

from .routes import setup_routes

click_completion.init()


async def start_background_tasks(app):
    app['history_client'] = HistoryClient(app['token'], app["management_url"], event_loop=app.loop)
    await app['history_client'].connect()


async def cleanup_background_tasks(app):
    pass


def create_app(loop, token, management_url, management_exchange):
    app = web.Application(loop=loop)
    app['token'] = token
    app['management_url'] = management_url
    app['management_exchange'] = management_exchange

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
    return app


def panic(loop, context):
    print("EXCEPTION: {}".format(context['message']))
    if context['exception']:
        print(context['exception'])
        traceback.print_tb(context['exception'].__traceback__)
    loop.stop()


@click.command()
@click.argument('management-url', default='amqp://localhost/')
@click.option('--token', default='dh2_http')
@click.option('--management-exchange', default='dh2.management')
#@click_log.simple_verbosity_option(logger)
def runserver_cmd(management_url, token, management_exchange):
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    #loop.set_exception_handler(panic)
    app = create_app(loop, token, management_url, management_exchange)
    # logger.info("starting management loop")
    web.run_app(app, port=4000)
