"""Main module for running http server"""
import asyncio
import logging
import traceback
from contextlib import suppress

import click

import aio_pika
import aiohttp_cors
import click_completion
import click_log
from aiohttp import web
from metricq import get_logger

from .client import Client
from .routes import setup_routes
from .version import version

logger = get_logger()

click_log.basic_config(logger)
logger.setLevel("INFO")
logger.handlers[0].formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s"
)

click_completion.init()


async def start_background_tasks(app):
    app["history_client"] = Client(
        app["token"], app["management_url"], client_version=version, event_loop=app.loop
    )
    await app["history_client"].connect()

    async def watchdog():
        try:
            await app["history_client"].stopped()
        except Exception as e:
            logger.error("history client encountered an error: {}", e)

        # The "graceful" way to stop an aiohttp runner is
        # about as graceful as an elephant seal, but it works.
        # It's also a private API, but what can we do?
        # https://github.com/aio-libs/aiohttp/issues/2950
        # This is exactly what the signal handlers do.
        # We could maybe be a little bit more graceful by
        # running app.shutdown() and app.cleanup() before this,
        # but then we get recursive mess again.
        # And just runnign shutdown() and cleanup() won't do it either.
        # See also:
        # https://github.com/aio-libs/aiohttp/issues/3638#issuecomment-621256659
        # Note that we suppress the Exception later so it doesn't end
        # up as a dreaded "Task exception was never retrieved"
        raise web.GracefulExit()

    app["history_client_watchdog"] = app.loop.create_task(watchdog())


async def cleanup_background_tasks(app):
    logger.debug("cleanup_background_tasks called")
    with suppress(KeyError):
        app["history_client_watchdog"].cancel()
        # If it was the watchdog who caused the "GracefulExit"
        # Then we can suppress it here, it has already done it's deed
        with suppress(web.GracefulExit):
            await app["history_client_watchdog"]
    with suppress(KeyError):
        await app["history_client"].stop()


def create_app(loop, token, management_url, management_exchange, cors_origin):
    app = web.Application(loop=loop)
    app["token"] = token
    app["management_url"] = management_url
    app["management_exchange"] = management_exchange
    app["last_perf_list"] = []

    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)

    cors = aiohttp_cors.setup(
        app,
        defaults={
            # Allow all to read all CORS-enabled resources from
            f"{cors_origin}": aiohttp_cors.ResourceOptions(
                allow_headers="*",
                allow_methods=("GET", "POST", "OPTIONS"),
                expose_headers="*",
                max_age=1728000,
            )
        },
    )

    setup_routes(app, cors)
    return app


def panic(loop, context):
    print("EXCEPTION: {}".format(context["message"]))
    if context["exception"]:
        print(context["exception"])
        traceback.print_tb(context["exception"].__traceback__)
    loop.stop()


@click.command()
@click.argument("management-url", default="amqp://localhost/")
@click.option("--token", default="metricq-grafana")
@click.option("--management-exchange", default="metricq.management")
@click.option("--debug/--no-debug", default=False)
@click.option("--log-to-journal/--no-log-to-journal", default=False)
@click.option("--host", default="0.0.0.0")
@click.option("--port", default="4000")
@click.option("--cors-origin", default="*")
@click_log.simple_verbosity_option(logger)
@click.version_option(version=version)
def runserver_cmd(
    management_url,
    token,
    management_exchange,
    debug,
    log_to_journal,
    host,
    port,
    cors_origin,
):
    loop = asyncio.get_event_loop()
    if debug:
        logger.warn("Using loop debug - this is slow")
        loop.set_debug(True)

    if log_to_journal:
        try:
            from systemd import journal

            logger.handlers[0] = journal.JournaldLogHandler()
        except ImportError:
            logger.error("Can't enable journal logger, systemd package not found!")

    app = create_app(loop, token, management_url, management_exchange, cors_origin)
    web.run_app(app, host=host, port=port, loop=loop)
