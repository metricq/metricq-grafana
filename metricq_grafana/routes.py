"""Module for route setup"""
import functools

from .amqp import get_analyze_data, get_history_data
from .views import (
    legacy_cntr_status,
    legacy_counter_data,
    search,
    metadata,
    test_connection,
    view_with_duration_measure,
)


def setup_routes(app, cors):
    """Setup routes and cors for app."""
    resource = cors.add(app.router.add_resource("/query"))
    cors.add(
        resource.add_route(
            "POST", functools.partial(view_with_duration_measure, get_history_data)
        )
    )

    resource = cors.add(app.router.add_resource("/analyze"))
    cors.add(
        resource.add_route(
            "POST", functools.partial(view_with_duration_measure, get_analyze_data)
        )
    )

    resource = cors.add(app.router.add_resource("/search"))
    cors.add(resource.add_route("POST", search))

    resource = cors.add(app.router.add_resource("/metadata"))
    cors.add(resource.add_route("POST", metadata))

    resource = cors.add(app.router.add_resource("/legacy/cntr_status.php"))
    cors.add(resource.add_route("POST", legacy_cntr_status))

    resource = cors.add(app.router.add_resource("/legacy/counter_data.php"))
    cors.add(resource.add_route("GET", legacy_counter_data))

    resource = cors.add(app.router.add_resource("/"))
    cors.add(resource.add_route("GET", test_connection))
