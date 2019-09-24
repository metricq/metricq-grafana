"""Module for route setup"""
from .views import (
    legacy_cntr_status,
    legacy_counter_data,
    query,
    search,
    test_connection,
)


def setup_routes(app, cors):
    """Setup routes and cors for app."""
    resource = cors.add(app.router.add_resource("/query"))
    cors.add(resource.add_route("POST", query))

    resource = cors.add(app.router.add_resource("/search"))
    cors.add(resource.add_route("POST", search))

    resource = cors.add(app.router.add_resource("/legacy/cntr_status.php"))
    cors.add(resource.add_route("POST", legacy_cntr_status))

    resource = cors.add(app.router.add_resource("/legacy/counter_data.php"))
    cors.add(resource.add_route("GET", legacy_counter_data))

    resource = cors.add(app.router.add_resource("/"))
    cors.add(resource.add_route("GET", test_connection))
