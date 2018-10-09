"""Module for route setup"""
from .views import query, search


def setup_routes(app, cors):
    """Setup routes and cors for app."""
    resource = cors.add(app.router.add_resource("/query"))
    cors.add(resource.add_route("POST", query))

    resource = cors.add(app.router.add_resource("/search"))
    cors.add(resource.add_route("POST", search))
