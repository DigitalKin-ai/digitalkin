"""Default entrypoint for a DigitalKin module's services."""

from digitalkin.services.service_provider import ServiceProvider
from digitalkin.services.storage.default_storage import DefaultStorage


class DefaultServiceProvider(ServiceProvider):
    """Service Instance used as default service in a Module.

    Currently only allow the default (local) database service.
    """

    storage = DefaultStorage()
