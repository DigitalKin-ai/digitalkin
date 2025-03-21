"""Dev entrypoint for a DigitalKin module's services."""

from digitalkin.services.service_provider import ServiceProvider
from digitalkin.services.storage.grpc_storage import GrpcStorage


class DevelopmentServiceProvider(ServiceProvider):
    """Service Instance used as a development service in a Module."""

    storage = GrpcStorage()
