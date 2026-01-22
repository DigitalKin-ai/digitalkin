"""This module contains obejct for setup strategies."""

from pydantic import BaseModel

from digitalkin.services.setup.version.setup_version_models import SetupVersionData


class SetupData(BaseModel):
    """Pydantic model for Setup data validation."""

    id: str
    name: str
    organization_id: str
    owner_id: str
    module_id: str
    current_setup_version: SetupVersionData
