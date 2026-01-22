"""Default cost."""

from digitalkin.logger import logger
from digitalkin.services.cost.cost_models import CostConfig, CostData, CostType
from digitalkin.services.cost.cost_strategy import (
    CostServiceError,
    CostStrategy,
)


class DefaultCost(CostStrategy):
    """Default cost strategy."""

    def __init__(self, mission_id: str, setup_id: str, setup_version_id: str, config: dict[str, CostConfig]) -> None:
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id, config=config)
        self.db: dict[str, list[CostData]] = {}

    # ══════════════════════════════════ Publics Methods ═══════════════════════════════════ #

    def create(
        self,
        name: str,
        cost_config_name: str,
        quantity: float,
    ) -> None:
        cost_config = self.config.get(cost_config_name)
        if cost_config is None:
            msg = f"Cost config {cost_config_name} not found in the configuration."
            logger.error(msg)
            raise CostServiceError(msg)
        cost_data = CostData.model_validate({
            "name": name,
            "cost": cost_config.rate * quantity,
            "unit": cost_config.unit,
            "type": cost_config.type,
            "mission_id": self.mission_id,
            "rate": cost_config.rate,
            "quantity": quantity,
            "setup_version_id": self.setup_version_id,
        })
        if cost_data.mission_id not in self.db:
            self.db[cost_data.mission_id] = []
        if cost_data.name in [cost.name for cost in self.db[cost_data.mission_id]]:
            msg = f"Cost with name {cost_data.name} already exists in mission {cost_data.mission_id}"
            logger.error(msg)
            raise CostServiceError(msg)
        self.db[cost_data.mission_id].append(cost_data)

    def list(
        self,
        names: list[str] | None = None,
            cost_types: list[CostType] | None = None,
    ) -> list[CostData]:
        if self.mission_id not in self.db:
            msg = f"Mission {self.mission_id} not found in the database."
            logger.warning(msg)
            raise CostServiceError(msg)

        return [
            cost
            for cost in self.db[self.mission_id]
            if (names and cost.name in names) or (cost_types and cost.type in cost_types)
        ]
