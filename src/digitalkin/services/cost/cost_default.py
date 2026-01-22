"""Default cost."""

from digitalkin.exception.cost import CostServiceError
from digitalkin.logger import logger
from digitalkin.models.services.cost import AmountLimit, CostConfig, CostData, CostType, QuantityLimit
from digitalkin.services.cost.cost_strategy import (
    CostStrategy,
)


class DefaultCost(CostStrategy):
    """Default cost strategy."""

    def __init__(self, mission_id: str, setup_id: str, setup_version_id: str, config: dict[str, CostConfig]) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version this strategy is associated with
            config: The configuration dictionary for the cost
        """
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id, config=config)
        self.db: dict[str, list[CostData]] = {}
        self._limits: dict[str, QuantityLimit | AmountLimit] = {}
        self._accumulated: dict[str, float] = {}

    # ══════════════════════════════════ Publics Methods ═══════════════════════════════════ #

    async def create(
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

    async def list(
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

    async def list_config(self) -> list[CostConfig]:
        return list(self.config.values())

    async def set_config(self, configs: list[CostConfig]) -> bool:
        self.config = {config.cost_name: config for config in configs}
        logger.debug("Cost configs stored in memory: %s", self.config)
        return True

    async def set_limits(self, limits: list[QuantityLimit | AmountLimit]) -> None:
        self._limits = {limit.name: limit for limit in limits}
        self._accumulated = {}

    async def check_limit(self, cost_config_name: str, quantity: float) -> bool:
        limit = self._limits.get(cost_config_name)
        if limit is None:
            return True

        cost_config = self.config.get(cost_config_name)
        if cost_config is None:
            return True

        if limit.limit_type == "quantity":
            current = self._accumulated.get(f"{cost_config_name}_quantity", 0)
            return current + quantity <= limit.max_value

        current = self._accumulated.get(f"{cost_config_name}_amount", 0)
        projected = cost_config.rate * quantity
        return current + projected <= limit.max_value
