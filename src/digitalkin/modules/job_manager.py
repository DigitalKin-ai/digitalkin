"""Background module manager."""

import asyncio
import uuid
from argparse import ArgumentParser, Namespace
from collections.abc import Callable, Coroutine
from typing import Any

from digitalkin.logger import logger
from digitalkin.models import ModuleStatus
from digitalkin.models.module import InputModelT, OutputModelT, SecretModelT, SetupModelT
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode
from digitalkin.utils.arg_parser import ArgParser, DevelopmentModeMappingAction


class JobManager(ArgParser):
    """Background module manager."""

    args: Namespace

    @staticmethod
    async def _job_specific_callback(
        callback: Callable[[str, OutputModelT], Coroutine[Any, Any, None]], job_id: str
    ) -> Callable[[OutputModelT], Coroutine[Any, Any, None]]:
        """Return a callback function for the job.

        Args:
            callback: Callback function to be called when the job is done
            job_id: Identifiant du module

        Returns:
            Callable: Callback function
        """

        def callback_wrapper(output_data: OutputModelT) -> Coroutine[Any, Any, None]:
            """Wrapper for the callback function.

            Args:
                output_data: Output data of the job

            Returns:
                Coroutine: Callback function
            """
            return callback(job_id, output_data)

        return callback_wrapper

    def _add_parser_args(self, parser: ArgumentParser) -> None:
        super()._add_parser_args(parser)
        parser.add_argument(
            "-d",
            "--dev-mode",
            env_var="SERVICE_MODE",
            choices=ServicesMode.__members__,
            default="local",
            action=DevelopmentModeMappingAction,
            dest="services_mode",
            help="Define Module Service configurations for endpoints",
        )

    def __init__(self, module_class: type[BaseModule]) -> None:
        """Initialize the job manager."""
        self.module_class = module_class
        self.modules: dict[str, BaseModule] = {}
        self._lock = asyncio.Lock()
        super().__init__()

        services_config = ServicesConfig(
            services_config_strategies=self.module_class.services_config_strategies,
            services_config_params=self.module_class.services_config_params,
            mode=self.args.services_mode,
        )
        setattr(self.module_class, "services_config", services_config)

    async def create_job(  # noqa: D417
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
        mission_id: str,
        setup_version_id: str,
        callback: Callable[[str, OutputModelT], Coroutine[Any, Any, None]],
    ) -> tuple[str, BaseModule[InputModelT, OutputModelT, SetupModelT, SecretModelT]]:  # type: ignore
        """Start new module job in background (asyncio).

        Args:
            module_class: Classe du module à instancier
            *args: Arguments à passer au constructeur du module
            **kwargs: Arguments à passer au constructeur du module

        Returns:
            str: job_id of the module entity
        """
        job_id = str(uuid.uuid4())
        """TODO: check uniqueness of the job_id"""
        # Création et démarrage du module
        module = self.module_class(job_id, mission_id=mission_id, setup_version_id=setup_version_id)
        self.modules[job_id] = module
        try:
            await module.start(input_data, setup_data, await JobManager._job_specific_callback(callback, job_id))
            logger.info("Module %s (%s) started successfully", job_id, module.name)
        except Exception:
            # En cas d'erreur, supprimer le module du gestionnaire
            del self.modules[job_id]
            logger.exception("Échec du démarrage du module %s: %s", job_id)
            raise
        else:
            return job_id, module

    async def stop_module(self, job_id: str) -> bool:
        """Arrête un module en cours d'exécution.

        Args:
            job_id: Identifiant du module à arrêter

        Returns:
            True si le module a été arrêté, False s'il n'existe pas.
        """
        async with self._lock:
            module = self.modules.get(job_id)
            if not module:
                logger.warning(f"Module {job_id} introuvable")
                return False
            try:
                await module.stop()
                logger.info(f"Module {job_id} ({module.name}) arrêté avec succès")
            except Exception as e:
                logger.error(f"Erreur lors de l'arrêt du module {job_id}: {e}")
                raise
            else:
                return True

    def get_module_status(self, job_id: str) -> ModuleStatus | None:
        """Obtient le statut d'un module.

        Args:
            job_id: Identifiant du module

        Returns:
            Le statut du module ou None si le module n'existe pas.
        """
        module = self.modules.get(job_id)
        return module.status if module else None

    def get_module(self, job_id: str) -> BaseModule | None:
        """Récupère une référence au module.

        Args:
            job_id: Identifiant du module

        Returns:
            Le module ou None s'il n'existe pas.
        """
        return self.modules.get(job_id)

    async def stop_all_modules(self) -> None:
        """Arrête tous les modules en cours d'exécution."""
        async with self._lock:
            stop_tasks = [self.stop_module(job_id) for job_id in list(self.modules.keys())]
            if stop_tasks:
                await asyncio.gather(*stop_tasks, return_exceptions=True)

    def list_modules(self) -> dict[str, dict[str, Any]]:
        """Liste tous les modules avec leur statut.

        Returns:
            Dictionnaire des modules avec leurs informations.
        """
        return {
            job_id: {
                "name": module.name,
                "status": module.status,
                "class": module.__class__.__name__,
            }
            for job_id, module in self.modules.items()
        }
