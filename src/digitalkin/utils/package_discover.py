"""Secure module discovery and import utility for trigger handlers."""

import importlib
import importlib.util
import logging
import pkgutil
import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import ClassVar

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.module_types import DataTrigger
from digitalkin.modules.trigger_handler import TriggerHandler
from digitalkin.utils.exceptions import DiscoveryError, UnsafePackageError

logger = logging.getLogger(__name__)


class ModuleDiscoverer:
    """Encapsulates secure, structured discovery and import of trigger modules.

    Attributes:
        packages: List of Python package paths to scan.
        file_pattern: Glob pattern to match module filenames.
        safe_mode: If True, skips unsafe imports.
        max_file_size: Maximum file size allowed for import (bytes).
    """

    FORBIDDEN_MODULE_PATTERNS: ClassVar[set[str]] = {
        "__pycache__",
        ".pyc",
        ".pyo",
        ".pyd",
        "test_",
        "_test",
        "conftest",
    }

    _trigger_handlers_cls: ClassVar[dict[str, list[type[TriggerHandler]]]] = {}

    def _validate_inputs(self) -> None:
        """Validate initial discovery inputs.

        Raises:
            DiscoveryError: If packages list is invalid.
            UnsafePackageError: If file pattern or package names are unsafe.
        """
        if not self.packages or not isinstance(self.packages, list):
            msg = "Packages must be a non-empty list"
            raise DiscoveryError(msg)
        self._validate_file_pattern()
        for pkg in self.packages:
            self._validate_package_name(pkg)

    def _discover_package(self, package_name: str) -> dict[str, bool]:
        """Import a package and scan its __path__ for modules.

        Args:
            package_name: Dotted path of the package to scan.

        Returns:
            Mapping of module names to import status.
        """
        try:
            pkg = importlib.import_module(package_name)
        except ImportError:
            logger.exception("Could not import package %s", package_name)
            return {}

        # getattr unavoidable: __path__ only exists on packages, not regular modules (Python runtime)
        paths = getattr(pkg, "__path__", None)
        if not paths:
            logger.warning("Package %s has no __path__", package_name)
            return {}

        results: dict[str, bool] = {}
        for path_str in paths:
            base_path = Path(path_str).resolve()
            results.update(self._discover_in_path(package_name, base_path))
        return results

    def _discover_in_path(self, package_name: str, base_path: Path) -> dict[str, bool]:
        """Walk a filesystem path to locate and process modules.

        Args:
            package_name: Root package name for prefixing.
            base_path: Filesystem path of the package.

        Returns:
            Mapping of module names to import status.
        """
        results: dict[str, bool] = {}
        if not base_path.is_dir():
            logger.warning("Invalid package path: %s", base_path)
            return results

        for _, module_name, is_pkg in pkgutil.walk_packages(
            [str(base_path)], prefix=f"{package_name}.", onerror=lambda e: logger.error("Walk error: %s", e)
        ):
            if is_pkg or module_name in results:
                continue
            results[module_name] = self._process_module(module_name, base_path, package_name)
        return results

    def _process_module(self, module_name: str, base_path: Path, package_name: str) -> bool:
        """Validate module file, import it, and validate the trigger class.

        Args:
            module_name: Full dotted module path.
            base_path: Filesystem base path of the package.
            package_name: Root package for path resolution.

        Returns:
            True if import and validation succeed, False otherwise.
        """
        try:  # noqa: PLW0717
            module_file = self._module_file_path(module_name, base_path, package_name)
            self._validate_module_path(module_file, base_path)
            if not fnmatch(module_file.name, self.file_pattern):
                return False
            if not self._is_safe_module_name(module_name):
                logger.debug("Skipping unsafe module: %s", module_name)
                return False
            if not self._safe_import_module(module_name, module_file):
                return False

        except UnsafePackageError:
            logger.exception("Security violation %s", module_name)
            return False
        except Exception:
            logger.exception("Error processing %s", module_name)
            return False
        return True

    @staticmethod
    def _module_file_path(module_name: str, base_path: Path, package_name: str) -> Path:
        """Compute filesystem Path for a module's .py file.

        Args:
            module_name: Full module name.
            base_path: Base directory of the package.
            package_name: Root package prefix.

        Returns:
            Path to the module's .py file.
        """
        rel = module_name.replace(f"{package_name}.", "").replace(".", "/")
        return base_path / f"{rel}.py"

    @staticmethod
    def _validate_package_name(package_name: str) -> None:
        """Validate that a package name is safe and well-formed.

        Args:
            package_name: Dotted Python package name.

        Raises:
            UnsafePackageError: On invalid package names.
        """
        if not package_name or not isinstance(package_name, str):
            msg = "Package name must be a non-empty string"
            raise UnsafePackageError(msg)
        if any(part in package_name for part in ("..", "/", "\\", "\x00")):
            msg = f"Invalid package name: {package_name}"
            raise UnsafePackageError(msg)
        if not all(part.isidentifier() for part in package_name.split(".")):
            msg = f"Invalid Python package name: {package_name}"
            raise UnsafePackageError(msg)

    def _validate_file_pattern(self) -> None:
        """Validate that the file glob pattern is safe.

        Raises:
            UnsafePackageError: On dangerous patterns.
        """
        pattern = self.file_pattern
        if not pattern or not isinstance(pattern, str):
            msg = "File pattern must be a non-empty string"
            raise UnsafePackageError(msg)
        if any(d in pattern for d in ("..", "/", "\\", "\x00", "**/")):
            msg = f"Dangerous pattern detected: {pattern}"
            raise UnsafePackageError(msg)
        if not pattern.endswith(".py"):
            msg = "Pattern must target Python files (.py)"
            raise UnsafePackageError(msg)

    def _validate_module_path(self, module_path: Path, base_path: Path) -> None:
        """Ensure module_path resides under base_path and is within size limits.

        Args:
            module_path: Path to the module file.
            base_path: Root directory for the package.

        Raises:
            UnsafePackageError: On invalid paths or oversize files.
        """
        try:  # noqa: PLW0717
            resolved_module = module_path.resolve()
            resolved_base = base_path.resolve()
            if not str(resolved_module).startswith(str(resolved_base)):
                msg = "Path traversal attempt: %s"
                raise UnsafePackageError(msg, module_path)
            if not resolved_module.exists() or not resolved_module.is_file():
                msg = "Invalid module path: %s"
                raise UnsafePackageError(msg, module_path)
            if resolved_module.stat().st_size > self.max_file_size:
                msg = "Module file too large: %s"
                raise UnsafePackageError(msg, module_path)
        except (OSError, ValueError) as e:
            msg = "Invalid module path: %s"
            raise UnsafePackageError(msg, module_path) from e

    def _is_safe_module_name(self, module_name: str) -> bool:
        """Check module name against forbidden patterns.

        Args:
            module_name: Full dotted module name.

        Returns:
            True if safe, False otherwise.
        """
        if not module_name or not all(part.isidentifier() for part in module_name.split(".")):
            return False
        return not any(p in module_name for p in self.FORBIDDEN_MODULE_PATTERNS)

    def _safe_import_module(self, module_name: str, module_path: Path) -> bool:
        """Import a module by spec and execute it.

        Args:
            module_name: Dotted module name.
            module_path: Filesystem path to .py file.

        Returns:
            True if imported successfully, False otherwise.
        """
        try:  # noqa: PLW0717
            if not self._is_safe_module_name(module_name):
                return False
            if module_name in sys.modules:
                logger.debug("Module %s already imported", module_name)
                return True
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            if spec is None or spec.loader is None:
                logger.error("Could not create valid spec for %s", module_name)
                return False
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            logger.debug("Successfully imported %s", module_name)
        except Exception:
            sys.modules.pop(module_name, None)
            logger.exception("Failed to import %s", module_name)
            return False
        return True

    def __str__(self) -> str:
        """Return a string representation of registered trigger handler classes."""
        return str(self._trigger_handlers_cls)

    def __init__(
        self,
        packages: list[str],
        file_pattern: str = "*_trigger.py",
        max_file_size: int = 1024 * 1024,  # 1Mb
    ) -> None:
        """Initialize the discoverer.

        Args:
            packages: List of package names to scan.
            file_pattern: Glob pattern for matching modules.
            max_file_size: Limit for module file sizes in bytes.
        """
        self.packages = packages
        self.file_pattern = file_pattern
        self.max_file_size = max_file_size

    def discover_modules(self) -> dict[str, bool]:
        """Discover and import matching modules across configured packages.

        Returns:
            results infos

        Raises:
            DiscoveryError: If initial inputs are invalid.
        """
        self._validate_inputs()
        results: dict[str, bool] = {}
        for pkg in self.packages:
            results.update(self._discover_package(pkg))
        return results

    def register_trigger(self, handler_cls: type[TriggerHandler]) -> type[TriggerHandler]:
        """Register a trigger handler class for a specific protocol.

        Args:
            handler_cls: The trigger handler class to register.

        Returns:
            The registered trigger handler class.

        Raises:
            ValueError: If a handler for the protocol is already registered.
        """
        key = handler_cls.protocol
        if key not in self._trigger_handlers_cls:
            self._trigger_handlers_cls[key] = []

        self._trigger_handlers_cls[key].append(handler_cls)
        return handler_cls

    def get_registered_protocols_with_info(self, *, exclude_utility: bool = False) -> dict[str, str]:
        """Get registered protocols with their descriptions.

        Args:
            exclude_utility: If True, exclude SDK utility protocols (healthcheck, etc.).

        Returns:
            Dict mapping protocol name to description (from handler description attribute).
        """
        result: dict[str, str] = {}
        for protocol, handlers in self._trigger_handlers_cls.items():
            if handlers:
                if exclude_utility:
                    from digitalkin.models.module.utility import UtilityProtocol

                    input_fmt = handlers[0].input_format
                    if isinstance(input_fmt, type) and issubclass(input_fmt, UtilityProtocol):
                        continue
                result[protocol] = handlers[0].description or protocol
        return result

    def init_handlers(self, context: ModuleContext) -> dict[str, tuple[TriggerHandler, ...]]:
        """Instantiate all registered trigger handler classes.

        Args:
            context: Module context to pass to each handler constructor.

        Returns:
            Mapping of protocol name to handler instance tuples.
        """
        return {
            protocol: tuple(handler_cls(context) for handler_cls in set(handlers_cls))
            for protocol, handlers_cls in self._trigger_handlers_cls.items()
        }

    @staticmethod
    def get_trigger(
        handlers: dict[str, tuple[TriggerHandler, ...]],
        protocol: str,
        input_instance: DataTrigger,
    ) -> TriggerHandler:
        """Retrieve a trigger handler instance based on the provided protocol and input instance type.

        Args:
            handlers: Mapping of protocol name to handler instance tuples.
            protocol: The protocol name (ignored internally, `input_instance.protocol` is used instead).
            input_instance: The input trigger instance used to determine the correct handler.

        Returns:
            TriggerHandler: An instance of the trigger handler matching the input format.

        Raises:
            ValueError: If no handler is registered for the specified protocol,
                        or if no handler matches the type of the input instance.
        """
        logger.debug("Trigger type invoked: %s", input_instance)
        protocol = input_instance.protocol

        if (protocols := handlers.get(protocol)) is None:
            msg = f"No handler for protocol '{protocol}'"
            raise ValueError(msg)

        try:
            handler_instance = next(x for x in protocols if isinstance(input_instance, x.input_format))
        except Exception as e:
            msg = f"No handler for input format '{type(input_instance)=}'"
            raise ValueError(msg) from e
        return handler_instance
