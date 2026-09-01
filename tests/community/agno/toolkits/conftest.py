"""Fixtures for the default toolkits tests.

agno is not installed in the SDK test environment, but the toolkit modules
subclass ``agno.tools.Toolkit`` at import time. This conftest installs a fake
``agno.tools`` module BEFORE the test modules import the toolkits, and removes
the fakes (plus the toolkit modules bound to them) after the session so other
tests see a pristine ``sys.modules``.
"""

import sys
import types
from typing import Any

import pytest


class _FakeToolkit:
    """Minimal stand-in for ``agno.tools.Toolkit``."""

    def __init__(self, name: str = "", tools: list[Any] | None = None, **kwargs: Any) -> None:
        self.name = name
        self.tools = list(tools or [])


def _install_fake_agno() -> dict[str, Any]:
    """Install fake agno modules into sys.modules, returning the displaced entries.

    No-op when the real agno is importable. The fake ``agno.tools`` is a plain module, not a
    package, so it cannot satisfy ``from agno.tools.function import Function`` — the import the
    registry toolkits make — and shadowing a working install with it makes the whole directory
    uncollectable.

    Returns:
        The sys.modules entries displaced by the fakes; empty when the real agno is present.
    """
    try:
        import agno.tools.function  # pylint: disable=C0415,W0611
    except ImportError:
        pass
    else:
        return {}
    saved = {key: sys.modules.get(key) for key in ("agno", "agno.tools")}
    agno_pkg = types.ModuleType("agno")
    agno_tools = types.ModuleType("agno.tools")
    agno_tools.Toolkit = _FakeToolkit  # type: ignore[attr-defined]
    agno_pkg.tools = agno_tools  # type: ignore[attr-defined]
    sys.modules["agno"] = agno_pkg
    sys.modules["agno.tools"] = agno_tools
    return saved


# Module-level install: conftest imports before the test modules in this directory,
# so their module-level toolkit imports resolve against the fake.
_SAVED_MODULES = _install_fake_agno()


@pytest.fixture(scope="session", autouse=True)
def _restore_agno_modules() -> Any:
    """Remove the fake agno modules and the toolkit modules bound to them after the session.

    Yields:
        None. Cleanup runs at session teardown.
    """
    yield
    for key, module in _SAVED_MODULES.items():
        if module is None:
            sys.modules.pop(key, None)
        else:
            sys.modules[key] = module
    for key in [k for k in sys.modules if k.startswith("digitalkin.community.agno.toolkits")]:
        sys.modules.pop(key, None)
