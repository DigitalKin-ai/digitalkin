import importlib
import sys
import types
from pathlib import Path

import pytest

# === STUB digitalkin.modules.trigger_handler to avoid circular import in tests ===
_fake_trigger_mod = types.ModuleType("digitalkin.modules.trigger_handler")


class TriggerHandlerStub:
    """Test stub for TriggerHandler."""


_fake_trigger_mod.TriggerHandler = TriggerHandlerStub
sys.modules["digitalkin.modules.trigger_handler"] = _fake_trigger_mod
# Stub base_module to prevent import cycles (if referenced)
sys.modules.setdefault("digitalkin.modules._base_module", types.ModuleType("digitalkin.modules._base_module"))

from digitalkin.utils.package_discover import (  # noqa: E402
    DiscoveryError,
    ModuleDiscoverer,
    SecurityError,
)

# Helper to create Python files


def write_file(path: Path, content: str = ""):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_validate_inputs_empty_packages():
    md = ModuleDiscoverer(packages=[], file_pattern="*_trigger.py")
    with pytest.raises(DiscoveryError):
        md._validate_inputs()


def test_validate_file_pattern_invalid():
    md = ModuleDiscoverer(packages=["pkg"], file_pattern="dangerous*/.py")
    with pytest.raises(SecurityError):
        md._validate_file_pattern()


@pytest.mark.parametrize("pattern", ["module.py", "*_trigger.py"])
def test_validate_file_pattern_valid(pattern):
    md = ModuleDiscoverer(packages=["pkg"], file_pattern=pattern)
    md._validate_file_pattern()


def test_validate_package_name_good():
    ModuleDiscoverer._validate_package_name("valid_pkg")
    ModuleDiscoverer._validate_package_name("pkg.subpkg.module")


def test_validate_package_name_bad():
    for name in ["", None, "..pkg", "pkg/et", "pkg\\mod", "in valid"]:
        with pytest.raises(SecurityError):
            ModuleDiscoverer._validate_package_name(name)


def test_is_safe_module_name():
    md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py")
    assert md._is_safe_module_name("pkg.module")
    assert not md._is_safe_module_name("pkg.test_module")
    assert not md._is_safe_module_name("pkg.__pycache__.mod")


def test_module_file_path():
    base = Path("/tmp/pkg")  # noqa: S108
    path = ModuleDiscoverer._module_file_path("pkg.sub.mod", base, "pkg")
    assert path == base / "sub/mod.py"


def test_validate_module_path(tmp_path):
    base = tmp_path / "pkg"
    file = base / "mod.py"
    write_file(file, "# content")
    md = ModuleDiscoverer(packages=["pkg"], file_pattern="*.py", max_file_size=10)
    md._validate_module_path(file, base)

    md_large = ModuleDiscoverer(packages=["pkg"], file_pattern="*.py", max_file_size=1)
    with pytest.raises(SecurityError):
        md_large._validate_module_path(file, base)

    other = tmp_path / "other.py"
    write_file(other)
    with pytest.raises(SecurityError):
        md._validate_module_path(other, base)


def test_safe_import_module_success(tmp_path):
    file = tmp_path / "good.py"
    write_file(file, "value = 1")
    md = ModuleDiscoverer(packages=["pkg"], file_pattern="*.py")

    result = md._safe_import_module("good", file)
    assert result
    assert "good" in sys.modules
    assert sys.modules["good"].value == 1
    sys.modules.pop("good", None)


def test_safe_import_module_fail(tmp_path):
    file = tmp_path / "bad.py"
    write_file(file, "raise RuntimeError('fail')")
    md = ModuleDiscoverer(packages=["pkg"], file_pattern="*.py")
    result = md._safe_import_module("bad", file)
    assert not result
    assert "bad" not in sys.modules


def test_process_module(tmp_path):
    pkg_dir = tmp_path / "pkg"
    write_file(pkg_dir / "__init__.py")
    write_file(pkg_dir / "mod_trigger.py", "# empty")

    md = ModuleDiscoverer(packages=[str(pkg_dir.parent)], file_pattern="*_trigger.py")
    assert md._process_module(f"{pkg_dir.name}.mod_trigger", pkg_dir, pkg_dir.name)

    write_file(pkg_dir / "other.py")
    assert not md._process_module(f"{pkg_dir.name}.other", pkg_dir, pkg_dir.name)

    write_file(pkg_dir / "test_bad_trigger.py")
    assert not md._process_module(f"{pkg_dir.name}.test_bad_trigger", pkg_dir, pkg_dir.name)


def test_discover_modules_integration(tmp_path):
    pkg_dir = tmp_path / "mypkg"
    write_file(pkg_dir / "__init__.py")
    write_file(pkg_dir / "good_trigger.py", "value = 1")
    write_file(pkg_dir / "bad_trigger.py", "raise Exception()")
    write_file(pkg_dir / "skip.txt", "data")
    sys.path.insert(0, str(tmp_path))

    md = ModuleDiscoverer(packages=["mypkg"], file_pattern="*_trigger.py")
    results = md.discover_modules()
    assert results.get("mypkg.good_trigger", False) is True
    assert results.get("mypkg.bad_trigger", True) is False

    sys.path.pop(0)


def test_discover_invalid_package_import(monkeypatch):
    md = ModuleDiscoverer(packages=["nonexistent_pkg"], file_pattern="*.py")
    monkeypatch.setattr(importlib, "import_module", lambda name: (_ for _ in ()).throw(ImportError()))  # noqa: ARG005
    results = md.discover_modules()
    assert results == {}


def test_discover_module_no_path(tmp_path):
    file = tmp_path / "solo.py"
    write_file(file, "# no package path")
    sys.path.insert(0, str(tmp_path))

    md = ModuleDiscoverer(packages=["solo"], file_pattern="*.py")
    results = md.discover_modules()
    assert results == {}

    sys.path.pop(0)


def test_nested_packages(tmp_path):
    base = tmp_path / "basepkg"
    sub = base / "subpkg"
    write_file(base / "__init__.py")
    write_file(sub / "__init__.py")
    write_file(sub / "deep_trigger.py", "x=42")
    sys.path.insert(0, str(tmp_path))

    md = ModuleDiscoverer(packages=["basepkg"], file_pattern="*_trigger.py")
    results = md.discover_modules()
    assert results.get("basepkg.subpkg.deep_trigger", False) is True

    sys.path.pop(0)


def test_skip_forbidden_directories(tmp_path):
    pkg_dir = tmp_path / "pkg"
    cache = pkg_dir / "__pycache__"
    write_file(pkg_dir / "__init__.py")
    write_file(cache / "mod_trigger.py")
    sys.path.insert(0, str(tmp_path))

    md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py")
    results = md.discover_modules()
    assert all("__pycache__" not in name for name in results)

    sys.path.pop(0)


def test_large_file_skipped(tmp_path):
    pkg_dir = tmp_path / "pkg"
    write_file(pkg_dir / "__init__.py")
    write_file(pkg_dir / "big_trigger.py", "a" * 2048)
    sys.path.insert(0, str(tmp_path))

    md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py", max_file_size=1024)
    results = md.discover_modules()
    assert "pkg.big_trigger" not in results

    sys.path.pop(0)


# ---------------------------------------------------------------------------
# init_handlers / get_trigger unit tests
# ---------------------------------------------------------------------------

from unittest.mock import Mock  # noqa: E402


@pytest.fixture()
def _clean_trigger_registry():
    """Save and restore _trigger_handlers_cls to avoid cross-test leaks."""
    original = dict(ModuleDiscoverer._trigger_handlers_cls)
    ModuleDiscoverer._trigger_handlers_cls = {}
    yield
    ModuleDiscoverer._trigger_handlers_cls = original


class _FakeInput:
    """Minimal input type with protocol attribute for get_trigger tests."""

    protocol = "alpha"


class _FakeInputBeta:
    """Second input type for multi-handler tests."""

    protocol = "beta"


class _HandlerAlpha:
    """Handler matching _FakeInput."""

    protocol = "alpha"
    input_format = _FakeInput

    def __init__(self, context):
        self.context = context


class _HandlerBeta:
    """Handler matching _FakeInputBeta."""

    protocol = "beta"
    input_format = _FakeInputBeta

    def __init__(self, context):
        self.context = context


class TestInitHandlers:
    """Tests for ModuleDiscoverer.init_handlers."""

    @pytest.mark.usefixtures("_clean_trigger_registry")
    def test_returns_dict(self):
        """init_handlers returns a dict, not None."""
        md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py")
        md._trigger_handlers_cls["alpha"] = [_HandlerAlpha]

        result = md.init_handlers(Mock())

        assert isinstance(result, dict)
        assert "alpha" in result

    @pytest.mark.usefixtures("_clean_trigger_registry")
    def test_handler_instances_not_classes(self):
        """Returned values are instantiated handler objects, not classes."""
        md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py")
        md._trigger_handlers_cls["alpha"] = [_HandlerAlpha]

        result = md.init_handlers(Mock())

        handler = result["alpha"][0]
        assert isinstance(handler, _HandlerAlpha)
        assert not isinstance(handler, type)

    @pytest.mark.usefixtures("_clean_trigger_registry")
    def test_multiple_protocols(self):
        """init_handlers returns entries for every registered protocol."""
        md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py")
        md._trigger_handlers_cls["alpha"] = [_HandlerAlpha]
        md._trigger_handlers_cls["beta"] = [_HandlerBeta]

        result = md.init_handlers(Mock())

        assert len(result) == 2
        assert isinstance(result["alpha"][0], _HandlerAlpha)
        assert isinstance(result["beta"][0], _HandlerBeta)

    @pytest.mark.usefixtures("_clean_trigger_registry")
    def test_deduplicates_handler_classes(self):
        """Duplicate class registrations produce a single handler instance."""
        md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py")
        md._trigger_handlers_cls["alpha"] = [_HandlerAlpha, _HandlerAlpha]

        result = md.init_handlers(Mock())

        assert len(result["alpha"]) == 1

    @pytest.mark.usefixtures("_clean_trigger_registry")
    def test_passes_context_to_constructors(self):
        """Each handler receives the provided context."""
        md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py")
        md._trigger_handlers_cls["alpha"] = [_HandlerAlpha]
        ctx = Mock()

        result = md.init_handlers(ctx)

        assert result["alpha"][0].context is ctx

    @pytest.mark.usefixtures("_clean_trigger_registry")
    def test_returns_fresh_dict_each_call(self):
        """Two calls to init_handlers return independent dicts with separate instances."""
        md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py")
        md._trigger_handlers_cls["alpha"] = [_HandlerAlpha]

        r1 = md.init_handlers(Mock())
        r2 = md.init_handlers(Mock())

        assert r1 is not r2
        assert r1["alpha"][0] is not r2["alpha"][0]

    @pytest.mark.usefixtures("_clean_trigger_registry")
    def test_empty_registry(self):
        """init_handlers returns empty dict when no handlers registered."""
        md = ModuleDiscoverer(packages=["pkg"], file_pattern="*_trigger.py")

        result = md.init_handlers(Mock())

        assert result == {}


class TestGetTrigger:
    """Tests for ModuleDiscoverer.get_trigger (static method)."""

    def test_selects_correct_handler(self):
        """get_trigger returns the handler whose input_format matches the instance."""
        handler = _HandlerAlpha(Mock())
        handlers = {"alpha": (handler,)}

        result = ModuleDiscoverer.get_trigger(handlers, "alpha", _FakeInput())

        assert result is handler

    def test_selects_among_multiple_protocols(self):
        """get_trigger picks the right protocol group."""
        ha = _HandlerAlpha(Mock())
        hb = _HandlerBeta(Mock())
        handlers = {"alpha": (ha,), "beta": (hb,)}

        assert ModuleDiscoverer.get_trigger(handlers, "alpha", _FakeInput()) is ha
        assert ModuleDiscoverer.get_trigger(handlers, "beta", _FakeInputBeta()) is hb

    def test_raises_on_unknown_protocol(self):
        """get_trigger raises ValueError for unregistered protocol."""
        with pytest.raises(ValueError, match="No handler for protocol"):
            ModuleDiscoverer.get_trigger({}, "missing", _FakeInput())

    def test_raises_on_mismatched_input_format(self):
        """get_trigger raises ValueError when no handler matches input type."""
        handler = _HandlerBeta(Mock())
        handlers = {"alpha": (handler,)}

        # _FakeInput has protocol="alpha" but handler expects _FakeInputBeta
        with pytest.raises(ValueError, match="No handler for input format"):
            ModuleDiscoverer.get_trigger(handlers, "alpha", _FakeInput())

    def test_uses_input_instance_protocol_not_parameter(self):
        """get_trigger uses input_instance.protocol, ignoring the protocol parameter."""
        handler = _HandlerAlpha(Mock())
        handlers = {"alpha": (handler,)}

        # Pass wrong protocol parameter — input_instance.protocol overrides it
        result = ModuleDiscoverer.get_trigger(handlers, "wrong", _FakeInput())

        assert result is handler
