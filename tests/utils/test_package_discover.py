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
    path.write_text(content)


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
