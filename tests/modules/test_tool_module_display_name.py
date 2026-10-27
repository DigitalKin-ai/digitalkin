"""Tests for the mandatory trigger display_name on ToolModule subclasses."""

from typing import Literal

import pytest

from digitalkin.models.module.base_types import DataModel, DataTrigger
from digitalkin.models.module.utility import HealthcheckPingInput
from digitalkin.modules.archetype_module import ArchetypeModule
from digitalkin.modules.tool_module import ToolModule


class _LabelledSearch(DataTrigger):
    """Search the documents."""

    display_name = "Search documents"
    protocol: Literal["search"] = "search"
    q: str


class _UnlabelledList(DataTrigger):
    """List the files."""

    protocol: Literal["list_files"] = "list_files"


class _MixedInput(DataModel[_LabelledSearch | _UnlabelledList]):
    """One labelled, one unlabelled trigger."""


class _LabelledInput(DataModel[_LabelledSearch]):
    """Only labelled triggers."""


class _WithUtilityInput(DataModel[_LabelledSearch | HealthcheckPingInput]):
    """Labelled trigger next to an SDK utility protocol."""


class TestToolModuleDisplayName:
    """ToolModule refuses tool triggers without display_name at class definition."""

    def test_tool_module_without_labels_fails(self) -> None:
        """An unlabelled trigger raises TypeError naming the module and the protocol."""
        with pytest.raises(TypeError, match=r"_Broken: trigger\(s\) without display_name: list_files"):

            class _Broken(ToolModule):
                input_format = _MixedInput

    def test_tool_module_with_labels_defines(self) -> None:
        """A fully labelled input defines without error."""

        class _Labelled(ToolModule):
            input_format = _LabelledInput

        assert _Labelled.input_format is _LabelledInput

    def test_abstract_tool_module_without_input_format_defines(self) -> None:
        """An intermediate subclass without its own input_format is not checked."""

        class _Abstract(ToolModule):
            pass

        class _Concrete(_Abstract):
            input_format = _LabelledInput

        assert _Concrete.input_format is _LabelledInput

    def test_utility_protocols_are_exempt(self) -> None:
        """SDK utility protocols in the input union need no label."""

        class _WithUtility(ToolModule):
            input_format = _WithUtilityInput

        assert _WithUtility.input_format is _WithUtilityInput

    def test_archetype_module_is_not_checked(self) -> None:
        """Archetype triggers are not tools and may stay unlabelled."""

        class _Archetype(ArchetypeModule):
            input_format = _MixedInput

        assert _Archetype.input_format is _MixedInput

    def test_error_tells_to_declare_a_plain_class_variable(self) -> None:
        """An annotated display_name is a field, not a label: the error says how to declare it."""
        with pytest.warns(UserWarning, match="display_name"):

            class _Annotated(DataTrigger):
                display_name: str = "List files"
                protocol: Literal["list_files"] = "list_files"

        class _AnnotatedInput(DataModel[_Annotated]):
            """Annotated label."""

        with pytest.raises(TypeError, match=r'plain class variable: display_name = "\.\.\."'):

            class _Broken(ToolModule):
                input_format = _AnnotatedInput
