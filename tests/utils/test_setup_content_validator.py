"""Content rules enforced on a setup's ``content`` before it is written."""

import pytest

from digitalkin.utils.setup_content_validator import SetupContentValidator

_LIMIT = 4096


class TestRejectOversizedOutputFormatSpec:
    """``output_format_spec`` must stay under the length the written setup can carry."""

    def test_accepts_a_spec_under_the_limit(self) -> None:
        content = {"output_format_spec": "x" * (_LIMIT - 1)}
        assert SetupContentValidator.reject_oversized_output_format_spec(content) is content

    def test_rejects_a_spec_exactly_at_the_limit(self) -> None:
        """The boundary is exclusive: 4096 is already too long."""
        with pytest.raises(ValueError, match="must stay under 4096"):
            SetupContentValidator.reject_oversized_output_format_spec({"output_format_spec": "x" * _LIMIT})

    def test_rejects_a_spec_over_the_limit_and_reports_its_size(self) -> None:
        with pytest.raises(ValueError, match="is 5000 characters"):
            SetupContentValidator.reject_oversized_output_format_spec({"output_format_spec": "x" * 5000})

    def test_names_the_offending_path_when_nested(self) -> None:
        content = {"agent": {"output_format_spec": "x" * _LIMIT}}
        with pytest.raises(ValueError, match=r"'agent\.output_format_spec'"):
            SetupContentValidator.reject_oversized_output_format_spec(content)

    def test_reaches_into_lists(self) -> None:
        content = {"agents": [{"name": "ok"}, {"output_format_spec": "x" * _LIMIT}]}
        with pytest.raises(ValueError, match="output_format_spec"):
            SetupContentValidator.reject_oversized_output_format_spec(content)

    def test_ignores_a_non_string_value(self) -> None:
        """Typing is the schema check's job; this rule only measures strings."""
        content = {"output_format_spec": {"nested": "x" * 5000}}
        assert SetupContentValidator.reject_oversized_output_format_spec(content) is content

    def test_leaves_other_long_fields_alone(self) -> None:
        content = {"prompt": "x" * 100_000}
        assert SetupContentValidator.reject_oversized_output_format_spec(content) is content

    def test_empty_content_is_a_no_op(self) -> None:
        assert SetupContentValidator.reject_oversized_output_format_spec({}) == {}
