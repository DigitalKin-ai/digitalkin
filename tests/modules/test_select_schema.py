"""Coverage for SelectSchema.build (auto-gen, custom-field, and None branches)."""

from __future__ import annotations

from pydantic import Field

from digitalkin.models.module.select_schema import SelectSchema


class TestSelectSchemaBuild:
    def test_none_when_no_protocols_and_no_custom_fields(self) -> None:
        assert SelectSchema.build({}) is None

    def test_auto_generates_from_protocols(self) -> None:
        result = SelectSchema.build({"message": "Process messages", "file": "Process files"})
        assert result is not None
        props = result["json_schema"]["properties"]
        assert props["message"]["title"] == "message"
        assert props["message"]["description"] == "Process messages"
        assert props["message"]["default"] is True
        assert props["message"]["type"] == "boolean"
        assert result["ui_schema"]["message"]["ui:widget"] == "checkbox"
        assert result["ui_schema"]["file"]["ui:widget"] == "checkbox"

    def test_custom_fields_take_precedence_over_protocols(self) -> None:
        class MySelect(SelectSchema):
            message: bool = Field(default=True, title="Message")
            file: bool = Field(default=False, title="File")

        result = MySelect.build({"ignored_protocol": "x"})
        assert result is not None
        props = result["json_schema"]["properties"]
        assert "message" in props
        assert "ignored_protocol" not in props
        assert result["ui_schema"]["message"]["ui:widget"] == "checkbox"
