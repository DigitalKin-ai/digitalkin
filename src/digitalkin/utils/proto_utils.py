"""Protobuf conversion utilities."""

from google.protobuf import json_format
from google.protobuf.message import Message


class ProtoUtils:
    """Protobuf message conversion helpers."""

    @staticmethod
    def proto_to_dict(msg: Message, *, with_defaults: bool = False) -> dict:
        """Convert a protobuf message to a dict preserving snake_case field names.

        Args:
            msg: Protobuf message to convert.
            with_defaults: If True, include fields with default/zero values.

        Returns:
            Dictionary representation with original field names preserved.
        """
        return json_format.MessageToDict(
            msg,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=with_defaults,
        )
