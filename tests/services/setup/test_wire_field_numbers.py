"""Field numbers the SDK depends on, pinned so a renumber fails here and not in production.

Protocol 1.0.2.dev2 inserted ``documentation`` at ``SetupVersion`` field 4, pushing ``content``
to 5 and ``creation_date`` to 6. A client on the previous pin decodes such a message WITHOUT
error — ``content`` reads as absent and ``creation_date`` as epoch, because a Struct and a
Timestamp are both length-delimited and protobuf skips the mismatch. That shipped: archetype-ada
on 1.0.4.dev1 failed every setup read against a newer backend with a bare "content Field
required" (2026-09-08/09).

Nothing in the SDK can detect that at runtime. These tests are the detector: bumping the pin to
a protocol that moves a field the SDK reads breaks the suite immediately.

The validation-proto restructure renumbered ``Setup`` and ``SetupVersion`` again (``structure``
now at 6 on the version, ``created_at`` at 7) and wrapped every response in a ``SetupResult``;
the numbers below are that layout.
"""

import pytest
from agentic_mesh_protocol.registry.v1 import registry_messages_pb2
from agentic_mesh_protocol.setup.v1 import setup_dto_pb2, setup_enums_pb2, setup_messages_pb2, setup_version_dto_pb2

pytestmark = [pytest.mark.contract, pytest.mark.unit]


class TestWireFieldNumbers:
    """Every message the setup and registry strategies decode, by number."""

    @staticmethod
    def _numbers(message: type) -> dict[str, int]:
        """Map field name to wire number for a proto message class.

        Args:
            message: The generated message class.

        Returns:
            ``{field name: field number}``.
        """
        return {field.name: field.number for field in message.DESCRIPTOR.fields}

    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            (
                    setup_messages_pb2.SetupVersion,
                {
                    "id": 1,
                    "setup_id": 2,
                    "version": 3,
                    "documentation": 4,
                    "content": 5,
                    "structure": 6,
                    "created_at": 7,
                },
            ),
            (
                    setup_messages_pb2.Setup,
                    {
                        "id": 1,
                        "organization_id": 2,
                        "owner_id": 3,
                        "module_id": 4,
                        "name": 5,
                        "status": 6,
                        "visibility": 7,
                        "current_setup_version": 8,
                    },
            ),
            (setup_messages_pb2.SetupRevision, {"content": 1, "structure": 2, "documentation": 3}),
            (setup_messages_pb2.SetupResult, {"identifier": 1, "setup": 2, "version": 3, "error": 4}),
            (setup_dto_pb2.GetSetupRequest, {"setup_id": 1, "version": 2, "structure_key": 3}),
            (setup_dto_pb2.CreateSetupRequest, {"name": 1, "revision": 2, "module_id": 3, "visibility": 4}),
            (
                    setup_dto_pb2.UpdateSetupRequest,
                    {"setup_id": 1, "name": 2, "status": 3, "revision": 4, "set_as_current": 5},
            ),
            (setup_dto_pb2.GetSetupResponse, {"result": 1}),
            (setup_dto_pb2.CreateSetupResponse, {"result": 1}),
            (setup_dto_pb2.UpdateSetupResponse, {"result": 1}),
            (setup_dto_pb2.ChangeVisibilityResponse, {"result": 1}),
            (setup_dto_pb2.DeleteSetupResponse, {"result": 1}),
            (setup_version_dto_pb2.ListSetupVersionsRequest, {"setup_id": 1, "version": 2, "pagination": 3}),
            (
                    setup_version_dto_pb2.ListSetupVersionsResponse,
                    {"results": 1, "bulk": 2, "current_setup_version_id": 3},
            ),
            (setup_version_dto_pb2.SetCurrentSetupVersionResponse, {"result": 1}),
        ],
    )
    def test_setup_message_numbers_are_unchanged(self, message: type, expected: dict[str, int]) -> None:
        assert self._numbers(message) == expected

    def test_setup_status_values_are_unchanged(self) -> None:
        """SetupStatus was renumbered (DRAFT used to be 0); the SDK maps it by name, but pin the values."""
        assert {v.name: v.number for v in setup_enums_pb2.SetupStatus.DESCRIPTOR.values} == {
            "SETUP_STATUS_UNSPECIFIED": 0,
            "DRAFT": 1,
            "WAITING_FOR_APPROVAL": 2,
            "READY": 3,
            "PAUSED": 4,
            "FAILED": 5,
            "ARCHIVED": 6,
            "NEEDS_CONFIGURATION": 7,
            "CONFIGURATION_FAILED": 8,
            "CONFIGURATION_SUCCEEDED": 9,
            "VALIDATING": 10,
        }

    def test_setup_summary_numbers_are_unchanged(self) -> None:
        """``structure`` took field 12 in 1.0.2, displacing ``tags`` to 13 — a break vs 1.0.1.

        Recorded rather than corrected: the renumber was accepted deliberately. This pins the
        result so the next move is a decision, not a surprise.
        """
        assert self._numbers(registry_messages_pb2.SetupSummary) == {
            "id": 1,
            "name": 2,
            "documentation": 3,
            "status": 4,
            "visibility": 5,
            "organization_id": 6,
            "module_id": 7,
            "module_name": 8,
            "module_type": 9,
            "setup_version_id": 10,
            "setup_version": 11,
            "structure": 12,
            "tags": 13,
        }
