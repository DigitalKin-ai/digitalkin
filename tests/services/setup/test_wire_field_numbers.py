"""Field numbers the SDK depends on, pinned so a renumber fails here and not in production.

Protocol 1.0.2.dev2 inserted ``documentation`` at ``SetupVersion`` field 4, pushing ``content``
to 5 and ``creation_date`` to 6. A client on the previous pin decodes such a message WITHOUT
error — ``content`` reads as absent and ``creation_date`` as epoch, because a Struct and a
Timestamp are both length-delimited and protobuf skips the mismatch. That shipped: archetype-ada
on 1.0.4.dev1 failed every setup read against a newer backend with a bare "content Field
required" (2026-09-08/09).

Nothing in the SDK can detect that at runtime. These tests are the detector: bumping the pin to
a protocol that moves a field the SDK reads breaks the suite immediately.
"""

import pytest
from agentic_mesh_protocol.registry.v1 import registry_models_pb2
from agentic_mesh_protocol.setup.v1 import setup_pb2

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
                setup_pb2.SetupVersion,
                {"id": 1, "setup_id": 2, "version": 3, "documentation": 4, "content": 5, "creation_date": 6},
            ),
            (
                setup_pb2.Setup,
                {
                    "id": 1,
                    "name": 2,
                    "organisation_id": 3,
                    "owner_id": 4,
                    "module_id": 5,
                    "current_setup_version": 6,
                    "status": 7,
                    "visibility": 8,
                },
            ),
            (setup_pb2.GetSetupRequest, {"setup_id": 1, "version": 2, "structure_key": 3}),
            (setup_pb2.GetSetupResponse, {"setup": 1, "setup_version": 2}),
            (setup_pb2.CreateSetupRequest, {"name": 1, "content": 2, "documentation": 3, "structure": 4}),
            (
                setup_pb2.UpdateSetupRequest,
                {"setup_id": 1, "name": 2, "content": 3, "set_as_current": 4, "documentation": 5, "structure": 6},
            ),
            (setup_pb2.CreateSetupResponse, {"success": 1, "setup": 2, "setup_version": 3, "structure": 4}),
            (setup_pb2.UpdateSetupResponse, {"success": 1, "setup": 2, "setup_version": 3, "structure": 4}),
        ],
    )
    def test_setup_message_numbers_are_unchanged(self, message: type, expected: dict[str, int]) -> None:
        assert self._numbers(message) == expected

    def test_setup_summary_numbers_are_unchanged(self) -> None:
        """``structure`` took field 12 in 1.0.2, displacing ``tags`` to 13 — a break vs 1.0.1.

        Recorded rather than corrected: the renumber was accepted deliberately. This pins the
        result so the next move is a decision, not a surprise.
        """
        assert self._numbers(registry_models_pb2.SetupSummary) == {
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
