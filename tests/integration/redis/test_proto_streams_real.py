"""Real-Redis integration test for ``ProtoStreamReader.read_structs(skip_to_seq)``.

Paired with the fakeredis unit test in ``tests/core/redis/test_proto_streams.py``;
validates the exact-cursor seek against real XREAD/entry-id semantics.
"""

from __future__ import annotations

import pytest
from google.protobuf import struct_pb2

from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


def _pb(i: int) -> bytes:
    s = struct_pb2.Struct()
    s.update({"protocol": "chunk", "i": i})
    return s.SerializeToString()


async def test_skip_to_seq_real(redis_client) -> None:
    task_id = "task_skip_real"
    key = f"task:{task_id}:stream"
    for sq in range(7):  # stored seq 0..6
        await redis_client.xadd(key, {"pb": _pb(sq), "seq": str(sq)})
    await redis_client.xadd(key, {"eos": b"true"})

    reader = ProtoStreamReader(task_id, redis_client)
    got = [s async for s in reader.read_structs(skip_to_seq=3)]

    assert len(got) == 3  # stored seq 4, 5, 6
    assert reader._last_seq == 6
    assert [s.fields["i"].number_value for s in got] == [4, 5, 6]
