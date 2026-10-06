# AG-UI events and pipeline state

Every frame a module streams to the front, what triggers it, and how the front uses it. The front never writes any of this: it only sends user messages (a resume after a human-in-the-loop pause is a message too). The SDK owns the whole AG-UI state.

## Envelope

Each frame is the `data` Struct of one `StreamClient`:

```json
{"root": {"protocol": "agui_tool_call_start", "createdAt": "…",
  "event": {"type": "TOOL_CALL_START", "toolCallId": "…", "toolCallName": "…",
            "subagentRunId": "…", "metadata": {"digitalkin": {…}}}},
 "annotations": {}}
```

- AG-UI events use `protocol = agui_<snake_case type>`. Transport sentinels use `stream.*` and carry no `event`.
- `subagentRunId` names the delegated agent that produced the event; group on it. Run-level events never carry it.
- Fields are camelCase and null fields are omitted.

## Pipeline state

`STATE_SNAPSHOT` follows every `RUN_STARTED` and replaces the front's state. Each later change is one `STATE_DELTA` (RFC 6902 JSON Patch, `add`/`replace` only; ids in paths are escaped `~`→`~0`, `/`→`~1`).

```json
{
  "run": {"status": "running", "error": null, "heartbeatAt": null},
  "tools": {"<toolCallId>": {"name": "search", "status": "running", "subagentRunId": null,
                             "startedAt": 1759650000000, "error": null}},
  "subagents": {"<subagentRunId>": {"name": "writer", "status": "running", "startedAt": 1759650000000, "error": null}},
  "interrupts": [{"id": "…", "reason": "tool_call", "toolCallId": "…", "message": "Waiting for the result of …"}]
}
```

| Path | Values | Changes when |
|---|---|---|
| `run.status` | `running` → `completed` \| `failed` \| `cancelled` \| `interrupted` | `RUN_FINISHED`, `RUN_ERROR`, cancel, HITL pause |
| `run.error` | message | the run fails or is cancelled |
| `run.heartbeatAt` | epoch ms | every `DIGITALKIN_MODULE_HEARTBEAT_INTERVAL_S` (30s) of silence while the run is open |
| `tools.<id>.status` | `running` → `completed` \| `failed` \| `awaiting_input` | tool start, end, failure, HITL pause |
| `tools.<id>.error` | `[CODE] message` or the tool's error JSON | the tool failed |
| `subagents.<id>.status` | `running` → `completed` \| `failed` | subagent start, end, failure |
| `interrupts` | list | the run pauses for a human; empty again on the resumed run |

Elapsed time is `now − startedAt`; `heartbeatAt` proves the task is alive while nothing else moves.

## Run lifecycle

| Protocol | Type | Emitted when | Front usage |
|---|---|---|---|
| `agui_run_started` | RUN_STARTED | A run begins, including the resume after a pause | Open the run, show "working" |
| `agui_state_snapshot` | STATE_SNAPSHOT | Right after RUN_STARTED | Replace local state |
| `agui_state_delta` | STATE_DELTA | Any status change above, and the heartbeat | Apply the patch, re-render the pipeline |
| `agui_run_finished` | RUN_FINISHED | The run completed **or** paused for a human | `outcome.type == "interrupt"`: show each `outcome.interrupts[i].message` and wait for the user. Otherwise: close the run. Legacy clients read `result.status == "awaiting_tool_result"` |
| `agui_run_error` | RUN_ERROR | The run ended badly | Close the run. `code == "cancelled"`: neutral "stopped". Anything else: error |

`RUN_ERROR.code` values: the agent's error type (from agno), `module_error` (the module raised), `permission_denied`, `cancelled`, `partial_tool_results` (resume without every pending tool result), `auto_continue_limit`.

## Text and reasoning

| Protocol | Type | Front usage |
|---|---|---|
| `agui_text_message_start` / `_content` / `_end` | TEXT_MESSAGE_* | Append each `delta` to the bubble `messageId` (`name` labels a subagent's bubble) |
| `agui_reasoning_start`, `agui_reasoning_message_start` / `_content` / `_end`, `agui_reasoning_end` | REASONING_* | Collapsible "thinking" block |
| `agui_messages_snapshot` | MESSAGES_SNAPSHOT | Sent around a HITL pause; replace the message list |

## Tool calls

| Protocol | Type | Emitted when | Front usage |
|---|---|---|---|
| `agui_tool_call_start` | TOOL_CALL_START | A tool is invoked; `toolCallName` is the display name (`<manager>_<action>` for managers) | Add a tool card |
| `agui_tool_call_args` | TOOL_CALL_ARGS | Right after start; the whole args JSON in one `delta` | Show arguments |
| `agui_tool_call_end` | TOOL_CALL_END | The tool finished, failed, or paused for input | Stop the spinner |
| `agui_tool_call_result` | TOOL_CALL_RESULT | The tool returned, **or failed** with `content = {"error": …}` | Show the result; failure is also `tools.<id>.status = failed` |

AG-UI's `TOOL_CALL_RESULT` has no error flag, so read the state for the outcome rather than parsing `content`.

## Subagents

| Protocol | Type | Front usage |
|---|---|---|
| `agui_subagent_started` | SUBAGENT_STARTED | Open a group for `subagentRunId` |
| `agui_subagent_finished` | SUBAGENT_FINISHED | Mark it done |
| `agui_subagent_error` | SUBAGENT_ERROR | Mark it failed. Not terminal: the parent run continues |

## Human in the loop

1. `TOOL_CALL_START` / `ARGS` / `END` for the frontend tool, with no result.
2. `STATE_DELTA`: `tools.<id>.status = awaiting_input`, `interrupts`, `run.status = interrupted`.
3. `RUN_FINISHED` with `outcome = {type: "interrupt", interrupts: [{id, reason: "tool_call", toolCallId, message}]}` and the legacy `result`.
4. The user answers in a message carrying every pending tool result. The resumed run sends `RUN_STARTED` + a fresh `STATE_SNAPSHOT`, then continues.

## Custom

| Protocol | `name` | Front usage |
|---|---|---|
| `agui_custom` | `source_citation` | Add `{url, title, description}` to the sources panel ([`agui_custom_events.md`](agui_custom_events.md)) |
| `agui_custom` | anything else, incl. events relayed from tool modules | Ignore unknown names |

## Transport sentinels

| Protocol | Meaning | Front usage |
|---|---|---|
| `stream.start` | Stream opened | — |
| `stream.heartbeat` | Task alive but silent, with no AG-UI run open | Ignore |
| `stream.warn` | Recoverable issue | Optional notice |
| `stream.error` | `code`, `message`, `fatal` | Fatal without a prior `RUN_ERROR`: treat as a run error |
| `stream.cancelled` | Task cancelled (`reason`) | Arrives after `RUN_ERROR code=cancelled` |
| `stream.end` | Always the last frame | Close the stream |

See [`gateway_protocol.md`](gateway_protocol.md) for the sentinel rules.

## What the front sees per situation

| Situation | Frames |
|---|---|
| A tool fails | `TOOL_CALL_END`, `TOOL_CALL_RESULT {"error"}`, delta `tools.<id>.status = failed`; the run continues |
| A tool or the model runs long | delta `run.heartbeatAt` every 30s of silence |
| The agent reports an error | delta `run.status = failed`, `RUN_ERROR`, `stream.end` |
| The module raises | `RUN_ERROR code=module_error` (if a run is open), `stream.end` immediately |
| The user cancels | delta `run.status = cancelled`, `RUN_ERROR code=cancelled`, `stream.cancelled`, `stream.end` |
| The process dies | no heartbeat → after `read_idle_timeout_s` (300s): `stream.error STREAM_IDLE_TIMEOUT`, `stream.end` |
| Waiting on the human | `RUN_FINISHED outcome.type = interrupt` |
