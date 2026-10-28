# AG-UI events and pipeline state

Every frame a module streams to the front, what triggers it, and how the front uses it. The front never writes any of this: it only sends user messages (a resume after a human-in-the-loop pause is a message too). The SDK owns the whole AG-UI state.

## Envelope

Each frame is the `data` Struct of one `StreamClient`:

```json
{"root": {"protocol": "agui_tool_call_start", "createdAt": "…",
  "event": {"type": "TOOL_CALL_START", "toolCallId": "…", "toolCallName": "…",
            "subagentRunId": "…",
            "metadata": {"digitalkin": {…}, "toolDisplayName": "…", "functionDisplayName": "…"}}},
 "annotations": {}}
```

- The SDK produces AG-UI **1.0** (`ag-ui-protocol==1.0.0`): every `RUN_STARTED` carries `protocolVersion: "1.0"`. A front on `@ag-ui/client` 1.x validates every event against the 1.0 schema.
- AG-UI events use `protocol = agui_<snake_case type>`. Transport sentinels use `stream.*` and carry no `event`.
- `subagentRunId` names the delegated agent that produced the event; group on it. Run-level events never carry it.
- Fields are camelCase. Optional fields are absent, never `null`; `null` only appears inside application data (`STATE_*` values and patch `value`, `CUSTOM.value`, activity `content`).
- No event leaves with an empty or invented id: an event whose id is missing is dropped and logged (`[VALIDATE AGUI-EMPTY-ID]`), since a 1.x client fails the run on a continuation or close of a message it never saw opened.
- SDK data rides in `metadata.digitalkin`, except the tool-call labels `metadata.toolDisplayName` / `metadata.functionDisplayName` on `TOOL_CALL_START`; the `ag-ui` key is reserved. A 1.x client merges event `metadata` onto the message it builds and sends it back in `RunAgentInput`, so nothing secret is ever written there.

## Pipeline state

`STATE_SNAPSHOT` follows every `RUN_STARTED` and replaces the front's state. Each later change is one `STATE_DELTA` (RFC 6902 JSON Patch, `add`/`replace` only; ids in paths are escaped `~`→`~0`, `/`→`~1`).

```json
{
  "run": {"status": "running", "error": null, "heartbeatAt": null},
  "tools": {"<toolCallId>": {"name": "rag__search", "toolDisplayName": "Document search",
                             "functionDisplayName": "Search documents", "status": "running",
                             "subagentRunId": null, "startedAt": 1759650000000, "error": null}},
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
| `subagents.<id>.status` | `running` → `completed` \| `failed` \| `suspended` | subagent start, end, failure, HITL pause |
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

`RUN_ERROR.code` values: the agent's error type (from agno), `module_error` (the module raised; `message` is always `module execution failed`, the detail stays in the server log), `permission_denied`, `cancelled`, `partial_tool_results` (resume without every pending tool result), `auto_continue_limit`, `INPUT_VALIDATION_ERROR` (the run input failed validation; a retired `binary` part is upgraded first when the kin uses `AgUiStreamInput`; sent as the first event, before `stream.error`).

A tool call, text or reasoning message still open when the run completes is closed before `RUN_FINISHED`; an unfinished tool call closes as a failure (`TOOL_CALL_RESULT {"error": "tool call did not complete before its run ended"}`). After `RUN_ERROR` nothing is closed: per AG-UI 1.0 the front treats every open message, tool call and subagent as abandoned.

## Text and reasoning

| Protocol | Type | Front usage |
|---|---|---|
| `agui_text_message_start` / `_content` / `_end` | TEXT_MESSAGE_* | Append each `delta` to the bubble `messageId` (`name` labels a subagent's bubble) |
| `agui_reasoning_start`, `agui_reasoning_message_start` / `_content` / `_end`, `agui_reasoning_end` | REASONING_* | Collapsible "thinking" block |
| `agui_messages_snapshot` | MESSAGES_SNAPSHOT | Not sent by the SDK itself; a module may send one from `PauseInfo.new_messages`. It carries no activity messages, so the front keeps its `sources` |

## Tool calls

| Protocol | Type | Emitted when | Front usage |
|---|---|---|---|
| `agui_tool_call_start` | TOOL_CALL_START | A tool is invoked | Add a tool card titled with `metadata.toolDisplayName` and `metadata.functionDisplayName` |
| `agui_tool_call_args` | TOOL_CALL_ARGS | Right after start; the whole args JSON in one `delta` | Show arguments |
| `agui_tool_call_end` | TOOL_CALL_END | The tool finished, failed, or paused for input | Stop the spinner |
| `agui_tool_call_result` | TOOL_CALL_RESULT | The tool returned, **or failed** with `content = {"error": …}` | Show the result; failure is also `tools.<id>.status = failed` |

AG-UI's `TOOL_CALL_RESULT` has no error flag, so read the state for the outcome rather than parsing `content`.

`toolCallName` stays the technical function name the LLM called (`services_manager`, `rag__search`, `ask_form`): the front dispatches frontend tools on it and the AG-UI client writes it into the message history. Show the labels instead; they ride in the event's `metadata`, beside `digitalkin`, and an absent label is not sent:

| Field | Content | Example |
|---|---|---|
| `toolDisplayName` | The tool: a module tool's registry setup name, a manager's or SDK toolkit's label, else the humanized function name | `Document search`, `Services Manager`, `Ask form` |
| `functionDisplayName` | The called function: the trigger's `display_name` (always set for a module tool, even with a single trigger), a manager action's label, a humanized name for a multi-function SDK toolkit. Absent for a single-function SDK toolkit, a frontend tool, a team member's tool, or when the run has no toolkit map (`AgnoStreamAdapter()` built without `toolkits`) | `Search documents`, `Create` |

Both are also in the state entry `tools.<id>`, where an absent label is `null`.

## Subagents

| Protocol | Type | Front usage |
|---|---|---|
| `agui_subagent_started` | SUBAGENT_STARTED | Open a group for `subagentRunId` |
| `agui_subagent_finished` | SUBAGENT_FINISHED | Mark it done; `outcome.type == "suspended"` means paused for a human and may continue under the same id on the resumed run |
| `agui_subagent_error` | SUBAGENT_ERROR | Mark it failed. Not terminal: the parent run continues |

Rules the SDK guarantees (AG-UI 1.0): a subagent is announced before any event attributed to it; an id is never reused within a run; a nested subagent's parent is announced first; every subagent is closed before `RUN_FINISHED` (success, or `suspended` on a HITL pause); every `*_END` carries the attribution of its `*_START`; parallel subagents interleave and one closing never closes another's messages.

## Human in the loop

1. `TOOL_CALL_START` / `ARGS` / `END` for the frontend tool, with no result.
2. `STATE_DELTA`: `tools.<id>.status = awaiting_input`, `interrupts`, `run.status = interrupted`.
3. `RUN_FINISHED` with `outcome = {type: "interrupt", interrupts: [{id, reason: "tool_call", toolCallId, message}]}` and the legacy `result`.
4. The user answers in a message carrying every pending tool result. The resumed run sends `RUN_STARTED` + a fresh `STATE_SNAPSHOT`, then continues.

## Sources

The sources the agent used travel as an AG-UI **activity**, not a custom event: they are messages with `role: "activity"` in the client's message list, survive message snapshots, and are stripped from `RunAgentInput`, so they never reach the LLM.

```json
{"type": "ACTIVITY_SNAPSHOT", "messageId": "sources-<uuid>[-<subagentRunId>]", "activityType": "sources",
 "replace": true, "subagentRunId": "…",
 "content": {"sources": [{"url": "https://docs.digitalkin.ai/pricing", "title": "Pricing – DigitalKin",
                          "description": "Pro plan is 49 €/month"}]}}
```

| Field | Rule |
|---|---|
| `messageId` | One activity message per owner and run: the agent, and each subagent (`-<subagentRunId>` suffix). A subagent's list never replaces its parent's |
| `content.sources` | The **complete** list so far; each snapshot replaces the previous one (`replace` is always explicit `true`) |
| `url` | http/https only; deduplicated without the fragment (`https://a.io` and `https://a.io/#x` are one source). Always present |
| `title`, `description` | Optional plain text (render as text, never HTML), at most 300 / 1000 characters. A later citation of a known URL only fills fields it lacked |
| cap | `DIGITALKIN_AGUI_MAX_SOURCES` (50) per run; extra sources are dropped and logged |
| `subagentRunId` | Present when a subagent cited them |

Ordering: an activity message is created where it arrives, so a run's sources precede its answer. **Attach them to the next assistant message of the same run with the same `subagentRunId`**, not the previous one.

Emitting is unchanged for code: model-native citations (agno `citations.urls`), `DkToolkit._cite(url, title=, description=)`, an agno `CustomEvent(name="source_citation", value={...})`, or `send_message(context, SourceCitationEvent(...))` from an `AgUiMixin` trigger. A remote tool module's sources are merged into the calling agent's list by `ModuleToolkit`. Invalid sources are logged and dropped; they never fail the run.

## Custom

AG-UI's extension point for application data is `CUSTOM` (`name` + `value`). DigitalKin ships every `CUSTOM` event under the `agui_custom` protocol; the front routes on `name` and ignores names it does not know.

```json
{"root": {"protocol": "agui_custom", "createdAt": "2026-09-29T10:14:03.512Z",
  "event": {"type": "CUSTOM", "name": "<event name>", "value": {…},
            "subagentRunId": "…", "metadata": {"digitalkin": {…}}}},
 "annotations": {}}
```

- Keys inside `value` are passed as-is; AG-UI fields are camelCase.
- `subagentRunId` and `metadata` appear only for events produced inside a subagent run; group on `subagentRunId` like text and tool events.
- Events relayed from tool modules keep their `metadata` and lose their own `subagentRunId`.
- Sources are not a custom event (see [Sources](#sources)); `source_citation` is accepted only as an input name and converted.

## Migrating the front to AG-UI 1.0

1. The front moves to `@ag-ui/client` 1.x first and renders sources from **both** the legacy `CUSTOM source_citation` event and the `sources` activity.
2. The SDK on AG-UI 1.0 is deployed: `source_citation` no longer travels as `CUSTOM`.
3. The front drops its `CUSTOM source_citation` handler.

Attachments: AG-UI 1.0 retired the `binary` content part, and a message carrying one fails input validation. A kin whose input trigger is `digitalkin.community.agno.AgUiStreamInput` accepts it anyway: each `binary` part with a `url` or `data` becomes an `image`/`audio`/`video`/`document` part (by MIME prefix, as `@ag-ui/client` 1.x converts it) with `metadata.fileId` and `metadata.filename`; a part carrying only an `id` is dropped and logged. The 1.0 shape to send:

```json
{ "type": "document",
  "source": { "type": "url", "value": "<url>", "mimeType": "application/pdf" },
  "metadata": { "fileId": "<file id>", "filename": "rapport.pdf" } }
```

The kin resolves the file itself (filesystem, RAG); the SDK never fetches a URL or a file.

The 1.x client's compatibility shims for 0.x producers expire on the dates listed in AG-UI's `DEPRECATIONS.md`; do not rely on them.

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
| The process dies | no heartbeat → after `DIGITALKIN_GATEWAY_STREAM_READ_IDLE_TIMEOUT_S` (300s): `stream.error STREAM_IDLE_TIMEOUT`, `stream.end` |
| Waiting on the human | open subagents `SUBAGENT_FINISHED outcome=suspended`, `RUN_FINISHED outcome.type = interrupt` |
| The run input is invalid | `RUN_ERROR code=INPUT_VALIDATION_ERROR`, `stream.error INPUT_VALIDATION_ERROR`, `stream.end` |
