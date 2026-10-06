# Production validation checklist

Every behaviour-changing fix that still needs production confirmation carries a
`# TODO(validate): <ID> <what>` comment next to a `[VALIDATE <ID>]` log line.
Search the prod logs for `[VALIDATE <ID>]` to confirm each behaviour, then delete
the marker pair.

Paths are relative to `src/digitalkin/`; line numbers point at the log call.

| ID | Location | Behaviour validated | Validated when prod logs show |
|----|----------|---------------------|-------------------------------|
| ADAPTER-RESET | `community/agno/agno_adapter.py:328` | Per-run dedup state (cited URLs, closed tool calls, completed runs) is cleared on each new top-level agno run | One line per top-level run; no duplicate citations or missing tool-call events on multi-turn threads |
| AGUI-TOOL-FAILED-STATUS | `community/agno/agno_adapter.py:684` | A tool Agno flags `tool_call_error` (incl. failed module tools, which now raise) surfaces as a failed tool, not a success | Each line matched on the front by `TOOL_CALL_RESULT {"error"}` + `tools/<id>/status = failed`; the LLM still receives the same error text |
| ASSOCIATE-TASK | `services/communication/grpc_communication.py:435` | M2M calls mint a child task id via `AssociateTask` | `parent=` / `child=` pairs with a non-empty, distinct child id on every M2M call |
| CANCEL-EARLY | `core/task_manager/task_executor.py:131` | A task cancelled before its first step is still finalized (status, stream end, cleanup) | Each occurrence followed by normal session cleanup; no leaked sessions |
| CANCEL-SENTINEL | `modules/_base_module.py:777` | `stream.cancelled` is emitted before `stream.end` on cancel | Consumers receive `stream.cancelled` with the logged `reason` before the end |
| CANCEL-TOMBSTONE | `core/task_manager/module_runner.py:220`, `:249`, `:306` | The `cancel:{id}` tombstone stops a task cancelled before/during registration | Lines appear for early cancels and those tasks end with `stream.cancelled` + EOS, not a full run |
| CHAN-EVICT | `grpc_servers/utils/grpc_client_wrapper.py:231` | An evicted channel stays open for its holders and closes on the last release | Evictions with `holder(s) > 0` produce no "channel closed" errors on in-flight calls |
| CHAT-HISTORY-CAP | `community/agno/toolkits/chat_history.py:158`, `:190`, `:196` | Chat-history page, read-id and content-length caps apply | Caps fire on long threads and the agent pages instead of failing |
| CLEANUP-ORDER | `core/task_manager/task_session.py:207` | Module `stop()` runs before context cleanup | One line per session; no "Error stopping module" / closed-client errors around it |
| CLEANUP-SHIELD | `core/task_manager/base_task_manager.py:125` | A cancel arriving during task cleanup lets the cleanup finish first | Each occurrence followed by complete cleanup (slot released, session removed) |
| COMM-REFCOUNT | `services/communication/grpc_communication.py:181` | One pooled channel ref per target per communication instance | One acquire per target per instance, not per call |
| CONFIG-SLOT | `core/job_manager/single_job_manager.py:157` | Config-setup sessions are tracked outside the task slots | Config sessions never block or consume task slots under load |
| ENUM-ENCODE | `services/registry/grpc_registry.py:467` | Registry filter enums always encode to a proto member (fail closed) | Line never appears (it only fires on Python/proto drift) |
| HITL-TRIM | `community/agno/hitl.py:98` | Paused runs are stored without history messages/events and still resume | Each paused run followed by a successful resume of the same `thread_id` |
| IDEM-RECLAIM | `grpc_servers/gateway_servicer.py:235` | A refused re-`StartStream` does not refresh the idem TTL | Refusals occur and duplicate task ids are never re-run |
| IDEM-RELEASE | `grpc_servers/gateway_servicer.py:817` | `idem:{id}` is released when the dial fails before the runner spawns | Retried `StartStream` for the same task id is accepted afterwards |
| IDEM-TTL-EOS | `core/task_manager/module_runner.py:160`, `grpc_servers/gateway_servicer.py:346` | `idem:{id}` TTL drops to `redis_stream_ttl` once EOS is written | One line per finished task; `idem:*` key count stays bounded in Redis |
| LEGACY-MODULE-TYPE | `models/services/registry.py:64`, `:68` | Legacy `module_type` values (`tool`, `kin`) are normalized | Line stops appearing once legacy setups are purged; then drop the normalization too |
| MODULE-FAILED-EOS | `modules/_base_module.py:590` | A module that raised still runs `stop()` and writes `stream.end` (plus `RUN_ERROR` when an AG-UI run is open) | Each line followed by EOS within seconds; `STREAM_IDLE_TIMEOUT` after module crashes disappears; module `stop()` hooks tolerate a failed run |
| NO-INPUT | `grpc_servers/gateway_servicer.py:432`, `:474`, `:1124` | Follow-up data is dropped when no `:input` consumer exists | Drops only for modules that take no follow-up input; no user-visible missing input |
| REPEAT-CANCEL | `core/task_manager/redis/redis_signal.py:218` | Repeated cancels are suppressed while a cancel is pending | Duplicate cancels logged and skipped; tasks still end once as cancelled |
| RUNNER-LEAK | `core/task_manager/module_runner.py:314`, `:373`, `core/job_manager/single_job_manager.py:305` | A preloaded module instance is released when its task never starts | Each line followed by cleanup; no growth in live module instances |
| SEARCH-DEADLINE | `services/registry/grpc_registry.py:270`, `:575` | The tightened agent-facing registry search deadline causes no spurious failures | Line is absent or rare, and never with `DEADLINE_EXCEEDED` under normal load |
| SECRET-FETCH | `services/secret/grpc_secret.py:63`, `:71` | Setup secrets are fetched per setup/mission | `success=True keys=N` for setups with secrets; `success=False` only where none exist |
| SEED-TTL | `grpc_servers/gateway_servicer.py:277` | The `stream.start` seed carries a TTL and drops stale stream/cursor keys | One line per accepted task; `stale keys dropped` > 0 only on retried task ids |
| SETUP-ACCESS | `grpc_servers/module_servicer.py:294`, `:298`, `:387` | Setup access is checked before the setup is resolved | `granted` for owned setups, `DENIED` only for foreign ones |
| SETUP-VALIDATION | `core/task_manager/module_runner.py:126` | A setup `ValidationError` surfaces as `SETUP_VALIDATION_ERROR` | Each line matched by a `stream.error` with that code on the consumer side |
| SHUTDOWN-ORDER | `grpc_servers/module_server.py:319` | The gRPC server stops before running tasks are cancelled | On deploys, line precedes task cancellation; no new tasks accepted during shutdown |
| SIGNAL-PUB | `grpc_servers/gateway_servicer.py:581` | `SendSignal` publishes without relying on the local registry | `receivers >= 1` for live tasks on any replica, and those tasks react |
| STORAGE-PERMISSION-DENIED | `services/storage/grpc_storage.py:175`, `:205`, `:245`, `:273`, `:314`, `:343` | Storage permission denials propagate to the caller as `PermissionDeniedError` | Denials appear only for foreign records and surface as access errors, not generic failures |
| SVC-MODE-CLOSE | `services/services_config.py:197` | Cached service singletons are closed on a mode switch | Each mode switch closes the previous singletons; no leaked channels |
| TASK-HEARTBEAT | `core/task_manager/module_runner.py:274` | A running-but-silent task writes a liveness frame every `heartbeat_interval_s` | Long tools/LLM calls (> `read_idle_timeout_s`) finish without `STREAM_IDLE_TIMEOUT`; no beats after a task ends |
| TOOL-CANCELLED | `models/module/module_context.py:549` | A cancelled tool call fails instead of returning a benign result | Parent runs log a tool failure and terminate normally |
| TOOL-DROP-ZERO | `models/module/tool_reference.py:225` | A tool resolving to 0 functions is dropped | Only for setups with no usable triggers; agents never see empty tools |
| TOOL-LAST-FRAME | `community/agno/module_toolkit.py:531` | Tool calls buffer only the last success/error frame | `kept success=True` or `error=True` per call; tool results unchanged for users |
| TTL-REFRESH | `core/task_manager/module_runner.py:166` | Every output re-arms `task:{id}:stream` (`redis_stream_initial_ttl`) and `idem:{id}` (`idem_ttl`) in the XADD pipeline until EOS | Line appears for tasks running longer than `redis_stream_initial_ttl`, with no `STREAM_IDLE_TIMEOUT` or lost stream and cancels answering `success=True` |
| UPSERT-UPDATE-FIRST | `services/storage/storage_strategy.py:455` | Upsert updates first and creates only on a miss | Line only on first write of a record; no duplicate records |

## Removing markers

Once an ID is validated, delete its `# TODO(validate)` comment and `[VALIDATE <ID>]`
log at every listed location and drop its row here. List what remains with:

```bash
rg -n "TODO\(validate\)|\[VALIDATE" src
```
