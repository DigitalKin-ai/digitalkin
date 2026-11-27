# Production validation checklist

Every behaviour-changing fix that still needs production confirmation carries a
`# TODO(validate): <ID> <what>` comment next to a `[VALIDATE <ID>]` log line.
Search the prod logs for `[VALIDATE <ID>]` to confirm each behaviour, then delete
the marker pair.

Paths are relative to `src/digitalkin/`; line numbers point at the log call.

| ID                             | Location                                                   | Behaviour validated
|--------------------------------|------------------------------------------------------------|---------------------
| AGUI-SUBAGENT-SUSPENDED        | `community/agno/agno_adapter.py:1065`                      | On a HITL pause, open subagents close with `outcome: suspended` before `RUN_FINISHED`                                                                                                                       |
| AGUI-USAGE-LATE                | `mixins/agui_mixin.py:261`                                 | A run's own LLM calls are in its `RUN_FINISHED` / `RUN_ERROR` `usage`: no `[VALIDATE AGUI-USAGE-LATE]` line, which only logs a call started before its run closed |
| CALIBRATION-SKIP-FALLBACK      | `community/agno/compaction.py:650`                         | A fallback model's token count never becomes a calibration sample; the next head-model response still calibrates                                                                                            |
| COMPACTION-MID-RUN-FOLD        | `community/agno/compaction.py:576`                         | A tool loop whose provider count passes the threshold folds the replayed history mid-run, at most once per history                                                                                          |
| COST-CACHE-WRITE               | `community/litellm/cost.py:356`                            | A prompt-cache write is billed once, at the cache-write price, and no longer also inside `chatbot_input` |
| COST-SERVED-MODEL              | `community/litellm/cost.py:203`                            | A fallback call is billed at the served model's price; an unpriced served model falls back to the requested price                                                                                           |
| DOTTED-INDEX                   | `utils/json_structure.py:110`                              | A `services_manager` path or map key with a dotted list index (`items.0.title`, Gemini) is stored and edited as `items[0].title`; patch/append/remove no longer refuse it as not found |
| ESCAPED-TEXT-REFUSED           | `utils/setup_content_validator.py:155`                     | A `services_manager` write whose text a model escaped twice (`\n`, `\u00e9` written out) is refused, and the model resends the plain text                                                                   |
| GENERATE-AT                    | `community/agno/toolkits/registry/services/action.py:1108` | A `generate` section redone with the `at` of its `failed` entry lands at that index of the list, not after the later slices; redone neighbours keep their order
| KEPT-REQUEST                   | `community/agno/db.py:471`                                 | A mission whose run was cut (timeout, cancel, crash), ended in error or was left paused still shows the user's request to the next run: `outline_chat_history` is not empty
| NO-TIMEOUT-RESEND              | `community/agno/safe_litellm.py:870`                       | A model request cut by the proxy timeout (504 at 120 s) fails at once instead of being resent twice unchanged; transient failures resend once                                                               |
| PAGE-ESTIMATE                  | `community/agno/toolkits/registry/services/action.py:448`  | A text `load` page's "about N more pages" matches the pages read after it, even when the page ends early on a paragraph (was "about 2254 more pages") |
| REGISTRY-PLAIN-CONFIG          | `services/registry/grpc_registry.py:148`                   | A setup's nested config reaches `SetupInfo.config` as plain dicts and lists, logged once per setup |
| RELAYED-NOT-RESULT             | `community/agno/module_toolkit.py:499`                     | A tool that streams its output or an error next to a custom event or activity snapshot (`custom_app_updated`, `sources`) reaches the agent with that output or error, not the relayed event |
| RUN-ERROR-CODE-AGNO-PROVIDER   | `community/agno/agno_adapter.py:598`                       | A native agno model's failure reaches `RUN_ERROR` with the code its wording names (overflow, overload, rate limit, timeout, 400/403/404, outage); one naming no cause is `UNKNOWN`, without a retryable hint |
| RUN-ERROR-CODE-MODEL           | `community/agno/safe_litellm.py:379`                       | A litellm provider failure reaches `RUN_ERROR` as its `MODEL_*` code, with the model and provider text in `message`                            |
| RUN-ERROR-CODE-UNMAPPED        | `community/agno/agno_adapter.py:603`                       | No agno error type reaches `RUN_ERROR` / `SUBAGENT_ERROR` as `UNKNOWN`; any logged one gets its own `RunErrorCode`                          |
| SERVICE-WRITE-LOCK             | `community/agno/toolkits/registry/base.py:122`             | Two parallel `services_manager` writes on one service (patch/append/remove/update/generate) both land instead of the last one overwriting the other                                                         |
| SET-VERSION-RECEIPT            | `community/agno/toolkits/registry/services/action.py:1199` | `services_manager` `set_version` answers a receipt (version, `content_chars`) instead of the activated version's whole configuration |
| SETUP-INT-ROUNDTRIP            | `services/setup/setup_strategy.py:58`                      | A setup's content read through the setup service has its whole numbers as int, not float (`1`, not `1.0`)
| SETUP-VALIDATORS               | `models/module/setup_types.py:157`                         | A setup class's own field and model validators run on the clean (runtime) model; one whose fields are all filtered out is dropped and logged                                                                |
| STAGED-RECEIPT                 | `community/agno/toolkits/registry/base.py:155`             | A `services_manager` write with `set_as_current=false` answers the version it cut (from the version history), and `set_version` on that id activates it
| STORAGE-INT-ROUNDTRIP          | `services/storage/grpc_storage.py:148`                     | Whole numbers stored in a record payload read back as `int` (Struct holds doubles), not `1.0`; numbers above 2**53 stay float                                                                               |
| STREAM-DROP-RETRY              | `community/agno/safe_litellm.py:816`                       | A stream the LiteLLM proxy drops before its first chunk (`MidStreamFallbackError`, `is_pre_first_chunk`) is resent once; after output it still fails                                                        |
| STREAM-ERROR-FRAME             | `community/agno/safe_litellm.py:808`                       | With the proxy's SSE keepalive on, a timeout (408, 504) or rejection (4xx but 429) error frame arriving under a 200 fails once instead of being resent; a 429 or 5xx frame is still resent once             |
| STREAM-REFUSAL-CLOSED          | `community/agno/safe_litellm.py:843`                       | A provider refusal in a stream is raised after the stream ends: no `Unclosed client session` after a `content_filter`
| TEXT-PAGE-FIT                  | `community/agno/toolkits/registry/services/action.py:436`  | A text `load` page longer as JSON than its budget (quotes, control characters, short tail the splitter returns whole) is cut to the budget on a word boundary
| TOOL-FAILURE-TEXT              | `community/agno/module_toolkit.py:438`                    | A tool that raises reaches the calling agent as `[MODULE_FAILED] <message>` (plus the retry hint of a `ToolError`), not `No successful response received from module` |
| TOOL-SCHEMA-PORTABLE           | `community/agno/safe_litellm.py:1027`, `:986`              | Tool definitions reach every provider: agno's `requires_confirmation`/`external_execution`/`approval_type` and the `discriminator` are dropped (logged at debug), `oneOf` is sent as `anyOf`; Mistral and Kimi kins answer without a fallback |
| TRIGGER-DEFAULT-ON             | `models/module/tool_reference.py:236`                      | A kin whose tool selection predates a trigger rename gets the renamed triggers on (logged with their names); a partial selection with current names is unchanged
| WRITER-STREAM                  | `community/agno/toolkits/registry/services/action.py:1035` | `generate` sections stream (reasoning untouched): with the proxy's SSE keepalive on, no section 504 at 120 s even when `Slow model stream` shows a long first-chunk silence; no reasoning text in a section |

## Removing markers

Once an ID is validated, delete its `# TODO(validate)` comment and `[VALIDATE <ID>]`
log at every listed location and drop its row here. List what remains with:

```bash
rg -n "TODO\(validate\)|\[VALIDATE" src
```
