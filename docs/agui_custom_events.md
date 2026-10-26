# AG-UI custom events

AG-UI has no dedicated event type for application data. Its extension point is `CUSTOM` (`name` + `value`); every AG-UI client receives it and routes on `name`. DigitalKin ships every `CUSTOM` event under the `agui_custom` protocol.

This page covers `CUSTOM` only; every other AG-UI event and the pipeline state are in [`agui_events.md`](agui_events.md).

## Envelope

```json
{"root": {"protocol": "agui_custom", "createdAt": "2026-09-29T10:14:03.512Z",
  "event": {"type": "CUSTOM", "name": "<event name>", "value": {…},
            "subagentRunId": "…", "metadata": {"digitalkin": {…}}}},
 "annotations": {}}
```

- AG-UI fields are camelCase (`subagentRunId`); keys inside `value` are passed as-is.
- `subagentRunId` and `metadata` are present only when the event was produced inside a delegated subagent run. Group on `subagentRunId`, like text and tool events.
- Null fields are omitted.

## Sources

Sources are no longer a custom event: they travel as the `sources` activity ([`agui_events.md#sources`](agui_events.md#sources)). The `source_citation` name is still accepted as an *input* (agno `CustomEvent`, `SourceCitationEvent`) and converted.
